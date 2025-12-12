import rasterio
import numpy as np
import geopandas as gpd
import pandas as pd
import os
import matplotlib.pyplot as plt
from rasterstats import zonal_stats
from rasterio.enums import Resampling
import rasterio.warp
from math import pi

#Rutas y Datos Principales
BASE_DIR = "/home/victor/Documentos/Proyección_Hotspots/"
RASTER_DIR = os.path.join(BASE_DIR, "RASTER/")
VECTOR_DIR = os.path.join(BASE_DIR, "VECTOR/")
OUTPUT_DIR = os.path.join(BASE_DIR, "RESULTADOS/")
NODATA_VAL_OUT = -9999.0
OUTPUT_EXCEL = os.path.join(OUTPUT_DIR, "Reporte_Hotspots_Zonal_MultiPais.xlsx")

# Rutas de SHP para el Analisis Zonal
VECTOR_PATHS = {
    "PARAGUAY_DEPTO": {"path": os.path.join(VECTOR_DIR, "paraguay_2/depts_estudio.shp"), "field": 'dpto_desc', "nivel": "Departamento"},
    "URUGUAY_DEPTO": {"path": os.path.join(VECTOR_DIR, "departamentos/c004Polygon.shp"), "field": 'nombre', "nivel": "Departamento"},
    "BRASIL_ESTADO": {"path": os.path.join(VECTOR_DIR, "datos_BR.gpkg"), "field": 'nome', "layer": 'estados_br', "nivel": "Estado"},
    "ARGENTINA_PROV": {"path": os.path.join(VECTOR_DIR, "provincia/Provincias.shp"), "field": 'nam', "nivel": "Provincia"},
    "ARGENTINA_REGION": {"path": os.path.join(VECTOR_DIR, "regiones/Regiones_ARG.shp"), "field": 'REGION', "nivel": "Región"}
}

# Variables bio
BIOS = {
    1: 'BIO1', # Temp media anual
    5: 'BIO5', # Temp max del mes mas calido
    14: 'BIO14', # Precipitacion del mes mas seco
    15: 'BIO15' # Estacionalidad de la precipitacion
}
FILE_PREFIX_HIST = "bio{}_his.tif"
FILE_PREFIX_FUT = "bio{}_fut.tif"

os.makedirs(OUTPUT_DIR, exist_ok=True)
print(f"Salida: {OUTPUT_DIR}")

## -----------------------------------------------------------
## PARTE A: DELTAS Y Z-SCORE
## -----------------------------------------------------------

print("Calculando Deltas y Normalizando Z-Score...")
delta_rasters_paths = {}
all_delta_values = []

# Deltas
for index, name in BIOS.items():
    hist_path = os.path.join(RASTER_DIR, FILE_PREFIX_HIST.format(index))
    fut_path = os.path.join(RASTER_DIR, FILE_PREFIX_FUT.format(index))
    delta_path = os.path.join(OUTPUT_DIR, f"DELTA_{name}.tif") 

    try:
        with rasterio.open(hist_path) as src_hist, rasterio.open(fut_path) as src_fut:
            data_hist = src_hist.read(1).astype(np.float32)
            data_fut = src_fut.read(1).astype(np.float32)
            delta_data = data_fut - data_hist
            
            nodata_hist = src_hist.nodata if src_hist.nodata is not None else NODATA_VAL_OUT
            nodata_fut = src_fut.nodata if src_fut.nodata is not None else NODATA_VAL_OUT
            valid_mask = (data_hist != nodata_hist) & (data_fut != nodata_fut) & (~np.isnan(delta_data))
            delta_data[~valid_mask] = NODATA_VAL_OUT 
            valid_values = delta_data[valid_mask]
            
            if valid_values.size > 0:
                all_delta_values.append(valid_values)
            
            out_meta = src_hist.profile.copy()
            out_meta.update({"dtype": 'float32', "nodata": NODATA_VAL_OUT})
            
            with rasterio.open(delta_path, "w", **out_meta) as dest:
                dest.write(delta_data, 1)
            
            delta_rasters_paths[name] = delta_path
            
    except Exception as e:
        print(f"ERROR al calcular Delta para BIO{index}: {e}")
        continue

# Calculo Global de Z-Score
try:
    all_values_combined = np.concatenate(all_delta_values)
    mean_global = np.mean(all_values_combined)
    std_global = np.std(all_values_combined)
except ValueError:
    print("ERROR: no hay valores delta validos")
    exit()

# Normalizacion Z-Score
normalized_rasters_paths = {}
for name, delta_path in delta_rasters_paths.items():
    with rasterio.open(delta_path) as src:
        delta_data = src.read(1).astype(np.float32)
        valid_mask = delta_data != NODATA_VAL_OUT
        z_score_data = np.full(delta_data.shape, NODATA_VAL_OUT, dtype=np.float32)
        z_score_data[valid_mask] = (delta_data[valid_mask] - mean_global) / std_global
        z_score_path = os.path.join(OUTPUT_DIR, f"Z_{name}.tif") 
        out_meta = src.meta.copy()
        out_meta.update({"dtype": 'float32', "nodata": NODATA_VAL_OUT})
        with rasterio.open(z_score_path, "w", **out_meta) as dest:
            dest.write(z_score_data, 1)
        normalized_rasters_paths[name] = z_score_path

# Calculo del Indice de impacto Agregado (SUMA DIRECTA, SIN INVERSION de BIO_14)
print("\nCalculando indice de impacto agregado...")
z_rasters = [rasterio.open(p) for p in normalized_rasters_paths.values()]
first_meta = z_rasters[0].meta.copy()
z_score_arrays = [r.read(1) for r in z_rasters]

impacto_agregado_data = np.zeros_like(z_score_arrays[0])
valid_sum_mask = np.full(z_score_arrays[0].shape, True)

# Suma directa de todos los Z-scores (Z_BIO1 + Z_BIO5 + Z_BIO14 + Z_BIO15)
for arr in z_score_arrays:
    current_valid = (arr != NODATA_VAL_OUT)
    impacto_agregado_data[current_valid] += arr[current_valid]
    valid_sum_mask &= current_valid 

impacto_agregado_data[~valid_sum_mask] = NODATA_VAL_OUT
impacto_agregado_path = os.path.join(OUTPUT_DIR, "INDICE_IMPACTO_AGREGADO.tif")
first_meta.update({"dtype": 'float32', "nodata": NODATA_VAL_OUT})
with rasterio.open(impacto_agregado_path, "w", **first_meta) as dest:
    dest.write(impacto_agregado_data, 1)
print(f"Indice de impacto agregado creado: {impacto_agregado_path}")


## -----------------------------------------------------------
## PARTE B: ANALISIS ZONAL Y CALCULO DE INDICES COMPUESTOS
## -----------------------------------------------------------

print("Realizando Analisis Zonal y calculo de indices")

rasters_to_analyze = {
    'delta_BIO1': delta_rasters_paths['BIO1'], 'delta_BIO5': delta_rasters_paths['BIO5'],
    'delta_BIO14': delta_rasters_paths['BIO14'], 'delta_BIO15': delta_rasters_paths['BIO15'],
    'z_BIO1': normalized_rasters_paths['BIO1'], 'z_BIO5': normalized_rasters_paths['BIO5'],
    'z_BIO14': normalized_rasters_paths['BIO14'], 'z_BIO15': normalized_rasters_paths['BIO15'],
    'Indice_consolidado_RASTER': impacto_agregado_path # Nombre temporal para evitar conflicto
}

all_country_reports = {} 

for country_key, settings in VECTOR_PATHS.items():
    
    if "layer" in settings:
        area_estudio = gpd.read_file(settings['path'], layer=settings['layer'])
    else:
        area_estudio = gpd.read_file(settings['path'])
        
    key_field = settings['field']
    nivel = settings['nivel']

    if key_field not in area_estudio.columns:
        area_estudio['NOMBRE_ZONA'] = area_estudio.index
    else:
        area_estudio['NOMBRE_ZONA'] = area_estudio[key_field].astype(str).str.title()
    
    zonal_results = []
    
    for index, row in area_estudio.iterrows():
        zone_stats = {}
        for stat_name, raster_path in rasters_to_analyze.items():
            stats = zonal_stats(
                vectors=[row.geometry], raster=raster_path,
                stats=['mean', 'min', 'max'], nodata=NODATA_VAL_OUT
            )[0]
            
            zone_stats.update({
                f'{stat_name}_mean': stats['mean'],
                f'{stat_name}_min': stats['min'],
                f'{stat_name}_max': stats['max']
            })

        zone_stats['NOMBRE_ZONA'] = row['NOMBRE_ZONA']
        zone_stats['PAIS_KEY'] = country_key.split('_')[0]
        zone_stats['NIVEL_ADM'] = nivel
        zonal_results.append(zone_stats)
    
    df_zonal = pd.DataFrame(zonal_results)

    # CALCULO DE INDICES COMPUESTOS (SUMA DIRECTA - SIN INVERSION BIO_14)
    df_zonal['I_estress_termico'] = df_zonal['z_BIO1_mean'] + df_zonal['z_BIO5_mean']
    df_zonal['I_estress_hidrico'] = df_zonal['z_BIO14_mean'] + df_zonal['z_BIO15_mean']
    
    # Indice Consolidado = Z_BIO1 + Z_BIO5 + Z_BIO14 + Z_BIO15 (Suma directa)
    df_zonal['Indice_consolidado'] = df_zonal['z_BIO1_mean'] + df_zonal['z_BIO5_mean'] + df_zonal['z_BIO14_mean'] + df_zonal['z_BIO15_mean']
    
    # Usamos el calculado directamente, no la media zonal del raster agregado
    df_zonal.drop(columns=['Indice_consolidado_RASTER_mean', 'Indice_consolidado_RASTER_min', 'Indice_consolidado_RASTER_max'], inplace=True)
    
    # Renombrar columnas para el formato del Excel/Radar
    df_zonal.rename(columns={
        'delta_BIO1_mean': 'delta_BIO1', 'delta_BIO5_mean': 'delta_BIO5',
        'delta_BIO14_mean': 'delta_BIO14', 'delta_BIO15_mean': 'delta_BIO15',
        'z_BIO1_mean': 'z_bio1', 'z_BIO5_mean': 'z_bio5',
        'z_BIO14_mean': 'z_bio14', 'z_BIO15_mean': 'z_bio15'
    }, inplace=True)
    
    df_ordenado = df_zonal.dropna(subset=['Indice_consolidado'])
    all_country_reports[country_key] = df_ordenado


## -----------------------------------------------------------
## PARTE C: GENERAR EXCEL Y RADAR
## -----------------------------------------------------------

print("Generando Excel y Radar...")

#  Excel MultiHoja
ranking_global_df = pd.concat(all_country_reports.values(), ignore_index=True)
ranking_global_df = ranking_global_df.sort_values(by='Indice_consolidado', ascending=False).dropna(subset=['Indice_consolidado'])
ranking_global_df['RANK'] = np.arange(1, len(ranking_global_df) + 1)

cols_output_excel = ['RANK', 'PAIS_KEY', 'NIVEL_ADM', 'NOMBRE_ZONA', 
               'I_estress_termico', 'I_estress_hidrico', 'Indice_consolidado',
               'delta_BIO1', 'delta_BIO5', 'delta_BIO14', 'delta_BIO15',
               'z_bio1', 'z_bio5', 'z_bio14', 'z_bio15']

with pd.ExcelWriter(OUTPUT_EXCEL, engine='xlsxwriter') as writer:
    ranking_global_df[cols_output_excel].to_excel(writer, sheet_name='Ranking Global de Riesgo', index=False)
    
    for country_key, df in all_country_reports.items():
        df.sort_values(by='Indice_consolidado', ascending=False, inplace=True)
        cols_sheet = ['PAIS_KEY', 'NIVEL_ADM', 'NOMBRE_ZONA', 
                      'I_estress_termico', 'I_estress_hidrico', 'Indice_consolidado'] + \
                     [col for col in df.columns if ('delta_' in col or 'z_' in col or '_min' in col or '_max' in col)]
        sheet_name = country_key.replace('_DEPTO', ' (Dptos)').replace('_ESTADO', ' (Ests)').replace('_PROV', ' (Prov)').replace('_REGION', ' (Regiones)')
        df.to_excel(writer, sheet_name=sheet_name, index=False, columns=cols_sheet)

print(f"Excel Estadistico generado: {OUTPUT_EXCEL}")

# ---------------------------------------------------
# RADAR (Adaptado a Z-scores de 4 BIOs)
# ---------------------------------------------------

def radar_comparativo_por_pais(df_pais, categorias_zscore, variables_bio, pais_nombre):

    if df_pais.empty or len(df_pais) < 3: 
        print(f"Datos insuficientes para el Radar de {pais_nombre}. (Se requieren 3 zonas).")
        return
        
    df_ordenado = df_pais.sort_values('Indice_consolidado', ascending=False)
    top_maiores = df_ordenado.head(3)
    top_menores = df_ordenado.tail(3).sort_values('Indice_consolidado', ascending=True)

    fig = plt.figure(figsize=(14, 6))
    cores_menores = ['#4daf4a', '#377eb8', '#e41a1c']  
    cores_maiores = ['#984ea3', '#ff7f00', '#a65628']  

    angles = [n / float(len(categorias_zscore)) * 2 * pi for n in range(len(categorias_zscore))]
    angles += angles[:1]
    
    max_val = df_pais[categorias_zscore].abs().max().max() * 1.2
    
    # Plot para mayor riesgo (Top 3)
    ax1 = fig.add_subplot(121, polar=True)
    ax1.set_title(f'Top 3 de Mayor Riesgo - {pais_nombre}', size=14, fontweight='bold', pad=20)
    
    for i, (idx, row) in enumerate(top_maiores.iterrows()):
        values = row[categorias_zscore].values.tolist()
        values += values[:1]
        ax1.plot(angles, values, linewidth=2, linestyle='solid', label=row['NOMBRE_ZONA'], color=cores_maiores[i])
        ax1.fill(angles, values, alpha=0.15, color=cores_maiores[i])

    ax1.set_theta_offset(pi / 2)
    ax1.set_theta_direction(-1)
    ax1.set_xticks(angles[:-1])
    ax1.set_xticklabels(variables_bio)
    ax1.set_rlim(-max_val, max_val)
    ax1.legend(loc='lower right', bbox_to_anchor=(1.3, -.1), title="Zonas")

    # Plot para menor riesgo (Top 3)
    ax2 = fig.add_subplot(122, polar=True)
    ax2.set_title(f'Top 3 de Menor Riesgo - {pais_nombre}', size=14, fontweight='bold', pad=20)
    
    for i, (idx, row) in enumerate(top_menores.iterrows()):
        values = row[categorias_zscore].values.tolist()
        values += values[:1]
        ax2.plot(angles, values, linewidth=2, linestyle='solid', label=row['NOMBRE_ZONA'], color=cores_menores[i])
        ax2.fill(angles, values, alpha=0.15, color=cores_menores[i])

    ax2.set_theta_offset(pi / 2)
    ax2.set_theta_direction(-1)
    ax2.set_xticks(angles[:-1])
    ax2.set_xticklabels(variables_bio)
    ax2.set_rlim(-max_val, max_val)
    ax2.legend(loc='lower right', bbox_to_anchor=(1.3, -.1), title="Zonas")

    plt.tight_layout()
    plt.show()


# Radar por Pais 

df_radar = pd.concat([
    all_country_reports["PARAGUAY_DEPTO"], # departamentos de paraguay
    all_country_reports["URUGUAY_DEPTO"], # departamentos de uruguay
    all_country_reports["BRASIL_ESTADO"], # estados de Brasil
    all_country_reports["ARGENTINA_PROV"] # Provincias de Argentina
], ignore_index=True)


paises_base = ["PARAGUAY", "URUGUAY", "BRASIL", "ARGENTINA"]
variables_bio_names = ['BIO1', 'BIO5', 'BIO14', 'BIO15']
categorias_zscore = ['z_bio1', 'z_bio5', 'z_bio14', 'z_bio15']

print("\nGenerando radar de Z-score por pais...")
for pais in paises_base:
    df_pais = df_radar[df_radar['PAIS_KEY'] == pais].copy()
    
    if not df_pais.empty:
        nivel = df_pais['NIVEL_ADM'].iloc[0]
        nombre_completo = f"{pais} ({nivel})"
        
        radar_comparativo_por_pais(df_pais, categorias_zscore, variables_bio_names, nombre_completo)

print("Proceso COMPLETADO")