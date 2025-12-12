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
OUTPUT_DIR = os.path.join(BASE_DIR, "DERIVADOS/")
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
    1: 'BIO1', # temperatura media anual
    5: 'BIO5', # temp max del mes mas calido
    14: 'BIO14', # precipitacion del mes mas seco
    15: 'BIO15' # estacionalidad de la precipitacion
}
FILE_PREFIX_HIST = "bio{}_his.tif"
FILE_PREFIX_FUT = "bio{}_fut.tif"

os.makedirs(OUTPUT_DIR, exist_ok=True)
print(f"Salida: {OUTPUT_DIR}")



## -----------------------------------------------------------
## PARTE A: CALCULO DE DELTAS Y NORMALIZACION Z-SCORE
## -----------------------------------------------------------

print("Calculando Deltas y Normalizando con Z-Score...")
delta_rasters_paths = {}
all_delta_values = []

# Calculo de Deltas y Z-Scores (se generan los Z-scores puros)
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

try:
    all_values_combined = np.concatenate(all_delta_values)
    mean_global = np.mean(all_values_combined)
    std_global = np.std(all_values_combined)
except ValueError:
    print("ERROR FATAL: No hay valores deltas validos ")
    exit()

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

# indice de Impacto Agregado (CON INVERSION DE Z_BIO14, ya que asi mayor valor = mayor riesgo)
print("\nCalculando indice de impacto agregado...")
z_rasters = [rasterio.open(p) for p in normalized_rasters_paths.values()]
first_meta = z_rasters[0].meta.copy()

# Cargar los arrays en el orden BIO1, BIO5, BIO14, BIO15
z_score_arrays = [r.read(1) for r in z_rasters]

# Aplicar la inversion del signo a BIO14 (índice 2)
# Fórmula: Z_BIO1 + Z_BIO5 + (-Z_BIO14) + Z_BIO15
impacto_agregado_data = z_score_arrays[0] + z_score_arrays[1] + (-z_score_arrays[2]) + z_score_arrays[3]

valid_sum_mask = np.full(z_score_arrays[0].shape, True)
for arr in z_score_arrays:
    valid_sum_mask &= (arr != NODATA_VAL_OUT)

impacto_agregado_data[~valid_sum_mask] = NODATA_VAL_OUT
impacto_agregado_path = os.path.join(OUTPUT_DIR, "INDICE_IMPACTO_AGREGADO.tif")
first_meta.update({"dtype": 'float32', "nodata": NODATA_VAL_OUT})
with rasterio.open(impacto_agregado_path, "w", **first_meta) as dest:
    dest.write(impacto_agregado_data, 1)
print(f"Indice Impacto Agregado creado correctamente: {impacto_agregado_path}")


## -----------------------------------------------------------
## PARTE B: ANALISIS ZONAL Y CALCULO DE INDICES COMPUESTOS
## -----------------------------------------------------------

print("Realizando analisis zonal y calculo de indices...")

rasters_to_analyze = {
    'delta_BIO1': normalized_rasters_paths['BIO1'], 'delta_BIO5': normalized_rasters_paths['BIO5'],
    'delta_BIO14': normalized_rasters_paths['BIO14'], 'delta_BIO15': normalized_rasters_paths['BIO15'],
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
    
    # Añadir el raster de impacto agregado (z14 invertido) al analisis zonal
    rasters_to_analyze_temp = rasters_to_analyze.copy()
    rasters_to_analyze_temp['Indice_consolidado'] = impacto_agregado_path
    
    for index, row in area_estudio.iterrows():
        zone_stats = {}
        for stat_name, raster_path in rasters_to_analyze_temp.items():
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

    # CALCULO DE INDICES COMPUESTOS (Basado en la media zonal de los Z-scores puros)
    # I_estress_termico: Z_BIO1 + Z_BIO5
    df_zonal['I_estress_termico'] = df_zonal['delta_BIO1_mean'] + df_zonal['delta_BIO5_mean']
    
    # I_estress_hidrico: Z_BIO15 + (-Z_BIO14) <-- Aplicando la logica invertida de riesgo
    df_zonal['I_estress_hidrico'] = df_zonal['delta_BIO15_mean'] + (-df_zonal['delta_BIO14_mean'])
    
    # Indice_consolidado: Usamos la media 
    df_zonal.rename(columns={
        'Indice_consolidado_mean': 'Indice_consolidado', 
        'delta_BIO1_mean': 'z_bio1', 'delta_BIO5_mean': 'z_bio5',
        'delta_BIO14_mean': 'z_bio14', 'delta_BIO15_mean': 'z_bio15'
    }, inplace=True)
    
    # Renombrar Min/Max para claridad
    df_zonal.rename(columns={
        'delta_BIO1_min': 'z_bio1_min', 'delta_BIO5_min': 'z_bio5_min',
        'delta_BIO14_min': 'z_bio14_min', 'delta_BIO15_min': 'z_bio15_min',
        'delta_BIO1_max': 'z_bio1_max', 'delta_BIO5_max': 'z_bio5_max',
        'delta_BIO14_max': 'z_bio14_max', 'delta_BIO15_max': 'z_bio15_max'
    }, inplace=True)
    
    df_ordenado = df_zonal.dropna(subset=['Indice_consolidado'])
    all_country_reports[country_key] = df_ordenado


## -----------------------------------------------------------
## PARTE C: GENERAR EXCEL Y GRAFICOS RADAR
## -----------------------------------------------------------

print("Generando Excel y Grafico de Radar...")

# Generación del Excel MultiHojas
ranking_global_df = pd.concat(all_country_reports.values(), ignore_index=True)
ranking_global_df = ranking_global_df.sort_values(by='Indice_consolidado', ascending=False).dropna(subset=['Indice_consolidado'])
ranking_global_df['RANK'] = np.arange(1, len(ranking_global_df) + 1)

cols_output_excel = ['RANK', 'PAIS_KEY', 'NIVEL_ADM', 'NOMBRE_ZONA', 
               'I_estress_termico', 'I_estress_hidrico', 'Indice_consolidado',
               'z_bio1', 'z_bio5', 'z_bio14', 'z_bio15']

with pd.ExcelWriter(OUTPUT_EXCEL, engine='xlsxwriter') as writer:
    ranking_global_df[cols_output_excel].to_excel(writer, sheet_name='Ranking Global de Riesgo', index=False)
    
    for country_key, df in all_country_reports.items():
        df.sort_values(by='Indice_consolidado', ascending=False, inplace=True)
        cols_sheet = ['PAIS_KEY', 'NIVEL_ADM', 'NOMBRE_ZONA', 
                      'I_estress_termico', 'I_estress_hidrico', 'Indice_consolidado'] + \
                     [col for col in df.columns if ('z_bio' in col or '_min' in col or '_max' in col)]
        sheet_name = country_key.replace('_DEPTO', ' (Dptos)').replace('_ESTADO', ' (Ests)').replace('_PROV', ' (Prov)').replace('_REGION', ' (Regiones)')
        df.to_excel(writer, sheet_name=sheet_name, index=False, columns=cols_sheet)

print(f"Reporte Estadístico Multi-Hoja generado en: {OUTPUT_EXCEL}")

# ---------------------------------------------------
# GRAFICO DE RADAR 
# ---------------------------------------------------

def radar_comparativo_por_pais(df_pais, variables_bio, pais_nombre):
    if df_pais.empty or len(df_pais) < 3: 
        print(f"Datos insuficientes para el Radar de {pais_nombre}. (Se requieren 3 zonas).")
        return
        
    df_ordenado = df_pais.sort_values('Indice_consolidado', ascending=False)
    top_maiores = df_ordenado.head(3)
    top_menores = df_ordenado.tail(3).sort_values('Indice_consolidado', ascending=True)

    fig = plt.figure(figsize=(14, 6))
    cores_menores = ['#4daf4a', '#377eb8', '#e41a1c']  
    cores_maiores = ['#984ea3', '#ff7f00', '#a65628']  

    # Columnas de Z-score a extraer
    categorias_zscore = ['z_bio1', 'z_bio5', 'z_bio14', 'z_bio15'] 
    
    # Nombres en el grafico (indicando que BIO14 tiene riesgo en el lado negativo)
    nombres_eje = ['BIO1', 'BIO5', 'BIO14 (Sequía)', 'BIO15'] 

    angles = [n / float(len(categorias_zscore)) * 2 * pi for n in range(len(categorias_zscore))]
    angles += angles[:1]
    
    # Para el limite del radar, consideramos el maximo absoluto de todos los Z-scores
    max_val = df_pais[categorias_zscore].abs().max().max() * 1.2
    
    # Plot para mayor riesgo (Top 3)
    ax1 = fig.add_subplot(121, polar=True)
    ax1.set_title(f'Top 3 de Mayor Riesgo {pais_nombre}', size=14, fontweight='bold', pad=20)
    
    for i, (idx, row) in enumerate(top_maiores.iterrows()):
        values = row[categorias_zscore].values.tolist()
        values += values[:1]
        ax1.plot(angles, values, linewidth=2, linestyle='solid', label=row['NOMBRE_ZONA'], color=cores_maiores[i])
        ax1.fill(angles, values, alpha=0.15, color=cores_maiores[i])

    ax1.set_theta_offset(pi / 2)
    ax1.set_theta_direction(-1)
    ax1.set_xticks(angles[:-1])
    ax1.set_xticklabels(nombres_eje)
    ax1.set_rlim(-max_val, max_val)
    ax1.legend(loc='lower right', bbox_to_anchor=(1.3, -.1), title="Zonas")

    # Plot para menor riesgo (TOP 3)
    ax2 = fig.add_subplot(122, polar=True)
    ax2.set_title(f'Top 3 de Menor Riesgo {pais_nombre}', size=14, fontweight='bold', pad=20)
    
    for i, (idx, row) in enumerate(top_menores.iterrows()):
        values = row[categorias_zscore].values.tolist()
        values += values[:1]
        ax2.plot(angles, values, linewidth=2, linestyle='solid', label=row['NOMBRE_ZONA'], color=cores_menores[i])
        ax2.fill(angles, values, alpha=0.15, color=cores_menores[i])

    ax2.set_theta_offset(pi / 2)
    ax2.set_theta_direction(-1)
    ax2.set_xticks(angles[:-1])
    ax2.set_xticklabels(nombres_eje)
    ax2.set_rlim(-max_val, max_val)
    ax2.legend(loc='lower right', bbox_to_anchor=(1.3, -.1), title="Zonas")

    plt.tight_layout()
    plt.show()


# Radar por pais
df_radar = pd.concat([
    all_country_reports["PARAGUAY_DEPTO"],
    all_country_reports["URUGUAY_DEPTO"],
    all_country_reports["BRASIL_ESTADO"],
    all_country_reports["ARGENTINA_PROV"]
], ignore_index=True)


paises_base = ["PARAGUAY", "URUGUAY", "BRASIL", "ARGENTINA"]
variables_bio_names = ['BIO1', 'BIO5', 'BIO14', 'BIO15']

print("\nGenerando graficos de Radar...")
for pais in paises_base:
    df_pais = df_radar[df_radar['PAIS_KEY'] == pais].copy()
    
    if not df_pais.empty:
        nivel = df_pais['NIVEL_ADM'].iloc[0]
        nombre_completo = f"{pais} ({nivel})"
        
        radar_comparativo_por_pais(df_pais, variables_bio_names, nombre_completo)

print("Proceso COMPLETADO")