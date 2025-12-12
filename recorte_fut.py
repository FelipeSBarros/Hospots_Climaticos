import rasterio
import fiona
from rasterio.mask import mask
import os
import numpy as np

# ruta carpeta raster multibanda
RASTER_DIR = "/home/victor/Documentos/Proyección_Hotspots/RASTER/"
# SHP de mascara 
SHAPE_PATH = "/home/victor/Documentos/Proyección_Hotspots/VECTOR/Area_Estudio/Area_Estudio.shp"
# raster multibanda
RASTER_FUTURO_NOMBRE = "wc2.1_30s_bioc_IPSL-CM6A-LR_ssp585_2041-2060.tif"

# Las bandas que necesitamos 1, 5, 14, 15
# rasterio usa indexacion basada en 1, entonces para el primer elemento va 1, no 0.
BANDAS_DESEADAS = [1, 5, 14, 15]

# resultados recortados
OUTPUT_DIR = "/home/victor/Documentos/Proyección_Hotspots/RASTER_Re/"
input_path = os.path.join(RASTER_DIR, RASTER_FUTURO_NOMBRE)


print(f"buscando SHP en: {SHAPE_PATH}")
# Crear el directorio de salida si no existe
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Carga las geometrias del SHP usando fiona
try:
    with fiona.open(SHAPE_PATH, "r") as shapefile:
        geometries = [feature["geometry"] for feature in shapefile]
    if not geometries:
        raise ValueError("El archivo SHP no contiene geometrias validas")
    print(f"Mascara cargada exitosamente: {len(geometries)} geometrias")
except Exception as e:
    print(f"ERROR: No se pudo cargar el SHP {e}")
    exit()

try:
    print(f"Iniciando procesamiento: {RASTER_FUTURO_NOMBRE}...")
    
    with rasterio.open(input_path) as src:
        
        # Iteramos sobre cada banda 
        for band_index in BANDAS_DESEADAS:
            
            print(f" Recortando Banda {band_index}...")
            
            # Recorte: Usamos mask, pero le digo que solo cargue y recorte una banda a la vez con el index.
            out_image_band, out_transform = mask(
                src, 
                geometries, 
                crop=True, 
                nodata=src.nodata,
                #bandas a recortar (de a una sola)
                indexes=[band_index] 
            )

            # Actualizar los metadatos para el nuevo raster
            out_meta = src.meta.copy()
            out_meta.update({
                "driver": "GTiff",
                "height": out_image_band.shape[1],
                "width": out_image_band.shape[2],
                "transform": out_transform,
                "count": 1, # Solo una banda
                "dtype": out_image_band.dtype
            })

            #Guardar archivo recortado
            output_filename = f"CLIP_BIO_{band_index}_FUTURO.tif"
            output_path = os.path.join(OUTPUT_DIR, output_filename)

            with rasterio.open(output_path, "w", **out_meta) as dest:
                # array 2D recortado (índice 0 de out_image_band)
                dest.write(out_image_band[0], 1) # esto me asegura obtener del multibanda unicamente una banda con su vector de datos, ya que rasterio envuelve en 3D
            
            print(f"archivo guardado: {output_filename}")

except rasterio.RasterioIOError:
    print(f"ERROR: No se pudo abrir o encontrar el raster: {input_path}")
except Exception as e:
    print(f"ERROR inesperado: {e}")

print("Proceso de extraccion y recorte completado")