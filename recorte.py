import rasterio
import fiona
from rasterio.mask import mask
import os

# archivos a recortar
RASTER_DIR = "./RASTER/"
# Ruta del archivo SHP que vamos a usar de mascara para el recorte
SHAPE_PATH = "./VECTOR/Area_Estudio/Area_Estudio.shp"
# Lista de rasters a procesar
RASTERS_A_RECORTAR = [
    "wc2.1_30s_bio_1.tif",
    "wc2.1_30s_bio_5.tif",
    "wc2.1_30s_bio_14.tif",
    "wc2.1_30s_bio_15.tif",
]
# Carpeta resultados recortados
OUTPUT_DIR = "./RASTER_Re/"

if not os.path.exists(SHAPE_PATH):
    print(f"Dato de mascara no encontrado: {SHAPE_PATH}")

# Crear directorio de salida si no existe
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Abre el archivo SHP con fiona y extraer las geometrías
# Se usa with para asegurar que el archivo se cierre correctamente despues de la lectura
with fiona.open(SHAPE_PATH, "r") as shapefile:
    # La funcion mask de rasterio necesita una lista de geometrias, entocnes recolectamos todas las geometrias de nuestro shp aca
    geometries = [feature["geometry"] for feature in shapefile]

if not geometries:
    print("ERROR: El archivo SHP no contiene geometrias")
else:
    print(f"SHP cargado exitosamente: {len(geometries)} geometrias")


for raster_name in RASTERS_A_RECORTAR:
    input_path = os.path.join(RASTER_DIR, raster_name)
    if not os.path.exists(input_path):
        print("ERROR: raster no encontrado")
        continue  # sigue para el proximo
    output_path = os.path.join(OUTPUT_DIR, f"recorte_{raster_name}")

    try:
        print(f"Procesando: {raster_name}...")
        # el archivo raster de entrada
        with rasterio.open(input_path) as src:
            # La funcion mask recorta el raster usando las geometrias obtenidas arriba
            out_image, out_transform = mask(src, geometries, crop=True)

            # Actualizar los metadatos para el nuevo raster
            out_meta = src.meta.copy()
            out_meta.update(
                {
                    "driver": "GTiff",
                    "height": out_image.shape[1],
                    "width": out_image.shape[2],
                    "transform": out_transform,
                    "nodata": src.nodata,
                }
            )

            # guardar raster recortado en la carpeta de salida
            with rasterio.open(output_path, "w", **out_meta) as dest:
                dest.write(out_image)

        print(f"Recorte exitoso! Guardado en: {output_path}")

    except rasterio.RasterioIOError:
        print(f" ERROR: No se pudo abrir o encontrar el archivo raster: {input_path}")
    except Exception as e:
        print(f" ERROR al procesar {raster_name}: {e}")


print("Proceso de recorte Completo")
