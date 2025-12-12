from osgeo import gdal

raster_ref = "/home/victor/Documentos/Proyección_Hotspots/RASTER/bio1_his.tif"
entrada_raster = "/home/victor/Documentos/Proyección_Hotspots/RASTER/bio15_fut.tif"
salida_raster = "/home/victor/Documentos/Proyección_Hotspots/RASTER/bio15_futali.tif"

#defino variables GLOBALES
ref = gdal.Open(raster_ref)
gt = ref.GetGeoTransform()
proj = ref.GetProjection()

# Tamaño de pixel
px_x = gt[1]
px_y = abs(gt[5])

# Extent exacto exacto 
xmin = gt[0]
ymax = gt[3]
xmax = xmin + ref.RasterXSize * px_x
ymin = ymax - ref.RasterYSize * px_y

#parametros de alineacion, elegimos metodo bilinear porque son datos continuos, esto es lo optimo para el tipo de datos que estamos trabajando
parametros = gdal.WarpOptions(
    format="GTiff", #Formato
    xRes=px_x, #resolucion X
    yRes=px_y, #resolucion Y
    outputBounds=(xmin, ymin, xmax, ymax), #extension geografica
    dstSRS=proj, #proyeccion capa
    resampleAlg="bilinear", # metodo de remuestreo, arriba explicamos el uso
    multithread=True, # para acelerar el proyecto, habilita un "proceso en paralelo", sinceramente no se si es util o no. pero acelera considerablemente el proceso
    dstNodata=-9999, # valor para NoData, pixeles sin dato
    creationOptions=["COMPRESS=LZW", "TILED=YES", "BIGTIFF=YES"], 
    #LZW: reduce el tamaño del archivo sin sacrificar ni un solo bit de la precision del raster, 
    #TILED-YES: mejora el rendimiento de lectura y visualizacion en software GIS,
    #bigtiff: medida de seguridad por si el archivo supera los 4gb, lo use al principio para recortar la capa de proyeccion 2041-2060.
    outputType=gdal.GDT_Float32 # tipo de dato de los pixeles de salida.
)

print("Reproyectando y alineando...")
gdal.Warp(salida_raster, entrada_raster, options=parametros)
print("Listo. Raster alineado y guardado en: ")
print(salida_raster)
