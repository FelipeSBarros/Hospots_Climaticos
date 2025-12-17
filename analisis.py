import rasterio
import numpy as np
import geopandas as gpd
from pathlib import Path
from rasterstats import zonal_stats
from rasterio.features import rasterize

# ===================================================
# CONFIGURACIÓN GENERAL
# ===================================================

RASTER_PATH = Path("./RASTER/modificados/")
VECTOR_PATH = Path("./VECTOR/Area_Estudio/Area_Estudio.shp")
OUTPUT_PATH = Path("./RASTER/derivados/")
NODATA_VAL_OUT = -9999

OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------
# VECTORES
# ---------------------------------------------------

regiones = gpd.read_file(VECTOR_PATH)

# ---------------------------------------------------
# BIOS
# ---------------------------------------------------

BIOS = {
    1: {
        "name": "BIO1",
        "hist_path": RASTER_PATH / "recorte_wc2.1_30s_bio_1.tif",
        "fut_path": RASTER_PATH / "recorte_bio_1_fut.tif",
    },
    5: {
        "name": "BIO5",
        "hist_path": RASTER_PATH / "recorte_wc2.1_30s_bio_5.tif",
        "fut_path": RASTER_PATH / "recorte_bio_5_fut.tif",
    },
    14: {
        "name": "BIO14",
        "hist_path": RASTER_PATH / "recorte_wc2.1_30s_bio_14.tif",
        "fut_path": RASTER_PATH / "recorte_bio_14_fut.tif",
    },
    15: {
        "name": "BIO15",
        "hist_path": RASTER_PATH / "recorte_wc2.1_30s_bio_15.tif",
        "fut_path": RASTER_PATH / "recorte_bio_15_fut.tif",
    },
}

# ===================================================
# A.1 DELTAS BIOCLIMÁTICOS (STREAMING + BIO14 INVERTIDO)
# ===================================================

for bio_idx, cfg in BIOS.items():
    # bio_idx, cfg = list(BIOS.items())[1]
    delta_path = OUTPUT_PATH.joinpath(f"DELTA_bio_{bio_idx}.tif")

    with rasterio.open(cfg["hist_path"]) as src_h, rasterio.open(
        cfg["fut_path"]
    ) as src_f:
        assert src_h.crs == src_f.crs
        assert src_h.transform == src_f.transform
        assert src_h.shape == src_f.shape

        profile = src_h.profile.copy()
        profile.update(dtype="float32", nodata=NODATA_VAL_OUT)

        with rasterio.open(delta_path, "w", **profile) as dst:
            for _, window in src_h.block_windows(1):
                h = src_h.read(1, window=window).astype("float32")
                f = src_f.read(1, window=window).astype("float32")

                delta = f - h

                mask = (h == src_h.nodata) | (f == src_f.nodata) | np.isnan(delta)

                delta[mask] = NODATA_VAL_OUT

                # Inversión temprana BIO14
                if bio_idx == 14:
                    delta = -delta

                dst.write(delta, 1, window=window)

    BIOS.get(bio_idx).update({"delta_path": delta_path})

    print(f"Delta bio_{bio_idx} generado")
    # ===================================================
    # A.2 MEDIA Y DESVIACIÓN REGIONAL (VECTORIAL)
    # ===================================================

    stats = zonal_stats(
        regiones,
        delta_path,
        stats=["mean", "std"],
        nodata=NODATA_VAL_OUT,
        all_touched=True,
    )

    regiones[f"mean_bio_{bio_idx}"] = [s["mean"] for s in stats]
    regiones[f"std_bio_{bio_idx}"] = [s["std"] for s in stats]

    if (regiones[f"std_bio_{bio_idx}"] == 0).any():
        regiones.loc[
            regiones[f"std_bio_{bio_idx}"] == 0, [f"std_bio_{bio_idx}"]
        ] = -9999
        # raise ValueError(f"STD = 0 detectado en BIO{bio_idx}")
        print(f"STD = 0 detectado en BIO{bio_idx}")

    print(f"Estadísticas regionales DELTA_bio_{bio_idx} calculadas")

    # ===================================================
    # A.3 RASTERIZACIÓN DE MEDIA Y STD
    # ===================================================

    with rasterio.open(BIOS[1]["hist_path"]) as ref:
        meta = ref.meta.copy()
        meta.update(dtype="float32", nodata=NODATA_VAL_OUT)

        for stat in ["mean", "std"]:
            # stat = 'mean'
            out_path = OUTPUT_PATH.joinpath(f"{stat.upper()}_bio_{bio_idx}.tif")

            shapes = (
                (geom, val)
                for geom, val in zip(
                    regiones.geometry, regiones[f"{stat}_bio_{bio_idx}"]
                )
            )

            raster = rasterize(
                shapes,
                out_shape=(ref.height, ref.width),
                transform=ref.transform,
                fill=NODATA_VAL_OUT,
                dtype="float32",
            )

            with rasterio.open(out_path, "w", **meta) as dst:
                dst.write(raster, 1)

            print(f"Raster {stat.upper()}_bio_{bio_idx} creado")
            BIOS.get(bio_idx).update({f"{stat}_path": out_path})
        # ===================================================
        # A.4 NORMALIZACIÓN Z-SCORE REGIONAL (STREAMING)
        # ===================================================

        mean_path = BIOS.get(bio_idx).get("mean_path")
        std_path = BIOS.get(bio_idx).get("std_path")
        z_path = OUTPUT_PATH.joinpath(f"Z_bio_{bio_idx}.tif")

    with rasterio.open(delta_path) as src_d, rasterio.open(
        mean_path
    ) as src_m, rasterio.open(std_path) as src_s:
        profile = src_d.profile.copy()
        profile.update(dtype="float32", nodata=NODATA_VAL_OUT)

        with rasterio.open(z_path, "w", **profile) as dst:
            for _, window in src_d.block_windows(1):
                d = src_d.read(1, window=window)
                m = src_m.read(1, window=window)
                s = src_s.read(1, window=window)

                z = np.full(d.shape, NODATA_VAL_OUT, dtype="float32")

                valid = s != NODATA_VAL_OUT  # & (s != 0) & (~np.isnan(s))

                z[valid] = (d[valid] - m[valid]) / s[valid]

                dst.write(z, 1, window=window)

    BIOS.get(bio_idx).update({"z_path": z_path})

    print(f"Z-score bio_{bio_idx} generado")

print("\nDelta, Normalización y Z-score creados")
