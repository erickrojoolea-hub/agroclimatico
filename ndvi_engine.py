"""
Motor NDVI vía Google Earth Engine.
Calcula series temporales de NDVI usando Sentinel-2 para cualquier punto lat/lon en Chile.
"""
import ee
import pandas as pd
import numpy as np
import streamlit as st
from datetime import datetime


GEE_PROJECT = "nvdi-493122"
S2_COLLECTION = "COPERNICUS/S2_SR_HARMONIZED"
CLOUD_THRESHOLD = 30  # porcentaje máximo de nubosidad


# ── Inicialización ────────────────────────────────────────────────────────────

def init_gee():
    """Inicializa Google Earth Engine con el proyecto configurado.
    Si ya está inicializado, no hace nada."""
    try:
        ee.Initialize(project=GEE_PROJECT)
    except ee.EEException:
        # Ya inicializado
        pass
    except Exception:
        ee.Authenticate()
        ee.Initialize(project=GEE_PROJECT)


# ── Funciones auxiliares ──────────────────────────────────────────────────────

def _build_aoi(lat: float, lon: float, radius_m: int = 500) -> ee.Geometry:
    """Crea un buffer circular alrededor del punto."""
    point = ee.Geometry.Point([lon, lat])
    return point.buffer(radius_m)


def _add_ndvi(image: ee.Image) -> ee.Image:
    """Agrega banda NDVI = (B8 - B4) / (B8 + B4) a la imagen."""
    ndvi = image.normalizedDifference(["B8", "B4"]).rename("NDVI")
    return image.addBands(ndvi)


def _mask_clouds(image: ee.Image) -> ee.Image:
    """Enmascara píxeles nubosos usando la banda SCL de Sentinel-2."""
    scl = image.select("SCL")
    # SCL valores: 3=sombra de nube, 8=nube media, 9=nube alta, 10=cirrus
    cloud_mask = (
        scl.neq(3)
        .And(scl.neq(8))
        .And(scl.neq(9))
        .And(scl.neq(10))
    )
    return image.updateMask(cloud_mask)


def _get_s2_collection(aoi: ee.Geometry, start_date: str, end_date: str) -> ee.ImageCollection:
    """Obtiene colección Sentinel-2 filtrada por área, fecha y nubosidad."""
    return (
        ee.ImageCollection(S2_COLLECTION)
        .filterBounds(aoi)
        .filterDate(start_date, end_date)
        .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", CLOUD_THRESHOLD))
        .map(_mask_clouds)
        .map(_add_ndvi)
    )


# ── Funciones principales ────────────────────────────────────────────────────

@st.cache_data(ttl=3600, show_spinner="Consultando Google Earth Engine...")
def get_ndvi_timeseries(
    lat: float,
    lon: float,
    radius_m: int = 500,
    start_year: int = 2017,
    end_year: int = 2025,
) -> pd.DataFrame:
    """
    Calcula serie temporal mensual de NDVI para un punto dado.

    Returns:
        DataFrame con columnas: date, ndvi_mean, ndvi_min, ndvi_max
    """
    init_gee()
    aoi = _build_aoi(lat, lon, radius_m)

    start_date = f"{start_year}-01-01"
    end_date = f"{end_year}-12-31"

    collection = _get_s2_collection(aoi, start_date, end_date)

    # Generar lista de meses
    months = []
    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            months.append((year, month))

    records = []
    # Procesar en lotes anuales para evitar timeouts de GEE
    for year in range(start_year, end_year + 1):
        y_start = f"{year}-01-01"
        y_end = f"{year}-12-31"
        year_col = collection.filterDate(y_start, y_end)

        for month in range(1, 13):
            m_start = f"{year}-{month:02d}-01"
            if month == 12:
                m_end = f"{year + 1}-01-01"
            else:
                m_end = f"{year}-{month + 1:02d}-01"

            monthly = year_col.filterDate(m_start, m_end)
            composite = monthly.select("NDVI").median()

            try:
                stats = composite.reduceRegion(
                    reducer=ee.Reducer.mean()
                        .combine(ee.Reducer.min(), sharedInputs=True)
                        .combine(ee.Reducer.max(), sharedInputs=True),
                    geometry=aoi,
                    scale=10,
                    maxPixels=1e8,
                ).getInfo()

                ndvi_mean = stats.get("NDVI_mean")
                ndvi_min = stats.get("NDVI_min")
                ndvi_max = stats.get("NDVI_max")

                records.append({
                    "date": f"{year}-{month:02d}-01",
                    "ndvi_mean": round(ndvi_mean, 4) if ndvi_mean is not None else None,
                    "ndvi_min": round(ndvi_min, 4) if ndvi_min is not None else None,
                    "ndvi_max": round(ndvi_max, 4) if ndvi_max is not None else None,
                })
            except ee.EEException as e:
                # Timeout u otro error de GEE — registrar valor nulo
                records.append({
                    "date": f"{year}-{month:02d}-01",
                    "ndvi_mean": None,
                    "ndvi_min": None,
                    "ndvi_max": None,
                })
            except Exception:
                records.append({
                    "date": f"{year}-{month:02d}-01",
                    "ndvi_mean": None,
                    "ndvi_min": None,
                    "ndvi_max": None,
                })

    df = pd.DataFrame(records)
    df["date"] = pd.to_datetime(df["date"])
    return df


@st.cache_data(ttl=3600, show_spinner="Calculando resumen anual NDVI...")
def get_ndvi_annual_summary(
    lat: float,
    lon: float,
    radius_m: int = 500,
    start_year: int = 2017,
    end_year: int = 2025,
) -> pd.DataFrame:
    """
    Resumen anual de NDVI: peak verano (enero), invierno (julio),
    promedio anual y clasificación de vegetación.

    Returns:
        DataFrame con columnas: year, ndvi_jan, ndvi_jul, ndvi_annual_mean, vegetation_status
    """
    ts = get_ndvi_timeseries(lat, lon, radius_m, start_year, end_year)

    ts["year"] = ts["date"].dt.year
    ts["month"] = ts["date"].dt.month

    records = []
    for year in range(start_year, end_year + 1):
        year_data = ts[ts["year"] == year]
        jan_data = year_data[year_data["month"] == 1]
        jul_data = year_data[year_data["month"] == 7]

        ndvi_jan = jan_data["ndvi_mean"].values[0] if len(jan_data) > 0 else None
        ndvi_jul = jul_data["ndvi_mean"].values[0] if len(jul_data) > 0 else None

        valid = year_data["ndvi_mean"].dropna()
        ndvi_annual_mean = round(valid.mean(), 4) if len(valid) > 0 else None

        records.append({
            "year": year,
            "ndvi_jan": ndvi_jan,
            "ndvi_jul": ndvi_jul,
            "ndvi_annual_mean": ndvi_annual_mean,
            "vegetation_status": _classify_vegetation(ndvi_annual_mean),
        })

    return pd.DataFrame(records)


def get_ndvi_map_url(
    lat: float,
    lon: float,
    zoom: int = 15,
    year: int = 2023,
) -> str:
    """
    Genera URL de tile layer para visualizar NDVI como overlay coloreado.

    Returns:
        URL template con {x}, {y}, {z} para usar en folium u otro mapa.
    """
    init_gee()
    aoi = _build_aoi(lat, lon, radius_m=5000)

    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"

    collection = _get_s2_collection(aoi, start_date, end_date)
    composite = collection.select("NDVI").median().clip(aoi)

    vis_params = {
        "min": 0.0,
        "max": 0.8,
        "palette": [
            "#d73027",  # rojo — suelo desnudo
            "#fc8d59",  # naranja
            "#fee08b",  # amarillo
            "#d9ef8b",  # verde claro
            "#91cf60",  # verde medio
            "#1a9850",  # verde oscuro — vegetación densa
        ],
    }

    try:
        map_id = composite.getMapId(vis_params)
        return map_id["tile_fetcher"].url_format
    except ee.EEException:
        return ""
    except Exception:
        return ""


# ── Clasificadores ────────────────────────────────────────────────────────────

def classify_land_use(ndvi_value: float) -> str:
    """Clasificación simple de uso de suelo a partir de NDVI."""
    if ndvi_value is None:
        return "Sin datos"
    if ndvi_value < 0.1:
        return "Suelo desnudo / urbano"
    elif ndvi_value < 0.2:
        return "Suelo con escasa vegetación"
    elif ndvi_value < 0.35:
        return "Pradera / pastizal"
    elif ndvi_value < 0.5:
        return "Cultivo anual / viña joven"
    elif ndvi_value < 0.7:
        return "Frutal establecido / viña adulta"
    else:
        return "Bosque / vegetación densa"


def _classify_vegetation(ndvi_value: float | None) -> str:
    """Clasificación del estado de vegetación según NDVI promedio anual."""
    if ndvi_value is None:
        return "Sin datos"
    if ndvi_value < 0.2:
        return "Sin vegetación"
    elif ndvi_value < 0.35:
        return "Vegetación escasa"
    elif ndvi_value < 0.5:
        return "Vegetación moderada"
    elif ndvi_value < 0.7:
        return "Vegetación densa"
    else:
        return "Vegetación muy densa"
