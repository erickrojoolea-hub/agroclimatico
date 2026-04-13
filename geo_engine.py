"""
geo_engine.py -- Motor de consultas geoespaciales punto-en-poligono.

Usa GeoJSON files de DGA para determinar:
- Cuenca / Subcuenca exacta del punto
- Zona de restriccion hidrica
- Zona de agotamiento
- Productividad de pozos
- Reserva de caudales
- Embalses cercanos con nombre
- Estaciones cercanas (fluvi, meteo, calidad) con coords WGS84

No requiere dependencias externas (sin geopandas/shapely).
"""

import json
import math
import os
import sqlite3
import logging
from typing import Optional
from functools import lru_cache

logger = logging.getLogger(__name__)

_BASE = os.path.dirname(os.path.abspath(__file__))
_GEOJSON_DIR = os.path.join(_BASE, "data", "geojson")
_DB_PATH = os.path.join(_BASE, "data", "db", "geospatial.db")


# ---------------------------------------------------------------------------
# Point-in-Polygon (Ray Casting)
# ---------------------------------------------------------------------------

def _point_in_ring(px: float, py: float, ring: list) -> bool:
    """Ray-casting algorithm for a single polygon ring.
    ring = [[lon, lat], [lon, lat], ...]
    """
    n = len(ring)
    inside = False
    j = n - 1
    for i in range(n):
        xi, yi = ring[i][0], ring[i][1]
        xj, yj = ring[j][0], ring[j][1]
        if ((yi > py) != (yj > py)) and (px < (xj - xi) * (py - yi) / (yj - yi) + xi):
            inside = not inside
        j = i
    return inside


def point_in_polygon(lon: float, lat: float, geometry: dict) -> bool:
    """Check if a point (lon, lat) is inside a GeoJSON geometry (Polygon or MultiPolygon)."""
    geom_type = geometry.get("type", "")
    coords = geometry.get("coordinates", [])

    if geom_type == "Polygon":
        # coords = [outer_ring, hole1, hole2, ...]
        if not coords:
            return False
        if not _point_in_ring(lon, lat, coords[0]):
            return False
        # Check holes
        for hole in coords[1:]:
            if _point_in_ring(lon, lat, hole):
                return False
        return True

    elif geom_type == "MultiPolygon":
        # coords = [polygon1, polygon2, ...]
        for polygon in coords:
            if not polygon:
                continue
            if _point_in_ring(lon, lat, polygon[0]):
                # Check holes
                in_hole = False
                for hole in polygon[1:]:
                    if _point_in_ring(lon, lat, hole):
                        in_hole = True
                        break
                if not in_hole:
                    return True
        return False

    return False


# ---------------------------------------------------------------------------
# GeoJSON Loading with cache
# ---------------------------------------------------------------------------

_GEOJSON_CACHE = {}


def _fix_encoding(text: str) -> str:
    """Fix broken UTF-8 encoding (text read as latin-1 instead of utf-8)."""
    if not text:
        return text or ""
    try:
        return text.encode("latin-1").decode("utf-8")
    except (UnicodeDecodeError, UnicodeEncodeError):
        return text


def _load_geojson(name: str) -> Optional[dict]:
    """Load a GeoJSON file from data/geojson/. Cached after first load."""
    if name in _GEOJSON_CACHE:
        return _GEOJSON_CACHE[name]

    path = os.path.join(_GEOJSON_DIR, f"{name}.geojson")
    if not os.path.isfile(path):
        logger.warning(f"GeoJSON file not found: {path}")
        return None

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        _GEOJSON_CACHE[name] = data
        logger.info(f"Loaded GeoJSON: {name} ({len(data.get('features', []))} features)")
        return data
    except Exception as e:
        logger.error(f"Error loading GeoJSON {name}: {e}")
        return None


def find_containing_features(lat: float, lon: float, geojson_name: str,
                              max_results: int = 0) -> list[dict]:
    """Find all features in a GeoJSON layer that contain the given point.
    Returns list of feature properties dicts.
    """
    data = _load_geojson(geojson_name)
    if not data:
        return []

    results = []
    for feat in data.get("features", []):
        geom = feat.get("geometry")
        if not geom:
            continue

        geom_type = geom.get("type", "")
        if geom_type not in ("Polygon", "MultiPolygon"):
            continue

        if point_in_polygon(lon, lat, geom):
            results.append(feat.get("properties", {}))
            if max_results and len(results) >= max_results:
                break

    return results


def find_nearest_point_features(lat: float, lon: float, geojson_name: str,
                                 max_results: int = 5, max_km: float = 100.0) -> list[dict]:
    """Find nearest Point features in a GeoJSON layer.
    Returns list of (properties, distance_km) tuples.
    """
    data = _load_geojson(geojson_name)
    if not data:
        return []

    results = []
    for feat in data.get("features", []):
        geom = feat.get("geometry")
        if not geom or geom.get("type") != "Point":
            continue

        coords = geom.get("coordinates", [])
        if len(coords) < 2:
            continue

        flon, flat = coords[0], coords[1]
        d = _haversine(lat, lon, flat, flon)
        if d <= max_km:
            props = dict(feat.get("properties", {}))
            props["_distancia_km"] = round(d, 1)
            results.append(props)

    results.sort(key=lambda x: x["_distancia_km"])
    return results[:max_results] if max_results else results


# ---------------------------------------------------------------------------
# Haversine
# ---------------------------------------------------------------------------

def _haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Distance in km between two lat/lon points."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
         * math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


# ---------------------------------------------------------------------------
# High-Level Query Functions
# ---------------------------------------------------------------------------

def get_cuenca(lat: float, lon: float) -> Optional[dict]:
    """Get the exact cuenca for a point using point-in-polygon.
    Returns: {"nombre": str, "codigo": str} or None
    """
    results = find_containing_features(lat, lon, "cuencas", max_results=1)
    if results:
        r = results[0]
        return {
            "nombre": r.get("NOM_CUEN", ""),
            "codigo": r.get("COD_CUEN", ""),
            "region": r.get("REGION", ""),
        }
    return None


def get_subcuenca(lat: float, lon: float) -> Optional[dict]:
    """Get the exact subcuenca for a point using point-in-polygon."""
    results = find_containing_features(lat, lon, "subcuencas", max_results=1)
    if results:
        r = results[0]
        return {
            "nombre": r.get("NOM_SUBC", ""),
            "codigo": r.get("COD_SUBC", ""),
            "cuenca": r.get("COD_CUEN", ""),
        }
    return None


def get_subsubcuenca(lat: float, lon: float) -> Optional[dict]:
    """Get the exact sub-subcuenca for a point."""
    results = find_containing_features(lat, lon, "subsubcuencas", max_results=1)
    if results:
        r = results[0]
        return {
            "nombre": r.get("NOM_SSUBC", ""),
            "codigo": r.get("COD_SSUBC", ""),
        }
    return None


def get_restriccion_hidrica(lat: float, lon: float) -> Optional[dict]:
    """Check if point falls within a water restriction zone.
    Returns restriction info or None if not restricted.
    """
    results = find_containing_features(lat, lon, "restricciones", max_results=1)
    if results:
        r = results[0]
        return {
            "codigo": r.get("COD_SHAC", ""),
            "region": r.get("REGION", ""),
            "regiones": r.get("REGIONES", ""),
            "tipo": r.get("TIPO_LIMIT", ""),
            "resolucion": r.get("RES_DGA", ""),
            "fecha": r.get("F_RES_DGA", ""),
            "acuifero": r.get("NOM_ACUIF", ""),
            "nombre_bna": r.get("NOM_BNA", ""),
        }
    return None


def get_agotamiento(lat: float, lon: float) -> Optional[dict]:
    """Check if point falls within a water depletion zone.
    Returns depletion info or None.
    """
    results = find_containing_features(lat, lon, "agotamiento", max_results=1)
    if results:
        r = results[0]
        return {
            "nombre": r.get("NOM_AGOTA", ""),
            "codigo": r.get("COD_AGOTA", ""),
            "region": r.get("REGION", ""),
            "resolucion": r.get("RES_DGA", ""),
            "fecha": r.get("F_RES_DGA", ""),
            "area_km2": r.get("AREA_KM2", 0),
        }
    return None


def get_productividad_pozos(lat: float, lon: float) -> Optional[dict]:
    """Get well productivity zone for a point.
    Returns productivity info or None.
    """
    results = find_containing_features(lat, lon, "productividad_pozos", max_results=1)
    if results:
        r = results[0]
        return {
            "productividad": r.get("PRODUCTIV", ""),
            "tipo": r.get("TIPO_PROD", ""),
            "area_km2": r.get("AREA_KM2", 0),
        }
    return None


def get_reserva_caudales(lat: float, lon: float) -> Optional[dict]:
    """Check if point falls within a flow reserve zone."""
    results = find_containing_features(lat, lon, "reserva_caudales", max_results=1)
    if results:
        r = results[0]
        return {
            "nombre": r.get("NOM_RC", r.get("NOMBRE", "")),
            "region": r.get("REGION", ""),
        }
    return None


def get_pozos_cercanos(lat: float, lon: float, max_km: float = 30.0,
                       max_results: int = 5) -> list[dict]:
    """Find nearest wells with productivity data."""
    results = find_nearest_point_features(lat, lon, "datos_pozos",
                                          max_results=max_results, max_km=max_km)
    pozos = []
    for r in results:
        pozos.append({
            "tipo": r.get("TIPO_POZO", ""),
            "profundidad_m": r.get("PROF_M", 0),
            "nivel_estatico_m": r.get("PROF_NE_M", ""),
            "productividad_ls": r.get("PRODUCTIV", 0),
            "calidad_salinidad": r.get("CAL_SUB", 0),
            "distancia_km": r.get("_distancia_km", 0),
        })
    return pozos


def get_nearest_isoyeta(lat: float, lon: float, max_km: float = 50.0) -> Optional[dict]:
    """Find nearest isoyeta (precipitation contour) to a point.
    Since isoyetas are LineStrings, we find the nearest one and interpolate.
    """
    data = _load_geojson("isoyetas")
    if not data:
        return None

    best = None
    best_dist = float("inf")

    for feat in data.get("features", []):
        geom = feat.get("geometry")
        if not geom:
            continue

        coords = geom.get("coordinates", [])
        geom_type = geom.get("type", "")

        # Get min distance to any segment of the linestring
        if geom_type == "LineString":
            for c in coords:
                if len(c) >= 2:
                    d = _haversine(lat, lon, c[1], c[0])
                    if d < best_dist:
                        best_dist = d
                        best = feat.get("properties", {})
        elif geom_type == "MultiLineString":
            for line in coords:
                for c in line:
                    if len(c) >= 2:
                        d = _haversine(lat, lon, c[1], c[0])
                        if d < best_dist:
                            best_dist = d
                            best = feat.get("properties", {})

    if best and best_dist <= max_km:
        return {
            "precipitacion_mm": best.get("VALOR_MM", 0),
            "variable": best.get("VARIABLE", ""),
            "distancia_km": round(best_dist, 1),
        }
    return None


# ---------------------------------------------------------------------------
# SQLite-based queries (embalses, estaciones) with corrected WGS84 coords
# ---------------------------------------------------------------------------

def _connect_geo():
    """Connect to geospatial.db."""
    if not os.path.isfile(_DB_PATH):
        return None
    try:
        conn = sqlite3.connect(_DB_PATH)
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.Error:
        return None


def get_embalses_cercanos(lat: float, lon: float, max_km: float = 100.0,
                          top_n: int = 5) -> list[dict]:
    """Find nearest embalses with names and details."""
    conn = _connect_geo()
    if not conn:
        return []

    try:
        rows = conn.execute("SELECT * FROM embalses").fetchall()
        results = []
        for r in rows:
            rlat = r["lat"]
            rlon = r["lon"]
            if rlat is None or rlon is None:
                continue
            d = _haversine(lat, lon, rlat, rlon)
            if d <= max_km:
                keys = r.keys()
                results.append({
                    "nombre": _fix_encoding(r["nombre"] or "Sin nombre"),
                    "distancia_km": round(d, 1),
                    "region": _fix_encoding(r["region"] or ""),
                    "comuna": _fix_encoding((r["comuna"] if "comuna" in keys else "") or ""),
                    "tipo": _fix_encoding((r["tipo"] if "tipo" in keys else "") or ""),
                    "estado": _fix_encoding((r["estado"] if "estado" in keys else "") or ""),
                    "uso": _fix_encoding((r["uso"] if "uso" in keys else "") or ""),
                    "propietario": _fix_encoding((r["propietario"] if "propietario" in keys else "") or ""),
                })
        results.sort(key=lambda x: x["distancia_km"])
        return results[:top_n]
    finally:
        conn.close()


def get_estaciones_cercanas(lat: float, lon: float, tipo: str = "fluviometricas",
                             max_km: float = 100.0, top_n: int = 5) -> list[dict]:
    """Find nearest monitoring stations.
    tipo: 'fluviometricas', 'meteorologicas', 'calidad_agua'
    """
    table_map = {
        "fluviometricas": "estaciones_fluviometricas",
        "meteorologicas": "estaciones_meteorologicas",
        "calidad_agua": "calidad_agua",
    }
    table = table_map.get(tipo)
    if not table:
        return []

    conn = _connect_geo()
    if not conn:
        return []

    try:
        rows = conn.execute(f"SELECT * FROM {table}").fetchall()
        results = []
        for r in rows:
            rlat = r["lat"]
            rlon = r["lon"]
            if rlat is None or rlon is None:
                continue
            d = _haversine(lat, lon, rlat, rlon)
            if d <= max_km:
                keys = r.keys()
                item = {
                    "nombre": _fix_encoding(r["nombre"] or "Sin nombre"),
                    "distancia_km": round(d, 1),
                    "region": _fix_encoding((r["region"] if "region" in keys else "") or ""),
                }
                if "cuenca" in keys:
                    item["cuenca"] = _fix_encoding(r["cuenca"] or "")
                if "comuna" in keys:
                    item["comuna"] = _fix_encoding(r["comuna"] or "")
                if "estado" in keys:
                    item["estado"] = r["estado"] or ""
                if "ica" in keys:
                    item["ica"] = r["ica"] or ""
                results.append(item)
        results.sort(key=lambda x: x["distancia_km"])
        return results[:top_n]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Comprehensive site analysis
# ---------------------------------------------------------------------------

def analisis_sitio(lat: float, lon: float) -> dict:
    """Run all geospatial analyses for a single point.
    Returns a comprehensive dict with all layers.
    """
    result = {
        "lat": lat,
        "lon": lon,
        "cuenca": get_cuenca(lat, lon),
        "subcuenca": get_subcuenca(lat, lon),
        "subsubcuenca": get_subsubcuenca(lat, lon),
        "restriccion_hidrica": get_restriccion_hidrica(lat, lon),
        "agotamiento": get_agotamiento(lat, lon),
        "productividad_pozos": get_productividad_pozos(lat, lon),
        "reserva_caudales": get_reserva_caudales(lat, lon),
        "pozos_cercanos": get_pozos_cercanos(lat, lon),
        "isoyeta": get_nearest_isoyeta(lat, lon),
        "embalses_cercanos": get_embalses_cercanos(lat, lon, top_n=5),
        "estaciones_fluviometricas": get_estaciones_cercanas(lat, lon, "fluviometricas", top_n=5),
        "estaciones_meteorologicas": get_estaciones_cercanas(lat, lon, "meteorologicas", top_n=5),
        "calidad_agua": get_estaciones_cercanas(lat, lon, "calidad_agua", top_n=5),
    }

    # Derive risk assessment
    result["riesgo_hidrico"] = _evaluar_riesgo_hidrico(result)

    return result


def _evaluar_riesgo_hidrico(data: dict) -> dict:
    """Evaluate water risk based on geospatial data."""
    alertas = []
    semaforo = "verde"
    texto = "Sin restricciones hidricas identificadas en el punto."

    if data.get("agotamiento"):
        semaforo = "rojo"
        nombre = data["agotamiento"].get("nombre", "")
        texto = f"El punto se ubica en zona de agotamiento: {nombre}."
        alertas.append(f"Zona de agotamiento declarada: {nombre}")

    if data.get("restriccion_hidrica"):
        if semaforo != "rojo":
            semaforo = "rojo"
        info = data["restriccion_hidrica"]
        tipo = info.get("tipo", "Restriccion")
        acuifero = info.get("acuifero", "")
        txt = f"Zona de restriccion hidrica ({tipo})"
        if acuifero:
            txt += f" - Acuifero: {acuifero}"
        alertas.append(txt)
        if "agotamiento" not in texto.lower():
            texto = f"El punto se ubica en zona con restriccion hidrica ({tipo})."

    if data.get("reserva_caudales"):
        if semaforo == "verde":
            semaforo = "amarillo"
        alertas.append(f"Zona de reserva de caudales: {data['reserva_caudales'].get('nombre', '')}")

    prod = data.get("productividad_pozos")
    if prod:
        tipo_prod = prod.get("tipo", "").lower()
        if "baja" in tipo_prod or "nula" in tipo_prod:
            if semaforo == "verde":
                semaforo = "amarillo"
            alertas.append(f"Productividad de pozos: {prod.get('productividad', '')} ({prod.get('tipo', '')})")

    if not alertas:
        alertas.append("No se identifican restricciones hidricas en este punto.")

    return {
        "semaforo": semaforo,
        "texto": texto,
        "alertas": alertas,
    }
