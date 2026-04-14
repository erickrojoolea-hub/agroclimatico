"""
elevation_engine.py — Estimación de altitud para cualquier punto de Chile
Usa interpolación IDW (Inverse Distance Weighting) con 879 estaciones CR2
que tienen altitud registrada.

Precisión: ~±100m en valles centrales, menor en zonas de alta variación topográfica.
Para informes avanzados es suficiente; para cálculos críticos usar DEM real.
"""

import math
import os
import json
from typing import Optional

# ── Datos de estaciones ─────────────────────────────────────────────────────
_STATIONS: list = []
_LOADED = False

# CAMELS-CL como fuente adicional de altitud por cuencas
_BASE = os.path.dirname(__file__)
_CAMELS_LOCAL = os.path.join(_BASE, 'data', 'estaciones', 'catchment_attributes.csv')
_CAMELS_SYMLINK = os.path.join(_BASE, 'datos_geo', 'CR2', 'CAMELS_CL_v202201', 'catchment_attributes.csv')
_CAMELS_PATH = _CAMELS_LOCAL if os.path.exists(_CAMELS_LOCAL) else _CAMELS_SYMLINK


def _haversine(lat1, lon1, lat2, lon2):
    R = 6371.0
    rlat1, rlon1, rlat2, rlon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = rlat2 - rlat1
    dlon = rlon2 - rlon1
    a = math.sin(dlat / 2) ** 2 + math.cos(rlat1) * math.cos(rlat2) * math.sin(dlon / 2) ** 2
    return R * 2 * math.asin(math.sqrt(a))


def _load_stations():
    """Carga estaciones CR2 con altitud."""
    global _STATIONS, _LOADED
    if _LOADED:
        return

    try:
        import sys
        sys.path.insert(0, os.path.dirname(__file__))
        from lectores.lector_cr2_estaciones import cargar_estaciones
        est_dict = cargar_estaciones()
        for code, info in est_dict.items():
            if info.get('alt') and info['alt'] > 0 and info.get('lat') and info.get('lon'):
                _STATIONS.append({
                    'lat': info['lat'],
                    'lon': info['lon'],
                    'alt': info['alt'],
                    'nombre': info.get('nombre', ''),
                })
    except Exception as e:
        print(f"[elevation] Error cargando estaciones CR2: {e}")

    # También cargar CAMELS-CL si existe
    if os.path.exists(_CAMELS_PATH):
        try:
            import csv
            with open(_CAMELS_PATH, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    try:
                        lat = float(row.get('gauge_lat', 0))
                        lon = float(row.get('gauge_lon', 0))
                        alt = float(row.get('mean_elev', 0))
                        if lat and lon and alt > 0:
                            _STATIONS.append({
                                'lat': lat, 'lon': lon, 'alt': alt,
                                'nombre': row.get('gauge_name', ''),
                            })
                    except (ValueError, TypeError):
                        continue
        except Exception:
            pass

    _LOADED = True


def estimar_altitud(lat: float, lon: float, n_vecinos: int = 5,
                    max_km: float = 50.0) -> Optional[dict]:
    """
    Estima la altitud de un punto usando IDW con estaciones cercanas.

    Returns:
        dict con:
            - alt: altitud estimada (m)
            - confianza: 'alta'|'media'|'baja'
            - n_estaciones: número de estaciones usadas
            - dist_min_km: distancia a la estación más cercana
            - estacion_cercana: nombre de la estación más cercana
            - metodo: descripción del método
    """
    _load_stations()
    if not _STATIONS:
        return None

    # Calcular distancias
    dists = []
    for s in _STATIONS:
        d = _haversine(lat, lon, s['lat'], s['lon'])
        if d <= max_km:
            dists.append((d, s))

    if not dists:
        return None

    dists.sort(key=lambda x: x[0])
    nearest = dists[:n_vecinos]

    # Si la estación más cercana está a <1km, usar directamente
    if nearest[0][0] < 1.0:
        return {
            'alt': round(nearest[0][1]['alt']),
            'confianza': 'alta',
            'n_estaciones': 1,
            'dist_min_km': round(nearest[0][0], 2),
            'estacion_cercana': nearest[0][1]['nombre'],
            'metodo': f"Estacion directa ({nearest[0][1]['nombre']}, {nearest[0][0]:.1f} km)",
        }

    # IDW: peso = 1/d^2
    sum_w = 0
    sum_wa = 0
    for d, s in nearest:
        if d < 0.001:
            d = 0.001
        w = 1.0 / (d * d)
        sum_w += w
        sum_wa += w * s['alt']

    alt_est = sum_wa / sum_w if sum_w > 0 else 0

    # Confianza basada en distancia y variación
    dist_min = nearest[0][0]
    if dist_min < 5:
        confianza = 'alta'
    elif dist_min < 15:
        confianza = 'media'
    else:
        confianza = 'baja'

    return {
        'alt': round(alt_est),
        'confianza': confianza,
        'n_estaciones': len(nearest),
        'dist_min_km': round(dist_min, 2),
        'estacion_cercana': nearest[0][1]['nombre'],
        'metodo': f"IDW con {len(nearest)} estaciones (más cercana: {nearest[0][1]['nombre']}, {dist_min:.1f} km)",
    }


def obtener_altitud(lat: float, lon: float) -> float:
    """
    Wrapper simple: retorna altitud en metros.
    Retorna 200.0 como default si no puede estimar.
    """
    result = estimar_altitud(lat, lon)
    if result:
        return float(result['alt'])
    return 200.0


if __name__ == "__main__":
    import time
    puntos = [
        (-29.91, -71.25, "La Serena"),
        (-33.40, -71.13, "Curacavi"),
        (-34.73, -71.65, "Lolol"),
        (-33.45, -70.65, "Santiago"),
        (-36.82, -73.05, "Concepcion"),
        (-34.17, -70.74, "Rancagua"),
    ]
    for lat, lon, nombre in puntos:
        t0 = time.time()
        r = estimar_altitud(lat, lon)
        t1 = time.time()
        if r:
            print(f"{nombre}: {r['alt']}m ({r['confianza']}) - {r['metodo']} [{t1 - t0:.3f}s]")
        else:
            print(f"{nombre}: No disponible")
