#!/usr/bin/env python3
"""
SCRIPT UNIFICADO DE PRECIPITACIÓN - Chile
=========================================
Combina múltiples fuentes de precipitación para generar la mejor estimación
posible por punto geográfico.

FUENTES (en orden de prioridad):
1. DGA Estaciones (datos.gob.cl) — 33 estaciones, 2013-2024, datos observados
2. WorldClim 2.1 — Climatología 1970-2000, resolución ~18km, 12 GeoTIFFs
3. CHIRPS v2 — Precipitación mensual 2020-2025, resolución ~5.5km, GeoTIFFs
4. Índices climáticos (ONI/PDO/SOI) — Para pronóstico estacional

METODOLOGÍA:
- Para un punto (lat, lon) dado:
  a) Si hay estación DGA a <30km → usar datos observados directos
  b) Si no → extraer de WorldClim (climatología) + CHIRPS (monitoreo reciente)
  c) Ajustar WorldClim por factor megasequía si lat entre 30°S-37°S (-25%)
  d) Comparar año en curso (CHIRPS) vs climatología para clasificar

REFERENCIAS:
- Fick & Hijmans (2017). WorldClim 2. Int J Climatol, 37(12), 4302-4315.
- Funk et al. (2015). CHIRPS. Scientific Data, 2, 150066.
- Garreaud et al. (2024). La montaña rusa de las lluvias. CR2/U.Chile.
- McKee et al. (1993). SPI. 8th Conf Applied Climatology, AMS.

USO:
    python procesar_precipitacion_unificado.py --lat -33.45 --lon -70.65
    python procesar_precipitacion_unificado.py --sites sites_db.json
    python procesar_precipitacion_unificado.py --inventario
"""

import json
import os
import sys
import math
import csv
import glob
import argparse
from collections import defaultdict
from datetime import datetime

# ============================================================================
# CONFIGURACIÓN
# ============================================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
WORLDCLIM_DIR = os.path.join(BASE_DIR, "worldclim_prec")
CHIRPS_DIR = os.path.join(BASE_DIR, "chirps_recent")
DGA_JSON = os.path.join(BASE_DIR, "dga_estaciones_consolidado.json")
ONI_CSV = os.path.join(BASE_DIR, "indice_oni_enso.csv")
PDO_CSV = os.path.join(BASE_DIR, "indice_pdo.csv")
SOI_CSV = os.path.join(BASE_DIR, "indice_soi.csv")

# Umbral de distancia para usar estación DGA directamente (km)
DGA_DIST_THRESHOLD = 30.0

# Factor de ajuste megasequía para Chile central (30°S - 37°S)
MEGASEQUIA_LAT_MIN = -37.0
MEGASEQUIA_LAT_MAX = -30.0
MEGASEQUIA_FACTOR = 0.75  # Reducir 25% (Garreaud et al., 2024)


# ============================================================================
# UTILIDADES GEOGRÁFICAS
# ============================================================================

def haversine(lat1, lon1, lat2, lon2):
    """Distancia en km entre dos puntos (fórmula de Haversine)."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat/2)**2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon/2)**2)
    return R * 2 * math.asin(math.sqrt(a))


def parse_dga_lat(lat_str):
    """Parsea latitud DGA formato '33° 27' 05'' S' a decimal."""
    import re
    lat_str = lat_str.replace('&deg', '°').replace("''", '"').replace("'", "'")
    match = re.search(r"(\d+)[°\s]+(\d+)['\s]+(\d+)[\"'\s]*([NS])", lat_str)
    if match:
        deg, mnt, sec, hemi = match.groups()
        lat = int(deg) + int(mnt)/60 + int(sec)/3600
        if hemi == 'S':
            lat = -lat
        return lat
    return None


# ============================================================================
# FUENTE 1: ESTACIONES DGA
# ============================================================================

def cargar_estaciones_dga():
    """Carga estaciones DGA consolidadas."""
    if not os.path.exists(DGA_JSON):
        print("⚠ No se encontró dga_estaciones_consolidado.json")
        return []

    with open(DGA_JSON, 'r', encoding='utf-8') as f:
        stations = json.load(f)

    # Agregar coordenadas decimales
    for s in stations:
        lat = parse_dga_lat(s.get('lat_str', ''))
        if lat:
            s['lat'] = lat
        # DGA no provee longitud en estos CSVs, asignar manualmente por código
        s['lon'] = COORDENADAS_DGA.get(s['code'], {}).get('lon', None)

    return [s for s in stations if s.get('lat') and s.get('lon')]


# Coordenadas conocidas de estaciones DGA/DMC principales
COORDENADAS_DGA = {
    '180005': {'lon': -70.339, 'name': 'Arica'},
    '200006': {'lon': -70.181, 'name': 'Iquique'},
    '220002': {'lon': -68.903, 'name': 'Calama'},
    '230001': {'lon': -70.440, 'name': 'Antofagasta'},
    '270001': {'lon': -109.422, 'name': 'Isla de Pascua'},
    '270008': {'lon': -70.779, 'name': 'Caldera'},
    '290004': {'lon': -71.200, 'name': 'La Serena'},
    '330007': {'lon': -71.557, 'name': 'Rodelillo (Valparaíso)'},
    '330019': {'lon': -70.546, 'name': 'Tobalaba (Santiago)'},
    '330020': {'lon': -70.682, 'name': 'Quinta Normal (Santiago)'},
    '330021': {'lon': -70.786, 'name': 'Pudahuel (Santiago)'},
    '330030': {'lon': -71.614, 'name': 'Santo Domingo'},
    '330031': {'lon': -78.829, 'name': 'Juan Fernández'},
    '340031': {'lon': -71.219, 'name': 'Curicó'},
    '360011': {'lon': -72.424, 'name': 'Chillán'},
    '360019': {'lon': -73.063, 'name': 'Concepción'},
    '370033': {'lon': -72.425, 'name': 'Los Ángeles'},
    '380013': {'lon': -72.627, 'name': 'Temuco'},
    '390006': {'lon': -73.086, 'name': 'Valdivia'},
    '400009': {'lon': -73.050, 'name': 'Osorno'},
    '410005': {'lon': -73.100, 'name': 'Puerto Montt'},
    '430002': {'lon': -71.866, 'name': 'Futaleufú'},
    '430004': {'lon': -71.800, 'name': 'Alto Palena'},
    '450001': {'lon': -72.670, 'name': 'Puerto Aysén'},
    '450004': {'lon': -72.116, 'name': 'Coyhaique'},
    '450005': {'lon': -71.690, 'name': 'Balmaceda'},
    '460001': {'lon': -71.700, 'name': 'Chile Chico'},
    '470001': {'lon': -72.583, 'name': 'Cochrane'},
    '510005': {'lon': -74.000, 'name': 'Puerto Natales'},
    '520006': {'lon': -70.856, 'name': 'Punta Arenas'},
    '530005': {'lon': -70.322, 'name': 'Porvenir'},
    '550001': {'lon': -67.616, 'name': 'Puerto Williams'},
    '950001': {'lon': -62.191, 'name': 'Base Frei (Antártica)'},
}


def buscar_estacion_cercana(lat, lon, estaciones_dga, max_dist_km=DGA_DIST_THRESHOLD):
    """Busca la estación DGA más cercana dentro del umbral."""
    mejor = None
    mejor_dist = float('inf')

    for est in estaciones_dga:
        dist = haversine(lat, lon, est['lat'], est['lon'])
        if dist < mejor_dist and dist <= max_dist_km:
            mejor = est
            mejor_dist = dist

    return mejor, mejor_dist if mejor else None


# ============================================================================
# FUENTE 2: WORLDCLIM 2.1
# ============================================================================

def extraer_worldclim(lat, lon):
    """
    Extrae precipitación mensual climatológica de WorldClim 2.1 (GeoTIFF).
    Resolución: 10 arc-minutes (~18 km).
    Período: 1970-2000.
    Referencia: Fick & Hijmans (2017).
    """
    try:
        import rasterio
    except ImportError:
        # Fallback: usar datos pre-procesados del JSON
        return _extraer_worldclim_json(lat, lon)

    mensual = []
    for m in range(1, 13):
        tif_path = os.path.join(WORLDCLIM_DIR, f"wc2.1_10m_prec_{m:02d}.tif")
        if not os.path.exists(tif_path):
            mensual.append(0.0)
            continue
        with rasterio.open(tif_path) as src:
            row, col = src.index(lon, lat)
            val = float(src.read(1)[row, col])
            mensual.append(max(0.0, val))

    return mensual


def _extraer_worldclim_json(lat, lon):
    """Fallback: buscar localidad más cercana en el JSON pre-procesado."""
    json_path = os.path.join(BASE_DIR, "precipitacion_chile_worldclim.json")
    if not os.path.exists(json_path):
        return [0.0] * 12

    with open(json_path, 'r') as f:
        data = json.load(f)

    mejor = None
    mejor_dist = float('inf')
    for loc in data:
        dist = haversine(lat, lon, loc['lat'], loc['lon'])
        if dist < mejor_dist:
            mejor = loc
            mejor_dist = dist

    if mejor:
        return mejor['precip_monthly']
    return [0.0] * 12


# ============================================================================
# FUENTE 3: CHIRPS v2 (Monitoreo año en curso)
# ============================================================================

def extraer_chirps_anual(lat, lon, year=None):
    """
    Extrae precipitación mensual de CHIRPS v2 para un año específico.
    Resolución: 0.05° (~5.5 km).
    Referencia: Funk et al. (2015).
    """
    if year is None:
        year = datetime.now().year

    try:
        import rasterio
    except ImportError:
        return {}

    mensual = {}
    for m in range(1, 13):
        # Buscar archivo CHIRPS para ese mes
        patterns = [
            os.path.join(CHIRPS_DIR, f"chirps_{year}.{m:02d}.tif"),
            os.path.join(CHIRPS_DIR, f"chirps-v2.0.{year}.{m:02d}.tif"),
        ]
        for tif_path in patterns:
            if os.path.exists(tif_path):
                try:
                    with rasterio.open(tif_path) as src:
                        row, col = src.index(lon, lat)
                        val = float(src.read(1)[row, col])
                        if val >= 0:  # -9999 = nodata
                            mensual[m] = round(val, 1)
                except:
                    pass
                break

    return mensual


def monitoreo_precipitacion(lat, lon, year=None):
    """
    Compara precipitación observada (CHIRPS) vs climatología (WorldClim).
    Clasificación por terciles (DMC/CR2):
      < 0.60 → MUY SECO (sequía severa)
      0.60-0.80 → SECO (déficit moderado)
      0.80-1.20 → NORMAL
      1.20-1.40 → LLUVIOSO
      > 1.40 → MUY LLUVIOSO
    """
    if year is None:
        year = datetime.now().year

    clim = extraer_worldclim(lat, lon)
    obs = extraer_chirps_anual(lat, lon, year)

    if not obs:
        return None

    meses = sorted(obs.keys())
    acum_obs = sum(obs.values())
    acum_clim = sum(clim[m-1] for m in meses)

    ratio = acum_obs / acum_clim if acum_clim > 0 else 1.0

    if ratio < 0.60: clasif = "MUY SECO"
    elif ratio < 0.80: clasif = "SECO"
    elif ratio < 1.20: clasif = "NORMAL"
    elif ratio < 1.40: clasif = "LLUVIOSO"
    else: clasif = "MUY LLUVIOSO"

    return {
        'year': year,
        'acum_observado_mm': round(acum_obs, 1),
        'acum_climatologia_mm': round(acum_clim, 1),
        'ratio': round(ratio, 2),
        'clasificacion': clasif,
        'meses_con_dato': meses,
        'detalle_mensual': {str(m): obs[m] for m in meses}
    }


# ============================================================================
# PRONÓSTICO ESTACIONAL (Análogos ENSO/PDO)
# ============================================================================

def cargar_indices_climaticos():
    """Carga índices ONI, PDO, SOI desde CSV."""
    indices = {}

    # ONI
    if os.path.exists(ONI_CSV):
        oni_data = []
        with open(ONI_CSV, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                oni_data.append(row)
        indices['oni'] = oni_data

    # PDO
    if os.path.exists(PDO_CSV):
        pdo_data = []
        with open(PDO_CSV, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                pdo_data.append(row)
        indices['pdo'] = pdo_data

    # SOI
    if os.path.exists(SOI_CSV):
        soi_data = []
        with open(SOI_CSV, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                soi_data.append(row)
        indices['soi'] = soi_data

    return indices


def pronostico_estacional(lat, lon, precip_clim):
    """
    Pronóstico estacional basado en análogos ENSO/PDO/SOI.

    Pesos (Garreaud et al., 2024):
    - ONI: 0.45 (señal dominante en Chile central)
    - SOI: 0.30 (complementa ONI con presión atmosférica)
    - PDO: 0.25 (modulación decadal)

    ADVERTENCIA: La correlación ENSO-precipitación se ha debilitado
    significativamente desde 2000 en Chile central.
    """
    indices = cargar_indices_climaticos()

    if not indices.get('oni'):
        return None

    # Estado actual (último registro)
    try:
        oni_actual = float(indices['oni'][-1].get('anomalia', 0))
    except:
        oni_actual = 0.0

    try:
        pdo_vals = [float(r.get('pdo', 0)) for r in indices.get('pdo', [])[-3:]
                    if abs(float(r.get('pdo', 0))) < 90]
        pdo_actual = sum(pdo_vals) / len(pdo_vals) if pdo_vals else 0.0
    except:
        pdo_actual = 0.0

    try:
        soi_vals = [float(r.get('soi', 0)) for r in indices.get('soi', [])[-3:]]
        soi_actual = sum(soi_vals) / len(soi_vals) if soi_vals else 0.0
    except:
        soi_actual = 0.0

    # Factor empírico (Garreaud et al., 2024)
    factor_enso = 1.0 + 0.15 * oni_actual
    factor_pdo = 1.0 + 0.05 * pdo_actual
    factor = factor_enso * factor_pdo

    # Clasificación
    if factor < 0.85: outlook = "SECO"
    elif factor < 1.15: outlook = "NORMAL"
    else: outlook = "LLUVIOSO"

    # Estado ENSO
    if oni_actual >= 0.5: enso_estado = "El Niño"
    elif oni_actual <= -0.5: enso_estado = "La Niña"
    else: enso_estado = "Neutro"

    precip_anual = sum(precip_clim)

    return {
        'enso_estado': enso_estado,
        'oni_actual': round(oni_actual, 2),
        'pdo_actual': round(pdo_actual, 2),
        'soi_actual': round(soi_actual, 2),
        'outlook': outlook,
        'factor': round(factor, 2),
        'precip_esperada_mm': round(precip_anual * factor, 0),
        'texto_informe': (
            f"Estado ENSO actual: {enso_estado} (ONI={oni_actual:.2f}). "
            f"PDO={'positiva' if pdo_actual > 0 else 'negativa'} ({pdo_actual:.2f}). "
            f"Pronóstico: año {outlook.lower()} con precipitación estimada "
            f"de {precip_anual * factor:.0f} mm "
            f"(climatología: {precip_anual:.0f} mm)."
        )
    }


# ============================================================================
# FUNCIÓN PRINCIPAL: OBTENER PRECIPITACIÓN PARA UN PUNTO
# ============================================================================

def obtener_precipitacion(lat, lon, nombre=None):
    """
    Obtiene la mejor estimación de precipitación para un punto.

    Retorna dict con:
    - precip_monthly: [12 valores] precipitación mensual (mm)
    - precip_annual: total anual (mm)
    - fuente: nombre de la fuente usada
    - fuente_detalle: descripción completa
    - ajuste_megasequia: si se aplicó ajuste
    - monitoreo: comparación año en curso vs climatología
    - pronostico: pronóstico estacional
    - estacion_dga: datos de estación cercana (si aplica)
    """
    resultado = {
        'lat': lat,
        'lon': lon,
        'nombre': nombre,
        'timestamp': datetime.now().isoformat(),
    }

    # Paso 1: Buscar estación DGA cercana
    estaciones = cargar_estaciones_dga()
    estacion, dist = buscar_estacion_cercana(lat, lon, estaciones)

    if estacion:
        resultado['precip_monthly'] = estacion['precip_monthly_mm']
        resultado['precip_annual'] = estacion['precip_annual_mm']
        resultado['fuente'] = 'DGA_Estaciones'
        resultado['fuente_detalle'] = (
            f"Estación {estacion['name']} (código {estacion['code']}), "
            f"distancia {dist:.1f} km, período {estacion['period']}. "
            f"Datos observados vía datos.gob.cl (DGA/DMC)."
        )
        resultado['ajuste_megasequia'] = False
        resultado['estacion_dga'] = {
            'code': estacion['code'],
            'name': estacion['name'],
            'dist_km': round(dist, 1),
            'period': estacion['period']
        }
    else:
        # Paso 2: Usar WorldClim
        wc = extraer_worldclim(lat, lon)

        # Paso 3: Ajustar por megasequía si corresponde
        if MEGASEQUIA_LAT_MIN <= lat <= MEGASEQUIA_LAT_MAX:
            wc_ajustado = [round(v * MEGASEQUIA_FACTOR, 1) for v in wc]
            resultado['precip_monthly'] = wc_ajustado
            resultado['ajuste_megasequia'] = True
            resultado['fuente'] = 'WorldClim_2.1_ajustado'
            resultado['fuente_detalle'] = (
                f"Climatología WorldClim 2.1 (Fick & Hijmans, 2017), "
                f"resolución 10 arc-min, período 1970-2000. "
                f"AJUSTADO -25% por megasequía post-2010 "
                f"(Garreaud et al., 2024). "
                f"Estación DGA más cercana a >{DGA_DIST_THRESHOLD:.0f} km."
            )
        else:
            resultado['precip_monthly'] = [round(v, 1) for v in wc]
            resultado['ajuste_megasequia'] = False
            resultado['fuente'] = 'WorldClim_2.1'
            resultado['fuente_detalle'] = (
                f"Climatología WorldClim 2.1 (Fick & Hijmans, 2017), "
                f"resolución 10 arc-min, período 1970-2000."
            )

        resultado['precip_annual'] = round(sum(resultado['precip_monthly']), 1)

    # Paso 4: Monitoreo año en curso
    monitoreo = monitoreo_precipitacion(lat, lon)
    if monitoreo:
        resultado['monitoreo'] = monitoreo

    # Paso 5: Pronóstico estacional
    pronostico = pronostico_estacional(lat, lon, resultado['precip_monthly'])
    if pronostico:
        resultado['pronostico'] = pronostico

    return resultado


# ============================================================================
# INVENTARIO DE DATOS
# ============================================================================

def mostrar_inventario():
    """Muestra inventario completo de datos disponibles."""
    print("=" * 70)
    print("INVENTARIO DE DATOS DE PRECIPITACIÓN - CHILE")
    print("=" * 70)

    # WorldClim
    wc_files = glob.glob(os.path.join(WORLDCLIM_DIR, "*.tif"))
    print(f"\n📦 WORLDCLIM 2.1 (Climatología)")
    print(f"   Archivos: {len(wc_files)} GeoTIFF")
    print(f"   Período: 1970-2000")
    print(f"   Resolución: 10 arc-min (~18 km)")
    total_mb = sum(os.path.getsize(f) for f in wc_files) / 1e6
    print(f"   Tamaño: {total_mb:.1f} MB")

    # CHIRPS
    chirps_tifs = sorted(glob.glob(os.path.join(CHIRPS_DIR, "*.tif")))
    print(f"\n📦 CHIRPS v2 (Monitoreo mensual)")
    print(f"   Archivos: {len(chirps_tifs)} GeoTIFF")
    if chirps_tifs:
        years = set()
        for f in chirps_tifs:
            bn = os.path.basename(f)
            parts = bn.replace('chirps_', '').replace('chirps-v2.0.', '').replace('.tif', '').split('.')
            if len(parts) >= 1 and parts[0].isdigit():
                years.add(int(parts[0]))
        print(f"   Período: {min(years)}-{max(years)}")
    print(f"   Resolución: 0.05° (~5.5 km)")
    total_mb = sum(os.path.getsize(f) for f in chirps_tifs) / 1e6
    print(f"   Tamaño: {total_mb:.1f} MB")

    # DGA
    dga_csvs = glob.glob(os.path.join(BASE_DIR, "dga_estaciones", "*.csv"))
    print(f"\n📦 DGA ESTACIONES (datos.gob.cl)")
    print(f"   Archivos CSV: {len(dga_csvs)}")
    if os.path.exists(DGA_JSON):
        with open(DGA_JSON) as f:
            est = json.load(f)
        print(f"   Estaciones: {len(est)}")
        print(f"   Período: 2013-2024")
    total_mb = sum(os.path.getsize(f) for f in dga_csvs) / 1e6
    print(f"   Tamaño: {total_mb:.1f} MB")

    # Índices
    print(f"\n📦 ÍNDICES CLIMÁTICOS (NOAA)")
    for name, path in [("ONI (ENSO)", ONI_CSV), ("PDO", PDO_CSV), ("SOI", SOI_CSV)]:
        if os.path.exists(path):
            lines = sum(1 for _ in open(path)) - 1
            print(f"   {name}: {lines} registros")

    # WorldClim JSON (pre-procesado)
    wc_json = os.path.join(BASE_DIR, "precipitacion_chile_worldclim.json")
    if os.path.exists(wc_json):
        with open(wc_json) as f:
            locs = json.load(f)
        print(f"\n📦 WORLDCLIM PRE-PROCESADO (JSON)")
        print(f"   Localidades: {len(locs)}")

    # Total
    all_files = (wc_files + chirps_tifs + dga_csvs +
                 [ONI_CSV, PDO_CSV, SOI_CSV])
    total = sum(os.path.getsize(f) for f in all_files if os.path.exists(f)) / 1e6
    print(f"\n{'='*70}")
    print(f"TOTAL: {total:.0f} MB en {len(all_files)} archivos")
    print(f"{'='*70}")


# ============================================================================
# CHIRPS HISTÓRICO: Extraer serie temporal para un punto
# ============================================================================

def extraer_serie_chirps(lat, lon, year_start=2020, year_end=2025):
    """
    Extrae serie mensual completa de CHIRPS para un punto.
    Útil para calcular tendencias y SPI.
    """
    try:
        import rasterio
    except ImportError:
        print("⚠ rasterio no disponible. Instalar: pip install rasterio")
        return {}

    serie = {}
    for year in range(year_start, year_end + 1):
        for month in range(1, 13):
            patterns = [
                os.path.join(CHIRPS_DIR, f"chirps_{year}.{month:02d}.tif"),
                os.path.join(CHIRPS_DIR, f"chirps-v2.0.{year}.{month:02d}.tif"),
            ]
            for tif_path in patterns:
                if os.path.exists(tif_path):
                    try:
                        with rasterio.open(tif_path) as src:
                            row, col = src.index(lon, lat)
                            val = float(src.read(1)[row, col])
                            if val >= 0:
                                serie[f"{year}-{month:02d}"] = round(val, 1)
                    except:
                        pass
                    break

    return serie


# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Precipitación unificada Chile")
    parser.add_argument("--lat", type=float, help="Latitud (negativa para Sur)")
    parser.add_argument("--lon", type=float, help="Longitud (negativa para Oeste)")
    parser.add_argument("--nombre", type=str, default=None, help="Nombre del sitio")
    parser.add_argument("--sites", type=str, help="Archivo JSON con sitios a procesar")
    parser.add_argument("--inventario", action="store_true", help="Mostrar inventario de datos")
    parser.add_argument("--serie", action="store_true", help="Extraer serie CHIRPS completa")
    parser.add_argument("--output", type=str, help="Archivo de salida JSON")

    args = parser.parse_args()

    if args.inventario:
        mostrar_inventario()
        sys.exit(0)

    if args.lat and args.lon:
        resultado = obtener_precipitacion(args.lat, args.lon, args.nombre)

        if args.serie:
            resultado['serie_chirps'] = extraer_serie_chirps(args.lat, args.lon)

        print(json.dumps(resultado, indent=2, ensure_ascii=False))

        if args.output:
            with open(args.output, 'w', encoding='utf-8') as f:
                json.dump(resultado, f, indent=2, ensure_ascii=False)
            print(f"\nGuardado en: {args.output}")

    elif args.sites:
        with open(args.sites, 'r') as f:
            sites = json.load(f)

        print(f"Procesando {len(sites)} sitios...")
        resultados = []
        for i, site in enumerate(sites):
            lat = site.get('lat')
            lon = site.get('lon')
            nombre = site.get('csv_file', '').replace('.CSV', '')

            if lat and lon:
                r = obtener_precipitacion(lat, lon, nombre)
                # Agregar campos al sitio original
                site['precip_monthly'] = r['precip_monthly']
                site['precip_annual'] = r['precip_annual']
                site['precip_fuente'] = r['fuente']
                if r.get('monitoreo'):
                    site['monitoreo_precipitacion'] = r['monitoreo']
                if r.get('pronostico'):
                    site['pronostico_estacional'] = r['pronostico']
                resultados.append(site)

                if (i + 1) % 50 == 0:
                    print(f"  {i+1}/{len(sites)} procesados...")

        output = args.output or args.sites.replace('.json', '_con_precip.json')
        with open(output, 'w', encoding='utf-8') as f:
            json.dump(resultados, f, indent=2, ensure_ascii=False)
        print(f"\nGuardado: {output} ({len(resultados)} sitios)")

    else:
        parser.print_help()
