#!/usr/bin/env python3
"""
process_data.py
===============
Processes raw data sources into SQLite databases for the agroclimatico project.

Tasks:
1. Enhanced electrico.db from Transparency CSV
2. geospatial.db from GeoJSON files
3. CONAF land use summaries into geospatial.db
"""

import os
import sys
import json
import sqlite3
import traceback
from pathlib import Path

import pandas as pd

# --- Paths ---
PROJECT_DIR = Path("/Users/erickrojoolea/Projects/agroclimatico")
DB_DIR = PROJECT_DIR / "data" / "db"
ELECTRICO_DB = DB_DIR / "electrico.db"
GEOSPATIAL_DB = DB_DIR / "geospatial.db"

CSV_PATH = Path("/Users/erickrojoolea/Desktop/Data/Transparencia_AU004T0051734.csv")
GEOJSON_DIR = Path("/Users/erickrojoolea/Desktop/Data Estado/Scrapping Estado/datos_geo/_geojson")
CONAF_DIR = Path("/Users/erickrojoolea/Desktop/Data Estado/Scrapping Estado/datos_geo/CONAF")

# Potencia ranges
POTENCIA_RANGES = [
    ('0-3 kW', 0, 3),
    ('3-6 kW', 3, 6),
    ('6-10 kW', 6, 10),
    ('10-20 kW', 10, 20),
    ('20-50 kW', 20, 50),
    ('50-100 kW', 50, 100),
    ('100+ kW', 100, float('inf')),
]

def classify_potencia(val):
    """Classify a potencia value into a range label."""
    for label, lo, hi in POTENCIA_RANGES:
        if lo <= val < hi:
            return label
    return '100+ kW'


# =============================================================================
# TASK 1: Enhanced electrico.db
# =============================================================================

def process_electrico():
    """Process transparency CSV into electrico.db summary tables."""
    print("\n" + "=" * 60)
    print("TASK 1: Processing electrico.db")
    print("=" * 60)

    if not CSV_PATH.exists():
        print(f"ERROR: CSV not found at {CSV_PATH}")
        return

    conn = sqlite3.connect(str(ELECTRICO_DB))
    cursor = conn.cursor()

    # Drop and recreate datos_brutos (empty - too large for git)
    cursor.execute("DROP TABLE IF EXISTS datos_brutos")
    cursor.execute("""
        CREATE TABLE datos_brutos (
            cliente_id TEXT,
            empresa_id TEXT,
            empresa TEXT,
            region_id TEXT,
            nombre_region TEXT,
            comuna_id TEXT,
            nombre_comuna TEXT,
            tipo_tension_red_id INTEGER,
            potencia_contratada REAL
        )
    """)
    print("  - Created empty datos_brutos table (schema only)")

    # Prepare aggregation accumulators
    # clientes_por_comuna: nombre_comuna -> {nombre_region, num_clientes, potencia_total_kw}
    clientes_comuna = {}
    # clientes_por_empresa_comuna: (empresa, nombre_comuna) -> {nombre_region, num_clientes, potencia_total}
    clientes_empresa_comuna = {}
    # resumen_regional: nombre_region -> {num_clientes, num_comunas set, potencia_total, empresas set}
    resumen_regional = {}
    # distribucion_potencia: (nombre_comuna, nombre_region, rango) -> {num_clientes, potencia_subtotal}
    dist_potencia = {}
    # tipo_tension: (nombre_comuna, tipo_tension_red_id) -> {num_clientes, potencia_total}
    tipo_tension = {}

    print(f"  - Reading CSV in chunks from {CSV_PATH.name}...")
    chunk_num = 0
    total_rows = 0

    for chunk in pd.read_csv(
        str(CSV_PATH),
        sep='|',
        encoding='utf-8-sig',
        chunksize=500000,
        dtype={
            'cliente_id': str,
            'empresa_id': str,
            'empresa': str,
            'region_id': str,
            'nombre_region': str,
            'comuna_id': str,
            'nombre_comuna': str,
            'tipo_tension_red_id': str,
            'potencia_contratada': str,
        }
    ):
        chunk_num += 1
        rows = len(chunk)
        total_rows += rows
        print(f"    Chunk {chunk_num}: {rows:,} rows (total: {total_rows:,})")

        # Clean potencia - period is decimal separator, so "6.000" = 6.0 kW
        chunk['potencia_kw'] = pd.to_numeric(chunk['potencia_contratada'], errors='coerce').fillna(0.0)
        chunk['tipo_tension_red_id_int'] = pd.to_numeric(chunk['tipo_tension_red_id'], errors='coerce').fillna(0).astype(int)

        # Aggregate clientes_por_comuna
        for (comuna, region), grp in chunk.groupby(['nombre_comuna', 'nombre_region']):
            key = comuna
            if key not in clientes_comuna:
                clientes_comuna[key] = {'nombre_region': region, 'num_clientes': 0, 'potencia_total_kw': 0.0}
            clientes_comuna[key]['num_clientes'] += len(grp)
            clientes_comuna[key]['potencia_total_kw'] += grp['potencia_kw'].sum()

        # Aggregate clientes_por_empresa_comuna
        for (empresa, comuna, region), grp in chunk.groupby(['empresa', 'nombre_comuna', 'nombre_region']):
            key = (empresa, comuna)
            if key not in clientes_empresa_comuna:
                clientes_empresa_comuna[key] = {'nombre_region': region, 'num_clientes': 0, 'potencia_total_kw': 0.0}
            clientes_empresa_comuna[key]['num_clientes'] += len(grp)
            clientes_empresa_comuna[key]['potencia_total_kw'] += grp['potencia_kw'].sum()

        # Aggregate resumen_regional
        for (region,), grp in chunk.groupby(['nombre_region']):
            if region not in resumen_regional:
                resumen_regional[region] = {
                    'num_clientes': 0,
                    'comunas': set(),
                    'potencia_total_kw': 0.0,
                    'empresas': set()
                }
            resumen_regional[region]['num_clientes'] += len(grp)
            resumen_regional[region]['comunas'].update(grp['nombre_comuna'].dropna().unique())
            resumen_regional[region]['potencia_total_kw'] += grp['potencia_kw'].sum()
            resumen_regional[region]['empresas'].update(grp['empresa'].dropna().unique())

        # Aggregate distribucion_potencia
        chunk['rango_potencia'] = chunk['potencia_kw'].apply(classify_potencia)
        for (comuna, region, rango), grp in chunk.groupby(['nombre_comuna', 'nombre_region', 'rango_potencia']):
            key = (comuna, region, rango)
            if key not in dist_potencia:
                dist_potencia[key] = {'num_clientes': 0, 'potencia_subtotal_kw': 0.0}
            dist_potencia[key]['num_clientes'] += len(grp)
            dist_potencia[key]['potencia_subtotal_kw'] += grp['potencia_kw'].sum()

        # Aggregate tipo_tension
        for (comuna, tt_id), grp in chunk.groupby(['nombre_comuna', 'tipo_tension_red_id_int']):
            key = (comuna, tt_id)
            if key not in tipo_tension:
                tipo_tension[key] = {'num_clientes': 0, 'potencia_total_kw': 0.0}
            tipo_tension[key]['num_clientes'] += len(grp)
            tipo_tension[key]['potencia_total_kw'] += grp['potencia_kw'].sum()

    print(f"  - Total rows processed: {total_rows:,}")

    # Write clientes_por_comuna
    cursor.execute("DROP TABLE IF EXISTS clientes_por_comuna")
    cursor.execute("""
        CREATE TABLE clientes_por_comuna (
            nombre_comuna TEXT PRIMARY KEY,
            nombre_region TEXT,
            num_clientes INTEGER,
            potencia_total_kw REAL
        )
    """)
    for comuna, data in clientes_comuna.items():
        cursor.execute(
            "INSERT INTO clientes_por_comuna VALUES (?, ?, ?, ?)",
            (comuna, data['nombre_region'], data['num_clientes'], round(data['potencia_total_kw'], 2))
        )
    print(f"  - clientes_por_comuna: {len(clientes_comuna)} rows")

    # Write clientes_por_empresa_comuna
    cursor.execute("DROP TABLE IF EXISTS clientes_por_empresa_comuna")
    cursor.execute("""
        CREATE TABLE clientes_por_empresa_comuna (
            empresa TEXT,
            nombre_comuna TEXT,
            nombre_region TEXT,
            num_clientes INTEGER,
            potencia_total_kw REAL,
            PRIMARY KEY (empresa, nombre_comuna)
        )
    """)
    for (empresa, comuna), data in clientes_empresa_comuna.items():
        cursor.execute(
            "INSERT INTO clientes_por_empresa_comuna VALUES (?, ?, ?, ?, ?)",
            (empresa, comuna, data['nombre_region'], data['num_clientes'], round(data['potencia_total_kw'], 2))
        )
    print(f"  - clientes_por_empresa_comuna: {len(clientes_empresa_comuna)} rows")

    # Write resumen_regional
    cursor.execute("DROP TABLE IF EXISTS resumen_regional")
    cursor.execute("""
        CREATE TABLE resumen_regional (
            nombre_region TEXT PRIMARY KEY,
            num_clientes INTEGER,
            num_comunas INTEGER,
            potencia_total_kw REAL,
            num_empresas INTEGER
        )
    """)
    for region, data in resumen_regional.items():
        cursor.execute(
            "INSERT INTO resumen_regional VALUES (?, ?, ?, ?, ?)",
            (region, data['num_clientes'], len(data['comunas']),
             round(data['potencia_total_kw'], 2), len(data['empresas']))
        )
    print(f"  - resumen_regional: {len(resumen_regional)} rows")

    # Write distribucion_potencia_comuna
    cursor.execute("DROP TABLE IF EXISTS distribucion_potencia_comuna")
    cursor.execute("""
        CREATE TABLE distribucion_potencia_comuna (
            nombre_comuna TEXT,
            nombre_region TEXT,
            rango_potencia TEXT,
            num_clientes INTEGER,
            potencia_subtotal_kw REAL
        )
    """)
    for (comuna, region, rango), data in dist_potencia.items():
        cursor.execute(
            "INSERT INTO distribucion_potencia_comuna VALUES (?, ?, ?, ?, ?)",
            (comuna, region, rango, data['num_clientes'], round(data['potencia_subtotal_kw'], 2))
        )
    print(f"  - distribucion_potencia_comuna: {len(dist_potencia)} rows")

    # Write tipo_tension_comuna
    cursor.execute("DROP TABLE IF EXISTS tipo_tension_comuna")
    cursor.execute("""
        CREATE TABLE tipo_tension_comuna (
            nombre_comuna TEXT,
            tipo_tension_red_id INTEGER,
            num_clientes INTEGER,
            potencia_total_kw REAL
        )
    """)
    for (comuna, tt_id), data in tipo_tension.items():
        cursor.execute(
            "INSERT INTO tipo_tension_comuna VALUES (?, ?, ?, ?)",
            (comuna, tt_id, data['num_clientes'], round(data['potencia_total_kw'], 2))
        )
    print(f"  - tipo_tension_comuna: {len(tipo_tension)} rows")

    conn.commit()
    conn.close()
    print("  DONE: electrico.db updated successfully.")


# =============================================================================
# TASK 2: geospatial.db from GeoJSON files
# =============================================================================

def get_centroid(geometry):
    """Extract centroid (lat, lon) from a GeoJSON geometry."""
    gtype = geometry.get('type', '')
    coords = geometry.get('coordinates', [])

    if gtype == 'Point':
        return coords[1], coords[0]  # lat, lon

    elif gtype == 'MultiPoint':
        if coords:
            lats = [c[1] for c in coords]
            lons = [c[0] for c in coords]
            return sum(lats) / len(lats), sum(lons) / len(lons)

    elif gtype == 'Polygon':
        ring = coords[0] if coords else []
        if ring:
            lats = [c[1] for c in ring]
            lons = [c[0] for c in ring]
            return sum(lats) / len(lats), sum(lons) / len(lons)

    elif gtype == 'MultiPolygon':
        if coords and coords[0] and coords[0][0]:
            ring = coords[0][0]
            lats = [c[1] for c in ring]
            lons = [c[0] for c in ring]
            return sum(lats) / len(lats), sum(lons) / len(lons)

    elif gtype == 'LineString':
        if coords:
            lats = [c[1] for c in coords]
            lons = [c[0] for c in coords]
            return sum(lats) / len(lats), sum(lons) / len(lons)

    elif gtype == 'MultiLineString':
        if coords and coords[0]:
            line = coords[0]
            lats = [c[1] for c in line]
            lons = [c[0] for c in line]
            return sum(lats) / len(lats), sum(lons) / len(lons)

    return None, None


def load_geojson(filename):
    """Load a GeoJSON file, trying multiple encodings."""
    filepath = GEOJSON_DIR / filename
    if not filepath.exists():
        # Try to find with different encoding of special chars
        print(f"    File not found: {filename}, searching alternatives...")
        # List directory and find closest match
        candidates = list(GEOJSON_DIR.glob("*"))
        # Try matching by removing special chars
        base = filename.replace('¢', '').replace('ó', '').lower()
        for c in candidates:
            if c.name.replace('¢', '').replace('ó', '').lower() == base:
                filepath = c
                print(f"    Found alternative: {c.name}")
                break
        else:
            print(f"    WARNING: Could not find file matching {filename}")
            return None

    for enc in ['utf-8', 'latin-1', 'cp1252', 'utf-8-sig']:
        try:
            with open(filepath, 'r', encoding=enc) as f:
                data = json.load(f)
            return data
        except (json.JSONDecodeError, UnicodeDecodeError):
            continue

    print(f"    WARNING: Could not decode {filepath.name} with any encoding")
    return None


def get_prop(props, *keys):
    """Get first non-None property value from a list of possible keys."""
    for k in keys:
        val = props.get(k)
        if val is not None:
            return str(val).strip() if isinstance(val, str) else val
        # Try case-insensitive
        for pk in props:
            if pk.lower() == k.lower():
                val = props[pk]
                if val is not None:
                    return str(val).strip() if isinstance(val, str) else val
    return None


def process_geospatial():
    """Create geospatial.db from GeoJSON files."""
    print("\n" + "=" * 60)
    print("TASK 2: Creating geospatial.db")
    print("=" * 60)

    if not GEOJSON_DIR.exists():
        print(f"ERROR: GeoJSON directory not found at {GEOJSON_DIR}")
        return

    conn = sqlite3.connect(str(GEOSPATIAL_DB))
    cursor = conn.cursor()

    # --- Embalses ---
    print("\n  Processing embalses...")
    try:
        data = load_geojson("DGA__Embalses__Inv_Embalses_DGA_DOH.geojson")
        if data:
            features = data.get('features', [])
            if features:
                print(f"    Property keys: {list(features[0]['properties'].keys())}")
            cursor.execute("DROP TABLE IF EXISTS embalses")
            cursor.execute("""
                CREATE TABLE embalses (
                    nombre TEXT, lat REAL, lon REAL, region TEXT, tipo TEXT
                )
            """)
            count = 0
            for f in features:
                props = f.get('properties', {})
                geom = f.get('geometry')
                if not geom:
                    continue
                lat, lon = get_centroid(geom)
                nombre = get_prop(props, 'NOMBRE', 'NOM_EMBALSE', 'nombre', 'Nombre')
                region = get_prop(props, 'REGION', 'NOM_REGION', 'Region', 'region')
                tipo = get_prop(props, 'TIPO', 'Tipo', 'tipo', 'TIPO_EMBALSE')
                cursor.execute("INSERT INTO embalses VALUES (?,?,?,?,?)",
                               (nombre, lat, lon, region, tipo))
                count += 1
            print(f"    embalses: {count} rows")
    except Exception as e:
        print(f"    ERROR processing embalses: {e}")
        traceback.print_exc()

    # --- Estaciones Fluviometricas ---
    print("\n  Processing estaciones_fluviometricas...")
    try:
        data = load_geojson("DGA__EstacionesFluviometricas__Fluviometricas.geojson")
        if data:
            features = data.get('features', [])
            if features:
                print(f"    Property keys: {list(features[0]['properties'].keys())}")
            cursor.execute("DROP TABLE IF EXISTS estaciones_fluviometricas")
            cursor.execute("""
                CREATE TABLE estaciones_fluviometricas (
                    nombre TEXT, lat REAL, lon REAL, region TEXT
                )
            """)
            count = 0
            for f in features:
                props = f.get('properties', {})
                geom = f.get('geometry')
                if not geom:
                    continue
                lat, lon = get_centroid(geom)
                nombre = get_prop(props, 'NOMBRE', 'nombre', 'Nombre', 'NOM_ESTACION')
                region = get_prop(props, 'REGION', 'NOM_REGION', 'Region', 'region')
                cursor.execute("INSERT INTO estaciones_fluviometricas VALUES (?,?,?,?)",
                               (nombre, lat, lon, region))
                count += 1
            print(f"    estaciones_fluviometricas: {count} rows")
    except Exception as e:
        print(f"    ERROR processing estaciones_fluviometricas: {e}")
        traceback.print_exc()

    # --- Estaciones Meteorologicas ---
    print("\n  Processing estaciones_meteorologicas...")
    try:
        data = load_geojson("DGA__EstacionesMeteorologicas__Meteorologicas.geojson")
        if data:
            features = data.get('features', [])
            if features:
                print(f"    Property keys: {list(features[0]['properties'].keys())}")
            cursor.execute("DROP TABLE IF EXISTS estaciones_meteorologicas")
            cursor.execute("""
                CREATE TABLE estaciones_meteorologicas (
                    nombre TEXT, lat REAL, lon REAL, region TEXT
                )
            """)
            count = 0
            for f in features:
                props = f.get('properties', {})
                geom = f.get('geometry')
                if not geom:
                    continue
                lat, lon = get_centroid(geom)
                nombre = get_prop(props, 'NOMBRE', 'nombre', 'Nombre', 'NOM_ESTACION')
                region = get_prop(props, 'REGION', 'NOM_REGION', 'Region', 'region')
                cursor.execute("INSERT INTO estaciones_meteorologicas VALUES (?,?,?,?)",
                               (nombre, lat, lon, region))
                count += 1
            print(f"    estaciones_meteorologicas: {count} rows")
    except Exception as e:
        print(f"    ERROR processing estaciones_meteorologicas: {e}")
        traceback.print_exc()

    # --- Calidad de Aguas ---
    print("\n  Processing calidad_agua...")
    try:
        data = load_geojson("DGA__EstacionesCalidadAguas__Calidad_de_Aguas.geojson")
        if data:
            features = data.get('features', [])
            if features:
                print(f"    Property keys: {list(features[0]['properties'].keys())}")
            cursor.execute("DROP TABLE IF EXISTS calidad_agua")
            cursor.execute("""
                CREATE TABLE calidad_agua (
                    nombre TEXT, lat REAL, lon REAL, ica TEXT
                )
            """)
            count = 0
            for f in features:
                props = f.get('properties', {})
                geom = f.get('geometry')
                if not geom:
                    continue
                lat, lon = get_centroid(geom)
                nombre = get_prop(props, 'NOMBRE', 'nombre', 'Nombre', 'NOM_ESTACION')
                ica = get_prop(props, 'ICA', 'ica', 'Ica')
                cursor.execute("INSERT INTO calidad_agua VALUES (?,?,?,?)",
                               (nombre, lat, lon, ica))
                count += 1
            print(f"    calidad_agua: {count} rows")
    except Exception as e:
        print(f"    ERROR processing calidad_agua: {e}")
        traceback.print_exc()

    # --- Restricciones Hidricas ---
    print("\n  Processing restricciones_hidricas...")
    try:
        # Filename uses ¢ instead of o with accent
        fname = "DGA__Areas_de_Restricci\u00a2n_Zonas_de_Prohibici\u00a2n__Areas_de_Restricci\u00a2n_Zonas_de_Prohibici\u00a2n.geojson"
        data = load_geojson(fname)
        if data:
            features = data.get('features', [])
            if features:
                print(f"    Property keys: {list(features[0]['properties'].keys())}")
            cursor.execute("DROP TABLE IF EXISTS restricciones_hidricas")
            cursor.execute("""
                CREATE TABLE restricciones_hidricas (
                    cod_shac TEXT, regiones TEXT, tipo TEXT, descripcion TEXT,
                    lat_centroid REAL, lon_centroid REAL
                )
            """)
            count = 0
            for f in features:
                props = f.get('properties', {})
                geom = f.get('geometry')
                if not geom:
                    continue
                lat, lon = get_centroid(geom)
                cod_shac = get_prop(props, 'COD_SHAC', 'cod_shac', 'CODSHAC')
                regiones = get_prop(props, 'REGIONES', 'REGION', 'regiones', 'NOM_REGION')
                tipo = get_prop(props, 'TIPO', 'tipo', 'Tipo')
                descripcion = get_prop(props, 'DESCRIPCION', 'descripcion', 'DESCRIPCIO', 'NOMBRE')
                cursor.execute("INSERT INTO restricciones_hidricas VALUES (?,?,?,?,?,?)",
                               (cod_shac, regiones, tipo, descripcion, lat, lon))
                count += 1
            print(f"    restricciones_hidricas: {count} rows")
    except Exception as e:
        print(f"    ERROR processing restricciones_hidricas: {e}")
        traceback.print_exc()

    # --- Agotamiento ---
    print("\n  Processing agotamiento...")
    try:
        fname = "Declaraci\u00a2n_de_Agotamiento__Declaraci\u00a2n_de_Agotamiento.geojson"
        data = load_geojson(fname)
        if data:
            features = data.get('features', [])
            if features:
                print(f"    Property keys: {list(features[0]['properties'].keys())}")
            cursor.execute("DROP TABLE IF EXISTS agotamiento")
            cursor.execute("""
                CREATE TABLE agotamiento (
                    nombre TEXT, region TEXT, lat_centroid REAL, lon_centroid REAL
                )
            """)
            count = 0
            for f in features:
                props = f.get('properties', {})
                geom = f.get('geometry')
                if not geom:
                    continue
                lat, lon = get_centroid(geom)
                nombre = get_prop(props, 'NOMBRE', 'NOM_AGOTA', 'nombre', 'Nombre', 'NOM_FUENTE')
                region = get_prop(props, 'REGION', 'NOM_REGION', 'region', 'REGIONES')
                cursor.execute("INSERT INTO agotamiento VALUES (?,?,?,?)",
                               (nombre, region, lat, lon))
                count += 1
            print(f"    agotamiento: {count} rows")
    except Exception as e:
        print(f"    ERROR processing agotamiento: {e}")
        traceback.print_exc()

    # --- Cuencas ---
    print("\n  Processing cuencas...")
    try:
        data = load_geojson("DGA__Cuencas_BNA__Cuencas_BNA.geojson")
        if data:
            features = data.get('features', [])
            if features:
                print(f"    Property keys: {list(features[0]['properties'].keys())}")
            cursor.execute("DROP TABLE IF EXISTS cuencas")
            cursor.execute("""
                CREATE TABLE cuencas (
                    nombre TEXT, cod_cuen TEXT, lat_centroid REAL, lon_centroid REAL
                )
            """)
            count = 0
            for f in features:
                props = f.get('properties', {})
                geom = f.get('geometry')
                if not geom:
                    continue
                lat, lon = get_centroid(geom)
                nombre = get_prop(props, 'NOM_CUEN', 'NOMBRE', 'nombre')
                cod_cuen = get_prop(props, 'COD_CUEN', 'cod_cuen', 'CODCUEN')
                cursor.execute("INSERT INTO cuencas VALUES (?,?,?,?)",
                               (nombre, cod_cuen, lat, lon))
                count += 1
            print(f"    cuencas: {count} rows")
    except Exception as e:
        print(f"    ERROR processing cuencas: {e}")
        traceback.print_exc()

    # --- SubCuencas ---
    print("\n  Processing subcuencas...")
    try:
        data = load_geojson("DGA__SubCuencas_BNA__SubCuencas_BNA.geojson")
        if data:
            features = data.get('features', [])
            if features:
                print(f"    Property keys: {list(features[0]['properties'].keys())}")
            cursor.execute("DROP TABLE IF EXISTS subcuencas")
            cursor.execute("""
                CREATE TABLE subcuencas (
                    nombre TEXT, cod_subc TEXT, nom_cuen TEXT,
                    lat_centroid REAL, lon_centroid REAL
                )
            """)
            count = 0
            for f in features:
                props = f.get('properties', {})
                geom = f.get('geometry')
                if not geom:
                    continue
                lat, lon = get_centroid(geom)
                nombre = get_prop(props, 'NOM_SUBC', 'NOMBRE', 'nombre')
                cod_subc = get_prop(props, 'COD_SUBC', 'cod_subc', 'CODSUBC')
                nom_cuen = get_prop(props, 'NOM_CUEN', 'nom_cuen')
                cursor.execute("INSERT INTO subcuencas VALUES (?,?,?,?,?)",
                               (nombre, cod_subc, nom_cuen, lat, lon))
                count += 1
            print(f"    subcuencas: {count} rows")
    except Exception as e:
        print(f"    ERROR processing subcuencas: {e}")
        traceback.print_exc()

    # --- Reserva de Caudales ---
    print("\n  Processing reserva_caudales...")
    try:
        data = load_geojson("DGA__Reserva_de_Caudales__Reserva_de_Caudales.geojson")
        if data:
            features = data.get('features', [])
            if features:
                print(f"    Property keys: {list(features[0]['properties'].keys())}")
            cursor.execute("DROP TABLE IF EXISTS reserva_caudales")
            cursor.execute("""
                CREATE TABLE reserva_caudales (
                    nombre TEXT, region TEXT, lat_centroid REAL, lon_centroid REAL
                )
            """)
            count = 0
            for f in features:
                props = f.get('properties', {})
                geom = f.get('geometry')
                if not geom:
                    continue
                lat, lon = get_centroid(geom)
                nombre = get_prop(props, 'NOMBRE', 'NOM_RESERV', 'nombre', 'Nombre')
                region = get_prop(props, 'REGION', 'NOM_REGION', 'region', 'REGIONES')
                cursor.execute("INSERT INTO reserva_caudales VALUES (?,?,?,?)",
                               (nombre, region, lat, lon))
                count += 1
            print(f"    reserva_caudales: {count} rows")
    except Exception as e:
        print(f"    ERROR processing reserva_caudales: {e}")
        traceback.print_exc()

    # --- Indice de Calidad de Aguas ---
    print("\n  Processing indice_calidad_agua...")
    try:
        data = load_geojson("DGA__Indice_de_Calidad_de_Aguas__ICA_Indice_Calidad_Aguas.geojson")
        if data:
            features = data.get('features', [])
            if features:
                print(f"    Property keys: {list(features[0]['properties'].keys())}")
            cursor.execute("DROP TABLE IF EXISTS indice_calidad_agua")
            cursor.execute("""
                CREATE TABLE indice_calidad_agua (
                    ica TEXT, region TEXT, lat_centroid REAL, lon_centroid REAL
                )
            """)
            count = 0
            for f in features:
                props = f.get('properties', {})
                geom = f.get('geometry')
                if not geom:
                    continue
                lat, lon = get_centroid(geom)
                ica = get_prop(props, 'ICA', 'ica', 'Ica', 'NOMBRE')
                region = get_prop(props, 'REGION', 'NOM_REGION', 'region', 'REGIONES')
                cursor.execute("INSERT INTO indice_calidad_agua VALUES (?,?,?,?)",
                               (ica, region, lat, lon))
                count += 1
            print(f"    indice_calidad_agua: {count} rows")
    except Exception as e:
        print(f"    ERROR processing indice_calidad_agua: {e}")
        traceback.print_exc()

    conn.commit()
    conn.close()
    print("\n  DONE: geospatial.db created successfully.")


# =============================================================================
# TASK 3: CONAF land use summaries
# =============================================================================

def process_conaf():
    """Process CONAF shapefiles into uso_suelo_comuna table in geospatial.db."""
    print("\n" + "=" * 60)
    print("TASK 3: Processing CONAF land use data")
    print("=" * 60)

    try:
        import shapefile
    except ImportError:
        print("  ERROR: pyshp not installed. Run: pip install pyshp")
        return

    if not CONAF_DIR.exists():
        print(f"  ERROR: CONAF directory not found at {CONAF_DIR}")
        return

    # Region folders to process
    region_folders = {
        '05': '05__regi_n_de_valpara_so_actualizaci_n_2019_v2',
        '06': '06__regi_n_de_ohiggins_actualizaci_n_2020',
        '07': '07__regi_n_del_maule_actualizaci_n_2024',
        '13': '13__regi_n_metropolitana_actualizaci_n_2019',
    }

    conn = sqlite3.connect(str(GEOSPATIAL_DB))
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS uso_suelo_comuna")
    cursor.execute("""
        CREATE TABLE uso_suelo_comuna (
            comuna TEXT,
            region_code TEXT,
            uso_tierra TEXT,
            superficie_ha REAL,
            num_registros INTEGER
        )
    """)

    total_inserted = 0

    for region_code, folder_name in region_folders.items():
        folder_path = CONAF_DIR / folder_name
        if not folder_path.exists():
            print(f"  WARNING: Folder not found: {folder_name}")
            continue

        print(f"\n  Processing region {region_code}: {folder_name}")

        # Find .shp files in this folder
        shp_files = list(folder_path.rglob("*.shp"))
        if not shp_files:
            print(f"    No .shp files found in {folder_name}")
            continue

        for shp_path in shp_files:
            print(f"    Reading: {shp_path.name}")
            try:
                sf = shapefile.Reader(str(shp_path), encoding='latin-1')
                fields = [f[0] for f in sf.fields[1:]]  # skip DeletionFlag
                print(f"    Fields: {fields}")

                # Find field indices
                nom_com_idx = None
                uso_tierra_idx = None
                superf_ha_idx = None

                for i, fname in enumerate(fields):
                    fl = fname.upper()
                    if fl in ('NOM_COM', 'NOM_COMUNA', 'NOMBRE_COM'):
                        nom_com_idx = i
                    elif fl in ('USO_TIERRA', 'USO', 'USO_SUELO'):
                        uso_tierra_idx = i
                    elif fl in ('SUPERF_HA', 'SUP_HA', 'SUPERFICIE', 'AREA_HA'):
                        superf_ha_idx = i

                if nom_com_idx is None or uso_tierra_idx is None:
                    print(f"    WARNING: Could not find NOM_COM or USO_TIERRA fields, skipping")
                    continue

                # Aggregate by (comuna, uso_tierra)
                aggregation = {}
                records = sf.records()
                for rec in records:
                    comuna = str(rec[nom_com_idx]).strip() if rec[nom_com_idx] else 'DESCONOCIDO'
                    uso = str(rec[uso_tierra_idx]).strip() if rec[uso_tierra_idx] else 'SIN CLASIFICAR'
                    sup = 0.0
                    if superf_ha_idx is not None:
                        try:
                            sup = float(rec[superf_ha_idx]) if rec[superf_ha_idx] else 0.0
                        except (ValueError, TypeError):
                            sup = 0.0

                    key = (comuna, uso)
                    if key not in aggregation:
                        aggregation[key] = {'superficie_ha': 0.0, 'num_registros': 0}
                    aggregation[key]['superficie_ha'] += sup
                    aggregation[key]['num_registros'] += 1

                # Insert
                count = 0
                for (comuna, uso), data in aggregation.items():
                    cursor.execute(
                        "INSERT INTO uso_suelo_comuna VALUES (?,?,?,?,?)",
                        (comuna, region_code, uso, round(data['superficie_ha'], 2), data['num_registros'])
                    )
                    count += 1
                total_inserted += count
                print(f"    Inserted {count} aggregated rows from {shp_path.name}")
                print(f"    Total records read: {len(records)}")

            except Exception as e:
                print(f"    ERROR reading {shp_path.name}: {e}")
                traceback.print_exc()

    conn.commit()
    conn.close()
    print(f"\n  DONE: uso_suelo_comuna total: {total_inserted} rows")


# =============================================================================
# SUMMARY
# =============================================================================

def print_summary():
    """Print summary stats for all databases."""
    print("\n" + "=" * 60)
    print("SUMMARY: Row counts for all tables")
    print("=" * 60)

    for db_name, db_path in [("electrico.db", ELECTRICO_DB), ("geospatial.db", GEOSPATIAL_DB)]:
        if not db_path.exists():
            print(f"\n  {db_name}: NOT FOUND")
            continue

        print(f"\n  {db_name}:")
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = cursor.fetchall()
        for (table_name,) in tables:
            cursor.execute(f"SELECT COUNT(*) FROM [{table_name}]")
            count = cursor.fetchone()[0]
            print(f"    {table_name}: {count:,} rows")
        conn.close()


# =============================================================================
# MAIN
# =============================================================================

if __name__ == '__main__':
    print("=" * 60)
    print("Agroclimatico Data Processing Script")
    print("=" * 60)

    process_electrico()
    process_geospatial()
    process_conaf()
    print_summary()

    print("\n\nAll tasks completed!")
