#!/usr/bin/env python3
"""
auto_data.py — Carga automática de datos para informes agroclimáticos.
Lee directamente de los archivos scrapeados en /Scrapping Estado/.
Input: nombre de comuna → Output: dict completo listo para engine_v3+.
"""

import csv, json, os, math, re
from collections import defaultdict
from datetime import datetime

# ═══════════════════════════════════════════════════════════════
# PATHS (ajustar si cambia la estructura)
# ═══════════════════════════════════════════════════════════════

BASE = os.path.dirname(os.path.abspath(__file__))
DATOS_GEO = os.path.join(BASE, "datos_geo")
DATOS_PRECIP = os.path.join(BASE, "datos_precipitacion")
DATOS_AGRO = os.path.join(BASE, "BD_agro")
DATOS_SII = os.path.join(BASE, "Data SII")

CATASTRO_DIR = os.path.join(DATOS_GEO, "CATASTRO_FRUTICOLA")
DGA_DIR = os.path.join(DATOS_GEO, "DGA")
ELECTRICO_CSV = os.path.join(DATOS_GEO, "_db", "electrico_por_comuna.csv")
CR2MET_CACHE = os.path.join(DATOS_PRECIP, "cr2met_tmin_heladas_cache.json")
DGA_ESTACIONES = os.path.join(DATOS_PRECIP, "dga_estaciones_consolidado.json")
ONI_CSV = os.path.join(DATOS_PRECIP, "indice_oni_enso.csv")
PDO_CSV = os.path.join(DATOS_PRECIP, "indice_pdo.csv")
SOI_CSV = os.path.join(DATOS_PRECIP, "indice_soi.csv")
ALERTAS_JSON = os.path.join(DATOS_PRECIP, "alertas_agroclimaticas_2026.json")

# Nuevas fuentes no-frutícolas
ODEPA_DIR = os.path.join(BASE, "datos_descargados", "ODEPA")
PRECIOS_MAYORISTAS_CSV = os.path.join(ODEPA_DIR, "precios_mayoristas", "precio_mayorista_fruta-hortaliza_2024.csv")
PRECIOS_UVA_DIR = os.path.join(ODEPA_DIR, "precios_uva_vinificacion")


# ═══════════════════════════════════════════════════════════════
# UTILIDADES
# ═══════════════════════════════════════════════════════════════

def _normalize(s):
    """Normaliza nombre de comuna para matching."""
    s = s.strip().lower()
    s = s.replace("á","a").replace("é","e").replace("í","i").replace("ó","o").replace("ú","u").replace("ñ","n")
    return s

def _parse_float(s):
    """Parsea float desde string con coma decimal."""
    if not s or s == "—" or s == "-": return 0.0
    return float(str(s).replace(",",".").strip())

def _haversine(lat1, lon1, lat2, lon2):
    """Distancia en km entre dos puntos."""
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.asin(math.sqrt(a))


# ═══════════════════════════════════════════════════════════════
# 1. CATASTRO FRUTÍCOLA (CIREN)
# ═══════════════════════════════════════════════════════════════

def load_catastro_fruticola(comuna):
    """Carga catastro frutícola para una comuna. Retorna dict con especies, riego, etc."""
    norm = _normalize(comuna)

    # Search ALL year files (newest first) — some years only cover certain regions
    files = sorted([f for f in os.listdir(CATASTRO_DIR) if f.startswith("catastro_fruticola_20") and f.endswith(".csv")], reverse=True)
    if not files:
        return None

    records = []
    source_year = None
    for fname in files:
        path = os.path.join(CATASTRO_DIR, fname)
        with open(path, encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            found = [row for row in reader if _normalize(row.get("Comuna","")) == norm]
        if found:
            records = found
            source_year = fname.replace("catastro_fruticola_","").replace(".csv","")
            break  # use most recent year that has data

    if not records:
        return None
    print(f"  [OK] Catastro frutícola: {len(records)} registros (año {source_year})")

    # Aggregate by species
    especies = defaultdict(lambda: {"superficie": 0.0, "n_expl": set(), "arboles": 0, "variedades": set()})
    riego_metodos = defaultdict(float)
    anios_plant = []
    total_sup = 0.0

    for r in records:
        esp = r.get("Especie","Otro").strip()
        sup = _parse_float(r.get("Superficie (ha)","0"))
        total_sup += sup
        especies[esp]["superficie"] += sup
        especies[esp]["n_expl"].add(r.get("Número explotación",""))
        especies[esp]["arboles"] += int(_parse_float(r.get("Numero de arboles","0")))
        var = r.get("Variedad","").strip()
        if var:
            especies[esp]["variedades"].add(var)

        metodo = r.get("Metodo de riego","Sin info").strip()
        riego_metodos[metodo] += sup

        anio = r.get("Anio plantacion","")
        if anio and anio.isdigit():
            anios_plant.append(int(anio))

    # Sort by surface
    esp_sorted = sorted(especies.items(), key=lambda x: x[1]["superficie"], reverse=True)

    # Build output
    especies_principales = []
    for nombre, data in esp_sorted[:7]:
        pct = round(data["superficie"] / total_sup * 100, 1) if total_sup > 0 else 0
        especies_principales.append({
            "nombre": nombre,
            "superficie": round(data["superficie"]),
            "n_expl": len(data["n_expl"]),
            "pct": pct,
            "pct_nacional": None,  # requires national aggregation
            "señal": "Consolidado" if pct > 15 else ("En expansión" if pct > 5 else "Nicho"),
            "variedades": sorted(data["variedades"])[:5],
        })

    # Riego breakdown
    riego_list = []
    for metodo, sup in sorted(riego_metodos.items(), key=lambda x: x[1], reverse=True):
        if sup > 0:
            riego_list.append({
                "metodo": metodo,
                "superficie": round(sup),
                "pct": round(sup / total_sup * 100, 1) if total_sup > 0 else 0,
            })

    # Riego tecnificado = goteo + micro aspersión + aspersión
    tec_metodos = {"goteo", "microaspersión", "micro aspersión", "aspersión", "microjet"}
    sup_tec = sum(riego_metodos[m] for m in riego_metodos if _normalize(m) in {_normalize(t) for t in tec_metodos})
    riego_tec_pct = round(sup_tec / total_sup * 100, 1) if total_sup > 0 else 0

    goteo_sup = sum(riego_metodos[m] for m in riego_metodos if "goteo" in _normalize(m))
    micro_sup = sum(riego_metodos[m] for m in riego_metodos if "micro" in _normalize(m))

    edad_prom = round(datetime.now().year - sum(anios_plant)/len(anios_plant)) if anios_plant else 12

    return {
        "sup_fruticola": round(total_sup),
        "n_especies": len(especies),
        "n_explotaciones": len(set(r.get("Número explotación","") for r in records)),
        "riego_tecnificado_pct": riego_tec_pct,
        "goteo_pct": round(goteo_sup / total_sup * 100, 1) if total_sup > 0 else 0,
        "microaspersion_pct": round(micro_sup / total_sup * 100, 1) if total_sup > 0 else 0,
        "edad_promedio_plantaciones": edad_prom,
        "especies_principales": especies_principales,
        "riego_metodos": riego_list,
        "year": files[-1].replace("catastro_fruticola_","").replace(".csv",""),
    }


# ═══════════════════════════════════════════════════════════════
# 2. DGA — PATENTES DE AGUA
# ═══════════════════════════════════════════════════════════════

def load_dga_patentes(comuna):
    """Carga patentes de agua DGA para una comuna."""
    norm = _normalize(comuna)

    # Find latest month file
    files = sorted([f for f in os.listdir(DGA_DIR) if f.startswith("patentes_aguas_") and f.endswith(".csv")])
    if not files:
        return None
    latest = os.path.join(DGA_DIR, files[-1])

    records = []
    with open(latest, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if _normalize(row.get("COMUNA","")) == norm:
                records.append(row)

    if not records:
        return None

    total_patentes = len(records)
    monto_total = sum(_parse_float(r.get("MONTO","0")) for r in records)
    saldo_total = sum(_parse_float(r.get("SALDO","0")) for r in records)
    pagado_total = sum(_parse_float(r.get("PAGADO","0")) for r in records)
    morosos = sum(1 for r in records if _parse_float(r.get("SALDO","0")) > 0)
    morosidad_pct = round(morosos / total_patentes * 100, 1) if total_patentes > 0 else 0

    return {
        "patentes_agua": total_patentes,
        "monto_patentes": round(monto_total / 1_000_000, 1),  # MM CLP
        "morosidad_pct": morosidad_pct,
        "saldo_impago_mm": round(saldo_total / 1_000_000, 1),
        "source_file": files[-1],
    }


# ═══════════════════════════════════════════════════════════════
# 3. INFRAESTRUCTURA ELÉCTRICA (SEC)
# ═══════════════════════════════════════════════════════════════

def load_electrico(comuna):
    """Carga datos eléctricos SEC para una comuna."""
    norm = _normalize(comuna)

    if not os.path.exists(ELECTRICO_CSV):
        return None

    target = None
    all_rows = []
    with open(ELECTRICO_CSV, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            all_rows.append(row)
            if _normalize(row.get("nombre_comuna","")) == norm:
                target = row

    if not target:
        return None

    # Calculate regional average
    region = target.get("nombre_region","")
    regional_pots = [_parse_float(r.get("potencia_promedio_kw","0")) for r in all_rows if r.get("nombre_region","") == region and _parse_float(r.get("potencia_promedio_kw","0")) > 0]
    pot_regional = round(sum(regional_pots) / len(regional_pots), 1) if regional_pots else 5.0
    pot_local = round(_parse_float(target.get("potencia_promedio_kw","0")), 1)

    return {
        "distribuidora": "CGE",  # TODO: from detailed data
        "clientes_electricos": int(_parse_float(target.get("total_clientes","0"))),
        "potencia_total_kw": round(_parse_float(target.get("potencia_total_kw","0"))),
        "potencia_promedio": pot_local,
        "potencia_regional": pot_regional,
        "diff_potencia_pct": round((pot_local - pot_regional) / pot_regional * 100) if pot_regional > 0 else 0,
    }


# ═══════════════════════════════════════════════════════════════
# 4. CR2MET — CLIMA (Tmin, heladas, precipitación)
# ═══════════════════════════════════════════════════════════════

def load_cr2met(comuna):
    """Carga datos CR2MET del cache para una comuna."""
    norm = _normalize(comuna)

    if not os.path.exists(CR2MET_CACHE):
        return None

    with open(CR2MET_CACHE, encoding="utf-8") as f:
        data = json.load(f)

    puntos = data.get("puntos", {})

    # Try exact name match first
    for nombre, p in puntos.items():
        if _normalize(nombre) == norm:
            return _parse_cr2met_point(p)

    # Try partial match
    for nombre, p in puntos.items():
        if norm in _normalize(nombre) or _normalize(nombre) in norm:
            return _parse_cr2met_point(p)

    return None

def _parse_cr2met_point(p):
    """Parse a CR2MET cache point into our standard format."""
    meses = p.get("por_mes", [])
    if len(meses) != 12:
        return None

    tmin_media = [round(m["tmin_media_C"], 1) for m in meses]
    tmin_abs = [round(m["tmin_min_abs_C"], 1) for m in meses]
    p_helada = [round(m["prob_helada_mensual"] * 100) for m in meses]
    heladas_anual = round(sum(m["dias_helada_año"] for m in meses))
    tmin_absoluta = round(min(tmin_abs), 1)

    return {
        "lat": p["lat"],
        "lon": p["lon"],
        "alt_m": p.get("alt_m", 0),
        "tmin_media": tmin_media,
        "tmin_abs": tmin_abs,
        "p_helada": p_helada,
        "heladas_anual": heladas_anual,
        "tmin_absoluta": tmin_absoluta,
        "periodo": p.get("periodo", "1991-2020"),
    }


# ═══════════════════════════════════════════════════════════════
# 5. ÍNDICES ENSO (ONI, PDO, SOI)
# ═══════════════════════════════════════════════════════════════

def load_enso_indices():
    """Carga últimos valores de ONI, PDO, SOI."""
    result = {}

    # ONI
    if os.path.exists(ONI_CSV):
        with open(ONI_CSV, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        if rows:
            last = rows[-1]
            result["oni"] = round(_parse_float(last.get("anomalia","0")), 2)
            result["enso_estado"] = last.get("estado_enso", "Neutro")

    # PDO
    if os.path.exists(PDO_CSV):
        with open(PDO_CSV, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        if rows:
            last = rows[-1]
            result["pdo"] = round(_parse_float(last.get("pdo","0")), 2)

    # SOI
    if os.path.exists(SOI_CSV):
        with open(SOI_CSV, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        if rows:
            last = rows[-1]
            result["soi"] = round(_parse_float(last.get("soi","0")), 2)

    return result


# ═══════════════════════════════════════════════════════════════
# 6. DGA ESTACIONES METEOROLÓGICAS (precip, TMY proxy)
# ═══════════════════════════════════════════════════════════════

def load_nearest_station(lat, lon):
    """Encuentra estación DGA más cercana y retorna datos de precipitación."""
    if not os.path.exists(DGA_ESTACIONES):
        return None

    with open(DGA_ESTACIONES, encoding="utf-8") as f:
        stations = json.load(f)

    best = None
    best_dist = float('inf')

    for st in stations:
        # Parse lat from string like "33° 27' 00'' S"
        lat_str = st.get("lat_str","")
        try:
            parts = re.findall(r'(\d+)', lat_str)
            if len(parts) >= 2:
                st_lat = -(float(parts[0]) + float(parts[1])/60 + (float(parts[2])/3600 if len(parts) > 2 else 0))
            else:
                continue
        except:
            continue

        # Lon not in data — estimate from name or skip
        # Use rough longitude matching by region
        dist = abs(st_lat - lat)  # simplified
        if dist < best_dist:
            best_dist = dist
            best = st

    if best:
        return {
            "station_name": best.get("name",""),
            "station_code": best.get("code",""),
            "precip_monthly_mm": best.get("precip_monthly_mm", [0]*12),
            "precip_annual_mm": best.get("precip_annual_mm", 0),
            "period": best.get("period",""),
        }
    return None


# ═══════════════════════════════════════════════════════════════
# 7. ALERTAS AGROCLIMÁTICAS (pre-calculadas)
# ═══════════════════════════════════════════════════════════════

def load_alertas(comuna):
    """Carga alertas agroclimáticas pre-calculadas."""
    norm = _normalize(comuna)

    if not os.path.exists(ALERTAS_JSON):
        return None

    with open(ALERTAS_JSON, encoding="utf-8") as f:
        data = json.load(f)

    # Search in features
    features = data if isinstance(data, list) else data.get("features", data.get("comunas", []))

    for feat in features:
        props = feat.get("properties", feat)
        if _normalize(props.get("comuna","")) == norm:
            return props

    return None


# ═══════════════════════════════════════════════════════════════
# 8. VIÑAS VINÍFERAS (ODEPA precios uva vinificación)
# ═══════════════════════════════════════════════════════════════

def load_vinas_viniferas(comuna, region=None):
    """Carga datos de viñas viníferas para la comuna o región.
    Retorna dict con variedades, precios, y contexto regional."""
    norm = _normalize(comuna)

    if not os.path.exists(PRECIOS_UVA_DIR):
        return None

    # Load all year files (newest first)
    files = sorted([f for f in os.listdir(PRECIOS_UVA_DIR)
                    if f.startswith("precio_uva_vinificacion_") and f.endswith(".csv")], reverse=True)
    if not files:
        return None

    # Determine region name for fallback
    region_norm = _normalize(region) if region else None

    records_comuna = []
    records_region = []

    for fname in files:
        path = os.path.join(PRECIOS_UVA_DIR, fname)
        try:
            with open(path, encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    row_comuna = _normalize(row.get("Comuna", ""))
                    row_region = _normalize(row.get("Region", ""))
                    if row_comuna == norm:
                        records_comuna.append(row)
                    elif region_norm and region_norm in row_region:
                        records_region.append(row)
        except Exception:
            continue

    # Use comuna-level if available, otherwise regional
    if records_comuna:
        records = records_comuna
        nivel = "comunal"
    elif records_region:
        records = records_region
        nivel = "regional"
    else:
        return None

    # Aggregate by varietal
    variedades = defaultdict(lambda: {"precios": [], "comunas": set(), "anios": set()})
    for r in records:
        var = r.get("Variedad", "").strip()
        if not var:
            continue
        precio = _parse_float(r.get("Precio", "0"))
        if precio > 0:
            variedades[var]["precios"].append(precio)
        variedades[var]["comunas"].add(r.get("Comuna", "").strip())
        anio = r.get("Anio", "")
        if anio:
            variedades[var]["anios"].add(anio)

    if not variedades:
        return None

    # Build sorted output
    var_list = []
    for nombre, data in sorted(variedades.items(), key=lambda x: len(x[1]["precios"]), reverse=True):
        precios = data["precios"]
        var_list.append({
            "variedad": nombre,
            "precio_promedio": round(sum(precios) / len(precios)) if precios else 0,
            "precio_min": round(min(precios)) if precios else 0,
            "precio_max": round(max(precios)) if precios else 0,
            "n_registros": len(precios),
            "comunas": sorted(data["comunas"])[:5],
            "anios": sorted(data["anios"]),
        })

    return {
        "vinas_viniferas": var_list[:15],
        "vinas_nivel": nivel,
        "vinas_n_variedades": len(variedades),
        "vinas_n_registros": len(records),
        "vinas_fuente": "ODEPA precios uva vinificación",
    }


# ═══════════════════════════════════════════════════════════════
# 9. HORTALIZAS REGIONALES (ODEPA precios mayoristas)
# ═══════════════════════════════════════════════════════════════

def load_hortalizas_regionales(region):
    """Carga productos hortícolas comercializados en la región.
    Retorna dict con productos top, precios, y volúmenes."""
    if not os.path.exists(PRECIOS_MAYORISTAS_CSV):
        return None

    region_norm = _normalize(region) if region else None
    if not region_norm:
        return None

    productos = defaultdict(lambda: {"precios": [], "volumen_total": 0, "mercados": set(), "variedades": set()})

    try:
        with open(PRECIOS_MAYORISTAS_CSV, encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("Subsector", "") != "Hortalizas y tubérculos":
                    continue
                if region_norm not in _normalize(row.get("Region", "")):
                    continue

                prod = row.get("Producto", "").strip()
                if not prod:
                    continue

                precio = _parse_float(row.get("Precio promedio", "0"))
                volumen = _parse_float(row.get("Volumen", "0"))

                if precio > 0:
                    productos[prod]["precios"].append(precio)
                productos[prod]["volumen_total"] += volumen
                productos[prod]["mercados"].add(row.get("Mercado", "").strip())
                var = row.get("Variedad / Tipo", "").strip()
                if var and var != "Sin especificar":
                    productos[prod]["variedades"].add(var)
    except Exception:
        return None

    if not productos:
        return None

    # Sort by volume (proxy for importance in the region)
    prod_list = []
    for nombre, data in sorted(productos.items(), key=lambda x: x[1]["volumen_total"], reverse=True):
        precios = data["precios"]
        prod_list.append({
            "producto": nombre,
            "precio_promedio": round(sum(precios) / len(precios)) if precios else 0,
            "volumen_total": round(data["volumen_total"]),
            "n_registros": len(precios),
            "mercados": sorted(data["mercados"])[:3],
            "variedades": sorted(data["variedades"])[:5],
        })

    return {
        "hortalizas": prod_list[:20],
        "hortalizas_n_productos": len(productos),
        "hortalizas_nivel": "regional",
        "hortalizas_fuente": "ODEPA precios mayoristas 2024",
    }


# ═══════════════════════════════════════════════════════════════
# 10. CATASTRO FRUTÍCOLA NACIONAL — Top especies país
# ═══════════════════════════════════════════════════════════════

def load_catastro_nacional_top(exclude_especies=None):
    """Carga top 10 especies frutícolas a nivel nacional.
    Excluye las que ya están en el catastro local (para no duplicar)."""
    files = sorted([f for f in os.listdir(CATASTRO_DIR)
                    if f.startswith("catastro_fruticola_20") and f.endswith(".csv")], reverse=True)
    if not files:
        return None

    # Use most recent file
    path = os.path.join(CATASTRO_DIR, files[0])
    especies = defaultdict(lambda: {"superficie": 0.0, "n_expl": set(), "n_comunas": set()})

    try:
        with open(path, encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                esp = row.get("Especie", "").strip()
                sup = _parse_float(row.get("Superficie (ha)", "0"))
                especies[esp]["superficie"] += sup
                especies[esp]["n_expl"].add(row.get("Número explotación", ""))
                especies[esp]["n_comunas"].add(row.get("Comuna", "").strip())
    except Exception:
        return None

    # Normalize exclusion list
    exclude_norm = set()
    if exclude_especies:
        exclude_norm = {_normalize(e) for e in exclude_especies}

    total_nacional = sum(d["superficie"] for d in especies.values())

    nac_list = []
    for nombre, data in sorted(especies.items(), key=lambda x: x[1]["superficie"], reverse=True):
        if _normalize(nombre) in exclude_norm:
            continue
        pct = round(data["superficie"] / total_nacional * 100, 1) if total_nacional > 0 else 0
        nac_list.append({
            "nombre": nombre,
            "superficie_nacional": round(data["superficie"]),
            "pct_nacional": pct,
            "n_comunas": len(data["n_comunas"]),
        })
        if len(nac_list) >= 10:
            break

    return {
        "nacional_top": nac_list,
        "nacional_sup_total": round(total_nacional),
        "nacional_fuente": f"ODEPA catastro frutícola {files[0].replace('catastro_fruticola_','').replace('.csv','')}",
    }


# ═══════════════════════════════════════════════════════════════
# 11. FUNCIÓN PRINCIPAL: auto_load_comuna()
# ═══════════════════════════════════════════════════════════════

# Coordenadas y stats SII por comuna — cargadas desde data/sii_comunas.json
# Generado por scripts/build_sii_data.py desde CR2MET + PVsyst + GPKG SII
_SII_COMUNAS_JSON = os.path.join(BASE, "data", "sii_comunas.json")

def _load_sii_comunas():
    """Carga el catálogo unificado de comunas (coords + stats SII)."""
    if not os.path.exists(_SII_COMUNAS_JSON):
        return {}
    with open(_SII_COMUNAS_JSON, encoding="utf-8") as f:
        raw = json.load(f)
    # Normaliza: key = nombre_normalizado → entry completa
    out = {}
    for nombre, entry in raw.items():
        entry["nombre_oficial"] = nombre
        out[_normalize(nombre)] = entry
    return out

_SII_COMUNAS = _load_sii_comunas()

# Hardcoded overrides para comunas calibradas manualmente (ciudad real, no grid CR2MET)
_COORDS_OVERRIDE = {
    "san clemente": (-35.55, -71.48, 250),
    "talca": (-35.43, -71.67, 102),
    "colbun": (-35.63, -71.47, 320),
    "molina": (-35.10, -71.27, 210),
    "curico": (-34.97, -71.23, 225),
    "maule": (-35.52, -71.70, 110),
    "longavi": (-35.83, -71.70, 240),
    "linares": (-35.85, -71.60, 160),
    "pelarco": (-35.28, -71.89, 220),
    "pencahue": (-35.33, -71.90, 240),
    "san javier": (-35.59, -71.73, 90),
    "yerbas buenas": (-35.75, -71.43, 360),
}

# Dict final: override primero, luego completa con SII/CR2MET/PVsyst
COORDS_COMUNAS = dict(_COORDS_OVERRIDE)
for k, v in _SII_COMUNAS.items():
    if v.get("lat") is not None:
        COORDS_COMUNAS.setdefault(k, (v["lat"], v["lon"], v.get("alt", 200)))


def load_sii_stats(comuna):
    """Retorna estadísticas SII para una comuna (avaluo, predios rurales, etc)."""
    entry = _SII_COMUNAS.get(_normalize(comuna))
    if not entry or not entry.get("total_predios"):
        return None
    return {
        "sii_cut": entry.get("cut"),
        "sii_region_oficial": entry.get("region"),
        "sii_total_predios": entry.get("total_predios", 0),
        "sii_n_agricola": entry.get("n_agricola", 0),
        "sii_n_forestal": entry.get("n_forestal", 0),
        "sii_n_habitacional": entry.get("n_habitacional", 0),
        "sii_n_eriazo": entry.get("n_eriazo", 0),
        "sii_pct_agricola": entry.get("pct_agricola", 0),
        "sii_avaluo_prom_agricola_clp": entry.get("avaluo_prom_agricola_clp", 0),
        "sii_sup_prom_agricola_m2": entry.get("sup_prom_agricola_m2", 0),
        "sii_sup_prom_agricola_ha": round(entry.get("sup_prom_agricola_m2", 0) / 10000, 2),
        "sii_sup_total_agricola_ha": entry.get("sup_total_agricola_ha", 0),
        "sii_valor_total_agricola_mmusd": entry.get("valor_total_agricola_mmusd", 0),
    }


def buscar_predios_cercanos(lat, lon, radio_km=5, max_results=10):
    """Busca predios rurales en el GPKG SII dentro de un radio del punto dado.
    Solo funciona si el GPKG está disponible (local development)."""
    gpkg_path = os.path.join(BASE, "Data SII", "predios_rurales_chile_2025S2.gpkg")
    if not os.path.exists(gpkg_path):
        # Cloud fallback: no predios individuales disponibles
        return []
    try:
        import sqlite3
        conn = sqlite3.connect(gpkg_path)
        c = conn.cursor()
        # Aprox 1 grado lat ≈ 111 km
        delta = radio_km / 111.0
        c.execute("""
            SELECT comuna, manzana, predio, lat, lon, avaluo_total, sup_terreno, destino, direccion_sii
            FROM rurales
            WHERE lat IS NOT NULL AND lat BETWEEN ? AND ?
              AND lon IS NOT NULL AND lon BETWEEN ? AND ?
            LIMIT ?
        """, (lat - delta, lat + delta, lon - delta / 0.8, lon + delta / 0.8, max_results * 3))
        rows = c.fetchall()
        conn.close()
        # Calcular distancia y ordenar
        predios = []
        for r in rows:
            d_km = _haversine(lat, lon, r[3], r[4])
            if d_km <= radio_km:
                predios.append({
                    "rol": f"{r[0]}-{r[1]}-{r[2]}", "lat": r[3], "lon": r[4],
                    "avaluo": r[5], "sup_m2": r[6], "destino": r[7],
                    "direccion": r[8], "dist_km": round(d_km, 2),
                })
        return sorted(predios, key=lambda x: x["dist_km"])[:max_results]
    except Exception:
        return []

def auto_load_comuna(comuna, lat=None, lon=None, altitud=None):
    """
    Carga automática de TODOS los datos para una comuna.
    Retorna dict compatible con engine_v3.

    Args:
        comuna: Nombre de la comuna (e.g., "San Clemente")
        lat, lon, altitud: Coordenadas (opcionales, se buscan en tabla si no se dan)
    """
    norm = _normalize(comuna)

    # Resolve coordinates
    if lat is None or lon is None:
        coords = COORDS_COMUNAS.get(norm, (None, None, None))
        lat, lon, altitud = coords if lat is None else (lat, lon, altitud or coords[2])
    if altitud is None:
        altitud = 200  # default

    print(f"[AUTO] Cargando datos para: {comuna} ({lat}, {lon}, {altitud}m)")

    result = {
        "comuna": comuna,
        "lat": lat, "lon": lon, "altitud": altitud,
        "fecha": datetime.now().strftime("%B %Y").replace("January","Enero").replace("February","Febrero").replace("March","Marzo").replace("April","Abril").replace("May","Mayo").replace("June","Junio").replace("July","Julio").replace("August","Agosto").replace("September","Septiembre").replace("October","Octubre").replace("November","Noviembre").replace("December","Diciembre"),
    }

    # 1. Catastro Frutícola
    cat = load_catastro_fruticola(comuna)
    if cat:
        result.update(cat)
        print(f"  [OK] Catastro: {cat['sup_fruticola']} ha, {cat['n_especies']} especies ({cat['year']})")
    else:
        print(f"  [WARN] Catastro frutícola no encontrado para {comuna}")
        result.update({"sup_fruticola": 0, "n_especies": 0, "n_explotaciones": 0,
                       "riego_tecnificado_pct": 0, "goteo_pct": 0, "microaspersion_pct": 0,
                       "edad_promedio_plantaciones": 0, "especies_principales": [], "riego_metodos": []})

    # 2. DGA Patentes
    dga = load_dga_patentes(comuna)
    if dga:
        result.update(dga)
        print(f"  [OK] DGA: {dga['patentes_agua']} patentes, morosidad {dga['morosidad_pct']}%")
    else:
        print(f"  [WARN] DGA patentes no encontradas para {comuna}")
        result.update({"patentes_agua": 0, "monto_patentes": 0, "morosidad_pct": 0})

    # 3. Eléctrico
    elec = load_electrico(comuna)
    if elec:
        result.update(elec)
        print(f"  [OK] Eléctrico: {elec['clientes_electricos']} clientes, {elec['potencia_promedio']} kW/cl")
    else:
        print(f"  [WARN] Datos eléctricos no encontrados para {comuna}")
        result.update({"distribuidora": "Sin datos", "clientes_electricos": 0,
                       "potencia_total_kw": 0, "potencia_promedio": 0, "potencia_regional": 0, "diff_potencia_pct": 0})

    # 4. CR2MET
    cr2 = load_cr2met(comuna)
    if cr2:
        result["cr2met"] = cr2
        print(f"  [OK] CR2MET: {cr2['heladas_anual']} heladas/año, Tmin abs {cr2['tmin_absoluta']}°C")
    else:
        print(f"  [WARN] CR2MET no encontrado para {comuna}")

    # 5. ENSO
    enso = load_enso_indices()
    if enso:
        result.update(enso)
        print(f"  [OK] ENSO: ONI={enso.get('oni','?')}, PDO={enso.get('pdo','?')}, SOI={enso.get('soi','?')}")

    # 6. Alertas
    alerta = load_alertas(comuna)
    if alerta:
        result["alerta_agro"] = alerta
        print(f"  [OK] Alerta: riesgo helada={alerta.get('riesgo_helada','?')}")

    # 7. Viñas viníferas (ODEPA)
    region_name = result.get("region", "Región del Maule")  # fallback
    vinas = load_vinas_viniferas(comuna, region=region_name)
    if vinas:
        result.update(vinas)
        print(f"  [OK] Viñas viníferas: {vinas['vinas_n_variedades']} variedades ({vinas['vinas_nivel']})")
    else:
        result.update({"vinas_viniferas": [], "vinas_nivel": "sin_datos", "vinas_n_variedades": 0})

    # 8. Hortalizas regionales (ODEPA)
    hort = load_hortalizas_regionales(region_name)
    if hort:
        result.update(hort)
        print(f"  [OK] Hortalizas: {hort['hortalizas_n_productos']} productos ({hort['hortalizas_nivel']})")
    else:
        result.update({"hortalizas": [], "hortalizas_n_productos": 0})

    # 9. Catastro nacional top (excluye especies locales para no duplicar)
    local_especies = [e["nombre"] for e in result.get("especies_principales", [])]
    nac = load_catastro_nacional_top(exclude_especies=local_especies)
    if nac:
        result.update(nac)
        print(f"  [OK] Nacional top: {len(nac['nacional_top'])} especies (excl. {len(local_especies)} locales)")
    else:
        result.update({"nacional_top": [], "nacional_sup_total": 0})

    # 10. SII — estadísticas prediales por comuna (avalúo, N° predios, % agrícola)
    sii = load_sii_stats(comuna)
    if sii:
        result.update(sii)
        # NO sobrescribir region — _add_derived_fields lo resuelve con formato Chile ("del Maule")
        print(f"  [OK] SII: {sii['sii_total_predios']} predios, {sii['sii_pct_agricola']}% agrícola, "
              f"avalúo prom ${sii['sii_avaluo_prom_agricola_clp']:,.0f}")
    else:
        result.update({"sii_total_predios": 0, "sii_n_agricola": 0, "sii_pct_agricola": 0,
                       "sii_avaluo_prom_agricola_clp": 0, "sii_sup_prom_agricola_ha": 0})

    # 11. SII predios cercanos (point-level) — solo si tenemos lat/lon
    if lat and lon:
        predios_cerca = buscar_predios_cercanos(lat, lon, radio_km=5, max_results=5)
        if predios_cerca:
            result["sii_predios_cercanos"] = predios_cerca
            print(f"  [OK] Predios cercanos SII: {len(predios_cerca)} dentro de 5km")

    # 12. Derived/estimated fields (from CR2MET + DGA station)
    _add_derived_fields(result)

    return result


def _add_derived_fields(d):
    """Calcula campos derivados que necesita el engine."""
    cr2 = d.get("cr2met", {})

    # Region/Provincia — preserve hardcoded Maule formatting, map SII for rest
    region_map = {
        "san clemente": ("del Maule", "Talca"),
        "talca": ("del Maule", "Talca"),
        "molina": ("del Maule", "Curicó"),
        "curico": ("del Maule", "Curicó"),
    }

    # Mapping SII region -> format "de/del X" (convención chilena oficial)
    SII_REGION_FORMAT = {
        "Arica y Parinacota": "de Arica y Parinacota",
        "Tarapacá": "de Tarapacá",
        "Antofagasta": "de Antofagasta",
        "Atacama": "de Atacama",
        "Coquimbo": "de Coquimbo",
        "Valparaíso": "de Valparaíso",
        "Metropolitana": "Metropolitana",
        "O'Higgins": "del Libertador Gral. Bernardo O'Higgins",
        "Maule": "del Maule",
        "Ñuble": "de Ñuble",
        "Biobío": "del Biobío",
        "La Araucanía": "de La Araucanía",
        "Los Ríos": "de Los Ríos",
        "Los Lagos": "de Los Lagos",
        "Aysén": "de Aysén del Gral. Carlos Ibáñez del Campo",
        "Magallanes": "de Magallanes y la Antártica Chilena",
    }

    norm = _normalize(d.get("comuna",""))
    if norm in region_map:
        reg_info = region_map[norm]
        d.setdefault("region", reg_info[0])
        d.setdefault("provincia", reg_info[1])
    elif d.get("sii_region_oficial"):
        # Usar región oficial del SII, formateada
        region_fmt = SII_REGION_FORMAT.get(d["sii_region_oficial"], d["sii_region_oficial"])
        d.setdefault("region", region_fmt)
        d.setdefault("provincia", "")  # Falta mapeo de provincias a futuro
    else:
        # Fallback genérico
        d.setdefault("region", "del Maule")
        d.setdefault("provincia", "Talca")

    # Distance to coast — lookup table for known communes, else estimate from lat/lon
    DIST_COSTA = {
        "san clemente": 115, "talca": 95, "molina": 100, "curico": 105,
        "colbun": 130, "linares": 105, "rancagua": 90, "chillan": 85,
        "los angeles": 110, "temuco": 80, "valdivia": 20, "osorno": 45,
        "santiago": 100, "rengo": 85, "san fernando": 95,
    }
    d.setdefault("distancia_costa", DIST_COSTA.get(_normalize(d.get("comuna","")),
        round(max(15, abs(abs(d.get("lon",-71.5)) - 71.6) * 110 + 15))))

    # Bioclimatic indices (estimated from CR2MET if available)
    if cr2:
        tmin = cr2.get("tmin_media", [10]*12)
        # Estimate Tmax from Tmin + VARIABLE diurnal range
        # Calibrado con INFODEP San Clemente (Santibáñez 2024) — típico Chile central 33-37°S
        # Verano ~18°C, invierno ~10°C por inversión térmica nocturna
        DIURNAL_RANGE = [18.0, 17.4, 16.2, 15.2, 12.7, 10.6, 10.2, 10.1, 11.4, 14.0, 16.5, 17.9]
        tmax_est = [round(tmin[i] + DIURNAL_RANGE[i], 1) for i in range(12)]
        d.setdefault("tmy", {
            "tmax": tmax_est,
            "tmin": tmin,  # CR2MET Tmin_media = TMY Tmin (validado vs INFODEP)
            "heladas": [round(max(0, cr2["p_helada"][i] / 100 * 30 * 0.4)) for i in range(12)],
            "hrs_frio": _estimate_chill_hours(tmin, tmax_est),
            "precip": [0]*12,  # filled from DGA station below
            "etp": _estimate_etp(tmax_est, d.get("lat", -35)),
        })

        # Winkler (oct-mar)
        winkler = 0
        for m in [9,10,11,0,1,2]:  # oct-mar
            tmean = (tmax_est[m] + tmin[m]) / 2
            if tmean > 10:
                winkler += (tmean - 10) * 30
        d.setdefault("winkler", round(winkler))

        # Hrs frío total
        hrs_frio_total = sum(d["tmy"]["hrs_frio"])
        d["tmy_anual"] = d.get("tmy_anual", {})
        d["tmy_anual"]["hrs_frio"] = hrs_frio_total
        d["tmy_anual"]["heladas"] = cr2["heladas_anual"]
        d["tmy_anual"]["precip"] = sum(d["tmy"]["precip"])
        d["tmy_anual"]["etp"] = sum(d["tmy"]["etp"])
        d["tmy_anual"]["tmax_media"] = round(sum(tmax_est)/12, 1)
        d["tmy_anual"]["tmin_media"] = round(sum(d["tmy"]["tmin"])/12, 1)

        d.setdefault("plh", round(365 - cr2["heladas_anual"] * 12))  # rough PLH
        # Días cálidos: distribución normal de Tmax diario (σ≈3°C, calibrado INFODEP)
        def _normal_cdf(x):
            """Approximation of cumulative normal distribution."""
            t = 1 / (1 + 0.2316419 * abs(x))
            d = 0.3989422802 * math.exp(-x*x/2)
            p = d*t*(0.3193815 + t*(-0.3565638 + t*(1.781478 + t*(-1.821256 + t*1.330274))))
            return 1-p if x > 0 else p
        sigma_tmax = 3.0  # desv. estándar típica Tmax diario Chile central
        dias_25 = sum(round(30 * _normal_cdf((t - 25) / sigma_tmax)) for t in tmax_est)
        dias_30 = sum(round(30 * _normal_cdf((t - 30) / sigma_tmax)) for t in tmax_est)
        dias_32 = sum(round(30 * _normal_cdf((t - 32) / sigma_tmax)) for t in tmax_est)
        d.setdefault("dias_calidos_25", dias_25)
        d.setdefault("dias_calidos_30", dias_30)
        d.setdefault("dias_calidos_32", dias_32)

        # Días grado: sum(max(0, Tmean-10)*30) para todos los meses
        dg_anual = sum(max(0, (tmax_est[i] + tmin[i])/2 - 10) * 30 for i in range(12))
        d.setdefault("dias_grado_anual", round(dg_anual))

        # Fototérmico INFODEP: combina radiación, días cálidos madurez, frescor nocturno
        # Simplificado: (Winkler/10) * (dias_calidos_25 / 100) * (1 si Tmin nocturna 10-11°C)
        wk = d.get("winkler", 1200)
        frescor = 1.0 if 9 <= tmin[1] <= 12 else 0.8  # feb Tmin como proxy frescor
        d.setdefault("fototermico", round(wk / 10 * max(0.3, min(1.2, dias_25 / 100)) * frescor))
        d.setdefault("radiacion_verano", 550)  # cal/cm2/dia estimate for central Chile
        d.setdefault("hr_verano", 55)
        d.setdefault("precip_floracion_sep", round(d["tmy"]["precip"][8], 1))
        d.setdefault("precip_cosecha_dic_ene", round(d["tmy"]["precip"][11] + d["tmy"]["precip"][0], 1))
        d.setdefault("precip_cosecha_mar_abr", round(d["tmy"]["precip"][2] + d["tmy"]["precip"][3], 1))
        d.setdefault("precip_cosecha_abr_may", round(d["tmy"]["precip"][3] + d["tmy"]["precip"][4], 1))
        d.setdefault("p99_diario", 88)
        d.setdefault("max_diario", 114)
        d.setdefault("dias_lluvia", 62)

    # CR2MET precipitation (use DGA station if available)
    station = load_nearest_station(d.get("lat", -35.5), d.get("lon", -71.5))
    if station and station["precip_annual_mm"] > 50:
        cr2.setdefault("precip_anual", round(station["precip_annual_mm"]))
        # Update TMY precip from station
        if "tmy" in d and station["precip_monthly_mm"]:
            d["tmy"]["precip"] = [round(v, 1) for v in station["precip_monthly_mm"]]
            d["tmy_anual"]["precip"] = round(station["precip_annual_mm"])

    # CR2MET historical precip trends (estimated)
    precip_an = cr2.get("precip_anual", d.get("tmy_anual", {}).get("precip", 500))
    cr2.setdefault("precip_anual", precip_an)
    cr2.setdefault("precip_1991_2005", round(precip_an * 1.15))  # pre-megasequía ~15% higher
    cr2.setdefault("precip_2006_2020", precip_an)
    cr2.setdefault("delta_precip_pct", round((precip_an - cr2["precip_1991_2005"]) / cr2["precip_1991_2005"] * 100, 1))

    # Defaults for fields the engine needs
    d.setdefault("zona_restriccion", False)
    d.setdefault("zona_agotamiento", False)
    d.setdefault("zona_prohibicion", False)
    d.setdefault("clientes_100kw", round(d.get("clientes_electricos", 0) * 0.002))
    d.setdefault("en_area_protegida", False)
    d.setdefault("en_tierra_indigena", False)
    d.setdefault("requiere_eia", False)
    d.setdefault("tiene_bosque_nativo", True)
    d.setdefault("dist_ruta_principal", 8)
    d.setdefault("dist_centro_urbano", 25)
    d.setdefault("pct_agricola", 10.0)
    d.setdefault("clase_suelo", "IV")
    d.setdefault("sup_total_comuna", round(d.get("sup_fruticola", 1000) * 35))  # rough estimate
    d.setdefault("n_riesgos_criticos", 1 if d.get("morosidad_pct", 0) > 25 else 0)
    d.setdefault("n_riesgos_moderados", 1)

    # Riesgos
    riesgos = []
    if d.get("morosidad_pct", 0) > 25:
        riesgos.append({"tipo": "Morosidad patentes agua", "nivel": "CRÍTICO",
                        "detalle": f"{d['morosidad_pct']}% — más del doble promedio nacional (~15%)"})
    riesgos.append({"tipo": "Riesgo incendio forestal", "nivel": "MODERADO",
                    "detalle": "Cobertura boscosa, zona de interfaz"})
    d.setdefault("riesgos", riesgos)

    # Uso suelo placeholder (will be improved with CONAF shapefile reader)
    d.setdefault("uso_suelo", [
        {"uso": "Áreas Desprovistas de Vegetación", "superficie": round(d.get("sup_total_comuna",450000)*0.41), "pct": 41.4},
        {"uso": "Bosques", "superficie": round(d.get("sup_total_comuna",450000)*0.23), "pct": 23.2},
        {"uso": "Praderas y Matorrales", "superficie": round(d.get("sup_total_comuna",450000)*0.19), "pct": 18.8},
        {"uso": "Terrenos Agrícolas", "superficie": round(d.get("sup_total_comuna",450000)*0.11), "pct": 10.8},
        {"uso": "Nieves Eternas y Glaciares", "superficie": round(d.get("sup_total_comuna",450000)*0.03), "pct": 3.3},
        {"uso": "Cuerpos de Agua", "superficie": round(d.get("sup_total_comuna",450000)*0.02), "pct": 2.3},
        {"uso": "Áreas Urbanas e Industriales", "superficie": round(d.get("sup_total_comuna",450000)*0.002), "pct": 0.2},
        {"uso": "Humedales", "superficie": round(d.get("sup_total_comuna",450000)*0.0003), "pct": 0.0},
    ])

    # Infraestructura hídrica placeholder
    d.setdefault("infraestructura_hidrica", [])

    # Comunas vecinas placeholder
    d.setdefault("n_comunas_vecinas", 6)
    d.setdefault("comunas_vecinas", [])


def _estimate_chill_hours(tmin_media, tmax_est=None):
    """Estima horas frío mensuales (<7°C) usando modelo sinusoidal de temperatura.
    Calibrado contra INFODEP San Clemente: target ~1555 hrs totales.
    Si tmax_est no se provee, usa rango diurno por defecto.
    """
    DIURNAL_RANGE = [18.0, 17.4, 16.2, 15.2, 12.7, 10.6, 10.2, 10.1, 11.4, 14.0, 16.5, 17.9]
    THRESH = 7.0  # umbral horas frío
    hrs = []
    for i, tmin in enumerate(tmin_media):
        tmax = tmax_est[i] if tmax_est else tmin + DIURNAL_RANGE[i]
        tmean = (tmax + tmin) / 2
        amp = (tmax - tmin) / 2
        if amp < 0.1:
            # Edge case: no amplitude
            hrs.append(744 if tmean < THRESH else 0)
            continue
        # Fraction of day below threshold using sinusoidal model
        ratio = (THRESH - tmean) / amp
        if ratio >= 1:
            hrs.append(round(30 * 24))  # all hours below threshold
        elif ratio <= -1:
            hrs.append(0)  # all hours above threshold
        else:
            # acos(-ratio)/π = fraction of cosine cycle below threshold
            frac_below = math.acos(-ratio) / math.pi
            hrs_below = frac_below * 24 * 30
            hrs.append(round(max(0, hrs_below)))
    return hrs

def _estimate_etp(tmax, lat):
    """Estima ETP mensual con Hargreaves simplificado."""
    # Rough Hargreaves: ETP = 0.0023 * Ra * (Tmean + 17.8) * (Tmax - Tmin)^0.5
    # Simplified: use latitude for extraterrestrial radiation proxy
    etp = []
    ra_proxy = [15, 13, 10, 7, 5, 3, 3.5, 5, 7, 10, 13, 15]  # relative solar radiation by month
    for i, t in enumerate(tmax):
        tmean = t - 6.5  # rough
        etp_val = ra_proxy[i] * max(0, tmean) * 1.0  # simplified
        etp.append(round(max(10, etp_val), 1))
    return etp


# ═══════════════════════════════════════════════════════════════
# TEST
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    d = auto_load_comuna("San Clemente")
    print("\n" + "="*60)
    print("RESUMEN AUTO_DATA — SAN CLEMENTE")
    print("="*60)
    print(f"Sup. frutícola: {d.get('sup_fruticola')} ha")
    print(f"N especies: {d.get('n_especies')}")
    print(f"Patentes agua: {d.get('patentes_agua')}")
    print(f"Morosidad: {d.get('morosidad_pct')}%")
    print(f"Clientes eléctricos: {d.get('clientes_electricos')}")
    print(f"Potencia promedio: {d.get('potencia_promedio')} kW/cl")
    cr2 = d.get("cr2met", {})
    print(f"CR2MET heladas/año: {cr2.get('heladas_anual')}")
    print(f"CR2MET Tmin abs: {cr2.get('tmin_absoluta')}°C")
    print(f"ENSO: ONI={d.get('oni')}, PDO={d.get('pdo')}, SOI={d.get('soi')}")
    print(f"Especies top-3: {[e['nombre'] for e in d.get('especies_principales',[])[:3]]}")
