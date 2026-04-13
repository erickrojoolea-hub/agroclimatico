"""
predial_engine.py -- Motor de Informes Prediales por comuna (Autofact-style).

Consulta SQLite databases:
  - catastro_fruticola.db  (SAG/CIREN -- produccion fruticola)
  - dga_derechos.db        (DGA -- derechos de agua / patentes)
  - electrico.db           (SEC -- infraestructura electrica)
  - geospatial.db          (geo -- embalses, estaciones, restricciones, etc.)

Genera un informe de 8 secciones con semaforos, texto analitico y alertas.
"""

import math
import os
import sqlite3
import logging
import unicodedata
from typing import Optional

try:
    from geo_engine import (
        analisis_sitio as _geo_analisis_sitio,
        get_restriccion_hidrica as _geo_restriccion,
        get_agotamiento as _geo_agotamiento,
        get_reserva_caudales as _geo_reserva,
        get_embalses_cercanos as _geo_embalses,
        get_estaciones_cercanas as _geo_estaciones,
        get_cuenca as _geo_cuenca,
    )
    _HAS_GEO_ENGINE = True
except ImportError:
    _HAS_GEO_ENGINE = False

logger = logging.getLogger(__name__)

_BASE = os.path.dirname(os.path.abspath(__file__))
_DB_DIR = os.path.join(_BASE, "data", "db")

_DB_FILES = {
    "catastro": os.path.join(_DB_DIR, "catastro_fruticola.db"),
    "dga": os.path.join(_DB_DIR, "dga_derechos.db"),
    "electrico": os.path.join(_DB_DIR, "electrico.db"),
    "geospatial": os.path.join(_DB_DIR, "geospatial.db"),
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# Alias table: nombres donde la normalizacion de acentos NO basta
# (cambio de letra real, typos en las bases de datos, etc.)
_COMUNA_ALIASES = {
    # Marchigüe (Nominatim/oficial) → Marchihue (catastro/SEC)
    "marchigue": "marchihue",
    # Doñihue (correct) → Doñiihue (typo in catastro_fruticola.db)
    "donihue": "doniihue",
    "doñihue": "doñiihue",
}


def normalize_comuna(name: str) -> str:
    """Remove accents, dieresis and normalize for search.
    Uses combining character removal to properly handle ü, ñ, etc.
    """
    if not name:
        return ""
    nfkd = unicodedata.normalize("NFKD", name)
    # Remove ALL combining marks (accents, dieresis, tilde on ñ, etc.)
    stripped = "".join(c for c in nfkd if not unicodedata.combining(c))
    # Fallback: force ASCII for any remaining non-ASCII chars
    stripped = stripped.encode("ASCII", "ignore").decode("ASCII")
    return stripped.strip()


def _resolve_comuna(name: str) -> str:
    """Resolve a comuna name through alias table + normalization.
    Returns the best candidate name for DB lookups.
    """
    if not name:
        return ""
    # Direct alias (case-insensitive)
    lower = name.lower().strip()
    if lower in _COMUNA_ALIASES:
        return _COMUNA_ALIASES[lower]
    # Normalize, then check alias
    norm = normalize_comuna(name).lower()
    if norm in _COMUNA_ALIASES:
        return _COMUNA_ALIASES[norm]
    return normalize_comuna(name)


def _fix_encoding(text: str) -> str:
    """Fix broken UTF-8 encoding (text read as latin-1 instead of utf-8)."""
    if not text:
        return text or ""
    try:
        return text.encode("latin-1").decode("utf-8")
    except (UnicodeDecodeError, UnicodeEncodeError):
        return text


def _comuna_variants(comuna: str) -> list[str]:
    """Return ordered unique list of comuna name variants to try in SQL."""
    norm = normalize_comuna(comuna)
    resolved = _resolve_comuna(comuna)
    raw = [
        comuna, resolved, resolved.title(), resolved.upper(),
        norm, norm.upper(), norm.title(), norm.lower(),
        comuna.upper(), comuna.title(), comuna.lower(),
    ]
    seen = set()
    out = []
    for v in raw:
        if v and v not in seen:
            seen.add(v)
            out.append(v)
    return out


def _connect(db_key: str) -> Optional[sqlite3.Connection]:
    path = _DB_FILES.get(db_key)
    if not path or not os.path.isfile(path):
        return None
    try:
        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.Error:
        return None


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    """Check if a table exists in the database."""
    try:
        row = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        ).fetchone()
        return row[0] > 0
    except Exception:
        return False


def _search_comuna_variants(conn: sqlite3.Connection, col: str, table: str,
                            comuna: str, extra_cols: str = "*",
                            where_extra: str = "",
                            limit: int = 0) -> list:
    """Try multiple normalizations + alias resolution to find a comuna."""
    norm = normalize_comuna(comuna)
    resolved = _resolve_comuna(comuna)

    # Build unique ordered list of variants to try
    raw_variants = [
        comuna,                   # Original: "Marchigüe"
        resolved,                 # Alias-resolved: "Marchihue"
        resolved.title(),         # "Marchihue"
        resolved.upper(),         # "MARCHIHUE"
        norm,                     # Normalized: "Marchigue"
        norm.upper(),             # "MARCHIGUE"
        norm.title(),             # "Marchigue"
        norm.lower(),             # "marchigue"
        comuna.upper(),           # "MARCHIGÜE"
        comuna.title(),           # "Marchigüe"
        comuna.lower(),           # "marchigüe"
    ]
    seen = set()
    unique_variants = []
    for v in raw_variants:
        if v and v not in seen:
            seen.add(v)
            unique_variants.append(v)

    for v in unique_variants:
        sql = f"SELECT {extra_cols} FROM {table} WHERE {col} = ?"
        if where_extra:
            sql += f" AND {where_extra}"
        if limit:
            sql += f" LIMIT {limit}"
        rows = conn.execute(sql, (v,)).fetchall()
        if rows:
            return rows

    # Fallback: UPPER comparison with all normalized variants
    for v in [norm.upper(), resolved.upper(), comuna.upper()]:
        sql = f"SELECT {extra_cols} FROM {table} WHERE UPPER({col}) = ?"
        if where_extra:
            sql += f" AND {where_extra}"
        if limit:
            sql += f" LIMIT {limit}"
        rows = conn.execute(sql, (v,)).fetchall()
        if rows:
            return rows

    # Last resort: fuzzy LIKE on first 5 normalized chars
    prefix = norm[:5].upper()
    if len(prefix) >= 4:
        sql = f"SELECT {extra_cols} FROM {table} WHERE UPPER({col}) LIKE ?"
        if where_extra:
            sql += f" AND {where_extra}"
        if limit:
            sql += f" LIMIT {limit}"
        rows = conn.execute(sql, (f"{prefix}%",)).fetchall()
        if rows:
            return rows

    return []


def _search_comuna_single(conn: sqlite3.Connection, col: str, table: str,
                           comuna: str, extra_cols: str = "*") -> Optional[sqlite3.Row]:
    """Find a single row for a comuna, trying all normalizations."""
    rows = _search_comuna_variants(conn, col, table, comuna, extra_cols, limit=1)
    return rows[0] if rows else None


def _haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Distance in km between two lat/lon points."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
         * math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _fmt_money(val: float) -> str:
    """Format money as Chilean pesos."""
    if val >= 1_000_000_000:
        return f"${val / 1_000_000_000:,.1f} MM"
    if val >= 1_000_000:
        return f"${val / 1_000_000:,.1f} M"
    return f"${val:,.0f}"


# ---------------------------------------------------------------------------
# Available comunas (needed by main.py)
# ---------------------------------------------------------------------------

def get_available_comunas() -> list[str]:
    """Sorted list of all comunas present in any database."""
    comunas = set()

    conn = _connect("catastro")
    if conn:
        rows = conn.execute("SELECT DISTINCT comuna FROM resumen_por_comuna").fetchall()
        comunas.update(r["comuna"] for r in rows)
        conn.close()

    conn = _connect("dga")
    if conn:
        rows = conn.execute("SELECT DISTINCT comuna FROM resumen_por_comuna").fetchall()
        comunas.update(r["comuna"] for r in rows)
        conn.close()

    conn = _connect("electrico")
    if conn:
        rows = conn.execute("SELECT DISTINCT nombre_comuna FROM clientes_por_comuna").fetchall()
        comunas.update(r["nombre_comuna"] for r in rows)
        conn.close()

    return sorted(c for c in comunas if c and c.strip())


# ---------------------------------------------------------------------------
# Section 1: Produccion Agricola
# ---------------------------------------------------------------------------

def _get_produccion(comuna: str) -> dict:
    result = {
        "disponible": False,
        "especies": [],
        "total_superficie_ha": 0.0,
        "total_explotaciones": 0,
        "variedades": [],
        "metodos_riego": [],
        "tipo_productor": [],
        "comparacion_nacional": [],
        "texto_analitico": "",
        "alertas": [],
        "semaforo": "rojo",
        "semaforo_texto": "Sin registro de produccion fruticola",
        "region": "",
        "provincia": "",
    }
    conn = _connect("catastro")
    if not conn:
        result["alertas"].append("Base de datos catastro_fruticola.db no disponible.")
        return result

    try:
        # --- Especies from resumen_por_comuna ---
        rows = _search_comuna_variants(
            conn, "comuna", "resumen_por_comuna", comuna,
            extra_cols="especie, superficie_ha, num_explotaciones",
        )
        if rows:
            result["disponible"] = True
            for r in rows:
                result["especies"].append({
                    "especie": r["especie"],
                    "superficie_ha": round(r["superficie_ha"] or 0, 2),
                    "num_explotaciones": r["num_explotaciones"] or 0,
                })
            # Sort by superficie desc
            result["especies"].sort(key=lambda x: x["superficie_ha"], reverse=True)
            result["total_superficie_ha"] = round(
                sum(e["superficie_ha"] for e in result["especies"]), 2)
            result["total_explotaciones"] = sum(
                e["num_explotaciones"] for e in result["especies"])

        # --- Region / Provincia from catastro_completo ---
        geo = _search_comuna_single(
            conn, "comuna", "catastro_completo", comuna,
            extra_cols="DISTINCT region, provincia",
        )
        if geo:
            result["region"] = geo["region"] or ""
            result["provincia"] = geo["provincia"] or ""

        # --- Variedades detail from catastro_completo ---
        vars_sql = (
            'SELECT especie, variedad, '
            'SUM(CAST(REPLACE("superficie_(ha)", \',\', \'.\') AS REAL)) as sup, '
            'COUNT(*) as cnt, '
            'metodo_de_riego, '
            'AVG(CAST(anio_plantacion AS REAL)) as anio_prom '
            'FROM catastro_completo '
            'WHERE UPPER(comuna) = UPPER(?) '
            'GROUP BY especie, variedad '
            'ORDER BY sup DESC LIMIT 30'
        )
        for v in _comuna_variants(comuna):
            vars_rows = conn.execute(vars_sql, (v,)).fetchall()
            if vars_rows:
                break
        else:
            vars_rows = []

        result["variedades"] = []
        for v in vars_rows:
            result["variedades"].append({
                "especie": v["especie"] or "",
                "variedad": v["variedad"] or "",
                "superficie_ha": round(v["sup"] or 0, 2),
                "registros": v["cnt"] or 0,
                "metodo_riego": v["metodo_de_riego"] or "Sin informacion",
                "anio_promedio": int(v["anio_prom"]) if v["anio_prom"] else None,
            })

        # --- Metodos de riego ---
        riego_sql = (
            'SELECT metodo_de_riego, COUNT(*) as cnt, '
            'SUM(CAST(REPLACE("superficie_(ha)", \',\', \'.\') AS REAL)) as sup '
            'FROM catastro_completo WHERE UPPER(comuna) = UPPER(?) '
            'GROUP BY metodo_de_riego ORDER BY sup DESC'
        )
        for v in _comuna_variants(comuna):
            riego_rows = conn.execute(riego_sql, (v,)).fetchall()
            if riego_rows:
                break
        else:
            riego_rows = []

        total_riego_sup = sum((r["sup"] or 0) for r in riego_rows)
        result["metodos_riego"] = []
        for r in riego_rows:
            sup = r["sup"] or 0
            pct = round(100 * sup / total_riego_sup, 1) if total_riego_sup > 0 else 0
            result["metodos_riego"].append({
                "metodo": r["metodo_de_riego"] or "Sin informacion",
                "registros": r["cnt"],
                "superficie_ha": round(sup, 2),
                "pct": pct,
            })

        # --- Tipo productor ---
        tipo_sql = (
            'SELECT tipo_productor, COUNT(DISTINCT "numero_explotacion") as n_exp, '
            'SUM(CAST(REPLACE("superficie_(ha)", \',\', \'.\') AS REAL)) as ha '
            'FROM catastro_completo WHERE UPPER(comuna) = UPPER(?) '
            'GROUP BY tipo_productor ORDER BY ha DESC'
        )
        for v in _comuna_variants(comuna):
            tipo_rows = conn.execute(tipo_sql, (v,)).fetchall()
            if tipo_rows:
                break
        else:
            tipo_rows = []

        result["tipo_productor"] = [
            {
                "tipo": t["tipo_productor"] or "Sin clasificar",
                "num_explotaciones": t["n_exp"] or 0,
                "hectareas": round(t["ha"] or 0, 2),
            }
            for t in tipo_rows
        ]

        # --- Comparacion nacional ---
        if result["especies"]:
            for esp in result["especies"][:5]:
                nac = conn.execute(
                    "SELECT superficie_ha, num_comunas FROM resumen_por_especie "
                    "WHERE especie = ?",
                    (esp["especie"],)
                ).fetchone()
                if nac:
                    total_nac = nac["superficie_ha"] or 1
                    num_comunas = nac["num_comunas"] or 1
                    prom_nac = total_nac / num_comunas
                    pct = round(100 * esp["superficie_ha"] / total_nac, 2) if total_nac > 0 else 0
                    result["comparacion_nacional"].append({
                        "especie": esp["especie"],
                        "superficie_comuna": esp["superficie_ha"],
                        "total_nacional": round(total_nac, 2),
                        "promedio_comuna_nacional": round(prom_nac, 2),
                        "pct_nacional": pct,
                    })

        # --- Semaforo ---
        total_ha = result["total_superficie_ha"]
        if total_ha > 10:
            result["semaforo"] = "verde"
            result["semaforo_texto"] = (
                f"Zona con produccion fruticola consolidada ({total_ha:.0f} ha)")
        elif total_ha >= 1:
            result["semaforo"] = "amarillo"
            result["semaforo_texto"] = (
                f"Produccion fruticola limitada ({total_ha:.1f} ha)")
        else:
            result["semaforo"] = "rojo"
            result["semaforo_texto"] = "Sin registro significativo de produccion fruticola"

        # --- Texto analitico ---
        if result["disponible"] and result["especies"]:
            top3 = ", ".join(e["especie"] for e in result["especies"][:3])
            top3_pct = round(
                100 * sum(e["superficie_ha"] for e in result["especies"][:3]) / total_ha, 1
            ) if total_ha > 0 else 0

            riego_dom = result["metodos_riego"][0] if result["metodos_riego"] else None
            riego_text = ""
            if riego_dom:
                met = riego_dom["metodo"]
                rpct = riego_dom["pct"]
                if met.lower() in ("goteo", "microaspersion", "riego por goteo"):
                    riego_text = (f"El metodo de riego predominante es {met} ({rpct}%), "
                                  "lo que indica alta tecnificacion.")
                elif met.lower() in ("surco", "tendido", "inundacion"):
                    riego_text = (f"El metodo de riego predominante es {met} ({rpct}%), "
                                  "lo que indica riego tradicional con potencial de mejora.")
                else:
                    riego_text = f"El metodo de riego predominante es {met} ({rpct}%)."

            antig_vals = [v["anio_promedio"] for v in result["variedades"]
                          if v["anio_promedio"] and v["anio_promedio"] > 1900]
            antig_text = ""
            if antig_vals:
                prom_anio = sum(antig_vals) / len(antig_vals)
                edad = 2026 - prom_anio
                antig_text = f" Las plantaciones tienen una antiguedad promedio de {edad:.0f} anios."

            result["texto_analitico"] = (
                f"En {comuna} se cultivan principalmente {top3}, que representan "
                f"el {top3_pct}% de la superficie fruticola total ({total_ha:.1f} ha). "
                f"{riego_text}{antig_text}"
            )
        else:
            result["texto_analitico"] = (
                f"No se registra produccion fruticola significativa en la comuna de {comuna}. "
                "Esto puede deberse a que la actividad agricola principal no es fruticola, "
                "o a que la comuna no esta incluida en el catastro SAG/CIREN."
            )

        # --- Alertas ---
        if total_ha > 0 and total_ha < 5:
            result["alertas"].append(
                "Superficie fruticola muy reducida. Verificar si existen cultivos "
                "no registrados en el catastro.")
        for esp_comp in result["comparacion_nacional"]:
            if esp_comp["pct_nacional"] > 5:
                result["alertas"].append(
                    f"{esp_comp['especie']}: esta comuna concentra el "
                    f"{esp_comp['pct_nacional']}% de la produccion nacional.")

    except Exception as e:
        logger.warning(f"Error en _get_produccion({comuna}): {e}")
        result["alertas"].append(f"Error consultando produccion: {e}")
    finally:
        conn.close()

    return result


# ---------------------------------------------------------------------------
# Section 2: Disponibilidad de Agua
# ---------------------------------------------------------------------------

def _get_agua(comuna: str, lat: float = None, lon: float = None) -> dict:
    result = {
        "disponible": False,
        "num_patentes": 0,
        "saldo_total": 0.0,
        "monto_total": 0.0,
        "saldo_promedio": 0.0,
        "monto_promedio": 0.0,
        "morosidad_pct": 0.0,
        "patentes_con_deuda": 0,
        "por_concesion": [],
        "por_tipo": [],
        "por_anio": [],
        "oficinas_cbr": [],
        "restriccion_hidrica": False,
        "agotamiento": False,
        "reserva_caudal": False,
        "infraestructura_cercana": [],
        "texto_analitico": "",
        "alertas": [],
        "semaforo": "rojo",
        "semaforo_texto": "Sin patentes de agua registradas",
    }
    conn = _connect("dga")
    if not conn:
        result["alertas"].append("Base de datos dga_derechos.db no disponible.")
        return result

    try:
        # --- Resumen ---
        row = _search_comuna_single(
            conn, "comuna", "resumen_por_comuna", comuna)
        if row:
            result["disponible"] = True
            result["num_patentes"] = row["num_patentes"] or 0
            result["saldo_total"] = row["saldo_total"] or 0
            result["monto_total"] = row["monto_total"] or 0
            result["saldo_promedio"] = round(row["saldo_promedio"] or 0, 0)
            result["monto_promedio"] = round(row["monto_promedio"] or 0, 0)
            if result["monto_total"] > 0:
                result["morosidad_pct"] = round(
                    100 * result["saldo_total"] / result["monto_total"], 1)

        # --- Patentes con deuda ---
        deuda_rows = _search_comuna_variants(
            conn, "comuna", "patentes", comuna,
            extra_cols="COUNT(*) as cnt",
            where_extra="saldo > 0",
        )
        if deuda_rows:
            result["patentes_con_deuda"] = deuda_rows[0]["cnt"] or 0

        # --- Por concesion ---
        conc_sql = (
            "SELECT concesion, COUNT(*) as cnt, "
            "ROUND(SUM(monto), 0) as m, ROUND(SUM(saldo), 0) as s "
            "FROM patentes WHERE UPPER(comuna) = UPPER(?) "
            "GROUP BY concesion ORDER BY m DESC"
        )
        _variants = _comuna_variants(comuna)
        for v in _variants:
            conc_rows = conn.execute(conc_sql, (v,)).fetchall()
            if conc_rows:
                break
        else:
            conc_rows = []

        result["por_concesion"] = [
            {
                "concesion": c["concesion"] or "Sin concesion",
                "num_patentes": c["cnt"],
                "monto_total": c["m"] or 0,
                "saldo_pendiente": c["s"] or 0,
            }
            for c in conc_rows
        ]

        # --- Por tipo persona ---
        tipo_sql = (
            "SELECT tipo_persona, COUNT(*) as cnt, ROUND(SUM(saldo), 0) as s "
            "FROM patentes WHERE UPPER(comuna) = UPPER(?) "
            "GROUP BY tipo_persona"
        )
        for v in _variants:
            tipo_rows = conn.execute(tipo_sql, (v,)).fetchall()
            if tipo_rows:
                break
        else:
            tipo_rows = []

        result["por_tipo"] = [
            {"tipo": t["tipo_persona"] or "Sin tipo", "cantidad": t["cnt"],
             "saldo": t["s"] or 0}
            for t in tipo_rows
        ]

        # --- Por anio ---
        anio_sql = (
            "SELECT CAST(anio_patente AS INTEGER) as anio, COUNT(*) as cnt, "
            "ROUND(SUM(saldo), 0) as s "
            "FROM patentes WHERE UPPER(comuna) = UPPER(?) AND anio_patente IS NOT NULL "
            "GROUP BY anio ORDER BY anio"
        )
        for v in _variants:
            anio_rows = conn.execute(anio_sql, (v,)).fetchall()
            if anio_rows:
                break
        else:
            anio_rows = []

        result["por_anio"] = [
            {"anio": a["anio"], "cantidad": a["cnt"], "saldo": a["s"] or 0}
            for a in anio_rows
        ]

        # --- Oficinas CBR ---
        cbr_sql = (
            "SELECT oficina_cbr, COUNT(*) as cnt "
            "FROM patentes WHERE UPPER(comuna) = UPPER(?) "
            "AND oficina_cbr IS NOT NULL AND oficina_cbr != '' "
            "GROUP BY oficina_cbr ORDER BY cnt DESC"
        )
        for v in _variants:
            cbr_rows = conn.execute(cbr_sql, (v,)).fetchall()
            if cbr_rows:
                break
        else:
            cbr_rows = []

        result["oficinas_cbr"] = [
            {"oficina": c["oficina_cbr"], "num_patentes": c["cnt"]}
            for c in cbr_rows
        ]

    except Exception as e:
        logger.warning(f"Error en _get_agua patentes ({comuna}): {e}")
        result["alertas"].append(f"Error consultando patentes: {e}")
    finally:
        conn.close()

    # --- Geospatial: restricciones, agotamiento, infraestructura cercana ---
    _enrich_agua_geospatial(result, comuna, lat, lon)

    # --- Semaforo ---
    n = result["num_patentes"]
    mor = result["morosidad_pct"]
    if result["agotamiento"] or mor > 30 or n == 0:
        result["semaforo"] = "rojo"
        reasons = []
        if result["agotamiento"]:
            reasons.append("zona con agotamiento declarado")
        if mor > 30:
            reasons.append(f"morosidad alta ({mor:.0f}%)")
        if n == 0:
            reasons.append("sin patentes registradas")
        result["semaforo_texto"] = "Riesgo alto: " + "; ".join(reasons)
    elif result["restriccion_hidrica"] or (10 <= mor <= 30) or n < 50:
        result["semaforo"] = "amarillo"
        reasons = []
        if result["restriccion_hidrica"]:
            reasons.append("zona con restriccion hidrica")
        if 10 <= mor <= 30:
            reasons.append(f"morosidad moderada ({mor:.0f}%)")
        if 0 < n < 50:
            reasons.append(f"pocas patentes ({n})")
        result["semaforo_texto"] = "Precaucion: " + "; ".join(reasons)
    elif n > 0:
        result["semaforo"] = "verde"
        result["semaforo_texto"] = (
            f"Situacion favorable: {n} patentes, morosidad {mor:.0f}%")

    # --- Texto analitico ---
    if result["disponible"]:
        salud = "buena salud financiera"
        if mor > 30:
            salud = "posible abandono o estres financiero significativo"
        elif mor > 10:
            salud = "un nivel moderado de estres financiero"

        result["texto_analitico"] = (
            f"En la comuna de {comuna} se registran {n} patentes de agua "
            f"por un monto total de {_fmt_money(result['monto_total'])}. "
            f"La tasa de morosidad es del {mor:.1f}%, lo que indica {salud}."
        )
        if result["restriccion_hidrica"]:
            result["texto_analitico"] += (
                " La zona se encuentra bajo restriccion hidrica, "
                "lo que puede limitar la obtencion de nuevos derechos.")
        if result["agotamiento"]:
            result["texto_analitico"] += (
                " Existe declaracion de agotamiento en la zona, "
                "lo que impide otorgar nuevos derechos de aprovechamiento.")
    else:
        result["texto_analitico"] = (
            f"No se encontraron patentes de agua registradas en la comuna de {comuna}. "
            "Se recomienda verificar directamente con la DGA.")

    # --- Alertas ---
    if mor > 30:
        result["alertas"].append(
            f"Morosidad alta ({mor:.1f}%): indica posible abandono de derechos.")
    if result["agotamiento"]:
        result["alertas"].append(
            "Zona con agotamiento declarado: no se otorgan nuevos derechos.")
    if result["restriccion_hidrica"]:
        result["alertas"].append(
            "Zona con restriccion hidrica vigente.")
    if result["patentes_con_deuda"] > 0:
        pct_deuda = round(100 * result["patentes_con_deuda"] / n, 1) if n > 0 else 0
        result["alertas"].append(
            f"{result['patentes_con_deuda']} patentes con saldo pendiente ({pct_deuda}%).")

    return result


def _enrich_agua_geospatial(result: dict, comuna: str, lat: float = None,
                            lon: float = None):
    """Add geospatial info to agua section using point-in-polygon (geo_engine)."""

    # --- Use geo_engine for point-specific analysis ---
    if _HAS_GEO_ENGINE and lat is not None and lon is not None:
        restriccion = _geo_restriccion(lat, lon)
        if restriccion:
            result["restriccion_hidrica"] = True
            result["restriccion_detalle"] = restriccion

        agotamiento = _geo_agotamiento(lat, lon)
        if agotamiento:
            result["agotamiento"] = True
            result["agotamiento_detalle"] = agotamiento

        reserva = _geo_reserva(lat, lon)
        if reserva:
            result["reserva_caudal"] = True
            result["reserva_detalle"] = reserva

        # Cuenca info
        cuenca = _geo_cuenca(lat, lon)
        if cuenca:
            result["cuenca"] = cuenca

        # Infrastructure cercana (embalses + estaciones with proper WGS84)
        infra = []
        embalses = _geo_embalses(lat, lon, top_n=3)
        for e in embalses:
            infra.append({
                "tipo": "Embalse",
                "nombre": e["nombre"],
                "distancia_km": e["distancia_km"],
                "comuna": e.get("comuna", ""),
                "uso": e.get("uso", ""),
            })
        for tipo_est, label in [
            ("fluviometricas", "Estacion fluviometrica"),
            ("meteorologicas", "Estacion meteorologica"),
            ("calidad_agua", "Punto calidad agua"),
        ]:
            ests = _geo_estaciones(lat, lon, tipo_est, top_n=3)
            for e in ests:
                infra.append({
                    "tipo": label,
                    "nombre": e["nombre"],
                    "distancia_km": e["distancia_km"],
                })
        infra.sort(key=lambda x: x["distancia_km"])
        result["infraestructura_cercana"] = infra
        return

    # --- Fallback: region-based lookup from SQLite ---
    conn = _connect("geospatial")
    if not conn:
        return

    try:
        region = ""
        dga_conn = _connect("dga")
        if dga_conn:
            row = _search_comuna_single(
                dga_conn, "comuna", "resumen_por_comuna", comuna,
                extra_cols="region")
            if row:
                region = row["region"] or ""
            dga_conn.close()

        if region and _table_exists(conn, "restricciones_hidricas"):
            norm_region = normalize_comuna(region).upper()
            rows = conn.execute(
                "SELECT * FROM restricciones_hidricas WHERE UPPER(regiones) LIKE ?",
                (f"%{norm_region}%",)
            ).fetchall()
            if rows:
                result["restriccion_hidrica"] = True

    except Exception as e:
        logger.warning(f"Error en geospatial agua ({comuna}): {e}")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Section 3: Uso de Suelo
# ---------------------------------------------------------------------------

def _get_uso_suelo(comuna: str) -> dict:
    result = {
        "disponible": False,
        "distribucion": [],
        "total_ha": 0.0,
        "pct_agricola": 0.0,
        "texto_analitico": "",
        "alertas": [],
        "semaforo": "rojo",
        "semaforo_texto": "Sin datos de uso de suelo",
        "fuente_ano": "CONAF / Catastro de uso de suelo",
    }
    conn = _connect("geospatial")
    if not conn:
        return result

    try:
        if not _table_exists(conn, "uso_suelo_comuna"):
            return result

        rows = _search_comuna_variants(
            conn, "comuna", "uso_suelo_comuna", comuna,
            extra_cols="uso_tierra, superficie_ha, num_registros",
        )
        if not rows:
            return result

        result["disponible"] = True
        total = sum(r["superficie_ha"] or 0 for r in rows)
        result["total_ha"] = round(total, 2)

        for r in rows:
            sup = r["superficie_ha"] or 0
            pct = round(100 * sup / total, 1) if total > 0 else 0
            result["distribucion"].append({
                "uso_tierra": r["uso_tierra"] or "Sin clasificar",
                "superficie_ha": round(sup, 2),
                "pct": pct,
            })
        result["distribucion"].sort(key=lambda x: x["superficie_ha"], reverse=True)

        # Calculate pct_agricola (sum of agricultural uses)
        agri_keys = ["agricola", "cultivo", "frutal", "hortaliza", "viña", "vina",
                     "cereales", "praderas", "riego", "secano"]
        agri_ha = 0
        forest_ha = 0
        for d in result["distribucion"]:
            uso_lower = d["uso_tierra"].lower()
            if any(k in uso_lower for k in agri_keys):
                agri_ha += d["superficie_ha"]
            if any(k in uso_lower for k in ["plantacion", "forestal", "bosque"]):
                forest_ha += d["superficie_ha"]

        pct_agri = round(100 * agri_ha / total, 1) if total > 0 else 0
        pct_forest = round(100 * forest_ha / total, 1) if total > 0 else 0
        result["pct_agricola"] = pct_agri

        # Semaforo
        if pct_agri > 30:
            result["semaforo"] = "verde"
            result["semaforo_texto"] = (
                f"Alta vocacion agricola ({pct_agri:.0f}% del territorio)")
        elif pct_agri >= 10 or pct_forest > 30:
            result["semaforo"] = "amarillo"
            txt = f"Vocacion agricola moderada ({pct_agri:.0f}%)"
            if pct_forest > 30:
                txt += f"; alta cobertura forestal ({pct_forest:.0f}%)"
            result["semaforo_texto"] = txt
        else:
            result["semaforo"] = "rojo"
            result["semaforo_texto"] = (
                f"Baja vocacion agricola ({pct_agri:.0f}% del territorio)")

        # Texto analitico
        top_uso = result["distribucion"][0] if result["distribucion"] else None
        if top_uso:
            result["texto_analitico"] = (
                f"La comuna de {comuna} cuenta con un total de {total:,.0f} ha. "
                f"El uso de suelo predominante es '{top_uso['uso_tierra']}' "
                f"({top_uso['pct']}%). La superficie con vocacion agricola "
                f"representa el {pct_agri:.1f}% del territorio."
            )
            if pct_forest > 30:
                result["texto_analitico"] += (
                    f" Destaca la cobertura forestal/plantaciones ({pct_forest:.1f}%), "
                    "lo que puede representar riesgo de incendio y competencia por agua.")
        else:
            result["texto_analitico"] = (
                f"No se dispone de informacion detallada de uso de suelo para {comuna}.")

        # Alertas
        if pct_forest > 30:
            result["alertas"].append(
                f"Alta cobertura forestal ({pct_forest:.0f}%): riesgo de incendio "
                "y posible competencia por recursos hidricos.")
        if pct_agri < 10:
            result["alertas"].append(
                "Baja vocacion agricola: verificar aptitud de suelos antes de invertir.")

    except Exception as e:
        logger.warning(f"Error en _get_uso_suelo({comuna}): {e}")
        result["alertas"].append(f"Error consultando uso de suelo: {e}")
    finally:
        conn.close()

    return result


# ---------------------------------------------------------------------------
# Section 4: Infraestructura Electrica
# ---------------------------------------------------------------------------

def _get_electrico(comuna: str) -> dict:
    result = {
        "disponible": False,
        "total_clientes": 0,
        "potencia_total_kw": 0.0,
        "potencia_promedio_kw": 0.0,
        "empresas": [],
        "comparacion_regional": {},
        "distribucion_potencia": [],
        "texto_analitico": "",
        "alertas": [],
        "semaforo": "rojo",
        "semaforo_texto": "Sin datos de infraestructura electrica",
    }
    conn = _connect("electrico")
    if not conn:
        result["alertas"].append("Base de datos electrico.db no disponible.")
        return result

    try:
        row = _search_comuna_single(
            conn, "nombre_comuna", "clientes_por_comuna", comuna)
        if row:
            result["disponible"] = True
            result["total_clientes"] = row["total_clientes"] or 0
            result["potencia_total_kw"] = round(row["potencia_total_kw"] or 0, 2)
            result["potencia_promedio_kw"] = round(row["potencia_promedio_kw"] or 0, 2)
            region_name = row["nombre_region"] or ""

            # --- Comparacion regional ---
            if region_name:
                reg_row = conn.execute(
                    "SELECT total_clientes, potencia_total_kw, num_comunas "
                    "FROM resumen_regional WHERE UPPER(nombre_region) = UPPER(?)",
                    (region_name,)
                ).fetchone()
                if not reg_row:
                    norm_reg = normalize_comuna(region_name)
                    reg_row = conn.execute(
                        "SELECT total_clientes, potencia_total_kw, num_comunas "
                        "FROM resumen_regional WHERE UPPER(nombre_region) = UPPER(?)",
                        (norm_reg,)
                    ).fetchone()
                if reg_row:
                    reg_clientes = reg_row["total_clientes"] or 1
                    reg_pot = reg_row["potencia_total_kw"] or 1
                    num_comunas_reg = reg_row["num_comunas"] or 1
                    pot_prom_reg = reg_pot / reg_clientes if reg_clientes > 0 else 0
                    diff = (
                        (result["potencia_promedio_kw"] - pot_prom_reg) / pot_prom_reg * 100
                        if pot_prom_reg > 0 else 0
                    )
                    result["comparacion_regional"] = {
                        "clientes_region": reg_clientes,
                        "potencia_promedio_region": round(pot_prom_reg, 2),
                        "diff_pct": round(diff, 1),
                    }

        # --- Empresas ---
        empresas_rows = _search_comuna_variants(
            conn, "nombre_comuna", "clientes_por_empresa_comuna", comuna,
            extra_cols="empresa, total_clientes, potencia_total_kw",
        )
        result["empresas"] = [
            {
                "empresa": e["empresa"],
                "clientes": e["total_clientes"],
                "potencia_kw": round(e["potencia_total_kw"] or 0, 2),
            }
            for e in empresas_rows
        ]
        result["empresas"].sort(key=lambda x: x["potencia_kw"], reverse=True)

        # --- Distribucion potencia (new table, might not exist) ---
        if _table_exists(conn, "distribucion_potencia_comuna"):
            dp_rows = _search_comuna_variants(
                conn, "nombre_comuna", "distribucion_potencia_comuna", comuna,
                extra_cols="rango_potencia, num_clientes, potencia_subtotal_kw",
            )
            result["distribucion_potencia"] = [
                {
                    "rango": d["rango_potencia"],
                    "num_clientes": d["num_clientes"],
                    "potencia_kw": round(d["potencia_subtotal_kw"] or 0, 2),
                }
                for d in dp_rows
            ]

        # --- Semaforo ---
        pot_prom = result["potencia_promedio_kw"]
        if pot_prom > 5:
            result["semaforo"] = "verde"
            result["semaforo_texto"] = (
                f"Capacidad electrica adecuada para actividad agricola/industrial "
                f"({pot_prom:.1f} kW promedio)")
        elif pot_prom >= 2:
            result["semaforo"] = "amarillo"
            result["semaforo_texto"] = (
                f"Capacidad electrica moderada ({pot_prom:.1f} kW promedio)")
        else:
            result["semaforo"] = "rojo"
            if result["disponible"]:
                result["semaforo_texto"] = (
                    f"Capacidad electrica limitada ({pot_prom:.1f} kW promedio)")
            else:
                result["semaforo_texto"] = "Sin datos de infraestructura electrica"

        # --- Texto analitico ---
        if result["disponible"]:
            emp_text = ""
            if result["empresas"]:
                emp_principal = result["empresas"][0]["empresa"]
                emp_text = f" La empresa distribuidora principal es {emp_principal}."

            comp_text = ""
            if result["comparacion_regional"]:
                diff = result["comparacion_regional"]["diff_pct"]
                if diff > 10:
                    comp_text = (f" La potencia promedio esta {diff:.0f}% por encima "
                                 "del promedio regional.")
                elif diff < -10:
                    comp_text = (f" La potencia promedio esta {abs(diff):.0f}% por debajo "
                                 "del promedio regional.")
                else:
                    comp_text = " La potencia promedio es similar al promedio regional."

            result["texto_analitico"] = (
                f"La comuna de {comuna} cuenta con {result['total_clientes']:,} clientes "
                f"electricos y una potencia total de {result['potencia_total_kw']:,.0f} kW "
                f"(promedio {pot_prom:.1f} kW por cliente).{emp_text}{comp_text}"
            )
        else:
            result["texto_analitico"] = (
                f"No se encontraron datos de infraestructura electrica para {comuna}.")

        # --- Alertas ---
        if pot_prom < 2 and result["disponible"]:
            result["alertas"].append(
                "Potencia promedio muy baja: puede requerir inversiones en "
                "infraestructura electrica para operaciones agricolas.")
        if result["comparacion_regional"] and result["comparacion_regional"].get("diff_pct", 0) < -20:
            result["alertas"].append(
                "Potencia significativamente inferior al promedio regional.")

    except Exception as e:
        logger.warning(f"Error en _get_electrico({comuna}): {e}")
        result["alertas"].append(f"Error consultando electrico: {e}")
    finally:
        conn.close()

    return result


# ---------------------------------------------------------------------------
# Section 5: Riesgos Territoriales
# ---------------------------------------------------------------------------

def _get_riesgos(comuna: str, agua: dict, uso_suelo: dict, produccion: dict) -> dict:
    result = {
        "disponible": True,
        "items": [],
        "semaforo_global": "verde",
        "semaforo_texto": "Sin riesgos significativos identificados",
        "texto_analitico": "",
        "alertas": [],
    }

    riesgos = []

    # Restriccion hidrica
    if agua.get("restriccion_hidrica"):
        riesgos.append({
            "riesgo": "Restriccion hidrica",
            "estado": "Zona con restriccion vigente",
            "semaforo": "amarillo",
        })
    else:
        riesgos.append({
            "riesgo": "Restriccion hidrica",
            "estado": "Sin restriccion identificada",
            "semaforo": "verde",
        })

    # Agotamiento
    if agua.get("agotamiento"):
        riesgos.append({
            "riesgo": "Agotamiento de derechos",
            "estado": "Zona con agotamiento declarado",
            "semaforo": "rojo",
        })
    else:
        riesgos.append({
            "riesgo": "Agotamiento de derechos",
            "estado": "Sin agotamiento declarado",
            "semaforo": "verde",
        })

    # Morosidad
    mor = agua.get("morosidad_pct", 0)
    if mor > 30:
        riesgos.append({
            "riesgo": "Morosidad de patentes",
            "estado": f"Morosidad alta ({mor:.0f}%)",
            "semaforo": "rojo",
        })
    elif mor > 10:
        riesgos.append({
            "riesgo": "Morosidad de patentes",
            "estado": f"Morosidad moderada ({mor:.0f}%)",
            "semaforo": "amarillo",
        })
    else:
        riesgos.append({
            "riesgo": "Morosidad de patentes",
            "estado": f"Morosidad baja ({mor:.0f}%)",
            "semaforo": "verde",
        })

    # Riesgo de incendio (forestry > 30%)
    pct_forest = 0
    if uso_suelo.get("disponible"):
        for d in uso_suelo.get("distribucion", []):
            uso_lower = d["uso_tierra"].lower()
            if any(k in uso_lower for k in ["plantacion", "forestal", "bosque"]):
                pct_forest += d.get("pct", 0)
    if pct_forest > 30:
        riesgos.append({
            "riesgo": "Riesgo de incendio forestal",
            "estado": f"Alta cobertura forestal ({pct_forest:.0f}%)",
            "semaforo": "rojo",
        })
    elif pct_forest > 15:
        riesgos.append({
            "riesgo": "Riesgo de incendio forestal",
            "estado": f"Cobertura forestal moderada ({pct_forest:.0f}%)",
            "semaforo": "amarillo",
        })

    # Baja superficie agricola
    if uso_suelo.get("disponible") and uso_suelo.get("pct_agricola", 0) < 10:
        riesgos.append({
            "riesgo": "Baja aptitud agricola",
            "estado": f"Solo {uso_suelo['pct_agricola']:.0f}% de superficie agricola",
            "semaforo": "rojo",
        })

    result["items"] = riesgos

    # Determine global semaforo
    semaforos = [r["semaforo"] for r in riesgos]
    if "rojo" in semaforos:
        result["semaforo_global"] = "rojo"
        n_rojo = semaforos.count("rojo")
        result["semaforo_texto"] = (
            f"{n_rojo} riesgo(s) critico(s) identificado(s)")
    elif "amarillo" in semaforos:
        result["semaforo_global"] = "amarillo"
        n_amarillo = semaforos.count("amarillo")
        result["semaforo_texto"] = (
            f"{n_amarillo} riesgo(s) moderado(s) identificado(s)")
    else:
        result["semaforo_global"] = "verde"
        result["semaforo_texto"] = "Sin riesgos significativos identificados"

    # Texto analitico
    riesgos_criticos = [r for r in riesgos if r["semaforo"] == "rojo"]
    riesgos_moderados = [r for r in riesgos if r["semaforo"] == "amarillo"]
    parts = []
    if riesgos_criticos:
        nombres = ", ".join(r["riesgo"].lower() for r in riesgos_criticos)
        parts.append(f"Se identificaron riesgos criticos: {nombres}.")
    if riesgos_moderados:
        nombres = ", ".join(r["riesgo"].lower() for r in riesgos_moderados)
        parts.append(f"Riesgos moderados: {nombres}.")
    if not parts:
        parts.append(
            f"No se identificaron riesgos significativos en la comuna de {comuna}.")

    result["texto_analitico"] = " ".join(parts)

    # Alertas
    for r in riesgos_criticos:
        result["alertas"].append(f"CRITICO: {r['riesgo']} - {r['estado']}")

    return result


# ---------------------------------------------------------------------------
# Section 6: Infraestructura y Logistica
# ---------------------------------------------------------------------------

def _get_infraestructura(comuna: str, lat: float = None, lon: float = None) -> dict:
    result = {
        "disponible": False,
        "cercanos": [],
        "cuenca": None,
        "subcuenca": None,
        "texto_analitico": "",
    }
    if lat is None or lon is None:
        result["texto_analitico"] = (
            "Seccion no disponible: se requieren coordenadas (lat/lon) "
            "para calcular distancias a infraestructura cercana.")
        return result

    # --- Use geo_engine for accurate point-specific queries ---
    if _HAS_GEO_ENGINE:
        try:
            from geo_engine import (
                get_cuenca, get_subcuenca, get_subsubcuenca,
                get_embalses_cercanos, get_estaciones_cercanas,
            )

            # Cuenca info via point-in-polygon
            cuenca = get_cuenca(lat, lon)
            subcuenca = get_subcuenca(lat, lon)
            subsubcuenca = get_subsubcuenca(lat, lon)
            result["cuenca"] = cuenca
            result["subcuenca"] = subcuenca
            result["subsubcuenca"] = subsubcuenca

            all_infra = []

            # Embalses with names
            embalses = get_embalses_cercanos(lat, lon, max_km=100, top_n=5)
            for e in embalses:
                all_infra.append({
                    "tipo": "Embalse",
                    "nombre": e["nombre"],
                    "distancia_km": e["distancia_km"],
                    "detalle": e.get("uso", ""),
                })

            # Estaciones with proper WGS84 coords
            for tipo_est, label in [
                ("fluviometricas", "Estacion fluviometrica"),
                ("meteorologicas", "Estacion meteorologica"),
                ("calidad_agua", "Punto calidad agua"),
            ]:
                ests = get_estaciones_cercanas(lat, lon, tipo_est, top_n=3)
                for e in ests:
                    all_infra.append({
                        "tipo": label,
                        "nombre": e["nombre"],
                        "distancia_km": e["distancia_km"],
                    })

            all_infra.sort(key=lambda x: x["distancia_km"])
            result["cercanos"] = all_infra

            if all_infra:
                result["disponible"] = True
                mas_cercano = all_infra[0]

                cuenca_text = ""
                if cuenca:
                    cuenca_text = f" El punto se ubica en la cuenca '{cuenca['nombre']}'"
                    if subcuenca:
                        cuenca_text += f", subcuenca '{subcuenca['nombre']}'"
                    cuenca_text += "."

                result["texto_analitico"] = (
                    f"El punto de infraestructura mas cercano es '{mas_cercano['nombre']}' "
                    f"({mas_cercano['tipo']}) a {mas_cercano['distancia_km']} km. "
                    f"Se identificaron {len(all_infra)} puntos de infraestructura "
                    f"en un radio de 100 km.{cuenca_text}"
                )
            else:
                result["texto_analitico"] = (
                    "No se encontraron puntos de infraestructura relevantes "
                    "en un radio de 100 km.")

            return result
        except Exception as e:
            logger.warning(f"Error using geo_engine for infra: {e}")
            # Fall through to SQLite-based fallback

    # --- Fallback: SQLite-based queries ---
    conn = _connect("geospatial")
    if not conn:
        result["texto_analitico"] = "Base de datos geospatial.db no disponible."
        return result

    try:
        all_infra = []
        tables_config = [
            ("embalses", "Embalse", "lat", "lon"),
            ("estaciones_fluviometricas", "Estacion fluviometrica", "lat", "lon"),
            ("estaciones_meteorologicas", "Estacion meteorologica", "lat", "lon"),
            ("calidad_agua", "Punto calidad agua", "lat", "lon"),
        ]
        for table, tipo, lat_col, lon_col in tables_config:
            if not _table_exists(conn, table):
                continue
            try:
                pts = conn.execute(
                    f"SELECT nombre, {lat_col}, {lon_col} FROM {table} "
                    f"WHERE {lat_col} IS NOT NULL AND {lon_col} IS NOT NULL"
                ).fetchall()
                for p in pts:
                    d = _haversine(lat, lon, p[lat_col], p[lon_col])
                    if d < 100:
                        all_infra.append({
                            "tipo": tipo,
                            "nombre": p["nombre"] or "Sin nombre",
                            "distancia_km": round(d, 1),
                        })
            except Exception:
                pass

        all_infra.sort(key=lambda x: x["distancia_km"])

        by_type = {}
        for item in all_infra:
            t = item["tipo"]
            if t not in by_type:
                by_type[t] = []
            if len(by_type[t]) < 3:
                by_type[t].append(item)

        result["cercanos"] = [
            item for items in by_type.values() for item in items
        ]
        result["cercanos"].sort(key=lambda x: x["distancia_km"])

        if result["cercanos"]:
            result["disponible"] = True
            mas_cercano = result["cercanos"][0]
            result["texto_analitico"] = (
                f"El punto de infraestructura mas cercano es '{mas_cercano['nombre']}' "
                f"({mas_cercano['tipo']}) a {mas_cercano['distancia_km']} km. "
                f"Se identificaron {len(result['cercanos'])} puntos de infraestructura "
                "en un radio de 100 km."
            )
        else:
            result["texto_analitico"] = (
                "No se encontraron puntos de infraestructura relevantes "
                "en un radio de 100 km.")

    except Exception as e:
        logger.warning(f"Error en _get_infraestructura({comuna}): {e}")
    finally:
        conn.close()

    return result


# ---------------------------------------------------------------------------
# Section 7: Vecinos Productivos
# ---------------------------------------------------------------------------

def _get_vecinos(comuna: str, provincia: str) -> dict:
    result = {
        "disponible": False,
        "comunas_provincia": [],
        "texto_analitico": "",
    }
    if not provincia:
        result["texto_analitico"] = (
            "No se pudo identificar la provincia para buscar vecinos productivos.")
        return result

    conn = _connect("catastro")
    if not conn:
        return result

    try:
        norm_prov = normalize_comuna(provincia)
        sql = (
            'SELECT comuna, especie, '
            'SUM(CAST(REPLACE("superficie_(ha)", \',\', \'.\') AS REAL)) as ha '
            'FROM catastro_completo '
            'WHERE UPPER(provincia) = UPPER(?) '
            'AND UPPER(comuna) != UPPER(?) '
            'GROUP BY comuna, especie '
            'ORDER BY ha DESC LIMIT 20'
        )
        norm_com = normalize_comuna(comuna)
        for pv in [provincia, norm_prov, norm_prov.upper(), norm_prov.title()]:
            for cv in [comuna, norm_com, norm_com.upper(), norm_com.title()]:
                rows = conn.execute(sql, (pv, cv)).fetchall()
                if rows:
                    break
            if rows:
                break
        else:
            rows = []

        if rows:
            result["disponible"] = True
            result["comunas_provincia"] = [
                {
                    "comuna": r["comuna"],
                    "especie": r["especie"],
                    "hectareas": round(r["ha"] or 0, 2),
                }
                for r in rows
            ]
            comunas_vec = list({r["comuna"] for r in rows})
            top_esp = rows[0]["especie"] if rows else ""
            result["texto_analitico"] = (
                f"En la provincia de {provincia} se identificaron {len(comunas_vec)} "
                f"comunas vecinas con produccion fruticola. El cultivo mas importante "
                f"en la zona es {top_esp}. Esto sugiere condiciones agroclimaticas "
                f"favorables para este tipo de cultivo."
            )
        else:
            result["texto_analitico"] = (
                f"No se encontro produccion fruticola significativa en comunas "
                f"vecinas de la provincia de {provincia}.")

    except Exception as e:
        logger.warning(f"Error en _get_vecinos({comuna}): {e}")
    finally:
        conn.close()

    return result


# ---------------------------------------------------------------------------
# Section 8: Recomendaciones
# ---------------------------------------------------------------------------

def _get_recomendaciones(comuna: str, produccion: dict, agua: dict,
                         electrico: dict, uso_suelo: dict, riesgos: dict) -> dict:
    result = {
        "checklist": [],
        "texto_final": "",
    }

    # --- Always include ---
    result["checklist"].append({
        "verificacion": "Derechos de agua",
        "descripcion": "Verificar existencia y vigencia de derechos de aprovechamiento",
        "accion": "Solicitar certificado de derechos de agua en la DGA",
    })
    result["checklist"].append({
        "verificacion": "Titulo de dominio",
        "descripcion": "Confirmar la titularidad del predio y ausencia de gravamenes",
        "accion": "Solicitar certificado de dominio vigente en el CBR correspondiente",
    })
    result["checklist"].append({
        "verificacion": "Avaluo fiscal",
        "descripcion": "Conocer el valor fiscal del predio para efectos tributarios",
        "accion": "Consultar en el SII el avaluo fiscal actualizado",
    })
    result["checklist"].append({
        "verificacion": "Estudio de suelos",
        "descripcion": "Evaluar aptitud agronomica del terreno",
        "accion": "Contratar estudio edafologico con laboratorio certificado (INIA, UC)",
    })

    # --- Conditional items ---
    # Distribuidora electrica
    if electrico.get("empresas"):
        dist = electrico["empresas"][0]["empresa"]
        result["checklist"].append({
            "verificacion": "Empalme electrico",
            "descripcion": f"Verificar capacidad y factibilidad de conexion",
            "accion": f"Solicitar factibilidad tecnica de empalme con {dist}",
        })

    # CBR
    if agua.get("oficinas_cbr"):
        for cbr in agua["oficinas_cbr"][:2]:
            result["checklist"].append({
                "verificacion": f"CBR {cbr['oficina']}",
                "descripcion": "Inscripcion y certificados de derechos de agua",
                "accion": f"Solicitar certificado de inscripcion en CBR {cbr['oficina']}",
            })

    # Subsidios
    result["checklist"].append({
        "verificacion": "Subsidio Ley 18.450",
        "descripcion": "Bonificacion a inversiones en riego y drenaje (hasta 90%)",
        "accion": "Consultar bases de concurso vigentes en CNR (www.cnr.gob.cl)",
    })
    result["checklist"].append({
        "verificacion": "Certificaciones SAG",
        "descripcion": "Certificaciones fitosanitarias para exportacion",
        "accion": "Consultar requisitos en SAG segun especie a cultivar",
    })

    # Riesgos-based
    if agua.get("agotamiento"):
        result["checklist"].append({
            "verificacion": "Derechos de agua existentes",
            "descripcion": "Zona con agotamiento declarado; solo transferencia posible",
            "accion": "Buscar derechos transferibles en el mercado secundario (Bolsa de Agua)",
        })
    if agua.get("restriccion_hidrica"):
        result["checklist"].append({
            "verificacion": "Restriccion hidrica",
            "descripcion": "Zona con restriccion; nuevos derechos pueden ser limitados",
            "accion": "Verificar tipo de restriccion y posibilidades con la DGA",
        })

    # INDAP / PRODESAL
    if produccion.get("tipo_productor"):
        for tp in produccion["tipo_productor"]:
            if tp["tipo"] and "pequeño" in tp["tipo"].lower():
                result["checklist"].append({
                    "verificacion": "Programas INDAP",
                    "descripcion": "Apoyo tecnico y financiero para pequenos agricultores",
                    "accion": "Consultar oficina INDAP local sobre PRODESAL y creditos",
                })
                break

    # Texto final
    n_checks = len(result["checklist"])
    criticos = [r for r in riesgos.get("items", []) if r["semaforo"] == "rojo"]
    if criticos:
        riesgo_text = (
            f" Se identificaron {len(criticos)} riesgos criticos que requieren "
            "atencion prioritaria antes de cualquier inversion.")
    else:
        riesgo_text = ""

    result["texto_final"] = (
        f"Se recomienda completar las {n_checks} verificaciones listadas "
        f"antes de proceder con la operacion.{riesgo_text} "
        "Este informe es de caracter referencial y no reemplaza la asesoria "
        "profesional de un abogado, agronomo o corredor de propiedades."
    )

    return result


# ---------------------------------------------------------------------------
# Section 9: Analisis del Sitio (point-specific geospatial)
# ---------------------------------------------------------------------------

def _get_analisis_sitio(lat: float, lon: float) -> dict:
    """New section: comprehensive point-specific geospatial analysis.
    Uses geo_engine for point-in-polygon queries against DGA GeoJSON data.
    """
    result = {
        "disponible": False,
        "cuenca": None,
        "subcuenca": None,
        "subsubcuenca": None,
        "restriccion_hidrica": None,
        "agotamiento": None,
        "productividad_pozos": None,
        "reserva_caudales": None,
        "pozos_cercanos": [],
        "isoyeta": None,
        "riesgo_hidrico": None,
        "texto_analitico": "",
        "alertas": [],
        "semaforo": "verde",
        "semaforo_texto": "Sin restricciones identificadas",
    }

    if not _HAS_GEO_ENGINE:
        result["texto_analitico"] = (
            "Motor geoespacial no disponible. "
            "No se pueden realizar consultas punto-en-poligono.")
        return result

    try:
        geo = _geo_analisis_sitio(lat, lon)

        result["disponible"] = True
        result["cuenca"] = geo.get("cuenca")
        result["subcuenca"] = geo.get("subcuenca")
        result["subsubcuenca"] = geo.get("subsubcuenca")
        result["restriccion_hidrica"] = geo.get("restriccion_hidrica")
        result["agotamiento"] = geo.get("agotamiento")
        result["productividad_pozos"] = geo.get("productividad_pozos")
        result["reserva_caudales"] = geo.get("reserva_caudales")
        result["pozos_cercanos"] = geo.get("pozos_cercanos", [])
        result["isoyeta"] = geo.get("isoyeta")
        result["riesgo_hidrico"] = geo.get("riesgo_hidrico")

        # Semaforo from riesgo_hidrico
        riesgo = geo.get("riesgo_hidrico", {})
        result["semaforo"] = riesgo.get("semaforo", "verde")
        result["alertas"] = riesgo.get("alertas", [])

        if result["semaforo"] == "rojo":
            result["semaforo_texto"] = "Restriccion hidrica critica"
        elif result["semaforo"] == "amarillo":
            result["semaforo_texto"] = "Precaucion: limitaciones hidricas"
        else:
            result["semaforo_texto"] = "Sin restricciones hidricas identificadas"

        # Build analytical text
        parts = []

        cuenca = result["cuenca"]
        subcuenca = result["subcuenca"]
        if cuenca:
            txt = f"El punto se ubica en la cuenca '{cuenca['nombre']}'"
            if subcuenca:
                txt += f", subcuenca '{subcuenca['nombre']}'"
            txt += "."
            parts.append(txt)

        iso = result["isoyeta"]
        if iso:
            parts.append(
                f"La precipitacion media anual estimada es de {iso['precipitacion_mm']} mm "
                f"(isoyeta mas cercana a {iso['distancia_km']} km)."
            )

        prod_pozos = result["productividad_pozos"]
        if prod_pozos:
            parts.append(
                f"Productividad de pozos en la zona: {prod_pozos['productividad']} "
                f"({prod_pozos['tipo']})."
            )

        pozos = result["pozos_cercanos"]
        if pozos:
            pozo_cercano = pozos[0]
            parts.append(
                f"El pozo mas cercano esta a {pozo_cercano['distancia_km']} km "
                f"(profundidad {pozo_cercano['profundidad_m']} m, "
                f"productividad {pozo_cercano['productividad_ls']} L/s)."
            )

        restriccion = result["restriccion_hidrica"]
        if restriccion:
            tipo = restriccion.get("tipo", "Restriccion")
            acuifero = restriccion.get("acuifero", "")
            txt = f"ALERTA: El punto se encuentra en zona de {tipo}"
            if acuifero:
                txt += f" (acuifero: {acuifero})"
            txt += "."
            parts.append(txt)

        agotamiento = result["agotamiento"]
        if agotamiento:
            nombre = agotamiento.get("nombre", "")
            parts.append(
                f"ALERTA CRITICA: Zona con declaracion de agotamiento: {nombre}. "
                "No se otorgan nuevos derechos de aprovechamiento."
            )

        result["texto_analitico"] = " ".join(parts) if parts else (
            "No se encontro informacion geoespacial detallada para este punto."
        )

    except Exception as e:
        logger.warning(f"Error en _get_analisis_sitio({lat}, {lon}): {e}")
        result["texto_analitico"] = f"Error en analisis geoespacial: {e}"
        result["alertas"].append(f"Error: {e}")

    return result


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def generate_predial_report(comuna: str, lat: float = None,
                            lon: float = None) -> dict:
    """Generate a complete 8-section predial report for a comuna."""

    produccion = _get_produccion(comuna)
    agua = _get_agua(comuna, lat, lon)
    uso_suelo = _get_uso_suelo(comuna)
    electrico = _get_electrico(comuna)

    # Determine region/provincia from available sources.
    # Try multiple databases to ensure we always find region/provincia,
    # even when the primary lookup fails due to name mismatch.
    region = (produccion.get("region")
              or agua.get("region", "")
              or "")
    provincia = produccion.get("provincia", "")

    # If region is still empty, try to infer from electrico
    if not region:
        conn = _connect("electrico")
        if conn:
            row = _search_comuna_single(
                conn, "nombre_comuna", "clientes_por_comuna", comuna,
                extra_cols="nombre_region")
            if row:
                region = row["nombre_region"] or ""
            conn.close()

    # If still empty, try catastro_completo with resolved name
    if not region:
        conn = _connect("catastro")
        if conn:
            for v in _comuna_variants(comuna):
                row = conn.execute(
                    "SELECT DISTINCT region, provincia FROM catastro_completo "
                    "WHERE UPPER(comuna) = UPPER(?) LIMIT 1", (v,)
                ).fetchone()
                if row:
                    region = row[0] or ""
                    provincia = row[1] or provincia
                    break
            conn.close()

    # If still empty, try DGA resumen
    if not region:
        conn = _connect("dga")
        if conn:
            for v in _comuna_variants(comuna):
                row = conn.execute(
                    "SELECT region FROM resumen_por_comuna "
                    "WHERE UPPER(comuna) = UPPER(?) LIMIT 1", (v,)
                ).fetchone()
                if row:
                    region = row[0] or ""
                    break
            conn.close()

    # Analisis del sitio (point-specific geospatial)
    analisis_sitio = None
    if lat is not None and lon is not None:
        analisis_sitio = _get_analisis_sitio(lat, lon)

    riesgos = _get_riesgos(comuna, agua, uso_suelo, produccion)
    infraestructura = _get_infraestructura(comuna, lat, lon)
    vecinos = _get_vecinos(comuna, provincia)
    recomendaciones = _get_recomendaciones(
        comuna, produccion, agua, electrico, uso_suelo, riesgos)

    secciones = []
    if analisis_sitio and analisis_sitio["disponible"]:
        secciones.append("Analisis del Sitio")
    if produccion["disponible"]:
        secciones.append("Produccion Agricola")
    if agua["disponible"]:
        secciones.append("Disponibilidad de Agua")
    if uso_suelo["disponible"]:
        secciones.append("Uso de Suelo")
    if electrico["disponible"]:
        secciones.append("Infraestructura Electrica")
    secciones.append("Riesgos Territoriales")
    if infraestructura["disponible"]:
        secciones.append("Infraestructura y Logistica")
    if vecinos["disponible"]:
        secciones.append("Vecinos Productivos")
    secciones.append("Recomendaciones")

    return {
        "comuna": comuna,
        "region": region,
        "provincia": provincia,
        "lat": lat,
        "lon": lon,
        "tiene_datos": any([
            produccion["disponible"],
            agua["disponible"],
            uso_suelo["disponible"],
            electrico["disponible"],
        ]),
        "secciones_disponibles": secciones,
        "analisis_sitio": analisis_sitio,
        "produccion": produccion,
        "agua": agua,
        "uso_suelo": uso_suelo,
        "electrico": electrico,
        "riesgos": riesgos,
        "infraestructura": infraestructura,
        "vecinos": vecinos,
        "recomendaciones": recomendaciones,
    }


# ---------------------------------------------------------------------------
# CLI test
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import sys
    import json

    c = sys.argv[1] if len(sys.argv) > 1 else "Rancagua"
    lat_arg = float(sys.argv[2]) if len(sys.argv) > 2 else None
    lon_arg = float(sys.argv[3]) if len(sys.argv) > 3 else None

    r = generate_predial_report(c, lat=lat_arg, lon=lon_arg)

    print("=" * 70)
    print(f"INFORME PREDIAL: {r['comuna']}")
    print(f"Region: {r['region']} | Provincia: {r['provincia']}")
    print(f"Tiene datos: {r['tiene_datos']}")
    print(f"Secciones: {', '.join(r['secciones_disponibles'])}")
    print("=" * 70)

    # Section 1: Produccion
    p = r["produccion"]
    print(f"\n--- 1. PRODUCCION AGRICOLA [{p['semaforo'].upper()}] ---")
    print(f"Semaforo: {p['semaforo_texto']}")
    if p["disponible"]:
        print(f"Superficie total: {p['total_superficie_ha']} ha")
        print(f"Explotaciones: {p['total_explotaciones']}")
        for e in p["especies"][:5]:
            print(f"  {e['especie']}: {e['superficie_ha']} ha "
                  f"({e['num_explotaciones']} explotaciones)")
        if p["metodos_riego"]:
            print("Metodos de riego:")
            for m in p["metodos_riego"]:
                print(f"  {m['metodo']}: {m['superficie_ha']} ha ({m['pct']}%)")
        if p["tipo_productor"]:
            print("Tipo productor:")
            for t in p["tipo_productor"]:
                print(f"  {t['tipo']}: {t['hectareas']} ha ({t['num_explotaciones']} exp)")
        if p["comparacion_nacional"]:
            print("Comparacion nacional:")
            for cn in p["comparacion_nacional"]:
                print(f"  {cn['especie']}: {cn['pct_nacional']}% del total nacional")
    print(f"Texto: {p['texto_analitico']}")
    if p["alertas"]:
        print(f"Alertas: {'; '.join(p['alertas'])}")

    # Section 2: Agua
    a = r["agua"]
    print(f"\n--- 2. DISPONIBILIDAD DE AGUA [{a['semaforo'].upper()}] ---")
    print(f"Semaforo: {a['semaforo_texto']}")
    if a["disponible"]:
        print(f"Patentes: {a['num_patentes']}")
        print(f"Monto total: {_fmt_money(a['monto_total'])}")
        print(f"Saldo pendiente: {_fmt_money(a['saldo_total'])}")
        print(f"Morosidad: {a['morosidad_pct']}%")
        print(f"Patentes con deuda: {a['patentes_con_deuda']}")
        if a["por_concesion"]:
            print("Por concesion:")
            for cc in a["por_concesion"][:5]:
                print(f"  {cc['concesion']}: {cc['num_patentes']} patentes")
        if a["oficinas_cbr"]:
            print("Oficinas CBR:")
            for o in a["oficinas_cbr"]:
                print(f"  {o['oficina']}: {o['num_patentes']} patentes")
    print(f"Restriccion hidrica: {a['restriccion_hidrica']}")
    print(f"Agotamiento: {a['agotamiento']}")
    if a["infraestructura_cercana"]:
        print("Infraestructura cercana:")
        for ic in a["infraestructura_cercana"][:5]:
            print(f"  {ic['tipo']}: {ic['nombre']} ({ic['distancia_km']} km)")
    print(f"Texto: {a['texto_analitico']}")
    if a["alertas"]:
        print(f"Alertas: {'; '.join(a['alertas'])}")

    # Section 3: Uso de suelo
    u = r["uso_suelo"]
    print(f"\n--- 3. USO DE SUELO [{u['semaforo'].upper()}] ---")
    print(f"Semaforo: {u['semaforo_texto']}")
    if u["disponible"]:
        print(f"Total: {u['total_ha']} ha | Agricola: {u['pct_agricola']}%")
        for d in u["distribucion"][:5]:
            print(f"  {d['uso_tierra']}: {d['superficie_ha']} ha ({d['pct']}%)")
    print(f"Texto: {u['texto_analitico']}")

    # Section 4: Electrico
    el = r["electrico"]
    print(f"\n--- 4. INFRAESTRUCTURA ELECTRICA [{el['semaforo'].upper()}] ---")
    print(f"Semaforo: {el['semaforo_texto']}")
    if el["disponible"]:
        print(f"Clientes: {el['total_clientes']:,}")
        print(f"Potencia total: {el['potencia_total_kw']:,.0f} kW")
        print(f"Potencia promedio: {el['potencia_promedio_kw']:.1f} kW")
        for emp in el["empresas"]:
            print(f"  {emp['empresa']}: {emp['clientes']} clientes, "
                  f"{emp['potencia_kw']:.0f} kW")
        if el["comparacion_regional"]:
            print(f"vs Region: {el['comparacion_regional']['diff_pct']:+.1f}%")
    print(f"Texto: {el['texto_analitico']}")

    # Section 5: Riesgos
    ri = r["riesgos"]
    print(f"\n--- 5. RIESGOS TERRITORIALES [{ri['semaforo_global'].upper()}] ---")
    print(f"Semaforo: {ri['semaforo_texto']}")
    for item in ri["items"]:
        print(f"  [{item['semaforo'].upper()}] {item['riesgo']}: {item['estado']}")
    print(f"Texto: {ri['texto_analitico']}")

    # Section 6: Infraestructura
    inf = r["infraestructura"]
    print(f"\n--- 6. INFRAESTRUCTURA Y LOGISTICA ---")
    print(f"Disponible: {inf['disponible']}")
    if inf["cercanos"]:
        for ci in inf["cercanos"][:5]:
            print(f"  {ci['tipo']}: {ci['nombre']} ({ci['distancia_km']} km)")
    print(f"Texto: {inf['texto_analitico']}")

    # Section 7: Vecinos
    v = r["vecinos"]
    print(f"\n--- 7. VECINOS PRODUCTIVOS ---")
    print(f"Disponible: {v['disponible']}")
    if v["comunas_provincia"]:
        for cv in v["comunas_provincia"][:10]:
            print(f"  {cv['comuna']} - {cv['especie']}: {cv['hectareas']} ha")
    print(f"Texto: {v['texto_analitico']}")

    # Section 8: Recomendaciones
    rec = r["recomendaciones"]
    print(f"\n--- 8. RECOMENDACIONES ---")
    for ch in rec["checklist"]:
        print(f"  [ ] {ch['verificacion']}: {ch['accion']}")
    print(f"Texto: {rec['texto_final']}")

    print(f"\n{'=' * 70}")
    print(f"Comunas disponibles en BD: {len(get_available_comunas())}")
