"""
predial_engine.py — Motor de Informes Prediales por comuna.

Consulta SQLite databases:
  - catastro_fruticola.db  (SAG/CIREN — producción frutícola)
  - dga_derechos.db        (DGA — derechos de agua / patentes)
  - electrico.db           (SEC — infraestructura eléctrica)
"""

import os
import sqlite3
import logging
from typing import Optional

logger = logging.getLogger(__name__)

_BASE = os.path.dirname(os.path.abspath(__file__))
_DB_DIR = os.path.join(_BASE, "data", "db")

_DB_FILES = {
    "catastro": os.path.join(_DB_DIR, "catastro_fruticola.db"),
    "dga": os.path.join(_DB_DIR, "dga_derechos.db"),
    "electrico": os.path.join(_DB_DIR, "electrico.db"),
}


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


# ---------------------------------------------------------------------------
# Available comunas
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
# Section: Producción Agrícola
# ---------------------------------------------------------------------------

def _get_produccion(comuna: str) -> dict:
    result = {"disponible": False, "especies": [], "variedades": [],
              "metodos_riego": [], "total_superficie_ha": 0,
              "total_explotaciones": 0, "region": "", "provincia": ""}
    conn = _connect("catastro")
    if not conn:
        return result

    # resumen_por_comuna: (comuna, especie, superficie_ha, num_explotaciones)
    rows = conn.execute(
        "SELECT especie, superficie_ha, num_explotaciones "
        "FROM resumen_por_comuna WHERE UPPER(comuna) = UPPER(?) "
        "ORDER BY superficie_ha DESC", (comuna,)
    ).fetchall()

    if rows:
        result["disponible"] = True
        for r in rows:
            result["especies"].append({
                "especie": r["especie"],
                "superficie_ha": round(r["superficie_ha"], 2),
                "num_explotaciones": r["num_explotaciones"],
            })
        result["total_superficie_ha"] = round(
            sum(e["superficie_ha"] for e in result["especies"]), 2)
        result["total_explotaciones"] = sum(
            e["num_explotaciones"] for e in result["especies"])

    # Region/provincia from catastro_completo
    geo = conn.execute(
        'SELECT DISTINCT Region, Provincia FROM catastro_fruticola '
        'WHERE UPPER(Comuna) = UPPER(?) LIMIT 1', (comuna,)
    ).fetchone()
    if geo:
        result["region"] = geo["Region"] or ""
        result["provincia"] = geo["Provincia"] or ""

    # Varieties detail
    vars_ = conn.execute(
        'SELECT especie, variedad, ROUND(SUM("superficie_(ha)"), 2) as sup, '
        'COUNT(*) as cnt FROM catastro_completo '
        'WHERE UPPER(comuna) = UPPER(?) '
        'GROUP BY especie, variedad ORDER BY sup DESC LIMIT 30', (comuna,)
    ).fetchall()
    result["variedades"] = [
        {"especie": v["especie"], "variedad": v["variedad"],
         "superficie_ha": v["sup"] or 0, "registros": v["cnt"]}
        for v in vars_
    ]

    # Irrigation methods
    riego = conn.execute(
        'SELECT metodo_de_riego, COUNT(*) as cnt, '
        'ROUND(SUM("superficie_(ha)"), 2) as sup '
        'FROM catastro_completo WHERE UPPER(comuna) = UPPER(?) '
        'GROUP BY metodo_de_riego ORDER BY sup DESC', (comuna,)
    ).fetchall()
    result["metodos_riego"] = [
        {"metodo": r["metodo_de_riego"] or "Sin información",
         "registros": r["cnt"], "superficie_ha": r["sup"] or 0}
        for r in riego
    ]

    conn.close()
    return result


# ---------------------------------------------------------------------------
# Section: Agua (DGA)
# ---------------------------------------------------------------------------

def _get_agua(comuna: str) -> dict:
    result = {"disponible": False, "num_patentes": 0, "saldo_total": 0,
              "monto_total": 0, "region": "", "por_tipo": [], "por_anio": []}
    conn = _connect("dga")
    if not conn:
        return result

    row = conn.execute(
        "SELECT * FROM resumen_por_comuna WHERE UPPER(comuna) = UPPER(?)",
        (comuna,)
    ).fetchone()

    if row:
        result["disponible"] = True
        result["region"] = row["region"]
        result["num_patentes"] = row["num_patentes"]
        result["saldo_total"] = row["saldo_total"] or 0
        result["monto_total"] = row["monto_total"] or 0
        result["saldo_promedio"] = round(row["saldo_promedio"] or 0)
        result["monto_promedio"] = round(row["monto_promedio"] or 0)

    # By person type
    tipos = conn.execute(
        "SELECT tipo_persona, COUNT(*) as cnt, ROUND(SUM(saldo),0) as s "
        "FROM patentes WHERE UPPER(comuna) = UPPER(?) "
        "GROUP BY tipo_persona", (comuna,)
    ).fetchall()
    result["por_tipo"] = [
        {"tipo": t["tipo_persona"], "cantidad": t["cnt"], "saldo": t["s"] or 0}
        for t in tipos
    ]

    # By year
    anios = conn.execute(
        "SELECT CAST(anio_patente AS INTEGER) as anio, COUNT(*) as cnt, "
        "ROUND(SUM(saldo),0) as s FROM patentes "
        "WHERE UPPER(comuna) = UPPER(?) AND anio_patente IS NOT NULL "
        "GROUP BY anio ORDER BY anio", (comuna,)
    ).fetchall()
    result["por_anio"] = [
        {"anio": a["anio"], "cantidad": a["cnt"], "saldo": a["s"] or 0}
        for a in anios
    ]

    conn.close()
    return result


# ---------------------------------------------------------------------------
# Section: Eléctrico
# ---------------------------------------------------------------------------

def _get_electrico(comuna: str) -> dict:
    result = {"disponible": False, "total_clientes": 0,
              "potencia_total_kw": 0, "potencia_promedio_kw": 0,
              "region": "", "empresas": []}
    conn = _connect("electrico")
    if not conn:
        return result

    row = conn.execute(
        "SELECT * FROM clientes_por_comuna "
        "WHERE UPPER(nombre_comuna) = UPPER(?)", (comuna,)
    ).fetchone()

    if row:
        result["disponible"] = True
        result["total_clientes"] = row["total_clientes"]
        result["potencia_total_kw"] = round(row["potencia_total_kw"], 2)
        result["potencia_promedio_kw"] = round(row["potencia_promedio_kw"], 2)
        result["region"] = row["nombre_region"]

    empresas = conn.execute(
        "SELECT empresa, total_clientes, potencia_total_kw "
        "FROM clientes_por_empresa_comuna "
        "WHERE UPPER(nombre_comuna) = UPPER(?) "
        "ORDER BY potencia_total_kw DESC", (comuna,)
    ).fetchall()
    result["empresas"] = [
        {"empresa": e["empresa"], "clientes": e["total_clientes"],
         "potencia_kw": round(e["potencia_total_kw"], 2)}
        for e in empresas
    ]

    conn.close()
    return result


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def generate_predial_report(comuna: str) -> dict:
    """Generate a complete predial report for a comuna."""
    prod = _get_produccion(comuna)
    agua = _get_agua(comuna)
    elec = _get_electrico(comuna)

    secciones = []
    if prod["disponible"]:
        secciones.append("Producción Agrícola")
    if agua["disponible"]:
        secciones.append("Derechos de Agua")
    if elec["disponible"]:
        secciones.append("Infraestructura Eléctrica")

    region = prod.get("region") or agua.get("region") or elec.get("region") or ""

    return {
        "comuna": comuna,
        "region": region,
        "provincia": prod.get("provincia", ""),
        "tiene_datos": len(secciones) > 0,
        "secciones_disponibles": secciones,
        "produccion": prod,
        "agua": agua,
        "electrico": elec,
    }


# ---------------------------------------------------------------------------
# CLI test
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import sys, json
    c = sys.argv[1] if len(sys.argv) > 1 else "Rancagua"
    r = generate_predial_report(c)
    print(f"Comuna: {r['comuna']} | Region: {r['region']}")
    print(f"Secciones: {r['secciones_disponibles']}")
    if r["produccion"]["disponible"]:
        print(f"Superficie frutícola: {r['produccion']['total_superficie_ha']} ha")
        for e in r["produccion"]["especies"][:5]:
            print(f"  {e['especie']}: {e['superficie_ha']} ha")
    if r["agua"]["disponible"]:
        print(f"Patentes agua: {r['agua']['num_patentes']}")
    if r["electrico"]["disponible"]:
        print(f"Clientes eléctricos: {r['electrico']['total_clientes']}")
    print(f"\nComunas disponibles: {len(get_available_comunas())}")
