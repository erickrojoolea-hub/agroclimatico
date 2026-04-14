"""
sii_engine.py — Motor de consulta de predios SII
Consulta la base de datos de 2M predios rurales de Chile (SII/catastral.cl).

Dos fuentes:
  1. GPKG principal (predios_rurales_chile_2025S2.gpkg) — 2M filas, 69K con lat/lon
  2. GPKGs por comuna (PKG/*.gpkg) — 36 comunas con polígonos + datos enriquecidos

Búsqueda por proximidad lat/lon. Si no encuentra, retorna None.
"""

import sqlite3
import math
import os
import json
import struct
from pathlib import Path
from typing import Optional

# ── Rutas ────────────────────────────────────────────────────────────────────
_BASE = Path("/Users/erickrojoolea/Desktop/Data Estado/Scrapping Estado/Data SII")
_GPKG_MAIN = _BASE / "predios_rurales_chile_2025S2.gpkg"
_GPKG_PKG_DIR = _BASE / "PKG"

# ── Destinos SII ────────────────────────────────────────────────────────────
DESTINOS_SII = {
    "A": "Agrícola",
    "W": "Sitio eriazo",
    "H": "Habitacional",
    "F": "Forestal",
    "V": "Otros/varios",
    "C": "Comercial",
    "L": "Hotel/turismo",
    "I": "Industrial",
    "E": "Educación/cultura",
    "Q": "Salud",
}

# ── Cache ────────────────────────────────────────────────────────────────────
_pkg_tables_cache: dict = {}
_pkg_bbox_cache: dict = {}  # {gpkg_path: (min_lat, max_lat, min_lon, max_lon)}
_pkg_bbox_loaded = False


def _haversine(lat1, lon1, lat2, lon2):
    """Distancia en km entre dos puntos."""
    R = 6371.0
    rlat1, rlon1, rlat2, rlon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = rlat2 - rlat1
    dlon = rlon2 - rlon1
    a = math.sin(dlat / 2) ** 2 + math.cos(rlat1) * math.cos(rlat2) * math.sin(dlon / 2) ** 2
    return R * 2 * math.asin(math.sqrt(a))


def _safe_float(val, default=0.0):
    """Convierte a float de forma segura."""
    if val is None or val == '':
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def _safe_int(val, default=0):
    if val is None or val == '':
        return default
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return default


# ── Parseo GeoPackage WKB → GeoJSON ─────────────────────────────────────────

def _gpkg_geom_to_coords(blob):
    """
    Extrae coordenadas simplificadas del blob GeoPackage.
    Retorna lista de anillos [[lon,lat], ...] para el primer polígono,
    o None si no se puede parsear.
    """
    if not blob or len(blob) < 8:
        return None
    try:
        # GeoPackage Binary header
        magic = blob[0:2]
        if magic != b'GP':
            return None
        flags = blob[3]
        byte_order = flags & 0x01  # 0=big-endian, 1=little-endian
        bo = '<' if byte_order else '>'
        envelope_type = (flags >> 1) & 0x07
        envelope_sizes = {0: 0, 1: 32, 2: 48, 3: 48, 4: 64}
        env_size = envelope_sizes.get(envelope_type, 0)
        wkb_start = 8 + env_size

        # Parse WKB
        wkb = blob[wkb_start:]
        if len(wkb) < 5:
            return None

        wkb_bo = '<' if wkb[0] == 1 else '>'
        geom_type = struct.unpack(wkb_bo + 'I', wkb[1:5])[0]

        # MultiPolygon (type 6) → get first polygon
        if geom_type == 6:
            n_polys = struct.unpack(wkb_bo + 'I', wkb[5:9])[0]
            if n_polys == 0:
                return None
            # First polygon starts after MultiPolygon header
            # It has its own WKB header (5 bytes)
            poly_wkb = wkb[9:]
            poly_bo = '<' if poly_wkb[0] == 1 else '>'
            return _parse_polygon_wkb(poly_wkb[5:], poly_bo)

        # Polygon (type 3)
        elif geom_type == 3:
            return _parse_polygon_wkb(wkb[5:], wkb_bo)

        return None
    except Exception:
        return None


def _parse_polygon_wkb(data, bo):
    """Parse polygon WKB body (after type header)."""
    try:
        n_rings = struct.unpack(bo + 'I', data[0:4])[0]
        if n_rings == 0:
            return None
        offset = 4
        # Only read first (exterior) ring
        n_pts = struct.unpack(bo + 'I', data[offset:offset + 4])[0]
        offset += 4
        coords = []
        for _ in range(n_pts):
            x, y = struct.unpack(bo + 'dd', data[offset:offset + 16])
            coords.append([round(x, 6), round(y, 6)])
            offset += 16
        return coords
    except Exception:
        return None


# ── Búsqueda en GPKG principal ───────────────────────────────────────────────

def _search_main_gpkg(lat, lon, max_km=1.0):
    """
    Busca predios en el GPKG principal por proximidad.
    """
    if not _GPKG_MAIN.exists():
        return []

    # Convert max_km to approximate degrees
    delta = max_km / 111.0 * 1.5  # 1 degree ≈ 111 km, with margin

    try:
        conn = sqlite3.connect(str(_GPKG_MAIN))
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute("""
            SELECT comuna, manzana, predio, rol, direccion_sii,
                   lat, lon, avaluo_total, avaluo_exento,
                   sup_terreno, destino, cuota_trimestral, serie,
                   CASE WHEN geom IS NOT NULL THEN 1 ELSE 0 END as has_geom,
                   geom
            FROM rurales
            WHERE lat BETWEEN ? AND ?
              AND lon BETWEEN ? AND ?
            ORDER BY (lat-?)*(lat-?) + (lon-?)*(lon-?)
            LIMIT 10
        """, (lat - delta, lat + delta, lon - delta, lon + delta,
              lat, lat, lon, lon))
        rows = c.fetchall()
        conn.close()

        results = []
        for r in rows:
            rlat, rlon = _safe_float(r['lat']), _safe_float(r['lon'])
            dist = _haversine(lat, lon, rlat, rlon)
            if dist > max_km:
                continue

            # Parse geometry if present
            polygon = None
            if r['has_geom'] and r['geom']:
                polygon = _gpkg_geom_to_coords(r['geom'])

            results.append({
                'fuente': 'gpkg_principal',
                'comuna_cut': r['comuna'],
                'manzana': r['manzana'],
                'predio': r['predio'],
                'rol': r['rol'] or f"{r['comuna']}-{r['manzana']}-{r['predio']}",
                'direccion': (r['direccion_sii'] or '').strip(),
                'lat': rlat,
                'lon': rlon,
                'avaluo_total': _safe_int(r['avaluo_total']),
                'avaluo_exento': _safe_int(r['avaluo_exento']),
                'sup_terreno_m2': _safe_float(r['sup_terreno']),
                'destino_cod': r['destino'] or '',
                'destino_desc': DESTINOS_SII.get(r['destino'], r['destino'] or ''),
                'cuota_trimestral': _safe_int(r['cuota_trimestral']),
                'serie': r['serie'] or '',
                'distancia_km': round(dist, 3),
                'has_polygon': polygon is not None,
                'polygon_coords': polygon,
            })
        return results
    except Exception as e:
        print(f"[sii_engine] Error GPKG principal: {e}")
        return []


# ── Búsqueda en GPKGs por comuna ────────────────────────────────────────────

def _get_pkg_table_name(gpkg_path):
    """Obtiene el nombre de tabla de features de un GPKG."""
    if gpkg_path in _pkg_tables_cache:
        return _pkg_tables_cache[gpkg_path]
    try:
        conn = sqlite3.connect(str(gpkg_path))
        c = conn.cursor()
        c.execute("SELECT table_name FROM gpkg_contents WHERE data_type='features'")
        row = c.fetchone()
        conn.close()
        name = row[0] if row else None
        _pkg_tables_cache[gpkg_path] = name
        return name
    except Exception:
        _pkg_tables_cache[gpkg_path] = None
        return None


def _load_pkg_bboxes():
    """Pre-carga bounding boxes de cada GPKG por comuna (una sola vez)."""
    global _pkg_bbox_loaded
    if _pkg_bbox_loaded or not _GPKG_PKG_DIR.exists():
        return

    cache_file = _GPKG_PKG_DIR / "_bbox_cache.json"

    # Try loading from disk cache
    if cache_file.exists():
        try:
            with open(cache_file, 'r') as f:
                data = json.load(f)
            for k, v in data.items():
                _pkg_bbox_cache[k] = tuple(v)
            _pkg_bbox_loaded = True
            return
        except Exception:
            pass

    # Build cache by scanning each GPKG
    for gpkg_file in _GPKG_PKG_DIR.glob("*.gpkg"):
        if gpkg_file.name.startswith("_"):
            continue
        try:
            table = _get_pkg_table_name(str(gpkg_file))
            if not table:
                continue
            conn = sqlite3.connect(str(gpkg_file))
            c = conn.cursor()
            c.execute(f"""
                SELECT MIN(CAST(lat AS REAL)), MAX(CAST(lat AS REAL)),
                       MIN(CAST(lon AS REAL)), MAX(CAST(lon AS REAL))
                FROM "{table}"
                WHERE lat IS NOT NULL AND lat != '' AND CAST(lat AS REAL) != 0
            """)
            row = c.fetchone()
            conn.close()
            if row and row[0] is not None:
                _pkg_bbox_cache[str(gpkg_file)] = (row[0], row[1], row[2], row[3])
        except Exception:
            continue

    # Save to disk
    try:
        with open(cache_file, 'w') as f:
            json.dump({k: list(v) for k, v in _pkg_bbox_cache.items()}, f)
    except Exception:
        pass

    _pkg_bbox_loaded = True


def _search_pkg_gpkgs(lat, lon, max_km=0.5):
    """
    Busca en los GPKGs por comuna. Estos tienen más datos y polígonos.
    Pre-filtra por bounding box para evitar escanear archivos innecesarios.
    """
    if not _GPKG_PKG_DIR.exists():
        return []

    _load_pkg_bboxes()

    delta = max_km / 111.0 * 1.5
    results = []

    # Only check GPKGs whose bounding box contains our search area
    candidate_files = []
    for gpkg_path, bbox in _pkg_bbox_cache.items():
        min_lat, max_lat, min_lon, max_lon = bbox
        if (lat - delta <= max_lat and lat + delta >= min_lat and
                lon - delta <= max_lon and lon + delta >= min_lon):
            candidate_files.append(gpkg_path)

    for gpkg_path in candidate_files:
        try:
            table = _get_pkg_table_name(gpkg_path)
            if not table:
                continue

            conn = sqlite3.connect(gpkg_path)
            conn.row_factory = sqlite3.Row
            c = conn.cursor()

            # lat/lon are TEXT in PKG files
            c.execute(f"""
                SELECT rol, lat, lon, supTerreno, valorTotal, valorExento,
                       destinoDescripcion, direccion_sii, nombreComuna, ubicacion,
                       txt_cuota_trimestral, serie, pol_area_m2,
                       manzana, predio, comuna,
                       txt_cod_destino, valorComercial_clp_m2,
                       geom
                FROM "{table}"
                WHERE CAST(lat AS REAL) BETWEEN ? AND ?
                  AND CAST(lon AS REAL) BETWEEN ? AND ?
                ORDER BY (CAST(lat AS REAL)-?)*(CAST(lat AS REAL)-?) +
                         (CAST(lon AS REAL)-?)*(CAST(lon AS REAL)-?)
                LIMIT 5
            """, (lat - delta, lat + delta, lon - delta, lon + delta,
                  lat, lat, lon, lon))

            for r in c.fetchall():
                rlat = _safe_float(r['lat'])
                rlon = _safe_float(r['lon'])
                if rlat == 0 and rlon == 0:
                    continue
                dist = _haversine(lat, lon, rlat, rlon)
                if dist > max_km:
                    continue

                polygon = None
                if r['geom']:
                    polygon = _gpkg_geom_to_coords(r['geom'])

                sup_terreno = _safe_float(r['supTerreno'])
                pol_area = _safe_float(r['pol_area_m2'])

                results.append({
                    'fuente': 'gpkg_comuna',
                    'comuna_cut': r['comuna'] or '',
                    'comuna_nombre': (r['nombreComuna'] or '').strip(),
                    'manzana': r['manzana'] or '',
                    'predio': r['predio'] or '',
                    'rol': r['rol'] or '',
                    'direccion': (r['direccion_sii'] or '').strip(),
                    'lat': rlat,
                    'lon': rlon,
                    'avaluo_total': _safe_int(r['valorTotal']),
                    'avaluo_exento': _safe_int(r['valorExento']),
                    'sup_terreno_m2': sup_terreno if sup_terreno > 0 else pol_area,
                    'destino_cod': (r['txt_cod_destino'] or '').strip(),
                    'destino_desc': (r['destinoDescripcion'] or '').strip(),
                    'cuota_trimestral': _safe_int(r['txt_cuota_trimestral']),
                    'serie': (r['serie'] or '').strip(),
                    'ubicacion': (r['ubicacion'] or '').strip(),
                    'valor_comercial_m2': _safe_int(r['valorComercial_clp_m2']),
                    'pol_area_m2': pol_area,
                    'distancia_km': round(dist, 3),
                    'has_polygon': polygon is not None,
                    'polygon_coords': polygon,
                })

            conn.close()
        except Exception as e:
            continue

    # Sort by distance
    results.sort(key=lambda x: x['distancia_km'])
    return results


# ── API pública ──────────────────────────────────────────────────────────────

def buscar_predio(lat: float, lon: float, max_km: float = 0.5) -> Optional[dict]:
    """
    Busca el predio más cercano a un punto.
    Primero busca en GPKGs por comuna (más datos), luego en GPKG principal.

    Returns: dict con datos del predio, o None si no se encuentra.
    """
    # 1. Buscar en GPKGs por comuna (más completos)
    pkg_results = _search_pkg_gpkgs(lat, lon, max_km=max_km)
    if pkg_results:
        return pkg_results[0]

    # 2. Fallback: GPKG principal
    main_results = _search_main_gpkg(lat, lon, max_km=max_km)
    if main_results:
        return main_results[0]

    return None


def buscar_predios_cercanos(lat: float, lon: float, max_km: float = 2.0,
                            max_results: int = 10) -> list:
    """
    Busca todos los predios cercanos (para vecindario, estadísticas).
    Combina ambas fuentes y deduplica por ROL.
    """
    pkg = _search_pkg_gpkgs(lat, lon, max_km=max_km)
    main = _search_main_gpkg(lat, lon, max_km=max_km)

    # Combine and deduplicate
    seen_roles = set()
    combined = []
    for r in pkg + main:
        rol = r.get('rol', '')
        if rol and rol in seen_roles:
            continue
        seen_roles.add(rol)
        combined.append(r)

    combined.sort(key=lambda x: x['distancia_km'])
    return combined[:max_results]


def estadisticas_vecindario(lat: float, lon: float, max_km: float = 5.0) -> dict:
    """
    Estadísticas agregadas del vecindario: conteo por destino, avalúo promedio,
    superficie promedio, etc.
    """
    predios = buscar_predios_cercanos(lat, lon, max_km=max_km, max_results=100)
    if not predios:
        return {}

    por_destino = {}
    avaluos = []
    superficies = []

    for p in predios:
        dest = p.get('destino_desc', 'Sin info')
        por_destino[dest] = por_destino.get(dest, 0) + 1
        av = p.get('avaluo_total', 0)
        if av > 0:
            avaluos.append(av)
        sup = p.get('sup_terreno_m2', 0)
        if sup > 0:
            superficies.append(sup)

    return {
        'total_predios': len(predios),
        'radio_km': max_km,
        'por_destino': por_destino,
        'avaluo_promedio': int(sum(avaluos) / len(avaluos)) if avaluos else 0,
        'avaluo_mediano': int(sorted(avaluos)[len(avaluos) // 2]) if avaluos else 0,
        'sup_promedio_m2': round(sum(superficies) / len(superficies), 1) if superficies else 0,
        'sup_promedio_ha': round(sum(superficies) / len(superficies) / 10000, 2) if superficies else 0,
    }


def formato_clp(valor: int) -> str:
    """Formatea un valor en CLP: $1.234.567"""
    if valor == 0:
        return "$0"
    return f"${valor:,.0f}".replace(",", ".")


def formato_superficie(m2: float) -> str:
    """Formatea superficie: m² si < 10000, ha si >= 10000."""
    if m2 <= 0:
        return "Sin info"
    if m2 < 10000:
        return f"{m2:,.0f} m²".replace(",", ".")
    ha = m2 / 10000
    return f"{ha:,.2f} ha".replace(",", ".")


# ── Test ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import time

    # Test La Serena (should have PKG data)
    t0 = time.time()
    result = buscar_predio(-29.91, -71.25)
    t1 = time.time()
    if result:
        print(f"La Serena ({t1 - t0:.3f}s):")
        print(f"  ROL: {result['rol']}")
        print(f"  Dirección: {result['direccion']}")
        print(f"  Destino: {result['destino_desc']}")
        print(f"  Avalúo: {formato_clp(result['avaluo_total'])}")
        print(f"  Superficie: {formato_superficie(result['sup_terreno_m2'])}")
        print(f"  Distancia: {result['distancia_km']} km")
        print(f"  Polígono: {'Sí' if result['has_polygon'] else 'No'}")
    else:
        print("La Serena: No encontrado")

    # Test Curacaví (may not have data)
    t0 = time.time()
    result2 = buscar_predio(-33.40, -71.13)
    t1 = time.time()
    print(f"\nCuracaví ({t1 - t0:.3f}s): {'Encontrado' if result2 else 'No disponible'}")

    # Stats
    t0 = time.time()
    stats = estadisticas_vecindario(-29.91, -71.25)
    t1 = time.time()
    if stats:
        print(f"\nVecindario La Serena ({t1 - t0:.3f}s):")
        print(f"  Total predios: {stats['total_predios']}")
        print(f"  Por destino: {stats['por_destino']}")
        print(f"  Avalúo promedio: {formato_clp(stats['avaluo_promedio'])}")
