#!/usr/bin/env python3
"""
LECTOR: CR2MET v2.0 (Productos Grillados)
==========================================
Lee datos grillados CR2MET de precipitación y temperatura mínima.

Archivos fuente (en datos_geo/CR2/):
  - CR2MET_pr_v2.0_day_1979_2020_005deg.nc    (precipitación diaria, ~10 GB)
  - CR2MET_tmin_v2.0_day_1979_2020_005deg.nc   (temp. mínima diaria, ~10 GB)

Formato: NetCDF con dims (time, lat, lon), resolución 0.05° (~5.5 km)
Período: 1979-01-01 a 2020-04-30 (15,096 días)
Grilla: 800 lat × 220 lon

Referencia:
  Boisier, J.P. et al. (2018). CR2MET. Centro de Ciencia del Clima y la
  Resiliencia, Universidad de Chile. https://www.cr2.cl/datos-productos-grillados/
"""

import os
import json
import numpy as np

_PROJECT_ROOT = os.path.join(os.path.dirname(__file__), '..')
BASE_CR2 = os.path.join(_PROJECT_ROOT, 'datos_geo', 'CR2')

PR_NC = os.path.join(BASE_CR2, 'CR2MET_pr_v2.0_day_1979_2020_005deg.nc')
TMIN_NC = os.path.join(BASE_CR2, 'CR2MET_tmin_v2.0_day_1979_2020_005deg.nc')

# Cache de climatologías pre-procesadas
# Buscar primero en data/climate/ (repo-local), luego en datos_precipitacion/ (symlink)
def _find_cache(name):
    for subdir in ('data/climate', 'datos_precipitacion'):
        p = os.path.join(_PROJECT_ROOT, subdir, name)
        if os.path.exists(p):
            return p
    return os.path.join(_PROJECT_ROOT, 'data', 'climate', name)

CACHE_DIR = os.path.join(_PROJECT_ROOT, 'datos_precipitacion')
PR_CACHE = _find_cache('cr2met_climatologia_puntos.json')
TMIN_CACHE = _find_cache('cr2met_tmin_heladas_cache.json')

# Caches densos (grilla completa pre-procesada)
PR_GRID_CACHE = _find_cache('cr2met_precipitacion_grilla.json')
HELADAS_GRID_CACHE = _find_cache('cr2met_heladas_grilla.json')
ESTACIONES_CLIM_CACHE = _find_cache('cr2_estaciones_climatologia.json')

# Caches en memoria (se cargan una vez)
_pr_grid_data = None
_heladas_grid_data = None
_estaciones_clim_data = None


def _haversine_simple(lat1, lon1, lat2, lon2):
    """Distancia aproximada en km (fórmula rápida)."""
    import math
    dlat = (lat2 - lat1) * 111.0
    dlon = (lon2 - lon1) * 111.0 * abs(math.cos(math.radians(lat1)))
    return (dlat**2 + dlon**2) ** 0.5


def _cargar_grilla_precipitacion():
    """Carga grilla densa de precipitación (lazy, una sola vez)."""
    global _pr_grid_data
    if _pr_grid_data is not None:
        return _pr_grid_data
    if os.path.exists(PR_GRID_CACHE):
        with open(PR_GRID_CACHE, 'r', encoding='utf-8') as f:
            _pr_grid_data = json.load(f)
        return _pr_grid_data
    return None


def _cargar_grilla_heladas():
    """Carga grilla densa de heladas (lazy, una sola vez)."""
    global _heladas_grid_data
    if _heladas_grid_data is not None:
        return _heladas_grid_data
    if os.path.exists(HELADAS_GRID_CACHE):
        with open(HELADAS_GRID_CACHE, 'r', encoding='utf-8') as f:
            _heladas_grid_data = json.load(f)
        return _heladas_grid_data
    return None


def _cargar_estaciones_climatologia():
    """Carga climatologías pre-calculadas de estaciones CR2 (lazy)."""
    global _estaciones_clim_data
    if _estaciones_clim_data is not None:
        return _estaciones_clim_data
    if os.path.exists(ESTACIONES_CLIM_CACHE):
        with open(ESTACIONES_CLIM_CACHE, 'r', encoding='utf-8') as f:
            _estaciones_clim_data = json.load(f)
        return _estaciones_clim_data
    return None


def precipitacion_grilla_punto(lat, lon, max_dist_km=15):
    """
    Obtiene climatología de precipitación desde la grilla densa pre-procesada.
    Usa IDW con los 4 puntos más cercanos de la grilla.

    Retorna: dict compatible con el formato existente, o None
    """
    grid = _cargar_grilla_precipitacion()
    if not grid or 'puntos' not in grid:
        return None

    puntos = grid['puntos']

    # Encontrar los 4 puntos más cercanos
    cercanos = []
    for p in puntos:
        d = _haversine_simple(lat, lon, p.get('lat_real', p['lat']), p.get('lon_real', p['lon']))
        if d < max_dist_km:
            cercanos.append((d, p))

    if not cercanos:
        return None

    cercanos.sort(key=lambda x: x[0])
    cercanos = cercanos[:4]

    # Si el punto más cercano está a <1km, usar directamente
    if cercanos[0][0] < 1.0:
        p = cercanos[0][1]
        return p['precip_monthly_mm'], p['precip_annual_mm'], p.get('stats_anuales'), p.get('extremos_diarios')

    # IDW con peso = 1/d²
    monthly = [0.0] * 12
    peso_total = 0
    annual_vals = []

    for d, p in cercanos:
        w = 1.0 / max(d, 0.01) ** 2
        peso_total += w
        for m in range(12):
            monthly[m] += p['precip_monthly_mm'][m] * w
        annual_vals.append(p['precip_annual_mm'])

    monthly = [round(v / peso_total, 1) for v in monthly]
    annual = round(sum(monthly), 1)

    # Stats del punto más cercano (no se interpolan bien)
    stats = cercanos[0][1].get('stats_anuales')
    extremos = cercanos[0][1].get('extremos_diarios')

    return monthly, annual, stats, extremos


def climatologia_mensual_punto_cache(lat, lon, variable='pr'):
    """
    Wrapper: intenta obtener climatología desde cache denso,
    si no existe usa NetCDF directamente.

    Retorna: lista de 12 valores [ene..dic]
    """
    if variable == 'pr':
        result = precipitacion_grilla_punto(lat, lon)
        if result:
            return result[0]  # solo monthly

    # Fallback a NetCDF
    nc_path = PR_NC if variable == 'pr' else TMIN_NC
    if os.path.exists(nc_path):
        return climatologia_mensual_punto(nc_path, variable, lat, lon)

    return None


def precipitacion_completa_punto(lat, lon):
    """
    Obtiene toda la info de precipitación para un punto:
    mensual, anual, stats, extremos, megasequía.
    Usa grilla densa primero, NetCDF como fallback.

    Retorna: dict con toda la información, o None
    """
    # 1. Intentar grilla densa
    result = precipitacion_grilla_punto(lat, lon)
    if result:
        monthly, annual, stats, extremos = result
        return {
            'mensual_mm': monthly,
            'anual_mm': annual,
            'stats_anuales': stats,
            'extremos_diarios': extremos,
            'fuente': 'CR2MET grillado (cache denso, 0.1°)',
        }

    # 2. Fallback: NetCDF directo
    if tiene_precipitacion():
        monthly = climatologia_mensual_punto(PR_NC, 'pr', lat, lon)
        ext = stats_extremos_punto(PR_NC, 'pr', lat, lon)
        return {
            'mensual_mm': monthly,
            'anual_mm': round(sum(monthly), 1),
            'stats_anuales': None,
            'extremos_diarios': ext,
            'fuente': 'CR2MET grillado (NetCDF directo)',
        }

    return None


def heladas_grilla_punto(lat, lon, max_dist_km=15):
    """
    Obtiene datos de heladas desde la grilla densa pre-procesada.
    Usa el punto más cercano (no interpola heladas, es más preciso).

    Retorna: dict compatible con heladas_reales_punto(), o None
    """
    grid = _cargar_grilla_heladas()
    if not grid or 'puntos' not in grid:
        return None

    puntos = grid['puntos']

    # Buscar punto más cercano
    mejor = None
    mejor_dist = float('inf')

    for key, p in puntos.items():
        d = _haversine_simple(lat, lon, p.get('lat_real', p['lat']), p.get('lon_real', p['lon']))
        if d < mejor_dist:
            mejor_dist = d
            mejor = p

    if mejor and mejor_dist < max_dist_km:
        # Adaptar campos para compatibilidad con formatos existentes
        por_mes_compat = []
        for m in mejor['por_mes']:
            mc = dict(m)
            if 'tmin_min_abs_C' in mc and 'tmin_minima_abs_C' not in mc:
                mc['tmin_minima_abs_C'] = mc['tmin_min_abs_C']
            if 'dias_helada_año' in mc and 'dias_helada_por_año' not in mc:
                mc['dias_helada_por_año'] = mc['dias_helada_año']
            por_mes_compat.append(mc)

        # Reconstruir meses_sin_helada si fue removido en compresión
        meses_sin = mejor.get('meses_sin_helada')
        if meses_sin is None:
            meses_sin = [pm['mes'] for pm in por_mes_compat
                        if pm.get('prob_helada_mensual', 1) < 0.05]

        periodo = mejor.get('periodo', '1991-2020')

        return {
            'fuente': mejor.get('fuente', 'CR2MET_tmin_v2.0_REAL'),
            'periodo': periodo,
            'por_mes': por_mes_compat,
            'tmin_absoluta_C': mejor.get('tmin_absoluta_C'),
            'dias_helada_año_promedio': mejor.get('dias_helada_año_promedio'),
            'periodo_libre_heladas_dias': mejor.get('periodo_libre_heladas_dias',
                                                     len(meses_sin) * 30),
            'meses_sin_helada': meses_sin,
            'dist_grilla_km': round(mejor_dist, 1),
            'metodologia': (
                f"Cache denso CR2MET v2.0, punto más cercano a {mejor_dist:.1f} km. "
                f"Período {periodo}."
            ),
        }

    return None


def tiene_precipitacion():
    """Verifica si el archivo de precipitación CR2MET existe."""
    return os.path.exists(PR_NC)


def tiene_temperatura():
    """Verifica si el archivo de temperatura mínima CR2MET existe."""
    return os.path.exists(TMIN_NC)


def extraer_serie_punto(nc_path, variable, lat, lon, fecha_inicio=None, fecha_fin=None):
    """
    Extrae serie temporal completa para un punto del NetCDF.

    Parámetros:
        nc_path: ruta al archivo .nc
        variable: 'pr' o 'tmin'
        lat, lon: coordenadas del punto
        fecha_inicio, fecha_fin: strings 'YYYY-MM-DD' (opcional)

    Retorna: dict {fecha_str: valor} (diario)
    """
    import xarray as xr

    ds = xr.open_dataset(nc_path)
    pixel = ds[variable].sel(lat=lat, lon=lon, method='nearest')

    if fecha_inicio:
        pixel = pixel.sel(time=slice(fecha_inicio, fecha_fin))

    pixel_data = pixel.load()
    ds.close()

    serie = {}
    for i, t in enumerate(pixel_data.time.values):
        fecha = str(t)[:10]
        val = float(pixel_data.values[i])
        if not np.isnan(val):
            serie[fecha] = round(val, 2)

    return serie


def climatologia_mensual_punto(nc_path, variable, lat, lon, periodo=(1991, 2020)):
    """
    Calcula climatología mensual para un punto.

    Retorna: lista de 12 valores [ene..dic]
    """
    import xarray as xr

    ds = xr.open_dataset(nc_path)
    pixel = ds[variable].sel(lat=lat, lon=lon, method='nearest')
    pixel = pixel.sel(time=slice(f'{periodo[0]}-01-01', f'{periodo[1]}-12-31'))

    if variable == 'pr':
        # Precipitación: suma mensual, luego promedio de cada mes
        monthly = pixel.resample(time='M').sum()
    else:
        # Temperatura: promedio mensual
        monthly = pixel.resample(time='M').mean()

    clim = monthly.groupby('time.month').mean().load().values
    ds.close()

    return [round(float(v), 1) if not np.isnan(v) else 0.0 for v in clim]


def stats_extremos_punto(nc_path, variable, lat, lon, periodo=(1991, 2020)):
    """
    Calcula estadísticas de extremos para un punto.

    Para precipitación: P95, P99, máximo, días de lluvia
    Para temperatura: mínima absoluta, días bajo 0°C, etc.
    """
    import xarray as xr

    ds = xr.open_dataset(nc_path)
    pixel = ds[variable].sel(lat=lat, lon=lon, method='nearest')
    pixel = pixel.sel(time=slice(f'{periodo[0]}-01-01', f'{periodo[1]}-12-31'))
    data = pixel.load().values
    ds.close()

    valid = data[~np.isnan(data)]

    if variable == 'pr':
        rain_days = valid[valid > 1.0]
        n_years = periodo[1] - periodo[0] + 1
        return {
            'p95_mm': round(float(np.percentile(rain_days, 95)), 1) if len(rain_days) > 100 else 0,
            'p99_mm': round(float(np.percentile(rain_days, 99)), 1) if len(rain_days) > 100 else 0,
            'max_diario_mm': round(float(np.max(valid)), 1) if len(valid) > 0 else 0,
            'dias_lluvia_por_año': round(len(rain_days) / n_years, 1),
        }
    else:  # tmin
        n_years = periodo[1] - periodo[0] + 1
        dias_helada = np.sum(valid < 0)
        dias_helada_severa = np.sum(valid < -2)

        # Stats por mes
        times = pixel.time.values
        heladas_por_mes = [0] * 12
        total_por_mes = [0] * 12
        for i, t in enumerate(times):
            m = int(str(t)[5:7]) - 1
            total_por_mes[m] += 1
            if not np.isnan(data[i]) and data[i] < 0:
                heladas_por_mes[m] += 1

        prob_helada_mes = []
        for m in range(12):
            if total_por_mes[m] > 0:
                prob_helada_mes.append(round(heladas_por_mes[m] / total_por_mes[m], 4))
            else:
                prob_helada_mes.append(0)

        return {
            'tmin_absoluta_C': round(float(np.min(valid)), 1) if len(valid) > 0 else None,
            'tmin_media_anual_C': round(float(np.mean(valid)), 1),
            'dias_helada_por_año': round(dias_helada / n_years, 1),
            'dias_helada_severa_por_año': round(dias_helada_severa / n_years, 1),
            'prob_helada_diaria_por_mes': prob_helada_mes,
            'tmin_mensual_media': list(climatologia_mensual_punto(
                nc_path, variable, pixel.lat.values, pixel.lon.values, periodo
            )) if False else None,  # evitar doble lectura
        }


def heladas_reales_punto(lat, lon, periodo=(1991, 2020)):
    """
    Calcula estadísticas de heladas REALES desde CR2MET Tmin.
    Esta es la función estrella: reemplaza el modelo empírico por datos observados.

    Retorna: dict con probabilidades mensuales de helada, PLH, extremos
    """
    if not tiene_temperatura():
        return None

    import xarray as xr

    ds = xr.open_dataset(TMIN_NC)
    pixel = ds['tmin'].sel(lat=lat, lon=lon, method='nearest')
    pixel = pixel.sel(time=slice(f'{periodo[0]}-01-01', f'{periodo[1]}-12-31'))
    data = pixel.load()
    ds.close()

    values = data.values
    times = data.time.values
    n_years = periodo[1] - periodo[0] + 1

    nombre_mes = ['Ene', 'Feb', 'Mar', 'Abr', 'May', 'Jun',
                  'Jul', 'Ago', 'Sep', 'Oct', 'Nov', 'Dic']

    # Probabilidad diaria de helada por mes
    heladas_mes = [0] * 12
    total_mes = [0] * 12
    tmin_sum = [0.0] * 12
    tmin_min = [999.0] * 12
    tmin_vals_mes = [[] for _ in range(12)]

    for i, t in enumerate(times):
        m = int(str(t)[5:7]) - 1
        v = float(values[i])
        if not np.isnan(v):
            total_mes[m] += 1
            tmin_sum[m] += v
            tmin_vals_mes[m].append(v)
            if v < tmin_min[m]:
                tmin_min[m] = v
            if v < 0:
                heladas_mes[m] += 1

    # Calcular estadísticas por mes
    resultado_meses = []
    for m in range(12):
        if total_mes[m] == 0:
            continue

        prob_dia = heladas_mes[m] / total_mes[m]
        dias_por_año = heladas_mes[m] / n_years
        tmin_media = tmin_sum[m] / total_mes[m]
        tmin_std = float(np.std(tmin_vals_mes[m])) if tmin_vals_mes[m] else 0

        # P(al menos 1 helada en el mes) = 1 - (1-p)^30
        prob_mes = 1 - (1 - prob_dia) ** 30

        resultado_meses.append({
            'mes': nombre_mes[m],
            'tmin_media_C': round(tmin_media, 1),
            'tmin_std_C': round(tmin_std, 1),
            'tmin_minima_abs_C': round(tmin_min[m], 1),
            'prob_helada_diaria': round(prob_dia, 4),
            'prob_helada_mensual': round(prob_mes, 4),
            'dias_helada_por_año': round(dias_por_año, 1),
        })

    # Período libre de heladas
    meses_libres = [r['mes'] for r in resultado_meses if r['prob_helada_mensual'] < 0.05]
    plh_dias = len(meses_libres) * 30

    return {
        'fuente': 'CR2MET_tmin_v2.0_REAL',
        'periodo': f'{periodo[0]}-{periodo[1]}',
        'por_mes': resultado_meses,
        'tmin_absoluta_C': round(min(tmin_min), 1),
        'dias_helada_año_promedio': round(sum(heladas_mes) / n_years, 1),
        'periodo_libre_heladas_dias': plh_dias,
        'meses_sin_helada': meses_libres,
        'metodologia': (
            "Frecuencia observada de Tmin < 0°C en datos diarios CR2MET v2.0 "
            f"(Boisier et al., 2018), período {periodo[0]}-{periodo[1]}, "
            "resolución 0.05° (~5.5 km)."
        ),
    }


def cargar_cache_precipitacion():
    """Carga climatologías pre-procesadas desde JSON cache."""
    if os.path.exists(PR_CACHE):
        with open(PR_CACHE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return []


def cargar_cache_tmin():
    """Carga cache de heladas pre-procesadas desde CR2MET Tmin."""
    if os.path.exists(TMIN_CACHE):
        with open(TMIN_CACHE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return None


def heladas_punto_cache_o_netcdf(lat, lon, nombre=None, max_dist_km=10):
    """
    Busca heladas para un punto: cache comunas → grilla densa → NetCDF.

    Parámetros:
        lat, lon: coordenadas del punto
        nombre: nombre de la comuna (para buscar en cache por nombre)
        max_dist_km: distancia máxima para usar un punto del cache

    Retorna: dict con datos de heladas, o None
    """
    # 1. Buscar en cache por nombre exacto (168 comunas)
    cache = cargar_cache_tmin()
    if cache and 'puntos' in cache:
        if nombre and nombre in cache['puntos']:
            return cache['puntos'][nombre]

        # 2. Buscar punto más cercano en cache de comunas
        mejor = None
        mejor_dist = float('inf')
        for pnombre, pdata in cache['puntos'].items():
            dist = _haversine_simple(lat, lon, pdata['lat'], pdata['lon'])
            if dist < mejor_dist:
                mejor_dist = dist
                mejor = pdata

        if mejor and mejor_dist < max_dist_km:
            return mejor

    # 3. Buscar en grilla densa (miles de puntos, ~11km resolución)
    grilla_result = heladas_grilla_punto(lat, lon, max_dist_km=max_dist_km)
    if grilla_result:
        return grilla_result

    # 4. Fallback: leer NetCDF directamente
    if tiene_temperatura():
        return heladas_reales_punto(lat, lon)

    return None


if __name__ == '__main__':
    print(f"CR2MET precipitación: {'✓' if tiene_precipitacion() else '✗'}")
    print(f"CR2MET temperatura:   {'✓' if tiene_temperatura() else '✗'}")

    cache = cargar_cache_precipitacion()
    print(f"Cache precipitación:  {len(cache)} puntos")

    if tiene_temperatura():
        print("\nCalculando heladas reales para Santiago...")
        h = heladas_reales_punto(-33.45, -70.65)
        if h:
            for m in h['por_mes']:
                print(f"  {m['mes']}: Tmin={m['tmin_media_C']:5.1f}°C | "
                      f"P(helada)={m['prob_helada_mensual']:5.1%} | "
                      f"Mín abs={m['tmin_minima_abs_C']:.1f}°C")
