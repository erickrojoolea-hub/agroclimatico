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
CACHE_DIR = os.path.join(_PROJECT_ROOT, 'datos_precipitacion')
PR_CACHE = os.path.join(CACHE_DIR, 'cr2met_climatologia_puntos.json')
TMIN_CACHE = os.path.join(CACHE_DIR, 'cr2met_tmin_heladas_cache.json')


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
    Busca heladas para un punto: primero en cache, luego en NetCDF.

    Parámetros:
        lat, lon: coordenadas del punto
        nombre: nombre de la comuna (para buscar en cache por nombre)
        max_dist_km: distancia máxima para usar un punto del cache

    Retorna: dict con datos de heladas, o None
    """
    # 1. Buscar en cache por nombre exacto
    cache = cargar_cache_tmin()
    if cache and 'puntos' in cache:
        if nombre and nombre in cache['puntos']:
            return cache['puntos'][nombre]

        # 2. Buscar punto más cercano en cache
        mejor = None
        mejor_dist = float('inf')
        for pnombre, pdata in cache['puntos'].items():
            dlat = (pdata['lat'] - lat) * 111.0
            dlon = (pdata['lon'] - lon) * 111.0 * abs(np.cos(np.radians(lat)))
            dist = (dlat**2 + dlon**2) ** 0.5
            if dist < mejor_dist:
                mejor_dist = dist
                mejor = pdata

        if mejor and mejor_dist < max_dist_km:
            return mejor

    # 3. Fallback: leer NetCDF directamente
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
