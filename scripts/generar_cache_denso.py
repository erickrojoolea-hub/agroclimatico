#!/usr/bin/env python3
"""
generar_cache_denso.py — Pre-procesa CR2MET NetCDF a caches JSON densos
=========================================================================
Genera grillas densas de climatología y heladas para todo Chile,
eliminando la necesidad de NetCDF en Streamlit Cloud.

Estrategia: carga el NetCDF entero en memoria, calcula climatologías
vectorizadas con numpy, luego muestrea cada 2 píxeles (~0.1°, ~11km).

Resultado: ~3,000-5,000 puntos terrestres con datos completos.

Uso:
    python3 scripts/generar_cache_denso.py
"""

import os
import sys
import json
import time
import numpy as np

PROJECT_ROOT = os.path.join(os.path.dirname(__file__), '..')
sys.path.insert(0, PROJECT_ROOT)

CR2_DIR = os.path.join(PROJECT_ROOT, 'datos_geo', 'CR2')
PR_NC = os.path.join(CR2_DIR, 'CR2MET_pr_v2.0_day_1979_2020_005deg.nc')
TMIN_NC = os.path.join(CR2_DIR, 'CR2MET_tmin_v2.0_day_1979_2020_005deg.nc')

OUT_DIR = os.path.join(PROJECT_ROOT, 'data', 'climate')
os.makedirs(OUT_DIR, exist_ok=True)

PERIODO = (1991, 2020)
STEP_PIXELS = 2  # muestrear cada 2 píxeles de la grilla original (0.05° × 2 = 0.1°)


def generar_cache_precipitacion():
    """Genera cache denso de precipitación usando operaciones vectorizadas."""
    import xarray as xr

    print("=" * 60)
    print("PRECIPITACIÓN CR2MET → Cache denso (vectorizado)")
    print("=" * 60)

    if not os.path.exists(PR_NC):
        print(f"ERROR: No se encontró {PR_NC}")
        return

    t0 = time.time()
    print(f"Abriendo {PR_NC}...")
    ds = xr.open_dataset(PR_NC)
    pr = ds['pr']

    lats = pr.lat.values
    lons = pr.lon.values
    print(f"  Grilla: {len(lats)} lat × {len(lons)} lon")
    print(f"  Rango: lat [{lats.min():.2f}, {lats.max():.2f}] lon [{lons.min():.2f}, {lons.max():.2f}]")

    # Seleccionar período climatológico
    print(f"  Seleccionando período {PERIODO[0]}-{PERIODO[1]}...")
    pr_clim = pr.sel(time=slice(f'{PERIODO[0]}-01-01', f'{PERIODO[1]}-12-31'))

    # Calcular suma mensual para todo el grid de una vez
    print("  Calculando sumas mensuales (toda la grilla)...")
    monthly = pr_clim.resample(time='ME').sum()
    print(f"    → {monthly.sizes}")

    # Climatología mensual: promedio de cada mes
    print("  Calculando climatología mensual...")
    clim = monthly.groupby('time.month').mean()
    clim_data = clim.load().values  # shape: (12, nlat, nlon)
    print(f"    → shape: {clim_data.shape}")

    # También calcular sumas anuales para estadísticas
    print("  Calculando sumas anuales...")
    annual = pr_clim.resample(time='YE').sum().load().values  # (nyears, nlat, nlon)
    print(f"    → shape: {annual.shape}")

    # Megasequía: dos períodos
    print("  Calculando megasequía (2006-2020 vs 1991-2005)...")
    pr_p1 = pr.sel(time=slice('1991-01-01', '2005-12-31'))
    pr_p2 = pr.sel(time=slice('2006-01-01', '2020-04-30'))
    p1_annual = pr_p1.resample(time='YE').sum().load().values  # (15, nlat, nlon)
    p2_annual = pr_p2.resample(time='YE').sum().load().values

    # Extremos: calcular sobre datos diarios (cargar todo en memoria ~10GB!)
    # Alternativa: solo para puntos muestreados
    print("  Cargando datos diarios para extremos...")
    daily_data = pr_clim.load().values  # (ndays, nlat, nlon) — this is big!
    times = pr_clim.time.values
    print(f"    → shape: {daily_data.shape} ({daily_data.nbytes / 1e9:.1f} GB)")

    ds.close()

    # Ahora muestrear cada STEP_PIXELS píxeles
    lat_idx = range(0, len(lats), STEP_PIXELS)
    lon_idx = range(0, len(lons), STEP_PIXELS)
    print(f"\n  Muestreando cada {STEP_PIXELS} píxeles: {len(lat_idx)} × {len(lon_idx)} = {len(lat_idx)*len(lon_idx)} puntos potenciales")

    puntos = []
    n_years = PERIODO[1] - PERIODO[0] + 1

    for ii, li in enumerate(lat_idx):
        for lj in lon_idx:
            # Verificar si hay datos (no océano)
            monthly_vals = clim_data[:, li, lj]
            if np.all(np.isnan(monthly_vals)):
                continue

            # Precipitación mensual
            precip_monthly = [round(float(v), 1) if not np.isnan(v) else 0.0 for v in monthly_vals]
            precip_annual = round(sum(precip_monthly), 1)

            if precip_annual == 0:
                continue

            # Stats anuales
            ann_vals = annual[:, li, lj]
            ann_valid = ann_vals[~np.isnan(ann_vals)]
            if len(ann_valid) < 5:
                continue

            # Megasequía
            p1_vals = p1_annual[:, li, lj]
            p2_vals = p2_annual[:, li, lj]
            p1_mean = float(np.nanmean(p1_vals))
            p2_mean = float(np.nanmean(p2_vals))
            cambio_pct = round((p2_mean - p1_mean) / p1_mean * 100, 1) if p1_mean > 10 else 0.0

            # Extremos diarios
            daily_vals = daily_data[:, li, lj]
            valid_daily = daily_vals[~np.isnan(daily_vals)]
            rain_days = valid_daily[valid_daily > 1.0]

            puntos.append({
                'lat': round(float(lats[li]), 4),
                'lon': round(float(lons[lj]), 4),
                'lat_real': round(float(lats[li]), 4),
                'lon_real': round(float(lons[lj]), 4),
                'precip_monthly_mm': precip_monthly,
                'precip_annual_mm': precip_annual,
                'stats_anuales': {
                    'media_mm': round(float(np.mean(ann_valid)), 1),
                    'desv_std_mm': round(float(np.std(ann_valid)), 1),
                    'min_mm': round(float(np.min(ann_valid)), 1),
                    'max_mm': round(float(np.max(ann_valid)), 1),
                    'cambio_2006_2020_vs_1991_2005_pct': cambio_pct,
                },
                'extremos_diarios': {
                    'p95_mm': round(float(np.percentile(rain_days, 95)), 1) if len(rain_days) > 100 else 0,
                    'p99_mm': round(float(np.percentile(rain_days, 99)), 1) if len(rain_days) > 100 else 0,
                    'max_diario_mm': round(float(np.max(valid_daily)), 1) if len(valid_daily) > 0 else 0,
                    'dias_lluvia_por_año': round(len(rain_days) / n_years, 1),
                },
                'fuente': 'CR2MET_v2.0_day_1979-2020',
                'periodo_climatologico': f'{PERIODO[0]}-{PERIODO[1]}',
            })

        if (ii + 1) % 50 == 0:
            pct = (ii + 1) / len(lat_idx) * 100
            print(f"  [{pct:5.1f}%] lat={lats[li]:.2f} | {len(puntos)} puntos válidos | {time.time()-t0:.0f}s")

    # Guardar
    out_path = os.path.join(OUT_DIR, 'cr2met_precipitacion_grilla.json')
    output = {
        'generado': time.strftime('%Y-%m-%dT%H:%M:%S'),
        'fuente': 'CR2MET_pr_v2.0_day_1979_2020_005deg.nc',
        'periodo': f'{PERIODO[0]}-{PERIODO[1]}',
        'resolucion_muestreo_deg': STEP_PIXELS * 0.05,
        'n_puntos': len(puntos),
        'metodologia': (
            f'Climatología mensual CR2MET v2.0, período {PERIODO[0]}-{PERIODO[1]}, '
            f'muestreado cada {STEP_PIXELS} píxeles ({STEP_PIXELS*0.05:.2f}°, ~{STEP_PIXELS*5.5:.0f} km). '
            'Precipitación: suma mensual promedio. '
            'Megasequía: cambio % 2006-2020 vs 1991-2005. '
            'Ref: Boisier et al. (2018). CR2MET. U. de Chile.'
        ),
        'puntos': puntos,
    }

    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False)

    size_mb = os.path.getsize(out_path) / 1024 / 1024
    elapsed = time.time() - t0
    print(f"\n✓ Precipitación: {len(puntos)} puntos → {out_path}")
    print(f"  Tamaño: {size_mb:.1f} MB | Tiempo: {elapsed:.0f}s")


def generar_cache_heladas():
    """Genera cache denso de heladas usando operaciones vectorizadas."""
    import xarray as xr

    print("\n" + "=" * 60)
    print("HELADAS CR2MET Tmin → Cache denso (vectorizado)")
    print("=" * 60)

    if not os.path.exists(TMIN_NC):
        print(f"ERROR: No se encontró {TMIN_NC}")
        return

    t0 = time.time()
    print(f"Abriendo {TMIN_NC}...")
    ds = xr.open_dataset(TMIN_NC)
    tmin = ds['tmin']

    lats = tmin.lat.values
    lons = tmin.lon.values
    print(f"  Grilla: {len(lats)} lat × {len(lons)} lon")

    # Cargar período completo en memoria
    print(f"  Cargando período {PERIODO[0]}-{PERIODO[1]}...")
    tmin_clim = tmin.sel(time=slice(f'{PERIODO[0]}-01-01', f'{PERIODO[1]}-12-31'))
    data = tmin_clim.load().values  # (ndays, nlat, nlon)
    times = tmin_clim.time.values
    print(f"    → shape: {data.shape} ({data.nbytes / 1e9:.1f} GB)")

    ds.close()

    # Extraer meses de cada timestamp
    months = np.array([int(str(t)[5:7]) for t in times])  # 1-12

    n_years = PERIODO[1] - PERIODO[0] + 1
    nombre_mes = ['Ene', 'Feb', 'Mar', 'Abr', 'May', 'Jun',
                  'Jul', 'Ago', 'Sep', 'Oct', 'Nov', 'Dic']

    # Muestrear
    lat_idx = range(0, len(lats), STEP_PIXELS)
    lon_idx = range(0, len(lons), STEP_PIXELS)
    print(f"  Muestreando: {len(lat_idx)} × {len(lon_idx)} puntos potenciales")

    puntos = {}
    for ii, li in enumerate(lat_idx):
        for lj in lon_idx:
            vals = data[:, li, lj]
            valid_mask = ~np.isnan(vals)

            if valid_mask.sum() < 365 * 10:
                continue

            # Estadísticas por mes
            por_mes = []
            heladas_total = 0

            for m in range(12):
                mes_mask = (months == m + 1) & valid_mask
                mes_vals = vals[mes_mask]

                if len(mes_vals) == 0:
                    por_mes.append({
                        'mes': nombre_mes[m],
                        'tmin_media_C': 0.0, 'tmin_std_C': 0.0,
                        'tmin_min_abs_C': 0.0,
                        'prob_helada_diaria': 0.0, 'prob_helada_mensual': 0.0,
                        'dias_helada_año': 0.0,
                    })
                    continue

                n_helada = int(np.sum(mes_vals < 0))
                heladas_total += n_helada
                prob_dia = n_helada / len(mes_vals)
                prob_mes = 1 - (1 - prob_dia) ** 30

                por_mes.append({
                    'mes': nombre_mes[m],
                    'tmin_media_C': round(float(np.mean(mes_vals)), 1),
                    'tmin_std_C': round(float(np.std(mes_vals)), 1),
                    'tmin_min_abs_C': round(float(np.min(mes_vals)), 1),
                    'prob_helada_diaria': round(prob_dia, 4),
                    'prob_helada_mensual': round(prob_mes, 4),
                    'dias_helada_año': round(n_helada / n_years, 1),
                })

            valid_vals = vals[valid_mask]
            tmin_abs = round(float(np.min(valid_vals)), 1)
            dias_helada_promedio = round(heladas_total / n_years, 1)
            meses_libres = [pm['mes'] for pm in por_mes if pm['prob_helada_mensual'] < 0.05]

            key = f"{lats[li]:.3f}_{lons[lj]:.3f}"
            puntos[key] = {
                'lat': round(float(lats[li]), 4),
                'lon': round(float(lons[lj]), 4),
                'lat_real': round(float(lats[li]), 4),
                'lon_real': round(float(lons[lj]), 4),
                'periodo': f'{PERIODO[0]}-{PERIODO[1]}',
                'por_mes': por_mes,
                'tmin_absoluta_C': tmin_abs,
                'dias_helada_año_promedio': dias_helada_promedio,
                'periodo_libre_heladas_dias': len(meses_libres) * 30,
                'meses_sin_helada': meses_libres,
                'fuente': 'CR2MET_tmin_v2.0_REAL',
            }

        if (ii + 1) % 50 == 0:
            pct = (ii + 1) / len(lat_idx) * 100
            print(f"  [{pct:5.1f}%] lat={lats[li]:.2f} | {len(puntos)} puntos válidos | {time.time()-t0:.0f}s")

    # Guardar
    out_path = os.path.join(OUT_DIR, 'cr2met_heladas_grilla.json')
    output = {
        'generado': time.strftime('%Y-%m-%dT%H:%M:%S'),
        'fuente': 'CR2MET_tmin_v2.0_day_1979_2020_005deg.nc',
        'periodo': f'{PERIODO[0]}-{PERIODO[1]}',
        'resolucion_muestreo_deg': STEP_PIXELS * 0.05,
        'n_puntos': len(puntos),
        'metodologia': (
            f'Frecuencia observada de Tmin < 0°C en datos diarios CR2MET v2.0, '
            f'período {PERIODO[0]}-{PERIODO[1]}, muestreado cada {STEP_PIXELS} píxeles '
            f'({STEP_PIXELS*0.05:.2f}°, ~{STEP_PIXELS*5.5:.0f} km). '
            'Prob mensual = 1-(1-p_diaria)^30. '
            'Ref: Boisier et al. (2018). CR2MET. U. de Chile.'
        ),
        'puntos': puntos,
    }

    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False)

    size_mb = os.path.getsize(out_path) / 1024 / 1024
    elapsed = time.time() - t0
    print(f"\n✓ Heladas: {len(puntos)} puntos → {out_path}")
    print(f"  Tamaño: {size_mb:.1f} MB | Tiempo: {elapsed:.0f}s")


def generar_cache_estaciones():
    """Pre-procesa climatologías de estaciones CR2."""
    print("\n" + "=" * 60)
    print("ESTACIONES CR2 → Cache de climatologías")
    print("=" * 60)

    t0 = time.time()
    try:
        from lectores.lector_cr2_estaciones import (
            cargar_estaciones, cargar_series_mensual, climatologia_estacion
        )
    except ImportError as e:
        print(f"ERROR importando: {e}")
        return

    estaciones = cargar_estaciones()
    print(f"  Estaciones cargadas: {len(estaciones)}")

    from lectores.lector_cr2_estaciones import MONTHLY_DATA
    if not os.path.exists(MONTHLY_DATA):
        print(f"  No se encontró archivo de series: {MONTHLY_DATA}")
        # Guardar solo metadata
        out_path = os.path.join(OUT_DIR, 'cr2_estaciones_climatologia.json')
        meta = {}
        for code, est in estaciones.items():
            meta[code] = {
                'nombre': est['nombre'], 'lat': est['lat'],
                'lon': est['lon'], 'alt': est['alt'],
                'cuenca': est.get('cuenca', ''),
            }
        output = {
            'generado': time.strftime('%Y-%m-%dT%H:%M:%S'),
            'fuente': 'CR2 U. Chile (metadata only)',
            'n_estaciones': len(meta), 'estaciones': meta,
        }
        with open(out_path, 'w', encoding='utf-8') as f:
            json.dump(output, f, ensure_ascii=False, indent=1)
        print(f"✓ Solo metadata: {len(meta)} estaciones")
        return

    series = cargar_series_mensual()
    print(f"  Series cargadas: {len(series)}")

    resultado = {}
    for code, est in estaciones.items():
        if code not in series:
            continue
        clim = climatologia_estacion(series[code], PERIODO)
        if clim is None:
            continue
        resultado[code] = {
            'nombre': est['nombre'], 'lat': est['lat'],
            'lon': est['lon'], 'alt': est['alt'],
            'cuenca': est.get('cuenca', ''),
            'institucion': est.get('institucion', ''),
            'climatologia_mm': clim,
            'anual_mm': round(sum(clim), 1),
            'periodo': f'{PERIODO[0]}-{PERIODO[1]}',
        }

    out_path = os.path.join(OUT_DIR, 'cr2_estaciones_climatologia.json')
    output = {
        'generado': time.strftime('%Y-%m-%dT%H:%M:%S'),
        'fuente': 'CR2 compilado U. Chile (879 estaciones)',
        'periodo': f'{PERIODO[0]}-{PERIODO[1]}',
        'n_estaciones': len(resultado),
        'estaciones': resultado,
    }
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=1)

    size_mb = os.path.getsize(out_path) / 1024 / 1024
    elapsed = time.time() - t0
    print(f"\n✓ Estaciones: {len(resultado)} con climatología → {out_path}")
    print(f"  Tamaño: {size_mb:.1f} MB | Tiempo: {elapsed:.0f}s")


if __name__ == '__main__':
    print("╔══════════════════════════════════════════════════════╗")
    print("║  GENERADOR DE CACHE DENSO PARA STREAMLIT CLOUD     ║")
    print("╠══════════════════════════════════════════════════════╣")
    print(f"║  Muestreo: cada {STEP_PIXELS} píxeles ({STEP_PIXELS*0.05:.2f}°, ~{STEP_PIXELS*5.5:.0f} km)       ║")
    print(f"║  Período: {PERIODO[0]}-{PERIODO[1]}                            ║")
    print("╚══════════════════════════════════════════════════════╝\n")

    t_total = time.time()

    generar_cache_precipitacion()
    generar_cache_heladas()
    generar_cache_estaciones()

    elapsed = time.time() - t_total
    print(f"\n{'='*60}")
    print(f"TOTAL: {elapsed:.0f}s ({elapsed/60:.1f} min)")
    print(f"{'='*60}")

    print("\nArchivos generados:")
    for f in sorted(os.listdir(OUT_DIR)):
        fp = os.path.join(OUT_DIR, f)
        if os.path.isfile(fp):
            size = os.path.getsize(fp) / 1024
            unit = 'KB'
            if size > 1024:
                size /= 1024
                unit = 'MB'
            print(f"  {f:45s} {size:6.1f} {unit}")
