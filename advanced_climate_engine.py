"""
advanced_climate_engine.py — Motor del Informe Climático Avanzado
================================================================
Análisis por punto geográfico exacto usando:
  - CR2MET: grilla 0.05° (precipitación + Tmin), 1979-2020
  - CR2 Estaciones: 879 estaciones, IDW
  - CHIRPS: monitoreo precipitación 2020-2025
  - Índices ENSO/PDO/SOI
  - Catastro frutícola: contexto agrícola comunal
  - Modelo empírico de heladas: fallback

Todas las fuentes operan a nivel de punto (lat, lon, alt).
La comuna solo entra como contexto agrícola.
"""

import os
import sys
import math
import json
import time
from typing import Optional
from datetime import datetime

# Asegurar que el directorio del proyecto está en el path
_PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)


def _haversine(lat1, lon1, lat2, lon2):
    R = 6371.0
    rlat1, rlon1, rlat2, rlon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = rlat2 - rlat1
    dlon = rlon2 - rlon1
    a = math.sin(dlat / 2) ** 2 + math.cos(rlat1) * math.cos(rlat2) * math.sin(dlon / 2) ** 2
    return R * 2 * math.asin(math.sqrt(a))


def _distancia_al_mar(lat, lon):
    """Distancia aprox. al océano Pacífico (costa chilena)."""
    # Simplificación: la costa de Chile está ~71.5°W en zona central
    # Ajustar por latitud
    if lat > -30:
        costa_lon = -71.3
    elif lat > -33:
        costa_lon = -71.5
    elif lat > -36:
        costa_lon = -71.8
    elif lat > -40:
        costa_lon = -73.2
    else:
        costa_lon = -73.5

    # Distancia EW en km
    dist_ew = abs(lon - costa_lon) * 111 * math.cos(math.radians(lat))
    # Si lon es más negativo que la costa, estamos en el mar → 0
    if lon < costa_lon:
        return max(0, dist_ew - 5)  # margen de error
    return dist_ew


def generar_informe_avanzado(lat: float, lon: float, alt: Optional[float] = None,
                              comuna: str = "") -> dict:
    """
    Genera informe climático avanzado para un punto exacto.

    Args:
        lat: Latitud WGS84
        lon: Longitud WGS84
        alt: Altitud (m). Si None, se estima desde estaciones.
        comuna: Nombre de la comuna (para contexto agrícola)

    Returns:
        dict con todas las secciones del informe
    """
    t_start = time.time()
    informe = {
        'lat': lat,
        'lon': lon,
        'comuna': comuna,
        'fecha_generacion': datetime.now().strftime("%Y-%m-%d %H:%M"),
        'secciones': {},
        'errores': [],
    }

    # ── 1. Altitud ───────────────────────────────────────────────────────
    if alt is None:
        try:
            from elevation_engine import estimar_altitud
            alt_result = estimar_altitud(lat, lon)
            if alt_result:
                alt = float(alt_result['alt'])
                informe['altitud'] = alt_result
            else:
                alt = 200.0
                informe['altitud'] = {'alt': 200, 'confianza': 'default', 'metodo': 'Default 200m'}
        except Exception as e:
            alt = 200.0
            informe['errores'].append(f"Altitud: {e}")
    else:
        informe['altitud'] = {'alt': round(alt), 'confianza': 'usuario', 'metodo': 'Proporcionado por usuario'}

    informe['alt'] = alt
    informe['distancia_mar_km'] = round(_distancia_al_mar(lat, lon), 1)

    # ── 2. Estado ENSO ───────────────────────────────────────────────────
    try:
        from lectores.lector_indices_climaticos import estado_enso_actual
        enso = estado_enso_actual()
        informe['secciones']['enso'] = enso
    except Exception as e:
        informe['errores'].append(f"ENSO: {e}")
        informe['secciones']['enso'] = {'estado': 'No disponible', 'oni_actual': 0}

    # ── 3. Precipitación CR2MET (mejor fuente: cache denso → NetCDF) ─────
    precip_data = None
    try:
        from lectores.lector_cr2met import (
            tiene_precipitacion, climatologia_mensual_punto, PR_NC,
            precipitacion_completa_punto
        )
        # Intentar cache denso primero, luego NetCDF
        pr_completa = precipitacion_completa_punto(lat, lon)
        if pr_completa:
            precip_data = {
                'fuente': pr_completa['fuente'],
                'mensual_mm': pr_completa['mensual_mm'],
                'anual_mm': pr_completa['anual_mm'],
                'meses': ['Ene', 'Feb', 'Mar', 'Abr', 'May', 'Jun',
                          'Jul', 'Ago', 'Sep', 'Oct', 'Nov', 'Dic'],
            }
            # Si el cache denso tiene stats y megasequía, agregarlos ya
            if pr_completa.get('stats_anuales'):
                stats = pr_completa['stats_anuales']
                cambio = stats.get('cambio_2006_2020_vs_1991_2005_pct', 0)
                if cambio != 0:
                    precip_data['megasequia'] = {
                        'periodo_1_mm': round(stats.get('media_mm', 0) / (1 + cambio/100), 1) if cambio else 0,
                        'periodo_2_mm': round(stats.get('media_mm', 0), 1),
                        'cambio_pct': cambio,
                        'interpretacion': 'Déficit significativo' if cambio < -15 else
                                          'Déficit moderado' if cambio < -5 else
                                          'Estable' if cambio < 5 else 'Aumento',
                    }
            if pr_completa.get('extremos_diarios'):
                precip_data['extremos'] = pr_completa['extremos_diarios']
    except Exception as e:
        informe['errores'].append(f"CR2MET precip: {e}")

    # ── 3b. Precipitación CR2 Estaciones (validación cruzada) ────────────
    precip_estaciones = None
    try:
        from lectores.lector_cr2_estaciones import cargar_estaciones, cargar_series_mensual, precipitacion_para_punto
        est = cargar_estaciones()
        try:
            series = cargar_series_mensual()
        except Exception:
            series = None

        if est and series:
            pe = precipitacion_para_punto(lat, lon, est, series, periodo=(1991, 2020))
            if pe and pe.get('precip_annual', 0) > 0:
                precip_estaciones = pe
        elif est:
            # Fallback: usar climatologías pre-calculadas de estaciones
            from lectores.lector_cr2met import _cargar_estaciones_climatologia, _haversine_simple
            est_clim = _cargar_estaciones_climatologia()
            if est_clim and 'estaciones' in est_clim:
                cercanas = []
                for code, ec in est_clim['estaciones'].items():
                    d = _haversine_simple(lat, lon, ec['lat'], ec['lon'])
                    if d < 50:
                        cercanas.append((d, ec))
                cercanas.sort(key=lambda x: x[0])
                if cercanas:
                    cercanas = cercanas[:5]
                    if len(cercanas) == 1:
                        ec = cercanas[0][1]
                        precip_estaciones = {
                            'precip_monthly': ec['climatologia_mm'],
                            'precip_annual': ec['anual_mm'],
                            'fuente': f'CR2_estacion_cache',
                            'estacion': ec['nombre'],
                            'dist_km': cercanas[0][0],
                            'metodo': f'estación más cercana (cache)',
                        }
                    else:
                        monthly = [0.0] * 12
                        peso_total = 0
                        est_usadas = []
                        for d, ec in cercanas:
                            w = 1.0 / max(d, 0.1) ** 2
                            peso_total += w
                            for m in range(12):
                                monthly[m] += ec['climatologia_mm'][m] * w
                            est_usadas.append(f"{ec['nombre']} ({d:.0f}km)")
                        monthly = [round(v / peso_total, 1) for v in monthly]
                        precip_estaciones = {
                            'precip_monthly': monthly,
                            'precip_annual': round(sum(monthly), 1),
                            'fuente': 'CR2_IDW_cache',
                            'estaciones_usadas': est_usadas,
                            'metodo': f'IDW con {len(cercanas)} estaciones (cache)',
                        }
    except Exception as e:
        informe['errores'].append(f"CR2 estaciones: {e}")

    # Fallback if CR2MET not available
    if not precip_data and precip_estaciones:
        precip_data = {
            'fuente': f"CR2 Estaciones IDW ({precip_estaciones.get('metodo', '')})",
            'mensual_mm': [round(v, 1) for v in precip_estaciones['precip_monthly']],
            'anual_mm': round(precip_estaciones['precip_annual'], 1),
            'meses': ['Ene', 'Feb', 'Mar', 'Abr', 'May', 'Jun',
                      'Jul', 'Ago', 'Sep', 'Oct', 'Nov', 'Dic'],
        }

    if precip_data:
        # Validación cruzada
        if precip_estaciones and precip_data['fuente'].startswith('CR2MET'):
            cr2met_anual = precip_data['anual_mm']
            est_anual = precip_estaciones['precip_annual']
            ratio = cr2met_anual / est_anual if est_anual > 0 else 0
            if 0.8 <= ratio <= 1.2:
                concordancia = 'Buena'
            elif 0.6 <= ratio <= 1.4:
                concordancia = 'Moderada'
            else:
                concordancia = 'Revisar'
            precip_data['validacion'] = {
                'cr2met_mm': cr2met_anual,
                'estaciones_mm': round(est_anual, 1),
                'ratio': round(ratio, 2),
                'concordancia': concordancia,
                'estaciones_usadas': precip_estaciones.get('estaciones_usadas', []),
            }

        # Tendencia megasequía (si el cache denso no la incluyó)
        if 'megasequia' not in precip_data:
            try:
                from lectores.lector_cr2met import climatologia_mensual_punto
                if tiene_precipitacion():
                    p1 = climatologia_mensual_punto(PR_NC, 'pr', lat, lon, periodo=(1991, 2005))
                    p2 = climatologia_mensual_punto(PR_NC, 'pr', lat, lon, periodo=(2006, 2020))
                    s1, s2 = sum(p1), sum(p2)
                    if s1 > 0:
                        cambio = (s2 - s1) / s1 * 100
                        precip_data['megasequia'] = {
                            'periodo_1_mm': round(s1, 1),
                            'periodo_2_mm': round(s2, 1),
                            'cambio_pct': round(cambio, 1),
                            'interpretacion': 'Déficit significativo' if cambio < -15 else
                                              'Déficit moderado' if cambio < -5 else
                                              'Estable' if cambio < 5 else 'Aumento',
                        }
            except Exception:
                pass

        informe['secciones']['precipitacion'] = precip_data

    # ── 4. Heladas y Tmin (CR2MET) ───────────────────────────────────────
    heladas_data = None
    try:
        from lectores.lector_cr2met import heladas_punto_cache_o_netcdf, tiene_temperatura
        heladas = heladas_punto_cache_o_netcdf(lat, lon, nombre=comuna or "punto")
        if heladas:
            heladas_data = heladas
            heladas_data['fuente'] = 'CR2MET Tmin grillado (0.05°, 1991-2020)'
    except Exception as e:
        informe['errores'].append(f"Heladas CR2MET: {e}")

    # Fallback: modelo empírico
    if not heladas_data:
        try:
            from modelo_heladas_extremos import evaluar_riesgo_climatico
            oni = informe['secciones'].get('enso', {}).get('oni_actual', 0)
            heladas = evaluar_riesgo_climatico(lat, lon, alt, nombre=comuna, oni_actual=oni)
            if heladas:
                heladas_data = heladas
                heladas_data['fuente'] = 'Modelo empírico (calibrado con Atlas Agroclimático)'
        except Exception as e:
            informe['errores'].append(f"Modelo heladas: {e}")

    if heladas_data:
        # Enriquecer con interpretación ENSO
        enso_estado = informe['secciones'].get('enso', {}).get('estado', '')
        if 'Niña' in enso_estado:
            heladas_data['efecto_enso'] = 'La Niña → primaveras más frías → MAYOR riesgo de heladas tardías'
            heladas_data['factor_enso'] = 'aumentado'
        elif 'Niño' in enso_estado:
            heladas_data['efecto_enso'] = 'El Niño → noches más cálidas → MENOR riesgo de heladas'
            heladas_data['factor_enso'] = 'reducido'
        else:
            heladas_data['efecto_enso'] = 'ENSO neutro → riesgo normal'
            heladas_data['factor_enso'] = 'normal'

        informe['secciones']['heladas'] = heladas_data

    # ── 5. Contexto agrícola (catastro frutícola) ────────────────────────
    try:
        from lectores.lector_catastro_fruticola import (
            superficie_por_comuna, especies_sensibles_helada,
        )
        catastro = superficie_por_comuna()
        umbrales = especies_sensibles_helada()

        # Find matching comuna
        comuna_match = comuna
        if comuna and comuna in catastro:
            cat_data = catastro[comuna]
        else:
            # Buscar la comuna más cercana con datos por distancia
            cat_data = None
            from lectores.lector_catastro_fruticola import COMUNAS_COORDS
            min_dist = 999
            for c_name, c_coords in COMUNAS_COORDS.items():
                if c_name in catastro:
                    d = _haversine(lat, lon, c_coords[0], c_coords[1])
                    if d < min_dist:
                        min_dist = d
                        comuna_match = c_name
                        cat_data = catastro[c_name]
            if min_dist > 30:
                cat_data = None  # Too far

        if cat_data:
            # Heladas agronómicas: cruzar umbrales con probabilidad
            heladas_agro = []
            if heladas_data and umbrales:
                por_mes = heladas_data.get('por_mes', [])
                # Get Sep/Oct probabilities
                p_sep = next((m for m in por_mes if m.get('mes') == 'Sep'), {})
                p_oct = next((m for m in por_mes if m.get('mes') == 'Oct'), {})

                for esp_name, esp_info in umbrales.items():
                    sup_ha = cat_data.get('especies', {}).get(esp_name, 0)
                    if sup_ha <= 0:
                        continue
                    umbral_flor = esp_info.get('floracion', -2.0)
                    # Estimate P(T < umbral) ≈ P(T < 0) * factor
                    # Factor: if umbral is higher (e.g., -1°C), probability is higher
                    factor = max(0, 1 + (umbral_flor + 2) * 0.3)
                    p_sep_esp = min(100, p_sep.get('prob_helada_mensual', 0) * factor)
                    p_oct_esp = min(100, p_oct.get('prob_helada_mensual', 0) * factor)

                    if p_sep_esp > 0 or p_oct_esp > 0:
                        riesgo = 'MUY ALTO' if p_sep_esp > 50 else \
                                 'ALTO' if p_sep_esp > 25 else \
                                 'MODERADO' if p_sep_esp > 10 else 'BAJO'
                        heladas_agro.append({
                            'especie': esp_name,
                            'superficie_ha': round(sup_ha),
                            'umbral_floracion': umbral_flor,
                            'p_dano_sep': round(p_sep_esp, 1),
                            'p_dano_oct': round(p_oct_esp, 1),
                            'sensibilidad': esp_info.get('sensibilidad', ''),
                            'riesgo': riesgo,
                        })

                heladas_agro.sort(key=lambda x: -x['p_dano_sep'])

            informe['secciones']['contexto_agro'] = {
                'comuna_match': comuna_match,
                'total_ha': cat_data.get('total_ha', 0),
                'top_especies': cat_data.get('top_especies', []),
                'heladas_agronomicas': heladas_agro,
            }
    except Exception as e:
        informe['errores'].append(f"Catastro: {e}")

    # ── 6. Monitoreo año en curso (CHIRPS) ───────────────────────────────
    try:
        sys.path.insert(0, os.path.join(_PROJECT_ROOT))
        from procesar_precipitacion_unificado import monitoreo_precipitacion
        monitoreo = monitoreo_precipitacion(lat, lon, year=datetime.now().year)
        if monitoreo:
            informe['secciones']['monitoreo'] = monitoreo
    except Exception as e:
        informe['errores'].append(f"Monitoreo CHIRPS: {e}")

    # ── 7. Balance hídrico simplificado ──────────────────────────────────
    if precip_data:
        try:
            pp = precip_data['mensual_mm']
            # ETP Hargreaves simplificado (necesita Tmax/Tmin, usamos aprox)
            # ETP ≈ 0.0023 * Ra * (T + 17.8) * sqrt(Tmax-Tmin)
            # Simplificación: curva típica zona central Chile
            etp_tipica = [150, 130, 100, 60, 30, 20, 20, 30, 50, 80, 110, 140]
            # Ajustar por latitud
            lat_factor = 1 + (abs(lat) - 33) * 0.02
            etp = [round(e * lat_factor, 1) for e in etp_tipica]

            balance = [round(p - e, 1) for p, e in zip(pp, etp)]
            deficit = [round(max(0, e - p), 1) for p, e in zip(pp, etp)]
            meses_estres = sum(1 for b in balance if b < -20)

            informe['secciones']['balance_hidrico'] = {
                'precipitacion_mm': pp,
                'etp_mm': etp,
                'balance_mm': balance,
                'deficit_mm': deficit,
                'deficit_anual_mm': round(sum(deficit), 1),
                'meses_estres': meses_estres,
                'meses': ['Ene', 'Feb', 'Mar', 'Abr', 'May', 'Jun',
                          'Jul', 'Ago', 'Sep', 'Oct', 'Nov', 'Dic'],
                'nota': 'ETP estimada con curva latitudinal simplificada',
            }
        except Exception as e:
            informe['errores'].append(f"Balance hídrico: {e}")

    # ── 8. Pronóstico estacional ─────────────────────────────────────────
    enso_data = informe['secciones'].get('enso', {})
    if precip_data and enso_data:
        try:
            oni = enso_data.get('oni_actual', 0)
            anual = precip_data['anual_mm']

            # Factor ENSO sobre precipitación (Montecinos & Aceituno 2003)
            if oni > 0.5:
                factor = 1.0 + min(0.4, oni * 0.2)  # El Niño: +20-40%
                outlook = 'LLUVIOSO'
            elif oni < -0.5:
                factor = 1.0 - min(0.3, abs(oni) * 0.15)  # La Niña: -20-30%
                outlook = 'SECO'
            else:
                factor = 1.0
                outlook = 'NORMAL'

            esperada = round(anual * factor, 1)
            rango = (round(esperada * 0.85, 1), round(esperada * 1.15, 1))

            informe['secciones']['pronostico'] = {
                'factor_enso': round(factor, 2),
                'precip_esperada_mm': esperada,
                'rango_mm': rango,
                'outlook': outlook,
                'nota': 'ADVERTENCIA: correlación ENSO-precipitación debilitada post-2000 (Garreaud 2024)',
            }
        except Exception as e:
            informe['errores'].append(f"Pronóstico: {e}")

    # ── 9. Resumen y recomendaciones ─────────────────────────────────────
    alertas = []
    heladas_sec = informe['secciones'].get('heladas', {})
    if heladas_sec:
        dias_hel = heladas_sec.get('dias_helada_año_promedio', 0)
        if dias_hel > 20:
            alertas.append(f"🔴 ALTO riesgo de heladas: {dias_hel:.0f} días/año promedio")
        elif dias_hel > 5:
            alertas.append(f"🟡 Riesgo moderado de heladas: {dias_hel:.0f} días/año")
        else:
            alertas.append(f"🟢 Bajo riesgo de heladas: {dias_hel:.0f} días/año")

        plh = heladas_sec.get('periodo_libre_heladas_dias', 0)
        if plh > 0:
            if plh < 180:
                alertas.append(f"🔴 Período libre de heladas corto: {plh} días")
            elif plh < 240:
                alertas.append(f"🟡 Período libre de heladas moderado: {plh} días")
            else:
                alertas.append(f"🟢 Período libre de heladas amplio: {plh} días")

    precip_sec = informe['secciones'].get('precipitacion', {})
    if precip_sec:
        anual = precip_sec.get('anual_mm', 0)
        if anual < 200:
            alertas.append(f"🔴 Zona árida: {anual:.0f} mm/año")
        elif anual < 400:
            alertas.append(f"🟡 Precipitación baja: {anual:.0f} mm/año — riego esencial")
        elif anual < 800:
            alertas.append(f"🟢 Precipitación moderada: {anual:.0f} mm/año")
        else:
            alertas.append(f"🟢 Precipitación abundante: {anual:.0f} mm/año")

        mega = precip_sec.get('megasequia', {})
        if mega and mega.get('cambio_pct', 0) < -15:
            alertas.append(f"🟡 Megasequía: {mega['cambio_pct']:.0f}% vs período 1991-2005")

    monitoreo = informe['secciones'].get('monitoreo', {})
    if monitoreo:
        clasif = monitoreo.get('clasificacion', '')
        if 'SECO' in clasif:
            alertas.append(f"🟡 Año en curso: {clasif}")

    informe['secciones']['resumen'] = {
        'alertas': alertas,
        'n_fuentes': sum(1 for k in ['precipitacion', 'heladas', 'enso', 'contexto_agro', 'monitoreo']
                         if k in informe['secciones']),
    }

    informe['tiempo_generacion_s'] = round(time.time() - t_start, 2)
    return informe


if __name__ == "__main__":
    # Test
    result = generar_informe_avanzado(-34.73, -71.65, comuna="Lolol")
    print(json.dumps({k: v for k, v in result.items() if k != 'secciones'}, indent=2, ensure_ascii=False))
    for sec_name, sec_data in result.get('secciones', {}).items():
        print(f"\n--- {sec_name} ---")
        if isinstance(sec_data, dict):
            for k, v in sec_data.items():
                if isinstance(v, list) and len(v) > 5:
                    print(f"  {k}: [{v[0]}, ..., {v[-1]}] ({len(v)} items)")
                else:
                    print(f"  {k}: {v}")
