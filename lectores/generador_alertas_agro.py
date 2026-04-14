#!/usr/bin/env python3
"""
GENERADOR DE ALERTAS AGROCLIMÁTICAS POR COMUNA — Chile 2026
=============================================================
Cruza datos climáticos con producción agrícola para generar alertas
georeferenciadas de riesgo de heladas, sequía y eventos extremos.

Este módulo es el integrador final: usa todos los lectores para producir
un JSON/GeoJSON con alertas por comuna listas para visualización en mapa.

Flujo:
  1. Lee catastro frutícola → identifica comunas y especies
  2. Para cada comuna: obtiene clima (CR2MET/CR2 estaciones/WorldClim)
  3. Calcula riesgo de helada por especie según umbrales
  4. Ajusta por ENSO actual
  5. Genera GeoJSON con alertas por comuna

Salida: JSON/GeoJSON para desplegar en mapa Leaflet/Mapbox
"""

import json
import os
import sys
import math
from datetime import datetime

# Importar lectores
sys.path.insert(0, os.path.dirname(__file__))
from lector_catastro_fruticola import (
    superficie_por_comuna, coords_comuna, especies_sensibles_helada, COMUNAS_COORDS
)
from lector_indices_climaticos import estado_enso_actual
from lector_cr2met import heladas_punto_cache_o_netcdf

# Importar modelo de heladas
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from modelo_heladas_extremos import (
    estimar_tmin_mensual, ajustar_por_enso, probabilidad_helada_mensual,
    clasificar_riesgo_helada, periodo_libre_heladas,
    calcular_riesgo_precipitacion_extrema
)


def generar_alertas_por_comuna(año_catastro=2024, usar_cr2met_tmin=False):
    """
    Genera alertas agroclimáticas para todas las comunas con producción frutícola.

    Parámetros:
        año_catastro: año del catastro frutícola a usar
        usar_cr2met_tmin: si True, usa datos reales de CR2MET Tmin (requiere archivo)

    Retorna: lista de alertas por comuna con datos para mapa
    """
    # 1. Cargar estado ENSO actual
    enso = estado_enso_actual()
    oni = enso['oni_actual'] if enso else 0

    # 2. Cargar catastro frutícola
    catastro = superficie_por_comuna(año_catastro)

    # 3. Cargar umbrales de helada por especie
    umbrales = especies_sensibles_helada()

    # 4. Intentar usar CR2MET Tmin (cache o NetCDF directo)
    usar_tmin_real = usar_cr2met_tmin
    # Siempre intentar cache primero (no requiere NetCDF)
    from lector_cr2met import cargar_cache_tmin, tiene_temperatura
    tmin_cache = cargar_cache_tmin()
    if tmin_cache:
        print(f"✓ Cache CR2MET Tmin disponible ({tmin_cache.get('n_puntos', 0)} puntos)")
        usar_tmin_real = True
    elif usar_cr2met_tmin and tiene_temperatura():
        print("✓ Usando CR2MET Tmin NetCDF directo (lento)")
    else:
        print("⚠ Sin datos CR2MET Tmin, usando modelo empírico")

    alertas = []
    comunas_sin_coords = []

    for comuna, datos_agro in catastro.items():
        coords = coords_comuna(comuna)
        if not coords:
            comunas_sin_coords.append(comuna)
            continue

        lat, lon, alt = coords

        # 5. Calcular heladas — prioridad: cache > NetCDF > modelo empírico
        heladas_data = None
        tmin_por_mes = {}

        if usar_tmin_real:
            heladas_data = heladas_punto_cache_o_netcdf(lat, lon, nombre=comuna)
            if heladas_data and 'por_mes' in heladas_data and heladas_data['por_mes']:
                tmin_por_mes = {m['mes']: m for m in heladas_data['por_mes']}
            else:
                heladas_data = None  # Forzar fallback

        if not heladas_data:
            # Modelo empírico (fallback)
            tmin_monthly, tmin_std = estimar_tmin_mensual(lat, lon, alt)
            tmin_enso, estado_enso_str = ajustar_por_enso(tmin_monthly, oni)

        # 6. Evaluar riesgo por especie principal
        riesgos_especies = []
        nombre_mes = ['Ene', 'Feb', 'Mar', 'Abr', 'May', 'Jun',
                      'Jul', 'Ago', 'Sep', 'Oct', 'Nov', 'Dic']

        for especie, ha in datos_agro['top_especies']:
            if especie not in umbrales:
                continue

            umbral = umbrales[especie]

            # Meses críticos: Sep (8), Oct (9), Nov (10) = floración
            riesgo_floracion = 'BAJO'
            prob_max_floracion = 0

            for m_idx in [8, 9, 10]:  # Sep, Oct, Nov
                if heladas_data and tmin_por_mes:
                    mes_key = nombre_mes[m_idx]
                    if mes_key in tmin_por_mes:
                        prob = tmin_por_mes[mes_key]['prob_helada_mensual']
                    else:
                        prob = 0
                else:
                    prob, _, _ = probabilidad_helada_mensual(
                        tmin_enso[m_idx], tmin_std[m_idx],
                        umbral=umbral['floracion']
                    )

                prob_max_floracion = max(prob_max_floracion, prob)

            riesgo_floracion = clasificar_riesgo_helada(prob_max_floracion)

            riesgos_especies.append({
                'especie': especie,
                'superficie_ha': ha,
                'sensibilidad': umbral['sensibilidad'],
                'umbral_floracion_C': umbral['floracion'],
                'prob_helada_floracion': round(prob_max_floracion, 3),
                'riesgo': riesgo_floracion,
            })

        # 7. Determinar riesgo general de la comuna
        if riesgos_especies:
            riesgo_max = max(r['prob_helada_floracion'] for r in riesgos_especies)
            riesgo_general = clasificar_riesgo_helada(riesgo_max)

            # Superficie en riesgo
            sup_alto_riesgo = sum(
                r['superficie_ha'] for r in riesgos_especies
                if r['riesgo'] in ['ALTO', 'MUY ALTO']
            )
        else:
            riesgo_general = 'SIN DATOS'
            sup_alto_riesgo = 0

        # 8. Datos de precipitación
        extremos = calcular_riesgo_precipitacion_extrema(lat, lon)

        # 9. Construir alerta
        alerta = {
            'comuna': comuna,
            'region': datos_agro['region'],
            'lat': lat,
            'lon': lon,
            'alt_m': alt,

            # Producción agrícola
            'agro': {
                'superficie_total_ha': datos_agro['total_ha'],
                'n_especies': datos_agro['n_especies'],
                'top_especies': datos_agro['top_especies'],
            },

            # Riesgo heladas
            'heladas': {
                'riesgo_general': riesgo_general,
                'superficie_riesgo_alto_ha': round(sup_alto_riesgo, 1),
                'riesgos_por_especie': riesgos_especies,
                'fuente': 'CR2MET_tmin_REAL' if heladas_data else 'modelo_empirico',
            },

            # Señal ENSO
            'enso': {
                'estado': enso['estado'] if enso else 'desconocido',
                'oni': oni,
                'efecto': (
                    'Aumenta riesgo heladas tardías' if enso and enso['estado'] == 'La Niña' else
                    'Reduce riesgo heladas' if enso and enso['estado'] == 'El Niño' else
                    'Neutro'
                ),
            },

            # Precipitación extrema
            'precipitacion': extremos if extremos else None,
        }

        # Color para mapa
        alerta['color_mapa'] = {
            'MUY ALTO': '#d32f2f',  # rojo
            'ALTO': '#f57c00',       # naranja
            'MODERADO': '#fbc02d',   # amarillo
            'BAJO': '#388e3c',       # verde
            'MUY BAJO': '#1b5e20',   # verde oscuro
            'SIN DATOS': '#9e9e9e',  # gris
        }.get(riesgo_general, '#9e9e9e')

        alertas.append(alerta)

    # Ordenar por riesgo (más alto primero)
    orden = {'MUY ALTO': 0, 'ALTO': 1, 'MODERADO': 2, 'BAJO': 3, 'MUY BAJO': 4, 'SIN DATOS': 5}
    alertas.sort(key=lambda x: (orden.get(x['heladas']['riesgo_general'], 5), -x['agro']['superficie_total_ha']))

    if comunas_sin_coords:
        print(f"⚠ {len(comunas_sin_coords)} comunas sin coordenadas: {comunas_sin_coords[:10]}...")

    return alertas, enso


def alertas_a_geojson(alertas):
    """Convierte alertas a formato GeoJSON para visualización en mapa."""
    features = []
    for a in alertas:
        props = {
            'comuna': a['comuna'],
            'region': a['region'],
            'riesgo_helada': a['heladas']['riesgo_general'],
            'color': a['color_mapa'],
            'superficie_ha': a['agro']['superficie_total_ha'],
            'sup_riesgo_alto_ha': a['heladas']['superficie_riesgo_alto_ha'],
            'enso': a['enso']['estado'],
            'top_especie': a['agro']['top_especies'][0][0] if a['agro']['top_especies'] else '',
            'top_especie_ha': a['agro']['top_especies'][0][1] if a['agro']['top_especies'] else 0,
        }

        # Tooltip text
        especies_txt = ', '.join(f"{e}({h:.0f}ha)" for e, h in a['agro']['top_especies'][:3])
        props['tooltip'] = (
            f"<b>{a['comuna']}</b><br>"
            f"Riesgo helada: <b style='color:{a['color_mapa']}'>{a['heladas']['riesgo_general']}</b><br>"
            f"Superficie: {a['agro']['superficie_total_ha']:.0f} ha<br>"
            f"Especies: {especies_txt}<br>"
            f"ENSO: {a['enso']['estado']} → {a['enso']['efecto']}"
        )

        feature = {
            'type': 'Feature',
            'geometry': {
                'type': 'Point',
                'coordinates': [a['lon'], a['lat']],
            },
            'properties': props,
        }
        features.append(feature)

    return {
        'type': 'FeatureCollection',
        'features': features,
        'metadata': {
            'generado': datetime.now().isoformat(),
            'n_comunas': len(features),
            'fuente_clima': 'CR2MET/WorldClim/CHIRPS',
            'fuente_agro': 'CIREN/ODEPA Catastro Frutícola',
        },
    }


def generar_resumen_nacional(alertas, enso):
    """Genera resumen nacional de alertas."""
    total_comunas = len(alertas)
    total_ha = sum(a['agro']['superficie_total_ha'] for a in alertas)
    ha_riesgo = sum(a['heladas']['superficie_riesgo_alto_ha'] for a in alertas)

    por_riesgo = {}
    for a in alertas:
        r = a['heladas']['riesgo_general']
        if r not in por_riesgo:
            por_riesgo[r] = {'comunas': 0, 'ha': 0}
        por_riesgo[r]['comunas'] += 1
        por_riesgo[r]['ha'] += a['agro']['superficie_total_ha']

    return {
        'fecha': datetime.now().strftime('%Y-%m-%d'),
        'estado_enso': enso['estado'] if enso else 'desconocido',
        'oni': enso['oni_actual'] if enso else 0,
        'total_comunas_analizadas': total_comunas,
        'total_superficie_fruticola_ha': round(total_ha, 0),
        'superficie_riesgo_alto_ha': round(ha_riesgo, 0),
        'pct_en_riesgo': round(ha_riesgo / total_ha * 100, 1) if total_ha > 0 else 0,
        'distribucion_riesgo': {k: v for k, v in sorted(por_riesgo.items())},
        'interpretacion': enso['interpretacion_agro'] if enso else '',
    }


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--cr2met-tmin', action='store_true', help='Usar CR2MET Tmin real')
    parser.add_argument('--output', type=str, default=None)
    parser.add_argument('--geojson', type=str, default=None)
    args = parser.parse_args()

    print("Generando alertas agroclimáticas...")
    alertas, enso = generar_alertas_por_comuna(
        año_catastro=2024,
        usar_cr2met_tmin=args.cr2met_tmin
    )

    resumen = generar_resumen_nacional(alertas, enso)

    print(f"\n{'='*60}")
    print(f"RESUMEN NACIONAL — {resumen['fecha']}")
    print(f"ENSO: {resumen['estado_enso']} (ONI={resumen['oni']})")
    print(f"{'='*60}")
    print(f"Comunas analizadas: {resumen['total_comunas_analizadas']}")
    print(f"Superficie total: {resumen['total_superficie_fruticola_ha']:,.0f} ha")
    print(f"Superficie en riesgo ALTO: {resumen['superficie_riesgo_alto_ha']:,.0f} ha ({resumen['pct_en_riesgo']:.1f}%)")
    print(f"\nDistribución:")
    for riesgo, data in resumen['distribucion_riesgo'].items():
        print(f"  {riesgo:12s}: {data['comunas']:3d} comunas, {data['ha']:8,.0f} ha")

    print(f"\nTop 10 comunas con mayor riesgo:")
    for a in alertas[:10]:
        esp = a['agro']['top_especies'][0] if a['agro']['top_especies'] else ('?', 0)
        print(f"  {a['comuna']:20s} | {a['heladas']['riesgo_general']:10s} | "
              f"{a['agro']['superficie_total_ha']:6.0f} ha | {esp[0]} ({esp[1]:.0f}ha)")

    # Guardar
    out_dir = os.path.join(os.path.dirname(__file__), '..')
    if args.output:
        out_path = args.output
    else:
        out_path = os.path.join(out_dir, 'alertas_agroclimaticas_2026.json')

    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump({
            'resumen': resumen,
            'alertas': alertas,
        }, f, indent=2, ensure_ascii=False)
    print(f"\n✅ Alertas guardadas: {out_path}")

    # GeoJSON
    geojson_path = args.geojson or os.path.join(out_dir, 'alertas_agroclimaticas_2026.geojson')
    geojson = alertas_a_geojson(alertas)
    with open(geojson_path, 'w', encoding='utf-8') as f:
        json.dump(geojson, f, ensure_ascii=False)
    print(f"✅ GeoJSON guardado: {geojson_path}")
