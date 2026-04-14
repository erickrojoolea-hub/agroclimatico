#!/usr/bin/env python3
"""
LECTOR: CR2 Estaciones de Precipitación
========================================
Lee datos de 879 estaciones pluviométricas compiladas por CR2 (U. Chile).

Archivos fuente:
  - cr2_prAmon_2019/cr2_prAmon_2019_stations.txt  (metadata estaciones)
  - cr2_prAmon_2019/cr2_prAmon_2019.txt            (series temporales mensual)
  - cr2_prDaily_2020/cr2_prDaily_2020_stations.txt  (metadata diario)
  - cr2_prDaily_2020/cr2_prDaily_2020.txt           (series temporales diario)

Formato: CSV transpuesto (estaciones en columnas, tiempo en filas).
  Fila 1: codigo_estacion
  Fila 5: nombre
  Fila 7: latitud
  Fila 8: longitud
  Fila 9+: datos (fecha en col 0, valores en cols 1..N)
  Missing: -9999

Referencia:
  CR2, U. de Chile. Compilado por Francisca Muñoz.
  http://www.cr2.cl/datos-de-precipitacion/
"""

import csv
import os
import json
import math
from collections import defaultdict

# Rutas relativas al directorio datos_geo/CR2/
_PROJECT = os.path.join(os.path.dirname(__file__), '..')
BASE_CR2 = os.path.join(_PROJECT, 'datos_geo', 'CR2')

# Buscar estaciones primero en data/estaciones/ (repo-local), luego en datos_geo/CR2/
_LOCAL_STATIONS = os.path.join(_PROJECT, 'data', 'estaciones', 'cr2_prAmon_2019_stations.txt')
_SYMLINK_STATIONS = os.path.join(BASE_CR2, 'cr2_prAmon_2019', 'cr2_prAmon_2019_stations.txt')
MONTHLY_STATIONS = _LOCAL_STATIONS if os.path.exists(_LOCAL_STATIONS) else _SYMLINK_STATIONS
MONTHLY_DATA = os.path.join(BASE_CR2, 'cr2_prAmon_2019', 'cr2_prAmon_2019.txt')
DAILY_STATIONS = os.path.join(BASE_CR2, 'cr2_prDaily_2020', 'cr2_prDaily_2020_stations.txt')
DAILY_DATA = os.path.join(BASE_CR2, 'cr2_prDaily_2020', 'cr2_prDaily_2020.txt')


def cargar_estaciones(path=MONTHLY_STATIONS):
    """
    Carga metadata de estaciones CR2.

    Retorna: dict {codigo: {nombre, lat, lon, alt, cuenca, inicio, fin, n_obs}}
    """
    estaciones = {}
    with open(path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            code = row['codigo_estacion']
            estaciones[code] = {
                'nombre': row['nombre'],
                'lat': float(row['latitud']),
                'lon': float(row['longitud']),
                'alt': float(row['altura']) if row['altura'] else 0,
                'institucion': row['institucion'],
                'cuenca': row['nombre_cuenca'],
                'subcuenca': row['nombre_sub_cuenca'],
                'inicio': row['inicio_observaciones'],
                'fin': row['fin_observaciones'],
                'n_obs': int(row['cantidad_observaciones']) if row['cantidad_observaciones'] else 0,
            }
    return estaciones


def cargar_series_mensual(path=MONTHLY_DATA):
    """
    Carga series de precipitación mensual (formato transpuesto CR2).

    El archivo tiene estaciones en columnas y meses en filas.
    Las primeras ~8 filas son metadata, luego vienen los datos.

    Retorna: dict {codigo_estacion: {(año, mes): valor_mm}}
    """
    series = defaultdict(dict)

    with open(path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    # Fila 0: codigos de estación
    codigos = lines[0].strip().split(',')
    # codigos[0] = 'codigo_estacion', codigos[1:] = códigos
    station_codes = codigos[1:]

    # Encontrar donde empiezan los datos (buscar primera línea con fecha)
    data_start = 0
    for i, line in enumerate(lines):
        if line.strip() and line[0:4].isdigit() and '-' in line[:10]:
            data_start = i
            break

    # Leer datos
    for line in lines[data_start:]:
        parts = line.strip().split(',')
        if len(parts) < 2:
            continue

        fecha = parts[0]  # formato YYYY-MM-01 o YYYY-MM
        try:
            year = int(fecha[:4])
            month = int(fecha[5:7])
        except (ValueError, IndexError):
            continue

        for j, val_str in enumerate(parts[1:]):
            if j >= len(station_codes):
                break
            try:
                val = float(val_str)
                if val != -9999 and val >= 0:
                    series[station_codes[j]][(year, month)] = val
            except ValueError:
                continue

    return dict(series)


def climatologia_estacion(series_estacion, periodo=(1991, 2020)):
    """
    Calcula climatología mensual para una estación.

    Parámetros:
        series_estacion: dict {(año, mes): valor_mm}
        periodo: tupla (año_inicio, año_fin)

    Retorna: lista de 12 valores [ene..dic] en mm, o None si datos insuficientes
    """
    mensual = defaultdict(list)
    for (year, month), val in series_estacion.items():
        if periodo[0] <= year <= periodo[1]:
            mensual[month].append(val)

    if not mensual or min(len(v) for v in mensual.values()) < 10:
        return None

    return [round(sum(mensual.get(m, [0])) / len(mensual.get(m, [1])), 1)
            for m in range(1, 13)]


def buscar_estaciones_cercanas(lat, lon, n=5, max_dist_km=50, estaciones=None):
    """
    Busca las N estaciones CR2 más cercanas a un punto.

    Retorna: lista de (codigo, nombre, dist_km, lat, lon)
    """
    if estaciones is None:
        estaciones = cargar_estaciones()

    resultados = []
    for code, est in estaciones.items():
        dlat = math.radians(est['lat'] - lat)
        dlon = math.radians(est['lon'] - lon)
        a = (math.sin(dlat/2)**2 +
             math.cos(math.radians(lat)) * math.cos(math.radians(est['lat'])) *
             math.sin(dlon/2)**2)
        dist = 6371 * 2 * math.asin(math.sqrt(a))

        if dist <= max_dist_km:
            resultados.append((code, est['nombre'], round(dist, 1), est['lat'], est['lon']))

    resultados.sort(key=lambda x: x[2])
    return resultados[:n]


def precipitacion_para_punto(lat, lon, estaciones=None, series=None, periodo=(1991, 2020)):
    """
    Obtiene la mejor estimación de precipitación mensual para un punto
    usando estaciones CR2 cercanas (IDW - Inverse Distance Weighting).

    Retorna: dict con precip_monthly, fuente, estaciones_usadas
    """
    if estaciones is None:
        estaciones = cargar_estaciones()
    if series is None:
        series = cargar_series_mensual()

    cercanas = buscar_estaciones_cercanas(lat, lon, n=5, max_dist_km=50, estaciones=estaciones)

    if not cercanas:
        return None

    # Calcular climatologías
    clims = []
    for code, nombre, dist, elat, elon in cercanas:
        if code in series:
            clim = climatologia_estacion(series[code], periodo)
            if clim:
                clims.append((dist, clim, nombre, code))

    if not clims:
        return None

    # IDW con la estación más cercana si solo hay una
    if len(clims) == 1:
        dist, clim, nombre, code = clims[0]
        return {
            'precip_monthly': clim,
            'precip_annual': round(sum(clim), 1),
            'fuente': f'CR2_estacion_{code}',
            'estacion': nombre,
            'dist_km': dist,
            'metodo': 'estación más cercana',
            'periodo': f'{periodo[0]}-{periodo[1]}',
        }

    # IDW: peso = 1/d²
    monthly = [0.0] * 12
    peso_total = 0
    est_usadas = []

    for dist, clim, nombre, code in clims:
        peso = 1.0 / max(dist, 0.1) ** 2
        peso_total += peso
        for m in range(12):
            monthly[m] += clim[m] * peso
        est_usadas.append(f"{nombre} ({dist:.0f}km)")

    monthly = [round(v / peso_total, 1) for v in monthly]

    return {
        'precip_monthly': monthly,
        'precip_annual': round(sum(monthly), 1),
        'fuente': 'CR2_IDW',
        'estaciones_usadas': est_usadas,
        'metodo': f'IDW con {len(clims)} estaciones',
        'periodo': f'{periodo[0]}-{periodo[1]}',
    }


if __name__ == '__main__':
    # Test
    est = cargar_estaciones()
    print(f"Estaciones cargadas: {len(est)}")

    series = cargar_series_mensual()
    print(f"Series cargadas: {len(series)}")

    # Test para Lolol
    result = precipitacion_para_punto(-34.73, -71.65, est, series)
    if result:
        print(f"\nLolol: {result['precip_annual']} mm/año")
        print(f"Fuente: {result['fuente']}")
        print(f"Método: {result['metodo']}")
