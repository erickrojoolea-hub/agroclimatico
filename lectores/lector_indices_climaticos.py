#!/usr/bin/env python3
"""
LECTOR: Índices Climáticos (ONI/ENSO, PDO, SOI)
================================================
Lee índices de variabilidad climática de gran escala para pronóstico estacional.

Archivos fuente (en datos_precipitacion/):
  - indice_oni_enso.csv  (ONI mensual, 914 registros, 1950-2026)
  - indice_pdo.csv       (PDO mensual, 2076 registros, 1854-2026)
  - indice_soi.csv       (SOI mensual, 1800 registros, 1951-2026)

Referencias:
  - ONI: Huang et al. (2017). ERSST v5. NOAA CPC.
  - PDO: Mantua et al. (1997). Bull AMS.
  - SOI: Trenberth (1984). Mon Wea Rev. NOAA CPC.
"""

import csv
import os

BASE = os.path.join(os.path.dirname(__file__), '..', 'datos_precipitacion')

ONI_CSV = os.path.join(BASE, 'indice_oni_enso.csv')
PDO_CSV = os.path.join(BASE, 'indice_pdo.csv')
SOI_CSV = os.path.join(BASE, 'indice_soi.csv')


def cargar_oni():
    """Carga índice ONI (ENSO). Retorna lista de dicts."""
    # Mapeo trimestre → mes central
    TRIM_MES = {
        'DJF': 1, 'JFM': 2, 'FMA': 3, 'MAM': 4, 'AMJ': 5, 'MJJ': 6,
        'JJA': 7, 'JAS': 8, 'ASO': 9, 'SON': 10, 'OND': 11, 'NDJ': 12,
    }
    data = []
    with open(ONI_CSV, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                trim = row.get('trimestre', '')
                mes = TRIM_MES.get(trim, 0)
                if mes == 0:
                    continue
                data.append({
                    'año': int(row['año']),
                    'mes': mes,
                    'anomalia': float(row['anomalia']),
                    'estado': row.get('estado_enso', ''),
                })
            except (ValueError, KeyError):
                continue
    return data


def cargar_pdo():
    """Carga índice PDO. Retorna lista de dicts."""
    data = []
    with open(PDO_CSV, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                val = float(row['pdo'])
                if abs(val) < 90:  # filtrar missing
                    data.append({
                        'año': int(row['año']),
                        'mes': int(row['mes']),
                        'pdo': val,
                    })
            except (ValueError, KeyError):
                continue
    return data


def cargar_soi():
    """Carga índice SOI. Retorna lista de dicts."""
    data = []
    with open(SOI_CSV, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                data.append({
                    'año': int(row['año']),
                    'mes': int(row['mes']),
                    'soi': float(row['soi']),
                })
            except (ValueError, KeyError):
                continue
    return data


def estado_enso_actual():
    """
    Retorna estado ENSO actual y pronóstico simplificado.

    Clasificación NOAA:
      ONI ≥ +0.5 durante 5 trimestres consecutivos → El Niño
      ONI ≤ -0.5 durante 5 trimestres consecutivos → La Niña
    """
    oni = cargar_oni()
    if not oni:
        return None

    ultimo = oni[-1]
    oni_val = ultimo['anomalia']

    # Promedio últimos 3 meses
    ultimos3 = [r['anomalia'] for r in oni[-3:]]
    oni_3m = sum(ultimos3) / len(ultimos3)

    if oni_3m >= 0.5:
        estado = 'El Niño'
    elif oni_3m <= -0.5:
        estado = 'La Niña'
    else:
        estado = 'Neutro'

    # Tendencia
    if len(oni) >= 6:
        oni_6m_ant = sum(r['anomalia'] for r in oni[-6:-3]) / 3
        tendencia = 'calentándose' if oni_3m > oni_6m_ant + 0.2 else \
                    'enfriándose' if oni_3m < oni_6m_ant - 0.2 else 'estable'
    else:
        tendencia = 'desconocida'

    # PDO
    pdo = cargar_pdo()
    pdo_3m = sum(r['pdo'] for r in pdo[-3:]) / 3 if pdo else 0

    # SOI
    soi = cargar_soi()
    soi_3m = sum(r['soi'] for r in soi[-3:]) / 3 if soi else 0

    return {
        'estado': estado,
        'oni_actual': round(oni_val, 2),
        'oni_3m': round(oni_3m, 2),
        'tendencia': tendencia,
        'pdo_3m': round(pdo_3m, 2),
        'soi_3m': round(soi_3m, 2),
        'fecha_ultimo_dato': f"{ultimo['año']}-{ultimo['mes']:02d}",
        'interpretacion_agro': _interpretar_para_agro(estado, oni_3m, pdo_3m),
    }


def _interpretar_para_agro(estado, oni, pdo):
    """Genera interpretación agroclimática del estado ENSO."""
    if estado == 'El Niño':
        return (
            f"El Niño activo (ONI={oni:+.2f}). Para Chile central: "
            "invierno más lluvioso que lo normal (+20-40%), "
            "temperaturas mínimas más altas → menos heladas invernales. "
            "PERO: primaveras pueden ser inestables con lluvias tardías."
        )
    elif estado == 'La Niña':
        return (
            f"La Niña activa (ONI={oni:+.2f}). Para Chile central: "
            "invierno más seco que lo normal (-20-30%), "
            "ALERTA: primaveras más frías → mayor riesgo de heladas tardías "
            "durante floración (Sep-Nov). Riesgo elevado para frutales."
        )
    else:
        if oni > 0.2:
            return (
                f"ENSO Neutro tendiendo a cálido (ONI={oni:+.2f}). "
                "Condiciones cercanas a lo normal con leve sesgo lluvioso."
            )
        elif oni < -0.2:
            return (
                f"ENSO Neutro tendiendo a frío (ONI={oni:+.2f}). "
                "Precaución: posible transición a La Niña. "
                "Monitorear riesgo de heladas tardías."
            )
        else:
            return (
                f"ENSO Neutro (ONI={oni:+.2f}). "
                "Condiciones climáticas cercanas al promedio."
            )


if __name__ == '__main__':
    estado = estado_enso_actual()
    if estado:
        print(f"Estado ENSO: {estado['estado']}")
        print(f"ONI actual: {estado['oni_actual']}")
        print(f"ONI 3 meses: {estado['oni_3m']}")
        print(f"Tendencia: {estado['tendencia']}")
        print(f"PDO 3 meses: {estado['pdo_3m']}")
        print(f"SOI 3 meses: {estado['soi_3m']}")
        print(f"\n{estado['interpretacion_agro']}")
