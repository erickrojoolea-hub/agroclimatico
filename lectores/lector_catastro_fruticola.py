#!/usr/bin/env python3
"""
LECTOR: Catastro Frutícola + Producción Agrícola por Comuna
============================================================
Carga datos de superficie frutícola y producción agrícola por comuna,
para cruzar con datos climáticos.

Archivos fuente:
  - BD_agro/catastro_fruticola_consolidado.csv (5,025 registros, 228 comunas, 59 especies)
  - CATASTRO_FRUTICOLA/catastro_fruticola_2024.csv (103,717 registros detallados)
  - ODEPA/*.xls (cultivos anuales, hortalizas, vides, superficie frutícola)

Fuente: CIREN/ODEPA Catastro Frutícola Nacional.
"""

import csv
import os
import json
from collections import defaultdict

BASE = os.path.join(os.path.dirname(__file__), '..')

# Buscar primero en data/agro/ (repo-local), luego en BD_agro/ (symlink)
_LOCAL_CONSOLIDADO = os.path.join(BASE, 'data', 'agro', 'catastro_fruticola_consolidado.csv')
_SYMLINK_CONSOLIDADO = os.path.join(BASE, 'BD_agro', 'catastro_fruticola_consolidado.csv')
CONSOLIDADO = _LOCAL_CONSOLIDADO if os.path.exists(_LOCAL_CONSOLIDADO) else _SYMLINK_CONSOLIDADO

DETALLADO_DIR = os.path.join(BASE, 'datos_geo', 'CATASTRO_FRUTICOLA')

# Coordenadas centrales aproximadas de comunas agrícolas principales
# Fuente: INE / IDE Chile
COMUNAS_COORDS = {
    # --- Región de Arica y Parinacota / Tarapacá ---
    "Arica": (-18.48, -70.33, 15), "Iquique": (-20.21, -70.13, 5),
    # --- Región de Atacama ---
    "Copiapó": (-27.37, -70.33, 391), "Vallenar": (-28.57, -70.76, 380),
    "Huasco": (-28.46, -71.22, 10), "Alto del Carmen": (-28.75, -70.48, 800),
    "Caldera": (-27.07, -70.27, 180), "Freirina": (-28.35, -70.18, 380),
    "Tierra Amarilla": (-30.27, -71.03, 780),
    # --- Región de Coquimbo ---
    "La Serena": (-29.90, -71.25, 28), "Coquimbo": (-29.95, -71.34, 16),
    "Ovalle": (-30.60, -71.20, 220), "Vicuña": (-30.03, -70.71, 643),
    "Monte Patria": (-30.69, -70.96, 365), "Paihuano": (-30.03, -70.52, 1050),
    "Paiguano": (-30.25, -70.78, 680),
    "Illapel": (-31.63, -71.17, 316), "Salamanca": (-31.77, -70.97, 510),
    "Combarbalá": (-31.65, -71.35, 420), "Punitaqui": (-30.85, -71.30, 480),
    "Canela": (-31.33, -71.53, 400), "La Higuera": (-29.25, -71.23, 380),
    "Río Hurtado": (-30.33, -71.25, 500), "Los Vilos": (-31.88, -71.52, 180),
    # --- Región de Valparaíso ---
    "La Ligua": (-32.45, -71.23, 65), "Petorca": (-32.25, -70.93, 420),
    "Cabildo": (-32.42, -71.07, 250), "San Felipe": (-32.75, -70.73, 640),
    "Los Andes": (-32.83, -70.60, 820), "Putaendo": (-32.63, -70.72, 740),
    "Catemu": (-32.80, -71.07, 410), "Llay-Llay": (-32.84, -71.00, 500),
    "Panquehue": (-32.80, -70.85, 470), "Quillota": (-32.88, -71.25, 120),
    "La Cruz": (-32.83, -71.23, 100), "Hijuelas": (-32.80, -71.15, 160),
    "Limache": (-33.00, -71.27, 80), "Olmué": (-33.00, -71.20, 200),
    "Valparaíso": (-33.05, -71.62, 42), "Casablanca": (-33.32, -71.41, 210),
    # --- Región Metropolitana ---
    "Santiago": (-33.45, -70.65, 520), "Buin": (-33.73, -70.73, 470),
    "Paine": (-33.81, -70.74, 460), "Isla de Maipo": (-33.75, -70.90, 360),
    "Talagante": (-33.66, -70.93, 350), "Melipilla": (-33.70, -71.22, 170),
    "Curacaví": (-33.40, -71.13, 270), "María Pinto": (-33.51, -71.12, 300),
    "Alhué": (-34.03, -71.10, 330),
    # --- Región de O'Higgins ---
    "Rancagua": (-34.17, -70.74, 500), "Rengo": (-34.40, -70.86, 310),
    "San Fernando": (-34.58, -71.00, 340), "Chimbarongo": (-34.72, -71.04, 220),
    "Peumo": (-34.39, -71.17, 180), "Pichidegua": (-34.37, -71.28, 150),
    "Requínoa": (-34.29, -70.81, 370), "Lolol": (-34.73, -71.65, 180),
    "Nancagua": (-34.67, -71.19, 200), "Codegua": (-34.32, -70.82, 500),
    "Coinco": (-34.38, -71.07, 520), "Coltauco": (-34.52, -70.97, 480),
    "Doñihue": (-34.43, -71.08, 480), "Doñiihue": (-34.43, -71.08, 480), "Graneros": (-34.77, -70.75, 540),
    "Las Cabras": (-34.28, -71.00, 500), "Machalí": (-34.47, -70.65, 580),
    "Malloa": (-34.85, -71.03, 450), "Marchihue": (-34.50, -71.97, 300),
    "Mostazal": (-34.17, -70.90, 520), "Olivar": (-34.73, -71.13, 420),
    "Palmilla": (-34.80, -71.42, 380), "Peralillo": (-34.57, -71.87, 300),
    "Placilla": (-33.05, -71.55, 420), "Pichilemu": (-34.38, -72.17, 200),
    "Pumanque": (-34.83, -72.03, 320), "Quinta de Tilcoco": (-34.15, -71.05, 540),
    "San Vicente": (-34.65, -71.07, 480), "Santa Cruz": (-34.63, -71.38, 360),
    "La Estrella": (-31.05, -71.55, 550), "Litueche": (-34.37, -71.73, 280),
    "Navidad": (-34.42, -71.90, 280), "Paredones": (-34.98, -71.90, 280),
    "Chépica": (-34.77, -71.03, 400),
    # --- Región del Maule ---
    "Curicó": (-34.98, -71.24, 228), "Sagrada Familia": (-34.99, -71.38, 145),
    "Molina": (-35.12, -71.28, 210), "Romeral": (-34.96, -71.12, 350),
    "Teno": (-34.87, -71.17, 240), "Talca": (-35.43, -71.67, 102),
    "San Javier": (-35.59, -71.73, 90), "Cauquenes": (-35.97, -72.32, 120),
    "San Clemente": (-35.55, -71.47, 200), "Linares": (-35.85, -71.60, 160),
    "Hualañe": (-34.83, -71.35, 380), "Licantén": (-35.07, -72.53, 100),
    "Rauco": (-34.92, -71.33, 420), "Río Claro": (-34.85, -70.58, 650),
    "Curepto": (-35.22, -72.33, 100), "Maule": (-35.17, -71.67, 200),
    "Pelarco": (-35.28, -71.89, 220), "Pencahue": (-35.33, -71.90, 240),
    "San Rafael": (-35.33, -71.98, 240), "Empedrado": (-35.50, -72.40, 110),
    "Constitución": (-35.33, -72.42, 80), "Colbún": (-35.63, -71.47, 320),
    "Longaví": (-35.83, -71.70, 240), "Parral": (-36.17, -71.85, 280),
    "Retiro": (-38.03, -71.97, 320), "Villa Alegre": (-36.00, -71.92, 280),
    "Yerbas Buenas": (-35.75, -71.43, 360), "Chanco": (-35.68, -72.47, 120),
    # --- Región de Ñuble ---
    "Chillán": (-36.62, -72.10, 124), "Chillán Viejo": (-36.62, -72.13, 200),
    "Coihueco": (-36.62, -71.83, 270), "Bulnes": (-36.74, -72.44, 150),
    "El Carmen": (-36.37, -71.82, 280), "Pemuco": (-36.67, -72.03, 230),
    "Pinto": (-36.33, -71.77, 260), "Quillón": (-36.65, -72.37, 140),
    "San Carlos": (-36.77, -71.97, 300), "San Fabián": (-36.12, -71.65, 320),
    "San Ignacio": (-36.28, -71.73, 280), "San Nicolás": (-36.30, -71.85, 260),
    "Ninhue": (-36.52, -72.27, 180), "Ñiquén": (-36.52, -71.98, 240),
    "Cobquecura": (-36.13, -72.78, 100), "Portezuelo": (-36.83, -71.67, 380),
    "Ranquil": (-36.82, -72.25, 160), "Yungay": (-36.90, -71.75, 320),
    # --- Región del Biobío ---
    "Los Ángeles": (-37.47, -72.35, 139), "Los Angeles": (-37.47, -72.35, 139),
    "Cabrero": (-37.03, -71.98, 280), "Mulchen": (-37.72, -72.27, 200),
    "Nacimiento": (-37.48, -72.07, 260), "Negrete": (-37.47, -72.37, 200),
    "Quilaco": (-37.62, -71.80, 300), "Quilleco": (-36.90, -72.47, 160),
    "Santa Bárbara": (-37.62, -72.20, 200), "Tucapel": (-37.45, -72.00, 280),
    "Yumbel": (-37.07, -72.53, 160), "San Rosendo": (-37.35, -71.93, 300),
    "Coelemu": (-36.37, -72.63, 120), "Florida": (-37.95, -71.82, 300),
    "Lebu": (-37.62, -73.65, 50), "Santa Juana": (-37.67, -72.63, 120),
    "Tomé": (-36.53, -72.33, 100), "Contulmo": (-37.98, -72.87, 160),
    "Los Sauces": (-38.17, -72.53, 120), "San Pablo": (-37.63, -72.00, 280),
    # --- Región de La Araucanía ---
    "Temuco": (-38.74, -72.60, 114), "Angol": (-37.81, -72.73, 180),
    "Collipulli": (-37.98, -71.83, 280), "Cunco": (-38.95, -71.97, 320),
    "Curacautín": (-38.43, -71.97, 380), "Curarrehue": (-39.37, -71.60, 400),
    "Ercilla": (-37.72, -72.02, 260), "Freire": (-38.82, -72.22, 140),
    "Galvarino": (-38.17, -72.33, 140), "Gorbea": (-39.13, -72.28, 160),
    "Lautaro": (-38.50, -72.00, 140), "Loncoche": (-39.45, -72.42, 120),
    "Melipeuco": (-38.73, -71.80, 420), "Nueva Imperial": (-38.73, -72.55, 120),
    "Padre las Casas": (-38.77, -72.05, 140), "Perquenco": (-38.22, -71.97, 280),
    "Pitrufquén": (-38.95, -72.03, 200), "Pucón": (-39.30, -71.95, 360),
    "Renaico": (-37.68, -72.07, 260), "Teodoro Schmidt": (-38.68, -72.53, 110),
    "Traiguén": (-37.78, -72.17, 240), "Victoria": (-38.23, -71.89, 320),
    "Vilcún": (-38.78, -71.83, 360), "Villarrica": (-39.28, -71.92, 320),
    # --- Región de Los Ríos ---
    "Valdivia": (-39.82, -73.25, 50), "La Unión": (-40.28, -72.57, 120),
    "Lanco": (-40.43, -72.60, 100), "Mariquina": (-39.70, -72.43, 100),
    "Máfil": (-39.72, -72.50, 80), "Paillaco": (-40.42, -72.43, 110),
    "Panguipulli": (-39.62, -72.20, 140), "Río Bueno": (-40.33, -72.40, 120),
    "Futrono": (-40.18, -71.87, 200), "Lago Ranco": (-40.30, -72.07, 150),
    "Los Lagos": (-40.50, -72.22, 100),
    # --- Región de Los Lagos ---
    "Osorno": (-40.57, -73.13, 30), "Puerto Montt": (-41.47, -72.93, 10),
    "Fresia": (-41.38, -72.07, 180), "Frutillar": (-41.13, -72.30, 250),
    "Llanquihue": (-41.28, -72.37, 200), "Los Muermos": (-41.43, -72.08, 180),
    "Puerto Octay": (-41.13, -72.25, 240), "Purranque": (-40.78, -72.37, 140),
    "Puyehue": (-40.73, -72.17, 180), "Río Negro": (-41.43, -72.33, 180),
}


def cargar_catastro_consolidado():
    """
    Carga catastro frutícola consolidado por comuna.

    Retorna: lista de dicts {año, region, comuna, especie, superficie_ha, n_arboles}
    """
    registros = []
    with open(CONSOLIDADO, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                registros.append({
                    'año': int(row['Anio']),
                    'region': row['Region'],
                    'comuna': row['Comuna'],
                    'especie': row['Especie'],
                    'superficie_ha': float(row['Superficie (ha)']),
                    'n_arboles': int(float(row['Numero de arboles'])),
                })
            except (ValueError, KeyError):
                continue
    return registros


def superficie_por_comuna(año=2024):
    """
    Calcula superficie frutícola total y por especie para cada comuna.

    Retorna: dict {comuna: {total_ha, especies: {especie: ha}, n_especies, top_especies}}
    """
    registros = cargar_catastro_consolidado()
    comunas = defaultdict(lambda: {'total_ha': 0, 'especies': defaultdict(float), 'n_arboles': 0})

    for r in registros:
        if r['año'] == año:
            c = r['comuna']
            comunas[c]['total_ha'] += r['superficie_ha']
            comunas[c]['especies'][r['especie']] += r['superficie_ha']
            comunas[c]['n_arboles'] += r['n_arboles']
            comunas[c]['region'] = r['region']

    resultado = {}
    for comuna, data in comunas.items():
        especies_sorted = sorted(data['especies'].items(), key=lambda x: -x[1])
        resultado[comuna] = {
            'total_ha': round(data['total_ha'], 1),
            'n_arboles': data['n_arboles'],
            'region': data.get('region', ''),
            'n_especies': len(data['especies']),
            'top_especies': [(e, round(h, 1)) for e, h in especies_sorted[:5]],
            'todas_especies': {e: round(h, 1) for e, h in especies_sorted},
        }

    return resultado


def coords_comuna(comuna):
    """Retorna (lat, lon, alt) para una comuna, o None."""
    return COMUNAS_COORDS.get(comuna)


def especies_sensibles_helada():
    """
    Retorna umbrales de daño por helada para las principales especies frutícolas.

    Referencia: INIA Boletín Técnico, Atlas Agroclimático (Santibáñez, 2017),
    UC Davis Fruit & Nut Research.
    """
    return {
        'Cerezo':      {'floracion': -2.2, 'cuaja': -1.1, 'fruto_verde': -1.0, 'sensibilidad': 'ALTA'},
        'Almendro':    {'floracion': -2.8, 'cuaja': -1.1, 'fruto_verde': -1.0, 'sensibilidad': 'MUY ALTA'},
        'Palto':       {'floracion': -1.0, 'cuaja': -0.5, 'fruto_verde': -1.5, 'sensibilidad': 'MUY ALTA'},
        'Nogal':       {'floracion': -1.0, 'cuaja': -1.0, 'fruto_verde': -2.0, 'sensibilidad': 'MODERADA'},
        'Vid de Mesa': {'floracion': -1.5, 'cuaja': -0.5, 'fruto_verde': -2.0, 'sensibilidad': 'ALTA'},
        'Avellano':    {'floracion': -3.5, 'cuaja': -2.0, 'fruto_verde': -2.0, 'sensibilidad': 'MODERADA'},
        'Manzano Rojo':{'floracion': -2.2, 'cuaja': -1.1, 'fruto_verde': -2.0, 'sensibilidad': 'MODERADA'},
        'Arándano Americano': {'floracion': -2.0, 'cuaja': -1.0, 'fruto_verde': -1.5, 'sensibilidad': 'ALTA'},
        'Ciruelo Europeo': {'floracion': -2.2, 'cuaja': -1.1, 'fruto_verde': -2.0, 'sensibilidad': 'MODERADA'},
        'Mandarino':   {'floracion': -1.0, 'cuaja': -0.5, 'fruto_verde': -2.0, 'sensibilidad': 'ALTA'},
        'Limonero':    {'floracion': -1.0, 'cuaja': -0.5, 'fruto_verde': -2.0, 'sensibilidad': 'MUY ALTA'},
        'Naranjo':     {'floracion': -1.5, 'cuaja': -1.0, 'fruto_verde': -2.0, 'sensibilidad': 'ALTA'},
        'Nectarino':   {'floracion': -2.8, 'cuaja': -1.1, 'fruto_verde': -2.0, 'sensibilidad': 'MODERADA'},
        'Kiwi':        {'floracion': -1.0, 'cuaja': -0.5, 'fruto_verde': -1.5, 'sensibilidad': 'ALTA'},
        'Olivo':       {'floracion': -2.0, 'cuaja': -1.5, 'fruto_verde': -3.0, 'sensibilidad': 'BAJA'},
    }


if __name__ == '__main__':
    sup = superficie_por_comuna(2024)
    print(f"Comunas con datos 2024: {len(sup)}")
    print("\nTop 10 comunas por superficie:")
    for c, d in sorted(sup.items(), key=lambda x: -x[1]['total_ha'])[:10]:
        top_esp = ', '.join(f"{e}({h:.0f}ha)" for e, h in d['top_especies'][:3])
        print(f"  {c:20s} | {d['total_ha']:8.1f} ha | {d['n_especies']} esp | {top_esp}")
