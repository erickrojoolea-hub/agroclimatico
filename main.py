"""
Visor Agroclimático Interactivo - Estilo Fernando Santibáñez / INFODEP
======================================================================
Mapa clickeable → detección de localidad → informe automático.
Solo comunas con datos PVsyst disponibles se habilitan.
"""
import streamlit as st
import pandas as pd
import numpy as np
import folium
from streamlit_folium import st_folium
import io
import os
import math

from climate_engine import (
    parse_pvsyst_csv, calc_monthly_climate, calc_dias_calidos_table,
    calc_heladas_intensidad, calc_winkler, calc_indice_fototermico,
    calc_horas_frescor_madurez, calc_bioclimatic_table,
    generar_analisis_textual, ESPECIES, PRECIP_DB
)
from pdf_generator import generate_pdf
from agro_database import (
    PLAGAS_ENFERMEDADES, CALENDARIO_FITOSANITARIO, FERTILIZACION,
    COSTOS_HECTAREA, DRONES_INFO, get_especie_data, resumen_costos,
)
try:
    from ndvi_engine import (
        init_gee, get_ndvi_timeseries, get_ndvi_annual_summary,
        get_ndvi_map_url, classify_land_use,
    )
    GEE_AVAILABLE = True
except Exception:
    GEE_AVAILABLE = False

# ── Registro de comunas con datos disponibles ────────────────────────────────
# Cada entrada: nombre, lat, lon, alt, archivo_pvsyst, precipitación mensual
COMUNAS_DB = {
    "Curacaví": {
        "lat": -33.40,
        "lon": -71.13,
        "alt": 200,
        "radio_km": 10,
        "pvsyst_file": None,  # se busca automáticamente
        "precip": [1, 7, 5, 12, 78, 111, 84, 72, 22, 15, 7, 3],
        "hr": [55, 57, 60, 65, 73, 79, 80, 77, 70, 64, 58, 55],
        "fuente_meteo": "Meteonorm 8.2 (2010-2019)",
        "region": "Metropolitana",
    },
    "Talhuén": {
        "lat": -34.4873,
        "lon": -71.3377,
        "alt": 144,
        "radio_km": 10,
        "pvsyst_file": None,
        "precip": [3.7, 4.9, 8.5, 24.7, 62.0, 90.4, 71.2, 54.0, 33.1, 14.8, 7.9, 5.7],
        "hr": [58, 60, 63, 70, 78, 83, 84, 80, 74, 67, 61, 58],
        "fuente_meteo": "Meteonorm 8.2 (2010-2019)",
        "region": "O'Higgins",
        "parcela_polygon": [
            [-71.34349, -34.48441],
            [-71.34392, -34.48648],
            [-71.33752, -34.48787],
            [-71.33728, -34.48604],
            [-71.34349, -34.48441],
        ],
    },
    # ── Agregar más comunas aquí ──
}

# Paths posibles para archivos PVsyst por comuna
_BASE = os.path.dirname(os.path.abspath(__file__))
PVSYST_FILES = {
    "Curacaví": [
        os.path.join(_BASE, "data", "Curacavi.CSV"),
        "/Users/erickrojoolea/Downloads/Curacavi.CSV",
    ],
    "Talhuén": [
        os.path.join(_BASE, "data", "Talhuen san vicente.CSV"),
        "/Users/erickrojoolea/Downloads/Talhuen san vicente.CSV",
    ],
}


def find_pvsyst_file(comuna_name=None):
    """Busca el archivo PVsyst para una comuna dada."""
    if comuna_name and comuna_name in PVSYST_FILES:
        for p in PVSYST_FILES[comuna_name]:
            if os.path.exists(p):
                return p
    # Fallback: buscar en todas
    for paths in PVSYST_FILES.values():
        for p in paths:
            if os.path.exists(p):
                return p
    return None


def haversine_km(lat1, lon1, lat2, lon2):
    """Distancia en km entre dos puntos geográficos."""
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.asin(math.sqrt(a))


def find_nearest_comuna(lat, lon):
    """Encuentra la comuna más cercana con datos. Retorna (nombre, info, distancia_km) o None."""
    best = None
    best_dist = float('inf')
    for nombre, info in COMUNAS_DB.items():
        d = haversine_km(lat, lon, info['lat'], info['lon'])
        if d < info['radio_km'] and d < best_dist:
            best = (nombre, info, d)
            best_dist = d
    return best


# ── Config ───────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Análisis Agroclimático Chile",
    page_icon="A",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ── CSS ──────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;700&display=swap');
    html, body, [class*="st-"] { font-family: 'DM Sans', sans-serif; }

    .main-header {
        background: linear-gradient(135deg, #1B5E20 0%, #2E7D32 50%, #43A047 100%);
        padding: 1rem 2rem;
        border-radius: 8px;
        margin-bottom: 1rem;
        color: white;
    }
    .main-header h1 { color: white; margin: 0; font-size: 1.6rem; font-weight: 700; }
    .main-header p { color: #C8E6C9; margin: 0.3rem 0 0 0; font-size: 0.82rem; }

    .metric-card {
        background: #FAFAFA;
        border-left: 4px solid #2E7D32;
        padding: 0.7rem 0.8rem;
        border-radius: 0 6px 6px 0;
        box-shadow: 0 1px 3px rgba(0,0,0,0.06);
        margin-bottom: 0.5rem;
    }
    .metric-card h3 { margin: 0; font-size: 0.72rem; color: #666; font-weight: 500; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .metric-card .value { font-size: 1.5rem; font-weight: 700; color: #2E7D32; }
    .metric-card .unit { font-size: 0.7rem; color: #999; }

    .no-data-box {
        background: #FFF3E0;
        border: 2px dashed #FF9800;
        border-radius: 8px;
        padding: 1.5rem;
        text-align: center;
        margin: 1rem 0;
    }
    .no-data-box h2 { color: #E65100; margin-bottom: 0.3rem; font-size: 1.1rem; }
    .no-data-box p { color: #BF360C; font-size: 0.88rem; }

    .site-card {
        background: #F1F8E9;
        border-left: 4px solid #2E7D32;
        border-radius: 0 8px 8px 0;
        padding: 1rem 1.2rem;
        margin-bottom: 0.5rem;
    }
    .site-card h3 { color: #1B5E20; margin: 0 0 0.4rem 0; font-size: 1.1rem; }
    .site-card .detail { color: #333; font-size: 0.85rem; line-height: 1.7; }

    .hint-box {
        background: #E8F5E9;
        border-left: 4px solid #2E7D32;
        padding: 0.6rem 1rem;
        border-radius: 0 6px 6px 0;
        font-size: 0.82rem;
        color: #1B5E20;
        margin: 0.5rem 0;
    }

    .comuna-chip {
        display: inline-block;
        background: #2E7D32;
        color: white;
        padding: 0.2rem 0.7rem;
        border-radius: 16px;
        font-size: 0.78rem;
        margin: 0.15rem;
    }

    /* Tabs: 2 rows, wrap, shorter labels */
    .stTabs [data-baseweb="tab-list"] {
        gap: 4px;
        flex-wrap: wrap !important;
        overflow-x: visible !important;
    }
    .stTabs [data-baseweb="tab"] {
        background-color: #E8F5E9;
        border-radius: 4px 4px 0 0;
        padding: 6px 12px;
        font-size: 0.82rem;
        white-space: nowrap;
    }
    .stTabs [aria-selected="true"] {
        background-color: #2E7D32 !important;
        color: white !important;
    }
</style>
""", unsafe_allow_html=True)


def render_metric(label, value, unit=""):
    st.markdown(f"""
    <div class="metric-card">
        <h3>{label}</h3>
        <span class="value">{value}</span>
        <span class="unit">{unit}</span>
    </div>
    """, unsafe_allow_html=True)


# ── Inicializar session_state ────────────────────────────────────────────────
if 'pin_lat' not in st.session_state:
    st.session_state.pin_lat = None
    st.session_state.pin_lon = None
    st.session_state.comuna_match = None
    st.session_state.processed = False


# ── Header ───────────────────────────────────────────────────────────────────
st.markdown("""
<div class="main-header">
    <h1>Visor Agroclimático Chile</h1>
    <p>Seleccione una ubicación en el mapa o en el panel derecho para generar el informe</p>
</div>
""", unsafe_allow_html=True)

# ── Layout: mapa grande + panel derecho ──────────────────────────────────────
col_map, col_panel = st.columns([3, 2])

with col_map:
    # Mapa centrado en Chile central
    center_lat = -33.5
    center_lon = -71.0
    zoom = 8

    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=zoom,
        tiles=None,
        control_scale=True,
    )

    # Capa base — Google Satellite Hybrid (satelital + nombres/rutas)
    folium.TileLayer(
        'https://{s}.google.com/vt/lyrs=s,h&x={x}&y={y}&z={z}',
        subdomains=['mt0', 'mt1', 'mt2', 'mt3'],
        name='Satelital',
        attr='Google',
        max_zoom=21,
        overlay=False,
    ).add_to(m)
    folium.TileLayer(
        'OpenStreetMap',
        name='Calles',
        overlay=False,
    ).add_to(m)

    # Marcadores de comunas con datos disponibles
    for nombre, info in COMUNAS_DB.items():
        folium.CircleMarker(
            [info['lat'], info['lon']],
            radius=12,
            color='#2E7D32',
            fill=True,
            fillColor='#4CAF50',
            fillOpacity=0.7,
            popup=folium.Popup(
                f"<b>{nombre}</b><br>"
                f"Región: {info['region']}<br>"
                f"Alt: {info['alt']}m<br>"
                f"<i>Datos PVsyst disponibles</i>",
                max_width=200
            ),
            tooltip=f"{nombre} (datos disponibles)",
        ).add_to(m)

        # Radio de cobertura
        folium.Circle(
            [info['lat'], info['lon']],
            radius=info['radio_km'] * 1000,
            color='#2E7D32',
            fill=True,
            fillOpacity=0.05,
            weight=1,
            dash_array='5,5',
        ).add_to(m)

        # Polígono de parcela si existe
        if 'parcela_polygon' in info:
            coords = [[p[1], p[0]] for p in info['parcela_polygon']]  # lat, lon
            folium.Polygon(
                locations=coords,
                color='#FFC107',
                fill=True,
                fillColor='#FFC107',
                fillOpacity=0.25,
                weight=2,
                popup=f"Parcela — {nombre}",
                tooltip="Parcela",
            ).add_to(m)

    # Pin del usuario si ya hizo click
    if st.session_state.pin_lat is not None:
        folium.Marker(
            [st.session_state.pin_lat, st.session_state.pin_lon],
            icon=folium.Icon(color='red', icon='crosshairs', prefix='fa'),
            tooltip="Punto seleccionado",
        ).add_to(m)

    folium.LayerControl().add_to(m)

    map_data = st_folium(
        m,
        width=None,
        height=550,
        returned_objects=["last_clicked"],
    )

    # Procesar click del mapa
    if map_data and map_data.get("last_clicked"):
        clicked_lat = map_data["last_clicked"]["lat"]
        clicked_lon = map_data["last_clicked"]["lng"]
        # Solo actualizar si es un click nuevo (diferente al guardado)
        prev_lat = st.session_state.get('pin_lat')
        prev_lon = st.session_state.get('pin_lon')
        is_new = (prev_lat is None or
                  abs(clicked_lat - (prev_lat or 0)) > 0.001 or
                  abs(clicked_lon - (prev_lon or 0)) > 0.001)
        if is_new and -56 <= clicked_lat <= -18 and -76 <= clicked_lon <= -66:
            st.session_state.pin_lat = clicked_lat
            st.session_state.pin_lon = clicked_lon
            st.session_state.comuna_match = find_nearest_comuna(clicked_lat, clicked_lon)
            st.session_state.processed = False
            st.rerun()

    # Indicación de comunas disponibles (solo si no hay selección)
    if st.session_state.comuna_match is None:
        st.markdown('<div class="hint-box">'
                    '<b>Comunas con datos:</b> '
                    + ' '.join(f'<span class="comuna-chip">{n}</span>' for n in COMUNAS_DB.keys())
                    + '</div>',
                    unsafe_allow_html=True)


# ── Panel derecho ────────────────────────────────────────────────────────────
with col_panel:

    # Selector directo de comuna (siempre visible)
    st.markdown("#### Seleccionar localidad")
    opciones = ["-- Haga click en el mapa --"] + list(COMUNAS_DB.keys())

    # Determinar índice actual
    current_idx = 0
    if st.session_state.comuna_match is not None:
        nombre_actual = st.session_state.comuna_match[0]
        if nombre_actual in opciones:
            current_idx = opciones.index(nombre_actual)

    seleccion = st.selectbox(
        "O seleccione directamente:",
        options=opciones,
        index=current_idx,
        key="comuna_selector",
        label_visibility="collapsed",
    )

    # Si se selecciona del dropdown, actualizar estado
    if seleccion != "-- Haga click en el mapa --":
        info_sel = COMUNAS_DB[seleccion]
        current_match = st.session_state.get('comuna_match')
        if current_match is None or current_match[0] != seleccion:
            st.session_state.pin_lat = info_sel['lat']
            st.session_state.pin_lon = info_sel['lon']
            st.session_state.comuna_match = (seleccion, info_sel, 0.0)
            st.session_state.processed = False
            st.rerun()

    st.markdown("---")

    # Estado 1: Sin selección
    if st.session_state.pin_lat is None or (seleccion == "-- Haga click en el mapa --" and st.session_state.comuna_match is None):
        st.markdown("""
        <div class="site-card">
            <h3>Seleccione un punto</h3>
            <div class="detail">
                Haga click en el mapa o elija una localidad del menú
                desplegable para generar el informe agroclimático.<br><br>
                Los <b>círculos verdes</b> indican comunas con datos PVsyst.
            </div>
        </div>
        """, unsafe_allow_html=True)

        for nombre, info in COMUNAS_DB.items():
            st.markdown(f"""
            **{nombre}** — Región {info['region']}
            - Alt: {info['alt']} m | Precip: {sum(info['precip']):.0f} mm/año
            """)

    # Estado 2: Click fuera de zona
    elif st.session_state.comuna_match is None:
        st.markdown(f"""
        <div class="no-data-box">
            <h2>Sin datos disponibles</h2>
            <p>
                No hay datos PVsyst para esta ubicación<br>
                <b>({st.session_state.pin_lat:.4f}°, {st.session_state.pin_lon:.4f}°)</b>
            </p>
            <p style="margin-top:1rem; font-size:0.85rem; color:#795548;">
                Seleccione una comuna del menú o haga click dentro del radio verde.
            </p>
        </div>
        """, unsafe_allow_html=True)

    # Estado 3: Match con comuna → mostrar datos del sitio + opciones
    else:
        nombre_match, info_match, dist_km = st.session_state.comuna_match

        st.markdown(f"""
        <div class="site-card">
            <h3>{nombre_match}</h3>
            <div class="detail">
                <b>Región:</b> {info_match['region']}<br>
                <b>Punto seleccionado:</b> {st.session_state.pin_lat:.4f}°, {st.session_state.pin_lon:.4f}°<br>
                <b>Distancia al centro de datos:</b> {dist_km:.1f} km<br>
                <b>Altitud referencia:</b> {info_match['alt']} m<br>
                <b>Precipitación anual:</b> {sum(info_match['precip']):.0f} mm<br>
                <b>Fuente meteorológica:</b> {info_match['fuente_meteo']}
            </div>
        </div>
        """, unsafe_allow_html=True)

        # Cultivos + botón principal arriba
        especies_disponibles = list(ESPECIES.keys())
        especies_labels = {k: ESPECIES[k]['nombre'] for k in especies_disponibles}
        especies_sel = st.multiselect(
            "Cultivos a evaluar",
            options=especies_disponibles,
            default=['VID', 'CEREZO', 'NOGAL', 'PALTO'],
            format_func=lambda x: especies_labels[x],
            key="cultivos_sel"
        )

        generar = st.button(
            "Generar Informe",
            type="primary",
            use_container_width=True,
            key="btn_generar"
        )

        # Opciones avanzadas debajo
        with st.expander("Configuración avanzada"):
            nombre_predio = st.text_input("Nombre del predio (opcional)", value="",
                                          key="predio_input")

            st.markdown("**Precipitación mensual (mm)**")
            meses_n = ['Ene', 'Feb', 'Mar', 'Abr', 'May', 'Jun',
                       'Jul', 'Ago', 'Sep', 'Oct', 'Nov', 'Dic']
            precip_inputs = []
            cols_p = st.columns(4)
            for i in range(12):
                with cols_p[i % 4]:
                    val = st.number_input(
                        meses_n[i], value=float(info_match['precip'][i]),
                        min_value=0.0, max_value=500.0, step=1.0,
                        key=f"pp_{i}"
                    )
                    precip_inputs.append(val)

        # Defaults si el expander no se abrió (widgets siempre ejecutan en Streamlit,
        # pero por seguridad definimos fallbacks)
        if 'precip_inputs' not in dir() or not precip_inputs:
            precip_inputs = list(info_match['precip'])
        if 'nombre_predio' not in dir():
            nombre_predio = ""

        if generar:
            pvsyst_path = find_pvsyst_file(nombre_match)
            if pvsyst_path is None:
                st.error(f"No se encontró el archivo PVsyst para {nombre_match}. "
                         "Verifique que el CSV esté en la carpeta Downloads.")
            else:
                with st.spinner("Procesando 8760 horas de datos meteorológicos..."):
                    try:
                        df = parse_pvsyst_csv(pvsyst_path)

                        monthly_df = calc_monthly_climate(
                            df, localidad=nombre_match,
                            precip_custom=precip_inputs,
                            hr_custom=info_match['hr'],
                            lat=info_match['lat']
                        )
                        dc_df = calc_dias_calidos_table(df)
                        hel_df = calc_heladas_intensidad(df)
                        winkler = calc_winkler(df)
                        fototermico, _, _, _ = calc_indice_fototermico(df)

                        bio_tables = {}
                        analisis_texts = {}
                        for esp_key in especies_sel:
                            bio_df = calc_bioclimatic_table(df, esp_key, monthly_df, precip_inputs)
                            bio_tables[esp_key] = bio_df
                            analisis_texts[esp_key] = generar_analisis_textual(esp_key, bio_df)

                        st.session_state.update({
                            'monthly_df': monthly_df,
                            'dc_df': dc_df,
                            'hel_df': hel_df,
                            'bio_tables': bio_tables,
                            'analisis_texts': analisis_texts,
                            'winkler': winkler,
                            'fototermico': fototermico,
                            'precip_inputs': precip_inputs,
                            'nombre_predio': nombre_predio,
                            'processed': True,
                        })
                        st.rerun()

                    except Exception as e:
                        st.error(f"Error: {str(e)}")
                        st.exception(e)


# ── Resultados (debajo del mapa, full width) ─────────────────────────────────
if st.session_state.get('processed') and st.session_state.comuna_match is not None:
    nombre_match, info_match, dist_km = st.session_state.comuna_match

    monthly_df = st.session_state['monthly_df']
    dc_df = st.session_state['dc_df']
    hel_df = st.session_state['hel_df']
    bio_tables = st.session_state['bio_tables']
    analisis_texts = st.session_state['analisis_texts']
    winkler = st.session_state['winkler']
    fototermico = st.session_state['fototermico']
    precip_inputs = st.session_state['precip_inputs']
    nombre_predio = st.session_state.get('nombre_predio', '')

    annual = monthly_df[monthly_df['MES'] == 'ANUAL'].iloc[0]
    data_m = monthly_df[monthly_df['MES'] != 'ANUAL']
    meses_lbl = ['E', 'F', 'M', 'A', 'M', 'J', 'J', 'A', 'S', 'O', 'N', 'D']

    st.markdown("---")
    st.markdown(f"## Informe Agroclimático — {nombre_match}")
    if nombre_predio:
        st.caption(f"Predio: {nombre_predio}")

    # ── Métricas ─────────────────────────────────────────────────────────
    mc1, mc2, mc3, mc4, mc5, mc6 = st.columns(6)
    with mc1:
        render_metric("T.Máx Enero", f"{data_m.iloc[0]['T.MAX']}°C")
    with mc2:
        render_metric("T.Mín Julio", f"{data_m.iloc[6]['T.MIN']}°C")
    with mc3:
        render_metric("Días-Grado", f"{int(annual['DIAS GRADO'])}", "base 10°C")
    with mc4:
        render_metric("Horas Frío", f"{int(annual['HRS.FRIO'])}", "<7°C anual")
    with mc5:
        render_metric("Winkler", f"{int(winkler)}", "días-grado")
    with mc6:
        render_metric("Fototérmico", f"{int(fototermico)}", "índice")

    st.markdown("---")

    # ── Tabs ─────────────────────────────────────────────────────────────
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt

    tabs = st.tabs([
        "Mensual",
        "Temperaturas",
        "Hídrico",
        "Heladas",
        "DG & Frío",
        "Cultivos",
        "NDVI",
        "Plagas",
        "Costos",
        "Drones",
        "Solar",
        "Descargas",
    ])

    # ── Tab 1: Tabla mensual ─────────────────────────────────────────────
    with tabs[0]:
        st.markdown("#### Resumen de Valores Mensuales para Algunos Parámetros Climáticos")
        display_cols = ['MES', 'T.MAX', 'T.MIN', 'T.MED', 'DIAS GRADO',
                        'DG.ACUM', 'D-cálidos', 'HRS.FRIO', 'HRS.FRES',
                        'R.SOLAR', 'H.RELAT', 'PRECIPIT', 'EVAP.POT',
                        'DEF.HIDR', 'EXC.HIDR', 'IND.HUMED', 'HELADAS']
        st.dataframe(
            monthly_df[display_cols].style.format({
                'T.MAX': '{:.1f}', 'T.MIN': '{:.1f}', 'T.MED': '{:.1f}',
                'DIAS GRADO': '{:.0f}', 'DG.ACUM': '{:.0f}',
                'R.SOLAR': '{:.0f}', 'PRECIPIT': '{:.1f}',
                'EVAP.POT': '{:.1f}', 'DEF.HIDR': '{:.1f}',
                'EXC.HIDR': '{:.1f}', 'IND.HUMED': '{:.2f}',
                'HELADAS': '{:.1f}'
            }).background_gradient(subset=['T.MAX'], cmap='YlOrRd')
            .background_gradient(subset=['T.MIN'], cmap='YlGnBu')
            .background_gradient(subset=['PRECIPIT'], cmap='Blues'),
            use_container_width=True,
            height=520
        )
        st.caption(
            "Días-grado acumulados a partir de octubre. Horas frío acumuladas desde mayo. "
            "Base: T efectivas 10°C, frío 7°C. Heladas: Tmín < 0°C."
        )

    # ── Tab 2: Temperaturas ──────────────────────────────────────────────
    with tabs[1]:
        st.markdown("#### Perfil Térmico Mensual")
        fig, ax = plt.subplots(figsize=(10, 4.5))
        x = range(12)
        ax.fill_between(x, data_m['T.MIN'].values, data_m['T.MAX'].values,
                        alpha=0.15, color='#2E7D32')
        ax.plot(x, data_m['T.MAX'].values, 'o-', color='#D32F2F', lw=2.5,
                label='T.Máxima', ms=6)
        ax.plot(x, data_m['T.MIN'].values, 's-', color='#1565C0', lw=2.5,
                label='T.Mínima', ms=6)
        ax.plot(x, data_m['T.MED'].values, '^--', color='#2E7D32', lw=1.5,
                label='T.Media', ms=5)
        ax.axhline(y=0, color='lightblue', lw=1, ls=':', alpha=0.7)
        ax.axhline(y=10, color='orange', lw=0.8, ls='--', alpha=0.5, label='Base DG (10°C)')
        ax.set_xticks(x)
        ax.set_xticklabels(meses_lbl)
        ax.set_ylabel('Temperatura (°C)')
        ax.legend(fontsize=9)
        ax.grid(True, alpha=0.3)
        ax.set_title(f'Perfil Térmico — {nombre_match}', fontsize=13,
                     fontweight='bold', color='#2E7D32')
        st.pyplot(fig)
        plt.close()

        st.markdown("#### Radiación Solar Mensual")
        fig2, ax2 = plt.subplots(figsize=(10, 3.5))
        bars = ax2.bar(x, data_m['R.SOLAR'].values, color='#FFD54F',
                       edgecolor='#F57F17', linewidth=0.8)
        ax2.set_xticks(x)
        ax2.set_xticklabels(meses_lbl)
        ax2.set_ylabel('cal/cm² día')
        ax2.grid(True, alpha=0.3, axis='y')
        ax2.set_title(f'Radiación Solar — {nombre_match}', fontsize=13,
                      fontweight='bold', color='#2E7D32')
        for bar, val in zip(bars, data_m['R.SOLAR'].values):
            ax2.text(bar.get_x() + bar.get_width()/2., bar.get_height() + 5,
                     f'{int(val)}', ha='center', va='bottom', fontsize=8)
        st.pyplot(fig2)
        plt.close()

    # ── Tab 3: Balance hídrico ───────────────────────────────────────────
    with tabs[2]:
        st.markdown("#### Balance Hídrico Mensual")
        fig, ax = plt.subplots(figsize=(10, 5))
        x = np.arange(12)
        width = 0.35
        ax.bar(x - width/2, data_m['EVAP.POT'].values, width, label='ETP (mm)',
               color='#FF8A65', edgecolor='#E64A19', linewidth=0.5)
        ax.bar(x + width/2, data_m['PRECIPIT'].values, width, label='Precipitación (mm)',
               color='#4FC3F7', edgecolor='#0277BD', linewidth=0.5)
        ax.set_xticks(x)
        ax.set_xticklabels(meses_lbl)
        ax.set_ylabel('mm/mes')
        ax.legend(fontsize=10)
        ax.grid(True, alpha=0.3, axis='y')
        ax.set_title(f'Balance Hídrico — {nombre_match}', fontsize=13,
                     fontweight='bold', color='#2E7D32')
        st.pyplot(fig)
        plt.close()

        st.markdown("#### Índice de Humedad Mensual (PP/ETP)")
        fig3, ax3 = plt.subplots(figsize=(10, 3.5))
        vals = data_m['IND.HUMED'].values
        colors_ih = ['#4FC3F7' if v >= 1 else '#FF8A65' if v < 0.5 else '#FFD54F' for v in vals]
        ax3.bar(x, vals, color=colors_ih, edgecolor='gray', linewidth=0.5)
        ax3.axhline(y=1.0, color='blue', ls='--', lw=1, alpha=0.5, label='PP = ETP')
        ax3.axhline(y=0.5, color='red', ls='--', lw=1, alpha=0.5, label='Necesita riego')
        ax3.set_xticks(x)
        ax3.set_xticklabels(meses_lbl)
        ax3.set_ylabel('PP / ETP')
        ax3.legend(fontsize=9)
        ax3.grid(True, alpha=0.3, axis='y')
        ax3.set_title(f'Índice de Humedad — {nombre_match}', fontsize=13,
                      fontweight='bold', color='#2E7D32')
        st.pyplot(fig3)
        plt.close()

    # ── Tab 4: Días cálidos y heladas ────────────────────────────────────
    with tabs[3]:
        col_dc, col_hel = st.columns(2)
        with col_dc:
            st.markdown("#### Días Cálidos por Umbral")
            st.dataframe(dc_df, use_container_width=True, hide_index=True)
        with col_hel:
            st.markdown("#### Heladas por Intensidad")
            st.dataframe(hel_df, use_container_width=True, hide_index=True)

        st.markdown("#### Distribución de Heladas por Mes")
        fig, ax = plt.subplots(figsize=(10, 4))
        hel_0 = hel_df[hel_df['UMBRAL'] == '0 °C']
        if len(hel_0) > 0:
            month_cols = ['ENE', 'FEB', 'MAR', 'ABR', 'MAY', 'JUN',
                          'JUL', 'AGO', 'SEP', 'OCT', 'NOV', 'DIC']
            vals = [hel_0.iloc[0].get(m, 0) for m in month_cols]
            ax.bar(range(12), vals, color='#90CAF9', edgecolor='#1565C0')
            ax.set_xticks(range(12))
            ax.set_xticklabels(meses_lbl)
            ax.set_ylabel('Días con helada')
            ax.set_title(f'Heladas (T<0°C) por mes — {nombre_match}', fontsize=12,
                         fontweight='bold', color='#2E7D32')
            ax.grid(True, alpha=0.3, axis='y')
        st.pyplot(fig)
        plt.close()

    # ── Tab 5: Días-grado y frío ─────────────────────────────────────────
    with tabs[4]:
        col_dg, col_hf = st.columns(2)
        with col_dg:
            st.markdown("#### Días-Grado Acumulados (Oct→Sep)")
            fig, ax = plt.subplots(figsize=(6, 4))
            order = [9, 10, 11, 0, 1, 2, 3, 4, 5, 6, 7, 8]
            labels_dg = ['O', 'N', 'D', 'E', 'F', 'M', 'A', 'M', 'J', 'J', 'A', 'S']
            dg_vals = [data_m.iloc[i]['DG.ACUM'] for i in order]
            ax.fill_between(range(12), dg_vals, alpha=0.3, color='#FF6F00')
            ax.plot(range(12), dg_vals, 'o-', color='#E65100', lw=2.5, ms=6)
            ax.set_xticks(range(12))
            ax.set_xticklabels(labels_dg)
            ax.set_ylabel('DG Acumulados')
            ax.grid(True, alpha=0.3)
            ax.set_title(f'Winkler: {int(winkler)} DG', fontsize=11, fontweight='bold')
            st.pyplot(fig)
            plt.close()

        with col_hf:
            st.markdown("#### Horas de Frío y Frescor")
            fig, ax = plt.subplots(figsize=(6, 4))
            ax.bar(range(12), data_m['HRS.FRES'].values, color='#90CAF9',
                   alpha=0.6, label='Frescor (<10°C)')
            ax.bar(range(12), data_m['HRS.FRIO'].values, color='#42A5F5',
                   label='Frío (<7°C)')
            ax.set_xticks(range(12))
            ax.set_xticklabels(meses_lbl)
            ax.set_ylabel('Horas')
            ax.legend(fontsize=8)
            ax.grid(True, alpha=0.3, axis='y')
            ax.set_title(f'Total: {int(annual["HRS.FRIO"])} hrs frío', fontsize=11,
                         fontweight='bold')
            st.pyplot(fig)
            plt.close()

    # ── Tab 6: Aptitud cultivos ──────────────────────────────────────────
    with tabs[5]:
        st.markdown("#### Índices Bioclimáticos por Especie")
        for esp_key in bio_tables:
            nombre_esp = ESPECIES[esp_key]['nombre']
            with st.expander(f"**{nombre_esp}**", expanded=True):
                bio_df = bio_tables[esp_key]
                st.dataframe(bio_df, use_container_width=True, hide_index=True)

                if esp_key in analisis_texts:
                    st.markdown("**Diagnóstico:**")
                    for line in analisis_texts[esp_key].split('\n'):
                        if line.strip():
                            if 'bajo riesgo' in line.lower() or 'adecuad' in line.lower():
                                st.markdown(f"- :green[{line.strip()}]")
                            elif 'riesgo moderado' in line.lower() or 'deficiencia' in line.lower():
                                st.markdown(f"- :orange[{line.strip()}]")
                            elif 'riesgo alto' in line.lower() or 'severo' in line.lower():
                                st.markdown(f"- :red[{line.strip()}]")
                            else:
                                st.markdown(f"- {line.strip()}")

    # ── Tab 7: NDVI Satelital (Google Earth Engine) ────────────────────
    with tabs[6]:
        st.markdown("#### Índice de Vegetación NDVI — Google Earth Engine")
        st.caption(
            "Análisis de vegetación mediante Sentinel-2 (banda NIR/Roja). "
            "NDVI varía de 0 (suelo desnudo) a 1 (vegetación densa)."
        )

        if not GEE_AVAILABLE:
            st.warning("Google Earth Engine no está configurado en este servidor. "
                       "El análisis NDVI está disponible solo en la versión local.")
        else:

            pin_lat = st.session_state.pin_lat or info_match['lat']
            pin_lon = st.session_state.pin_lon or info_match['lon']

            col_ndvi_cfg, col_ndvi_res = st.columns([1, 2])
            with col_ndvi_cfg:
                ndvi_start = st.number_input("Año inicio NDVI", 2017, 2025, 2019, key="ndvi_y0")
                ndvi_end = st.number_input("Año fin NDVI", 2017, 2025, 2025, key="ndvi_y1")
                ndvi_radius = st.slider("Radio de análisis (m)", 100, 2000, 500, 100, key="ndvi_r")
                btn_ndvi = st.button("Consultar NDVI", type="primary", key="btn_ndvi")

            if btn_ndvi:
                with st.spinner("Consultando Google Earth Engine (Sentinel-2)..."):
                    try:
                        ts_ndvi = get_ndvi_timeseries(
                            pin_lat, pin_lon, ndvi_radius, int(ndvi_start), int(ndvi_end)
                        )
                        annual_ndvi = get_ndvi_annual_summary(
                            pin_lat, pin_lon, ndvi_radius, int(ndvi_start), int(ndvi_end)
                        )
                        st.session_state['ndvi_ts'] = ts_ndvi
                        st.session_state['ndvi_annual'] = annual_ndvi
                    except Exception as exc:
                        st.error(f"Error al consultar GEE: {exc}")

            # Mostrar resultados si existen
            if 'ndvi_ts' in st.session_state:
                ts_ndvi = st.session_state['ndvi_ts']
                annual_ndvi = st.session_state['ndvi_annual']

                with col_ndvi_res:
                    valid = ts_ndvi.dropna(subset=['ndvi_mean'])
                    if len(valid) > 0:
                        latest = valid.iloc[-1]
                        st.markdown(
                            f"**Último NDVI:** {latest['ndvi_mean']:.3f} "
                            f"({classify_land_use(latest['ndvi_mean'])}) — "
                            f"{latest['date'].strftime('%b %Y')}"
                        )

                # Gráfico de serie temporal
                st.markdown("##### Serie Temporal NDVI mensual")
                valid = ts_ndvi.dropna(subset=['ndvi_mean'])
                if len(valid) > 0:
                    fig_ndvi, ax_ndvi = plt.subplots(figsize=(12, 4))
                    ax_ndvi.fill_between(valid['date'], valid['ndvi_min'], valid['ndvi_max'],
                                         alpha=0.15, color='#2E7D32', label='Rango min-max')
                    ax_ndvi.plot(valid['date'], valid['ndvi_mean'], '-o', color='#2E7D32',
                                 lw=2, ms=3, label='NDVI medio')
                    ax_ndvi.axhline(0.35, ls='--', color='#FF9800', lw=1, alpha=0.6, label='Umbral cultivo')
                    ax_ndvi.set_ylabel('NDVI')
                    ax_ndvi.set_ylim(0, 1)
                    ax_ndvi.legend(fontsize=8)
                    ax_ndvi.grid(True, alpha=0.3)
                    ax_ndvi.set_title(f'NDVI — {nombre_match} ({pin_lat:.4f}, {pin_lon:.4f})',
                                      fontsize=12, fontweight='bold', color='#2E7D32')
                    st.pyplot(fig_ndvi)
                    plt.close()
                else:
                    st.warning("No se obtuvieron datos NDVI válidos para el período seleccionado.")

                # Resumen anual
                st.markdown("##### Resumen Anual de Vegetación")
                if annual_ndvi is not None and len(annual_ndvi) > 0:
                    st.dataframe(
                        annual_ndvi.rename(columns={
                            'year': 'Año', 'ndvi_jan': 'NDVI Ene (verano)',
                            'ndvi_jul': 'NDVI Jul (invierno)',
                            'ndvi_annual_mean': 'NDVI Promedio',
                            'vegetation_status': 'Estado vegetación',
                        }),
                        use_container_width=True, hide_index=True,
                    )

                # Mapa NDVI overlay
                st.markdown("##### Mapa NDVI (año más reciente)")
                try:
                    tile_url = get_ndvi_map_url(pin_lat, pin_lon, year=int(ndvi_end))
                    if tile_url:
                        ndvi_map = folium.Map(location=[pin_lat, pin_lon], zoom_start=15, tiles=None)
                        folium.TileLayer(
                            'https://{s}.google.com/vt/lyrs=s&x={x}&y={y}&z={z}',
                            subdomains=['mt0','mt1','mt2','mt3'],
                            name='Satelital', attr='Google'
                        ).add_to(ndvi_map)
                        folium.TileLayer(
                            tiles=tile_url, name='NDVI', attr='GEE Sentinel-2', overlay=True
                        ).add_to(ndvi_map)
                        if 'parcela_polygon' in info_match:
                            coords = [[p[1], p[0]] for p in info_match['parcela_polygon']]
                            folium.Polygon(locations=coords, color='#FFC107', fill=False, weight=2).add_to(ndvi_map)
                        folium.CircleMarker([pin_lat, pin_lon], radius=5, color='red', fill=True).add_to(ndvi_map)
                        folium.LayerControl().add_to(ndvi_map)
                        st_folium(ndvi_map, width=None, height=450, key="ndvi_map", returned_objects=[])
                    else:
                        st.info("No se pudo generar el overlay NDVI. Intente con otro año.")
                except Exception as exc:
                    st.warning(f"Mapa NDVI no disponible: {exc}")

    # ── Tab 8: Fitosanidad ──────────────────────────────────────────────
    with tabs[7]:
        st.markdown("#### Fitosanidad — Enfermedades y Plagas")
        st.caption("Base de datos curada de principales problemas fitosanitarios "
                   "para las especies evaluadas. Fuentes: SAG, INIA, CIREN.")

        for esp_key in bio_tables:
            if esp_key not in PLAGAS_ENFERMEDADES:
                continue
            nombre_esp = ESPECIES[esp_key]['nombre']
            pe = PLAGAS_ENFERMEDADES[esp_key]

            with st.expander(f"**{nombre_esp}**", expanded=False):
                st.markdown("##### Enfermedades")
                for enf in pe.get('enfermedades', []):
                    st.markdown(f"**{enf['nombre']}** — *{enf['agente']}*")
                    st.markdown(f"- Síntomas: {enf['sintomas']}")
                    st.markdown(f"- Época de riesgo: {enf['epoca_riesgo']}")
                    st.markdown(f"- Control: {enf['control']}")
                    st.markdown("---")

                st.markdown("##### Plagas")
                for pla in pe.get('plagas', []):
                    st.markdown(f"**{pla['nombre']}** — *{pla['tipo']}*")
                    st.markdown(f"- Daño: {pla['daño']}")
                    st.markdown(f"- Época: {pla['epoca']}")
                    st.markdown(f"- Control: {pla['control']}")
                    st.markdown("---")

                # Calendario fitosanitario si existe
                if esp_key in CALENDARIO_FITOSANITARIO:
                    st.markdown("##### Calendario Fitosanitario Mensual")
                    cal = CALENDARIO_FITOSANITARIO[esp_key]
                    for mes_num in range(1, 13):
                        if mes_num in cal and cal[mes_num]:
                            st.markdown(f"- {cal[mes_num]}")

    # ── Tab 9: Costos & Fertilización ────────────────────────────────────
    with tabs[8]:
        st.markdown("#### Costos por Hectárea y Plan de Fertilización")
        st.caption("Valores referenciales en USD para zona central de Chile. "
                   "Fuentes: ODEPA, FIA, consultoras agrícolas.")

        # Tabla comparativa de costos
        st.markdown("##### Comparativa de Costos e Ingresos")
        cost_rows = []
        for esp_key in bio_tables:
            if esp_key not in COSTOS_HECTAREA:
                continue
            c = COSTOS_HECTAREA[esp_key]
            cost_rows.append({
                'Especie': ESPECIES[esp_key]['nombre'],
                'Establecimiento (USD/ha)': f"${c['establecimiento_usd_ha']:,}",
                'Mantención anual (USD/ha)': f"${c['mantencion_anual_usd_ha']:,}",
                'Rendimiento (ton/ha)': c['rendimiento_ton_ha'],
                'Precio FOB (USD/kg)': f"${c['precio_fob_usd_kg']:.2f}",
                'Ingreso bruto (USD/ha)': f"${c['ingreso_bruto_usd_ha']:,}",
                'Margen est. (%)': c['margen_estimado_pct'],
                'Años hasta prod.': c['años_hasta_produccion'],
            })
        if cost_rows:
            st.dataframe(pd.DataFrame(cost_rows), use_container_width=True, hide_index=True)

        # Detalle por especie con fertilización
        for esp_key in bio_tables:
            if esp_key not in COSTOS_HECTAREA:
                continue
            nombre_esp = ESPECIES[esp_key]['nombre']
            c = COSTOS_HECTAREA[esp_key]

            with st.expander(f"**{nombre_esp}** — Detalle", expanded=False):
                col_c1, col_c2 = st.columns(2)
                with col_c1:
                    st.markdown("**Inversión y retorno**")
                    st.markdown(f"- Establecimiento: **${c['establecimiento_usd_ha']:,}/ha**")
                    st.markdown(f"- Mantención anual: **${c['mantencion_anual_usd_ha']:,}/ha**")
                    st.markdown(f"- Vida útil: {c['vida_util_años']} años")
                    st.markdown(f"- Años hasta producción: {c['años_hasta_produccion']}")
                    margen_usd = int(c['ingreso_bruto_usd_ha'] * c['margen_estimado_pct'] / 100)
                    st.markdown(f"- Margen neto estimado: **${margen_usd:,}/ha/año**")
                with col_c2:
                    st.markdown("**Mercado**")
                    st.markdown(f"- Rendimiento: {c['rendimiento_ton_ha']} ton/ha")
                    st.markdown(f"- Precio FOB: ${c['precio_fob_usd_kg']:.2f}/kg")
                    st.markdown(f"- Ingreso bruto: **${c['ingreso_bruto_usd_ha']:,}/ha**")
                st.markdown(f"*{c['notas']}*")

                # Fertilización
                if esp_key in FERTILIZACION:
                    st.markdown("---")
                    st.markdown("**Plan de Fertilización**")
                    fert = FERTILIZACION[esp_key]
                    if 'npk_anual' in fert:
                        npk = fert['npk_anual']
                        st.markdown(f"Dosis anual (kg/ha): **N** {npk.get('N','-')} · "
                                    f"**P2O5** {npk.get('P2O5','-')} · **K2O** {npk.get('K2O','-')}")
                    if 'microelementos' in fert:
                        st.markdown(f"Microelementos clave: {', '.join(fert['microelementos'])}")
                    if 'costo_estimado_usd_ha' in fert:
                        st.markdown(f"Costo estimado fertilización: **${fert['costo_estimado_usd_ha']:,}/ha/año**")
                    if 'programa_mensual' in fert:
                        st.markdown("**Programa mensual:**")
                        for mes_info in fert['programa_mensual']:
                            st.markdown(f"  - {mes_info}")

    # ── Tab 10: Drones ──────────────────────────────────────────────────
    with tabs[9]:
        st.markdown("#### Tecnología de Drones en Fruticultura")
        st.caption("Información sobre aplicaciones, equipos y normativa para drones agrícolas en Chile.")

        st.markdown("##### Aplicaciones en Agricultura")
        for app in DRONES_INFO['aplicaciones']:
            with st.expander(f"**{app['nombre']}** — ~USD {app['costo_referencial_usd_ha']}/ha"):
                st.markdown(app['descripcion'])
                st.markdown(f"**Beneficio:** {app['beneficio']}")

        st.markdown("---")
        col_v, col_l = st.columns(2)
        with col_v:
            st.markdown("##### Ventajas")
            for v in DRONES_INFO['ventajas']:
                st.markdown(f"- {v}")
        with col_l:
            st.markdown("##### Limitaciones")
            for l in DRONES_INFO['limitaciones']:
                st.markdown(f"- {l}")

        st.markdown("---")
        st.markdown("##### Equipos Recomendados")
        for eq in DRONES_INFO['equipos_recomendados']:
            with st.expander(f"**{eq['modelo']}** — {eq['uso']}"):
                st.markdown(f"- **Capacidad:** {eq['capacidad']}")
                st.markdown(f"- **Precio referencial:** {eq['precio_referencial']}")
                st.markdown(f"- **Características:** {eq['caracteristicas']}")

        st.markdown("---")
        st.markdown("##### Normativa en Chile (DGAC / SAG)")
        st.markdown(DRONES_INFO['normativa_chile'])

    # ── Tab 11: Energía Solar ────────────────────────────────────────────
    with tabs[10]:
        st.markdown("#### Disponibilidad de Energía Solar")
        st.caption("Potencial fotovoltaico estimado a partir de datos de irradiación GHI (PVsyst / Meteonorm 8.2).")

        # Calcular energía solar desde datos mensuales
        ghi_monthly = data_m['R.SOLAR'].values  # cal/cm2/día
        # Convertir cal/cm2/día → kWh/m2/día (1 cal/cm2 = 0.01163 kWh/m2)
        ghi_kwh = ghi_monthly * 0.01163
        ghi_annual = sum(ghi_kwh * [31,28,31,30,31,30,31,31,30,31,30,31])

        col_e1, col_e2, col_e3 = st.columns(3)
        with col_e1:
            render_metric("GHI Anual", f"{ghi_annual:.0f}", "kWh/m2/año")
        with col_e2:
            # Factor de planta estimado (GHI / 8760 * eficiencia)
            peak_sun_hours = ghi_annual / 365
            render_metric("HSP Promedio", f"{peak_sun_hours:.1f}", "kWh/m2/día")
        with col_e3:
            # Producción estimada 1 kWp: GHI * 0.80 (PR)
            prod_kwp = ghi_annual * 0.80
            render_metric("Producción 1 kWp", f"{prod_kwp:.0f}", "kWh/año")

        st.markdown("##### Irradiación Solar Mensual (kWh/m2/día)")
        fig_sol, ax_sol = plt.subplots(figsize=(10, 4))
        bars_s = ax_sol.bar(range(12), ghi_kwh, color='#FFD54F', edgecolor='#F57F17', lw=0.8)
        ax_sol.set_xticks(range(12))
        ax_sol.set_xticklabels(meses_lbl)
        ax_sol.set_ylabel('kWh/m2/día')
        ax_sol.grid(True, alpha=0.3, axis='y')
        ax_sol.set_title(f'Irradiación Global Horizontal — {nombre_match}',
                         fontsize=12, fontweight='bold', color='#2E7D32')
        for bar, val in zip(bars_s, ghi_kwh):
            ax_sol.text(bar.get_x() + bar.get_width()/2., bar.get_height() + 0.05,
                        f'{val:.1f}', ha='center', va='bottom', fontsize=8)
        st.pyplot(fig_sol)
        plt.close()

        st.markdown("##### Estimaciones para Bombeo Solar y Riego")
        st.markdown(f"""
        | Parámetro | Valor |
        |---|---|
        | **GHI anual** | {ghi_annual:.0f} kWh/m2/año |
        | **Horas sol pico promedio** | {peak_sun_hours:.1f} h/día |
        | **Producción 1 kWp (PR=80%)** | {prod_kwp:.0f} kWh/año |
        | **Sistema 5 kWp** | {prod_kwp*5:.0f} kWh/año |
        | **Sistema 10 kWp** | {prod_kwp*10:.0f} kWh/año |
        | **Bomba 1 HP solar** | ~{peak_sun_hours*0.75*365:.0f} kWh/año (~{peak_sun_hours*0.75*365/0.746:.0f} m3/año a 30m) |
        """)

        st.info(
            "**Nota:** Los valores de producción asumen un Performance Ratio (PR) del 80%, "
            "típico para instalaciones bien dimensionadas en Chile central. "
            "El recurso solar en esta ubicación es "
            + ("**excelente**" if ghi_annual > 1800 else "**bueno**" if ghi_annual > 1500 else "**moderado**")
            + f" ({ghi_annual:.0f} kWh/m2/año)."
        )

    # ── Tab 12: Descargas ────────────────────────────────────────────────
    with tabs[11]:
        st.markdown("#### Descargar Informe y Datos")
        col_pdf, col_csv = st.columns(2)

        with col_pdf:
            st.markdown("##### Informe PDF")
            st.caption("PDF completo estilo Santibáñez con tablas, gráficos y diagnóstico por cultivo.")

            if st.button("Generar PDF", type="primary", key="btn_pdf"):
                with st.spinner("Generando informe PDF..."):
                    try:
                        pdf_bytes = generate_pdf(
                            localidad=nombre_match,
                            lat=info_match['lat'], lon=info_match['lon'],
                            alt=info_match['alt'],
                            monthly_df=monthly_df,
                            dc_df=dc_df,
                            hel_df=hel_df,
                            bio_tables=bio_tables,
                            analisis_texts=analisis_texts,
                            winkler=winkler,
                            fototermico=fototermico,
                            nombre_predio=nombre_predio
                        )
                        st.download_button(
                            label="Descargar PDF",
                            data=pdf_bytes,
                            file_name=f"Informe_Agroclimatico_{nombre_match}.pdf",
                            mime="application/pdf",
                            type="primary",
                            key="dl_pdf"
                        )
                        st.success("PDF generado correctamente")
                    except Exception as e:
                        st.error(f"Error: {str(e)}")
                        st.exception(e)

        with col_csv:
            st.markdown("##### Datos CSV")
            csv_monthly = monthly_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                "Tabla Mensual (CSV)", data=csv_monthly,
                file_name=f"tabla_mensual_{nombre_match}.csv",
                mime="text/csv", key="dl_csv_1"
            )
            csv_dc = dc_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                "Días Cálidos (CSV)", data=csv_dc,
                file_name=f"dias_calidos_{nombre_match}.csv",
                mime="text/csv", key="dl_csv_2"
            )
            csv_hel = hel_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                "Heladas (CSV)", data=csv_hel,
                file_name=f"heladas_{nombre_match}.csv",
                mime="text/csv", key="dl_csv_3"
            )
            for i, (esp_key, bio_df) in enumerate(bio_tables.items()):
                nombre_esp = ESPECIES[esp_key]['nombre']
                csv_bio = bio_df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    f"{nombre_esp} (CSV)", data=csv_bio,
                    file_name=f"bioclimatico_{nombre_esp}_{nombre_match}.csv",
                    mime="text/csv", key=f"dl_bio_{i}"
                )


# ── Footer ───────────────────────────────────────────────────────────────────
st.markdown("---")
st.markdown("""
<div style="text-align: center; color: #999; font-size: 0.8rem; padding: 0.5rem 0;">
    Visor Agroclimático v1.0 &nbsp;|&nbsp; Datos: PVsyst / Meteonorm 8.2 (TMY 2010-2019) &nbsp;|&nbsp;
    Metodología: Fernando Santibáñez, Dr. en Bioclimatología &nbsp;|&nbsp;
    Motor: 8760 horas de datos horarios
</div>
""", unsafe_allow_html=True)
