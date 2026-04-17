"""
Informes Premium v4 — Generador de informes agroclimaticos premium.
Usa engine_v4.py + auto_data.py para generar 3 PDFs por comuna.
"""
import streamlit as st
import os
import sys
import tempfile

# Add parent directory to path for imports
_BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _BASE not in sys.path:
    sys.path.insert(0, _BASE)

from auto_data import auto_load_comuna, COORDS_COMUNAS, _SII_COMUNAS

st.set_page_config(
    page_title="Informes Premium | Visor Agroclimatico",
    page_icon="📊",
    layout="wide",
)

# ── Header ──────────────────────────────────────────────────
st.markdown(f"""
<h1 style='color:#2E7D32;'>Informes Agroclimaticos Premium</h1>
<p style='font-size:1.1em;color:#555;'>
Genera 3 informes profesionales (Basico, Avanzado, Predial) para <b>{len(COORDS_COMUNAS)} comunas de Chile</b>.
Motor v4 con scoring SAC/SRC/STI/IAP y datos CIREN, DGA, SEC, CR2MET, ODEPA, SII.
</p>
""", unsafe_allow_html=True)

st.divider()

# ── Selector de comuna (con filtro por región) ──────────────
# Agrupar por region usando _SII_COMUNAS
regiones_map = {}
for k, v in _SII_COMUNAS.items():
    region = v.get("region") or "Otra"
    regiones_map.setdefault(region, []).append(v.get("nombre_oficial", k.title()))

# Comunas sin region (solo en override Maule)
for k in COORDS_COMUNAS:
    if k not in _SII_COMUNAS:
        regiones_map.setdefault("Maule", []).append(k.title())

col_r, col_c, col_info = st.columns([1, 2, 1])

with col_r:
    regiones_ordenadas = sorted(regiones_map.keys())
    region_sel = st.selectbox(
        "Región",
        options=["Todas"] + regiones_ordenadas,
        index=regiones_ordenadas.index("Maule") + 1 if "Maule" in regiones_ordenadas else 0,
    )

with col_c:
    if region_sel == "Todas":
        opciones = sorted({c for lista in regiones_map.values() for c in lista})
    else:
        opciones = sorted(set(regiones_map.get(region_sel, [])))
    default_idx = opciones.index("San Clemente") if "San Clemente" in opciones else 0
    comuna = st.selectbox(
        f"Comuna ({len(opciones)} disponibles)",
        options=opciones,
        index=default_idx,
    )

with col_info:
    # Resolve coords via COORDS_COMUNAS (lowercase key)
    from auto_data import _normalize
    key = _normalize(comuna)
    coords = COORDS_COMUNAS.get(key)
    if coords:
        st.metric("Coord", f"{coords[0]:.2f}S, {abs(coords[1]):.2f}O")
        st.metric("Alt", f"{coords[2]} m")
    # Show quick SII preview
    sii = _SII_COMUNAS.get(key)
    if sii and sii.get("total_predios"):
        st.caption(f"🏞️ **{sii['total_predios']:,} predios rurales** — {sii['pct_agricola']}% agrícolas")

st.divider()

# ── Generar informes ────────────────────────────────────────
if st.button("Generar 3 Informes Premium", type="primary", use_container_width=True):
    with st.spinner(f"Generando informes para {comuna}... (10-20 segundos)"):
        try:
            # Import engine here to avoid slow startup
            from engine_v4 import (
                ranking_sac, calcular_src, calcular_sti, calcular_iap,
                run_qa, get_styles, ReportTemplate,
                generate_basico, generate_avanzado, generate_predial,
                NOMBRES_ESPECIE, NOMBRES_VARIABLES,
            )

            # 1. Load data
            d = auto_load_comuna(comuna)

            # 2. Calculate scores
            sac_ranking = ranking_sac(d)
            src = calcular_src(d)
            sti = calcular_sti(d)
            iap = calcular_iap(sac_ranking, src, sti)

            # 3. QA
            qa = run_qa(d, sac_ranking, src, sti, iap)
            passed = sum(1 for _, p, _ in qa if p)
            total_qa = len(qa)

            # 4. Generate PDFs in temp directory
            styles = get_styles()
            comuna_file = comuna.replace(" ", "").replace("a\u0301","a").replace("e\u0301","e").replace("i\u0301","i").replace("o\u0301","o").replace("u\u0301","u").replace("n\u0303","n")

            tmpdir = tempfile.mkdtemp()
            pdfs = {}

            # Basico
            fname = os.path.join(tmpdir, f"Informe_Basico_{comuna_file}_v4.pdf")
            doc = ReportTemplate(fname, "Informe Agroclimatico")
            doc.build(generate_basico(d, sac_ranking, src, styles))
            with open(fname, "rb") as f:
                pdfs["basico"] = f.read()

            # Avanzado
            fname = os.path.join(tmpdir, f"Informe_Avanzado_{comuna_file}_v4.pdf")
            doc = ReportTemplate(fname, "Informe de Riesgo Climatico y Prediccion")
            doc.build(generate_avanzado(d, sac_ranking, src, styles))
            with open(fname, "rb") as f:
                pdfs["avanzado"] = f.read()

            # Predial
            fname = os.path.join(tmpdir, f"Informe_Predial_{comuna_file}_v4.pdf")
            doc = ReportTemplate(fname, "Informe de Due Diligence Predial")
            doc.build(generate_predial(d, sac_ranking, src, sti, iap, styles))
            with open(fname, "rb") as f:
                pdfs["predial"] = f.read()

            # Store in session state
            st.session_state["v4_pdfs"] = pdfs
            st.session_state["v4_comuna"] = comuna
            st.session_state["v4_sac"] = sac_ranking
            st.session_state["v4_src"] = src
            st.session_state["v4_sti"] = sti
            st.session_state["v4_iap"] = iap
            st.session_state["v4_qa"] = (passed, total_qa)
            st.session_state["v4_data"] = d

            st.success(f"3 informes generados para {comuna} | QA: {passed}/{total_qa} checks passed")

        except Exception as e:
            st.error(f"Error generando informes: {str(e)}")
            import traceback
            st.code(traceback.format_exc())

# ── Mostrar resultados ──────────────────────────────────────
if "v4_pdfs" in st.session_state:
    comuna_gen = st.session_state["v4_comuna"]
    pdfs = st.session_state["v4_pdfs"]
    sac_ranking = st.session_state["v4_sac"]
    src = st.session_state["v4_src"]
    sti = st.session_state["v4_sti"]
    iap = st.session_state["v4_iap"]
    passed, total_qa = st.session_state["v4_qa"]

    st.markdown(f"### Resultados para {comuna_gen}")

    # ── Scoring Summary ────────────────────────────────
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        color = "#2E7D32" if iap["iap"] >= 65 else ("#F9A825" if iap["iap"] >= 40 else "#C62828")
        st.markdown(f"<div style='text-align:center;padding:12px;border:2px solid {color};border-radius:8px;'>"
                    f"<span style='font-size:2em;font-weight:bold;color:{color};'>{iap['iap']}/100</span><br>"
                    f"<span style='font-size:0.9em;'>IAP: {iap['veredicto']}</span></div>", unsafe_allow_html=True)
    with c2:
        st.metric("SAC Top-3", f"{iap['sac_score']}/100")
    with c3:
        st.metric("SRC Riesgo", f"{src['score']}/100", delta=f"{src['clasificacion']}", delta_color="inverse")
    with c4:
        st.metric("STI Territorial", f"{sti['score']}/100")
    with c5:
        st.metric("QA Checks", f"{passed}/{total_qa}")

    st.divider()

    # ── Datos SII ──────────────────────────────────────
    d = st.session_state.get("v4_data", {})
    if d.get("sii_total_predios"):
        st.markdown("#### Contexto Predial SII (2025 S2)")
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Predios rurales", f"{d['sii_total_predios']:,}")
        c2.metric("% Agrícola", f"{d['sii_pct_agricola']}%")
        c3.metric("Avalúo prom. agr.", f"${d['sii_avaluo_prom_agricola_clp']/1_000_000:.1f}M")
        c4.metric("Sup. prom. agr.", f"{d.get('sii_sup_prom_agricola_ha', 0):.1f} ha")
        c5.metric("Valor total agr.", f"{d.get('sii_valor_total_agricola_mmusd', 0):,.0f} MM USD")

        # Predios cercanos (si los hay)
        predios_cerca = d.get("sii_predios_cercanos", [])
        if predios_cerca:
            with st.expander(f"Predios rurales cercanos al punto ({len(predios_cerca)})"):
                cerca_data = []
                for p in predios_cerca:
                    cerca_data.append({
                        "ROL": p["rol"],
                        "Dist (km)": p["dist_km"],
                        "Destino": p["destino"],
                        "Sup (m²)": f"{p['sup_m2']:,.0f}" if p.get("sup_m2") else "--",
                        "Avalúo CLP": f"${p['avaluo']:,.0f}" if p.get("avaluo") else "--",
                        "Dirección": p.get("direccion") or "--",
                    })
                st.dataframe(cerca_data, use_container_width=True, hide_index=True)

        st.divider()

    # ── SAC Ranking Table ──────────────────────────────
    st.markdown("#### Ranking SAC por Especie")
    try:
        from engine_v4 import NOMBRES_ESPECIE
    except ImportError:
        NOMBRES_ESPECIE = {}
    sac_data = []
    for i, s in enumerate(sac_ranking, 1):
        sac_data.append({
            "#": i,
            "Especie": NOMBRES_ESPECIE.get(s["especie"], s["especie"]),
            "Sup. Local (ha)": f"{s.get('sup_local', 0):,.0f}" if s.get("es_local") else "--",
            "SAC": s["score"],
            "Veredicto": s["veredicto"],
            "Tipo": s.get("senal", "Referencia"),
        })
    st.dataframe(sac_data, use_container_width=True, hide_index=True)

    st.divider()

    # ── Download buttons ───────────────────────────────
    st.markdown("#### Descargar Informes PDF")
    c1, c2, c3 = st.columns(3)
    comuna_file = comuna_gen.replace(" ", "")
    with c1:
        st.download_button(
            "Informe Basico",
            data=pdfs["basico"],
            file_name=f"Informe_Basico_{comuna_file}_v4.pdf",
            mime="application/pdf",
            use_container_width=True,
        )
    with c2:
        st.download_button(
            "Informe Avanzado (Riesgo Climatico)",
            data=pdfs["avanzado"],
            file_name=f"Informe_Avanzado_{comuna_file}_v4.pdf",
            mime="application/pdf",
            use_container_width=True,
        )
    with c3:
        st.download_button(
            "Informe Predial (Due Diligence)",
            data=pdfs["predial"],
            file_name=f"Informe_Predial_{comuna_file}_v4.pdf",
            mime="application/pdf",
            use_container_width=True,
        )

    # ── Desglose SRC ───────────────────────────────────
    with st.expander("Desglose Score Riesgo Climatico (SRC)"):
        for comp, info in src["desglose"].items():
            label = comp.replace("_", " ").title()
            val = info["score"]
            col_bar = "#2E7D32" if val < 30 else ("#F9A825" if val < 60 else "#C62828")
            st.markdown(f"**{label}** (peso {info['peso']:.0%}): {val}/100 → contribucion {val*info['peso']:.1f}")
            st.progress(val / 100)

    # ── Desglose STI ───────────────────────────────────
    with st.expander("Desglose Score Territorial (STI)"):
        for comp, info in sti["desglose"].items():
            label = comp.replace("_", " ").title()
            val = info["score"]
            st.markdown(f"**{label}** (peso {info['peso']:.0%}): {val}/100 → contribucion {val*info['peso']:.1f}")
            st.progress(val / 100)

# ── Footer ──────────────────────────────────────────────────
st.divider()
st.markdown("""
<p style='text-align:center;color:#999;font-size:0.85em;'>
Informes Agricolas Chile | Toro Energy | Motor v4<br>
Fuentes: CIREN, DGA, SEC, CR2MET v2.0, ODEPA, NOAA CPC
</p>
""", unsafe_allow_html=True)
