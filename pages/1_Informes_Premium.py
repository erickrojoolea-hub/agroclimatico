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

from auto_data import auto_load_comuna, COORDS_COMUNAS

st.set_page_config(
    page_title="Informes Premium | Visor Agroclimatico",
    page_icon="📊",
    layout="wide",
)

# ── Header ──────────────────────────────────────────────────
st.markdown("""
<h1 style='color:#2E7D32;'>Informes Agroclimaticos Premium</h1>
<p style='font-size:1.1em;color:#555;'>
Genera 3 informes profesionales (Basico, Avanzado, Predial) para cualquier comuna del Maule.
Motor v4 con scoring SAC/SRC/STI/IAP y datos CIREN, DGA, SEC, CR2MET, ODEPA.
</p>
""", unsafe_allow_html=True)

st.divider()

# ── Selector de comuna ──────────────────────────────────────
COMUNAS_DISPONIBLES = {
    k.title(): v for k, v in COORDS_COMUNAS.items()
}

col1, col2 = st.columns([2, 1])
with col1:
    comuna = st.selectbox(
        "Selecciona una comuna",
        options=sorted(COMUNAS_DISPONIBLES.keys()),
        index=sorted(COMUNAS_DISPONIBLES.keys()).index("San Clemente") if "San Clemente" in COMUNAS_DISPONIBLES else 0,
    )
with col2:
    coords = COMUNAS_DISPONIBLES[comuna]
    st.metric("Coordenadas", f"{coords[0]:.2f}S, {abs(coords[1]):.2f}O")
    st.metric("Altitud", f"{coords[2]} m s.n.m.")

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
