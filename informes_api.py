"""Cliente del backend Santibáñez v4 (Cloudflare Tunnel).

Expone render_tab_informes() para usar desde una página Streamlit.
"""
import base64
import requests
import streamlit as st

DEFAULT_API = "https://art-bomb-bloomberg-clay.trycloudflare.com"


def _api_url() -> str:
    try:
        return st.secrets.get("INFORMES_API_URL", DEFAULT_API)
    except Exception:
        return DEFAULT_API


@st.cache_data(show_spinner=False, ttl=3600)
def healthcheck(api_url: str) -> dict | None:
    try:
        r = requests.get(f"{api_url.rstrip('/')}/health", timeout=4)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        return {"ok": False, "error": str(e)}


@st.cache_data(show_spinner=False, ttl=1800)
def fetch_informe(api_url: str, lat: float, lon: float, predio_label: str | None = None):
    """Devuelve (pdf_bytes, headers_dict). Raise en error."""
    r = requests.post(
        f"{api_url.rstrip('/')}/informe",
        json={"lat": lat, "lon": lon, "predio_label": predio_label},
        timeout=180,
    )
    if r.status_code != 200:
        try:
            detail = r.json().get("detail", r.text)
        except Exception:
            detail = r.text
        raise RuntimeError(f"HTTP {r.status_code}: {detail}")
    return r.content, dict(r.headers)


def _chile_continental(lat: float, lon: float) -> bool:
    return -56.0 <= lat <= -17.0 and -76.0 <= lon <= -66.0


def render_tab_informes():
    """Renderiza la sección completa de informes. Llamar desde una página Streamlit."""
    st.markdown(
        """
        <style>
        div.stButton > button[kind="primary"],
        div.stDownloadButton > button[kind="primary"] {
            background-color: #7C3AED;
            border-color: #7C3AED;
            color: #FFFFFF;
        }
        div.stButton > button[kind="primary"]:hover,
        div.stDownloadButton > button[kind="primary"]:hover {
            background-color: #6D28D9;
            border-color: #6D28D9;
            color: #FFFFFF;
        }
        div.stButton > button[kind="primary"]:focus,
        div.stDownloadButton > button[kind="primary"]:focus {
            background-color: #6D28D9;
            border-color: #6D28D9;
            color: #FFFFFF;
            box-shadow: 0 0 0 0.2rem rgba(124, 58, 237, 0.25);
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.header("🌾 Informe Agroclimático Santibáñez v4")
    api_url = _api_url()

    h = healthcheck(api_url)
    if h and h.get("ok"):
        st.success(f"🟢 Backend conectado · {h.get('comunas_conocidas','?')} comunas conocidas")
    else:
        st.error(
            "🔴 Backend no responde. Verificá que `launch_api.sh` esté corriendo "
            f"y que la URL en secrets sea la vigente.\n\n{h}"
        )
        st.stop()

    st.markdown(
        "Ingresá una coordenada dentro de Chile continental y generá un informe "
        "agroclimático completo (~40-50 pág, 30-90 s de espera)."
    )

    # Aplicar override de ejemplos ANTES de crear los widgets
    lat_default = st.session_state.pop("_example_lat", -35.55)
    lon_default = st.session_state.pop("_example_lon", -71.48)

    col1, col2, col3 = st.columns([1, 1, 2])
    with col1:
        lat = st.number_input(
            "Latitud", value=lat_default, format="%.6f",
            min_value=-56.0, max_value=-17.0,
        )
    with col2:
        lon = st.number_input(
            "Longitud", value=lon_default, format="%.6f",
            min_value=-76.0, max_value=-66.0,
        )
    with col3:
        predio_label = st.text_input("Etiqueta del predio (opcional)", value="")

    st.caption("Ejemplos rápidos:")
    ec1, ec2, ec3 = st.columns(3)
    if ec1.button("Parral UTM"):
        st.session_state["_example_lat"] = -36.143091
        st.session_state["_example_lon"] = -71.644688
        st.rerun()
    if ec2.button("San Clemente"):
        st.session_state["_example_lat"] = -35.55
        st.session_state["_example_lon"] = -71.48
        st.rerun()
    if ec3.button("Talca"):
        st.session_state["_example_lat"] = -35.43
        st.session_state["_example_lon"] = -71.67
        st.rerun()

    if st.button("Generar informe", type="primary"):
        if not _chile_continental(lat, lon):
            st.warning("Las coordenadas están fuera de Chile continental.")
            return

        with st.spinner(f"Generando informe para ({lat:.4f}, {lon:.4f})… esto toma 30-90 s"):
            try:
                pdf_bytes, headers = fetch_informe(api_url, lat, lon, predio_label or None)
            except Exception as e:
                st.error(f"Error generando informe: {e}")
                return

        st.session_state["_ultimo_pdf"] = pdf_bytes
        st.session_state["_ultimo_headers"] = headers

    pdf_bytes = st.session_state.get("_ultimo_pdf")
    headers = st.session_state.get("_ultimo_headers", {})
    if pdf_bytes:
        comuna = headers.get("X-Comuna-Resuelta", "?")
        dist = headers.get("X-Distancia-Comuna-Km", "?")
        alt = headers.get("X-Altitud-Usada", "?")

        st.success("✅ Informe generado")
        st.caption(
            f"Comuna resuelta: **{comuna}** · distancia al centroide: **{dist} km** · "
            f"altitud usada: **{alt} m** · {len(pdf_bytes)/1e6:.1f} MB"
        )

        st.download_button(
            "📥 Descargar PDF",
            data=pdf_bytes,
            file_name=f"informe_santibanez_{comuna}.pdf",
            mime="application/pdf",
            type="primary",
        )

        b64 = base64.b64encode(pdf_bytes).decode()
        st.components.v1.html(
            f'<iframe src="data:application/pdf;base64,{b64}" '
            f'width="100%" height="900" '
            f'style="border:1px solid #ddd;border-radius:8px"></iframe>',
            height=920,
        )
