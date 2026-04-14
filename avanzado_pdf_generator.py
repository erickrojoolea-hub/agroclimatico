"""
avanzado_pdf_generator.py — Generador de Informe Meteorológico Avanzado (PDF)
==============================================================================
Informe profesional de 15+ páginas con relato analítico, metodología, fuentes,
predicciones multi-método, riesgos cuantificados y recomendaciones.

Fuentes: CR2MET v2.0, CR2 Estaciones, PVsyst TMY, ENSO/PDO/SOI,
Catastro Frutícola CIREN/ODEPA, CHIRPS v2, Atlas Agroclimático Santibáñez.
"""

import io
import math
from datetime import datetime

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import cm, mm
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT, TA_JUSTIFY
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    Image, PageBreak, HRFlowable, KeepTogether,
)
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle

# ── Paleta ────────────────────────────────────────────────────────────
V_OSC = colors.HexColor('#1B5E20')
V_MED = colors.HexColor('#2E7D32')
V_CLA = colors.HexColor('#C8E6C9')
V_FND = colors.HexColor('#E8F5E9')
AZ    = colors.HexColor('#1565C0')
AZ_CL = colors.HexColor('#E3F2FD')
RJ    = colors.HexColor('#C62828')
RJ_CL = colors.HexColor('#FFEBEE')
NJ    = colors.HexColor('#E65100')
NJ_CL = colors.HexColor('#FFF3E0')
AM    = colors.HexColor('#F9A825')
GR    = colors.HexColor('#F5F5F5')
GR_OS = colors.HexColor('#424242')
GR_MD = colors.HexColor('#9E9E9E')
BL    = colors.white

MESES = ['Ene','Feb','Mar','Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic']
MESES_FULL = ['Enero','Febrero','Marzo','Abril','Mayo','Junio',
              'Julio','Agosto','Septiembre','Octubre','Noviembre','Diciembre']


def _S():
    """Crea todos los estilos."""
    base = getSampleStyleSheet()
    c = {}
    def _p(name, parent='Normal', **kw):
        c[name] = ParagraphStyle(name, parent=base[parent], **kw)

    _p('Portada', 'Title', fontSize=28, leading=34, textColor=V_OSC, alignment=TA_LEFT, spaceAfter=4)
    _p('PortadaSub', fontSize=16, leading=20, textColor=V_MED, spaceAfter=8)
    _p('Sec', 'Heading1', fontSize=15, leading=18, textColor=V_OSC, spaceBefore=14, spaceAfter=6)
    _p('Sub', 'Heading2', fontSize=12, leading=15, textColor=V_MED, spaceBefore=8, spaceAfter=4)
    _p('Sub2', 'Heading3', fontSize=10.5, leading=13, textColor=V_MED, spaceBefore=6, spaceAfter=3)
    _p('B', fontSize=9.5, leading=13, textColor=GR_OS, alignment=TA_JUSTIFY, spaceAfter=6)
    _p('Bn', fontSize=9.5, leading=13, textColor=colors.black, alignment=TA_JUSTIFY,
       spaceAfter=6, fontName='Helvetica-Bold')
    _p('Sm', fontSize=8.5, leading=11, textColor=GR_OS, alignment=TA_JUSTIFY, spaceAfter=4)
    _p('Src', fontSize=7.5, leading=10, textColor=GR_MD, spaceAfter=2)
    _p('Pie', fontSize=7, leading=9, textColor=GR_MD, alignment=TA_CENTER)
    _p('Alert', fontSize=9.5, leading=13, textColor=RJ, fontName='Helvetica-Bold', spaceAfter=4)
    return c


def _tbl(data, col_w, header_bg=V_OSC, alt_row=True, font_sz=8):
    """Tabla estilizada."""
    t = Table(data, colWidths=col_w)
    cmds = [
        ('BACKGROUND', (0,0), (-1,0), header_bg),
        ('TEXTCOLOR', (0,0), (-1,0), BL),
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ('FONTSIZE', (0,0), (-1,-1), font_sz),
        ('ALIGN', (0,0), (-1,-1), 'CENTER'),
        ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
        ('GRID', (0,0), (-1,-1), 0.3, GR_MD),
        ('TOPPADDING', (0,0), (-1,-1), 3),
        ('BOTTOMPADDING', (0,0), (-1,-1), 3),
    ]
    if alt_row:
        for i in range(1, len(data)):
            if i % 2 == 0:
                cmds.append(('BACKGROUND', (0,i), (-1,i), GR))
    t.setStyle(TableStyle(cmds))
    t._extra_cmds = cmds  # guardar para modificaciones posteriores
    return t


def _tbl_add_style(t, *cmds):
    """Agrega comandos de estilo adicionales a una tabla."""
    t.setStyle(TableStyle(list(cmds)))


def _alert_box(text, nivel='info'):
    bgs = {'rojo': (RJ_CL, RJ), 'naranja': (NJ_CL, NJ), 'verde': (V_FND, V_MED),
           'azul': (AZ_CL, AZ), 'info': (GR, GR_OS)}
    bg, fg = bgs.get(nivel, bgs['info'])
    p = Paragraph(text, ParagraphStyle('_ab', fontSize=9, leading=12, textColor=fg,
                                        fontName='Helvetica-Bold'))
    t = Table([[p]], colWidths=[460])
    t.setStyle(TableStyle([
        ('BACKGROUND',(0,0),(-1,-1),bg), ('BOX',(0,0),(-1,-1),1,fg),
        ('TOPPADDING',(0,0),(-1,-1),6), ('BOTTOMPADDING',(0,0),(-1,-1),6),
        ('LEFTPADDING',(0,0),(-1,-1),10), ('RIGHTPADDING',(0,0),(-1,-1),10),
    ]))
    return t


def _chart(plot_fn, w=440, h=170):
    """Wrapper para generar chart como Image."""
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        fig = plot_fn(plt)
        if fig is None:
            return None
        buf = io.BytesIO()
        fig.savefig(buf, format='png', dpi=150, bbox_inches='tight')
        plt.close(fig)
        buf.seek(0)
        return Image(buf, width=w, height=h)
    except Exception:
        return None


def _kpi_row(items):
    """items: list of (valor, label)"""
    n = len(items)
    cw = 460 / n
    r1 = [it[0] for it in items]
    r2 = [it[1] for it in items]
    t = Table([r1, r2], colWidths=[cw]*n, rowHeights=[26, 14])
    t.setStyle(TableStyle([
        ('ALIGN',(0,0),(-1,-1),'CENTER'), ('VALIGN',(0,0),(-1,-1),'MIDDLE'),
        ('FONTNAME',(0,0),(-1,0),'Helvetica-Bold'), ('FONTSIZE',(0,0),(-1,0),15),
        ('TEXTCOLOR',(0,0),(-1,0),V_OSC), ('FONTSIZE',(0,1),(-1,1),7.5),
        ('TEXTCOLOR',(0,1),(-1,1),GR_OS), ('BACKGROUND',(0,0),(-1,-1),V_FND),
        ('BOX',(0,0),(-1,-1),0.5,V_CLA),
    ]))
    return t


# ══════════════════════════════════════════════════════════════════════
#  GRÁFICOS
# ══════════════════════════════════════════════════════════════════════

def _plot_temp_profile(plt, monthly_df):
    """Perfil térmico: Tmax, Tmed, Tmin mensual."""
    fig, ax = plt.subplots(figsize=(6.5, 2.8))
    tmax = [monthly_df.loc['T.MAX', m] for m in MESES]
    tmin = [monthly_df.loc['T.MIN', m] for m in MESES]
    tmed = [monthly_df.loc['T.MED', m] for m in MESES]

    ax.fill_between(range(12), tmin, tmax, alpha=0.15, color='#E65100')
    ax.plot(range(12), tmax, 'o-', color='#C62828', linewidth=1.5, markersize=4, label='T.Máx')
    ax.plot(range(12), tmed, 's-', color='#F9A825', linewidth=1.5, markersize=4, label='T.Med')
    ax.plot(range(12), tmin, 'o-', color='#1565C0', linewidth=1.5, markersize=4, label='T.Mín')
    ax.axhline(y=0, color='#C62828', linewidth=0.8, linestyle=':')

    for i in range(12):
        ax.text(i, tmax[i]+0.5, f'{tmax[i]:.0f}', ha='center', fontsize=5.5, color='#C62828')
        ax.text(i, tmin[i]-1.2, f'{tmin[i]:.0f}', ha='center', fontsize=5.5, color='#1565C0')

    ax.set_xticks(range(12))
    ax.set_xticklabels(MESES, fontsize=7)
    ax.set_ylabel('Temperatura (°C)', fontsize=8)
    ax.set_title('Perfil Térmico Mensual', fontsize=10, fontweight='bold', color='#1B5E20')
    ax.legend(fontsize=7, loc='upper right')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.tick_params(labelsize=7)
    fig.tight_layout()
    return fig


def _plot_precip_bar(plt, precip_data):
    fig, ax = plt.subplots(figsize=(6.5, 2.3))
    vals = precip_data.get('mensual_mm', [0]*12)
    mx = max(vals) if vals else 1
    bar_colors = ['#1565C0' if v > mx*0.7 else '#2E7D32' for v in vals]
    bars = ax.bar(MESES, vals, color=bar_colors, alpha=0.85, edgecolor='white', linewidth=0.5)
    for bar, v in zip(bars, vals):
        if v > 0:
            ax.text(bar.get_x()+bar.get_width()/2, v+mx*0.02, f'{v:.0f}',
                   ha='center', va='bottom', fontsize=6, color='#424242')
    ax.set_ylabel('mm', fontsize=8)
    ax.set_title(f'Precipitación Mensual — {sum(vals):.0f} mm/año', fontsize=9,
                fontweight='bold', color='#1B5E20')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.tick_params(labelsize=7)
    fig.tight_layout()
    return fig


def _plot_frost_profile(plt, heladas_data):
    por_mes = heladas_data.get('por_mes', [])
    if not por_mes: return None
    fig, ax1 = plt.subplots(figsize=(6.5, 2.5))
    meses_h = [m.get('mes','') for m in por_mes]
    probs = [m.get('prob_helada_mensual',0)*100 for m in por_mes]
    tmin = [m.get('tmin_media_C', m.get('tmin_min_abs_C',0)) for m in por_mes]
    cols = ['#C62828' if p>50 else '#E65100' if p>20 else '#F9A825' if p>5 else '#2E7D32' for p in probs]
    bars = ax1.bar(meses_h, probs, color=cols, alpha=0.8, edgecolor='white')
    ax1.set_ylabel('P(helada) %', fontsize=8, color='#C62828')
    for b,v in zip(bars,probs):
        if v>1: ax1.text(b.get_x()+b.get_width()/2, v+1, f'{v:.0f}%', ha='center', fontsize=6)
    ax2 = ax1.twinx()
    ax2.plot(meses_h, tmin, '--', lw=1.2, marker='o', ms=3, color='#1565C0')
    ax2.set_ylabel('Tmin (°C)', fontsize=8, color='#1565C0')
    ax2.axhline(y=0, color='#C62828', lw=0.8, ls=':')
    ax1.set_title('Probabilidad de Helada y Temperatura Mínima', fontsize=9, fontweight='bold', color='#1B5E20')
    ax1.spines['top'].set_visible(False)
    ax1.tick_params(labelsize=7); ax2.tick_params(labelsize=7)
    fig.tight_layout()
    return fig


def _plot_balance(plt, balance):
    import numpy as np
    pp = balance.get('precipitacion_mm',[0]*12)
    etp = balance.get('etp_mm',[0]*12)
    fig, ax = plt.subplots(figsize=(6.5, 2.5))
    x = np.arange(12); w = 0.35
    ax.bar(x-w/2, pp, w, label='Precipitación', color='#1565C0', alpha=0.8)
    ax.bar(x+w/2, etp, w, label='ETP', color='#E65100', alpha=0.8)
    for i in range(12):
        if etp[i] > pp[i]:
            ax.annotate('', xy=(i,pp[i]), xytext=(i,etp[i]),
                        arrowprops=dict(arrowstyle='<->', color='#C62828', lw=0.7))
    ax.set_xticks(x); ax.set_xticklabels(MESES, fontsize=7)
    ax.set_ylabel('mm', fontsize=8)
    ax.set_title('Balance Hídrico (P vs ETP)', fontsize=9, fontweight='bold', color='#1B5E20')
    ax.legend(fontsize=7); ax.spines['top'].set_visible(False); ax.spines['right'].set_visible(False)
    fig.tight_layout()
    return fig


def _plot_degree_days(plt, monthly_df):
    fig, ax = plt.subplots(figsize=(6.5, 2.3))
    dg = [monthly_df.loc['DIAS GRADO', m] for m in MESES]
    dg12 = [monthly_df.loc['DIAS GRA12', m] for m in MESES]
    ax.bar(range(12), dg, 0.4, label='Base 10°C', color='#E65100', alpha=0.8, align='center')
    ax.bar([x+0.4 for x in range(12)], dg12, 0.4, label='Base 12°C', color='#F9A825', alpha=0.8, align='center')
    ax.set_xticks([x+0.2 for x in range(12)]); ax.set_xticklabels(MESES, fontsize=7)
    ax.set_ylabel('Grados-día', fontsize=8)
    ax.set_title('Días Grado Mensuales', fontsize=9, fontweight='bold', color='#1B5E20')
    ax.legend(fontsize=7); ax.spines['top'].set_visible(False); ax.spines['right'].set_visible(False)
    fig.tight_layout()
    return fig


def _plot_chill_hours(plt, monthly_df):
    fig, ax = plt.subplots(figsize=(6.5, 2.3))
    hf = [monthly_df.loc['HRS.FRIO', m] for m in MESES]
    hfr = [monthly_df.loc['HRS.FRES', m] for m in MESES]
    ax.bar(range(12), hfr, color='#90CAF9', alpha=0.7, label='Hrs Frescas (<10°C)')
    ax.bar(range(12), hf, color='#1565C0', alpha=0.9, label='Hrs Frío (<7°C)')
    acum = [monthly_df.loc['HF.ACUM', m] for m in MESES]
    ax2 = ax.twinx()
    ax2.plot(range(12), acum, 'r-', lw=1.5, marker='s', ms=3, label='Acum. May-Dic')
    ax2.set_ylabel('Acumuladas', fontsize=8, color='red')
    ax.set_xticks(range(12)); ax.set_xticklabels(MESES, fontsize=7)
    ax.set_ylabel('Horas/mes', fontsize=8)
    ax.set_title('Horas de Frío y Frescas', fontsize=9, fontweight='bold', color='#1B5E20')
    ax.legend(fontsize=6.5, loc='upper left'); ax2.legend(fontsize=6.5, loc='upper right')
    ax.spines['top'].set_visible(False)
    fig.tight_layout()
    return fig


def _plot_solar_radiation(plt, monthly_df):
    fig, ax = plt.subplots(figsize=(6.5, 2.3))
    rad = [monthly_df.loc['R.SOLAR', m] for m in MESES]
    ax.plot(range(12), rad, 'o-', color='#F9A825', lw=2, ms=5)
    ax.fill_between(range(12), rad, alpha=0.2, color='#F9A825')
    for i, v in enumerate(rad):
        ax.text(i, v+8, f'{v:.0f}', ha='center', fontsize=6)
    ax.set_xticks(range(12)); ax.set_xticklabels(MESES, fontsize=7)
    ax.set_ylabel('cal/cm²/día', fontsize=8)
    ax.set_title('Radiación Solar Media Diaria', fontsize=9, fontweight='bold', color='#1B5E20')
    ax.spines['top'].set_visible(False); ax.spines['right'].set_visible(False)
    fig.tight_layout()
    return fig


def _plot_enso_forecast(plt, oni_data, precip_data):
    """Pronóstico multi-método con bandas de confianza."""
    import numpy as np
    pp_anual = precip_data.get('anual_mm', 500)
    mega = precip_data.get('megasequia', {})
    cambio = mega.get('cambio_pct', 0)

    # Método 1: ENSO directo
    estado = oni_data.get('estado', 'Neutro')
    oni = oni_data.get('oni_actual', 0)
    if estado == 'El Niño':
        factor_enso = 1 + min(oni * 0.15, 0.4)
    elif estado == 'La Niña':
        factor_enso = 1 + max(oni * 0.15, -0.3)
    else:
        factor_enso = 1.0
    m1 = pp_anual * factor_enso

    # Método 2: Tendencia lineal (megasequía)
    tasa_anual = cambio / 15 if cambio else 0  # % por año
    m2 = pp_anual * (1 + tasa_anual * 5 / 100)  # 5 años adelante

    # Método 3: Climatología + regresión ENSO
    m3 = pp_anual * 0.5 + m1 * 0.5  # blend

    metodos = ['ENSO\ndirecto', 'Tendencia\nlineal', 'Blend\nENSO+Clim', 'Climatología']
    valores = [m1, m2, m3, pp_anual]
    errores = [pp_anual*0.18, pp_anual*0.22, pp_anual*0.15, pp_anual*0.20]

    fig, ax = plt.subplots(figsize=(6.5, 2.5))
    cols = ['#1565C0', '#E65100', '#2E7D32', '#9E9E9E']
    bars = ax.bar(metodos, valores, color=cols, alpha=0.85, edgecolor='white')
    ax.errorbar(metodos, valores, yerr=errores, fmt='none', ecolor='black', capsize=4, lw=1)

    for b, v in zip(bars, valores):
        ax.text(b.get_x()+b.get_width()/2, v+10, f'{v:.0f}', ha='center', fontsize=8, fontweight='bold')

    ax.axhline(y=pp_anual, color='gray', ls='--', lw=0.8, label=f'Normal ({pp_anual:.0f} mm)')
    ax.set_ylabel('Precipitación (mm)', fontsize=8)
    ax.set_title('Predicción Multi-Método: Precipitación Esperada', fontsize=10,
                fontweight='bold', color='#1B5E20')
    ax.legend(fontsize=7)
    ax.spines['top'].set_visible(False); ax.spines['right'].set_visible(False)
    ax.tick_params(labelsize=7)
    fig.tight_layout()
    return fig


def _plot_frost_prediction(plt, heladas_data, enso):
    """Predicción de heladas con modulación ENSO."""
    por_mes = heladas_data.get('por_mes', [])
    if not por_mes: return None

    estado = enso.get('estado', 'Neutro')
    # Factor ENSO para heladas
    if estado == 'La Niña':
        factor = 1.25  # más heladas
    elif estado == 'El Niño':
        factor = 0.75  # menos heladas
    else:
        factor = 1.0

    fig, ax = plt.subplots(figsize=(6.5, 2.5))
    meses_h = [m.get('mes','') for m in por_mes]
    dias_base = [m.get('dias_helada_por_año', m.get('dias_helada_año', 0)) for m in por_mes]
    dias_enso = [d * factor for d in dias_base]

    x = range(12)
    ax.bar([i-0.2 for i in x], dias_base, 0.35, label='Climatología', color='#1565C0', alpha=0.8)
    ax.bar([i+0.2 for i in x], dias_enso, 0.35, label=f'Ajuste {estado}', color='#C62828' if factor>1 else '#2E7D32', alpha=0.8)
    ax.set_xticks(x); ax.set_xticklabels(meses_h, fontsize=7)
    ax.set_ylabel('Días helada/año', fontsize=8)
    ax.set_title(f'Predicción de Heladas — Escenario {estado} (factor {factor:.2f})',
                fontsize=9, fontweight='bold', color='#1B5E20')
    ax.legend(fontsize=7)
    ax.spines['top'].set_visible(False); ax.spines['right'].set_visible(False)
    fig.tight_layout()
    return fig


# ══════════════════════════════════════════════════════════════════════
#  GENERADOR PRINCIPAL
# ══════════════════════════════════════════════════════════════════════

def generate_avanzado_pdf(localidad, lat, lon, alt, avanzado_report,
                           monthly_df=None, dc_df=None, hel_df=None,
                           bio_tables=None, analisis_texts=None,
                           winkler=0, fototermico=0, huglin=0, huglin_clase='',
                           noches_frias=0, noches_frias_clase='',
                           porciones_frio=0, prob_helada=None,
                           helada_tardia=None, tipo_helada=None):
    """
    Genera PDF profesional de 15+ páginas.

    Parámetros:
        - avanzado_report: dict de generar_informe_avanzado()
        - monthly_df: DataFrame 18×13 (opcional, de climate_engine)
        - dc_df, hel_df, bio_tables: tablas de Santibáñez (opcional)
        - winkler, huglin, etc.: índices bioclimáticos (opcional)
    """
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=letter,
        leftMargin=2.2*cm, rightMargin=2.2*cm,
        topMargin=2*cm, bottomMargin=2*cm)

    s = _S()
    E = []  # elements
    has_meteo = monthly_df is not None

    sec = avanzado_report.get('secciones', {})
    precip = sec.get('precipitacion', {})
    heladas = sec.get('heladas', {})
    enso = sec.get('enso', {})
    agro = sec.get('contexto_agro', {})
    balance = sec.get('balance_hidrico', {})
    pronostico = sec.get('pronostico', {})
    resumen = sec.get('resumen', {})
    dist_mar = avanzado_report.get('distancia_mar_km', 0)
    fecha = avanzado_report.get('fecha_generacion', datetime.now().strftime('%Y-%m-%d'))
    n_sec = 0

    # ════════════════════════════════════════════════════════════════
    # PORTADA (página 1)
    # ════════════════════════════════════════════════════════════════
    E.append(Spacer(1, 30))
    E.append(Paragraph('Informe Meteorológico<br/>Avanzado', s['Portada']))
    E.append(Paragraph(localidad, s['PortadaSub']))
    E.append(Spacer(1, 4))
    E.append(Paragraph(
        f'Lat: {abs(lat):.4f}°S | Lon: {abs(lon):.4f}°W | Alt: {alt:.0f} m s.n.m. | '
        f'Dist. costa: {dist_mar:.0f} km | Generado: {fecha}', s['Src']))
    E.append(Spacer(1, 10))

    pp_a = precip.get('anual_mm', 0)
    dias_h = heladas.get('dias_helada_año_promedio', 0)
    plh = heladas.get('periodo_libre_heladas_dias', 0)
    E.append(_kpi_row([
        (f'{pp_a:.0f} mm', 'Precip. anual'),
        (f'{dias_h:.0f} d/año', 'Heladas'),
        (f'{plh} días', 'Per. libre heladas'),
        (enso.get('estado','N/D'), 'ENSO'),
        (f'{alt:.0f} m', 'Altitud'),
    ]))
    E.append(Spacer(1, 8))

    # Alertas
    for a in resumen.get('alertas', []):
        clean = a.replace('🔴 ','').replace('🟡 ','').replace('🟢 ','')
        niv = 'rojo' if '🔴' in a else 'naranja' if '🟡' in a else 'verde'
        E.append(_alert_box(clean, niv))
        E.append(Spacer(1, 2))

    E.append(Spacer(1, 6))
    E.append(HRFlowable(width='100%', thickness=1, color=V_CLA))

    # ════════════════════════════════════════════════════════════════
    # 1. RESUMEN EJECUTIVO
    # ════════════════════════════════════════════════════════════════
    n_sec += 1
    E.append(Paragraph(f'{n_sec}. Resumen Ejecutivo', s['Sec']))
    E.append(Paragraph(_relato_resumen(localidad, lat, lon, alt, dist_mar,
                                        precip, heladas, enso, agro, balance, pronostico,
                                        has_meteo, winkler, huglin), s['B']))

    # ════════════════════════════════════════════════════════════════
    # 2. RÉGIMEN TÉRMICO
    # ════════════════════════════════════════════════════════════════
    E.append(PageBreak())
    n_sec += 1
    E.append(Paragraph(f'{n_sec}. Régimen Térmico', s['Sec']))

    if has_meteo:
        E.append(Paragraph(
            f'El análisis térmico se basa en datos TMY (Typical Meteorological Year) de 8.760 horas, '
            f'procesados según la metodología de Fernando Santibáñez (Atlas Agroclimático de Chile). '
            f'La temperatura máxima media anual es de <b>{monthly_df.loc["T.MAX","Anual"]:.1f}°C</b>, '
            f'la media de <b>{monthly_df.loc["T.MED","Anual"]:.1f}°C</b> y la mínima de '
            f'<b>{monthly_df.loc["T.MIN","Anual"]:.1f}°C</b>. '
            f'La amplitud térmica media es de '
            f'<b>{monthly_df.loc["T.MAX","Anual"]-monthly_df.loc["T.MIN","Anual"]:.1f}°C</b>, '
            f'lo que es {"alto, típico de climas continentales" if monthly_df.loc["T.MAX","Anual"]-monthly_df.loc["T.MIN","Anual"] > 15 else "moderado, con influencia oceánica"}.',
            s['B']))

        # Gráfico perfil térmico
        ch = _chart(lambda plt: _plot_temp_profile(plt, monthly_df), h=190)
        if ch: E.append(ch); E.append(Spacer(1, 6))

        # Tabla Tmax, Tmin, Tmed
        E.append(Paragraph(f'{n_sec}.1 Temperaturas Mensuales (°C)', s['Sub']))
        rows = [[''] + MESES + ['Anual']]
        for var, label in [('T.MAX','T.Máx'), ('T.MED','T.Med'), ('T.MIN','T.Mín')]:
            row = [label]
            for m in MESES + ['Anual']:
                row.append(f'{monthly_df.loc[var, m]:.1f}')
            rows.append(row)
        E.append(_tbl(rows, [40]+[31]*12+[36]))
        E.append(Spacer(1, 4))
        E.append(Paragraph('Fuente: Meteonorm 8.2 / PVsyst TMY (2010-2019). Metodología: Santibáñez (2017).', s['Src']))

        # Días grado
        E.append(Paragraph(f'{n_sec}.2 Días Grado y Acumulación Térmica', s['Sub']))
        dg_anual = monthly_df.loc['DIAS GRADO', 'Anual']
        dg12_anual = monthly_df.loc['DIAS GRA12', 'Anual']
        E.append(Paragraph(
            f'Los días grado base 10°C acumulados anualmente son <b>{dg_anual:.0f} °C·día</b> '
            f'y base 12°C son <b>{dg12_anual:.0f} °C·día</b>. La acumulación térmica efectiva '
            f'(Oct-Mar) determina la aptitud para cultivos de ciclo largo como vid y frutales de '
            f'hoja caduca.', s['B']))
        ch = _chart(lambda plt: _plot_degree_days(plt, monthly_df), h=160)
        if ch: E.append(ch)

        # Radiación solar
        E.append(Paragraph(f'{n_sec}.3 Radiación Solar', s['Sub']))
        rad_max = max(monthly_df.loc['R.SOLAR', m] for m in MESES)
        rad_min = min(monthly_df.loc['R.SOLAR', m] for m in MESES)
        E.append(Paragraph(
            f'La radiación solar media diaria oscila entre <b>{rad_min:.0f} cal/cm²/día</b> '
            f'en invierno y <b>{rad_max:.0f} cal/cm²/día</b> en verano. '
            f'Este rango es {"adecuado para la mayoría de los cultivos frutícolas" if rad_max > 350 else "limitante para cultivos de alta demanda lumínica"}. '
            f'La radiación condiciona la coloración de frutos (manzanas, cerezas), la acumulación '
            f'de azúcares (vid) y la eficiencia fotosintética general.',
            s['B']))
        ch = _chart(lambda plt: _plot_solar_radiation(plt, monthly_df), h=160)
        if ch: E.append(ch)

    elif heladas.get('por_mes'):
        # Sin meteo, usar datos CR2MET de Tmin
        E.append(Paragraph(
            'El análisis térmico se basa en datos grillados CR2MET v2.0 (temperatura mínima). '
            'Para el perfil completo de temperatura máxima y media se requieren datos TMY.', s['B']))
        por_mes = heladas['por_mes']
        rows = [['Mes'] + [m.get('mes','') for m in por_mes]]
        rows.append(['Tmin media'] + [f'{m.get("tmin_media_C",0):.1f}' for m in por_mes])
        rows.append(['Tmin abs.'] + [f'{m.get("tmin_minima_abs_C",m.get("tmin_min_abs_C",0)):.1f}' for m in por_mes])
        E.append(_tbl(rows, [55]+[33]*12))
        E.append(Paragraph('Fuente: CR2MET Tmin v2.0 (Boisier et al., 2018). Período 1991-2020.', s['Src']))

    # ════════════════════════════════════════════════════════════════
    # 3. HORAS DE FRÍO Y VERNALIZACIÓN
    # ════════════════════════════════════════════════════════════════
    if has_meteo:
        E.append(PageBreak())
        n_sec += 1
        E.append(Paragraph(f'{n_sec}. Horas de Frío y Requerimientos de Vernalización', s['Sec']))

        hf_anual = monthly_df.loc['HRS.FRIO', 'Anual']
        hfr_anual = monthly_df.loc['HRS.FRES', 'Anual']
        hf_acum_max = max(monthly_df.loc['HF.ACUM', m] for m in MESES)

        E.append(Paragraph(
            f'Las horas de frío (T<7°C) acumuladas anualmente son <b>{hf_anual:.0f} horas</b> '
            f'y las horas frescas (T<10°C) son <b>{hfr_anual:.0f} horas</b>. '
            f'La acumulación máxima mayo-diciembre alcanza <b>{hf_acum_max:.0f} horas</b>. '
            f'Estos valores son {"suficientes para la mayoría de frutales caducifolios (cerezo, manzano, nogal)" if hf_anual > 800 else "limitantes para especies de alto requerimiento (cerezo, manzano)" if hf_anual > 400 else "insuficientes para la mayoría de frutales caducifolios"}.',
            s['B']))

        ch = _chart(lambda plt: _plot_chill_hours(plt, monthly_df), h=165)
        if ch: E.append(ch); E.append(Spacer(1, 4))

        # Tabla de requerimientos por especie
        E.append(Paragraph(f'{n_sec}.1 Requerimientos de Frío por Especie', s['Sub']))
        req_data = [
            ['Especie', 'Req. HF', 'Disponible', 'Estado'],
            ['Cerezo', '800-1200', f'{hf_anual:.0f}', 'Cumple' if hf_anual >= 800 else 'Déficit'],
            ['Manzano', '600-1000', f'{hf_anual:.0f}', 'Cumple' if hf_anual >= 600 else 'Déficit'],
            ['Nogal', '400-700', f'{hf_anual:.0f}', 'Cumple' if hf_anual >= 400 else 'Déficit'],
            ['Vid', '100-400', f'{hf_anual:.0f}', 'Cumple' if hf_anual >= 100 else 'Déficit'],
            ['Almendro', '200-500', f'{hf_anual:.0f}', 'Cumple' if hf_anual >= 200 else 'Déficit'],
            ['Palto', '0-50', f'{hf_anual:.0f}', 'N/A (perenne)'],
            ['Arándano', '400-800', f'{hf_anual:.0f}', 'Cumple' if hf_anual >= 400 else 'Déficit'],
            ['Kiwi', '600-800', f'{hf_anual:.0f}', 'Cumple' if hf_anual >= 600 else 'Déficit'],
        ]
        t = _tbl(req_data, [90, 80, 80, 80])
        E.append(t)

        if porciones_frio > 0:
            E.append(Paragraph(f'{n_sec}.2 Porciones de Frío Dinámicas (Fishman 1987)', s['Sub']))
            E.append(Paragraph(
                f'Las <b>Porciones de Frío Dinámicas</b> acumuladas son <b>{porciones_frio:.0f} CP</b>. '
                f'Este modelo es más preciso que las horas de frío clásicas porque considera '
                f'la irreversibilidad de la acumulación y el efecto negativo de temperaturas altas '
                f'intercaladas. Referencia: Fishman et al. (1987), Erez et al. (1990).', s['B']))

            cp_req = [
                ['Especie', 'Req. CP', 'Disponible', 'Estado'],
                ['Cerezo', '60-80', f'{porciones_frio:.0f}', 'Cumple' if porciones_frio >= 60 else 'Déficit'],
                ['Manzano', '50-70', f'{porciones_frio:.0f}', 'Cumple' if porciones_frio >= 50 else 'Déficit'],
                ['Nogal', '30-50', f'{porciones_frio:.0f}', 'Cumple' if porciones_frio >= 30 else 'Déficit'],
                ['Almendro', '20-40', f'{porciones_frio:.0f}', 'Cumple' if porciones_frio >= 20 else 'Déficit'],
                ['Arándano', '35-55', f'{porciones_frio:.0f}', 'Cumple' if porciones_frio >= 35 else 'Déficit'],
            ]
            E.append(_tbl(cp_req, [90, 80, 80, 80]))

    # ════════════════════════════════════════════════════════════════
    # 4. ANÁLISIS DE HELADAS
    # ════════════════════════════════════════════════════════════════
    E.append(PageBreak())
    n_sec += 1
    E.append(Paragraph(f'{n_sec}. Análisis de Heladas y Riesgo de Daño', s['Sec']))

    if heladas:
        tmin_abs = heladas.get('tmin_absoluta_C', 0)
        meses_sin = heladas.get('meses_sin_helada', [])
        E.append(Paragraph(_relato_heladas(heladas, localidad, alt, enso), s['B']))

        ch = _chart(lambda plt: _plot_frost_profile(plt, heladas), h=175)
        if ch: E.append(ch); E.append(Spacer(1, 4))

        # Tabla mensual detallada
        por_mes = heladas.get('por_mes', [])
        if por_mes:
            E.append(Paragraph(f'{n_sec}.1 Estadísticas Mensuales de Helada', s['Sub']))
            rows = [['Mes', 'Tmin media', 'Tmin absoluta', 'P(helada/mes)', 'Días helada/año']]
            for m in por_mes:
                prob = m.get('prob_helada_mensual', 0)
                rows.append([
                    m.get('mes',''), f'{m.get("tmin_media_C",0):.1f}°C',
                    f'{m.get("tmin_minima_abs_C",m.get("tmin_min_abs_C",0)):.1f}°C',
                    f'{prob*100:.0f}%',
                    f'{m.get("dias_helada_por_año",m.get("dias_helada_año",0)):.1f}',
                ])
            t = _tbl(rows, [45, 75, 75, 80, 80])
            # Colorear filas de riesgo
            extra = []
            for i, m in enumerate(por_mes):
                p = m.get('prob_helada_mensual', 0)
                if p > 0.5:
                    extra.append(('BACKGROUND', (0,i+1), (-1,i+1), RJ_CL))
                elif p > 0.2:
                    extra.append(('BACKGROUND', (0,i+1), (-1,i+1), NJ_CL))
            if extra:
                _tbl_add_style(t, *extra)
            E.append(t)
            E.append(Spacer(1, 4))

    # Tabla intensidad de heladas (de PVsyst si disponible)
    if hel_df is not None:
        E.append(Paragraph(f'{n_sec}.2 Intensidad de Heladas por Umbral', s['Sub']))
        E.append(Paragraph(
            'La siguiente tabla muestra el número promedio de días que la temperatura '
            'desciende bajo cada umbral, lo que permite evaluar la severidad de las heladas. '
            'Heladas bajo -4°C se consideran severas con daño irreversible en la mayoría '
            'de los frutales en floración.', s['B']))
        rows = [[''] + list(hel_df.columns)]
        for idx in hel_df.index:
            rows.append([str(idx)] + [f'{v:.1f}' if isinstance(v, float) else str(v) for v in hel_df.loc[idx]])
        E.append(_tbl(rows, [55]+[31]*len(hel_df.columns), font_sz=7))
        E.append(Paragraph('Fuente: PVsyst TMY. Metodología Santibáñez (2017).', s['Src']))

    # Helada tardía
    if helada_tardia:
        E.append(Paragraph(f'{n_sec}.3 Riesgo de Helada Tardía (Sep-Nov)', s['Sub']))
        total_t = helada_tardia.get('total', 0)
        por_mes_t = helada_tardia.get('por_mes', {})
        alerta_t = helada_tardia.get('alerta', '')
        nivel_t = helada_tardia.get('nivel', '')
        E.append(Paragraph(
            f'Se registran en promedio <b>{total_t:.1f} días de helada tardía</b> '
            f'(septiembre-noviembre), con la siguiente distribución: '
            f'{", ".join(f"{m}: {v:.1f} días" for m, v in por_mes_t.items() if v > 0)}. '
            f'<b>{alerta_t}</b>', s['B']))
        if nivel_t in ('CRITICO', 'ALTO'):
            E.append(_alert_box(
                f'RIESGO {nivel_t}: Las heladas tardías coinciden con floración de frutales '
                f'de hoja caduca. Riesgo de pérdida parcial o total de producción.', 'rojo'))

    # Tipo de helada
    if tipo_helada:
        E.append(Paragraph(f'{n_sec}.4 Clasificación del Tipo de Helada', s['Sub']))
        rad = tipo_helada.get('radiativa', 0)
        adv = tipo_helada.get('advectiva', 0)
        total = tipo_helada.get('total', rad + adv)
        pct_rad = rad/total*100 if total > 0 else 0
        E.append(Paragraph(
            f'Del total de <b>{total} eventos de helada</b>, el <b>{pct_rad:.0f}% son '
            f'radiativas</b> ({rad} eventos) y el <b>{100-pct_rad:.0f}% advectivas</b> '
            f'({adv} eventos). Las heladas radiativas ocurren en noches despejadas y calmas '
            f'por inversión térmica y son controlables con ventiladores o aspersión. '
            f'Las advectivas se deben a entrada de masas de aire polar y son más difíciles de mitigar.',
            s['B']))

    E.append(Paragraph(
        f'Fuente: {heladas.get("fuente","CR2MET Tmin v2.0")}. Período 1991-2020. '
        'Ref: Boisier et al. (2018).', s['Src']))

    # ════════════════════════════════════════════════════════════════
    # 5. PRECIPITACIÓN Y SEQUÍA
    # ════════════════════════════════════════════════════════════════
    E.append(PageBreak())
    n_sec += 1
    E.append(Paragraph(f'{n_sec}. Precipitación, Sequía y Eventos Extremos', s['Sec']))

    if precip:
        E.append(Paragraph(_relato_precip(precip, localidad), s['B']))

        ch = _chart(lambda plt: _plot_precip_bar(plt, precip), h=160)
        if ch: E.append(ch); E.append(Spacer(1, 4))

        # Tabla mensual
        if precip.get('mensual_mm'):
            rows = [['Mes']+MESES+['Anual'],
                    ['mm']+[f'{v:.0f}' for v in precip['mensual_mm']]+[f'{pp_a:.0f}']]
            t = _tbl(rows, [35]+[32]*12+[38])
            _tbl_add_style(t, ('BACKGROUND',(-1,0),(-1,-1),V_CLA),
                           ('FONTNAME',(-1,1),(-1,1),'Helvetica-Bold'))
            E.append(t)
            E.append(Spacer(1, 4))

        # Validación
        val = precip.get('validacion')
        if val:
            E.append(Paragraph(f'{n_sec}.1 Validación Cruzada', s['Sub']))
            E.append(Paragraph(
                f'CR2MET: {val.get("cr2met_mm",0):.0f} mm vs Estaciones: {val.get("estaciones_mm",0):.0f} mm. '
                f'Ratio: {val.get("ratio",0):.2f}. Concordancia: <b>{val.get("concordancia","N/D")}</b>. '
                f'Estaciones: {", ".join(val.get("estaciones_usadas",[])[:4])}.', s['Sm']))

        # Megasequía
        mega = precip.get('megasequia')
        if mega:
            E.append(Paragraph(f'{n_sec}.2 Tendencia de Megasequía', s['Sub']))
            E.append(Paragraph(_relato_megasequia(mega), s['B']))
            cambio = mega.get('cambio_pct', 0)
            if cambio < -15:
                E.append(_alert_box(
                    f'DÉFICIT SIGNIFICATIVO: {abs(cambio):.0f}% menos que 1991-2005. '
                    'Zona de megasequía activa (Garreaud et al., 2024).', 'rojo'))

        # Extremos
        ext = precip.get('extremos') or precip.get('extremos_diarios')
        if ext:
            E.append(Paragraph(f'{n_sec}.3 Eventos Extremos', s['Sub']))
            E.append(Paragraph(
                f'P95: <b>{ext.get("p95_mm",0):.0f} mm</b> | '
                f'P99: <b>{ext.get("p99_mm",0):.0f} mm</b> | '
                f'Máx diario: <b>{ext.get("max_diario_mm",0):.0f} mm</b> | '
                f'Días lluvia/año: <b>{ext.get("dias_lluvia_por_año",0):.0f}</b>', s['Bn']))

    # ════════════════════════════════════════════════════════════════
    # 6. BALANCE HÍDRICO
    # ════════════════════════════════════════════════════════════════
    if balance:
        n_sec += 1
        E.append(Paragraph(f'{n_sec}. Balance Hídrico', s['Sec']))
        deficit = balance.get('deficit_anual_mm', 0)
        meses_e = balance.get('meses_estres', 0)
        E.append(Paragraph(_relato_balance(balance, precip), s['B']))
        ch = _chart(lambda plt: _plot_balance(plt, balance), h=170)
        if ch: E.append(ch); E.append(Spacer(1, 4))

        # Tabla
        if balance.get('precipitacion_mm') and balance.get('etp_mm'):
            pp_m = balance['precipitacion_mm']
            etp_m = balance['etp_mm']
            bal_m = [pp_m[i]-etp_m[i] for i in range(12)]
            rows = [
                ['']+MESES,
                ['P (mm)']+[f'{v:.0f}' for v in pp_m],
                ['ETP (mm)']+[f'{v:.0f}' for v in etp_m],
                ['Balance']+[f'{v:+.0f}' for v in bal_m],
            ]
            t = _tbl(rows, [50]+[33]*12, font_sz=7.5)
            extra = []
            for i in range(12):
                if bal_m[i] < -20:
                    extra.append(('BACKGROUND',(i+1,3),(i+1,3),RJ_CL))
                elif bal_m[i] < 0:
                    extra.append(('BACKGROUND',(i+1,3),(i+1,3),NJ_CL))
                else:
                    extra.append(('BACKGROUND',(i+1,3),(i+1,3),V_FND))
            if extra:
                _tbl_add_style(t, *extra)
            E.append(t)

    # ════════════════════════════════════════════════════════════════
    # 7. ÍNDICES BIOCLIMÁTICOS
    # ════════════════════════════════════════════════════════════════
    if has_meteo and (winkler > 0 or huglin > 0):
        E.append(PageBreak())
        n_sec += 1
        E.append(Paragraph(f'{n_sec}. Índices Bioclimáticos y Clasificación Vitícola', s['Sec']))
        E.append(Paragraph(
            'Los índices bioclimáticos permiten clasificar el potencial agroclimático de un '
            'territorio según estándares internacionales. Estos índices son fundamentales para '
            'la selección de variedades y la planificación de nuevas plantaciones.', s['B']))

        # Tabla de índices
        idx_rows = [['Índice', 'Valor', 'Clasificación', 'Interpretación']]
        if winkler > 0:
            if winkler < 1111: wk_cl = 'Región I (Frío)'
            elif winkler < 1389: wk_cl = 'Región II (Fresco)'
            elif winkler < 1667: wk_cl = 'Región III (Templado)'
            elif winkler < 1944: wk_cl = 'Región IV (Cálido)'
            else: wk_cl = 'Región V (Muy Cálido)'
            idx_rows.append(['Winkler (°C·día)', f'{winkler:.0f}', wk_cl,
                            'Acumulación térmica Oct-Mar, base 10°C'])

        if huglin > 0:
            idx_rows.append(['Huglin (IH)', f'{huglin:.0f}', huglin_clase or '',
                            'Índice heliotérmico con factor latitud'])

        if fototermico > 0:
            idx_rows.append(['Fototérmico', f'{fototermico:.1f}', '',
                            'Potencial de madurez Feb-Mar'])

        if noches_frias > 0:
            idx_rows.append(['Noches Frías (°C)', f'{noches_frias:.1f}', noches_frias_clase or '',
                            'Tmin media marzo, potencial aromático'])

        if porciones_frio > 0:
            idx_rows.append(['Porciones Frío (CP)', f'{porciones_frio:.0f}', '',
                            'Modelo dinámico Fishman (1987)'])

        E.append(_tbl(idx_rows, [100, 65, 120, 160], font_sz=8))
        E.append(Spacer(1, 6))

        # Detalle Winkler
        if winkler > 0:
            E.append(Paragraph(f'{n_sec}.1 Índice de Winkler y Aptitud Vitícola', s['Sub']))
            E.append(Paragraph(
                f'El índice de Winkler para este punto es <b>{winkler:.0f} °C·día</b>, '
                f'clasificando como <b>{wk_cl}</b>. '
                'Este índice acumula los grados-día base 10°C entre octubre y marzo (hemisferio sur) '
                'y es el estándar internacional para la clasificación de regiones vitícolas '
                '(Amerine & Winkler, 1944). '
                f'{"La zona es apta para variedades tintas de ciclo largo como Cabernet Sauvignon y Carménère." if winkler > 1400 else "La zona favorece variedades de ciclo corto como Pinot Noir y Chardonnay." if winkler > 1100 else "La zona tiene limitaciones térmicas para la mayoría de las variedades."}',
                s['B']))

            wk_ref = [
                ['Región', 'Rango (°C·día)', 'Variedades tipo'],
                ['I - Frío', '<1.111', 'Pinot Noir, Gewürztraminer'],
                ['II - Fresco', '1.111-1.389', 'Chardonnay, Sauvignon Blanc, Riesling'],
                ['III - Templado', '1.389-1.667', 'Merlot, Cabernet Franc, Semillón'],
                ['IV - Cálido', '1.667-1.944', 'Cabernet Sauvignon, Carménère, Syrah'],
                ['V - Muy Cálido', '>1.944', 'País, Carignan, Monastrell'],
            ]
            E.append(_tbl(wk_ref, [80, 100, 260], font_sz=7.5))

        # Detalle Huglin
        if huglin > 0:
            E.append(Paragraph(f'{n_sec}.2 Índice de Huglin (Heliotérmico)', s['Sub']))
            E.append(Paragraph(
                f'El índice de Huglin es <b>{huglin:.0f}</b> ({huglin_clase}). '
                'A diferencia del Winkler, incorpora un coeficiente de duración del día '
                'que varía con la latitud, siendo más representativo para latitudes altas. '
                'Fue desarrollado por Huglin (1978) y es ampliamente utilizado en la '
                'viticultura europea.', s['B']))

        # Noches frías
        if noches_frias > 0:
            E.append(Paragraph(f'{n_sec}.3 Índice de Noches Frías', s['Sub']))
            E.append(Paragraph(
                f'La temperatura mínima media de marzo es <b>{noches_frias:.1f}°C</b> '
                f'({noches_frias_clase}). Las noches frescas durante la maduración favorecen '
                f'la síntesis de compuestos aromáticos y la retención de acidez en uvas, '
                f'resultando en vinos de mayor complejidad. Valores bajo 12°C se consideran '
                f'óptimos para calidad aromática (Tonietto & Carbonneau, 2004).', s['B']))

    # ════════════════════════════════════════════════════════════════
    # 8. APTITUD POR ESPECIE
    # ════════════════════════════════════════════════════════════════
    if bio_tables and analisis_texts:
        E.append(PageBreak())
        n_sec += 1
        E.append(Paragraph(f'{n_sec}. Aptitud Bioclimática por Especie', s['Sec']))
        E.append(Paragraph(
            'Se evalúa la aptitud agroclimática para las principales especies frutícolas, '
            'considerando temperaturas de floración, cuaja y maduración, estrés por calor, '
            'requerimientos de frío, riesgo de heladas y precipitación. Cada variable se '
            'clasifica en semáforo (favorable / precaución / desfavorable).', s['B']))

        for especie, df in bio_tables.items():
            E.append(Paragraph(f'{n_sec}.{list(bio_tables.keys()).index(especie)+1} {especie}', s['Sub2']))

            # Tabla bioclimática
            rows = [list(df.columns)]
            for _, row in df.iterrows():
                rows.append([str(v) for v in row.values])
            n_cols = len(df.columns)
            cw = [460 // n_cols] * n_cols
            E.append(_tbl(rows, cw, font_sz=7))

            # Texto análisis
            txt = analisis_texts.get(especie, '')
            if txt:
                E.append(Paragraph(txt, s['Sm']))
            E.append(Spacer(1, 4))

    # Riesgo agronómico de heladas por especie (catastro)
    if agro and agro.get('heladas_agronomicas'):
        if not bio_tables:
            E.append(PageBreak())
            n_sec += 1
        else:
            n_sec += 1
        E.append(Paragraph(f'{n_sec}. Riesgo de Helada por Especie (Catastro Frutícola)', s['Sec']))
        E.append(Paragraph(_relato_agro(agro, heladas, localidad), s['B']))

        ha_list = agro['heladas_agronomicas']
        rows = [['Especie', 'Sup. (ha)', 'Umbral (°C)', 'P(daño Sep)', 'P(daño Oct)', 'Sensibilidad', 'Riesgo']]
        for h in ha_list[:15]:
            rows.append([
                h.get('especie',''), f'{h.get("superficie_ha",0):.0f}',
                f'{h.get("umbral_floracion",0):.1f}',
                f'{h.get("p_dano_sep",0):.0f}%', f'{h.get("p_dano_oct",0):.0f}%',
                h.get('sensibilidad',''), h.get('riesgo',''),
            ])
        t = _tbl(rows, [85, 45, 55, 55, 55, 60, 55], font_sz=7.5)
        extra = []
        for i, h in enumerate(ha_list[:15]):
            r = h.get('riesgo','')
            if r in ('MUY ALTO','ALTO'):
                extra.append(('BACKGROUND',(-1,i+1),(-1,i+1),RJ_CL))
                extra.append(('TEXTCOLOR',(-1,i+1),(-1,i+1),RJ))
            elif r == 'MODERADO':
                extra.append(('BACKGROUND',(-1,i+1),(-1,i+1),NJ_CL))
        if extra:
            _tbl_add_style(t, *extra)
        E.append(t)
        E.append(Paragraph(
            'Fuente: Catastro Frutícola CIREN/ODEPA. Umbrales: INIA, Atlas Agroclimático '
            '(Santibáñez, 2017), UC Davis Fruit & Nut Research.', s['Src']))

    # ════════════════════════════════════════════════════════════════
    # 9. ENSO Y PREDICCIÓN MULTI-MÉTODO
    # ════════════════════════════════════════════════════════════════
    E.append(PageBreak())
    n_sec += 1
    E.append(Paragraph(f'{n_sec}. ENSO, Variabilidad Climática y Predicción', s['Sec']))

    # Contexto ENSO
    E.append(Paragraph(f'{n_sec}.1 Estado Actual del ENSO', s['Sub']))
    E.append(Paragraph(_relato_enso(enso, lat), s['B']))
    interp = enso.get('interpretacion_agro', '')
    if interp:
        E.append(_alert_box(interp, 'azul'))
        E.append(Spacer(1, 4))

    # Predicción multi-método
    E.append(Paragraph(f'{n_sec}.2 Predicción de Precipitación — Múltiples Métodos', s['Sub']))
    E.append(Paragraph(
        'Se presentan tres métodos independientes de estimación de la precipitación esperada '
        'para el próximo año hidrológico. La convergencia entre métodos aumenta la confianza '
        'del pronóstico, mientras que la divergencia indica mayor incertidumbre.', s['B']))

    # Calcular predicciones
    oni = enso.get('oni_actual', 0)
    estado_e = enso.get('estado', 'Neutro')
    mega = precip.get('megasequia', {})
    cambio = mega.get('cambio_pct', 0)

    if estado_e == 'El Niño':
        factor_e = 1 + min(oni * 0.15, 0.4)
    elif estado_e == 'La Niña':
        factor_e = 1 + max(oni * 0.15, -0.3)
    else:
        factor_e = 1.0

    m1 = pp_a * factor_e
    tasa = cambio / 15 if cambio else 0
    m2 = pp_a * (1 + tasa * 3 / 100)
    m3 = pp_a * 0.5 + m1 * 0.5

    ch = _chart(lambda plt: _plot_enso_forecast(plt, enso, precip), h=175)
    if ch: E.append(ch); E.append(Spacer(1, 4))

    pred_rows = [
        ['Método', 'Estimación (mm)', 'Incertidumbre', 'Fundamento'],
        ['ENSO directo', f'{m1:.0f}', f'±{pp_a*0.18:.0f} mm',
         f'Factor {factor_e:.2f} × climatología'],
        ['Tendencia lineal', f'{m2:.0f}', f'±{pp_a*0.22:.0f} mm',
         f'Tasa {tasa:.1f}%/año (megasequía)'],
        ['Blend ENSO+Clim', f'{m3:.0f}', f'±{pp_a*0.15:.0f} mm',
         '50% ENSO + 50% climatología'],
        ['Climatología', f'{pp_a:.0f}', f'±{pp_a*0.20:.0f} mm',
         'Promedio 1991-2020'],
    ]
    E.append(_tbl(pred_rows, [85, 80, 80, 200], font_sz=7.5))
    E.append(Spacer(1, 4))

    E.append(Paragraph(
        '<b>Interpretación:</b> La predicción más confiable es el Blend ENSO+Climatología cuando '
        'el ONI está en rango ±0.5. En eventos ENSO fuertes (|ONI| > 1.0), el método ENSO directo '
        'gana relevancia. La tendencia lineal captura el efecto de largo plazo de la megasequía '
        'pero no eventos interanuales.', s['B']))

    # Predicción de heladas
    E.append(Paragraph(f'{n_sec}.3 Predicción de Heladas — Escenario ENSO', s['Sub']))
    E.append(Paragraph(
        'El ENSO modula el riesgo de heladas en Chile central: La Niña incrementa el riesgo '
        'de heladas tardías en un 20-30% por cielos más despejados y menor advección de aire '
        'húmedo. El Niño reduce el riesgo por noches más cálidas y nubosas.', s['B']))

    ch = _chart(lambda plt: _plot_frost_prediction(plt, heladas, enso), h=175)
    if ch: E.append(ch)

    E.append(Paragraph(
        'Fuente: NOAA CPC (ONI, PDO, SOI). Garreaud et al. (2024). '
        'Nota: correlación ENSO-precipitación debilitada post-2000.', s['Src']))

    # Pronóstico estacional
    if pronostico:
        E.append(Paragraph(f'{n_sec}.4 Pronóstico Estacional Integrado', s['Sub']))
        outlook = pronostico.get('outlook', 'NORMAL')
        pe = pronostico.get('precip_esperada_mm', 0)
        rango = pronostico.get('rango_mm', (0, 0))
        niv = 'rojo' if outlook == 'SECO' else 'verde' if outlook == 'LLUVIOSO' else 'azul'
        E.append(_alert_box(
            f'Outlook: {outlook} — Precipitación esperada: {pe:.0f} mm '
            f'(rango {rango[0]:.0f}–{rango[1]:.0f} mm). Factor ENSO: {pronostico.get("factor_enso",1):.2f}',
            niv))

    # ════════════════════════════════════════════════════════════════
    # 10. TABLA CLIMÁTICA COMPLETA (18 variables)
    # ════════════════════════════════════════════════════════════════
    if has_meteo:
        E.append(PageBreak())
        n_sec += 1
        E.append(Paragraph(f'{n_sec}. Tabla Climática Completa — 18 Variables', s['Sec']))
        E.append(Paragraph(
            'Tabla resumen con las 18 variables agroclimáticas calculadas según la metodología '
            'de Fernando Santibáñez (Atlas Agroclimático de Chile, 2017). Basada en 8.760 horas '
            'de datos meteorológicos típicos (TMY Meteonorm 8.2).', s['B']))

        rows = [['Variable'] + MESES + ['Anual']]
        for var in monthly_df.index:
            row = [str(var)]
            for m in MESES + ['Anual']:
                v = monthly_df.loc[var, m]
                if isinstance(v, float):
                    row.append(f'{v:.1f}' if abs(v) < 100 else f'{v:.0f}')
                else:
                    row.append(str(v))
            rows.append(row)

        t = _tbl(rows, [60]+[28]*12+[33], font_sz=6)
        _tbl_add_style(t,
            ('BACKGROUND',(-1,0),(-1,-1),V_CLA),
            ('FONTNAME',(0,1),(0,-1),'Helvetica-Bold'),
            ('FONTSIZE',(0,0),(0,-1),6),
            ('ALIGN',(0,1),(0,-1),'LEFT'))
        E.append(t)
        E.append(Spacer(1, 4))
        E.append(Paragraph(
            'T.MAX/MIN/MED: °C | DIAS GRADO: base 10°C | DIAS GRA12: base 12°C | '
            'DG.ACUM: acumulado Oct-Sep | D-cálidos: días >25°C | '
            'HRS.FRIO: horas <7°C | HRS.FRES: horas <10°C | HF.ACUM: acumulado May-Dic | '
            'R.SOLAR: cal/cm²/día | H.RELAT: % | PRECIPIT: mm | EVAP.POT: mm (FAO-56) | '
            'DEF/EXC.HIDR: mm | IND.HUMED: P/ETP | HELADAS: días Tmin<0°C', s['Src']))

    # Días cálidos
    if dc_df is not None and has_meteo:
        E.append(Paragraph(f'{n_sec}.1 Días Cálidos por Umbral de Temperatura', s['Sub']))
        rows = [[''] + list(dc_df.columns)]
        for idx in dc_df.index:
            rows.append([str(idx)] + [f'{v:.1f}' if isinstance(v, float) else str(v) for v in dc_df.loc[idx]])
        E.append(_tbl(rows, [55]+[31]*len(dc_df.columns), font_sz=7))
        E.append(Paragraph(
            'Días con temperatura máxima superando umbrales de 25°C, 30°C y 35°C. '
            'Relevante para estrés calórico en frutales, golpe de sol y calidad de fruta.', s['Src']))

    # ════════════════════════════════════════════════════════════════
    # 11. METODOLOGÍA Y FUENTES
    # ════════════════════════════════════════════════════════════════
    E.append(PageBreak())
    n_sec += 1
    E.append(Paragraph(f'{n_sec}. Metodología, Fuentes y Limitaciones', s['Sec']))
    E.append(Paragraph(_metodologia(has_meteo), s['B']))

    E.append(Paragraph(f'{n_sec}.1 Referencias Bibliográficas', s['Sub']))
    refs = [
        'Boisier, J.P. et al. (2018). CR2MET: Productos grillados. Centro de Ciencia del Clima (CR)², U. Chile.',
        'Santibáñez, F. (2017). Atlas Agroclimático de Chile. Tomos I-IV. CIREN / FIA.',
        'Huang, B. et al. (2017). ERSST v5. NOAA NCEI.',
        'Mantua, N.J. et al. (1997). Pacific Interdecadal Climate Oscillation. Bull. AMS, 78(6).',
        'Allen, R.G. et al. (1998). Crop evapotranspiration. FAO-56.',
        'Garreaud, R. et al. (2024). The Central Chile Mega Drought. Earth\'s Future, 12(1).',
        'CIREN/ODEPA (2024). Catastro Frutícola Nacional.',
        'Amerine, M.A. & Winkler, A.J. (1944). Composition and quality of musts and wines. Hilgardia.',
        'Huglin, P. (1978). Nouveau mode d\'évaluation des possibilités héliothermiques. Symp. Int. Oenol.',
        'Tonietto, J. & Carbonneau, A. (2004). A multicriteria climatic classification system. Agric. For. Met.',
        'Fishman, S. et al. (1987). The temperature dependence of dormancy breaking. J. Theor. Biol.',
        'Trenberth, K.E. (1984). Signal versus noise in the Southern Oscillation. Mon. Wea. Rev.',
    ]
    for i, r in enumerate(refs):
        E.append(Paragraph(f'[{i+1}] {r}', s['Src']))

    # Disclaimer
    E.append(Spacer(1, 12))
    E.append(HRFlowable(width='100%', thickness=0.5, color=GR_MD))
    E.append(Paragraph(
        '<b>Aviso legal:</b> Informe generado automáticamente con fines de planificación agrícola. '
        'Datos de fuentes públicas y modelos con resolución limitada (~5.5 km). Para decisiones '
        'de inversión, complementar con estación meteorológica local y asesoría profesional. '
        'Las predicciones estacionales tienen incertidumbre inherente.', s['Src']))
    E.append(Paragraph(
        f'Generado: {fecha} | Visor Agroclimático v1.1 | Motor: CR2MET + PVsyst TMY + ENSO/PDO/SOI',
        s['Pie']))

    doc.build(E)
    return buf.getvalue()


# ══════════════════════════════════════════════════════════════════════
#  RELATOS
# ══════════════════════════════════════════════════════════════════════

def _relato_resumen(loc, lat, lon, alt, dm, precip, heladas, enso, agro, balance, pron, has_meteo, wk, hg):
    p = []
    p.append(f'El predio analizado se ubica en <b>{loc}</b> ({abs(lat):.2f}°S, {abs(lon):.2f}°W), '
             f'a <b>{alt:.0f} m s.n.m.</b>')
    if dm: p.append(f' y a {dm:.0f} km de la costa')
    p.append('. ')
    pp = precip.get('anual_mm', 0)
    if pp:
        cl = 'árido' if pp<200 else 'semiárido' if pp<400 else 'mediterráneo' if pp<800 else 'templado lluvioso'
        p.append(f'El clima es <b>{cl}</b> con {pp:.0f} mm/año. ')
    mega = precip.get('megasequia', {}).get('cambio_pct', 0)
    if mega < -10: p.append(f'La megasequía ha reducido la precipitación en un <b>{abs(mega):.0f}%</b>. ')
    dh = heladas.get('dias_helada_año_promedio', 0)
    plh = heladas.get('periodo_libre_heladas_dias', 0)
    if dh > 0:
        nv = 'elevado' if dh>20 else 'moderado' if dh>5 else 'bajo'
        p.append(f'Riesgo de heladas <b>{nv}</b> ({dh:.0f} d/año, PLH {plh} días). ')
    e = enso.get('estado', '')
    if e and e != 'No disponible':
        p.append(f'ENSO: <b>{e}</b> (ONI={enso.get("oni_actual",0):+.2f}). ')
    if agro and agro.get('total_ha'):
        p.append(f'Superficie frutícola comunal: <b>{agro["total_ha"]:.0f} ha</b>. ')
    if balance:
        df = balance.get('deficit_anual_mm', 0)
        me = balance.get('meses_estres', 0)
        if df > 0: p.append(f'Déficit hídrico: {df:.0f} mm/año ({me} meses de estrés). ')
    if has_meteo and wk > 0:
        p.append(f'Winkler: {wk:.0f} °C·día. ')
    if has_meteo and hg > 0:
        p.append(f'Huglin: {hg:.0f}. ')
    return ''.join(p)


def _relato_enso(enso, lat):
    if not enso or enso.get('estado') == 'No disponible':
        return 'Información ENSO no disponible.'
    e = enso.get('estado','Neutro')
    oni = enso.get('oni_actual',0)
    oni3 = enso.get('oni_3m',0)
    tend = enso.get('tendencia','estable')
    pdo = enso.get('pdo_3m',0)
    soi = enso.get('soi_3m',0)
    t = (f'El ONI registra <b>{oni:+.2f}</b> (trimestral: {oni3:+.2f}). Estado: <b>{e}</b>, '
         f'tendencia <b>{tend}</b>. PDO: {pdo:+.2f}, SOI: {soi:+.2f}. ')
    if -38 < lat < -28:
        t += ('Para Chile central, El Niño → invierno más lluvioso (+20-40%), menos heladas invernales. '
              'La Niña → sequía invernal, mayor riesgo de heladas tardías en floración (Sep-Nov). ')
    t += 'Garreaud et al. (2024): correlación ENSO-precipitación debilitada post-2000.'
    return t


def _relato_heladas(heladas, loc, alt, enso):
    d = heladas.get('dias_helada_año_promedio',0)
    plh = heladas.get('periodo_libre_heladas_dias',0)
    ta = heladas.get('tmin_absoluta_C',0)
    ms = heladas.get('meses_sin_helada',[])
    por_mes = heladas.get('por_mes',[])
    t = (f'En {loc} ({alt:.0f} m) se registran <b>{d:.0f} días de helada/año</b> '
         f'(Tmin<0°C), con mínima absoluta de <b>{ta:.1f}°C</b>. '
         f'PLH: <b>{plh} días</b> ({", ".join(ms) if ms else "N/D"}). ')
    riesgo = [(m.get('mes',''),m.get('prob_helada_mensual',0)) for m in por_mes if m.get('prob_helada_mensual',0)>0.1]
    if riesgo:
        t += f'Meses con P>10%: {", ".join(f"{m} ({p*100:.0f}%)" for m,p in riesgo)}. '
    tardias = [m for m in por_mes if m.get('mes','') in ('Sep','Oct','Nov') and m.get('prob_helada_mensual',0)>0.05]
    if tardias:
        t += '<b>ATENCIÓN: heladas tardías Sep-Nov detectadas</b>, riesgo para floración frutícola. '
    if alt < 300:
        t += 'Heladas predominantemente radiativas (controlables con aspersión/ventiladores). '
    else:
        t += 'Heladas radiativas y advectivas posibles a esta altitud. '
    return t


def _relato_precip(precip, loc):
    pp = precip.get('anual_mm',0)
    ms = precip.get('mensual_mm',[0]*12)
    mi = ms.index(max(ms)) if ms else 6
    mx = max(ms) if ms else 0
    conc = mx/pp*100 if pp>0 else 0
    sec = sum(1 for v in ms if v<10)
    inv = sum(ms[4:8])
    pi = inv/pp*100 if pp>0 else 0
    t = (f'Precipitación media: <b>{pp:.0f} mm/año</b>. Mes más lluvioso: '
         f'<b>{MESES[mi]}</b> ({mx:.0f} mm, {conc:.0f}% del total). '
         f'Mayo-agosto: {pi:.0f}% del total. {sec} meses secos (<10 mm). ')
    if sec >= 6: t += 'Período seco prolongado, riego indispensable. '
    return t


def _relato_megasequia(mega):
    p1 = mega.get('periodo_1_mm',0)
    p2 = mega.get('periodo_2_mm',0)
    c = mega.get('cambio_pct',0)
    t = f'Precipitación 1991-2005: {p1:.0f} mm → 2006-2020: {p2:.0f} mm (<b>{c:+.1f}%</b>). '
    if c < -15:
        t += ('Consistente con la megasequía de Chile central (2010-presente), '
              'la más prolongada en al menos 1.000 años (Garreaud et al., 2024). '
              'Planificar considerando la tendencia actual como "nuevo normal".')
    elif c < -5: t += 'Tendencia moderada a la baja, monitorear.'
    else: t += 'Precipitación estable entre períodos.'
    return t


def _relato_balance(balance, precip):
    d = balance.get('deficit_anual_mm',0)
    me = balance.get('meses_estres',0)
    pp = precip.get('anual_mm',0)
    t = ('Balance hídrico: precipitación vs ETP (Penman-Monteith FAO-56 simplificado). ')
    if d > 0:
        rd = d/pp*100 if pp>0 else 0
        t += (f'Déficit anual: <b>{d:.0f} mm</b> ({rd:.0f}% de la precipitación). '
              f'<b>{me} meses</b> con estrés hídrico significativo. ')
        if me >= 6: t += 'Riego necesario ≥6 meses. Evaluar derechos DGA y riego tecnificado. '
        elif me >= 3: t += 'Riego complementario en verano requerido. '
    return t


def _relato_agro(agro, heladas, loc):
    c = agro.get('comuna_match',loc)
    ha = agro.get('total_ha',0)
    top = agro.get('top_especies',[])
    hal = agro.get('heladas_agronomicas',[])
    t = f'La comuna de <b>{c}</b> registra <b>{ha:.0f} ha</b> frutícolas (CIREN/ODEPA). '
    if top:
        t += 'Principales: ' + ', '.join(f'{e[0]} ({e[1]:.0f} ha)' for e in top[:5]) + '. '
    alto = [h for h in hal if h.get('riesgo') in ('MUY ALTO','ALTO')]
    if alto:
        t += f'<b>{len(alto)} especies con riesgo ALTO/MUY ALTO</b> de daño por helada: '
        t += ', '.join(h['especie'] for h in alto[:5]) + '. '
        t += 'Se recomienda protección activa (aspersión, calefactores) o seguros agrícolas. '
    return t


def _metodologia(has_meteo):
    t = ('Este informe integra múltiples fuentes de datos para un diagnóstico agroclimático integral:<br/><br/>'
         '<b>Precipitación:</b> CR2MET v2.0 (0.05°, 1979-2020), validado con 879 estaciones CR2 (IDW, w=1/d²). '
         'Climatología OMM 1991-2020.<br/><br/>'
         '<b>Temperatura y heladas:</b> CR2MET Tmin v2.0. P(helada) = frecuencia Tmin<0°C. '
         'P(mensual) = 1-(1-p_diaria)^30. PLH = meses consecutivos con P<5%.<br/><br/>')
    if has_meteo:
        t += ('<b>Datos horarios:</b> PVsyst TMY Meteonorm 8.2 (8.760 horas). '
              'Procesamiento según metodología Santibáñez (2017): 18 variables mensuales, '
              'índices de Winkler, Huglin, fototérmico, noches frías.<br/><br/>'
              '<b>Horas de frío:</b> Modelo clásico (T<7°C) y Porciones de Frío Dinámicas '
              '(Fishman et al., 1987) para evaluación de vernalización.<br/><br/>')
    t += ('<b>ENSO:</b> ONI (NOAA CPC), PDO (Mantua 1997), SOI (Trenberth 1984). '
          'Pronóstico multi-método: ENSO directo, tendencia lineal, blend.<br/><br/>'
          '<b>Riesgo agronómico:</b> Umbrales INIA, Atlas Agroclimático, UC Davis. '
          'Catastro CIREN/ODEPA.<br/><br/>'
          '<b>Balance hídrico:</b> ETP con curva latitudinal calibrada FAO-56.<br/><br/>'
          '<b>Limitaciones:</b> Resolución espacial ~5.5 km. Predicciones con incertidumbre inherente. '
          'Para riego usar estación local.')
    return t
