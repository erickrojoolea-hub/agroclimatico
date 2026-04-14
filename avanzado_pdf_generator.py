"""
avanzado_pdf_generator.py — Generador de Informe Meteorológico Avanzado (PDF)
==============================================================================
Informe profesional con relato analítico, metodología, fuentes,
riesgos cuantificados y recomendaciones para agricultores.

Basado en: CR2MET v2.0, CR2 Estaciones, ENSO/PDO/SOI,
Catastro Frutícola CIREN/ODEPA, CHIRPS v2.

Estilo: Fernando Santibáñez / Atlas Agroclimático de Chile.
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
from reportlab.graphics.shapes import Drawing, Rect, String

# ── Paleta de colores ─────────────────────────────────────────────────
VERDE_OSCURO = colors.HexColor('#1B5E20')
VERDE_MEDIO = colors.HexColor('#2E7D32')
VERDE_CLARO = colors.HexColor('#C8E6C9')
VERDE_FONDO = colors.HexColor('#E8F5E9')
AZUL = colors.HexColor('#1565C0')
AZUL_CLARO = colors.HexColor('#E3F2FD')
ROJO = colors.HexColor('#C62828')
ROJO_CLARO = colors.HexColor('#FFEBEE')
NARANJA = colors.HexColor('#E65100')
NARANJA_CLARO = colors.HexColor('#FFF3E0')
AMARILLO = colors.HexColor('#F9A825')
GRIS = colors.HexColor('#F5F5F5')
GRIS_OSCURO = colors.HexColor('#424242')
GRIS_MEDIO = colors.HexColor('#9E9E9E')
BLANCO = colors.white
NEGRO = colors.black

MESES = ['Ene', 'Feb', 'Mar', 'Abr', 'May', 'Jun',
         'Jul', 'Ago', 'Sep', 'Oct', 'Nov', 'Dic']


def _create_styles():
    """Crea estilos tipográficos del informe."""
    styles = getSampleStyleSheet()

    custom = {
        'Portada': ParagraphStyle(
            'Portada', parent=styles['Title'],
            fontSize=26, leading=32, textColor=VERDE_OSCURO,
            alignment=TA_LEFT, spaceAfter=6,
        ),
        'Subtitulo': ParagraphStyle(
            'Subtitulo', parent=styles['Normal'],
            fontSize=13, leading=16, textColor=GRIS_OSCURO,
            alignment=TA_LEFT, spaceAfter=4,
        ),
        'Seccion': ParagraphStyle(
            'Seccion', parent=styles['Heading1'],
            fontSize=15, leading=18, textColor=VERDE_OSCURO,
            spaceBefore=14, spaceAfter=6,
            borderWidth=0, borderPadding=0,
        ),
        'SubSeccion': ParagraphStyle(
            'SubSeccion', parent=styles['Heading2'],
            fontSize=12, leading=15, textColor=VERDE_MEDIO,
            spaceBefore=8, spaceAfter=4,
        ),
        'Cuerpo': ParagraphStyle(
            'Cuerpo', parent=styles['Normal'],
            fontSize=9.5, leading=13, textColor=GRIS_OSCURO,
            alignment=TA_JUSTIFY, spaceAfter=6,
        ),
        'CuerpoNegrita': ParagraphStyle(
            'CuerpoNegrita', parent=styles['Normal'],
            fontSize=9.5, leading=13, textColor=NEGRO,
            alignment=TA_JUSTIFY, spaceAfter=6,
            fontName='Helvetica-Bold',
        ),
        'Alerta': ParagraphStyle(
            'Alerta', parent=styles['Normal'],
            fontSize=9.5, leading=13, textColor=ROJO,
            alignment=TA_LEFT, spaceAfter=4,
            fontName='Helvetica-Bold',
        ),
        'AlertaModerada': ParagraphStyle(
            'AlertaModerada', parent=styles['Normal'],
            fontSize=9.5, leading=13, textColor=NARANJA,
            alignment=TA_LEFT, spaceAfter=4,
            fontName='Helvetica-Bold',
        ),
        'Positivo': ParagraphStyle(
            'Positivo', parent=styles['Normal'],
            fontSize=9.5, leading=13, textColor=VERDE_MEDIO,
            alignment=TA_LEFT, spaceAfter=4,
            fontName='Helvetica-Bold',
        ),
        'Fuente': ParagraphStyle(
            'Fuente', parent=styles['Normal'],
            fontSize=7.5, leading=10, textColor=GRIS_MEDIO,
            alignment=TA_LEFT, spaceAfter=2,
        ),
        'Pie': ParagraphStyle(
            'Pie', parent=styles['Normal'],
            fontSize=7, leading=9, textColor=GRIS_MEDIO,
            alignment=TA_CENTER,
        ),
        'KPI': ParagraphStyle(
            'KPI', parent=styles['Normal'],
            fontSize=20, leading=24, textColor=VERDE_OSCURO,
            alignment=TA_CENTER, fontName='Helvetica-Bold',
        ),
        'KPILabel': ParagraphStyle(
            'KPILabel', parent=styles['Normal'],
            fontSize=8, leading=10, textColor=GRIS_OSCURO,
            alignment=TA_CENTER,
        ),
    }
    return {**{k: styles[k] for k in styles.byName}, **custom}


def _make_kpi_table(kpis):
    """
    Genera tabla de KPIs visuales.
    kpis: list of (valor, unidad, label)
    """
    row_vals = []
    row_labels = []
    for val, unit, label in kpis:
        row_vals.append(f'{val}{unit}')
        row_labels.append(label)

    n = len(kpis)
    col_w = 460 / n

    data = [row_vals, row_labels]
    t = Table(data, colWidths=[col_w] * n, rowHeights=[28, 16])
    t.setStyle(TableStyle([
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 16),
        ('TEXTCOLOR', (0, 0), (-1, 0), VERDE_OSCURO),
        ('FONTSIZE', (0, 1), (-1, 1), 7.5),
        ('TEXTCOLOR', (0, 1), (-1, 1), GRIS_OSCURO),
        ('BACKGROUND', (0, 0), (-1, -1), VERDE_FONDO),
        ('BOX', (0, 0), (-1, -1), 0.5, VERDE_CLARO),
        ('LINEBELOW', (0, 0), (-1, 0), 0.3, VERDE_CLARO),
    ]))
    return t


def _make_alert_box(text, nivel='info'):
    """Genera caja de alerta coloreada."""
    color_map = {
        'rojo': (ROJO_CLARO, ROJO),
        'naranja': (NARANJA_CLARO, NARANJA),
        'verde': (VERDE_FONDO, VERDE_MEDIO),
        'azul': (AZUL_CLARO, AZUL),
        'info': (GRIS, GRIS_OSCURO),
    }
    bg, fg = color_map.get(nivel, color_map['info'])
    data = [[Paragraph(text, ParagraphStyle('alert_inner', fontSize=9, leading=12,
                                             textColor=fg, fontName='Helvetica-Bold'))]]
    t = Table(data, colWidths=[460])
    t.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, -1), bg),
        ('BOX', (0, 0), (-1, -1), 1, fg),
        ('TOPPADDING', (0, 0), (-1, -1), 6),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
        ('LEFTPADDING', (0, 0), (-1, -1), 10),
        ('RIGHTPADDING', (0, 0), (-1, -1), 10),
    ]))
    return t


def _plot_precip_chart(precip_data):
    """Genera gráfico de precipitación mensual como imagen."""
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt

        fig, ax = plt.subplots(figsize=(6.5, 2.5))
        mensual = precip_data.get('mensual_mm', [0]*12)

        bars = ax.bar(MESES, mensual, color='#2E7D32', alpha=0.85, edgecolor='#1B5E20', linewidth=0.5)

        # Destacar meses más lluviosos
        max_val = max(mensual) if mensual else 0
        for bar, val in zip(bars, mensual):
            if val > max_val * 0.8:
                bar.set_color('#1565C0')
            if val > 0:
                ax.text(bar.get_x() + bar.get_width()/2, val + max_val*0.02,
                       f'{val:.0f}', ha='center', va='bottom', fontsize=6.5, color='#424242')

        ax.set_ylabel('Precipitación (mm)', fontsize=8)
        ax.set_title(f'Climatología Mensual — {precip_data.get("anual_mm", 0):.0f} mm/año',
                    fontsize=9, fontweight='bold', color='#1B5E20')
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.tick_params(labelsize=7)
        fig.tight_layout()

        buf = io.BytesIO()
        fig.savefig(buf, format='png', dpi=150, bbox_inches='tight')
        plt.close(fig)
        buf.seek(0)
        return Image(buf, width=440, height=170)
    except Exception:
        return None


def _plot_heladas_chart(heladas_data):
    """Genera gráfico de probabilidad de heladas por mes."""
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt

        por_mes = heladas_data.get('por_mes', [])
        if not por_mes:
            return None

        meses_h = [m.get('mes', '') for m in por_mes]
        probs = [m.get('prob_helada_mensual', 0) * 100 for m in por_mes]
        tmin = [m.get('tmin_media_C', m.get('tmin_min_abs_C', 0)) for m in por_mes]

        fig, ax1 = plt.subplots(figsize=(6.5, 2.5))

        # Barras de probabilidad
        bar_colors = ['#C62828' if p > 50 else '#E65100' if p > 20 else '#F9A825' if p > 5 else '#2E7D32'
                      for p in probs]
        bars = ax1.bar(meses_h, probs, color=bar_colors, alpha=0.8, edgecolor='white', linewidth=0.5)
        ax1.set_ylabel('Prob. helada (%)', fontsize=8, color='#C62828')
        ax1.set_ylim(0, max(max(probs) * 1.2, 10))

        for bar, val in zip(bars, probs):
            if val > 1:
                ax1.text(bar.get_x() + bar.get_width()/2, val + 1,
                       f'{val:.0f}%', ha='center', va='bottom', fontsize=6, color='#424242')

        # Línea de Tmin
        ax2 = ax1.twinx()
        ax2.plot(meses_h, tmin, '--', linewidth=1.2, marker='o', markersize=3, color='#1565C0')
        ax2.set_ylabel('Tmin media (°C)', fontsize=8, color='#1565C0')
        ax2.axhline(y=0, color='#C62828', linewidth=0.8, linestyle=':')

        ax1.set_title('Probabilidad de Heladas y Temperatura Mínima', fontsize=9,
                     fontweight='bold', color='#1B5E20')
        ax1.spines['top'].set_visible(False)
        ax1.tick_params(labelsize=7)
        ax2.tick_params(labelsize=7)
        fig.tight_layout()

        buf = io.BytesIO()
        fig.savefig(buf, format='png', dpi=150, bbox_inches='tight')
        plt.close(fig)
        buf.seek(0)
        return Image(buf, width=440, height=170)
    except Exception:
        return None


def _plot_balance_hidrico(balance_data):
    """Genera gráfico de balance hídrico."""
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        import numpy as np

        pp = balance_data.get('precipitacion_mm', [0]*12)
        etp = balance_data.get('etp_mm', [0]*12)

        fig, ax = plt.subplots(figsize=(6.5, 2.5))
        x = np.arange(12)
        w = 0.35

        ax.bar(x - w/2, pp, w, label='Precipitación', color='#1565C0', alpha=0.8)
        ax.bar(x + w/2, etp, w, label='ETP', color='#E65100', alpha=0.8)

        # Zona de déficit
        for i in range(12):
            if etp[i] > pp[i]:
                ax.annotate('', xy=(i, pp[i]), xytext=(i, etp[i]),
                           arrowprops=dict(arrowstyle='<->', color='#C62828', lw=0.8))

        ax.set_xticks(x)
        ax.set_xticklabels(MESES, fontsize=7)
        ax.set_ylabel('mm', fontsize=8)
        ax.set_title('Balance Hídrico Simplificado (P vs ETP)', fontsize=9,
                    fontweight='bold', color='#1B5E20')
        ax.legend(fontsize=7, loc='upper right')
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        fig.tight_layout()

        buf = io.BytesIO()
        fig.savefig(buf, format='png', dpi=150, bbox_inches='tight')
        plt.close(fig)
        buf.seek(0)
        return Image(buf, width=440, height=170)
    except Exception:
        return None


def generate_avanzado_pdf(localidad, lat, lon, alt, avanzado_report):
    """
    Genera el Informe Meteorológico Avanzado en PDF.

    Parámetros:
        localidad: nombre del lugar
        lat, lon: coordenadas
        alt: altitud (m)
        avanzado_report: dict retornado por generar_informe_avanzado()

    Retorna: bytes del PDF
    """
    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=letter,
        leftMargin=2.2*cm, rightMargin=2.2*cm,
        topMargin=2*cm, bottomMargin=2*cm,
    )

    S = _create_styles()
    elements = []

    secciones = avanzado_report.get('secciones', {})
    precip = secciones.get('precipitacion', {})
    heladas = secciones.get('heladas', {})
    enso = secciones.get('enso', {})
    agro = secciones.get('contexto_agro', {})
    balance = secciones.get('balance_hidrico', {})
    pronostico = secciones.get('pronostico', {})
    resumen = secciones.get('resumen', {})
    monitoreo = secciones.get('monitoreo', {})

    fecha_gen = avanzado_report.get('fecha_generacion', datetime.now().strftime('%Y-%m-%d'))
    dist_mar = avanzado_report.get('distancia_mar_km', 0)

    # ════════════════════════════════════════════════════════════════
    # PORTADA
    # ════════════════════════════════════════════════════════════════
    elements.append(Spacer(1, 40))
    elements.append(Paragraph('Informe Meteorológico Avanzado', S['Portada']))
    elements.append(Paragraph(f'{localidad}', ParagraphStyle(
        'loc', fontSize=18, leading=22, textColor=VERDE_MEDIO, spaceAfter=8)))
    elements.append(Spacer(1, 6))

    # Línea de metadata
    meta_text = (
        f'Coordenadas: {lat:.4f}°S, {abs(lon):.4f}°W  |  '
        f'Altitud: {alt:.0f} m s.n.m.  |  '
        f'Dist. al mar: {dist_mar:.0f} km  |  '
        f'Generado: {fecha_gen}'
    )
    elements.append(Paragraph(meta_text, S['Fuente']))
    elements.append(Spacer(1, 10))

    # KPIs principales
    pp_anual = precip.get('anual_mm', 0)
    dias_hel = heladas.get('dias_helada_año_promedio', 0)
    plh = heladas.get('periodo_libre_heladas_dias', 0)
    estado_enso = enso.get('estado', 'N/D')
    oni = enso.get('oni_actual', 0)

    kpis = [
        (f'{pp_anual:.0f}', ' mm', 'Precip. anual'),
        (f'{dias_hel:.0f}', ' d/año', 'Heladas'),
        (f'{plh}', ' días', 'Per. libre heladas'),
        (estado_enso, '', 'ENSO'),
        (f'{alt:.0f}', ' m', 'Altitud'),
    ]
    elements.append(_make_kpi_table(kpis))
    elements.append(Spacer(1, 10))

    # Alertas
    alertas = resumen.get('alertas', [])
    if alertas:
        for a in alertas:
            if 'ALTO' in a or 'árida' in a.lower():
                elements.append(_make_alert_box(a.replace('🔴 ', ''), 'rojo'))
            elif 'Megasequía' in a or 'moderado' in a.lower() or 'baja' in a.lower() or 'SECO' in a:
                elements.append(_make_alert_box(a.replace('🟡 ', ''), 'naranja'))
            else:
                elements.append(_make_alert_box(a.replace('🟢 ', ''), 'verde'))
            elements.append(Spacer(1, 3))

    elements.append(Spacer(1, 6))
    elements.append(HRFlowable(width='100%', thickness=1, color=VERDE_CLARO))

    # ════════════════════════════════════════════════════════════════
    # 1. RESUMEN EJECUTIVO
    # ════════════════════════════════════════════════════════════════
    elements.append(Paragraph('1. Resumen Ejecutivo', S['Seccion']))

    # Generar relato automático
    resumen_text = _generar_relato_resumen(localidad, lat, lon, alt, dist_mar,
                                            precip, heladas, enso, agro, balance, pronostico)
    elements.append(Paragraph(resumen_text, S['Cuerpo']))

    # ════════════════════════════════════════════════════════════════
    # 2. CONTEXTO ENSO Y VARIABILIDAD CLIMÁTICA
    # ════════════════════════════════════════════════════════════════
    elements.append(Paragraph('2. Contexto Climático: ENSO y Variabilidad Interanual', S['Seccion']))

    enso_text = _generar_relato_enso(enso, lat)
    elements.append(Paragraph(enso_text, S['Cuerpo']))

    if enso.get('interpretacion_agro'):
        elements.append(_make_alert_box(enso['interpretacion_agro'], 'azul'))
        elements.append(Spacer(1, 4))

    elements.append(Paragraph(
        'Fuente: NOAA CPC — Oceanic Niño Index (ONI), Pacific Decadal Oscillation (PDO), '
        'Southern Oscillation Index (SOI). Huang et al. (2017), Mantua et al. (1997).',
        S['Fuente']))

    # ════════════════════════════════════════════════════════════════
    # 3. PRECIPITACIÓN
    # ════════════════════════════════════════════════════════════════
    elements.append(PageBreak())
    elements.append(Paragraph('3. Análisis de Precipitación', S['Seccion']))

    if precip:
        precip_text = _generar_relato_precipitacion(precip, localidad, lat)
        elements.append(Paragraph(precip_text, S['Cuerpo']))

        # Gráfico
        chart = _plot_precip_chart(precip)
        if chart:
            elements.append(chart)
            elements.append(Spacer(1, 6))

        # Tabla mensual
        if precip.get('mensual_mm'):
            elements.append(Paragraph('3.1 Climatología Mensual', S['SubSeccion']))
            data = [['Mes'] + MESES + ['Anual'],
                    ['mm'] + [f'{v:.0f}' for v in precip['mensual_mm']] +
                    [f'{precip.get("anual_mm", 0):.0f}']]
            t = Table(data, colWidths=[35] + [32]*12 + [38])
            t.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), VERDE_OSCURO),
                ('TEXTCOLOR', (0, 0), (-1, 0), BLANCO),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 7.5),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('GRID', (0, 0), (-1, -1), 0.3, GRIS_MEDIO),
                ('BACKGROUND', (-1, 0), (-1, -1), VERDE_CLARO),
                ('FONTNAME', (-1, 1), (-1, 1), 'Helvetica-Bold'),
            ]))
            elements.append(t)
            elements.append(Spacer(1, 6))

        # Validación cruzada
        validacion = precip.get('validacion')
        if validacion:
            elements.append(Paragraph('3.2 Validación Cruzada CR2MET vs Estaciones', S['SubSeccion']))
            val_text = (
                f'La precipitación estimada por CR2MET ({validacion.get("cr2met_mm", 0):.0f} mm/año) '
                f'fue comparada con la interpolación IDW de estaciones pluviométricas cercanas '
                f'({validacion.get("estaciones_mm", 0):.0f} mm/año). '
                f'La razón CR2MET/Estaciones es {validacion.get("ratio", 0):.2f}, '
                f'lo que indica una concordancia <b>{validacion.get("concordancia", "N/D")}</b>. '
            )
            if validacion.get('estaciones_usadas'):
                val_text += f'Estaciones utilizadas: {", ".join(validacion["estaciones_usadas"][:3])}.'
            elements.append(Paragraph(val_text, S['Cuerpo']))

        # Megasequía
        mega = precip.get('megasequia')
        if mega:
            elements.append(Paragraph('3.3 Tendencia de Megasequía', S['SubSeccion']))
            mega_text = _generar_relato_megasequia(mega, localidad)
            elements.append(Paragraph(mega_text, S['Cuerpo']))

            cambio = mega.get('cambio_pct', 0)
            if cambio < -15:
                elements.append(_make_alert_box(
                    f'DÉFICIT SIGNIFICATIVO: La precipitación del período 2006-2020 fue un '
                    f'{abs(cambio):.0f}% inferior al período 1991-2005. '
                    f'Este punto se encuentra en zona de megasequía activa.',
                    'rojo'))
            elif cambio < -5:
                elements.append(_make_alert_box(
                    f'Déficit moderado: reducción del {abs(cambio):.0f}% respecto al período base.',
                    'naranja'))

        # Extremos
        extremos = precip.get('extremos') or precip.get('extremos_diarios')
        if extremos:
            elements.append(Paragraph('3.4 Eventos Extremos de Precipitación', S['SubSeccion']))
            ext_text = (
                f'En el período climatológico, el percentil 95 de precipitación diaria es '
                f'<b>{extremos.get("p95_mm", 0):.0f} mm</b> y el percentil 99 alcanza '
                f'<b>{extremos.get("p99_mm", 0):.0f} mm</b>. El máximo diario registrado fue '
                f'<b>{extremos.get("max_diario_mm", 0):.0f} mm</b>. '
                f'Se registran en promedio <b>{extremos.get("dias_lluvia_por_año", 0):.0f} días '
                f'de lluvia por año</b> (>1 mm/día).'
            )
            elements.append(Paragraph(ext_text, S['Cuerpo']))

        elements.append(Paragraph(
            f'Fuente: {precip.get("fuente", "CR2MET v2.0")}. '
            'Ref: Boisier et al. (2018). CR2MET. Centro de Ciencia del Clima, U. de Chile.',
            S['Fuente']))

    # ════════════════════════════════════════════════════════════════
    # 4. HELADAS Y TEMPERATURA MÍNIMA
    # ════════════════════════════════════════════════════════════════
    elements.append(Paragraph('4. Análisis de Heladas y Temperatura Mínima', S['Seccion']))

    if heladas:
        heladas_text = _generar_relato_heladas(heladas, localidad, alt, enso)
        elements.append(Paragraph(heladas_text, S['Cuerpo']))

        # Gráfico
        chart = _plot_heladas_chart(heladas)
        if chart:
            elements.append(chart)
            elements.append(Spacer(1, 6))

        # Tabla mensual de heladas
        por_mes = heladas.get('por_mes', [])
        if por_mes:
            elements.append(Paragraph('4.1 Estadísticas Mensuales de Helada', S['SubSeccion']))
            headers = ['Mes', 'Tmin media', 'Tmin abs.', 'P(helada/mes)', 'Días helada/año']
            rows = [headers]
            for m in por_mes:
                prob = m.get('prob_helada_mensual', 0)
                rows.append([
                    m.get('mes', ''),
                    f'{m.get("tmin_media_C", 0):.1f} °C',
                    f'{m.get("tmin_minima_abs_C", m.get("tmin_min_abs_C", 0)):.1f} °C',
                    f'{prob*100:.0f}%',
                    f'{m.get("dias_helada_por_año", m.get("dias_helada_año", 0)):.1f}',
                ])

            t = Table(rows, colWidths=[45, 70, 70, 85, 85])
            style_cmds = [
                ('BACKGROUND', (0, 0), (-1, 0), VERDE_OSCURO),
                ('TEXTCOLOR', (0, 0), (-1, 0), BLANCO),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 8),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('GRID', (0, 0), (-1, -1), 0.3, GRIS_MEDIO),
            ]
            # Colorear filas con helada significativa
            for i, m in enumerate(por_mes):
                prob = m.get('prob_helada_mensual', 0)
                if prob > 0.5:
                    style_cmds.append(('BACKGROUND', (0, i+1), (-1, i+1), ROJO_CLARO))
                elif prob > 0.2:
                    style_cmds.append(('BACKGROUND', (0, i+1), (-1, i+1), NARANJA_CLARO))
                elif prob > 0.05:
                    style_cmds.append(('BACKGROUND', (0, i+1), (-1, i+1), colors.HexColor('#FFF8E1')))

            t.setStyle(TableStyle(style_cmds))
            elements.append(t)
            elements.append(Spacer(1, 6))

        # Riesgo ENSO
        efecto = heladas.get('efecto_enso')
        if efecto:
            elements.append(Paragraph('4.2 Efecto ENSO sobre Heladas', S['SubSeccion']))
            elements.append(Paragraph(efecto, S['Cuerpo']))

        elements.append(Paragraph(
            f'Fuente: {heladas.get("fuente", "CR2MET Tmin v2.0")}. '
            'Metodología: frecuencia observada de Tmin < 0°C en datos diarios, '
            'período 1991-2020, resolución ~5.5 km. P(mensual) = 1-(1-p_diaria)^30.',
            S['Fuente']))

    # ════════════════════════════════════════════════════════════════
    # 5. RIESGO AGRONÓMICO POR ESPECIE
    # ════════════════════════════════════════════════════════════════
    if agro and agro.get('heladas_agronomicas'):
        elements.append(PageBreak())
        elements.append(Paragraph('5. Riesgo Agronómico de Heladas por Especie', S['Seccion']))

        agro_text = _generar_relato_agro(agro, heladas, localidad)
        elements.append(Paragraph(agro_text, S['Cuerpo']))

        # Tabla de riesgo por especie
        ha_list = agro['heladas_agronomicas']
        headers = ['Especie', 'Sup. (ha)', 'Umbral (°C)', 'P(daño Sep)', 'P(daño Oct)', 'Riesgo']
        rows = [headers]
        for h in ha_list[:15]:  # top 15
            rows.append([
                h.get('especie', ''),
                f'{h.get("superficie_ha", 0):.0f}',
                f'{h.get("umbral_floracion", 0):.1f}',
                f'{h.get("p_dano_sep", 0):.0f}%',
                f'{h.get("p_dano_oct", 0):.0f}%',
                h.get('riesgo', ''),
            ])

        t = Table(rows, colWidths=[95, 55, 65, 65, 65, 65])
        style_cmds = [
            ('BACKGROUND', (0, 0), (-1, 0), VERDE_OSCURO),
            ('TEXTCOLOR', (0, 0), (-1, 0), BLANCO),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 8),
            ('ALIGN', (1, 0), (-1, -1), 'CENTER'),
            ('ALIGN', (0, 0), (0, -1), 'LEFT'),
            ('GRID', (0, 0), (-1, -1), 0.3, GRIS_MEDIO),
        ]
        for i, h in enumerate(ha_list[:15]):
            riesgo = h.get('riesgo', '')
            if riesgo == 'MUY ALTO':
                style_cmds.append(('BACKGROUND', (-1, i+1), (-1, i+1), ROJO_CLARO))
                style_cmds.append(('TEXTCOLOR', (-1, i+1), (-1, i+1), ROJO))
            elif riesgo == 'ALTO':
                style_cmds.append(('BACKGROUND', (-1, i+1), (-1, i+1), NARANJA_CLARO))
                style_cmds.append(('TEXTCOLOR', (-1, i+1), (-1, i+1), NARANJA))

        t.setStyle(TableStyle(style_cmds))
        elements.append(t)
        elements.append(Spacer(1, 6))

        elements.append(Paragraph(
            'Fuente: Catastro Frutícola Nacional (CIREN/ODEPA). '
            'Umbrales de daño: INIA Boletín Técnico, Atlas Agroclimático (Santibáñez 2017), '
            'UC Davis Fruit & Nut Research.',
            S['Fuente']))

    # ════════════════════════════════════════════════════════════════
    # 6. BALANCE HÍDRICO
    # ════════════════════════════════════════════════════════════════
    if balance:
        elements.append(Paragraph('6. Balance Hídrico Simplificado', S['Seccion']))

        balance_text = _generar_relato_balance(balance, localidad, precip)
        elements.append(Paragraph(balance_text, S['Cuerpo']))

        chart = _plot_balance_hidrico(balance)
        if chart:
            elements.append(chart)
            elements.append(Spacer(1, 6))

        # Tabla mensual
        if balance.get('precipitacion_mm') and balance.get('etp_mm'):
            pp_m = balance['precipitacion_mm']
            etp_m = balance['etp_mm']
            bal_m = balance.get('balance_mm', [pp_m[i] - etp_m[i] for i in range(12)])

            data = [
                [''] + MESES,
                ['P (mm)'] + [f'{v:.0f}' for v in pp_m],
                ['ETP (mm)'] + [f'{v:.0f}' for v in etp_m],
                ['Balance'] + [f'{v:+.0f}' for v in bal_m],
            ]
            t = Table(data, colWidths=[50] + [33]*12)
            style_cmds = [
                ('BACKGROUND', (0, 0), (-1, 0), VERDE_OSCURO),
                ('TEXTCOLOR', (0, 0), (-1, 0), BLANCO),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 7.5),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('GRID', (0, 0), (-1, -1), 0.3, GRIS_MEDIO),
            ]
            for i in range(12):
                if bal_m[i] < -20:
                    style_cmds.append(('BACKGROUND', (i+1, 3), (i+1, 3), ROJO_CLARO))
                elif bal_m[i] < 0:
                    style_cmds.append(('BACKGROUND', (i+1, 3), (i+1, 3), NARANJA_CLARO))
                else:
                    style_cmds.append(('BACKGROUND', (i+1, 3), (i+1, 3), VERDE_FONDO))

            t.setStyle(TableStyle(style_cmds))
            elements.append(t)

        elements.append(Spacer(1, 4))
        elements.append(Paragraph(
            'Nota: ETP estimada con curva latitudinal simplificada (Penman-Monteith FAO-56 adaptado). '
            'Para cálculos de riego, usar ETo local con datos de estación meteorológica.',
            S['Fuente']))

    # ════════════════════════════════════════════════════════════════
    # 7. PRONÓSTICO ESTACIONAL
    # ════════════════════════════════════════════════════════════════
    if pronostico:
        elements.append(Paragraph('7. Pronóstico Estacional', S['Seccion']))

        pron_text = _generar_relato_pronostico(pronostico, enso, localidad, precip)
        elements.append(Paragraph(pron_text, S['Cuerpo']))

        outlook = pronostico.get('outlook', 'NORMAL')
        rango = pronostico.get('rango_mm', (0, 0))
        pe = pronostico.get('precip_esperada_mm', 0)

        nivel = 'rojo' if outlook == 'SECO' else 'verde' if outlook == 'LLUVIOSO' else 'azul'
        elements.append(_make_alert_box(
            f'Outlook: {outlook} — Precipitación esperada: {pe:.0f} mm '
            f'(rango: {rango[0]:.0f} – {rango[1]:.0f} mm)',
            nivel))

        elements.append(Spacer(1, 4))
        nota = pronostico.get('nota', '')
        if nota:
            elements.append(Paragraph(f'Nota: {nota}', S['Fuente']))

    # ════════════════════════════════════════════════════════════════
    # 8. METODOLOGÍA Y FUENTES
    # ════════════════════════════════════════════════════════════════
    elements.append(PageBreak())
    elements.append(Paragraph('8. Metodología y Fuentes de Datos', S['Seccion']))

    metodo_text = _generar_seccion_metodologia(precip, heladas)
    elements.append(Paragraph(metodo_text, S['Cuerpo']))

    elements.append(Paragraph('8.1 Referencias Bibliográficas', S['SubSeccion']))
    refs = [
        'Boisier, J.P. et al. (2018). CR2MET: Productos grillados de precipitación y temperatura. '
        'Centro de Ciencia del Clima y la Resiliencia (CR)², Universidad de Chile.',
        'Santibáñez, F. (2017). Atlas Agroclimático de Chile. Estado actual y tendencias del clima. '
        'Tomo I-IV. CIREN / FIA.',
        'Huang, B. et al. (2017). Extended Reconstructed Sea Surface Temperature (ERSST), Version 5. '
        'NOAA National Centers for Environmental Information.',
        'Mantua, N.J. et al. (1997). A Pacific Interdecadal Climate Oscillation. '
        'Bulletin of the American Meteorological Society, 78(6), 1069-1079.',
        'Allen, R.G. et al. (1998). Crop evapotranspiration: Guidelines for computing crop water '
        'requirements. FAO Irrigation and Drainage Paper 56.',
        'Garreaud, R. et al. (2024). The Central Chile Mega Drought (2010-2023). '
        'Earth\'s Future, 12(1).',
        'CIREN/ODEPA (2024). Catastro Frutícola Nacional. Gobierno de Chile.',
    ]
    for i, ref in enumerate(refs):
        elements.append(Paragraph(f'[{i+1}] {ref}', S['Fuente']))
        elements.append(Spacer(1, 2))

    # Disclaimer
    elements.append(Spacer(1, 12))
    elements.append(HRFlowable(width='100%', thickness=0.5, color=GRIS_MEDIO))
    elements.append(Spacer(1, 4))
    elements.append(Paragraph(
        '<b>Aviso legal:</b> Este informe es generado automáticamente con fines informativos '
        'y de planificación agrícola. Los datos provienen de fuentes públicas y modelos climáticos '
        'con resolución limitada (~5.5 km). Para decisiones de inversión críticas, se recomienda '
        'complementar con estación meteorológica local y asesoría agronómica profesional. '
        'Las proyecciones estacionales tienen incertidumbre inherente.',
        S['Fuente']))

    elements.append(Spacer(1, 6))
    elements.append(Paragraph(
        f'Informe generado el {fecha_gen} | Visor Agroclimático v1.1 | '
        'Motor: CR2MET v2.0 + CR2 Estaciones + ENSO/PDO/SOI',
        S['Pie']))

    # Build
    doc.build(elements)
    return buf.getvalue()


# ══════════════════════════════════════════════════════════════════════
# GENERADORES DE RELATO
# ══════════════════════════════════════════════════════════════════════

def _generar_relato_resumen(localidad, lat, lon, alt, dist_mar,
                             precip, heladas, enso, agro, balance, pronostico):
    """Genera párrafo de resumen ejecutivo narrativo."""
    parts = []
    parts.append(
        f'El predio analizado se ubica en <b>{localidad}</b> '
        f'({abs(lat):.2f}°S, {abs(lon):.2f}°W), a una altitud de <b>{alt:.0f} m s.n.m.</b>'
    )
    if dist_mar:
        parts.append(f' y a {dist_mar:.0f} km de la costa')
    parts.append('. ')

    # Precipitación
    pp = precip.get('anual_mm', 0)
    if pp > 0:
        if pp < 200:
            clima = 'árido'
        elif pp < 400:
            clima = 'semiárido'
        elif pp < 800:
            clima = 'mediterráneo'
        else:
            clima = 'templado lluvioso'
        parts.append(
            f'El régimen pluviométrico es <b>{clima}</b>, con una precipitación media anual de '
            f'<b>{pp:.0f} mm</b>, concentrada principalmente entre mayo y agosto. '
        )

    # Megasequía
    mega = precip.get('megasequia', {})
    cambio = mega.get('cambio_pct', 0)
    if cambio < -10:
        parts.append(
            f'Se observa un <b>déficit del {abs(cambio):.0f}%</b> en el período 2006-2020 respecto '
            f'a 1991-2005, consistente con la megasequía que afecta Chile central. '
        )

    # Heladas
    dias_hel = heladas.get('dias_helada_año_promedio', 0)
    plh = heladas.get('periodo_libre_heladas_dias', 0)
    if dias_hel > 0:
        if dias_hel > 20:
            nivel_h = 'elevado'
        elif dias_hel > 5:
            nivel_h = 'moderado'
        else:
            nivel_h = 'bajo'
        parts.append(
            f'El riesgo de heladas es <b>{nivel_h}</b>, con un promedio de '
            f'<b>{dias_hel:.0f} días de helada por año</b> y un período libre de heladas de '
            f'<b>{plh} días</b>. '
        )

    # ENSO
    estado = enso.get('estado', '')
    if estado and estado != 'No disponible':
        oni = enso.get('oni_actual', 0)
        parts.append(
            f'El estado actual del ENSO es <b>{estado}</b> (ONI={oni:+.2f}), '
        )
        if estado == 'El Niño':
            parts.append('lo que sugiere un invierno potencialmente más lluvioso que lo normal. ')
        elif estado == 'La Niña':
            parts.append('lo que implica mayor riesgo de sequía y heladas tardías. ')
        else:
            parts.append('indicando condiciones cercanas al promedio climático. ')

    # Agro
    if agro and agro.get('total_ha'):
        parts.append(
            f'La comuna registra <b>{agro["total_ha"]:.0f} ha</b> de superficie frutícola. '
        )

    # Balance
    if balance:
        deficit = balance.get('deficit_anual_mm', 0)
        meses_estres = balance.get('meses_estres', 0)
        if deficit > 0:
            parts.append(
                f'El déficit hídrico anual estimado es de <b>{deficit:.0f} mm</b>, '
                f'con <b>{meses_estres} meses</b> de estrés hídrico significativo, '
                f'lo que hace del riego un componente esencial para la producción agrícola. '
            )

    return ''.join(parts)


def _generar_relato_enso(enso, lat):
    """Genera texto sobre el estado ENSO y su implicancia."""
    if not enso or enso.get('estado') == 'No disponible':
        return 'No se dispone de información actualizada sobre el estado ENSO.'

    estado = enso.get('estado', 'Neutro')
    oni = enso.get('oni_actual', 0)
    oni_3m = enso.get('oni_3m', 0)
    tendencia = enso.get('tendencia', 'estable')
    pdo = enso.get('pdo_3m', 0)
    soi = enso.get('soi_3m', 0)

    text = (
        f'El Índice Oceánico del Niño (ONI) registra un valor de <b>{oni:+.2f}</b>, '
        f'con un promedio trimestral de <b>{oni_3m:+.2f}</b>. '
        f'El estado actual del ENSO se clasifica como <b>{estado}</b>, '
        f'con tendencia <b>{tendencia}</b>. '
    )

    text += (
        f'El Índice de Oscilación Decadal del Pacífico (PDO) se encuentra en <b>{pdo:+.2f}</b> '
        f'y el Índice de Oscilación del Sur (SOI) en <b>{soi:+.2f}</b>. '
    )

    # Implicancia para Chile central
    if -38 < lat < -28:
        text += (
            'Para Chile central, la fase ENSO tiene una correlación significativa con la '
            'precipitación invernal: El Niño tiende a producir inviernos más lluviosos (+20-40%), '
            'mientras que La Niña se asocia con sequía invernal y mayor riesgo de heladas tardías '
            'durante la floración (septiembre-noviembre). '
        )
    elif lat <= -38:
        text += (
            'En la zona sur de Chile, la señal ENSO es más débil, pero La Niña tiende '
            'a intensificar los sistemas frontales y aumentar la precipitación. '
        )

    text += (
        'Sin embargo, Garreaud et al. (2024) advierten que la correlación ENSO-precipitación '
        'se ha debilitado significativamente desde el año 2000 en Chile central.'
    )

    return text


def _generar_relato_precipitacion(precip, localidad, lat):
    """Genera relato sobre el régimen de precipitación."""
    pp = precip.get('anual_mm', 0)
    mensual = precip.get('mensual_mm', [0]*12)

    # Mes más lluvioso
    max_mes_idx = mensual.index(max(mensual)) if mensual else 6
    max_mes_val = max(mensual) if mensual else 0
    concentracion = max_mes_val / pp * 100 if pp > 0 else 0

    # Meses secos (< 10mm)
    meses_secos = sum(1 for v in mensual if v < 10)
    # Meses con > 50% del total
    pp_invierno = sum(mensual[4:8])  # May-Ago
    pct_invierno = pp_invierno / pp * 100 if pp > 0 else 0

    text = (
        f'La precipitación media anual en {localidad} es de <b>{pp:.0f} mm</b>, '
        f'con un régimen marcadamente estacional. El mes más lluvioso es '
        f'<b>{MESES[max_mes_idx]}</b> ({max_mes_val:.0f} mm, {concentracion:.0f}% del total anual). '
        f'El período mayo-agosto concentra el <b>{pct_invierno:.0f}%</b> de la precipitación anual. '
        f'Se registran <b>{meses_secos} meses secos</b> (<10 mm/mes), '
    )

    if meses_secos >= 6:
        text += 'lo que configura un período seco prolongado típico del clima mediterráneo chileno. '
    elif meses_secos >= 4:
        text += 'indicando una estación seca bien definida. '
    else:
        text += 'con precipitaciones distribuidas a lo largo del año. '

    fuente = precip.get('fuente', 'CR2MET')
    text += (
        f'Estos valores provienen de {fuente}, '
        f'representando la climatología del período 1991-2020.'
    )

    return text


def _generar_relato_megasequia(mega, localidad):
    """Genera relato sobre la tendencia de megasequía."""
    p1 = mega.get('periodo_1_mm', 0)
    p2 = mega.get('periodo_2_mm', 0)
    cambio = mega.get('cambio_pct', 0)
    interp = mega.get('interpretacion', '')

    text = (
        f'Comparando los períodos 1991-2005 y 2006-2020, la precipitación media anual '
        f'pasó de <b>{p1:.0f} mm</b> a <b>{p2:.0f} mm</b>, representando un '
        f'<b>cambio del {cambio:+.1f}%</b>. '
    )

    if cambio < -15:
        text += (
            'Este déficit es consistente con la megasequía que afecta Chile central desde 2010, '
            'considerada la más prolongada en al menos 1,000 años (Garreaud et al., 2024). '
            'La reducción sostenida de precipitación tiene implicaciones directas sobre la '
            'disponibilidad de agua para riego, la recarga de acuíferos y la viabilidad de '
            'cultivos de secano. Se recomienda planificar considerando la tendencia actual '
            'como el "nuevo normal" para los próximos 10-20 años.'
        )
    elif cambio < -5:
        text += (
            'Se observa una tendencia moderada a la baja, que debe ser monitoreada '
            'en el contexto del cambio climático regional.'
        )
    else:
        text += 'La precipitación se mantiene relativamente estable entre ambos períodos.'

    return text


def _generar_relato_heladas(heladas, localidad, alt, enso):
    """Genera relato sobre el régimen de heladas."""
    dias = heladas.get('dias_helada_año_promedio', 0)
    plh = heladas.get('periodo_libre_heladas_dias', 0)
    tmin_abs = heladas.get('tmin_absoluta_C', 0)
    meses_sin = heladas.get('meses_sin_helada', [])

    por_mes = heladas.get('por_mes', [])
    meses_riesgo = [(m.get('mes', ''), m.get('prob_helada_mensual', 0))
                    for m in por_mes if m.get('prob_helada_mensual', 0) > 0.1]

    text = (
        f'En {localidad} ({alt:.0f} m s.n.m.) se registran en promedio '
        f'<b>{dias:.0f} días de helada por año</b> (Tmin < 0°C), '
        f'con una temperatura mínima absoluta de <b>{tmin_abs:.1f}°C</b> '
        f'en el período 1991-2020. '
    )

    text += (
        f'El período libre de heladas es de <b>{plh} días</b>, '
        f'abarcando los meses de {", ".join(meses_sin) if meses_sin else "N/D"}. '
    )

    if meses_riesgo:
        meses_r_txt = ', '.join([f'{m} ({p*100:.0f}%)' for m, p in meses_riesgo])
        text += (
            f'Los meses con probabilidad significativa de helada (>10%) son: '
            f'<b>{meses_r_txt}</b>. '
        )

    # Heladas tardías (Sep-Nov)
    heladas_tardias = [m for m in por_mes
                       if m.get('mes', '') in ('Sep', 'Oct', 'Nov')
                       and m.get('prob_helada_mensual', 0) > 0.05]
    if heladas_tardias:
        text += (
            '<b>ATENCIÓN:</b> Se detecta riesgo de heladas tardías (septiembre-noviembre), '
            'coincidentes con el período de floración de frutales de hoja caduca. '
            'Estas heladas representan el mayor riesgo agronómico ya que pueden causar '
            'pérdidas parciales o totales de la producción. '
        )

    # Tipo de helada según altitud y distancia al mar
    if alt < 300:
        text += (
            'Dada la baja altitud, las heladas son predominantemente de tipo <b>radiativo</b> '
            '(inversión térmica nocturna en noches despejadas y calmas), lo que permite '
            'estrategias de control como ventiladores, calefactores o riego por aspersión. '
        )
    else:
        text += (
            'A esta altitud, las heladas pueden ser tanto <b>radiativas</b> como <b>advectivas</b> '
            '(entrada de masas de aire polar). Las heladas advectivas son más difíciles de controlar '
            'y requieren protección pasiva (selección de sitio, cortinas cortaviento). '
        )

    return text


def _generar_relato_agro(agro, heladas, localidad):
    """Genera relato sobre el contexto agrícola."""
    comuna = agro.get('comuna_match', localidad)
    total_ha = agro.get('total_ha', 0)
    top = agro.get('top_especies', [])
    ha_list = agro.get('heladas_agronomicas', [])

    text = (
        f'La comuna de <b>{comuna}</b> registra <b>{total_ha:.0f} hectáreas</b> de '
        f'superficie frutícola según el Catastro Nacional (CIREN/ODEPA). '
    )

    if top:
        esp_text = ', '.join([f'{e[0]} ({e[1]:.0f} ha)' for e in top[:5]])
        text += f'Las principales especies son: {esp_text}. '

    # Análisis de riesgo
    alto_riesgo = [h for h in ha_list if h.get('riesgo') in ('MUY ALTO', 'ALTO')]
    if alto_riesgo:
        especies_r = ', '.join([h['especie'] for h in alto_riesgo[:5]])
        text += (
            f'<b>Se identifican {len(alto_riesgo)} especies con riesgo ALTO o MUY ALTO '
            f'de daño por helada durante floración:</b> {especies_r}. '
            'Para estas especies, se recomienda implementar sistemas activos de protección '
            '(aspersión sobre copa, calefactores, ventiladores) o considerar seguros agrícolas. '
        )

    mod_riesgo = [h for h in ha_list if h.get('riesgo') == 'MODERADO']
    if mod_riesgo:
        text += (
            f'Otras {len(mod_riesgo)} especies presentan riesgo moderado y se beneficiarían '
            'de monitoreo meteorológico y sistemas de alerta temprana. '
        )

    text += (
        'La tabla siguiente cruza los umbrales de daño por especie (INIA/UC Davis) con la '
        'probabilidad observada de helada en los meses de floración (septiembre-octubre).'
    )

    return text


def _generar_relato_balance(balance, localidad, precip):
    """Genera relato sobre el balance hídrico."""
    deficit = balance.get('deficit_anual_mm', 0)
    meses_estres = balance.get('meses_estres', 0)
    pp = precip.get('anual_mm', 0)

    text = (
        f'El balance hídrico simplificado compara la precipitación mensual con la '
        f'evapotranspiración potencial (ETP) estimada mediante una curva latitudinal '
        f'calibrada con Penman-Monteith FAO-56. '
    )

    if deficit > 0:
        ratio_deficit = deficit / pp * 100 if pp > 0 else 0
        text += (
            f'El déficit hídrico anual estimado es de <b>{deficit:.0f} mm</b>, equivalente '
            f'al <b>{ratio_deficit:.0f}%</b> de la precipitación anual. '
            f'Se identifican <b>{meses_estres} meses con estrés hídrico significativo</b> '
            f'(balance < -20 mm), correspondientes a la estación seca. '
        )

        if meses_estres >= 6:
            text += (
                'Este nivel de déficit requiere riego complementario durante al menos '
                '6 meses del año para mantener la producción agrícola. '
                'Se recomienda evaluar la disponibilidad de derechos de agua (DGA) '
                'y la factibilidad de sistemas de riego tecnificado (goteo o microaspersión). '
            )
        elif meses_estres >= 3:
            text += (
                'El déficit se concentra en verano y requiere riego complementario. '
                'Sistemas de riego eficiente pueden reducir la demanda en un 30-50%. '
            )

    return text


def _generar_relato_pronostico(pronostico, enso, localidad, precip):
    """Genera relato del pronóstico estacional."""
    outlook = pronostico.get('outlook', 'NORMAL')
    pe = pronostico.get('precip_esperada_mm', 0)
    rango = pronostico.get('rango_mm', (0, 0))
    factor = pronostico.get('factor_enso', 1.0)

    text = (
        f'Basándose en el estado actual del ENSO y la climatología del punto, '
        f'se estima una precipitación de <b>{pe:.0f} mm</b> para el año hidrológico en curso '
        f'(rango de confianza: {rango[0]:.0f} – {rango[1]:.0f} mm). '
        f'El factor ENSO aplicado es <b>{factor:.2f}</b>. '
    )

    if outlook == 'SECO':
        text += (
            'El pronóstico es <b>SECO</b>: se espera una temporada con precipitación '
            'por debajo de lo normal. Se recomienda maximizar la eficiencia de riego, '
            'reducir la superficie de secano y asegurar reservas de agua. '
        )
    elif outlook == 'LLUVIOSO':
        text += (
            'El pronóstico es <b>LLUVIOSO</b>: se espera precipitación por sobre lo normal. '
            'Se recomienda verificar drenajes, preparar accesos y considerar el riesgo '
            'de enfermedades fúngicas asociadas a exceso de humedad. '
        )
    else:
        text += (
            'El pronóstico indica condiciones <b>NORMALES</b>. Se recomienda mantener '
            'los planes de riego habituales y monitorear actualizaciones del pronóstico ENSO. '
        )

    return text


def _generar_seccion_metodologia(precip, heladas):
    """Genera la sección de metodología."""
    text = (
        'Este informe integra múltiples fuentes de datos climáticos para generar un '
        'diagnóstico agroclimático completo del punto analizado. '
        'A continuación se describen las fuentes y métodos utilizados:<br/><br/>'

        '<b>Precipitación:</b> Datos grillados CR2MET v2.0 (Boisier et al., 2018), '
        'con resolución espacial de 0.05° (~5.5 km) y período 1979-2020. '
        'La climatología se calcula sobre el período de referencia 1991-2020 de la OMM. '
        'Se realiza validación cruzada con estaciones pluviométricas CR2 (879 estaciones) '
        'usando interpolación IDW (Inverse Distance Weighting, peso = 1/d²).<br/><br/>'

        '<b>Temperatura mínima y heladas:</b> Datos grillados CR2MET Tmin v2.0, '
        'misma resolución y período. La probabilidad de helada se calcula como la '
        'frecuencia observada de días con Tmin < 0°C. La probabilidad mensual se estima '
        'como P(mes) = 1 - (1 - p_diaria)^30. El período libre de heladas (PLH) se define '
        'como los meses consecutivos con P(helada mensual) < 5%.<br/><br/>'

        '<b>Índices climáticos:</b> ONI (Oceanic Niño Index, NOAA CPC), '
        'PDO (Pacific Decadal Oscillation, Mantua et al. 1997) y '
        'SOI (Southern Oscillation Index, Trenberth 1984). '
        'Se usa el promedio trimestral para determinar el estado ENSO.<br/><br/>'

        '<b>Riesgo agronómico:</b> Umbrales de daño por helada según INIA Boletín Técnico, '
        'Atlas Agroclimático (Santibáñez, 2017) y UC Davis. La probabilidad de daño se '
        'calcula cruzando P(Tmin < umbral) con el calendario fenológico de cada especie. '
        'Superficies frutícolas del Catastro Nacional CIREN/ODEPA.<br/><br/>'

        '<b>Balance hídrico:</b> ETP estimada con curva latitudinal simplificada, calibrada '
        'con Penman-Monteith FAO-56 (Allen et al., 1998). Es una estimación de referencia; '
        'para diseño de riego se recomienda usar datos de estación meteorológica local.<br/><br/>'

        '<b>Megasequía:</b> Análisis de tendencia comparando precipitación media del período '
        '2006-2020 versus 1991-2005, en el contexto de la megasequía de Chile central '
        '(Garreaud et al., 2024).'
    )
    return text
