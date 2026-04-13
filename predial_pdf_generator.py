"""
Generador de Informes Prediales PDF — Estilo "Autofact para Predios Agrícolas"
==============================================================================
Genera un documento profesional de 8 secciones:
  1. Ficha + Resumen Semáforo (página 1 — decisión en 2 minutos)
  2. Producción Agrícola (catastro frutícola, comparación nacional)
  3. Disponibilidad de Agua (DGA, restricciones, infraestructura)
  4. Uso de Suelo y Vegetación (CONAF)
  5. Infraestructura Eléctrica (SEC, comparación regional)
  6. Riesgos Territoriales (semáforo consolidado)
  7. Vecinos Productivos (cluster provincial)
  8. Recomendaciones (checklist de verificación)

Usa ReportLab para PDF nativo. Diseño inspirado en informes Autofact.
"""

import io
import os
import tempfile
from datetime import datetime

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.units import cm, mm
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    PageBreak, Image, KeepTogether, HRFlowable,
)
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT, TA_JUSTIFY


# ── Colores ──────────────────────────────────────────────────────────────────
VERDE_OSCURO = colors.HexColor('#1B5E20')
VERDE_PRIMARIO = colors.HexColor('#2E7D32')
VERDE_CLARO = colors.HexColor('#E8F5E9')
VERDE_MEDIO = colors.HexColor('#66BB6A')

AZUL_AGUA = colors.HexColor('#1565C0')
AZUL_CLARO = colors.HexColor('#E3F2FD')

NARANJA = colors.HexColor('#E65100')
NARANJA_CLARO = colors.HexColor('#FFF3E0')

ROJO = colors.HexColor('#C62828')
ROJO_CLARO = colors.HexColor('#FFEBEE')

AMARILLO = colors.HexColor('#F9A825')
AMARILLO_CLARO = colors.HexColor('#FFFDE7')

GRIS_FONDO = colors.HexColor('#FAFAFA')
GRIS_BORDE = colors.HexColor('#E0E0E0')
GRIS_TEXTO = colors.HexColor('#616161')
TEXTO_OSCURO = colors.HexColor('#1a2a1a')
BLANCO = colors.white


def _create_styles():
    """Estilos tipográficos para el informe predial."""
    styles = getSampleStyleSheet()

    styles.add(ParagraphStyle(
        'PredialTitulo', parent=styles['Title'],
        fontSize=26, textColor=VERDE_OSCURO, spaceAfter=4,
        alignment=TA_LEFT, fontName='Helvetica-Bold',
    ))
    styles.add(ParagraphStyle(
        'PredialSubtitulo', parent=styles['Heading2'],
        fontSize=12, textColor=GRIS_TEXTO, spaceAfter=16,
        alignment=TA_LEFT, fontName='Helvetica',
    ))
    styles.add(ParagraphStyle(
        'SeccionTitulo', parent=styles['Heading2'],
        fontSize=14, textColor=VERDE_OSCURO, spaceBefore=20,
        spaceAfter=8, fontName='Helvetica-Bold',
    ))
    styles.add(ParagraphStyle(
        'SeccionSubtitulo', parent=styles['Normal'],
        fontSize=9, textColor=GRIS_TEXTO, spaceAfter=8,
        fontName='Helvetica-Oblique', leading=12,
    ))
    styles.add(ParagraphStyle(
        'SubSeccion', parent=styles['Heading3'],
        fontSize=11, textColor=VERDE_PRIMARIO, spaceBefore=12,
        spaceAfter=4, fontName='Helvetica-Bold',
    ))
    styles.add(ParagraphStyle(
        'Cuerpo', parent=styles['Normal'],
        fontSize=9, leading=13, alignment=TA_JUSTIFY,
        spaceAfter=6, textColor=TEXTO_OSCURO,
    ))
    styles.add(ParagraphStyle(
        'CuerpoNegrita', parent=styles['Normal'],
        fontSize=9, leading=13, fontName='Helvetica-Bold',
        textColor=TEXTO_OSCURO,
    ))
    styles.add(ParagraphStyle(
        'Destacado', parent=styles['Normal'],
        fontSize=10, leading=14, fontName='Helvetica-Bold',
        textColor=VERDE_OSCURO, spaceBefore=4, spaceAfter=4,
    ))
    styles.add(ParagraphStyle(
        'KPI_Valor', parent=styles['Normal'],
        fontSize=22, fontName='Helvetica-Bold',
        textColor=VERDE_OSCURO, alignment=TA_CENTER, spaceAfter=0,
    ))
    styles.add(ParagraphStyle(
        'KPI_Label', parent=styles['Normal'],
        fontSize=8, textColor=GRIS_TEXTO, alignment=TA_CENTER, spaceAfter=2,
    ))
    styles.add(ParagraphStyle(
        'Pie', parent=styles['Normal'],
        fontSize=7, textColor=GRIS_TEXTO, leading=9,
    ))
    styles.add(ParagraphStyle(
        'FuenteCita', parent=styles['Normal'],
        fontSize=7.5, textColor=GRIS_TEXTO, leading=10,
        spaceBefore=8, fontName='Helvetica-Oblique',
    ))
    styles.add(ParagraphStyle(
        'AlertaTexto', parent=styles['Normal'],
        fontSize=8.5, leading=12, textColor=colors.HexColor('#E65100'),
    ))
    return styles


# ── Helpers ──────────────────────────────────────────────────────────────────

def _format_clp(value):
    """Formatea un valor numérico como CLP."""
    if value is None or value == 0:
        return "N/D"
    if abs(value) >= 1_000_000_000:
        return f"${value / 1_000_000_000:,.1f} MM"
    if abs(value) >= 1_000_000:
        return f"${value / 1_000_000:,.1f} M"
    return f"${value:,.0f}"


def _sem_icon(semaforo):
    """Unicode icon for semáforo."""
    return {'verde': '\u2705', 'amarillo': '\u26A0', 'rojo': '\u274C'}.get(semaforo, '\u2753')


def _sem_label(semaforo):
    return {'verde': 'OK', 'amarillo': 'ATENCIÓN', 'rojo': 'ALERTA'}.get(semaforo, 'N/D')


def _build_table(headers, rows, col_widths=None, highlight_first_col=False):
    """Tabla con header verde oscuro, filas alternadas."""
    all_rows = [headers] + rows
    t = Table(all_rows, colWidths=col_widths, repeatRows=1)
    style_cmds = [
        ('BACKGROUND', (0, 0), (-1, 0), VERDE_OSCURO),
        ('TEXTCOLOR', (0, 0), (-1, 0), BLANCO),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 8),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 6),
        ('TOPPADDING', (0, 0), (-1, 0), 6),
        ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 1), (-1, -1), 8),
        ('BOTTOMPADDING', (0, 1), (-1, -1), 4),
        ('TOPPADDING', (0, 1), (-1, -1), 4),
        ('TEXTCOLOR', (0, 1), (-1, -1), TEXTO_OSCURO),
        ('LINEBELOW', (0, 0), (-1, 0), 1.5, VERDE_OSCURO),
        ('LINEBELOW', (0, 1), (-1, -1), 0.5, GRIS_BORDE),
        ('ALIGN', (1, 0), (-1, -1), 'RIGHT'),
        ('ALIGN', (0, 0), (0, -1), 'LEFT'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
    ]
    for i in range(1, len(all_rows)):
        if i % 2 == 0:
            style_cmds.append(('BACKGROUND', (0, i), (-1, i), GRIS_FONDO))
    if highlight_first_col:
        style_cmds.append(('FONTNAME', (0, 1), (0, -1), 'Helvetica-Bold'))
        style_cmds.append(('TEXTCOLOR', (0, 1), (0, -1), VERDE_OSCURO))
    t.setStyle(TableStyle(style_cmds))
    return t


def _chart_to_image(fig, width_cm=16, dpi=150):
    """Matplotlib figure to ReportLab Image."""
    buf = io.BytesIO()
    fig.savefig(buf, format='png', dpi=dpi, bbox_inches='tight',
                facecolor='white', edgecolor='none')
    buf.seek(0)
    plt.close(fig)
    tmp = tempfile.NamedTemporaryFile(suffix='.png', delete=False)
    tmp.write(buf.read())
    tmp.close()
    img = Image(tmp.name, width=width_cm * cm, height=None)
    img._restrictSize(width_cm * cm, 20 * cm)
    return img


def _build_semaforo_box(color_str, label, description):
    """Indicador visual de semáforo con fondo coloreado."""
    color_map = {
        'verde': ('#2E7D32', '#E8F5E9', 'Favorable'),
        'amarillo': ('#F9A825', '#FFFDE7', 'Precaución'),
        'rojo': ('#C62828', '#FFEBEE', 'Crítico'),
        'gris': ('#9E9E9E', '#F5F5F5', 'Sin datos'),
    }
    hex_color, bg_hex, default_label = color_map.get(color_str, color_map['gris'])
    display_label = label or default_label
    bg = colors.HexColor(bg_hex)

    data = [[
        Paragraph(f'<font color="{hex_color}" size="14">\u25CF</font>',
                  ParagraphStyle('s', alignment=TA_CENTER)),
        Paragraph(f'<b><font color="{hex_color}">{display_label}</font></b><br/>'
                  f'<font size="8" color="#616161">{description}</font>',
                  ParagraphStyle('s', fontSize=9, leading=12)),
    ]]
    t = Table(data, colWidths=[1.5 * cm, 14 * cm])
    t.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, -1), bg),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('LEFTPADDING', (0, 0), (-1, -1), 8),
        ('RIGHTPADDING', (0, 0), (-1, -1), 8),
        ('TOPPADDING', (0, 0), (-1, -1), 8),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
        ('BOX', (0, 0), (-1, -1), 0.5, colors.HexColor(hex_color)),
    ]))
    return t


def _build_alert_box(text, level='warning'):
    """Caja de alerta estilo Autofact (amarilla/roja/verde)."""
    config = {
        'warning': ('#FFF3E0', '#FF9800', '#E65100'),
        'critical': ('#FFEBEE', '#C62828', '#B71C1C'),
        'success': ('#E8F5E9', '#2E7D32', '#1B5E20'),
        'info': ('#E3F2FD', '#1565C0', '#0D47A1'),
    }
    bg_hex, border_hex, text_hex = config.get(level, config['warning'])

    data = [[Paragraph(
        f'<font color="{text_hex}" size="8.5">{text}</font>',
        ParagraphStyle('alert', fontSize=8.5, leading=12)
    )]]
    t = Table(data, colWidths=[15.5 * cm])
    t.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, -1), colors.HexColor(bg_hex)),
        ('LEFTPADDING', (0, 0), (-1, -1), 14),
        ('RIGHTPADDING', (0, 0), (-1, -1), 12),
        ('TOPPADDING', (0, 0), (-1, -1), 8),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
        ('LINEBEFORE', (0, 0), (0, -1), 4, colors.HexColor(border_hex)),
    ]))
    return t


def _build_kpi_row(kpis, page_width):
    """Fila de KPIs (valor + label)."""
    n = len(kpis)
    if n == 0:
        return Spacer(1, 0)
    col_w = page_width / n
    cells = []
    for valor, label, unidad in kpis:
        cell_content = Paragraph(
            f'<font size="20" color="#2E7D32"><b>{valor}</b></font>'
            f'<font size="8" color="#616161"> {unidad}</font><br/>'
            f'<font size="8" color="#616161">{label}</font>',
            ParagraphStyle('kpi', alignment=TA_CENTER, leading=14,
                           spaceBefore=4, spaceAfter=4)
        )
        cells.append(cell_content)
    t = Table([cells], colWidths=[col_w] * n)
    t.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, -1), VERDE_CLARO),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('TOPPADDING', (0, 0), (-1, -1), 10),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 10),
        ('LINEBEFORE', (1, 0), (-1, -1), 0.5, VERDE_MEDIO),
        ('BOX', (0, 0), (-1, -1), 0.5, VERDE_PRIMARIO),
    ]))
    return t


def _add_fuente(elements, styles, text):
    """Agrega cita de fuente al pie de sección."""
    elements.append(Paragraph(f"FUENTE: {text}", styles['FuenteCita']))


def _add_alerts(elements, alertas):
    """Agrega cajas de alerta si hay."""
    if not alertas:
        return
    for alerta in alertas:
        level = 'warning'
        if alerta.startswith('❌') or 'ALERTA' in alerta.upper():
            level = 'critical'
        elif alerta.startswith('✅'):
            level = 'success'
        elements.append(_build_alert_box(alerta, level))
        elements.append(Spacer(1, 2 * mm))


# ══════════════════════════════════════════════════════════════════════════════
# SECCIONES DEL INFORME
# ══════════════════════════════════════════════════════════════════════════════

def _section_portada(report, styles):
    """Portada + ficha del predio."""
    elements = []
    elements.append(Spacer(1, 2 * cm))
    elements.append(HRFlowable(width="100%", thickness=3, color=VERDE_OSCURO))
    elements.append(Spacer(1, 0.5 * cm))
    elements.append(Paragraph("INFORME PREDIAL", styles['PredialTitulo']))
    elements.append(Paragraph(
        f"Análisis Territorial — {report.get('comuna', 'N/D')}",
        styles['PredialSubtitulo']
    ))
    elements.append(Spacer(1, 0.3 * cm))
    elements.append(HRFlowable(width="100%", thickness=1, color=VERDE_PRIMARIO))
    elements.append(Spacer(1, 1 * cm))

    # Info box
    region = report.get('region', 'N/D')
    provincia = report.get('provincia', 'N/D')
    fecha = datetime.now().strftime('%d de %B de %Y')
    lat = report.get('lat')
    lon = report.get('lon')
    coord_str = f"{lat:.4f}°, {lon:.4f}°" if lat and lon else "No especificadas"

    info_data = [
        ['Comuna', report.get('comuna', 'N/D')],
        ['Región', region],
        ['Provincia', provincia],
        ['Coordenadas', coord_str],
        ['Fecha del informe', fecha],
    ]
    info_table = Table(info_data, colWidths=[5 * cm, 12 * cm])
    info_table.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONTNAME', (1, 0), (1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('TEXTCOLOR', (0, 0), (0, -1), VERDE_OSCURO),
        ('TEXTCOLOR', (1, 0), (1, -1), TEXTO_OSCURO),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
        ('TOPPADDING', (0, 0), (-1, -1), 8),
        ('LINEBELOW', (0, 0), (-1, -2), 0.5, GRIS_BORDE),
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
    ]))
    elements.append(info_table)
    elements.append(Spacer(1, 1.5 * cm))

    # ── Semáforo grid (Autofact page 1 style) ────────────────────────────
    elements.append(Paragraph("RESUMEN DEL PREDIO", styles['SubSeccion']))
    elements.append(Spacer(1, 0.3 * cm))

    sem_items = _get_semaforo_summary(report)
    for item in sem_items:
        sem = item['semaforo']
        color_map = {
            'verde': ('#2E7D32', '#E8F5E9'),
            'amarillo': ('#F9A825', '#FFFDE7'),
            'rojo': ('#C62828', '#FFEBEE'),
            'gris': ('#9E9E9E', '#F5F5F5'),
        }
        hex_color, bg_hex = color_map.get(sem, color_map['gris'])
        icon = {'verde': '\u2705', 'amarillo': '\u26A0\uFE0F', 'rojo': '\u274C'}.get(sem, '\u2753')

        row_data = [[
            Paragraph(f'<font color="{hex_color}" size="11">\u25CF</font>',
                      ParagraphStyle('s', alignment=TA_CENTER)),
            Paragraph(f'<b>{item["seccion"]}</b>',
                      ParagraphStyle('s', fontSize=9, fontName='Helvetica-Bold')),
            Paragraph(f'<font color="{hex_color}">{item["texto"]}</font>',
                      ParagraphStyle('s', fontSize=8, textColor=colors.HexColor(hex_color))),
        ]]
        row_t = Table(row_data, colWidths=[1.2 * cm, 5 * cm, 10.3 * cm])
        row_t.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, -1), colors.HexColor(bg_hex)),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('TOPPADDING', (0, 0), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
            ('LEFTPADDING', (0, 0), (-1, -1), 6),
        ]))
        elements.append(row_t)
        elements.append(Spacer(1, 1.5 * mm))

    elements.append(Spacer(1, 1 * cm))

    # Disclaimer
    elements.append(Paragraph(
        "Este informe fue generado automáticamente a partir de bases de datos públicas "
        "(SAG/CIREN, DGA, SEC, CONAF, ODEPA). La información es referencial y no constituye una "
        "evaluación técnica vinculante. Se recomienda complementar con inspección en terreno "
        "y asesoría profesional.",
        styles['Pie']
    ))
    elements.append(PageBreak())
    return elements


def _get_semaforo_summary(report):
    """Extrae el resumen de semáforos de todas las secciones."""
    items = []

    # Producción
    prod = report.get('produccion', {})
    sem = prod.get('semaforo', 'gris')
    texto = prod.get('semaforo_texto', 'Sin datos de producción frutícola')
    items.append({'seccion': 'Producción Agrícola', 'semaforo': sem, 'texto': texto})

    # Agua
    agua = report.get('agua', {})
    sem = agua.get('semaforo', 'gris')
    texto = agua.get('semaforo_texto', 'Sin datos de patentes de agua')
    items.append({'seccion': 'Disponibilidad Hídrica', 'semaforo': sem, 'texto': texto})

    # Uso de suelo
    uso = report.get('uso_suelo', {})
    sem = uso.get('semaforo', 'gris')
    texto = uso.get('semaforo_texto', 'Sin datos de uso de suelo CONAF')
    items.append({'seccion': 'Uso de Suelo', 'semaforo': sem, 'texto': texto})

    # Eléctrico
    elec = report.get('electrico', {})
    sem = elec.get('semaforo', 'gris')
    texto = elec.get('semaforo_texto', 'Sin datos de infraestructura eléctrica')
    items.append({'seccion': 'Infraestructura Eléctrica', 'semaforo': sem, 'texto': texto})

    # Riesgos
    riesgos = report.get('riesgos', {})
    sem = riesgos.get('semaforo_global', 'gris')
    texto = riesgos.get('semaforo_texto', 'Sin evaluación de riesgos')
    items.append({'seccion': 'Riesgos Territoriales', 'semaforo': sem, 'texto': texto})

    # Infraestructura
    infra = report.get('infraestructura', {})
    if infra.get('disponible'):
        items.append({
            'seccion': 'Infraestructura Logística',
            'semaforo': 'verde',
            'texto': f"{len(infra.get('cercanos', []))} puntos de infraestructura identificados",
        })

    return items


def _section_resumen_ejecutivo(report, styles, page_width):
    """Resumen ejecutivo: decisión en 2 minutos."""
    elements = []
    elements.append(Paragraph("1. RESUMEN EJECUTIVO", styles['SeccionTitulo']))
    elements.append(Paragraph(
        "Visión general de los indicadores clave del predio y su entorno territorial.",
        styles['SeccionSubtitulo']))
    elements.append(HRFlowable(width="100%", thickness=1.5, color=VERDE_OSCURO))
    elements.append(Spacer(1, 0.3 * cm))

    prod = report.get('produccion', {})
    agua = report.get('agua', {})
    elec = report.get('electrico', {})
    uso = report.get('uso_suelo', {})

    # KPIs
    kpis = []
    if prod.get('disponible'):
        kpis.append((f"{prod.get('total_superficie_ha', 0):,.0f}", "Sup. Frutícola", "ha"))
        kpis.append((str(len(prod.get('especies', []))), "Especies", ""))
    if agua.get('disponible'):
        kpis.append((f"{agua.get('num_patentes', 0):,}", "Patentes Agua", ""))
    if elec.get('disponible'):
        kpis.append((f"{elec.get('total_clientes', 0):,}", "Clientes Eléctricos", ""))
    if kpis:
        elements.append(_build_kpi_row(kpis[:5], page_width))
        elements.append(Spacer(1, 0.5 * cm))

    # Narrative
    comuna = report.get('comuna', 'la comuna')
    parts = [
        f"La comuna de <b>{comuna}</b>"
        + (f", ubicada en la {report.get('region', '')}" if report.get('region') else "")
        + ", presenta el siguiente perfil territorial:"
    ]
    if prod.get('disponible') and prod.get('especies'):
        top3 = prod['especies'][:3]
        top3_text = ', '.join(f"{e['especie']} ({e['superficie_ha']:,.0f} ha)" for e in top3)
        parts.append(
            f"La actividad frutícola abarca <b>{prod['total_superficie_ha']:,.0f} ha</b> "
            f"en <b>{len(prod['especies'])} especies</b>. Principales: {top3_text}."
        )
    if agua.get('disponible'):
        mor = agua.get('morosidad_pct', 0)
        parts.append(
            f"Se registran <b>{agua['num_patentes']:,} patentes de agua</b> "
            f"(monto {_format_clp(agua.get('monto_total', 0))}). "
            f"Morosidad: {mor:.1f}%."
        )
    if elec.get('disponible'):
        parts.append(
            f"Infraestructura eléctrica: <b>{elec['total_clientes']:,} clientes</b>, "
            f"{elec['potencia_total_kw']:,.0f} kW totales "
            f"({elec['potencia_promedio_kw']:.1f} kW/cliente)."
        )
    if uso.get('disponible'):
        parts.append(
            f"Uso de suelo: {uso.get('pct_agricola', 0):.0f}% agrícola "
            f"de {uso.get('total_ha', 0):,.0f} ha totales registradas."
        )

    for part in parts:
        elements.append(Paragraph(part, styles['Cuerpo']))

    # Semáforo hídrico
    elements.append(Spacer(1, 0.4 * cm))
    sem = agua.get('semaforo', 'gris')
    sem_texto = agua.get('semaforo_texto', 'Sin datos suficientes para evaluar.')
    elements.append(_build_semaforo_box(sem, None, sem_texto))

    elements.append(PageBreak())
    return elements


def _section_produccion(report, styles, page_width):
    """Sección 2: Producción Agrícola."""
    elements = []
    prod = report.get('produccion', {})

    elements.append(Paragraph("2. PRODUCCIÓN AGRÍCOLA", styles['SeccionTitulo']))
    elements.append(Paragraph(
        "Revisa qué se produce en esta comuna y sus alrededores. Estos datos ayudan a evaluar "
        "el potencial productivo del predio.",
        styles['SeccionSubtitulo']))
    elements.append(HRFlowable(width="100%", thickness=1.5, color=VERDE_OSCURO))
    elements.append(Spacer(1, 0.3 * cm))

    if not prod.get('disponible'):
        elements.append(Paragraph(
            "No se encontraron datos de catastro frutícola para esta comuna.",
            styles['Cuerpo']))
        elements.append(Spacer(1, 0.3 * cm))
        elements.append(_build_semaforo_box('rojo', 'Sin registro',
                                             'No hay producción frutícola registrada en esta comuna.'))
        _add_fuente(elements, styles,
                    "Catastro Frutícola Nacional, CIREN — Centro de Información de Recursos Naturales")
        return elements

    # Analytical text
    texto = prod.get('texto_analitico', '')
    if texto:
        elements.append(Paragraph(texto, styles['Cuerpo']))
        elements.append(Spacer(1, 0.2 * cm))

    # Alerts
    _add_alerts(elements, prod.get('alertas', []))

    # Semáforo
    sem = prod.get('semaforo', 'verde')
    sem_texto = prod.get('semaforo_texto', '')
    if sem_texto:
        elements.append(Spacer(1, 0.2 * cm))
        elements.append(_build_semaforo_box(sem, _sem_label(sem), sem_texto))
        elements.append(Spacer(1, 0.3 * cm))

    # Chart: top species
    if prod.get('especies'):
        elements.append(Paragraph("Distribución por Especie", styles['SubSeccion']))
        top = prod['especies'][:12]
        names = [e['especie'] for e in top]
        areas = [e['superficie_ha'] for e in top]

        fig, ax = plt.subplots(figsize=(7, max(2.5, len(names) * 0.32)))
        bars = ax.barh(range(len(names)), areas, color='#4CAF50',
                       edgecolor='#2E7D32', linewidth=0.5, height=0.65)
        ax.set_yticks(range(len(names)))
        ax.set_yticklabels(names, fontsize=8)
        ax.invert_yaxis()
        ax.set_xlabel('Superficie (ha)', fontsize=8)
        ax.grid(True, alpha=0.2, axis='x')
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        for bar, val in zip(bars, areas):
            if max(areas) > 0:
                ax.text(bar.get_width() + max(areas) * 0.02,
                        bar.get_y() + bar.get_height() / 2,
                        f'{val:,.0f} ha', va='center', fontsize=7, color='#333')
        plt.tight_layout()
        elements.append(_chart_to_image(fig, width_cm=15))

        # Table
        headers = ['Especie', 'Superficie (ha)', 'N° Explotaciones', '% del Total']
        total_ha = prod.get('total_superficie_ha', 1) or 1
        rows = []
        for e in prod['especies']:
            pct = (e['superficie_ha'] / total_ha) * 100
            rows.append([
                e['especie'], f"{e['superficie_ha']:,.1f}",
                str(e['num_explotaciones']), f"{pct:.1f}%",
            ])
        rows.append(['TOTAL', f"{prod['total_superficie_ha']:,.1f}",
                      str(prod.get('total_explotaciones', '')), '100.0%'])
        elements.append(Spacer(1, 0.3 * cm))
        elements.append(_build_table(headers, rows,
                                     col_widths=[6*cm, 3.5*cm, 3.5*cm, 3*cm],
                                     highlight_first_col=True))

    # National comparison
    comp = prod.get('comparacion_nacional', [])
    if comp:
        elements.append(Spacer(1, 0.5 * cm))
        elements.append(Paragraph("Comparación con Promedio Nacional", styles['SubSeccion']))
        headers = ['Especie', 'Esta Comuna (ha)', 'Total Nacional (ha)',
                    'Prom. por Comuna (ha)', '% Nacional']
        rows = []
        for c in comp[:10]:
            rows.append([
                c.get('especie', ''),
                f"{c.get('superficie_comuna', 0):,.1f}",
                f"{c.get('total_nacional', 0):,.0f}",
                f"{c.get('promedio_comuna_nacional', 0):,.1f}",
                f"{c.get('pct_nacional', 0):.2f}%",
            ])
        elements.append(_build_table(headers, rows,
                                     col_widths=[4*cm, 3*cm, 3.5*cm, 3.5*cm, 2.5*cm]))

    # Irrigation methods
    if prod.get('metodos_riego'):
        elements.append(Spacer(1, 0.5 * cm))
        elements.append(Paragraph("Métodos de Riego", styles['SubSeccion']))
        headers = ['Método', 'Superficie (ha)', '% del Total']
        total_riego = sum(r.get('superficie_ha', 0) for r in prod['metodos_riego']) or 1
        rows = [[r.get('metodo', 'N/D'),
                 f"{r.get('superficie_ha', 0):,.1f}",
                 f"{r.get('pct', r.get('superficie_ha', 0) / total_riego * 100):.1f}%"]
                for r in prod['metodos_riego']]
        elements.append(_build_table(headers, rows,
                                     col_widths=[7*cm, 4.5*cm, 4.5*cm],
                                     highlight_first_col=True))

    # Producer types
    if prod.get('tipo_productor'):
        elements.append(Spacer(1, 0.5 * cm))
        elements.append(Paragraph("Tipo de Productor", styles['SubSeccion']))
        headers = ['Tipo', 'N° Explotaciones', 'Superficie (ha)']
        rows = [[t.get('tipo', 'N/D'),
                 str(t.get('num_explotaciones', 0)),
                 f"{t.get('hectareas', 0):,.1f}"]
                for t in prod['tipo_productor']]
        elements.append(_build_table(headers, rows,
                                     col_widths=[6*cm, 5*cm, 5*cm],
                                     highlight_first_col=True))

    _add_fuente(elements, styles,
                "Catastro Frutícola Nacional, CIREN — Centro de Información de Recursos Naturales (2021-2025)")
    elements.append(PageBreak())
    return elements


def _section_agua(report, styles, page_width):
    """Sección 3: Disponibilidad de Agua."""
    elements = []
    agua = report.get('agua', {})

    elements.append(Paragraph("3. DISPONIBILIDAD DE AGUA", styles['SeccionTitulo']))
    elements.append(Paragraph(
        "El agua es el recurso más crítico para la agricultura en Chile. Esta sección presenta "
        "los derechos de agua registrados, restricciones vigentes y morosidad.",
        styles['SeccionSubtitulo']))
    elements.append(HRFlowable(width="100%", thickness=1.5, color=AZUL_AGUA))
    elements.append(Spacer(1, 0.3 * cm))

    if not agua.get('disponible'):
        elements.append(Paragraph(
            "No se encontraron registros de patentes de agua para esta comuna.",
            styles['Cuerpo']))
        elements.append(_build_semaforo_box('rojo', 'Sin datos',
                                             'No hay patentes de agua registradas.'))
        _add_fuente(elements, styles,
                    "DGA — Dirección General de Aguas, Ministerio de Obras Públicas")
        return elements

    # Analytical text
    texto = agua.get('texto_analitico', '')
    if texto:
        elements.append(Paragraph(texto, styles['Cuerpo']))

    # Alerts
    _add_alerts(elements, agua.get('alertas', []))

    # Semáforo
    sem = agua.get('semaforo', 'amarillo')
    sem_texto = agua.get('semaforo_texto', '')
    if sem_texto:
        elements.append(Spacer(1, 0.2 * cm))
        elements.append(_build_semaforo_box(sem, _sem_label(sem), sem_texto))
        elements.append(Spacer(1, 0.3 * cm))

    # KPIs
    mor = agua.get('morosidad_pct', 0)
    kpis = [
        (f"{agua.get('num_patentes', 0):,}", "Patentes", ""),
        (_format_clp(agua.get('monto_total', 0)), "Monto Total", ""),
        (f"{mor:.1f}%", "Morosidad", ""),
    ]
    elements.append(_build_kpi_row(kpis, page_width))
    elements.append(Spacer(1, 0.3 * cm))

    # Summary table (Autofact style)
    restriccion = agua.get('restriccion_hidrica', False)
    agotamiento = agua.get('agotamiento', False)
    sum_headers = ['Indicador', 'Valor', 'Resultado']
    sum_rows = [
        ['Total patentes registradas', f"{agua.get('num_patentes', 0):,}", '—'],
        ['Monto total', _format_clp(agua.get('monto_total', 0)), '—'],
        ['Tasa de morosidad', f"{mor:.1f}%",
         'OK' if mor < 10 else ('ATENCIÓN' if mor < 30 else 'ALERTA')],
        ['Zona de restricción hídrica', 'Sí' if restriccion else 'No',
         'ALERTA' if restriccion else 'OK'],
        ['Declaración de agotamiento', 'Sí' if agotamiento else 'No',
         'ALERTA' if agotamiento else 'OK'],
    ]
    elements.append(_build_table(sum_headers, sum_rows,
                                 col_widths=[6*cm, 5*cm, 5.5*cm],
                                 highlight_first_col=True))

    # By concesión type
    if agua.get('por_concesion'):
        elements.append(Spacer(1, 0.5 * cm))
        elements.append(Paragraph("Distribución por Tipo de Concesión", styles['SubSeccion']))
        headers = ['Concesión', 'N° Patentes', 'Monto Total', 'Saldo Pendiente']
        rows = [[c.get('concesion', 'N/D'), str(c.get('num_patentes', 0)),
                 _format_clp(c.get('monto_total', 0)),
                 _format_clp(c.get('saldo_pendiente', 0))]
                for c in agua['por_concesion']]
        elements.append(_build_table(headers, rows,
                                     col_widths=[5*cm, 3*cm, 4*cm, 4.5*cm],
                                     highlight_first_col=True))

    # CBR offices
    if agua.get('oficinas_cbr'):
        elements.append(Spacer(1, 0.5 * cm))
        elements.append(Paragraph("Oficinas CBR (inscripción de derechos)", styles['SubSeccion']))
        headers = ['Oficina CBR', 'N° Patentes']
        rows = [[o.get('oficina', 'N/D'), str(o.get('num_patentes', 0))]
                for o in agua['oficinas_cbr'][:10]]
        elements.append(_build_table(headers, rows,
                                     col_widths=[12*cm, 4.5*cm],
                                     highlight_first_col=True))

    # Nearby infrastructure
    if agua.get('infraestructura_cercana'):
        elements.append(Spacer(1, 0.5 * cm))
        elements.append(Paragraph("Infraestructura Hídrica Cercana", styles['SubSeccion']))
        headers = ['Tipo', 'Nombre', 'Distancia (km)']
        rows = [[i.get('tipo', ''), i.get('nombre', ''),
                 f"{i.get('distancia_km', 0):.1f}"]
                for i in agua['infraestructura_cercana'][:10]]
        elements.append(_build_table(headers, rows,
                                     col_widths=[4*cm, 8.5*cm, 4*cm],
                                     highlight_first_col=True))

    # Evolution chart
    if agua.get('por_anio') and len(agua['por_anio']) > 1:
        elements.append(Spacer(1, 0.5 * cm))
        elements.append(Paragraph("Evolución Anual de Patentes", styles['SubSeccion']))
        anios = [str(a['anio']) for a in agua['por_anio']]
        cantidades = [a['cantidad'] for a in agua['por_anio']]
        fig, ax = plt.subplots(figsize=(7, 3))
        ax.bar(range(len(anios)), cantidades, color='#42A5F5',
               edgecolor='#1565C0', linewidth=0.5)
        ax.set_xticks(range(len(anios)))
        ax.set_xticklabels(anios, fontsize=7, rotation=45)
        ax.set_ylabel('N° Patentes', fontsize=8)
        ax.grid(True, alpha=0.2, axis='y')
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        plt.tight_layout()
        elements.append(_chart_to_image(fig, width_cm=14))

    _add_fuente(elements, styles,
                "DGA — Dirección General de Aguas, Ministerio de Obras Públicas (2025)")
    elements.append(PageBreak())
    return elements


def _section_uso_suelo(report, styles, page_width):
    """Sección 4: Uso de Suelo y Vegetación (CONAF)."""
    elements = []
    uso = report.get('uso_suelo', {})

    elements.append(Paragraph("4. USO DE SUELO Y VEGETACIÓN", styles['SeccionTitulo']))
    elements.append(Paragraph(
        "Distribución del uso de suelo en la comuna según el catastro de vegetación de CONAF. "
        "Permite entender qué hay alrededor del predio.",
        styles['SeccionSubtitulo']))
    elements.append(HRFlowable(width="100%", thickness=1.5, color=VERDE_PRIMARIO))
    elements.append(Spacer(1, 0.3 * cm))

    if not uso.get('disponible'):
        elements.append(Paragraph(
            "Los datos de uso de suelo de CONAF no están disponibles para esta región. "
            "Las regiones con cobertura incluyen: Valparaíso, O'Higgins, Maule, Biobío, "
            "Araucanía, Metropolitana, Los Ríos, Arica y Parinacota, Tarapacá, Antofagasta.",
            styles['Cuerpo']))
        elements.append(_build_semaforo_box('gris', 'Sin datos',
                                             'Región sin cobertura de datos CONAF.'))
        _add_fuente(elements, styles,
                    "CONAF — Corporación Nacional Forestal, Catastro de Vegetación")
        return elements

    # Analytical text
    texto = uso.get('texto_analitico', '')
    if texto:
        elements.append(Paragraph(texto, styles['Cuerpo']))

    _add_alerts(elements, uso.get('alertas', []))

    # Semáforo
    sem = uso.get('semaforo', 'amarillo')
    sem_texto = uso.get('semaforo_texto', '')
    if sem_texto:
        elements.append(Spacer(1, 0.2 * cm))
        elements.append(_build_semaforo_box(sem, _sem_label(sem), sem_texto))
        elements.append(Spacer(1, 0.3 * cm))

    # Distribution table
    dist = uso.get('distribucion', [])
    if dist:
        elements.append(Paragraph("Distribución de Uso de Suelo", styles['SubSeccion']))
        headers = ['Uso de Suelo', 'Superficie (ha)', '% del Total']
        rows = [[d.get('uso_tierra', 'N/D'),
                 f"{d.get('superficie_ha', 0):,.1f}",
                 f"{d.get('pct', 0):.1f}%"]
                for d in dist]
        total_ha = uso.get('total_ha', sum(d.get('superficie_ha', 0) for d in dist))
        rows.append(['TOTAL', f"{total_ha:,.1f}", '100.0%'])
        elements.append(_build_table(headers, rows,
                                     col_widths=[7*cm, 4.5*cm, 5*cm],
                                     highlight_first_col=True))

        # Pie chart
        if len(dist) >= 2:
            elements.append(Spacer(1, 0.3 * cm))
            labels = [d.get('uso_tierra', '') for d in dist[:8]]
            vals = [d.get('superficie_ha', 0) for d in dist[:8]]
            color_list = ['#2E7D32', '#4CAF50', '#66BB6A', '#81C784',
                          '#A5D6A7', '#C8E6C9', '#E8F5E9', '#F1F8E9']
            fig, ax = plt.subplots(figsize=(6, 4))
            ax.pie(vals, labels=labels, autopct='%1.0f%%',
                   colors=color_list[:len(vals)], textprops={'fontsize': 7},
                   startangle=90, pctdistance=0.8)
            ax.set_title('Distribución de Uso de Suelo', fontsize=10,
                         fontweight='bold', color='#2E7D32')
            plt.tight_layout()
            elements.append(_chart_to_image(fig, width_cm=12))

    _add_fuente(elements, styles,
                f"CONAF — Corporación Nacional Forestal, Catastro de Vegetación "
                f"(actualización {uso.get('fuente_ano', 'N/D')})")
    elements.append(PageBreak())
    return elements


def _section_electrico(report, styles, page_width):
    """Sección 5: Infraestructura Eléctrica."""
    elements = []
    elec = report.get('electrico', {})

    elements.append(Paragraph("5. INFRAESTRUCTURA ELÉCTRICA", styles['SeccionTitulo']))
    elements.append(Paragraph(
        "La disponibilidad eléctrica es clave para riego tecnificado, cámaras de frío y "
        "procesamiento. Revisa la capacidad de la red en esta zona.",
        styles['SeccionSubtitulo']))
    elements.append(HRFlowable(width="100%", thickness=1.5, color=NARANJA))
    elements.append(Spacer(1, 0.3 * cm))

    if not elec.get('disponible'):
        elements.append(Paragraph(
            "No se encontraron datos de infraestructura eléctrica para esta comuna.",
            styles['Cuerpo']))
        elements.append(_build_semaforo_box('rojo', 'Sin datos',
                                             'No hay datos de infraestructura eléctrica.'))
        _add_fuente(elements, styles,
                    "SEC — Superintendencia de Electricidad y Combustibles, transparencia (2024)")
        return elements

    # Analytical text
    texto = elec.get('texto_analitico', '')
    if texto:
        elements.append(Paragraph(texto, styles['Cuerpo']))

    _add_alerts(elements, elec.get('alertas', []))

    # Semáforo
    sem = elec.get('semaforo', 'verde')
    sem_texto = elec.get('semaforo_texto', '')
    if sem_texto:
        elements.append(Spacer(1, 0.2 * cm))
        elements.append(_build_semaforo_box(sem, _sem_label(sem), sem_texto))
        elements.append(Spacer(1, 0.3 * cm))

    # KPIs
    kpis = [
        (f"{elec.get('total_clientes', 0):,}", "Clientes", ""),
        (f"{elec.get('potencia_total_kw', 0):,.0f}", "Potencia Total", "kW"),
        (f"{elec.get('potencia_promedio_kw', 0):.1f}", "Pot. Promedio", "kW/cliente"),
    ]
    elements.append(_build_kpi_row(kpis, page_width))
    elements.append(Spacer(1, 0.3 * cm))

    # Comparison table (if regional data available)
    comp = elec.get('comparacion_regional', {})
    if comp:
        elements.append(Paragraph("Comparación Regional", styles['SubSeccion']))
        headers = ['Indicador', f"{report.get('comuna', 'Comuna')}", 'Promedio Regional', 'Diferencia']
        diff = comp.get('diff_pct', 0)
        diff_str = f"+{diff:.0f}%" if diff > 0 else f"{diff:.0f}%"
        rows = [
            ['Potencia Promedio (kW)',
             f"{elec.get('potencia_promedio_kw', 0):.1f}",
             f"{comp.get('potencia_promedio_region', 0):.1f}",
             diff_str],
        ]
        elements.append(_build_table(headers, rows,
                                     col_widths=[5*cm, 3.5*cm, 4*cm, 4*cm],
                                     highlight_first_col=True))

    # Companies
    if elec.get('empresas'):
        elements.append(Spacer(1, 0.5 * cm))
        elements.append(Paragraph("Distribuidoras Eléctricas", styles['SubSeccion']))
        headers = ['Empresa', 'Clientes', 'Potencia (kW)']
        rows = [[e.get('empresa', 'N/D'),
                 str(e.get('clientes', 0)),
                 f"{e.get('potencia_kw', 0):,.1f}"]
                for e in elec['empresas']]
        elements.append(_build_table(headers, rows,
                                     col_widths=[8*cm, 4*cm, 4.5*cm],
                                     highlight_first_col=True))

    # Power distribution
    if elec.get('distribucion_potencia'):
        elements.append(Spacer(1, 0.5 * cm))
        elements.append(Paragraph("Distribución por Rango de Potencia", styles['SubSeccion']))
        headers = ['Rango', 'N° Clientes', 'Potencia Subtotal (kW)']
        rows = [[d.get('rango', ''), str(d.get('num_clientes', 0)),
                 f"{d.get('potencia_kw', 0):,.1f}"]
                for d in elec['distribucion_potencia']]
        elements.append(_build_table(headers, rows,
                                     col_widths=[5*cm, 5.5*cm, 6*cm],
                                     highlight_first_col=True))

    _add_fuente(elements, styles,
                "SEC — Superintendencia de Electricidad y Combustibles, datos de transparencia (2024)")
    elements.append(PageBreak())
    return elements


def _section_riesgos(report, styles, page_width):
    """Sección 6: Riesgos Territoriales."""
    elements = []
    riesgos = report.get('riesgos', {})

    elements.append(Paragraph("6. RIESGOS TERRITORIALES", styles['SeccionTitulo']))
    elements.append(Paragraph(
        "Identifica riesgos que podrían afectar tu inversión: restricciones hídricas, "
        "zonas protegidas y otros factores.",
        styles['SeccionSubtitulo']))
    elements.append(HRFlowable(width="100%", thickness=1.5, color=ROJO))
    elements.append(Spacer(1, 0.3 * cm))

    if not riesgos.get('disponible'):
        elements.append(Paragraph(
            "No se pudo completar la evaluación de riesgos para esta ubicación. "
            "Datos geoespaciales no disponibles.",
            styles['Cuerpo']))
        _add_fuente(elements, styles,
                    "DGA, CONAF, MMA — elaboración propia")
        return elements

    # Analytical text
    texto = riesgos.get('texto_analitico', '')
    if texto:
        elements.append(Paragraph(texto, styles['Cuerpo']))

    _add_alerts(elements, riesgos.get('alertas', []))

    # Risk items table (Autofact style)
    items = riesgos.get('items', [])
    if items:
        elements.append(Spacer(1, 0.3 * cm))
        headers = ['RIESGO', 'ESTADO', 'RESULTADO']
        rows = []
        for item in items:
            sem = item.get('semaforo', 'gris')
            resultado = _sem_label(sem)
            rows.append([
                item.get('riesgo', ''),
                item.get('estado', ''),
                resultado,
            ])
        elements.append(_build_table(headers, rows,
                                     col_widths=[5.5*cm, 7*cm, 4*cm],
                                     highlight_first_col=True))

    # Global semáforo
    sem_global = riesgos.get('semaforo_global', 'amarillo')
    sem_texto = riesgos.get('semaforo_texto', '')
    if sem_texto:
        elements.append(Spacer(1, 0.4 * cm))
        labels = {
            'verde': 'SIN RIESGOS MAYORES',
            'amarillo': 'REQUIERE ATENCIÓN',
            'rojo': 'RIESGOS CRÍTICOS DETECTADOS',
        }
        elements.append(_build_semaforo_box(
            sem_global, labels.get(sem_global, ''), sem_texto))

    _add_fuente(elements, styles,
                "DGA (MOP), CONAF, MMA — Ministerio del Medio Ambiente")
    elements.append(PageBreak())
    return elements


def _section_vecinos(report, styles, page_width):
    """Sección 7: Vecinos Productivos."""
    elements = []
    vecinos = report.get('vecinos', {})

    elements.append(Paragraph("7. VECINOS PRODUCTIVOS", styles['SeccionTitulo']))
    elements.append(Paragraph(
        "Conoce qué producen los agricultores en la zona. Un entorno productivo consolidado "
        "facilita acceso a servicios, mano de obra y mercados.",
        styles['SeccionSubtitulo']))
    elements.append(HRFlowable(width="100%", thickness=1.5, color=VERDE_PRIMARIO))
    elements.append(Spacer(1, 0.3 * cm))

    if not vecinos.get('disponible'):
        elements.append(Paragraph(
            "No se encontraron datos de producción frutícola en comunas vecinas.",
            styles['Cuerpo']))
        _add_fuente(elements, styles,
                    "Catastro Frutícola Nacional, CIREN (2021-2025)")
        return elements

    texto = vecinos.get('texto_analitico', '')
    if texto:
        elements.append(Paragraph(texto, styles['Cuerpo']))
        elements.append(Spacer(1, 0.3 * cm))

    comunas = vecinos.get('comunas_provincia', [])
    if comunas:
        headers = ['Comuna', 'Especie', 'Superficie (ha)']
        rows = [[c.get('comuna', ''), c.get('especie', ''),
                 f"{c.get('hectareas', 0):,.1f}"]
                for c in comunas[:20]]
        elements.append(_build_table(headers, rows,
                                     col_widths=[5.5*cm, 5.5*cm, 5.5*cm],
                                     highlight_first_col=True))

    _add_fuente(elements, styles,
                "Catastro Frutícola Nacional, CIREN (2021-2025)")
    return elements


def _section_recomendaciones(report, styles, page_width):
    """Sección 8: Recomendaciones — checklist de verificación."""
    elements = []
    recs = report.get('recomendaciones', {})

    elements.append(Spacer(1, 0.5 * cm))
    elements.append(Paragraph("8. RECOMENDACIONES", styles['SeccionTitulo']))
    elements.append(Paragraph(
        "Antes de comprar o invertir, te recomendamos verificar los siguientes puntos.",
        styles['SeccionSubtitulo']))
    elements.append(HRFlowable(width="100%", thickness=1.5, color=VERDE_OSCURO))
    elements.append(Spacer(1, 0.3 * cm))

    checklist = recs.get('checklist', [])
    if checklist:
        headers = ['Verificación', 'Descripción', 'Acción']
        rows = [[f"\u2610 {c.get('verificacion', '')}",
                 c.get('descripcion', ''),
                 c.get('accion', '')]
                for c in checklist]
        elements.append(_build_table(headers, rows,
                                     col_widths=[4.5*cm, 6.5*cm, 5.5*cm],
                                     highlight_first_col=True))
    else:
        # Default checklist
        defaults = [
            ('\u2610 Derechos de agua', 'Verificar inscripción en CBR', 'Solicitar certificado'),
            ('\u2610 Título de dominio', 'Confirmar propiedad vigente', 'Conservador de Bienes Raíces'),
            ('\u2610 Avalúo fiscal', 'Consultar valor actualizado', 'sii.cl o municipalidad'),
            ('\u2610 Estudio de suelo', 'Análisis técnico in situ', 'Contratar ing. agrónomo'),
            ('\u2610 Disponibilidad eléctrica', 'Verificar capacidad de empalme', 'Contactar distribuidora'),
            ('\u2610 Subsidios riego', 'Ley 18.450 disponibles', 'ley18450.cnr.gob.cl'),
            ('\u2610 Certificaciones SAG', 'Estado sanitario de la zona', 'sag.gob.cl'),
        ]
        headers = ['Verificación', 'Descripción', 'Acción']
        elements.append(_build_table(headers, defaults,
                                     col_widths=[4.5*cm, 6.5*cm, 5.5*cm],
                                     highlight_first_col=True))

    # Final text
    elements.append(Spacer(1, 0.5 * cm))
    texto_final = recs.get('texto_final',
        "Este informe consolida información de fuentes públicas oficiales vigente al "
        f"{datetime.now().strftime('%d/%m/%Y')}. Sin embargo, la información puede cambiar "
        "o no reflejar la situación actual del predio específico. Recomendamos complementar "
        "con visita en terreno y asesoría profesional."
    )
    elements.append(_build_alert_box(texto_final, 'info'))

    return elements


def _section_fuentes(report, styles):
    """Sección de fuentes y metodología."""
    elements = []
    elements.append(Spacer(1, 1 * cm))
    elements.append(HRFlowable(width="100%", thickness=0.5, color=GRIS_BORDE))
    elements.append(Spacer(1, 0.3 * cm))

    elements.append(Paragraph("FUENTES Y METODOLOGÍA", styles['SubSeccion']))
    fuentes = [
        "Catastro Frutícola Nacional — SAG / CIREN (Centro de Información de Recursos Naturales)",
        "Dirección General de Aguas (DGA) — Patentes de derechos de agua, Ministerio de Obras Públicas",
        "Superintendencia de Electricidad y Combustibles (SEC) — Clientes y potencia contratada",
        "CONAF — Corporación Nacional Forestal, Catastro de Vegetación Nativa",
        "Oficina de Estudios y Políticas Agrarias (ODEPA) — Estadísticas sectoriales",
        "Capas geoespaciales DGA: cuencas, restricciones, embalses, estaciones de monitoreo",
    ]
    for f in fuentes:
        elements.append(Paragraph(f"\u2022 {f}", styles['Pie']))

    elements.append(Spacer(1, 0.3 * cm))
    elements.append(Paragraph(
        "Los datos presentados corresponden a los registros más recientes disponibles en cada fuente. "
        "Este informe es generado automáticamente y tiene carácter referencial. "
        "No reemplaza una evaluación profesional en terreno.",
        styles['Pie']
    ))
    elements.append(Spacer(1, 0.5 * cm))
    elements.append(Paragraph(
        f"Generado el {datetime.now().strftime('%d/%m/%Y %H:%M')} — "
        "Informes Agrícolas Chile | Toro Energy",
        styles['Pie']
    ))
    return elements


# ══════════════════════════════════════════════════════════════════════════════
# FUNCIÓN PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════

def generate_predial_pdf(report: dict) -> bytes:
    """
    Genera un PDF completo del Informe Predial (8 secciones, estilo Autofact).

    Args:
        report: dict generado por predial_engine.generate_predial_report()

    Returns:
        bytes del PDF generado
    """
    buffer = io.BytesIO()

    doc = SimpleDocTemplate(
        buffer,
        pagesize=letter,
        leftMargin=2 * cm,
        rightMargin=2 * cm,
        topMargin=1.5 * cm,
        bottomMargin=2 * cm,
        title=f"Informe Predial — {report.get('comuna', 'N/D')}",
        author="Informes Agrícolas Chile — Toro Energy",
    )

    styles = _create_styles()
    page_width = letter[0] - 4 * cm

    elements = []

    # 1. Portada + Semáforo grid
    elements.extend(_section_portada(report, styles))

    # 2. Resumen Ejecutivo
    elements.extend(_section_resumen_ejecutivo(report, styles, page_width))

    # 3. Producción Agrícola
    elements.extend(_section_produccion(report, styles, page_width))

    # 4. Disponibilidad de Agua
    elements.extend(_section_agua(report, styles, page_width))

    # 5. Uso de Suelo
    elements.extend(_section_uso_suelo(report, styles, page_width))

    # 6. Infraestructura Eléctrica
    elements.extend(_section_electrico(report, styles, page_width))

    # 7. Riesgos Territoriales
    elements.extend(_section_riesgos(report, styles, page_width))

    # 8. Vecinos Productivos
    elements.extend(_section_vecinos(report, styles, page_width))

    # 9. Recomendaciones
    elements.extend(_section_recomendaciones(report, styles, page_width))

    # 10. Fuentes
    elements.extend(_section_fuentes(report, styles))

    doc.build(elements)
    pdf_bytes = buffer.getvalue()
    buffer.close()
    return pdf_bytes


# ── CLI test ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    sys.path.insert(0, os.path.dirname(__file__))
    from predial_engine import generate_predial_report

    comuna = sys.argv[1] if len(sys.argv) > 1 else "Rancagua"
    lat = float(sys.argv[2]) if len(sys.argv) > 2 else None
    lon = float(sys.argv[3]) if len(sys.argv) > 3 else None
    print(f"Generando PDF para {comuna}...")

    report = generate_predial_report(comuna, lat=lat, lon=lon)
    pdf_bytes = generate_predial_pdf(report)

    out_path = f"Informe_Predial_{comuna}.pdf"
    with open(out_path, 'wb') as f:
        f.write(pdf_bytes)
    print(f"PDF generado: {out_path} ({len(pdf_bytes):,} bytes)")
    print(f"Secciones: {report.get('secciones_disponibles', [])}")
    print(f"Semáforos: Prod={report.get('produccion',{}).get('semaforo','N/A')}, "
          f"Agua={report.get('agua',{}).get('semaforo','N/A')}, "
          f"Elec={report.get('electrico',{}).get('semaforo','N/A')}")
