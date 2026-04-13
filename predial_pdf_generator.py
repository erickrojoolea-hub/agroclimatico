"""
Generador de Informes Prediales PDF — Estilo "Autofact para Predios Agrícolas"
==============================================================================
Genera un documento profesional con:
  - Resumen ejecutivo (decisión en 2 minutos)
  - Semáforos hídricos (verde/amarillo/rojo)
  - Tablas con headers claros y filas alternadas
  - Gráficos de distribución de cultivos
  - Indicadores numéricos destacados
  - Comparativas (comuna vs promedio regional)

Usa ReportLab para PDF nativo.
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
from reportlab.graphics.shapes import Drawing, Rect, String, Circle
from reportlab.graphics.charts.piecharts import Pie
from reportlab.graphics import renderPDF


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
        'PredialTitulo',
        parent=styles['Title'],
        fontSize=24,
        textColor=VERDE_OSCURO,
        spaceAfter=4,
        alignment=TA_LEFT,
        fontName='Helvetica-Bold',
    ))
    styles.add(ParagraphStyle(
        'PredialSubtitulo',
        parent=styles['Heading2'],
        fontSize=12,
        textColor=GRIS_TEXTO,
        spaceAfter=16,
        alignment=TA_LEFT,
        fontName='Helvetica',
    ))
    styles.add(ParagraphStyle(
        'SeccionTitulo',
        parent=styles['Heading2'],
        fontSize=14,
        textColor=VERDE_OSCURO,
        spaceBefore=20,
        spaceAfter=8,
        fontName='Helvetica-Bold',
    ))
    styles.add(ParagraphStyle(
        'SubSeccion',
        parent=styles['Heading3'],
        fontSize=11,
        textColor=VERDE_PRIMARIO,
        spaceBefore=12,
        spaceAfter=4,
        fontName='Helvetica-Bold',
    ))
    styles.add(ParagraphStyle(
        'Cuerpo',
        parent=styles['Normal'],
        fontSize=9,
        leading=13,
        alignment=TA_JUSTIFY,
        spaceAfter=6,
        textColor=TEXTO_OSCURO,
    ))
    styles.add(ParagraphStyle(
        'CuerpoNegrita',
        parent=styles['Normal'],
        fontSize=9,
        leading=13,
        fontName='Helvetica-Bold',
        textColor=TEXTO_OSCURO,
    ))
    styles.add(ParagraphStyle(
        'Destacado',
        parent=styles['Normal'],
        fontSize=10,
        leading=14,
        fontName='Helvetica-Bold',
        textColor=VERDE_OSCURO,
        spaceBefore=4,
        spaceAfter=4,
    ))
    styles.add(ParagraphStyle(
        'KPI_Valor',
        parent=styles['Normal'],
        fontSize=22,
        fontName='Helvetica-Bold',
        textColor=VERDE_OSCURO,
        alignment=TA_CENTER,
        spaceAfter=0,
    ))
    styles.add(ParagraphStyle(
        'KPI_Label',
        parent=styles['Normal'],
        fontSize=8,
        textColor=GRIS_TEXTO,
        alignment=TA_CENTER,
        spaceAfter=2,
    ))
    styles.add(ParagraphStyle(
        'Pie',
        parent=styles['Normal'],
        fontSize=7,
        textColor=GRIS_TEXTO,
        leading=9,
    ))
    styles.add(ParagraphStyle(
        'SemaforoTexto',
        parent=styles['Normal'],
        fontSize=9,
        leading=12,
        alignment=TA_LEFT,
    ))

    return styles


def _format_clp(value):
    """Formatea un valor numérico como CLP."""
    if value is None or value == 0:
        return "N/D"
    if abs(value) >= 1_000_000_000:
        return f"${value / 1_000_000_000:,.1f} MM"
    if abs(value) >= 1_000_000:
        return f"${value / 1_000_000:,.1f} M"
    return f"${value:,.0f}"


def _semaforo_color(semaforo_str):
    """Retorna color ReportLab para un semáforo."""
    mapping = {
        'verde': VERDE_PRIMARIO,
        'amarillo': AMARILLO,
        'rojo': ROJO,
    }
    return mapping.get(semaforo_str, GRIS_TEXTO)


def _build_table(headers, rows, col_widths=None, highlight_first_col=False):
    """Construye una tabla ReportLab con estilo profesional."""
    all_rows = [headers] + rows
    t = Table(all_rows, colWidths=col_widths, repeatRows=1)

    style_cmds = [
        # Header
        ('BACKGROUND', (0, 0), (-1, 0), VERDE_OSCURO),
        ('TEXTCOLOR', (0, 0), (-1, 0), BLANCO),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 8),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 6),
        ('TOPPADDING', (0, 0), (-1, 0), 6),

        # Body
        ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 1), (-1, -1), 8),
        ('BOTTOMPADDING', (0, 1), (-1, -1), 4),
        ('TOPPADDING', (0, 1), (-1, -1), 4),
        ('TEXTCOLOR', (0, 1), (-1, -1), TEXTO_OSCURO),

        # Grid
        ('LINEBELOW', (0, 0), (-1, 0), 1.5, VERDE_OSCURO),
        ('LINEBELOW', (0, 1), (-1, -1), 0.5, GRIS_BORDE),

        # Alignment
        ('ALIGN', (1, 0), (-1, -1), 'RIGHT'),
        ('ALIGN', (0, 0), (0, -1), 'LEFT'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
    ]

    # Alternating row colors
    for i in range(1, len(all_rows)):
        if i % 2 == 0:
            style_cmds.append(('BACKGROUND', (0, i), (-1, i), GRIS_FONDO))

    # Highlight first column
    if highlight_first_col:
        style_cmds.append(('FONTNAME', (0, 1), (0, -1), 'Helvetica-Bold'))
        style_cmds.append(('TEXTCOLOR', (0, 1), (0, -1), VERDE_OSCURO))

    t.setStyle(TableStyle(style_cmds))
    return t


def _chart_to_image(fig, width_cm=16, dpi=150):
    """Convierte una figura matplotlib a Image de ReportLab."""
    buf = io.BytesIO()
    fig.savefig(buf, format='png', dpi=dpi, bbox_inches='tight',
                facecolor='white', edgecolor='none')
    buf.seek(0)
    plt.close(fig)

    # Save to temp file (ReportLab needs a file path or uses PIL)
    tmp = tempfile.NamedTemporaryFile(suffix='.png', delete=False)
    tmp.write(buf.read())
    tmp.close()

    img = Image(tmp.name, width=width_cm * cm, height=None)
    # Maintain aspect ratio
    img._restrictSize(width_cm * cm, 20 * cm)
    return img


def _build_semaforo_box(color_str, label, description):
    """Construye un indicador visual de semáforo."""
    color_map = {
        'verde': ('#2E7D32', '#E8F5E9', 'Favorable'),
        'amarillo': ('#F9A825', '#FFFDE7', 'Precaución'),
        'rojo': ('#C62828', '#FFEBEE', 'Crítico'),
        'gris': ('#9E9E9E', '#F5F5F5', 'Sin datos'),
    }
    hex_color, bg_hex, default_label = color_map.get(color_str, color_map['gris'])

    display_label = label or default_label
    bg = colors.HexColor(bg_hex)
    fg = colors.HexColor(hex_color)

    data = [[
        Paragraph(f'<font color="{hex_color}" size="14">\u25CF</font>', ParagraphStyle('s', alignment=TA_CENTER)),
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
        ('ROUNDEDCORNERS', [4, 4, 4, 4]),
    ]))
    return t


def _build_kpi_row(kpis, page_width):
    """Construye una fila de KPIs (valor + label)."""
    n = len(kpis)
    col_w = page_width / n

    cells = []
    for valor, label, unidad in kpis:
        cell_content = Paragraph(
            f'<font size="20" color="#2E7D32"><b>{valor}</b></font>'
            f'<font size="8" color="#616161"> {unidad}</font><br/>'
            f'<font size="8" color="#616161">{label}</font>',
            ParagraphStyle('kpi', alignment=TA_CENTER, leading=14, spaceBefore=4, spaceAfter=4)
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


# ══════════════════════════════════════════════════════════════════════════════
# SECCIONES DEL INFORME
# ══════════════════════════════════════════════════════════════════════════════

def _section_portada(report, styles):
    """Portada del informe."""
    elements = []
    elements.append(Spacer(1, 2 * cm))

    # Línea decorativa superior
    elements.append(HRFlowable(width="100%", thickness=3, color=VERDE_OSCURO))
    elements.append(Spacer(1, 0.5 * cm))

    elements.append(Paragraph("INFORME PREDIAL", styles['PredialTitulo']))
    elements.append(Paragraph(
        f"Análisis Territorial — {report['comuna']}",
        styles['PredialSubtitulo']
    ))

    elements.append(Spacer(1, 0.3 * cm))
    elements.append(HRFlowable(width="100%", thickness=1, color=VERDE_PRIMARIO))
    elements.append(Spacer(1, 1.5 * cm))

    # Info box
    region = report.get('region', 'N/D')
    provincia = report.get('provincia', 'N/D')
    fecha = datetime.now().strftime('%d de %B de %Y')
    secciones = report.get('secciones_disponibles', [])

    info_data = [
        ['Comuna', report['comuna']],
        ['Región', region],
        ['Provincia', provincia],
        ['Fecha del informe', fecha],
        ['Secciones incluidas', ', '.join(secciones) if secciones else 'Sin datos'],
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

    elements.append(Spacer(1, 2 * cm))

    # Disclaimer
    elements.append(Paragraph(
        "Este informe fue generado automáticamente a partir de bases de datos públicas "
        "(SAG, DGA, SEC, ODEPA). La información es referencial y no constituye una "
        "evaluación técnica vinculante. Se recomienda complementar con inspección en terreno.",
        styles['Pie']
    ))

    elements.append(PageBreak())
    return elements


def _section_resumen_ejecutivo(report, styles, page_width):
    """Resumen ejecutivo: decisión en 2 minutos."""
    elements = []
    elements.append(Paragraph("1. RESUMEN EJECUTIVO", styles['SeccionTitulo']))
    elements.append(HRFlowable(width="100%", thickness=1.5, color=VERDE_OSCURO))
    elements.append(Spacer(1, 0.3 * cm))

    prod = report['produccion']
    agua = report['agua']
    elec = report['electrico']

    # KPIs principales
    kpis = []
    if prod['disponible']:
        kpis.append((f"{prod['total_superficie_ha']:,.0f}", "Superficie Frutícola", "ha"))
        kpis.append((str(len(prod['especies'])), "Especies", ""))
    if agua['disponible']:
        kpis.append((f"{agua['num_patentes']:,}", "Patentes de Agua", ""))
    if elec['disponible']:
        kpis.append((f"{elec['total_clientes']:,}", "Clientes Eléctricos", ""))

    if kpis:
        elements.append(_build_kpi_row(kpis[:4], page_width))
        elements.append(Spacer(1, 0.5 * cm))

    # Narrative summary
    narrativa_parts = []
    narrativa_parts.append(
        f"La comuna de <b>{report['comuna']}</b>"
        + (f", ubicada en la {report['region']}" if report.get('region') else "")
        + ", presenta el siguiente perfil territorial:"
    )

    if prod['disponible'] and prod['especies']:
        top3 = prod['especies'][:3]
        top3_text = ', '.join(f"{e['especie']} ({e['superficie_ha']:,.0f} ha)" for e in top3)
        narrativa_parts.append(
            f"La actividad frutícola abarca <b>{prod['total_superficie_ha']:,.0f} hectáreas</b> "
            f"distribuidas en <b>{len(prod['especies'])} especies</b>. "
            f"Los cultivos principales son: {top3_text}."
        )

    if agua['disponible']:
        narrativa_parts.append(
            f"Existen <b>{agua['num_patentes']:,} patentes de agua</b> registradas en la DGA "
            f"con un monto total de {_format_clp(agua['monto_total'])}."
        )

    if elec['disponible']:
        narrativa_parts.append(
            f"La infraestructura eléctrica conecta <b>{elec['total_clientes']:,} clientes</b> "
            f"con una potencia total de {elec['potencia_total_kw']:,.0f} kW "
            f"(promedio {elec['potencia_promedio_kw']:.1f} kW/cliente)."
        )

    for part in narrativa_parts:
        elements.append(Paragraph(part, styles['Cuerpo']))

    # Semáforo hídrico
    elements.append(Spacer(1, 0.4 * cm))
    elements.append(Paragraph("Indicador de Situación Hídrica", styles['SubSeccion']))

    if agua['disponible'] and agua['monto_total'] and agua['monto_total'] > 0:
        ratio = agua['saldo_total'] / agua['monto_total']
        if ratio < 0.3:
            sem_color, sem_label = 'verde', 'Favorable'
            sem_desc = (f"Ratio saldo/monto: {ratio:.1%}. La comuna presenta un bajo nivel de impago "
                        "en patentes de agua, lo que sugiere buena disponibilidad y capacidad de pago.")
        elif ratio < 0.6:
            sem_color, sem_label = 'amarillo', 'Precaución'
            sem_desc = (f"Ratio saldo/monto: {ratio:.1%}. Existe un nivel moderado de saldos pendientes. "
                        "Se recomienda verificar disponibilidad hídrica efectiva antes de invertir.")
        else:
            sem_color, sem_label = 'rojo', 'Crítico'
            sem_desc = (f"Ratio saldo/monto: {ratio:.1%}. Alto nivel de impago en patentes de agua, "
                        "lo que puede indicar estrés hídrico o restricciones de uso.")
    else:
        sem_color, sem_label = 'gris', 'Sin datos'
        sem_desc = "No hay suficientes datos de patentes de agua para determinar la situación hídrica."

    elements.append(_build_semaforo_box(sem_color, sem_label, sem_desc))

    elements.append(PageBreak())
    return elements


def _section_produccion(report, styles, page_width):
    """Sección de producción agrícola."""
    elements = []
    prod = report['produccion']

    elements.append(Paragraph("2. PRODUCCIÓN AGRÍCOLA", styles['SeccionTitulo']))
    elements.append(HRFlowable(width="100%", thickness=1.5, color=VERDE_OSCURO))
    elements.append(Spacer(1, 0.3 * cm))

    if not prod['disponible']:
        elements.append(Paragraph(
            "No se encontraron datos de catastro frutícola para esta comuna en las bases de datos consultadas.",
            styles['Cuerpo']
        ))
        return elements

    elements.append(Paragraph(
        f"Según el Catastro Frutícola Nacional (SAG/CIREN), la comuna de {report['comuna']} "
        f"registra <b>{prod['total_superficie_ha']:,.0f} hectáreas</b> de superficie frutícola "
        f"distribuidas en <b>{len(prod['especies'])} especies</b>.",
        styles['Cuerpo']
    ))

    # Chart: top species horizontal bars
    if prod['especies']:
        elements.append(Spacer(1, 0.3 * cm))
        elements.append(Paragraph("Distribución por Especie", styles['SubSeccion']))

        top = prod['especies'][:12]
        names = [e['especie'] for e in top]
        areas = [e['superficie_ha'] for e in top]

        fig, ax = plt.subplots(figsize=(7, max(2.5, len(names) * 0.32)))
        y_pos = range(len(names))
        bars = ax.barh(y_pos, areas, color='#4CAF50', edgecolor='#2E7D32', linewidth=0.5, height=0.65)
        ax.set_yticks(y_pos)
        ax.set_yticklabels(names, fontsize=8)
        ax.invert_yaxis()
        ax.set_xlabel('Superficie (ha)', fontsize=8)
        ax.grid(True, alpha=0.2, axis='x')
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        for bar, val in zip(bars, areas):
            ax.text(bar.get_width() + max(areas) * 0.02,
                    bar.get_y() + bar.get_height() / 2,
                    f'{val:,.0f} ha', va='center', fontsize=7, color='#333')
        plt.tight_layout()
        elements.append(_chart_to_image(fig, width_cm=15))

        # Table
        elements.append(Spacer(1, 0.3 * cm))
        headers = ['Especie', 'Superficie (ha)', 'N° Explotaciones', '% del Total']
        total_ha = prod['total_superficie_ha'] or 1
        rows = []
        for e in prod['especies']:
            pct = (e['superficie_ha'] / total_ha) * 100
            rows.append([
                e['especie'],
                f"{e['superficie_ha']:,.1f}",
                str(e['num_explotaciones']),
                f"{pct:.1f}%",
            ])
        # Total row
        rows.append([
            'TOTAL',
            f"{prod['total_superficie_ha']:,.1f}",
            str(prod['total_explotaciones']),
            '100.0%',
        ])
        elements.append(_build_table(headers, rows,
                                     col_widths=[6*cm, 3.5*cm, 3.5*cm, 3*cm],
                                     highlight_first_col=True))

    # Irrigation methods
    if prod['metodos_riego']:
        elements.append(Spacer(1, 0.5 * cm))
        elements.append(Paragraph("Métodos de Riego", styles['SubSeccion']))

        headers = ['Método', 'Superficie (ha)', 'Registros']
        rows = [[r['metodo'], f"{r['superficie_ha']:,.1f}", str(r['registros'])]
                for r in prod['metodos_riego']]
        elements.append(_build_table(headers, rows,
                                     col_widths=[8*cm, 4*cm, 4*cm],
                                     highlight_first_col=True))

    # Top varieties
    if prod['variedades']:
        elements.append(Spacer(1, 0.5 * cm))
        elements.append(Paragraph("Principales Variedades", styles['SubSeccion']))

        headers = ['Especie', 'Variedad', 'Superficie (ha)']
        rows = [[v['especie'], v['variedad'], f"{v['superficie_ha']:,.1f}"]
                for v in prod['variedades'][:20]]
        elements.append(_build_table(headers, rows,
                                     col_widths=[5*cm, 6*cm, 5*cm],
                                     highlight_first_col=True))

    elements.append(PageBreak())
    return elements


def _section_agua(report, styles, page_width):
    """Sección de derechos de agua."""
    elements = []
    agua = report['agua']

    elements.append(Paragraph("3. DERECHOS DE AGUA", styles['SeccionTitulo']))
    elements.append(HRFlowable(width="100%", thickness=1.5, color=VERDE_OSCURO))
    elements.append(Spacer(1, 0.3 * cm))

    if not agua['disponible']:
        elements.append(Paragraph(
            "No se encontraron registros de patentes de agua (DGA) para esta comuna.",
            styles['Cuerpo']
        ))
        return elements

    # KPIs agua
    kpis = [
        (f"{agua['num_patentes']:,}", "Patentes Registradas", ""),
        (_format_clp(agua['monto_total']), "Monto Total", "CLP"),
        (_format_clp(agua['saldo_total']), "Saldo Pendiente", "CLP"),
    ]
    elements.append(_build_kpi_row(kpis, page_width))
    elements.append(Spacer(1, 0.3 * cm))

    elements.append(Paragraph(
        f"La Dirección General de Aguas (DGA) registra <b>{agua['num_patentes']:,} patentes</b> "
        f"de agua en la comuna de {report['comuna']}, perteneciente a la "
        f"{agua.get('region', 'región no identificada')}. "
        f"El monto total asociado es de {_format_clp(agua['monto_total'])}, "
        f"con un saldo pendiente de {_format_clp(agua['saldo_total'])}.",
        styles['Cuerpo']
    ))

    # By person type
    if agua['por_tipo']:
        elements.append(Spacer(1, 0.3 * cm))
        elements.append(Paragraph("Distribución por Tipo de Titular", styles['SubSeccion']))
        headers = ['Tipo', 'Cantidad', 'Saldo Total']
        rows = [[t['tipo'] or 'N/D', str(t['cantidad']), _format_clp(t['saldo'])]
                for t in agua['por_tipo']]
        elements.append(_build_table(headers, rows,
                                     col_widths=[6*cm, 4*cm, 6*cm],
                                     highlight_first_col=True))

    # Evolution by year
    if agua['por_anio'] and len(agua['por_anio']) > 1:
        elements.append(Spacer(1, 0.5 * cm))
        elements.append(Paragraph("Evolución Anual de Patentes", styles['SubSeccion']))

        anios = [str(a['anio']) for a in agua['por_anio']]
        cantidades = [a['cantidad'] for a in agua['por_anio']]

        fig, ax = plt.subplots(figsize=(7, 3))
        ax.bar(range(len(anios)), cantidades, color='#42A5F5', edgecolor='#1565C0', linewidth=0.5)
        ax.set_xticks(range(len(anios)))
        ax.set_xticklabels(anios, fontsize=8, rotation=45)
        ax.set_ylabel('N° Patentes', fontsize=8)
        ax.grid(True, alpha=0.2, axis='y')
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        plt.tight_layout()
        elements.append(_chart_to_image(fig, width_cm=14))

    elements.append(PageBreak())
    return elements


def _section_electrico(report, styles, page_width):
    """Sección de infraestructura eléctrica."""
    elements = []
    elec = report['electrico']

    elements.append(Paragraph("4. INFRAESTRUCTURA ELÉCTRICA", styles['SeccionTitulo']))
    elements.append(HRFlowable(width="100%", thickness=1.5, color=VERDE_OSCURO))
    elements.append(Spacer(1, 0.3 * cm))

    if not elec['disponible']:
        elements.append(Paragraph(
            "No se encontraron datos de infraestructura eléctrica para esta comuna.",
            styles['Cuerpo']
        ))
        return elements

    # KPIs
    kpis = [
        (f"{elec['total_clientes']:,}", "Clientes Conectados", ""),
        (f"{elec['potencia_total_kw']:,.0f}", "Potencia Total", "kW"),
        (f"{elec['potencia_promedio_kw']:.1f}", "Pot. Promedio", "kW/cliente"),
    ]
    elements.append(_build_kpi_row(kpis, page_width))
    elements.append(Spacer(1, 0.3 * cm))

    elements.append(Paragraph(
        f"La comuna cuenta con <b>{elec['total_clientes']:,} clientes eléctricos</b> "
        f"conectados, con una potencia total contratada de "
        f"<b>{elec['potencia_total_kw']:,.0f} kW</b> "
        f"y un promedio de {elec['potencia_promedio_kw']:.1f} kW por cliente.",
        styles['Cuerpo']
    ))

    # By company
    if elec['empresas']:
        elements.append(Spacer(1, 0.3 * cm))
        elements.append(Paragraph("Distribuidoras Eléctricas", styles['SubSeccion']))
        headers = ['Empresa', 'Clientes', 'Potencia (kW)']
        rows = [[e['empresa'], str(e['clientes']), f"{e['potencia_kw']:,.1f}"]
                for e in elec['empresas']]
        elements.append(_build_table(headers, rows,
                                     col_widths=[8*cm, 4*cm, 4*cm],
                                     highlight_first_col=True))

    return elements


def _section_fuentes(report, styles):
    """Sección de fuentes y metodología."""
    elements = []
    elements.append(Spacer(1, 1 * cm))
    elements.append(HRFlowable(width="100%", thickness=0.5, color=GRIS_BORDE))
    elements.append(Spacer(1, 0.3 * cm))

    elements.append(Paragraph("FUENTES Y METODOLOGÍA", styles['SubSeccion']))
    fuentes = [
        "Catastro Frutícola Nacional — SAG / CIREN",
        "Dirección General de Aguas (DGA) — Patentes de derechos de agua",
        "Superintendencia de Electricidad y Combustibles (SEC) — Clientes y potencia contratada",
        "Oficina de Estudios y Políticas Agrarias (ODEPA) — Estadísticas sectoriales",
    ]
    for f in fuentes:
        elements.append(Paragraph(f"• {f}", styles['Pie']))

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
    Genera un PDF completo del Informe Predial.

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
        author="Informes Agrícolas Chile",
    )

    styles = _create_styles()
    page_width = letter[0] - 4 * cm  # usable width

    elements = []

    # 1. Portada
    elements.extend(_section_portada(report, styles))

    # 2. Resumen Ejecutivo
    elements.extend(_section_resumen_ejecutivo(report, styles, page_width))

    # 3. Producción Agrícola
    elements.extend(_section_produccion(report, styles, page_width))

    # 4. Agua
    elements.extend(_section_agua(report, styles, page_width))

    # 5. Eléctrico
    elements.extend(_section_electrico(report, styles, page_width))

    # 6. Fuentes
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
    print(f"Generando PDF para {comuna}...")

    report = generate_predial_report(comuna)
    pdf_bytes = generate_predial_pdf(report)

    out_path = f"Informe_Predial_{comuna}.pdf"
    with open(out_path, 'wb') as f:
        f.write(pdf_bytes)
    print(f"PDF generado: {out_path} ({len(pdf_bytes):,} bytes)")
