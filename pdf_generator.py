"""
Generador de informes PDF estilo Fernando Santibáñez / INFODEP.
Usa ReportLab para crear un PDF profesional con tablas y gráficos.
"""
import io
import os
import tempfile
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import seaborn as sns
import numpy as np
import pandas as pd
from datetime import datetime

from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.units import cm, mm
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    PageBreak, Image, KeepTogether
)
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_JUSTIFY


# ── Colores tema agro ────────────────────────────────────────────────────────
VERDE_OSCURO = colors.HexColor('#2E7D32')
VERDE_CLARO = colors.HexColor('#C8E6C9')
VERDE_MEDIO = colors.HexColor('#66BB6A')
GRIS = colors.HexColor('#F5F5F5')
BLANCO = colors.white
NEGRO = colors.black


def create_styles():
    """Crea estilos de párrafo para el PDF."""
    styles = getSampleStyleSheet()

    styles.add(ParagraphStyle(
        'TituloInforme',
        parent=styles['Title'],
        fontSize=22,
        textColor=VERDE_OSCURO,
        spaceAfter=20,
        alignment=TA_CENTER,
    ))
    styles.add(ParagraphStyle(
        'SubtituloInforme',
        parent=styles['Heading2'],
        fontSize=14,
        textColor=VERDE_OSCURO,
        spaceAfter=10,
        alignment=TA_CENTER,
    ))
    styles.add(ParagraphStyle(
        'SeccionTitulo',
        parent=styles['Heading2'],
        fontSize=13,
        textColor=VERDE_OSCURO,
        spaceBefore=15,
        spaceAfter=8,
        borderWidth=1,
        borderColor=VERDE_OSCURO,
        borderPadding=4,
    ))
    styles.add(ParagraphStyle(
        'SubSeccion',
        parent=styles['Heading3'],
        fontSize=11,
        textColor=VERDE_OSCURO,
        spaceBefore=10,
        spaceAfter=5,
        fontName='Helvetica-Bold',
    ))
    styles.add(ParagraphStyle(
        'CuerpoTexto',
        parent=styles['Normal'],
        fontSize=9,
        leading=12,
        alignment=TA_JUSTIFY,
        spaceAfter=6,
    ))
    styles.add(ParagraphStyle(
        'NotaPie',
        parent=styles['Normal'],
        fontSize=7,
        textColor=colors.gray,
        leading=9,
    ))
    return styles


def make_monthly_table(monthly_df):
    """
    Crea la tabla principal de resumen mensual (estilo Santibáñez).
    """
    # Columnas a mostrar
    display_cols = [
        'T.MAX', 'T.MIN', 'T.MED', 'DIAS GRADO', 'DIAS GRA12',
        'DG.ACUM', 'D-cálidos', 'HRS.FRIO', 'HRS.FRES', 'HF.ACUM',
        'R.SOLAR', 'H.RELAT', 'PRECIPIT', 'EVAP.POT',
        'DEF.HIDR', 'EXC.HIDR', 'IND.HUMED', 'HELADAS'
    ]
    units = [
        '°C', '°C', '°C', 'D.G', 'D.G',
        'D.G', 'Días', 'Hrs', 'Hrs', 'Hrs',
        'Ly/día', '%', 'mm', 'mm',
        'mm', 'mm', 'pp/etp', 'días'
    ]

    month_labels = ['ENE', 'FEB', 'MAR', 'ABR', 'MAY', 'JUN',
                    'JUL', 'AGO', 'SEP', 'OCT', 'NOV', 'DIC', 'ANUAL']

    # Encabezado
    header = ['PARÁMETRO'] + month_labels + ['UNID.']

    # Filas
    rows = [header]
    for i, col in enumerate(display_cols):
        row = [col]
        for _, r in monthly_df.iterrows():
            val = r.get(col, '')
            if pd.isna(val):
                row.append('*')
            elif isinstance(val, float):
                if val == int(val):
                    row.append(str(int(val)))
                else:
                    row.append(f'{val:.1f}')
            else:
                row.append(str(val))
        row.append(units[i])
        rows.append(row)

    # Estilo tabla
    style = TableStyle([
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 5.5),
        ('FONTSIZE', (0, 0), (0, -1), 5.5),
        ('ALIGN', (1, 0), (-1, -1), 'CENTER'),
        ('ALIGN', (0, 0), (0, -1), 'LEFT'),
        ('BACKGROUND', (0, 0), (-1, 0), VERDE_OSCURO),
        ('TEXTCOLOR', (0, 0), (-1, 0), BLANCO),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [BLANCO, GRIS]),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('TOPPADDING', (0, 0), (-1, -1), 2),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 2),
        ('LEFTPADDING', (0, 0), (-1, -1), 2),
        ('RIGHTPADDING', (0, 0), (-1, -1), 2),
        # Columna ANUAL en negrita
        ('FONTNAME', (13, 0), (13, -1), 'Helvetica-Bold'),
        ('BACKGROUND', (13, 1), (13, -1), VERDE_CLARO),
    ])

    col_widths = [55] + [35]*13 + [30]
    t = Table(rows, colWidths=col_widths, repeatRows=1)
    t.setStyle(style)
    return t


def make_dias_calidos_table(dc_df):
    """Tabla de días cálidos por umbral."""
    header = ['PARÁMETRO'] + ['ENE', 'FEB', 'MAR', 'ABR', 'MAY', 'JUN',
                               'JUL', 'AGO', 'SEP', 'OCT', 'NOV', 'DIC', 'ANUAL'] + ['UNID.']
    rows = [header]
    for _, r in dc_df.iterrows():
        row = [r['UMBRAL']]
        for m in ['ENE', 'FEB', 'MAR', 'ABR', 'MAY', 'JUN',
                  'JUL', 'AGO', 'SEP', 'OCT', 'NOV', 'DIC', 'ANUAL']:
            val = r.get(m, 0)
            row.append(f'{val:.1f}' if isinstance(val, float) else str(val))
        row.append('°C')
        rows.append(row)

    style = TableStyle([
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 6.5),
        ('ALIGN', (1, 0), (-1, -1), 'CENTER'),
        ('BACKGROUND', (0, 0), (-1, 0), VERDE_OSCURO),
        ('TEXTCOLOR', (0, 0), (-1, 0), BLANCO),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [BLANCO, GRIS]),
        ('TOPPADDING', (0, 0), (-1, -1), 2),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 2),
    ])

    col_widths = [55] + [35]*13 + [30]
    t = Table(rows, colWidths=col_widths)
    t.setStyle(style)
    return t


def make_heladas_table(hel_df):
    """Tabla de heladas por intensidad."""
    header = ['UMBRAL'] + ['ENE', 'FEB', 'MAR', 'ABR', 'MAY', 'JUN',
                            'JUL', 'AGO', 'SEP', 'OCT', 'NOV', 'DIC', 'ANUAL']
    rows = [header]
    for _, r in hel_df.iterrows():
        row = [r['UMBRAL']]
        for m in ['ENE', 'FEB', 'MAR', 'ABR', 'MAY', 'JUN',
                  'JUL', 'AGO', 'SEP', 'OCT', 'NOV', 'DIC', 'ANUAL']:
            val = r.get(m, 0)
            row.append(f'{val:.1f}' if isinstance(val, float) else str(val))
        rows.append(row)

    style = TableStyle([
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 6.5),
        ('ALIGN', (1, 0), (-1, -1), 'CENTER'),
        ('BACKGROUND', (0, 0), (-1, 0), VERDE_OSCURO),
        ('TEXTCOLOR', (0, 0), (-1, 0), BLANCO),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [BLANCO, GRIS]),
        ('TOPPADDING', (0, 0), (-1, -1), 2),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 2),
    ])

    col_widths = [45] + [37]*13
    t = Table(rows, colWidths=col_widths)
    t.setStyle(style)
    return t


def make_bioclimatic_table(bio_df, especie_nombre):
    """Tabla bioclimática por especie."""
    header = ['VARIABLE', 'VALOR', 'UNIDAD', 'VALOR RECOMENDABLE', 'Riesgo']
    rows = [header]
    for _, r in bio_df.iterrows():
        riesgo_str = ''
        if r['Riesgo'] != '' and r['Riesgo'] != 0:
            riesgo_str = str(r['Riesgo'])
        rows.append([
            str(r['VARIABLE']),
            str(r['VALOR']),
            str(r['UNIDAD']),
            str(r['VALOR RECOMENDABLE']),
            riesgo_str
        ])

    style = TableStyle([
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 7.5),
        ('ALIGN', (1, 0), (-2, -1), 'CENTER'),
        ('BACKGROUND', (0, 0), (-1, 0), VERDE_OSCURO),
        ('TEXTCOLOR', (0, 0), (-1, 0), BLANCO),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [BLANCO, GRIS]),
        ('TOPPADDING', (0, 0), (-1, -1), 3),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
    ])

    col_widths = [150, 55, 65, 120, 45]
    t = Table(rows, colWidths=col_widths)
    t.setStyle(style)
    return t


# ── Gráficos ─────────────────────────────────────────────────────────────────

def plot_temp_profile(monthly_df):
    """Gráfico de perfil térmico mensual."""
    fig, ax = plt.subplots(figsize=(7, 3.5))
    meses = ['E', 'F', 'M', 'A', 'M', 'J', 'J', 'A', 'S', 'O', 'N', 'D']
    x = range(12)
    data = monthly_df[monthly_df['MES'] != 'ANUAL']

    ax.fill_between(x, data['T.MIN'].values, data['T.MAX'].values, alpha=0.2, color='#2E7D32')
    ax.plot(x, data['T.MAX'].values, 'o-', color='#D32F2F', linewidth=2, label='T.Máx', markersize=5)
    ax.plot(x, data['T.MIN'].values, 's-', color='#1565C0', linewidth=2, label='T.Mín', markersize=5)
    ax.plot(x, data['T.MED'].values, '^--', color='#2E7D32', linewidth=1.5, label='T.Med', markersize=4)
    ax.axhline(y=0, color='lightblue', linewidth=1, linestyle=':', alpha=0.7)

    ax.set_xticks(x)
    ax.set_xticklabels(meses)
    ax.set_ylabel('Temperatura (°C)')
    ax.set_title('Perfil Térmico Mensual', fontsize=11, fontweight='bold', color='#2E7D32')
    ax.legend(fontsize=8, loc='upper right')
    ax.grid(True, alpha=0.3)
    plt.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format='png', dpi=150, bbox_inches='tight')
    plt.close(fig)
    buf.seek(0)
    return buf


def plot_water_balance(monthly_df):
    """Gráfico balance hídrico: ETP vs Precipitación."""
    fig, ax = plt.subplots(figsize=(7, 3.5))
    meses = ['E', 'F', 'M', 'A', 'M', 'J', 'J', 'A', 'S', 'O', 'N', 'D']
    x = np.arange(12)
    data = monthly_df[monthly_df['MES'] != 'ANUAL']
    width = 0.35

    ax.bar(x - width/2, data['EVAP.POT'].values, width, label='ETP (mm)',
           color='#FF8A65', edgecolor='#E64A19', linewidth=0.5)
    ax.bar(x + width/2, data['PRECIPIT'].values, width, label='Precipitación (mm)',
           color='#4FC3F7', edgecolor='#0277BD', linewidth=0.5)

    ax.set_xticks(x)
    ax.set_xticklabels(meses)
    ax.set_ylabel('mm/mes')
    ax.set_title('Balance Hídrico Mensual (ETP vs Precipitación)', fontsize=11,
                 fontweight='bold', color='#2E7D32')
    ax.legend(fontsize=8)
    ax.grid(True, alpha=0.3, axis='y')
    plt.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format='png', dpi=150, bbox_inches='tight')
    plt.close(fig)
    buf.seek(0)
    return buf


def plot_degree_days(monthly_df):
    """Gráfico días-grado acumulados."""
    fig, ax = plt.subplots(figsize=(7, 3.5))
    data = monthly_df[monthly_df['MES'] != 'ANUAL']

    # Reordenar desde octubre
    order = [9, 10, 11, 0, 1, 2, 3, 4, 5, 6, 7, 8]  # índices oct→sep
    labels = ['O', 'N', 'D', 'E', 'F', 'M', 'A', 'M', 'J', 'J', 'A', 'S']
    dg_vals = [data.iloc[i]['DG.ACUM'] for i in order]

    ax.fill_between(range(12), dg_vals, alpha=0.3, color='#FF6F00')
    ax.plot(range(12), dg_vals, 'o-', color='#E65100', linewidth=2.5, markersize=6)

    ax.set_xticks(range(12))
    ax.set_xticklabels(labels)
    ax.set_ylabel('Días-Grado Acumulados (base 10°C)')
    ax.set_title('Acumulación de Días-Grado (Oct → Sep)', fontsize=11,
                 fontweight='bold', color='#2E7D32')
    ax.grid(True, alpha=0.3)
    plt.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format='png', dpi=150, bbox_inches='tight')
    plt.close(fig)
    buf.seek(0)
    return buf


def plot_frost_hours(monthly_df):
    """Gráfico horas de frío acumuladas."""
    fig, ax = plt.subplots(figsize=(7, 3.5))
    meses = ['E', 'F', 'M', 'A', 'M', 'J', 'J', 'A', 'S', 'O', 'N', 'D']
    data = monthly_df[monthly_df['MES'] != 'ANUAL']

    ax.bar(range(12), data['HRS.FRIO'].values, color='#42A5F5',
           edgecolor='#1565C0', linewidth=0.5, label='Hrs Frío (<7°C)')
    ax.bar(range(12), data['HRS.FRES'].values, color='#90CAF9',
           edgecolor='#42A5F5', linewidth=0.5, alpha=0.5, label='Hrs Frescor (<10°C)')

    ax.set_xticks(range(12))
    ax.set_xticklabels(meses)
    ax.set_ylabel('Horas')
    ax.set_title('Horas de Frío y Frescor Mensuales', fontsize=11,
                 fontweight='bold', color='#2E7D32')
    ax.legend(fontsize=8)
    ax.grid(True, alpha=0.3, axis='y')
    plt.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format='png', dpi=150, bbox_inches='tight')
    plt.close(fig)
    buf.seek(0)
    return buf


def plot_solar_radiation(monthly_df):
    """Gráfico de radiación solar mensual."""
    fig, ax = plt.subplots(figsize=(7, 3))
    meses = ['E', 'F', 'M', 'A', 'M', 'J', 'J', 'A', 'S', 'O', 'N', 'D']
    data = monthly_df[monthly_df['MES'] != 'ANUAL']

    ax.bar(range(12), data['R.SOLAR'].values, color='#FFD54F',
           edgecolor='#F57F17', linewidth=0.8)
    ax.set_xticks(range(12))
    ax.set_xticklabels(meses)
    ax.set_ylabel('cal/cm² día')
    ax.set_title('Radiación Solar Mensual', fontsize=11,
                 fontweight='bold', color='#2E7D32')
    ax.grid(True, alpha=0.3, axis='y')
    plt.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format='png', dpi=150, bbox_inches='tight')
    plt.close(fig)
    buf.seek(0)
    return buf


# ── Generador PDF principal ──────────────────────────────────────────────────

def generate_pdf(localidad, lat, lon, alt, monthly_df, dc_df, hel_df,
                 bio_tables, analisis_texts, winkler, fototermico,
                 nombre_predio=""):
    """
    Genera el PDF completo estilo Santibáñez.

    Args:
        localidad: nombre del lugar
        lat, lon, alt: coordenadas y altitud
        monthly_df: DataFrame con variables mensuales
        dc_df: DataFrame días cálidos
        hel_df: DataFrame heladas por intensidad
        bio_tables: dict {especie: DataFrame bioclimático}
        analisis_texts: dict {especie: texto análisis}
        winkler: valor índice Winkler
        fototermico: valor índice fototérmico
        nombre_predio: nombre opcional del predio

    Returns:
        bytes del PDF generado
    """
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=letter,
        leftMargin=1.5*cm,
        rightMargin=1.5*cm,
        topMargin=2*cm,
        bottomMargin=2*cm
    )
    styles = create_styles()
    elements = []

    # ── Portada ──────────────────────────────────────────────────────────
    elements.append(Spacer(1, 80))
    elements.append(Paragraph('Análisis Agroclimático', styles['TituloInforme']))
    elements.append(Paragraph(localidad, styles['TituloInforme']))
    if nombre_predio:
        elements.append(Paragraph(f'Predio: {nombre_predio}', styles['SubtituloInforme']))
    elements.append(Spacer(1, 30))
    elements.append(Paragraph(
        f'Coordenadas: {lat:.4f}°S, {abs(lon):.4f}°O &nbsp;|&nbsp; Altitud: {alt}m',
        styles['SubtituloInforme']
    ))
    elements.append(Spacer(1, 20))
    elements.append(Paragraph(
        'Informe generado con datos PVsyst (Meteonorm 8.2)<br/>'
        'Análisis de datos horarios 8760h (TMY)',
        styles['SubtituloInforme']
    ))
    elements.append(Spacer(1, 60))
    elements.append(Paragraph(
        f'Santiago, {datetime.now().strftime("%B %Y").capitalize()}',
        styles['SubtituloInforme']
    ))
    elements.append(PageBreak())

    # ── Información metodológica ─────────────────────────────────────────
    elements.append(Paragraph('Información necesaria para la interpretación del informe',
                              styles['SeccionTitulo']))
    elements.append(Paragraph(
        'El diagnóstico presentado consiste en un perfil agroclimático completo que incluye '
        'los valores mensuales de las temperaturas, los días grado, las horas de frío, la '
        'radiación solar, la humedad relativa, las precipitaciones, la evapotranspiración '
        'potencial y otros índices climáticos que ayudan a la evaluación del potencial '
        'productivo del sector.',
        styles['CuerpoTexto']
    ))
    elements.append(Paragraph(
        'Los valores presentados corresponden a un Año Meteorológico Típico (TMY) generado '
        'por Meteonorm 8.2, basado en datos satelitales y de estaciones meteorológicas del '
        'período 2010-2019. La resolución temporal es horaria (8760 horas), lo que permite '
        'un cálculo preciso de las horas de frío, horas de frescor, y días-grado mediante '
        'integración horaria directa, en lugar de aproximaciones basadas en temperaturas '
        'máximas y mínimas diarias.',
        styles['CuerpoTexto']
    ))
    elements.append(Paragraph(
        'Los datos de precipitación provienen de registros históricos de estaciones '
        'meteorológicas cercanas (fuentes: CR2, Meteochile). Los datos de humedad relativa '
        'se estiman a partir de patrones climatológicos regionales.',
        styles['CuerpoTexto']
    ))
    elements.append(Spacer(1, 10))

    # Variables definidas
    elements.append(Paragraph('VARIABLES CLIMÁTICAS DESCRITAS EN EL ESTUDIO',
                              styles['SeccionTitulo']))
    var_defs = [
        ('<b>T.MAX</b>: Temperatura máxima media del mes. Promedio mensual de las máximas diarias.',),
        ('<b>T.MIN</b>: Temperatura mínima media del mes. Promedio mensual de las mínimas diarias.',),
        ('<b>T.MED</b>: Temperatura media del mes.',),
        ('<b>DIAS GRADO</b>: Días-grado mensuales (T>10°C). Índice de disponibilidad de calor para el desarrollo vegetal.',),
        ('<b>DG.ACUM</b>: Días-grado acumulados a partir del 1 de octubre.',),
        ('<b>D-cálidos</b>: Número de días con Tmax >25°C.',),
        ('<b>HRS.FRIO</b>: Horas mensuales con T<7°C. Umbral para romper receso invernal en hoja caduca.',),
        ('<b>HRS.FRES</b>: Horas de frescor (T<10°C).',),
        ('<b>R.SOLAR</b>: Radiación solar mensual promedio diaria (cal/cm² día). Calculada a partir de GHI horario PVsyst.',),
        ('<b>H.RELAT</b>: Humedad relativa media mensual.',),
        ('<b>PRECIPIT</b>: Precipitación media mensual.',),
        ('<b>ETP</b>: Evapotranspiración potencial mensual (Hargreaves).',),
        ('<b>DEF.HIDR</b>: Déficit hídrico mensual = max(ETP - Precip, 0).',),
        ('<b>IND.HUMED</b>: Índice de humedad = Precip/ETP. Valores <0.5 indican necesidad de riego.',),
        ('<b>HELADAS</b>: Número promedio de heladas por mes (Tmin <0°C).',),
    ]
    for vd in var_defs:
        elements.append(Paragraph(vd[0], styles['CuerpoTexto']))

    elements.append(PageBreak())

    # ── Análisis agroclimático global ─────────────────────────────────────
    elements.append(Paragraph('Análisis agroclimático global', styles['SeccionTitulo']))

    annual = monthly_df[monthly_df['MES'] == 'ANUAL'].iloc[0]
    data = monthly_df[monthly_df['MES'] != 'ANUAL']

    tmax_ene = data.iloc[0]['T.MAX']
    tmin_ene = data.iloc[0]['T.MIN']
    tmax_jul = data.iloc[6]['T.MAX']
    tmin_jul = data.iloc[6]['T.MIN']

    heladas_total = annual['HELADAS']
    # Meses con heladas
    meses_heladas = []
    for _, r in data.iterrows():
        if r['HELADAS'] > 0:
            meses_heladas.append(r['MES'].lower())
    meses_hel_str = ' y '.join(meses_heladas) if meses_heladas else 'ningún mes'

    elements.append(Paragraph(
        f'Las temperaturas máximas se elevan en promedio hasta los {tmax_ene}°C en enero. '
        f'En julio estas descienden a {tmax_jul}°C. Las mínimas varían entre {tmin_ene}°C '
        f'y {tmin_jul}°C en los mismos meses. Durante el período estival ocurren '
        f'{int(annual["D-cálidos"])} días cálidos (días con temperaturas máximas por sobre 25°C). '
        f'La acumulación anual de días grado es de {int(annual["DIAS GRADO"])} días grado (T>10°C). '
        f'Las horas de frío al 31 de julio alcanzan a {int(data.iloc[6]["HF.ACUM"] or 0)}. '
        f'La radiación solar es elevada en verano llegando a {int(data.iloc[0]["R.SOLAR"])} (enero) '
        f'calorías/cm² día.',
        styles['CuerpoTexto']
    ))
    elements.append(Paragraph(
        f'La precipitación alcanza un promedio anual de {annual["PRECIPIT"]} mm. La '
        f'evapotranspiración alcanza su máximo en enero con alrededor de '
        f'{data.iloc[0]["EVAP.POT"]/data.iloc[0]["n_days"]:.1f} mm/día '
        f'({data.iloc[0]["EVAP.POT"]:.0f} mm/mes), totalizando {annual["EVAP.POT"]:.0f} mm por año.',
        styles['CuerpoTexto']
    ))

    if heladas_total > 0:
        elements.append(Paragraph(
            f'La estadística muestra que en el sector hay una incidencia de {heladas_total:.0f} '
            f'heladas anuales, las que se extienden entre {meses_hel_str}.',
            styles['CuerpoTexto']
        ))
    else:
        elements.append(Paragraph(
            'El sector presenta muy baja incidencia de heladas.',
            styles['CuerpoTexto']
        ))

    elements.append(Spacer(1, 5))

    # Índices clave
    elements.append(Paragraph(
        f'<b>Índice de Winkler:</b> {int(winkler)} días-grado (oct-mar). &nbsp;&nbsp; '
        f'<b>Índice Fototérmico:</b> {int(fototermico)}.',
        styles['CuerpoTexto']
    ))
    elements.append(PageBreak())

    # ── Tabla principal mensual ──────────────────────────────────────────
    elements.append(Paragraph(
        'RESUMEN DE VALORES MENSUALES PARA ALGUNOS PARÁMETROS CLIMÁTICOS',
        styles['SeccionTitulo']
    ))
    elements.append(make_monthly_table(monthly_df))
    elements.append(Spacer(1, 5))
    elements.append(Paragraph(
        'Días-grado acumulados a partir de octubre. Horas frío acumuladas desde mayo a diciembre. '
        'Suma de temperaturas efectivas base 10°C, horas de frío base 7°C. '
        'Días con heladas: temperatura mínima inferior a 0°C.',
        styles['NotaPie']
    ))
    elements.append(Spacer(1, 15))

    # ── Gráficos ─────────────────────────────────────────────────────────
    # Temperatura
    img_buf = plot_temp_profile(monthly_df)
    elements.append(Image(img_buf, width=16*cm, height=8*cm))
    elements.append(Spacer(1, 10))

    # Balance hídrico
    img_buf = plot_water_balance(monthly_df)
    elements.append(Image(img_buf, width=16*cm, height=8*cm))
    elements.append(PageBreak())

    # ── Días cálidos ─────────────────────────────────────────────────────
    elements.append(Paragraph('RESUMEN DE DÍAS CÁLIDOS MENSUALES', styles['SeccionTitulo']))
    elements.append(make_dias_calidos_table(dc_df))
    elements.append(Spacer(1, 15))

    # ── Heladas por intensidad ───────────────────────────────────────────
    elements.append(Paragraph(
        'NÚMERO DE HELADAS MENSUALES CON DISTINTAS INTENSIDADES (Cobertizo 1.5m)',
        styles['SeccionTitulo']
    ))
    elements.append(make_heladas_table(hel_df))
    elements.append(Spacer(1, 15))

    # Más gráficos
    img_buf = plot_degree_days(monthly_df)
    elements.append(Image(img_buf, width=16*cm, height=8*cm))
    elements.append(Spacer(1, 10))

    img_buf = plot_frost_hours(monthly_df)
    elements.append(Image(img_buf, width=16*cm, height=8*cm))
    elements.append(PageBreak())

    # Radiación solar
    img_buf = plot_solar_radiation(monthly_df)
    elements.append(Image(img_buf, width=16*cm, height=7*cm))
    elements.append(Spacer(1, 15))

    # ── Tablas bioclimáticas por especie ──────────────────────────────────
    elements.append(Paragraph('TABLAS BIOCLIMÁTICAS POR ESPECIE', styles['SeccionTitulo']))
    elements.append(Paragraph(
        'En esta sección se resume el grado de cumplimiento de las exigencias bioclimáticas '
        'de las diferentes especies en el sitio. La tabla entrega el valor obtenido por la '
        'especie y el valor recomendable para una producción segura.',
        styles['CuerpoTexto']
    ))
    elements.append(Spacer(1, 10))

    for esp_key, bio_df in bio_tables.items():
        from climate_engine import ESPECIES
        nombre = ESPECIES[esp_key]['nombre']
        elements.append(Paragraph(
            f'ÍNDICES BIOCLIMÁTICOS PARA {nombre.upper()}',
            styles['SubSeccion']
        ))
        elements.append(make_bioclimatic_table(bio_df, nombre))
        elements.append(Spacer(1, 5))

        # Análisis textual
        if esp_key in analisis_texts:
            for line in analisis_texts[esp_key].split('\n'):
                if line.strip():
                    elements.append(Paragraph(line.strip(), styles['CuerpoTexto']))
        elements.append(Spacer(1, 15))

    # ── Pie final ────────────────────────────────────────────────────────
    elements.append(Spacer(1, 20))
    elements.append(Paragraph(
        f'Informe generado automáticamente el {datetime.now().strftime("%d/%m/%Y %H:%M")}. '
        f'Fuente de datos meteorológicos: PVsyst / Meteonorm 8.2 (TMY 2010-2019). '
        f'Precipitación: registros históricos estaciones cercanas.',
        styles['NotaPie']
    ))

    # Build
    doc.build(elements)
    buffer.seek(0)
    return buffer.getvalue()
