"""
Motor de cálculo agroclimático.
Parsea datos PVsyst (8760h) y calcula todas las variables del informe Santibáñez.
"""
import pandas as pd
import numpy as np
from io import StringIO
import math


# ── Precipitación por defecto (fuentes: Wikipedia/CR2) ──────────────────────
# Diccionario: {nombre_localidad: [ene, feb, mar, ..., dic]}
PRECIP_DB = {
    "Curacaví": [1, 7, 5, 12, 78, 111, 84, 72, 22, 15, 7, 3],
    "Lolol": [7.9, 9.6, 13.6, 35.6, 132.2, 158.7, 131.2, 110.3, 44.6, 26.3, 14.6, 10.4],
    "Santiago": [0.4, 0.8, 3.2, 12.4, 42.6, 70.4, 86.6, 51.8, 22.0, 13.4, 9.2, 1.6],
    "Rancagua": [1.0, 2.0, 5.0, 15.0, 65.0, 95.0, 80.0, 60.0, 25.0, 15.0, 8.0, 2.0],
}

# Humedad relativa estimada (%) - patrón mediterráneo Chile central
HR_DEFAULT = {
    "Curacaví": [55, 57, 60, 65, 73, 79, 80, 77, 70, 64, 58, 55],
    "Lolol": [66, 67, 71, 75, 80, 83, 84, 82, 79, 74, 70, 67],
    "default": [58, 60, 63, 68, 75, 80, 81, 78, 72, 66, 60, 57],
}


def parse_pvsyst_csv(filepath_or_buffer):
    """
    Parsea un CSV exportado de PVsyst (8760 horas).
    Retorna DataFrame con columnas: datetime, GHI, DHI, BHI, T_amb, WindVel
    """
    # Leer todo el archivo
    if isinstance(filepath_or_buffer, str):
        with open(filepath_or_buffer, 'r', encoding='latin-1') as f:
            raw = f.read()
    else:
        raw = filepath_or_buffer.read().decode('latin-1')

    # Buscar la línea de encabezado de datos
    lines = raw.split('\n')
    header_idx = None
    for i, line in enumerate(lines):
        if line.strip().startswith('date,'):
            header_idx = i
            break

    if header_idx is None:
        raise ValueError("No se encontró encabezado 'date,' en el archivo PVsyst")

    # Reconstruir CSV desde el encabezado (saltando la línea de unidades)
    data_lines = [lines[header_idx]] + lines[header_idx + 2:]
    data_str = '\n'.join([l for l in data_lines if l.strip()])

    df = pd.read_csv(StringIO(data_str), skipinitialspace=True)

    # Renombrar columnas
    col_map = {}
    for c in df.columns:
        cl = c.strip().lower()
        if 'date' in cl:
            col_map[c] = 'datetime_str'
        elif cl == 'globhor':
            col_map[c] = 'GHI'
        elif cl == 'diffhor':
            col_map[c] = 'DHI'
        elif cl == 'beamhor':
            col_map[c] = 'BHI'
        elif 't_amb' in cl or 'tamb' in cl:
            col_map[c] = 'T_amb'
        elif 'wind' in cl:
            col_map[c] = 'WindVel'

    df = df.rename(columns=col_map)

    # Parsear fecha
    df['datetime'] = pd.to_datetime(df['datetime_str'].str.strip(), format='%d/%m/%y %H:%M')
    df['month'] = df['datetime'].dt.month
    df['day'] = df['datetime'].dt.day
    df['hour'] = df['datetime'].dt.hour
    df['date'] = df['datetime'].dt.date

    return df


def calc_monthly_climate(df, localidad="default", precip_custom=None, hr_custom=None, lat=-33.4, alt=200.0):
    """
    Calcula las 17+ variables mensuales del informe agroclimático.
    Usa Penman-Monteith FAO-56 como método principal de ETP.

    Args:
        df: DataFrame con datos horarios PVsyst
        localidad: nombre para buscar en DB de precipitación
        precip_custom: lista 12 valores mensuales de precipitación (mm), override
        hr_custom: lista 12 valores mensuales de humedad relativa (%), override
        lat: latitud para cálculo ETP
        alt: altitud m.s.n.m. para Penman-Monteith

    Returns:
        DataFrame con variables mensuales + fila anual
    """
    months = range(1, 13)
    month_names = ['ENE', 'FEB', 'MAR', 'ABR', 'MAY', 'JUN',
                   'JUL', 'AGO', 'SEP', 'OCT', 'NOV', 'DIC']

    # Precipitación
    if precip_custom:
        precip = precip_custom
    elif localidad in PRECIP_DB:
        precip = PRECIP_DB[localidad]
    else:
        precip = PRECIP_DB.get("Santiago", [0]*12)

    # Humedad relativa
    if hr_custom:
        hr_vals = hr_custom
    elif localidad in HR_DEFAULT:
        hr_vals = HR_DEFAULT[localidad]
    else:
        hr_vals = HR_DEFAULT["default"]

    results = []

    for m in months:
        mdf = df[df['month'] == m]
        daily = mdf.groupby('date')

        # Temperaturas
        t_max_daily = daily['T_amb'].max()
        t_min_daily = daily['T_amb'].min()
        t_mean_daily = daily['T_amb'].mean()

        t_max = t_max_daily.mean()
        t_min = t_min_daily.mean()
        t_med = (t_max + t_min) / 2

        # Días-grado base 10°C (método horario)
        dg_hourly = mdf['T_amb'].apply(lambda t: max(t - 10, 0)).sum() / 24

        # Días-grado base 12°C
        dg12_hourly = mdf['T_amb'].apply(lambda t: max(t - 12, 0)).sum() / 24

        # Días cálidos (Tmax > 25°C)
        d_calidos = (t_max_daily > 25).sum()

        # Horas frío (T < 7°C)
        hrs_frio = (mdf['T_amb'] < 7).sum()

        # Horas frescor (T < 10°C)
        hrs_fres = (mdf['T_amb'] < 10).sum()

        # Radiación solar (W/m2 → cal/cm2·día)
        # GHI promedio diario en Wh/m2, luego convertir
        # 1 W/m2 durante 1 hora = 1 Wh/m2
        # Integral diaria: sum(GHI_hora) = Wh/m2/día
        ghi_daily = daily['GHI'].sum()  # Wh/m2 por día
        ghi_mean = ghi_daily.mean()  # Wh/m2 promedio diario
        # Convertir Wh/m2 a cal/cm2: 1 Wh/m2 = 0.086 cal/cm2
        r_solar = ghi_mean * 0.086

        # Humedad relativa
        h_relat = hr_vals[m - 1]

        # Precipitación
        precipit = precip[m - 1]

        n_days = len(t_max_daily)
        delta_t = max(t_max - t_min, 0.1)

        # Radiación solar en MJ/m²/día
        rs_mj = ghi_mean * 3600 / 1e6

        # Viento medio mensual
        wind_m = mdf['WindVel'].mean() if 'WindVel' in mdf.columns else 2.0

        # Día del año central del mes (para Penman-Monteith)
        doy_mid = sum([0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334][m-1:m]) + 15

        # ETP: Penman-Monteith FAO-56 (método principal) con fallback Hargreaves
        try:
            lat_rad = math.radians(lat)
            etp_month = penman_monteith_fao56_monthly(
                t_max=t_max, t_min=t_min, t_med=t_med,
                hr=h_relat, u2=wind_m, rs_mj=rs_mj,
                lat_rad=lat_rad, alt=alt,
                n_days=n_days, doy_mid=doy_mid,
            )
            etp_method = "PM-FAO56"
        except Exception:
            # Fallback: Hargreaves
            ra_est = rs_mj / 0.5 if rs_mj > 0 else 5.0
            etp_daily = 0.0023 * (t_med + 17.8) * (delta_t ** 0.5) * ra_est
            etp_month = etp_daily * n_days
            etp_method = "Hargreaves"

        # Déficit y excedente hídrico
        def_hidr = max(etp_month - precipit, 0)
        exc_hidr = max(precipit - etp_month, 0)

        # Índice de humedad
        ind_humed = precipit / etp_month if etp_month > 0 else 0

        # Heladas (días con Tmin < 0°C)
        heladas = (t_min_daily < 0).sum()

        results.append({
            'MES': month_names[m - 1],
            'month_num': m,
            'T.MAX': round(t_max, 1),
            'T.MIN': round(t_min, 1),
            'T.MED': round(t_med, 1),
            'DIAS GRADO': round(dg_hourly, 0),
            'DIAS GRA12': round(dg12_hourly, 0),
            'DG.ACUM': 0,  # se calcula después
            'D-cálidos': int(d_calidos),
            'HRS.FRIO': int(hrs_frio),
            'HRS.FRES': int(hrs_fres),
            'HF.ACUM': 0,  # se calcula después
            'R.SOLAR': round(r_solar, 0),
            'H.RELAT': h_relat,
            'PRECIPIT': round(precipit, 1),
            'EVAP.POT': round(etp_month, 1),
            'DEF.HIDR': round(def_hidr, 1),
            'EXC.HIDR': round(exc_hidr, 1),
            'IND.HUMED': round(ind_humed, 2),
            'HELADAS': round(heladas, 1),
            'n_days': n_days,
        })

    rdf = pd.DataFrame(results)

    # DG acumulados desde octubre (mes 10)
    # Orden: oct, nov, dic, ene, feb, mar, abr, ...
    order_dg = [10, 11, 12, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    dg_acum = 0
    for m in order_dg:
        idx = rdf[rdf['month_num'] == m].index[0]
        dg_acum += rdf.loc[idx, 'DIAS GRADO']
        rdf.loc[idx, 'DG.ACUM'] = round(dg_acum, 0)

    # Horas frío acumuladas desde mayo (mes 5) hasta diciembre
    order_hf = [5, 6, 7, 8, 9, 10, 11, 12]
    hf_acum = 0
    for m in order_hf:
        idx = rdf[rdf['month_num'] == m].index[0]
        hf_acum += rdf.loc[idx, 'HRS.FRIO']
        rdf.loc[idx, 'HF.ACUM'] = hf_acum
    # Meses fuera del rango: marcar con *
    for m in [1, 2, 3, 4]:
        idx = rdf[rdf['month_num'] == m].index[0]
        rdf.loc[idx, 'HF.ACUM'] = None

    # Fila anual
    annual = {
        'MES': 'ANUAL',
        'month_num': 0,
        'T.MAX': round(rdf['T.MAX'].mean(), 1),
        'T.MIN': round(rdf['T.MIN'].mean(), 1),
        'T.MED': round(rdf['T.MED'].mean(), 1),
        'DIAS GRADO': round(rdf['DIAS GRADO'].sum(), 0),
        'DIAS GRA12': round(rdf['DIAS GRA12'].sum(), 0),
        'DG.ACUM': round(rdf['DIAS GRADO'].sum(), 0),
        'D-cálidos': int(rdf['D-cálidos'].sum()),
        'HRS.FRIO': int(rdf['HRS.FRIO'].sum()),
        'HRS.FRES': int(rdf['HRS.FRES'].sum()),
        'HF.ACUM': int(rdf['HRS.FRIO'].sum()),
        'R.SOLAR': round(rdf['R.SOLAR'].mean(), 0),
        'H.RELAT': round(rdf['H.RELAT'].mean(), 0),
        'PRECIPIT': round(rdf['PRECIPIT'].sum(), 1),
        'EVAP.POT': round(rdf['EVAP.POT'].sum(), 1),
        'DEF.HIDR': round(rdf['DEF.HIDR'].sum(), 1),
        'EXC.HIDR': round(rdf['EXC.HIDR'].sum(), 1),
        'IND.HUMED': round(rdf['PRECIPIT'].sum() / rdf['EVAP.POT'].sum(), 2) if rdf['EVAP.POT'].sum() > 0 else 0,
        'HELADAS': round(rdf['HELADAS'].sum(), 1),
        'n_days': int(rdf['n_days'].sum()),
    }
    rdf = pd.concat([rdf, pd.DataFrame([annual])], ignore_index=True)

    return rdf


def calc_dias_calidos_table(df):
    """
    Tabla de días cálidos por umbral (>20°C, >25°C, >30°C) por mes.
    """
    month_names = ['ENE', 'FEB', 'MAR', 'ABR', 'MAY', 'JUN',
                   'JUL', 'AGO', 'SEP', 'OCT', 'NOV', 'DIC']
    umbrales = [20, 25, 30]
    results = []

    for umbral in umbrales:
        row = {'UMBRAL': f'>{umbral} °C'}
        total = 0
        for m in range(1, 13):
            mdf = df[df['month'] == m]
            daily_max = mdf.groupby('date')['T_amb'].max()
            count = (daily_max > umbral).sum()
            row[month_names[m-1]] = round(count, 1)
            total += count
        row['ANUAL'] = round(total, 1)
        results.append(row)

    return pd.DataFrame(results)


def calc_heladas_intensidad(df):
    """
    Tabla de heladas por intensidad (0°C a -10°C) por mes.
    """
    month_names = ['ENE', 'FEB', 'MAR', 'ABR', 'MAY', 'JUN',
                   'JUL', 'AGO', 'SEP', 'OCT', 'NOV', 'DIC']
    umbrales = [0, -2, -4, -6, -8, -10]
    results = []

    for umbral in umbrales:
        row = {'UMBRAL': f'{umbral} °C'}
        total = 0
        for m in range(1, 13):
            mdf = df[df['month'] == m]
            daily_min = mdf.groupby('date')['T_amb'].min()
            count = (daily_min < umbral).sum()
            row[month_names[m-1]] = round(count, 1)
            total += count
        row['ANUAL'] = round(total, 1)
        results.append(row)

    return pd.DataFrame(results)


def calc_winkler(df):
    """
    Índice de Winkler: días-grado acumulados entre octubre y marzo (base 10°C).
    """
    meses_ciclo = [10, 11, 12, 1, 2, 3]
    total = 0
    for m in meses_ciclo:
        mdf = df[df['month'] == m]
        dg = mdf['T_amb'].apply(lambda t: max(t - 10, 0)).sum() / 24
        total += dg
    return round(total, 0)


def calc_indice_fototermico(df):
    """
    Índice fototérmico: combina radiación solar, días cálidos a madurez
    y frescor nocturno (feb-mar).

    Fórmula simplificada basada en Santibáñez:
    IF = (R_solar_madurez / R_ref) * Indice_frescor * (D_calidos_madurez / D_ref) * 100

    Donde:
    - R_solar_madurez: radiación feb+mar promedio diaria (cal/cm2·d)
    - Indice_frescor: basado en T.min nocturna marzo (óptimo 10-11°C = 1.0)
    - D_calidos: días >25°C en feb+mar
    """
    # Radiación feb-mar
    feb_mar = df[df['month'].isin([2, 3])]
    daily_ghi = feb_mar.groupby('date')['GHI'].sum()
    r_solar = daily_ghi.mean() * 0.086  # cal/cm2·d

    # T min nocturna marzo
    mar = df[df['month'] == 3]
    daily_min = mar.groupby('date')['T_amb'].min()
    t_min_mar = daily_min.mean()

    # Índice de frescor (óptimo 10-11°C)
    if 10 <= t_min_mar <= 11:
        i_frescor = 1.0
    elif t_min_mar < 10:
        i_frescor = max(0.3, 1.0 - (10 - t_min_mar) * 0.1)
    else:
        i_frescor = max(0.3, 1.0 - (t_min_mar - 11) * 0.1)

    # Días cálidos feb-mar
    daily_max = feb_mar.groupby('date')['T_amb'].max()
    d_calidos = (daily_max > 25).sum()

    # Fórmula
    r_ref = 500  # referencia cal/cm2·d
    d_ref = 40   # referencia días cálidos

    indice = (r_solar / r_ref) * i_frescor * (d_calidos / d_ref) * 100

    return round(indice, 0), round(r_solar, 1), round(t_min_mar, 1), int(d_calidos)


def calc_horas_frescor_madurez(df):
    """Horas con T<10°C entre febrero y marzo."""
    feb_mar = df[df['month'].isin([2, 3])]
    return int((feb_mar['T_amb'] < 10).sum())


# ── Nuevas variables Santibáñez / FAO-56 / Atlas Agroclimático ──────────────

def penman_monteith_fao56_monthly(t_max, t_min, t_med, hr, u2, rs_mj, lat_rad, alt, n_days, doy_mid):
    """
    ETP Penman-Monteith FAO-56 mensual (mm/mes).
    Basado en Allen et al. (1998), estándar FAO.

    Args:
        t_max, t_min, t_med: temperaturas media del mes (°C)
        hr: humedad relativa media (%)
        u2: velocidad del viento a 2m (m/s)
        rs_mj: radiación solar incidente diaria (MJ/m²/día) = GHI
        lat_rad: latitud en radianes
        alt: altitud m s.n.m.
        n_days: días del mes
        doy_mid: día del año central del mes
    """
    from math import exp, cos, sin, acos, tan, sqrt, pi

    # Presión atmosférica
    P = 101.3 * ((293 - 0.0065 * alt) / 293) ** 5.26

    # Constante psicrométrica
    gamma = 0.000665 * P

    # Presión de vapor
    es = 0.6108 * exp(17.27 * t_med / (t_med + 237.3))
    ea = es * hr / 100.0

    # Pendiente curva presión-temperatura
    delta = 4098 * es / (t_med + 237.3) ** 2

    # Radiación extraterrestre
    dr = 1 + 0.033 * cos(2 * pi * doy_mid / 365)
    decl = 0.409 * sin(2 * pi * doy_mid / 365 - 1.39)

    ws_arg = -tan(lat_rad) * tan(decl)
    ws_arg = max(-1.0, min(1.0, ws_arg))
    ws = acos(ws_arg)

    Ra = (24 * 60 / pi) * 0.0820 * dr * (
        ws * sin(lat_rad) * sin(decl) + cos(lat_rad) * cos(decl) * sin(ws)
    )
    if Ra <= 0:
        Ra = 0.1

    # Radiación cielo despejado
    Rso = (0.75 + 2e-5 * alt) * Ra
    if Rso <= 0:
        Rso = 0.1

    # Radiación neta onda corta
    Rns = (1 - 0.23) * rs_mj

    # Radiación neta onda larga
    sigma = 4.903e-9
    Tk = t_med + 273.16
    rs_ratio = min(rs_mj / Rso, 1.0)
    Rnl = sigma * Tk**4 * (0.34 - 0.14 * sqrt(max(ea, 0.01))) * (1.35 * rs_ratio - 0.35)

    # Radiación neta
    Rn = Rns - Rnl
    G = 0  # Para períodos mensuales

    # ETo diaria
    denom = delta + gamma * (1 + 0.34 * u2)
    if denom == 0:
        return 0
    ETo = (0.408 * delta * (Rn - G) + gamma * 900 / (t_med + 273) * u2 * (es - ea)) / denom

    return max(ETo, 0) * n_days


def calc_porciones_frio(df):
    """
    Modelo dinámico de porciones de frío (Fishman et al., 1987).
    Complementa las horas de frío simples. Más preciso para cerezo y manzano.
    Período: mayo a agosto.

    Returns: porciones de frío acumuladas (float)
    """
    from math import exp

    # Filtrar mayo a agosto
    winter = df[df['month'].isin([5, 6, 7, 8])].sort_values('datetime')
    temps = winter['T_amb'].values

    # Parámetros del modelo
    e0 = 4153.5
    e1 = 12888.8
    a0 = 1.395e5
    a1 = 2.567e18

    intermediario = 0.0
    porciones = 0.0

    for T_celsius in temps:
        Tk = T_celsius + 273.0
        if Tk <= 0:
            continue

        k0 = a0 * exp(-e0 / Tk)
        k1 = a1 * exp(-e1 / Tk)

        eq = k0 / (k0 + k1) if (k0 + k1) > 0 else 0
        intermediario = eq - (eq - intermediario) * exp(-(k0 + k1))

        if intermediario >= 1.0:
            porciones += intermediario
            intermediario = 0

    return round(porciones, 1)


def calc_huglin(df, lat):
    """
    Índice heliotérmico de Huglin.
    Período: Oct 1 - Mar 31 (hemisferio sur).
    Complementa Winkler; incorpora amplitud térmica diaria.

    Returns: (HI, clase_str)
    """
    abs_lat = abs(lat)
    if abs_lat <= 40:
        d = 1.0
    elif abs_lat <= 42:
        d = 1.02
    elif abs_lat <= 44:
        d = 1.03
    elif abs_lat <= 46:
        d = 1.04
    else:
        d = 1.06

    meses_huglin = [10, 11, 12, 1, 2, 3]
    HI = 0

    for m in meses_huglin:
        mdf = df[df['month'] == m]
        daily = mdf.groupby('date')
        for _, day_data in daily:
            t_max = day_data['T_amb'].max()
            t_med = day_data['T_amb'].mean()
            contrib = max(((t_med - 10) + (t_max - 10)) / 2, 0) * d
            HI += contrib

    HI = round(HI, 0)

    if HI < 1500:
        clase = "Muy frío (no viable)"
    elif HI < 1800:
        clase = "Frío (blancos aromáticos)"
    elif HI < 2100:
        clase = "Templado (Pinot Noir, Chardonnay)"
    elif HI < 2400:
        clase = "Templado-cálido (Cabernet, Merlot)"
    elif HI < 3000:
        clase = "Cálido (Syrah, Garnacha)"
    else:
        clase = "Muy cálido"

    return HI, clase


def calc_noches_frias(df):
    """
    Índice de Noches Frías (Cool Night Index - IF).
    Tmin media de marzo. Indicador de potencial aromático en vides.

    Returns: (IF_value, clase_str)
    """
    mar = df[df['month'] == 3]
    daily_min = mar.groupby('date')['T_amb'].min()
    IF = round(daily_min.mean(), 1)

    if IF < 12:
        clase = "Noches muy frías (alta concentración aromática)"
    elif IF < 14:
        clase = "Noches frías (buena acidez)"
    elif IF < 18:
        clase = "Noches templadas (equilibrado)"
    else:
        clase = "Noches cálidas (riesgo pérdida acidez)"

    return IF, clase


def calc_dias_libres_helada(df):
    """
    Días libres de helada: racha más larga de días consecutivos sin T < 0°C.
    Variable estándar del Atlas Agroclimático de Chile.

    Returns: int (número de días de la racha más larga)
    """
    daily = df.groupby('date')
    t_min_daily = daily['T_amb'].min().sort_index()

    max_racha = 0
    racha = 0
    for tmin in t_min_daily.values:
        if tmin >= 0:
            racha += 1
            max_racha = max(max_racha, racha)
        else:
            racha = 0

    return max_racha


def calc_prob_helada_mensual(df):
    """
    Probabilidad de helada por mes (%).
    Más útil que el conteo absoluto.

    Returns: dict {mes: probabilidad %}
    """
    month_names = ['ENE', 'FEB', 'MAR', 'ABR', 'MAY', 'JUN',
                   'JUL', 'AGO', 'SEP', 'OCT', 'NOV', 'DIC']
    result = {}
    for m in range(1, 13):
        mdf = df[df['month'] == m]
        daily = mdf.groupby('date')
        t_min_daily = daily['T_amb'].min()
        n_days = len(t_min_daily)
        n_frost = (t_min_daily < 0).sum()
        prob = round(100 * n_frost / n_days, 1) if n_days > 0 else 0
        result[month_names[m - 1]] = prob
    return result


def calc_helada_tardia(df):
    """
    Helada tardía: días con Tmin < 0°C en Sep, Oct, Nov.
    Es el riesgo productivo más relevante para frutales.

    Returns: dict con total, por_mes, y alerta
    """
    meses = {9: 'SEP', 10: 'OCT', 11: 'NOV'}
    total = 0
    por_mes = {}
    for m, name in meses.items():
        mdf = df[df['month'] == m]
        daily = mdf.groupby('date')
        t_min_daily = daily['T_amb'].min()
        n_frost = int((t_min_daily < 0).sum())
        por_mes[name] = n_frost
        total += n_frost

    if total == 0:
        alerta = "Sin riesgo de helada tardía"
        nivel = "verde"
    elif total <= 3:
        alerta = "Riesgo bajo de helada tardía"
        nivel = "amarillo"
    else:
        alerta = f"Riesgo alto: {total} días con helada tardía (Sep-Nov)"
        nivel = "rojo"

    return {
        "total": total,
        "por_mes": por_mes,
        "alerta": alerta,
        "nivel": nivel,
    }


def calc_tipo_helada(df):
    """
    Clasificación de heladas por tipo (radiativa vs advectiva).
    Basado en metodología Santibáñez/Infodep.

    Helada radiativa: noches despejadas, sin viento (WindVel < 1.5 m/s),
    día previo con GHI alto.
    Helada advectiva: masa de aire polar, con viento.

    Returns: dict con conteo por tipo y mes
    """
    daily = df.groupby('date')
    t_min_daily = daily['T_amb'].min()
    wind_night = df[df['hour'].isin([0, 1, 2, 3, 4, 5, 6])].groupby('date')['WindVel'].mean()
    # GHI del día previo (proxy de nubosidad)
    ghi_daily = daily['GHI'].sum()

    frost_days = t_min_daily[t_min_daily < 0].index
    radiativa = 0
    advectiva = 0
    por_mes = {}

    for day in frost_days:
        month = day.month if hasattr(day, 'month') else int(str(day)[5:7])
        mes_name = ['ENE', 'FEB', 'MAR', 'ABR', 'MAY', 'JUN',
                     'JUL', 'AGO', 'SEP', 'OCT', 'NOV', 'DIC'][month - 1]

        w = wind_night.get(day, 2.0)
        g = ghi_daily.get(day, 3000)

        if w < 1.5 and g > 2000:  # Noche calma + día despejado
            radiativa += 1
            tipo = "radiativa"
        else:
            advectiva += 1
            tipo = "advectiva"

        if mes_name not in por_mes:
            por_mes[mes_name] = {"radiativa": 0, "advectiva": 0}
        por_mes[mes_name][tipo] += 1

    return {
        "radiativa": radiativa,
        "advectiva": advectiva,
        "total": radiativa + advectiva,
        "por_mes": por_mes,
    }


# ── Tablas bioclimáticas por especie ─────────────────────────────────────────

# Definición de umbrales por especie (basado en Santibáñez)
ESPECIES = {
    'VID': {
        'nombre': 'Vid',
        'meses_flor': [10, 11],       # oct-nov
        'meses_cuaja': [11, 12],       # nov-dic
        'meses_madurez': [2, 3],       # feb-mar
        'meses_cosecha': [3, 4],       # mar-abr
        'meses_ciclo': [10, 11, 12, 1, 2, 3],
        'umbrales': {
            'heladas_ciclo': {'recom': '<1', 'max_ok': 1},
            'heladas_flor': {'recom': '0', 'max_ok': 0},
            'tmax_flor_cuaja': {'recom': '18-25', 'min': 18, 'max': 25},
            'tmin_flor_cuaja': {'recom': '>8', 'min': 8},
            'precip_flor': {'recom': '<20', 'max': 20},
            'dias_calidos_25': {'recom': '90-130'},
            'dias_templados_20': {'recom': '>140'},
            'tmax_madurez': {'recom': '25-28', 'min': 25, 'max': 28},
            'tmin_madurez': {'recom': '>10', 'min': 10},
            'dias_calidos_madurez': {'recom': ''},
            'precip_cosecha': {'recom': '<20', 'max': 20},
            'rsolar_madurez': {'recom': '>500', 'min': 500},
            'frio_invernal': {'recom': '>0.95'},
            'frescor_madurez': {'recom': '>25', 'min': 25},
            'winkler': {'recom': '800-1900'},
            'fototermico': {'recom': '>80', 'min': 80},
        }
    },
    'CEREZO': {
        'nombre': 'Cerezo',
        'meses_flor': [9, 10],
        'meses_cuaja': [10],
        'meses_madurez': [12, 1],
        'meses_cosecha': [12, 1],
        'meses_ciclo': [9, 10, 11, 12, 1, 2, 3],
        'umbrales': {
            'heladas_ciclo': {'recom': '<1.0'},
            'heladas_flor': {'recom': '<0.5'},
            'tmax_flor_cuaja': {'recom': '15-22'},
            'tmin_flor_cuaja': {'recom': '8-14'},
            'precip_flor': {'recom': '<30'},
            'dias_calidos_25': {'recom': '40-100'},
            'dias_templados_20': {'recom': '>120'},
            'tmax_madurez': {'recom': '23-27'},
            'tmin_madurez': {'recom': '8-12'},
            'precip_cosecha': {'recom': '<15'},
            'rsolar_madurez': {'recom': '450-550'},
            'frio_invernal': {'recom': '>0.95'},
        }
    },
    'NOGAL': {
        'nombre': 'Nogal',
        'meses_flor': [10],
        'meses_cuaja': [10, 11],
        'meses_madurez': [2, 3],
        'meses_cosecha': [3, 4],
        'meses_ciclo': [10, 11, 12, 1, 2, 3],
        'umbrales': {
            'heladas_ciclo': {'recom': '<0.5'},
            'heladas_flor': {'recom': '<0.1'},
            'tmax_flor_cuaja': {'recom': '15-22'},
            'tmin_flor_cuaja': {'recom': '>8'},
            'precip_flor': {'recom': '<30'},
            'dias_calidos_25': {'recom': '90-120'},
            'dias_templados_20': {'recom': '>150'},
            'tmax_madurez': {'recom': '>25'},
            'tmin_madurez': {'recom': '>10'},
            'precip_cosecha': {'recom': '<20'},
            'rsolar_madurez': {'recom': '>500'},
            'frio_invernal': {'recom': '>0.95'},
        }
    },
    'ALMENDRO': {
        'nombre': 'Almendro',
        'meses_flor': [8, 9],
        'meses_cuaja': [9],
        'meses_madurez': [1, 2],
        'meses_cosecha': [2, 3],
        'meses_ciclo': [8, 9, 10, 11, 12, 1, 2, 3],
        'umbrales': {
            'heladas_ciclo': {'recom': '<1'},
            'heladas_flor': {'recom': '<0.5'},
            'tmax_flor_cuaja': {'recom': '>12'},
            'tmin_flor_cuaja': {'recom': '>7'},
            'precip_flor': {'recom': '<30'},
            'dias_calidos_25': {'recom': '>50'},
            'dias_templados_20': {'recom': '>150'},
            'tmax_madurez': {'recom': '25-28'},
            'tmin_madurez': {'recom': '>10'},
            'precip_cosecha': {'recom': '<30'},
            'rsolar_madurez': {'recom': '>500'},
            'frio_invernal': {'recom': '>0.95'},
        }
    },
    'PALTO': {
        'nombre': 'Palto',
        'meses_flor': [9, 10],
        'meses_cuaja': [10, 11],
        'meses_madurez': [5, 6, 7],
        'meses_cosecha': [6, 7, 8],
        'meses_ciclo': list(range(1, 13)),
        'umbrales': {
            'heladas_ciclo': {'recom': '<2'},
            'heladas_flor': {'recom': '<0.5'},
            'tmax_flor_cuaja': {'recom': '20-25'},
            'tmin_flor_cuaja': {'recom': '>10'},
            'precip_flor': {'recom': '<50'},
            'tmax_madurez': {'recom': '>15'},
            'tmin_madurez': {'recom': '>5'},
        }
    },
    'ARANDANO': {
        'nombre': 'Arándano',
        'meses_flor': [10],
        'meses_cuaja': [10, 11],
        'meses_madurez': [1, 2],
        'meses_cosecha': [1, 2],
        'meses_ciclo': [10, 11, 12, 1, 2, 3],
        'umbrales': {
            'heladas_ciclo': {'recom': '<3'},
            'heladas_flor': {'recom': '<1'},
            'tmax_flor_cuaja': {'recom': '15-20'},
            'tmin_flor_cuaja': {'recom': '>5'},
            'precip_flor': {'recom': '<50'},
            'dias_calidos_25': {'recom': '60-90'},
            'tmax_madurez': {'recom': '23-27'},
            'tmin_madurez': {'recom': '8-12'},
            'precip_cosecha': {'recom': '<90'},
            'rsolar_madurez': {'recom': '>400'},
            'frio_invernal': {'recom': '>0.95'},
        }
    },
    'FRAMBUESO': {
        'nombre': 'Frambueso',
        'meses_flor': [10, 11],
        'meses_cuaja': [11],
        'meses_madurez': [12, 1],
        'meses_cosecha': [12, 1],
        'meses_ciclo': [10, 11, 12, 1, 2, 3],
        'umbrales': {
            'heladas_ciclo': {'recom': '<2'},
            'heladas_flor': {'recom': '<0.5'},
            'tmax_flor_cuaja': {'recom': '15-22'},
            'tmin_flor_cuaja': {'recom': '>5'},
            'precip_flor': {'recom': '<100'},
            'dias_calidos_25': {'recom': '<80'},
            'dias_templados_20': {'recom': '>80'},
            'tmax_madurez': {'recom': '20-25'},
            'tmin_madurez': {'recom': '>8'},
            'precip_cosecha': {'recom': '<30'},
            'rsolar_madurez': {'recom': '>500'},
            'frio_invernal': {'recom': '>0.95'},
        }
    },
    'AVELLANO EUROPEO': {
        'nombre': 'Avellano Europeo',
        'meses_flor': [7, 8],
        'meses_cuaja': [8, 9],
        'meses_madurez': [2, 3],
        'meses_cosecha': [3, 4],
        'meses_ciclo': [7, 8, 9, 10, 11, 12, 1, 2, 3],
        'umbrales': {
            'heladas_ciclo': {'recom': '<10'},
            'heladas_flor': {'recom': '<6'},
            'tmax_flor_cuaja': {'recom': '15-22'},
            'tmin_flor_cuaja': {'recom': '>8'},
            'precip_flor': {'recom': '<100'},
            'dias_calidos_25': {'recom': '60-100'},
            'dias_templados_20': {'recom': '>140'},
            'tmax_madurez': {'recom': '21-28'},
            'tmin_madurez': {'recom': '>8'},
            'precip_cosecha': {'recom': '<50'},
            'rsolar_madurez': {'recom': '>450'},
            'frio_invernal': {'recom': '>0.95'},
        }
    },
    'CIRUELO JAPONES': {
        'nombre': 'Ciruelo Japonés',
        'meses_flor': [9],
        'meses_cuaja': [9, 10],
        'meses_madurez': [1, 2],
        'meses_cosecha': [1, 2],
        'meses_ciclo': [9, 10, 11, 12, 1, 2, 3],
        'umbrales': {
            'heladas_ciclo': {'recom': '<2'},
            'heladas_flor': {'recom': '<0.5'},
            'tmax_flor_cuaja': {'recom': '15-20'},
            'tmin_flor_cuaja': {'recom': '8-14'},
            'precip_flor': {'recom': '<30'},
            'dias_calidos_25': {'recom': '60-100'},
            'dias_templados_20': {'recom': '>120'},
            'tmax_madurez': {'recom': '25-30'},
            'tmin_madurez': {'recom': '>8'},
            'precip_cosecha': {'recom': '<80'},
            'rsolar_madurez': {'recom': '>500'},
            'frio_invernal': {'recom': '>0.95'},
        }
    },
}


def calc_bioclimatic_table(df, especie_key, monthly_df, precip_list):
    """
    Calcula la tabla bioclimática para una especie dada.
    Retorna lista de dicts con: variable, valor, unidad, recomendable, riesgo.
    """
    esp = ESPECIES[especie_key]

    meses_ciclo = esp['meses_ciclo']
    meses_flor = esp.get('meses_flor', [])
    meses_cuaja = esp.get('meses_cuaja', [])
    meses_flor_cuaja = list(set(meses_flor + meses_cuaja))
    meses_madurez = esp.get('meses_madurez', [])
    meses_cosecha = esp.get('meses_cosecha', [])

    # Filtros
    ciclo = df[df['month'].isin(meses_ciclo)]
    flor_cuaja = df[df['month'].isin(meses_flor_cuaja)]
    madurez = df[df['month'].isin(meses_madurez)]
    cosecha_df = df[df['month'].isin(meses_cosecha)]

    # Heladas en ciclo
    ciclo_daily_min = ciclo.groupby('date')['T_amb'].min()
    heladas_ciclo = (ciclo_daily_min < 0).sum()

    # Heladas en floración
    flor = df[df['month'].isin(meses_flor)]
    flor_daily_min = flor.groupby('date')['T_amb'].min()
    heladas_flor = (flor_daily_min < 0).sum()

    # T max/min en flor y cuaja
    fc_daily_max = flor_cuaja.groupby('date')['T_amb'].max()
    fc_daily_min = flor_cuaja.groupby('date')['T_amb'].min()
    tmax_fc = fc_daily_max.mean() if len(fc_daily_max) > 0 else 0
    tmin_fc = fc_daily_min.mean() if len(fc_daily_min) > 0 else 0

    # Precipitación en floración
    precip_flor = sum(precip_list[m-1] for m in meses_flor)

    # Días cálidos >25°C en ciclo
    ciclo_daily_max = ciclo.groupby('date')['T_amb'].max()
    dias_calidos_25 = (ciclo_daily_max > 25).sum()

    # Días templados >20°C en ciclo
    dias_templados_20 = (ciclo_daily_max > 20).sum()

    # T max/min pinta-madurez
    if len(madurez) > 0:
        mad_daily_max = madurez.groupby('date')['T_amb'].max()
        mad_daily_min = madurez.groupby('date')['T_amb'].min()
        tmax_mad = mad_daily_max.mean()
        tmin_mad = mad_daily_min.mean()
        dias_calidos_mad = (mad_daily_max > 25).sum()
    else:
        tmax_mad = tmin_mad = 0
        dias_calidos_mad = 0

    # Precipitación cosecha
    precip_cosecha = sum(precip_list[m-1] for m in meses_cosecha)

    # Radiación solar madurez
    if len(madurez) > 0:
        mad_ghi_daily = madurez.groupby('date')['GHI'].sum()
        rsolar_mad = mad_ghi_daily.mean() * 0.086
    else:
        rsolar_mad = 0

    # Índice de frío invernal
    # Basado en horas frío acumuladas vs requerimiento típico
    meses_invierno = [5, 6, 7, 8]
    inv = df[df['month'].isin(meses_invierno)]
    hrs_frio_inv = (inv['T_amb'] < 7).sum()
    # Índice: ratio respecto a requerimiento (ej. 800 horas para caducos)
    req_frio = 800
    idx_frio = min(hrs_frio_inv / req_frio, 1.0) if req_frio > 0 else 0

    # Horas frescor madurez
    if len(madurez) > 0:
        frescor_mad = (madurez['T_amb'] < 10).sum()
    else:
        frescor_mad = 0

    # Winkler (solo para vid)
    winkler = calc_winkler(df)

    # Índice fototérmico (solo para vid)
    fototermico, _, _, _ = calc_indice_fototermico(df)

    # ── Evaluar riesgos ──
    def eval_riesgo(valor, recom_str):
        """Evalúa riesgo simple: 0=sin riesgo, 1=leve, 2=moderado, 3=alto."""
        if not recom_str:
            return ''
        # Intentar parsear rangos
        recom_str = recom_str.strip()
        try:
            if recom_str.startswith('<'):
                limit = float(recom_str[1:])
                if valor <= limit:
                    return ''
                elif valor <= limit * 1.5:
                    return 1
                elif valor <= limit * 2:
                    return 2
                else:
                    return 3
            elif recom_str.startswith('>'):
                limit = float(recom_str[1:])
                if valor >= limit:
                    return ''
                elif valor >= limit * 0.7:
                    return -1
                elif valor >= limit * 0.5:
                    return -2
                else:
                    return -3
            elif '-' in recom_str:
                parts = recom_str.split('-')
                lo, hi = float(parts[0]), float(parts[1])
                if lo <= valor <= hi:
                    return ''
                elif valor < lo:
                    diff = (lo - valor) / lo if lo != 0 else 0
                    return -1 if diff < 0.2 else -2
                else:
                    diff = (valor - hi) / hi if hi != 0 else 0
                    return 1 if diff < 0.2 else 2
        except (ValueError, IndexError):
            return ''
        return ''

    # Construir tabla
    umbrales = esp.get('umbrales', {})
    rows = []

    def add_row(variable, valor, unidad, recom_key):
        recom = umbrales.get(recom_key, {}).get('recom', '')
        riesgo = eval_riesgo(valor, recom)
        rows.append({
            'VARIABLE': variable,
            'VALOR': round(valor, 1) if isinstance(valor, float) else valor,
            'UNIDAD': unidad,
            'VALOR RECOMENDABLE': recom,
            'Riesgo': riesgo
        })

    add_row('N° Heladas en el ciclo', heladas_ciclo, 'N°', 'heladas_ciclo')
    add_row('N° Heladas en floración', heladas_flor, 'N°', 'heladas_flor')
    add_row('T max en flor y cuaja', tmax_fc, '°C', 'tmax_flor_cuaja')
    add_row('T min en flor y cuaja', tmin_fc, '°C', 'tmin_flor_cuaja')
    add_row('Precipitación en flor', precip_flor, 'mm', 'precip_flor')
    add_row('N° días con Tmax > 25°C', dias_calidos_25, 'N°', 'dias_calidos_25')
    add_row('N° días con Tmax > 20°C', dias_templados_20, 'N°', 'dias_templados_20')
    add_row('T max pinta-madurez', tmax_mad, '°C', 'tmax_madurez')
    add_row('T min pinta-madurez', tmin_mad, '°C', 'tmin_madurez')

    if especie_key == 'VID':
        add_row('Días cálidos a madurez', dias_calidos_mad, '°C', 'dias_calidos_madurez')

    add_row('Precipitación cosecha', precip_cosecha, 'mm', 'precip_cosecha')
    add_row('Rad. solar madurez', rsolar_mad, 'cal/cm2 d', 'rsolar_madurez')
    add_row('Indice de frio invernal', idx_frio, '-', 'frio_invernal')

    if especie_key == 'VID':
        add_row('Frescor a madurez', frescor_mad, 'horas', 'frescor_madurez')
        add_row('Indice de Winkler', winkler, 'días-grado', 'winkler')
        add_row('Indice fototermico', fototermico, '-', 'fototermico')

    return pd.DataFrame(rows)


def generar_analisis_textual(especie_key, bio_df):
    """
    Genera el análisis textual automático basado en los valores y riesgos.
    """
    esp = ESPECIES[especie_key]
    nombre = esp['nombre']
    lines = []

    for _, row in bio_df.iterrows():
        var = row['VARIABLE']
        val = row['VALOR']
        riesgo = row['Riesgo']
        recom = row['VALOR RECOMENDABLE']

        if 'Heladas' in var and 'ciclo' in var:
            if riesgo == '' or riesgo == 0:
                lines.append("Bajo riesgo de heladas durante el ciclo.")
            elif abs(riesgo) == 1 if isinstance(riesgo, (int, float)) else False:
                lines.append("Riesgo leve de heladas durante el ciclo.")
            else:
                lines.append("Riesgo moderado a alto de heladas durante el ciclo.")

        elif 'Heladas' in var and 'floración' in var.lower():
            if riesgo == '' or riesgo == 0:
                lines.append("Bajo riesgo de heladas durante la floración.")
            elif abs(riesgo) == 1 if isinstance(riesgo, (int, float)) else False:
                lines.append("Riesgo leve de heladas en floración.")
            else:
                lines.append("Riesgo moderado de heladas en floración.")

        elif 'T max' in var and 'flor' in var:
            if riesgo == '' or riesgo == 0:
                lines.append("Temperaturas máximas adecuadas en floración y cuaja.")
            else:
                lines.append("Temperaturas máximas ligeramente fuera de rango en floración, ocasionales problemas con la cuaja.")

        elif 'T min' in var and 'flor' in var:
            if riesgo == '' or riesgo == 0:
                lines.append("Temperaturas mínimas adecuadas en floración y cuaja.")
            else:
                lines.append(f"Leve deficiencia térmica nocturna en floración (T.min={val}°C).")

        elif 'Precipitación' in var and 'flor' in var:
            if riesgo == '' or riesgo == 0:
                lines.append("Bajo riesgo de precipitación en floración.")
            elif abs(riesgo) == 1 if isinstance(riesgo, (int, float)) else False:
                lines.append("Precipitación en flor ligeramente excesiva.")
            else:
                lines.append("Precipitación en flor fuertemente excesiva.")

        elif 'días' in var.lower() and '25' in var:
            if riesgo == '' or riesgo == 0:
                lines.append("Adecuado número de días cálidos en fase de crecimiento del fruto.")
            else:
                lines.append("Número de días cálidos fuera de rango óptimo.")

        elif 'T max' in var and 'madurez' in var:
            if riesgo == '' or riesgo == 0:
                lines.append("Temperaturas diurnas en rangos favorables durante la madurez de los frutos.")
            else:
                lines.append("Temperaturas diurnas algo elevadas durante la madurez podrían afectar rendimiento, calibre y calidad.")

        elif 'T min' in var and 'madurez' in var:
            if riesgo == '' or riesgo == 0:
                lines.append("Temperaturas nocturnas adecuadas durante la madurez de los frutos.")
            else:
                lines.append("Temperaturas nocturnas fuera de rango durante la madurez.")

        elif 'Precipitación' in var and 'cosecha' in var:
            if riesgo == '' or riesgo == 0:
                lines.append("Problemas leves o inexistentes de precipitación a cosecha.")
            elif abs(riesgo) == 1 if isinstance(riesgo, (int, float)) else False:
                lines.append("Problemas leves a medianos de precipitación a cosecha.")
            else:
                lines.append("Problemas medios a severos de precipitación a cosecha, la producción debe ser protegida.")

        elif 'Rad. solar' in var:
            if riesgo == '' or riesgo == 0:
                lines.append("Luminosidad adecuada para las exigencias de la especie.")
            else:
                lines.append("Luminosidad insuficiente para la especie.")

        elif 'frio invernal' in var.lower():
            if val >= 0.95:
                lines.append("Frío invernal en niveles adecuados.")
            elif val >= 0.8:
                lines.append("Leve deficiencia de frío invernal.")
            else:
                lines.append("Mediana deficiencia de frío invernal.")

    return '\n'.join(lines)
