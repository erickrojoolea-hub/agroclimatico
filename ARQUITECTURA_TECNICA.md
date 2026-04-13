# Arquitectura Tecnica - Visor Agroclimatico Chile

## Resumen Ejecutivo

Plataforma Streamlit que genera dos productos de informe agricola para cualquier punto en Chile:
- **Informe Meteorologico** ($100K CLP): Analisis agroclimatico con 8760 horas de datos PVsyst/Meteonorm
- **Informe Predial** ($100K CLP): Diagnostico territorial con 8 secciones (produccion, agua, suelo, electrico, riesgos, infraestructura, vecinos, recomendaciones)

---

## Flujo de Datos Principal

```
Usuario hace click en mapa (lat, lon)
    |
    +---> METEO: find_nearest_comuna(lat, lon, max_km=50)
    |       |
    |       +---> Busca en 349 sitios PVsyst (haversine)
    |       +---> find_pvsyst_file() → data/pvsyst/{nombre}.CSV
    |       +---> parse_pvsyst_csv() → DataFrame 8760 filas
    |       +---> calc_monthly_climate() → 13x20 tabla mensual
    |       +---> calc_bioclimatic_table() por especie
    |       +---> Renderiza tabs + genera PDF
    |
    +---> PREDIAL: find_predial_comuna(lat, lon)
            |
            +---> reverse_geocode_comuna() via Nominatim OSM
            +---> generate_predial_report(comuna, lat, lon)
                    |
                    +---> 8 secciones en paralelo
                    +---> Cada seccion consulta su DB
                    +---> Semaforos + alertas + texto analitico
```

---

## Archivos del Proyecto

### Codigo Python

| Archivo | Lineas | Funcion |
|---------|--------|---------|
| `main.py` | ~1824 | App Streamlit: mapa, UI, tabs, renderizado |
| `climate_engine.py` | ~844 | Parser PVsyst CSV, calculo 17+ variables mensuales, indices bioclimaticos |
| `predial_engine.py` | ~1601 | Motor de 8 secciones, consultas a 4 bases SQLite |
| `pdf_generator.py` | ~600 | PDF meteorologico con ReportLab (tablas + graficos) |
| `predial_pdf_generator.py` | ~500 | PDF predial con semaforos y alertas |
| `agro_database.py` | ~300 | Datos estaticos: plagas, calendario fitosanitario, costos |
| `ndvi_engine.py` | ~200 | Google Earth Engine para NDVI (opcional) |
| `scripts/convert_pvsyst.py` | ~350 | Convierte archivos .SIT/.MET de PVsyst a CSV |

### Datos

| Archivo/Directorio | Tamano | Contenido |
|---------------------|--------|-----------|
| `data/pvsyst/` | 125 MB | 353 archivos CSV con 8760 horas cada uno |
| `data/sites_db.json` | 22 KB | Registro de 349 sitios con coordenadas y radios |
| `data/db/catastro_fruticola.db` | ~15 MB | 278,085 registros de catastro agricola |
| `data/db/dga_derechos.db` | ~20 MB | 260,623 patentes de derechos de agua |
| `data/db/electrico.db` | 268 KB | Infraestructura electrica por comuna |
| `data/db/geospatial.db` | 372 KB | Embalses, estaciones, cuencas, restricciones |
| `data/db/odepa.db` | ~50 KB | Estadisticas nacionales agricolas |

---

## 1. INFORME METEOROLOGICO (climate_engine.py)

### Entrada: PVsyst CSV
```
date,GlobHor,DiffHor,BeamHor,T_Amb,WindVel
    ,W/m2,W/m2,W/m2,C,m/s
01/01/90 00:00,0,0,0,23.76,1.4999
01/01/90 01:00,0,0,0,22.54,1.2
... (8760 filas = 1 ano completo, hora por hora)
```

### Funcion: parse_pvsyst_csv(filepath) → DataFrame
- Lee CSV con encoding latin-1
- Busca linea que empieza con "date,"
- Salta fila de unidades
- Retorna DataFrame con columnas: `datetime, GHI, DHI, BHI, T_amb, WindVel, month, day, hour, date`

### Funcion: calc_monthly_climate(df, localidad, precip_custom, hr_custom, lat) → DataFrame

**Parametros de entrada:**
- `df`: 8760 filas horarias
- `localidad`: nombre para buscar precipitacion en PRECIP_DB
- `precip_custom`: lista de 12 valores mensuales (mm), override manual
- `hr_custom`: lista de 12 valores mensuales (%), override manual
- `lat`: latitud para calculo ETP

**Salida: DataFrame de 13 filas (12 meses + ANUAL) con 20 columnas:**

| Columna | Calculo | Unidad |
|---------|---------|--------|
| MES | Nombre del mes | - |
| T.MAX | Promedio de maximas diarias | C |
| T.MIN | Promedio de minimas diarias | C |
| T.MED | (T.MAX + T.MIN) / 2 | C |
| DIAS GRADO | Sum hourly max(T-10, 0) / 24 | dias-grado |
| DIAS GRA12 | Sum hourly max(T-12, 0) / 24 | dias-grado |
| DG.ACUM | Acumulado Oct→Sep | dias-grado |
| D-calidos | Dias con Tmax > 25C | dias |
| HRS.FRIO | Horas con T < 7C | horas |
| HRS.FRES | Horas con T < 10C | horas |
| HF.ACUM | Acumulado May→Dic | horas |
| R.SOLAR | GHI diario × 0.086 | cal/cm2/dia |
| H.RELAT | De HR_DEFAULT o custom | % |
| PRECIPIT | De PRECIP_DB o custom | mm |
| EVAP.POT | Hargreaves: 0.0023×(Tmed+17.8)×sqrt(Tmax-Tmin)×Ra×n_dias | mm |
| DEF.HIDR | max(ETP - Precip, 0) | mm |
| EXC.HIDR | max(Precip - ETP, 0) | mm |
| IND.HUMED | Precip / ETP | - |
| HELADAS | Dias con Tmin < 0C | dias |

### Indices adicionales

**Winkler** (`calc_winkler`): Suma de dias-grado base 10C de Oct-Mar. Determina aptitud varietal:
- <800: no viable
- 800-1500: variedades blancas
- 1500-1900: variedades tintas
- >1900: clima calido

**Fototermico** (`calc_indice_fototermico`): Combina radiacion, frescor nocturno y dias calidos en Feb-Mar

**Dias calidos** (`calc_dias_calidos_table`): Matriz 4 umbrales (18, 20, 25, 28C) × 12 meses

**Heladas** (`calc_heladas_intensidad`): Matriz 4 intensidades (0, -2, -4, -6C) × 12 meses

### Tablas bioclimaticas por especie

**Especies soportadas:**
VID, CEREZO, CIRUELO, MANZANO, PERAL, KIWI, ARANDANO, AVELLANO

**Funcion: calc_bioclimatic_table(df, especie_key, monthly_df, precip_list)**

Calcula ~14 variables por especie segun fases fenologicas:
- Heladas en ciclo y floracion
- T max/min en flor, cuaja, madurez
- Precipitacion en flor y cosecha
- Dias calidos (>25C, >20C)
- Radiacion solar en madurez
- Indice de frio invernal
- Winkler, Fototermico (solo vid)

Cada variable tiene: VALOR, UNIDAD, VALOR RECOMENDABLE, Riesgo (-3 a +3)

---

## 2. INFORME PREDIAL (predial_engine.py)

### Funcion principal: generate_predial_report(comuna, lat, lon) → dict

Genera un reporte con 8 secciones. Cada seccion retorna:
```python
{
    "disponible": bool,        # Hay datos?
    "semaforo": "rojo|amarillo|verde",
    "semaforo_texto": "...",   # Resumen corto
    "texto_analitico": "...",  # 2-3 oraciones de analisis
    "alertas": ["..."],        # Alertas contextuales
    # + campos especificos de la seccion
}
```

### Seccion 1: Produccion Agricola
**DB:** catastro_fruticola.db → tablas: catastro_completo (278K filas), resumen_por_comuna, resumen_por_especie

**Datos que retorna:**
- `especies`: lista de {especie, superficie_ha, num_explotaciones}
- `variedades`: top 30 por superficie
- `metodos_riego`: {metodo, registros, superficie_ha, pct}
- `tipo_productor`: {tipo, num_explotaciones, hectareas}
- `comparacion_nacional`: % de produccion nacional por especie

**Semaforo:** verde (>10ha), amarillo (1-10ha), rojo (<1ha)

### Seccion 2: Derechos de Agua
**DB:** dga_derechos.db → tablas: patentes (260K filas), resumen_por_comuna

**Datos que retorna:**
- `num_patentes`, `saldo_total`, `morosidad_pct`
- `por_concesion`: desglose por tipo (consuntiva/no consuntiva)
- `por_tipo`: persona natural vs juridica
- `por_anio`: evolucion anual
- `restriccion_hidrica`, `agotamiento`, `reserva_caudal`: flags de riesgo

**Enriquecimiento geoespacial** (si lat/lon disponibles):
- Distancia a embalses cercanos (top 3 en 100km)
- Distancia a estaciones fluviometricas
- Distancia a puntos de calidad de agua
- Verificacion de zona de restriccion/agotamiento

**Semaforo:** rojo (agotamiento o morosidad>30%), amarillo (restriccion o <50 patentes), verde (normal)

### Seccion 3: Uso de Suelo
**DB:** geospatial.db → tabla: uso_suelo_comuna (878 filas, fuente CONAF/CIREN)

**Datos:** distribucion de uso (agricultura, bosque, urbano, pastizal), % agricola

**Semaforo:** verde (>20% agricola), amarillo (5-20%), rojo (<5%)

### Seccion 4: Infraestructura Electrica
**DB:** electrico.db → tablas: clientes_por_comuna (329), distribucion_potencia_comuna (2251), clientes_por_empresa_comuna (482)

**Datos:** clientes, potencia total/promedio, empresas distribuidoras, distribucion por rango de potencia

### Seccion 5: Riesgos Territoriales
**Sintetiza** datos de secciones 1-4 para evaluar:
- Riesgo hidrico (deuda agua, restricciones)
- Concentracion de uso de suelo
- Dependencia agricola
- Proximidad a infraestructura

### Seccion 6: Infraestructura y Logistica
**DB:** geospatial.db → tablas: embalses, estaciones_fluviometricas, estaciones_meteorologicas, calidad_agua, cuencas, subcuencas

**Calcula distancia haversine** desde el punto exacto (lat, lon) a cada infraestructura dentro de 100km
- Top 3 embalses mas cercanos con distancia en km
- Top 3 estaciones fluviometricas
- Top 3 estaciones meteorologicas
- Cuenca y subcuenca correspondiente

### Seccion 7: Comunas Vecinas
**DB:** catastro_fruticola.db → busca comunas de la misma provincia

### Seccion 8: Recomendaciones
**Sintetiza** las 7 secciones anteriores en recomendaciones estrategicas con prioridad (inmediata / corto / mediano plazo)

---

## 3. BASES DE DATOS - Esquemas

### catastro_fruticola.db

**catastro_completo** (278,085 filas)
```sql
anio INTEGER, region TEXT, provincia TEXT, comuna TEXT,
numero_explotacion INTEGER, tipo_productor TEXT,
especie TEXT, variedad TEXT, anio_plantacion INTEGER,
metodo_de_riego TEXT, numero_de_arboles INTEGER,
superficie REAL  -- en hectareas
```

**resumen_por_comuna** (2,365 filas)
```sql
comuna TEXT, especie TEXT, superficie_ha REAL, num_explotaciones INTEGER
```

### dga_derechos.db

**patentes** (260,623 filas)
```sql
cuenta REAL, rut_rol TEXT, tipo_persona TEXT,
monto REAL,  -- monto total CLP
saldo REAL,  -- saldo pendiente CLP
pagado REAL, fecha TEXT,
comuna TEXT, concesion TEXT, provincia TEXT,
oficina_cbr TEXT, anio_patente REAL
```

**resumen_por_comuna** (345 filas)
```sql
region TEXT, comuna TEXT, num_patentes INTEGER,
saldo_total REAL, saldo_promedio REAL,
monto_total REAL, monto_promedio REAL
```

### electrico.db

**clientes_por_comuna** (329 filas)
```sql
nombre_comuna TEXT PRIMARY KEY, nombre_region TEXT,
total_clientes INTEGER, potencia_total_kw REAL, potencia_promedio_kw REAL
```

**clientes_por_empresa_comuna** (482 filas)
```sql
empresa TEXT, nombre_comuna TEXT, nombre_region TEXT,
total_clientes INTEGER, potencia_total_kw REAL
```

**distribucion_potencia_comuna** (2,251 filas)
```sql
nombre_comuna TEXT, rango_potencia TEXT,
num_clientes INTEGER, potencia_subtotal_kw REAL
```

### geospatial.db

**embalses** (1,367 filas) — lat/lon WGS84
```sql
nombre TEXT, lat REAL, lon REAL, region TEXT, tipo TEXT
```

**estaciones_fluviometricas** (543 filas) — ATENCION: lat/lon en UTM, no WGS84
```sql
nombre TEXT, lat REAL, lon REAL, region TEXT
```

**estaciones_meteorologicas** (740 filas) — ATENCION: UTM
```sql
nombre TEXT, lat REAL, lon REAL, region TEXT
```

**calidad_agua** (1,435 filas) — UTM
```sql
nombre TEXT, lat REAL, lon REAL, ica TEXT
```

**uso_suelo_comuna** (878 filas) — sin coordenadas
```sql
comuna TEXT, region_code TEXT, uso_tierra TEXT,
superficie_ha REAL, num_registros INTEGER
```

**restricciones_hidricas** (233 filas) — lat/lon WGS84
```sql
cod_shac TEXT, regiones TEXT, tipo TEXT, descripcion TEXT,
lat_centroid REAL, lon_centroid REAL
```

**cuencas** (139 filas), **subcuencas** (489 filas) — lat/lon centroides
```sql
nombre TEXT, cod_cuen TEXT, lat_centroid REAL, lon_centroid REAL
```

**reserva_caudales** (47 filas) — lat/lon
```sql
nombre TEXT, region TEXT, lat_centroid REAL, lon_centroid REAL
```

---

## 4. SITIOS PVSYST (sites_db.json)

349 estaciones meteorologicas Meteonorm 8.2 cubriendo Chile de Arica (-18S) a Punta Arenas (-55S).

**Estructura de cada entrada:**
```json
{
    "lat": -33.4022,
    "lon": -71.1335,
    "alt": 222.0,
    "radio_km": 5.67,
    "csv_file": "Curacavi.CSV",
    "fuente_meteo": "Meteonorm 8.2 (2010-2019), Sat=100%",
    "global_h_monthly": [279.2, 224.6, ...],
    "t_amb_monthly": [20.31, 19.87, ...],
    "wind_monthly": [2.7, 2.5, ...],
    "hr_monthly": [55.0, 57.0, ...]
}
```

**Logica de busqueda:** `find_nearest_comuna(lat, lon)` calcula haversine a los 349 sitios y retorna el mas cercano dentro de 50km.

**Radios:** Cada sitio tiene un radio_km (max 20km) calculado para evitar solapamiento. Pero la busqueda real usa 50km para maxima cobertura.

---

## 5. PROBLEMAS CONOCIDOS Y OPORTUNIDADES

### Coordenadas UTM vs WGS84
Las tablas `estaciones_fluviometricas`, `estaciones_meteorologicas` y `calidad_agua` en geospatial.db tienen coordenadas en UTM (zona 19S), no lat/lon. La funcion `_haversine()` espera lat/lon → los calculos de distancia para estas tablas son incorrectos.

**Solucion:** Convertir UTM a lat/lon con pyproj, o reprocessar los GeoJSON originales.

### Precipitacion y Humedad
Actualmente la precipitacion viene de un dict hardcodeado con solo 4 localidades (Curacavi, Lolol, Santiago, Rancagua). Para las 349 estaciones nuevas, se usa el fallback "Santiago".

**Oportunidad:** Los archivos SIT ya contienen humedad relativa mensual (hr_monthly en sites_db.json). La precipitacion podria estimarse de fuentes como CR2 o WorldClim.

### Busqueda por Comuna
El predial usa `reverse_geocode_comuna()` via Nominatim (API externa, 5s timeout). Si falla, no hay informe predial.

**Oportunidad:** Cache local de geometrias comunales para geocoding offline.

### Performance de Bases de Datos
Sin indices explícitos en las tablas SQLite. La busqueda por comuna usa `UPPER()` que impide uso de indices.

**Oportunidad:** Crear indices en columna `comuna` de cada tabla.

---

## 6. COMO EXTENDER EL SISTEMA

### Agregar nueva seccion al informe predial:
1. Crear funcion `_get_nueva_seccion(comuna, lat, lon)` en `predial_engine.py`
2. Retornar dict con claves estandar: disponible, semaforo, texto_analitico, alertas
3. Agregar llamada en `generate_predial_report()`
4. Agregar tab de renderizado en `main.py`
5. Agregar seccion en `predial_pdf_generator.py`

### Agregar nueva especie al informe meteo:
1. Agregar entrada en `ESPECIES` dict en `climate_engine.py`
2. Definir: meses_ciclo, meses_flor, meses_cuaja, meses_madurez, meses_cosecha
3. Definir umbrales para cada variable bioclimatica
4. La tabla se genera automaticamente

### Agregar nueva fuente de datos:
1. Procesar datos a SQLite con script en `scripts/`
2. Colocar .db en `data/db/`
3. Agregar a `.gitattributes` si >50MB
4. Crear funcion de consulta en `predial_engine.py`
