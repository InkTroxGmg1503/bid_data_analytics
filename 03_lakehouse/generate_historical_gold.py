# Databricks notebook — Generación de datos históricos sintéticos
# Escribe directamente en gold/congestion_por_zona/datos.parquet (ADLS)
#
# Output: ~525 960 filas  (6 zonas × 24 h × 365.25 días × 10 años)
# Schema: idéntico al producido por gold_notebook.py
#
# Patrones calibrados con:
#   - TomTom Traffic Index Lima 2023 (hora punta)
#   - SENAMHI Lima (estacionalidad garúa jun-oct)
#   - MTC 2024 (crecimiento parque automotor ~4 % anual)
#   - UK DfT 2025 + ajuste Lima (composición vehicular)

# COMMAND ----------

# %pip install adlfs fsspec pyarrow pandas numpy
# Ejecutar solo la primera vez

# COMMAND ----------

import pandas as pd
import numpy as np
import adlfs

ADLS_ACCOUNT   = "traficolima"
ADLS_KEY       = ""           # <-- pega tu ADLS_KEY aquí
ADLS_CONTAINER = "trafico-lima"
SO = {"account_name": ADLS_ACCOUNT, "account_key": ADLS_KEY}

ANIO_INICIO = 2015
ANIO_FIN    = 2024
SEED        = 42

assert ADLS_KEY, "ERROR: pega tu ADLS_KEY antes de continuar"
rng = np.random.default_rng(SEED)

# COMMAND ----------

# ── CONSTANTES ────────────────────────────────────────────────────────────────

ZONAS = [
    {"nombre": "Via Expresa - Centro",               "base": 0.65, "vel_libre": 80.0},
    {"nombre": "Javier Prado - San Isidro",          "base": 0.55, "vel_libre": 60.0},
    {"nombre": "Panamericana Norte - Independencia", "base": 0.50, "vel_libre": 80.0},
    {"nombre": "Carretera Central - Ate",            "base": 0.58, "vel_libre": 80.0},
    {"nombre": "Av. Brasil - Magdalena",             "base": 0.45, "vel_libre": 40.0},
    {"nombre": "Costa Verde - Miraflores",           "base": 0.40, "vel_libre": 70.0},
]

# Factor horario — curva bimodal hora punta Lima (TomTom 2023)
FACTOR_HORA = np.array([
    0.08, 0.05, 0.04, 0.04, 0.06, 0.14,   # 00-05
    0.38, 0.82, 1.00, 0.87, 0.66, 0.60,   # 06-11
    0.70, 0.75, 0.65, 0.62, 0.72, 0.92,   # 12-17
    1.00, 0.92, 0.72, 0.52, 0.36, 0.20,   # 18-23
])

# Factor día de semana (0=lun … 6=dom)
FACTOR_DIA = np.array([1.15, 1.10, 1.10, 1.12, 1.25, 0.78, 0.48])

# Factor mensual — garúa jun-oct sube congestión (SENAMHI Lima)
FACTOR_MES = np.array([
    1.00, 1.00, 1.02, 1.00, 0.98,   # ene-may
    1.10, 1.12, 1.12, 1.10, 1.08,   # jun-oct
    1.00, 1.00,                       # nov-dic
])

# Capacidad base por zona (veh/h en flujo libre)
CAPACIDAD = np.array([1020, 720, 900, 900, 420, 840])

# Composición vehicular Lima (UK DfT 2025 ajustado)
COMPOSICION = {
    "pct_auto_taxi":     0.55,
    "pct_combi_minibus": 0.20,
    "pct_moto_mototaxi": 0.12,
    "pct_bus":           0.05,
    "pct_camioneta_lgv": 0.06,
    "pct_camion_hgv":    0.02,
}

TIPOS_EVENTO  = ["ninguno"] * 12 + ["deportivo", "cultural", "accidente", "obras", "marcha", "feriado"]
SEVER_EVENTO  = {"ninguno": "ninguna", "deportivo": "media", "cultural": "baja",
                  "accidente": "alta", "obras": "media", "marcha": "alta", "feriado": "baja"}
IMPACTO_EVENTO = {"ninguno": 1.0, "deportivo": 1.35, "cultural": 1.15,
                   "accidente": 1.55, "obras": 1.40, "marcha": 1.50, "feriado": 0.85}

NIVEL_SERVICIO_MAP = ["A", "B", "C", "D", "E", "F"]

# COMMAND ----------

# ── GENERACIÓN DE TIMESTAMPS ──────────────────────────────────────────────────

print("Generando index temporal...")
fechas = pd.date_range(
    start=f"{ANIO_INICIO}-01-01",
    end=f"{ANIO_FIN}-12-31 23:00:00",
    freq="h"
)
print(f"  {len(fechas):,} horas  ×  {len(ZONAS)} zonas  =  {len(fechas)*len(ZONAS):,} filas")

# COMMAND ----------

# ── GENERACIÓN POR ZONA ───────────────────────────────────────────────────────

bloques = []

for z_idx, zona in enumerate(ZONAS):
    n = len(fechas)

    # ── Índices temporales ────────────────────────────────────────────────────
    hora      = fechas.hour.values                        # 0-23
    dia_sem   = fechas.dayofweek.values                   # 0=lun, 6=dom
    mes_idx   = fechas.month.values - 1                   # 0-11
    anio      = fechas.year.values
    dia_anio  = fechas.dayofyear.values

    # ── Factores base de congestión ───────────────────────────────────────────
    f_hora  = FACTOR_HORA[hora]
    f_dia   = FACTOR_DIA[dia_sem]
    f_mes   = FACTOR_MES[mes_idx]
    f_anio  = 1.0 + 0.04 * (anio - ANIO_INICIO)          # +4 % anual MTC 2024
    f_zona  = zona["base"]

    congestion_base = (f_hora * f_dia * f_mes * f_anio * f_zona).clip(0, 1)

    # ── Eventos (≈7 % de horas con impacto) ──────────────────────────────────
    tipo_ev  = rng.choice(TIPOS_EVENTO, size=n)
    impacto  = np.array([IMPACTO_EVENTO[t] for t in tipo_ev], dtype="float32")
    severidad = np.array([SEVER_EVENTO[t]  for t in tipo_ev])

    # ── Clima (Lima: temperatura suave 15-26 °C, humedad alta en garúa) ──────
    # Temperatura: mínima en jul-ago, máxima en feb
    temp_base = 20 + 3 * np.sin(2 * np.pi * (dia_anio - 45) / 365)
    temperatura_c    = (temp_base + rng.normal(0, 1.2, n)).clip(-5, 45).astype("float32")
    humedad_pct      = (70 + 15 * FACTOR_MES[mes_idx] + rng.normal(0, 5, n)).clip(0, 100).astype("float32")
    # Lluvia muy escasa en Lima; algo más en garúa y verano
    prob_lluvia = np.where((mes_idx >= 5) & (mes_idx <= 9), 0.08, 0.02)
    llueve      = rng.random(n) < prob_lluvia
    precipitacion_mm = np.where(llueve, rng.exponential(2.0, n), 0.0).clip(0, 50).astype("float32")
    viento_kmh       = (12 + rng.normal(0, 3, n)).clip(0, 60).astype("float32")
    # WMO code: 0=despejado, 45=garúa, 61=lluvia ligera
    codigo_clima = np.where(llueve, 61,
                   np.where((mes_idx >= 5) & (mes_idx <= 9), 45, 0)).astype("float32")

    # ── Congestión final ──────────────────────────────────────────────────────
    lluvia_boost  = np.where(precipitacion_mm > 0.5, 1.18, 1.0)
    congestion_ratio = (congestion_base * impacto * lluvia_boost
                        + rng.normal(0, 0.05, n)).clip(0, 1).astype("float32")

    # ── Sensores ──────────────────────────────────────────────────────────────
    # intensidad: proporcional a capacidad × congestion
    capacidad_zona  = CAPACIDAD[z_idx]
    intensidad_veh_hora = (capacidad_zona * congestion_ratio
                           + rng.normal(0, 20, n)).clip(0).astype("float32")

    # nivel_servicio A-F segun congestion_ratio
    ns_idx = np.digitize(congestion_ratio, [0.30, 0.50, 0.65, 0.80, 0.95])
    nivel_servicio = np.array([NIVEL_SERVICIO_MAP[i] for i in ns_idx])

    # ── GPS ───────────────────────────────────────────────────────────────────
    # congestion_factor 1.0-5.0
    congestion_factor  = (1.0 + 4.0 * congestion_ratio + rng.normal(0, 0.15, n)).clip(1, 5).astype("float32")
    velocidad_kmh      = (zona["vel_libre"] * (1 - 0.9 * congestion_ratio)
                          + rng.normal(0, 3, n)).clip(3, 130).astype("float32")
    duracion_trafico_s = (300 * congestion_factor + rng.normal(0, 60, n)).clip(60).astype("float32")

    # ── Cámaras ───────────────────────────────────────────────────────────────
    total_vehiculos_zona = intensidad_veh_hora / 12  # intervalo 5 min equiv
    vehiculos_entrada    = (total_vehiculos_zona * rng.uniform(0.45, 0.55, n)).astype("float32")
    vehiculos_salida     = (total_vehiculos_zona - vehiculos_entrada).clip(0).astype("float32")
    total_vehiculos_zona = total_vehiculos_zona.astype("float32")

    pct_cols = {}
    for k, prop in COMPOSICION.items():
        pct_cols[k] = (prop * 100 + rng.normal(0, 1.5, n)).clip(0, 100).astype("float32")
    pct_cols["pct_transporte_publico"] = (pct_cols["pct_bus"] + pct_cols["pct_combi_minibus"]).astype("float32")

    # ── Feature engineering ───────────────────────────────────────────────────
    hora_sin         = np.sin(2 * np.pi * hora / 24).astype("float32")
    hora_cos         = np.cos(2 * np.pi * hora / 24).astype("float32")
    es_hora_punta    = np.isin(hora, [7, 8, 9, 17, 18, 19]).astype("int8")
    es_fin_de_semana = (dia_sem >= 5).astype("int8")
    periodo_dia      = np.select(
        [hora <= 5, hora <= 11, hora <= 16, hora <= 20],
        ["madrugada", "manana", "tarde", "noche_temprana"],
        default="noche"
    )
    lluvia_flag      = (precipitacion_mm > 0.5).astype("int8")
    lluvia_intensa   = (precipitacion_mm > 5.0).astype("int8")

    vel_relativa     = (velocidad_kmh / zona["vel_libre"]).clip(0, 1.5).astype("float32")
    presion_evento   = (impacto - 1.0).astype("float32")
    ratio_flujo      = (vehiculos_entrada / np.where(vehiculos_salida == 0, 1, vehiculos_salida)).clip(0.3, 3.0).astype("float32")

    # Índice de congestión compuesto (GPS 60 % + sensor 30 % + cámaras 10 %)
    cam_max          = total_vehiculos_zona.max() or 1.0
    indice_congestion = (
        congestion_factor * 0.60 +
        (congestion_ratio + 1.0) * 0.30 +
        (total_vehiculos_zona / cam_max + 1.0) * 0.10
    ).round(3).astype("float32")

    # Noticias (sintético simple)
    noticias_trafico_cnt     = (congestion_ratio * 3 + rng.poisson(0.5, n)).astype("int32")
    sentimiento_negativo_cnt = (noticias_trafico_cnt * rng.uniform(0.3, 0.7, n)).astype("int32")
    relevance_score_max      = (congestion_ratio * 0.8 + rng.uniform(0, 0.2, n)).astype("float32")

    # ── Armar DataFrame ───────────────────────────────────────────────────────
    df_z = pd.DataFrame({
        "zona":                      zona["nombre"],
        "fecha":                     fechas.strftime("%Y-%m-%d"),
        "hora":                      hora.astype("int32"),
        "dia_semana":                dia_sem.astype("int32"),
        "temperatura_c":             temperatura_c,
        "humedad_pct":               humedad_pct,
        "precipitacion_mm":          precipitacion_mm,
        "viento_kmh":                viento_kmh,
        "codigo_clima":              codigo_clima,
        "congestion_ratio":          congestion_ratio,
        "intensidad_veh_hora":       intensidad_veh_hora,
        "nivel_servicio":            nivel_servicio,
        "congestion_factor":         congestion_factor,
        "velocidad_kmh":             velocidad_kmh,
        "duracion_trafico_s":        duracion_trafico_s,
        "total_vehiculos_zona":      total_vehiculos_zona,
        **pct_cols,
        "vehiculos_entrada":         vehiculos_entrada,
        "vehiculos_salida":          vehiculos_salida,
        "impacto_factor_evento":     impacto,
        "tipo_evento":               tipo_ev,
        "severidad":                 severidad,
        "tiene_evento":              (tipo_ev != "ninguno").astype("int8"),
        "es_feriado":                (tipo_ev == "feriado").astype("int8"),
        "noticias_trafico_cnt":      noticias_trafico_cnt,
        "sentimiento_negativo_cnt":  sentimiento_negativo_cnt,
        "relevance_score_max":       relevance_score_max,
        "hora_sin":                  hora_sin,
        "hora_cos":                  hora_cos,
        "es_hora_punta":             es_hora_punta,
        "es_fin_de_semana":          es_fin_de_semana,
        "periodo_dia":               periodo_dia,
        "lluvia_flag":               lluvia_flag,
        "lluvia_intensa":            lluvia_intensa,
        "velocidad_relativa":        vel_relativa,
        "presion_evento":            presion_evento,
        "ratio_flujo_camara":        ratio_flujo,
        "indice_congestion":         indice_congestion,
        "_calidad_fuente":           "calibrado",
    })
    bloques.append(df_z)
    print(f"  {zona['nombre']}: {len(df_z):,} filas generadas")

gold = pd.concat(bloques, ignore_index=True)
gold = gold.sort_values(["zona", "fecha", "hora"]).reset_index(drop=True)
print(f"\nTotal: {len(gold):,} filas × {len(gold.columns)} columnas")

# COMMAND ----------

# ── LAG FEATURES ──────────────────────────────────────────────────────────────

print("Calculando lag features...")
gold["congestion_factor_lag1h"] = (gold.groupby("zona")["congestion_factor"]
                                       .shift(1).fillna(method="bfill").astype("float32"))
gold["indice_congestion_lag1h"] = (gold.groupby("zona")["indice_congestion"]
                                       .shift(1).fillna(method="bfill").astype("float32"))
gold["tendencia_congestion"]    = (gold["indice_congestion"] - gold["indice_congestion_lag1h"]).round(3).astype("float32")

# COMMAND ----------

# ── VARIABLE OBJETIVO ─────────────────────────────────────────────────────────

print("Clasificando nivel_congestion...")
gold["nivel_congestion"] = np.select(
    [gold["congestion_factor"] < 1.25,
     gold["congestion_factor"] < 1.70],
    ["bajo", "medio"],
    default="alto"
)

print("\nDistribución nivel_congestion:")
print(gold["nivel_congestion"].value_counts().to_string())

# COMMAND ----------

# ── GUARDAR EN ADLS ───────────────────────────────────────────────────────────

print("\nGuardando en ADLS...")
ruta = f"abfs://{ADLS_CONTAINER}/gold/congestion_por_zona/datos.parquet"
gold.to_parquet(ruta, storage_options=SO, index=False)

print(f"\nListo.")
print(f"  Filas:   {len(gold):,}")
print(f"  Columnas: {len(gold.columns)}")
print(f"  Ruta:    {ruta}")
print(f"  Años:    {ANIO_INICIO}-{ANIO_FIN}")
print(f"\nDistribución por año:")
print(gold.groupby(gold['fecha'].str[:4])['nivel_congestion']
          .value_counts().unstack(fill_value=0).to_string())
