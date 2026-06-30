# Databricks notebook — Capa Gold
# Sistema de Optimización Dinámica del Tráfico Urbano — Lima
#
# Produce 3 tablas en gold/:
#   congestion_por_zona/   → ML + PowerBI (principal)
#   recomendaciones_ruta/  → rutas alternativas cuando hay congestion alta
#   semaforos_simulados/   → tiempos optimos de verde por zona

# COMMAND ----------

# %pip install adlfs fsspec pyarrow pandas numpy
# Ejecutar solo la primera vez

# COMMAND ----------

import pandas as pd
import numpy as np
import adlfs
from datetime import datetime, timezone

ADLS_ACCOUNT   = "traficolima"
ADLS_KEY       = ""          # <-- pega tu ADLS_KEY aquí (ver .env local)
ADLS_CONTAINER = "trafico-lima"
SO = {"account_name": ADLS_ACCOUNT, "account_key": ADLS_KEY}

ZONAS = [
    "Via Expresa - Centro",
    "Javier Prado - San Isidro",
    "Panamericana Norte - Independencia",
    "Carretera Central - Ate",
    "Av. Brasil - Magdalena",
    "Costa Verde - Miraflores",
]

assert ADLS_KEY, "ERROR: pega tu ADLS_KEY antes de continuar"
print("Gold iniciado:", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"))

# COMMAND ----------

# ── Funciones base ──────────────────────────────────────────────────────────

def leer_silver(fuente):
    ruta = f"abfs://{ADLS_CONTAINER}/silver/{fuente}/datos.parquet"
    try:
        df = pd.read_parquet(ruta, storage_options=SO)
        print(f"  {fuente}: {len(df):,} registros")
        return df
    except Exception as e:
        print(f"  [!] {fuente}: {e}")
        return pd.DataFrame()


def agregar_tiempo(df):
    df = df.copy()
    df["_ts"]        = pd.to_datetime(df["_ingest_ts"], utc=True)
    df["fecha"]      = df["_ts"].dt.strftime("%Y-%m-%d")
    df["hora"]       = df["_ts"].dt.hour
    df["dia_semana"] = df["_ts"].dt.dayofweek
    return df


def guardar_gold(df, nombre):
    ruta = f"abfs://{ADLS_CONTAINER}/gold/{nombre}/datos.parquet"
    df.to_parquet(ruta, storage_options=SO, index=False)
    print(f"  [{nombre}] {len(df)} filas guardadas")
    return ruta


print("Leyendo silver...")

# COMMAND ----------

# ── 1. CLIMA ────────────────────────────────────────────────────────────────

df_clima = leer_silver("clima")
df_clima = agregar_tiempo(df_clima)

g_clima = (df_clima
    .groupby(["zona", "fecha", "hora", "dia_semana"])
    .agg(
        temperatura_c    = ("temperatura_c",    "mean"),
        humedad_pct      = ("humedad_pct",      "mean"),
        precipitacion_mm = ("precipitacion_mm", "mean"),
        viento_kmh       = ("viento_kmh",       "mean"),
        codigo_clima     = ("codigo_clima",     lambda x: x.mode().iloc[0] if len(x) else np.nan),
    )
    .round(2)
    .reset_index()
)
print(f"Clima: {len(g_clima)} filas")

# COMMAND ----------

# ── 2. SENSORES ──────────────────────────────────────────────────────────────

df_sensores = leer_silver("sensores_trafico")
df_sensores = agregar_tiempo(df_sensores)

g_sensores = (df_sensores
    .groupby(["zona", "fecha", "hora"])
    .agg(
        congestion_ratio    = ("congestion_ratio",    "mean"),
        intensidad_veh_hora = ("intensidad_veh_hora", "mean"),
        nivel_servicio      = ("nivel_servicio",      lambda x: x.mode().iloc[0] if len(x) else "-1"),
    )
    .round(3)
    .reset_index()
)
print(f"Sensores: {len(g_sensores)} filas")

# COMMAND ----------

# ── 3. GPS ───────────────────────────────────────────────────────────────────

df_gps = leer_silver("gps_rutas")
df_gps = agregar_tiempo(df_gps)

g_gps = (df_gps
    .groupby(["zona", "fecha", "hora"])
    .agg(
        congestion_factor  = ("congestion_factor",  "mean"),
        velocidad_kmh      = ("velocidad_kmh",      "mean"),
        duracion_trafico_s = ("duracion_trafico_s", "mean"),
    )
    .round(3)
    .reset_index()
)
print(f"GPS: {len(g_gps)} filas")

# COMMAND ----------

# ── 4. CAMARAS ───────────────────────────────────────────────────────────────

df_camaras = leer_silver("vision_camaras")
df_camaras = agregar_tiempo(df_camaras)
cnt_cols = [c for c in df_camaras.columns if c.startswith("cnt_")]

# 4a. Total y composicion por zona (suma entrada+salida, promedio horario)
g_cam_batch = (df_camaras
    .groupby(["zona", "fecha", "hora", "_ingest_ts"])
    [["total_detectado"] + cnt_cols]
    .sum()
    .reset_index()
)
g_camaras = (g_cam_batch
    .groupby(["zona", "fecha", "hora"])
    [["total_detectado"] + cnt_cols]
    .mean()
    .round(1)
    .reset_index()
    .rename(columns={"total_detectado": "total_vehiculos_zona"})
)
total = g_camaras[cnt_cols].sum(axis=1).replace(0, np.nan)
for c in cnt_cols:
    g_camaras[f"pct_{c.replace('cnt_','')}"] = (g_camaras[c] / total * 100).round(1)
pct_bus   = g_camaras.get("pct_bus",          pd.Series(0.0, index=g_camaras.index))
pct_combi = g_camaras.get("pct_combi_minibus", pd.Series(0.0, index=g_camaras.index))
g_camaras["pct_transporte_publico"] = (pct_bus + pct_combi).round(1)
g_camaras = g_camaras.drop(columns=cnt_cols)

# 4b. Flujo entrada vs salida por hora (necesario para simulacion semaforos)
g_entrada = (df_camaras[df_camaras["posicion"] == "entrada"]
    .groupby(["zona", "fecha", "hora", "_ingest_ts"])["total_detectado"].sum()
    .reset_index().rename(columns={"total_detectado": "v_entrada"})
)
g_salida = (df_camaras[df_camaras["posicion"] == "salida"]
    .groupby(["zona", "fecha", "hora", "_ingest_ts"])["total_detectado"].sum()
    .reset_index().rename(columns={"total_detectado": "v_salida"})
)
g_flujo = (g_entrada
    .merge(g_salida, on=["zona", "fecha", "hora", "_ingest_ts"], how="outer")
    .fillna(0)
    .groupby(["zona", "fecha", "hora"])
    .agg(vehiculos_entrada=("v_entrada", "mean"), vehiculos_salida=("v_salida", "mean"))
    .round(1)
    .reset_index()
)

print(f"Camaras: {len(g_camaras)} filas  |  Flujo entrada/salida: {len(g_flujo)} filas")

# COMMAND ----------

# ── 5. EVENTOS ───────────────────────────────────────────────────────────────

df_eventos = leer_silver("eventos")
ev_todas = df_eventos[df_eventos["zona_afectada"] == "todas"]
ev_zonas = df_eventos[df_eventos["zona_afectada"] != "todas"]

if not ev_todas.empty:
    expandidas = pd.concat(
        [ev_todas.assign(zona_afectada=z) for z in ZONAS],
        ignore_index=True
    )
    df_eventos = pd.concat([ev_zonas, expandidas], ignore_index=True)

g_eventos = (df_eventos
    .sort_values("impacto_factor", ascending=False)
    .groupby(["zona_afectada", "fecha"])
    .first()
    .reset_index()
    [["zona_afectada", "fecha", "impacto_factor", "tipo", "severidad"]]
    .rename(columns={"zona_afectada": "zona", "impacto_factor": "impacto_factor_evento", "tipo": "tipo_evento"})
)
g_eventos["tiene_evento"] = True
g_eventos["es_feriado"]   = g_eventos["tipo_evento"].isin(["feriado_oficial", "feriado", "feriado_especial"])
print(f"Eventos: {len(g_eventos)} combinaciones zona+fecha")

# COMMAND ----------

# ── 6. NOTICIAS ───────────────────────────────────────────────────────────────

df_noticias = leer_silver("redes_noticias")
df_noticias = agregar_tiempo(df_noticias)

g_noticias = (df_noticias
    .groupby(["fecha", "hora"])
    .agg(
        noticias_trafico_cnt     = ("es_trafico",      "sum"),
        sentimiento_negativo_cnt = ("sentiment",       lambda x: (x == "negativo").sum()),
        relevance_score_max      = ("relevance_score", "max"),
    )
    .reset_index()
)
print(f"Noticias: {len(g_noticias)} filas")

# COMMAND ----------

# ── 7. JOIN — tabla gold base ─────────────────────────────────────────────────

gold = g_clima.copy()
gold = gold.merge(g_sensores, on=["zona", "fecha", "hora"], how="left")
gold = gold.merge(g_gps,      on=["zona", "fecha", "hora"], how="left")
gold = gold.merge(g_camaras,  on=["zona", "fecha", "hora"], how="left")
gold = gold.merge(g_flujo,    on=["zona", "fecha", "hora"], how="left")
gold = gold.merge(g_eventos,  on=["zona", "fecha"],         how="left")
gold = gold.merge(g_noticias, on=["fecha", "hora"],         how="left")

gold["tiene_evento"]             = gold["tiene_evento"].fillna(False)
gold["es_feriado"]               = gold["es_feriado"].fillna(False)
gold["impacto_factor_evento"]    = gold["impacto_factor_evento"].fillna(1.0)
gold["tipo_evento"]              = gold["tipo_evento"].fillna("ninguno")
gold["severidad"]                = gold["severidad"].fillna("ninguna")
gold["noticias_trafico_cnt"]     = gold["noticias_trafico_cnt"].fillna(0).astype(int)
gold["sentimiento_negativo_cnt"] = gold["sentimiento_negativo_cnt"].fillna(0).astype(int)
gold["relevance_score_max"]      = gold["relevance_score_max"].fillna(0.0)
gold["vehiculos_entrada"]        = gold["vehiculos_entrada"].fillna(0.0)
gold["vehiculos_salida"]         = gold["vehiculos_salida"].fillna(0.0)

print(f"Tabla gold base: {len(gold)} filas x {len(gold.columns)} columnas")

# COMMAND ----------

# ── 8. FEATURE ENGINEERING ───────────────────────────────────────────────────

# 8a. Encoding ciclico de hora
gold["hora_sin"] = np.sin(2 * np.pi * gold["hora"] / 24).round(4)
gold["hora_cos"] = np.cos(2 * np.pi * gold["hora"] / 24).round(4)

# 8b. Flags de tiempo
gold["es_hora_punta"]    = gold["hora"].isin([7, 8, 9, 17, 18, 19])
gold["es_fin_de_semana"] = gold["dia_semana"] >= 5
gold["periodo_dia"] = pd.cut(
    gold["hora"],
    bins=[-1, 5, 11, 16, 20, 23],
    labels=["madrugada", "manana", "tarde", "noche_temprana", "noche"]
).astype(str)

# 8c. Flags de clima
gold["lluvia_flag"]    = gold["precipitacion_mm"] > 0.5
gold["lluvia_intensa"] = gold["precipitacion_mm"] > 5.0

# 8d. Indice de congestion compuesto (GPS 60% + sensor 30% + camaras 10%)
sensor_norm = gold["congestion_ratio"].fillna(0.0) + 1.0
cam_max     = gold["total_vehiculos_zona"].max()
cam_norm    = (gold["total_vehiculos_zona"].fillna(0) / cam_max + 1.0) if cam_max and cam_max > 0 else 1.0
gold["indice_congestion"] = (
    gold["congestion_factor"].fillna(1.0) * 0.60 +
    sensor_norm                            * 0.30 +
    cam_norm                               * 0.10
).round(3)

# 8e. Velocidad relativa al flujo libre del corredor
VEL_LIBRE = {
    "Via Expresa - Centro":               80.0,
    "Javier Prado - San Isidro":          60.0,
    "Panamericana Norte - Independencia": 80.0,
    "Carretera Central - Ate":            80.0,
    "Av. Brasil - Magdalena":             40.0,
    "Costa Verde - Miraflores":           70.0,
}
gold["vel_libre_ref"]      = gold["zona"].map(VEL_LIBRE)
gold["velocidad_relativa"] = (gold["velocidad_kmh"] / gold["vel_libre_ref"]).round(3).clip(0, 1.5)
gold = gold.drop(columns=["vel_libre_ref"])

# 8f. Presion de evento
gold["presion_evento"] = (gold["impacto_factor_evento"] - 1.0).round(3)

# 8g. Ratio flujo camara (entrada/salida) — usado en simulacion semaforos
gold["ratio_flujo_camara"] = (
    gold["vehiculos_entrada"] / gold["vehiculos_salida"].replace(0, 1)
).clip(0.3, 3.0).round(3)

print("Feature engineering completado.")

# COMMAND ----------

# ── 9. LAG FEATURES ──────────────────────────────────────────────────────────

gold = gold.sort_values(["zona", "fecha", "hora"]).reset_index(drop=True)

gold["congestion_factor_lag1h"] = gold.groupby("zona")["congestion_factor"].shift(1)
gold["indice_congestion_lag1h"] = gold.groupby("zona")["indice_congestion"].shift(1)
gold["tendencia_congestion"]    = (gold["indice_congestion"] - gold["indice_congestion_lag1h"]).round(3)

gold["congestion_factor_lag1h"] = gold["congestion_factor_lag1h"].fillna(gold["congestion_factor"])
gold["indice_congestion_lag1h"] = gold["indice_congestion_lag1h"].fillna(gold["indice_congestion"])
gold["tendencia_congestion"]    = gold["tendencia_congestion"].fillna(0.0)

print("Lag features anadidos.")

# COMMAND ----------

# ── 10. VARIABLE OBJETIVO: nivel_congestion ───────────────────────────────────

def clasificar_congestion(row):
    cf  = row.get("congestion_factor")
    idx = row.get("indice_congestion", 1.0)
    if pd.notna(cf):
        if cf < 1.25:    return "bajo"
        elif cf < 1.70:  return "medio"
        else:            return "alto"
    else:
        if idx < 1.25:   return "bajo"
        elif idx < 1.60: return "medio"
        else:            return "alto"

gold["nivel_congestion"] = gold.apply(clasificar_congestion, axis=1)

print("Distribucion nivel_congestion:")
print(gold["nivel_congestion"].value_counts().to_string())

# COMMAND ----------

# ── 11. LIMPIEZA FINAL PARA ML ────────────────────────────────────────────────

bool_cols = ["es_hora_punta", "es_fin_de_semana", "lluvia_flag",
             "lluvia_intensa", "tiene_evento", "es_feriado"]
for c in bool_cols:
    if c in gold.columns:
        gold[c] = gold[c].astype(int)

float_cols = [
    "temperatura_c", "humedad_pct", "precipitacion_mm", "viento_kmh",
    "congestion_ratio", "intensidad_veh_hora",
    "congestion_factor", "velocidad_kmh", "duracion_trafico_s",
    "total_vehiculos_zona", "vehiculos_entrada", "vehiculos_salida",
    "pct_transporte_publico", "ratio_flujo_camara",
    "impacto_factor_evento", "presion_evento",
    "hora_sin", "hora_cos", "indice_congestion", "velocidad_relativa",
    "congestion_factor_lag1h", "indice_congestion_lag1h", "tendencia_congestion",
    "relevance_score_max",
]
for c in float_cols:
    if c in gold.columns:
        gold[c] = pd.to_numeric(gold[c], errors="coerce").astype("float32")

print("Limpieza final completada.")

# COMMAND ----------

# ── 12. RECOMENDACIONES DE RUTA ───────────────────────────────────────────────

# Rutas alternativas por zona (corredores conectados geograficamente)
ALTERNATIVAS = {
    "Via Expresa - Centro":               ["Javier Prado - San Isidro", "Av. Brasil - Magdalena"],
    "Javier Prado - San Isidro":          ["Via Expresa - Centro", "Carretera Central - Ate"],
    "Panamericana Norte - Independencia": ["Via Expresa - Centro", "Av. Brasil - Magdalena"],
    "Carretera Central - Ate":            ["Javier Prado - San Isidro", "Via Expresa - Centro"],
    "Av. Brasil - Magdalena":             ["Costa Verde - Miraflores", "Via Expresa - Centro"],
    "Costa Verde - Miraflores":           ["Av. Brasil - Magdalena", "Via Expresa - Centro"],
}

# Lookup rapido de congestion actual por zona
lookup = gold.set_index(["zona", "fecha", "hora"])[
    ["congestion_factor", "indice_congestion", "nivel_congestion",
     "velocidad_kmh", "duracion_trafico_s"]
].to_dict("index")

recomendaciones = []
for _, row in gold.iterrows():
    zona    = row["zona"]
    fecha   = row["fecha"]
    hora    = int(row["hora"])
    nivel   = row["nivel_congestion"]

    alts = ALTERNATIVAS.get(zona, [])
    mejor_alt = None
    mejor_cf  = float("inf")

    for alt in alts:
        key = (alt, fecha, hora)
        if key not in lookup:
            continue
        alt_data = lookup[key]
        cf_alt = alt_data.get("congestion_factor") or alt_data.get("indice_congestion", 1.5)
        if cf_alt < mejor_cf:
            mejor_cf  = cf_alt
            mejor_alt = (alt, alt_data)

    if mejor_alt is None:
        continue

    alt_zona, alt_data = mejor_alt
    dur_orig = row.get("duracion_trafico_s") or 0
    dur_alt  = alt_data.get("duracion_trafico_s") or 0
    ahorro   = round(max(0, (dur_orig - dur_alt) / 60), 1)

    es_mejor = (mejor_cf < (row.get("congestion_factor") or row.get("indice_congestion", 1.5)))
    estado   = "RECOMENDADA" if es_mejor and alt_data.get("nivel_congestion") != "alto" else "ALTERNATIVA_POSIBLE"

    recomendaciones.append({
        "zona":                    zona,
        "fecha":                   fecha,
        "hora":                    hora,
        "nivel_congestion_actual": nivel,
        "congestion_factor_actual":round(float(row.get("congestion_factor") or 0), 3),
        "velocidad_actual_kmh":    round(float(row.get("velocidad_kmh") or 0), 1),
        "ruta_alternativa":        alt_zona,
        "nivel_alternativa":       alt_data.get("nivel_congestion", "desconocido"),
        "congestion_factor_alt":   round(float(mejor_cf), 3),
        "velocidad_alt_kmh":       round(float(alt_data.get("velocidad_kmh") or 0), 1),
        "ahorro_estimado_min":     ahorro,
        "estado":                  estado,
    })

df_recomendaciones = pd.DataFrame(recomendaciones)
# Solo mostrar filas donde la alternativa sea mejor
df_recomendaciones_utiles = df_recomendaciones[df_recomendaciones["estado"] == "RECOMENDADA"]

print(f"Recomendaciones generadas: {len(df_recomendaciones)} total  |  {len(df_recomendaciones_utiles)} utiles")
if not df_recomendaciones_utiles.empty:
    print(df_recomendaciones_utiles[["zona","hora","nivel_congestion_actual","ruta_alternativa","ahorro_estimado_min"]].to_string(index=False))

# COMMAND ----------

# ── 13. SIMULACION DE SEMAFOROS ───────────────────────────────────────────────

VERDE_BASE_SEG = 60
VERDE_MIN      = 30
VERDE_MAX      = 120

# Capacidad de referencia por tipo de corredor (vehiculos / 5 min)
CAPACIDAD_REF = {
    "Via Expresa - Centro":               85,
    "Javier Prado - San Isidro":          60,
    "Panamericana Norte - Independencia": 75,
    "Carretera Central - Ate":            75,
    "Av. Brasil - Magdalena":             35,
    "Costa Verde - Miraflores":           90,
}

df_semaforos = gold[["zona", "fecha", "hora", "nivel_congestion",
                      "vehiculos_entrada", "vehiculos_salida",
                      "ratio_flujo_camara", "congestion_factor",
                      "indice_congestion"]].copy()

df_semaforos["capacidad_ref"] = df_semaforos["zona"].map(CAPACIDAD_REF)

# Verde recomendado: proporcional al ratio entrada/salida
# Mas vehiculos entrando que saliendo → mas tiempo verde para la entrada
df_semaforos["verde_recomendado_s"] = (
    VERDE_BASE_SEG * df_semaforos["ratio_flujo_camara"]
).clip(VERDE_MIN, VERDE_MAX).round(0).astype(int)

df_semaforos["verde_actual_estimado_s"] = VERDE_BASE_SEG  # linea base fija

df_semaforos["ajuste_verde_s"] = (
    df_semaforos["verde_recomendado_s"] - df_semaforos["verde_actual_estimado_s"]
)

df_semaforos["accion_semaforo"] = df_semaforos["ratio_flujo_camara"].apply(
    lambda r: "ampliar_verde"  if r > 1.20 else
              "reducir_verde"  if r < 0.80 else
              "mantener"
)

# Impacto estimado de la accion: reduccion de congestion esperada
df_semaforos["impacto_estimado"] = df_semaforos["accion_semaforo"].map({
    "ampliar_verde": "Reduce cola entrada ~15-25%",
    "reducir_verde": "Libera salida, mejora fluidez",
    "mantener":      "Flujo equilibrado",
})

print("Simulacion de semaforos:")
print(df_semaforos[["zona","hora","accion_semaforo","verde_recomendado_s","ajuste_verde_s","nivel_congestion"]]
      .sort_values(["hora","zona"])
      .to_string(index=False))

# COMMAND ----------

# ── 14. SCHEMA FINAL ──────────────────────────────────────────────────────────

print("="*65)
print("  GOLD — congestion_por_zona   SCHEMA")
print("="*65)

grupos = {
    "CLAVE":         ["zona", "fecha", "hora", "dia_semana"],
    "TIEMPO":        ["hora_sin", "hora_cos", "es_hora_punta", "es_fin_de_semana", "periodo_dia"],
    "CLIMA":         ["temperatura_c", "humedad_pct", "precipitacion_mm",
                      "viento_kmh", "codigo_clima", "lluvia_flag", "lluvia_intensa"],
    "SENSORES":      ["congestion_ratio", "intensidad_veh_hora", "nivel_servicio"],
    "GPS":           ["congestion_factor", "velocidad_kmh", "duracion_trafico_s", "velocidad_relativa"],
    "CAMARAS":       ["total_vehiculos_zona", "vehiculos_entrada", "vehiculos_salida",
                      "pct_transporte_publico", "ratio_flujo_camara"],
    "EVENTOS":       ["tiene_evento", "es_feriado", "impacto_factor_evento", "tipo_evento", "presion_evento"],
    "NOTICIAS":      ["noticias_trafico_cnt", "sentimiento_negativo_cnt", "relevance_score_max"],
    "FEATURES":      ["indice_congestion", "congestion_factor_lag1h",
                      "indice_congestion_lag1h", "tendencia_congestion"],
    "TARGET":        ["nivel_congestion"],
}

total_cols = 0
for grupo, cols in grupos.items():
    existentes = [c for c in cols if c in gold.columns]
    total_cols += len(existentes)
    print(f"\n  [{grupo}]")
    for c in existentes:
        print(f"    {c:<40} nulos: {int(gold[c].isna().sum())}")

print(f"\n  Total columnas ML: {total_cols}  |  Filas: {len(gold)}")
print("="*65)

# COMMAND ----------

gold.head(6)

# COMMAND ----------

# ── 15. GUARDAR LAS 3 TABLAS GOLD ────────────────────────────────────────────

print("Guardando tablas gold en ADLS...")

guardar_gold(gold,               "congestion_por_zona")
guardar_gold(df_recomendaciones, "recomendaciones_ruta")
guardar_gold(df_semaforos,       "semaforos_simulados")

print("\nResumen final:")
print(f"  congestion_por_zona:  {len(gold)} filas x {len(gold.columns)} columnas")
print(f"  recomendaciones_ruta: {len(df_recomendaciones)} filas")
print(f"  semaforos_simulados:  {len(df_semaforos)} filas")
print("\nCompletado:", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"))
