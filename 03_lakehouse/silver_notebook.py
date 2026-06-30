# Databricks notebook — Capa Silver
# Sistema de Optimización Dinámica del Tráfico Urbano — Lima
#
# INSTRUCCIONES:
#   1. Copiar este archivo completo a un notebook nuevo en Databricks
#   2. Reemplazar ADLS_KEY con tu clave de acceso de Azure Storage
#   3. Ejecutar celda por celda (o "Run All")
#
# Lo que hace silver:
#   - Lee todos los Parquet de bronze desde ADLS Gen2
#   - Deduplica, valida tipos y filtra nulos en campos clave
#   - Genera tabla de métricas de calidad por fuente
#   - Escribe los datos limpios en silver/

# COMMAND ----------

# %pip install adlfs fsspec pyarrow pandas
# Ejecuta esta celda primero y reinicia el kernel si es la primera vez

# COMMAND ----------

import pandas as pd
import adlfs
from datetime import datetime, timezone

# --- Credenciales ADLS ---
# Pega aquí tu clave de acceso de Azure Storage (la encontrarás en el .env local)
ADLS_ACCOUNT   = "traficolima"
ADLS_KEY       = ""          # <-- pega tu ADLS_KEY aquí (ver .env local)
ADLS_CONTAINER = "trafico-lima"

SO = {"account_name": ADLS_ACCOUNT, "account_key": ADLS_KEY}

assert ADLS_KEY, "ERROR: debes pegar tu ADLS_KEY arriba antes de continuar"
print(f"Conectado a: {ADLS_ACCOUNT} / {ADLS_CONTAINER}")

# COMMAND ----------

# ── Funciones de lectura/escritura ──────────────────────────────────────────

def leer_bronze(fuente):
    """Lee todos los Parquet de una fuente desde bronze, retorna DataFrame."""
    import pyarrow.dataset as ds
    fs = adlfs.AzureBlobFileSystem(**SO)
    patron = f"{ADLS_CONTAINER}/bronze/{fuente}/**/*.parquet"
    archivos = fs.glob(patron)
    if not archivos:
        print(f"  [!] {fuente}: sin archivos en bronze todavía")
        return pd.DataFrame()
    # pyarrow.dataset resuelve automáticamente conflictos de schema entre archivos
    dataset = ds.dataset(archivos, filesystem=fs, format="parquet")
    df = dataset.to_table().to_pandas()
    # Normaliza _ingest_ts a string ISO sin microsegundos (mezcla histórico + real)
    if "_ingest_ts" in df.columns:
        df["_ingest_ts"] = (pd.to_datetime(df["_ingest_ts"], format="mixed", utc=True)
                              .dt.strftime("%Y-%m-%dT%H:%M:%S+00:00"))
    print(f"  {fuente}: {len(df)} registros bronze leídos ({len(archivos)} archivos)")
    return df


def guardar_silver(df, fuente):
    """Escribe DataFrame limpio en silver/."""
    if df.empty:
        print(f"  [!] {fuente}: DataFrame vacío, no se escribe silver")
        return
    ruta = f"abfs://{ADLS_CONTAINER}/silver/{fuente}/datos.parquet"
    df.to_parquet(ruta, storage_options=SO, index=False)
    print(f"  {fuente}: {len(df)} registros guardados en silver")


def reporte_calidad(fuente, df_raw, df_silver, campo_clave):
    """Retorna dict con métricas de calidad para el resumen final."""
    n_raw    = len(df_raw)
    n_silver = len(df_silver)
    nulos    = df_silver[campo_clave].isna().sum() if campo_clave in df_silver else 0
    return {
        "fuente":            fuente,
        "registros_bronze":  n_raw,
        "registros_silver":  n_silver,
        "descartados":       n_raw - n_silver,
        "pct_retenidos":     round(n_silver / n_raw * 100, 1) if n_raw else 0,
        "nulos_campo_clave": nulos,
        "campo_clave":       campo_clave,
        "calidad_fuente":    df_raw["_calidad_fuente"].iloc[0] if "_calidad_fuente" in df_raw and not df_raw.empty else "?",
    }

metricas = []
print("Silver iniciado:", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"))

# COMMAND ----------

# ── 1. CLIMA ────────────────────────────────────────────────────────────────
print("\n[1/6] Clima")

df = leer_bronze("clima")
if not df.empty:
    df_s = (df
        .dropna(subset=["temperatura_c", "humedad_pct", "zona"])
        .drop_duplicates(subset=["zona", "_ingest_ts"])
        .astype({
            "temperatura_c":    "float32",
            "humedad_pct":      "float32",
            "precipitacion_mm": "float32",
            "viento_kmh":       "float32",
            "codigo_clima":     "Int16",
        })
        .sort_values(["zona", "_ingest_ts"])
        .reset_index(drop=True)
    )
    guardar_silver(df_s, "clima")
    metricas.append(reporte_calidad("clima", df, df_s, "temperatura_c"))

# COMMAND ----------

# ── 2. SENSORES DE TRÁFICO ───────────────────────────────────────────────────
print("\n[2/6] Sensores de tráfico")

df = leer_bronze("sensores_trafico")
if not df.empty:
    df_s = df.dropna(subset=["congestion_ratio", "zona"]).drop_duplicates(subset=["zona", "_ingest_ts"])
    for c in ["intensidad_veh_hora", "capacidad_veh_hora", "sensores_activos"]:
        if c in df_s.columns:
            df_s[c] = pd.to_numeric(df_s[c], errors="coerce").round().astype("Int32")
    df_s = (df_s
        .astype({
            "congestion_ratio": "float32",
            "ocupacion_pct":    "float32",
            "carga_pct":        "float32",
        })
        .query("0 <= congestion_ratio <= 1")
        .sort_values(["zona", "_ingest_ts"])
        .reset_index(drop=True)
    )
    guardar_silver(df_s, "sensores_trafico")
    metricas.append(reporte_calidad("sensores_trafico", df, df_s, "congestion_ratio"))

# COMMAND ----------

# ── 3. GPS / RUTAS ───────────────────────────────────────────────────────────
print("\n[3/6] GPS / Rutas")

df = leer_bronze("gps_rutas")
if not df.empty:
    df_s = df.dropna(subset=["velocidad_kmh", "congestion_factor", "zona"]).drop_duplicates(subset=["zona", "_ingest_ts"])
    for c in ["distancia_m", "duracion_libre_s", "duracion_trafico_s"]:
        if c in df_s.columns:
            df_s[c] = pd.to_numeric(df_s[c], errors="coerce").round().astype("Int32")
    df_s = (df_s
        .astype({
            "velocidad_kmh":     "float32",
            "congestion_factor": "float32",
        })
        .query("1 <= velocidad_kmh <= 130 and 1.0 <= congestion_factor <= 5.0")
        .sort_values(["zona", "_ingest_ts"])
        .reset_index(drop=True)
    )
    guardar_silver(df_s, "gps_rutas")
    metricas.append(reporte_calidad("gps_rutas", df, df_s, "congestion_factor"))

# COMMAND ----------

# ── 4. CÁMARAS / VISIÓN ──────────────────────────────────────────────────────
print("\n[4/6] Cámaras")

df = leer_bronze("vision_camaras")
if not df.empty:
    df_s = (df
        .dropna(subset=["total_detectado", "zona", "camara_id"])
        .drop_duplicates(subset=["camara_id", "_ingest_ts"])
        .query("total_detectado >= 0")
        .astype({
            "total_detectado":  "Int32",
            "hora_local":       "Int8",
        })
        .sort_values(["zona", "camara_id", "_ingest_ts"])
        .reset_index(drop=True)
    )
    guardar_silver(df_s, "vision_camaras")
    metricas.append(reporte_calidad("vision_camaras", df, df_s, "total_detectado"))

# COMMAND ----------

# ── 5. EVENTOS ───────────────────────────────────────────────────────────────
print("\n[5/6] Eventos")

df = leer_bronze("eventos")
if not df.empty:
    df_s = df.dropna(subset=["nombre", "fecha", "impacto_factor"]).drop_duplicates(subset=["nombre", "fecha"])
    if "dias_para_evento" in df_s.columns:
        df_s["dias_para_evento"] = pd.to_numeric(df_s["dias_para_evento"], errors="coerce").round().astype("Int16")
    df_s = (df_s
        .astype({"impacto_factor": "float32"})
        .query("1.0 <= impacto_factor <= 2.0")
        .sort_values(["fecha", "nombre"])
        .reset_index(drop=True)
    )
    guardar_silver(df_s, "eventos")
    metricas.append(reporte_calidad("eventos", df, df_s, "impacto_factor"))

# COMMAND ----------

# ── 6. NOTICIAS ──────────────────────────────────────────────────────────────
print("\n[6/6] Noticias")

df = leer_bronze("redes_noticias")
if not df.empty:
    df_s = (df
        .dropna(subset=["titulo", "relevance_score"])
        .drop_duplicates(subset=["item_id"])        # dedup por URL hash
        .astype({
            "relevance_score": "float32",
            "es_trafico":      "bool",
        })
        .query("0 <= relevance_score <= 1")
        .sort_values(["_ingest_ts", "relevance_score"], ascending=[True, False])
        .reset_index(drop=True)
    )
    guardar_silver(df_s, "redes_noticias")
    metricas.append(reporte_calidad("redes_noticias", df, df_s, "relevance_score"))

# COMMAND ----------

# ── RESUMEN DE CALIDAD ───────────────────────────────────────────────────────
print("\n" + "="*65)
print("  RESUMEN DE CALIDAD — Bronze → Silver")
print("="*65)

df_resumen = pd.DataFrame(metricas)
print(df_resumen.to_string(index=False))

print("\nLeyenda calidad_fuente:")
print("  real      = dato real de Lima / Perú")
print("  proxy     = dato real de Madrid mapeado a Lima")
print("  calibrado = sintético con patrones reales")
print("="*65)
print("Silver completado:", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"))
