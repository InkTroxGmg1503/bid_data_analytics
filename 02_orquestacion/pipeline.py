"""
Pipeline batch: Bronze ADLS → Silver → Gold → ADLS → Power BI CSVs en ADLS

Flujo:
  1. Lee bronze de las últimas 24h desde ADLS
  2. Aplica limpieza silver
  3. Aplica feature engineering gold + predicción ML
  4. Descarga gold histórico desde ADLS
  5. Merge: histórico + nuevo real (dedup por zona+fecha+hora)
  6. Sube gold combinado a ADLS
  7. Exporta KPIs directamente a ADLS (sin guardado local)

Uso:
    python 02_orquestacion/pipeline.py
    python 02_orquestacion/pipeline.py --skip-upload   (no sube a ADLS)
"""

import sys
import argparse
import traceback
import subprocess
from pathlib import Path
from datetime import datetime, timedelta, timezone

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import os
import pandas as pd
import numpy as np
import adlfs
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

ADLS_ACCOUNT   = os.getenv("ADLS_ACCOUNT", "traficolima")
ADLS_KEY       = os.getenv("ADLS_KEY", "")
ADLS_CONTAINER = os.getenv("ADLS_CONTAINER", "trafico-lima")
SO = {"account_name": ADLS_ACCOUNT, "account_key": ADLS_KEY}

ZONAS = [
    "Via Expresa - Centro",
    "Javier Prado - San Isidro",
    "Panamericana Norte - Independencia",
    "Carretera Central - Ate",
    "Av. Brasil - Magdalena",
    "Costa Verde - Miraflores",
]

def log(msg):
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


# ============================================================
# PASO 1 — LEER BRONZE DESDE ADLS (últimas 24h)
# ============================================================

def leer_bronze_adls(fuente, horas=24):
    """Lee los Parquet de una fuente desde ADLS bronze, últimas `horas` horas."""
    fs = adlfs.AzureBlobFileSystem(account_name=ADLS_ACCOUNT, account_key=ADLS_KEY)
    ahora = datetime.now(timezone.utc)
    fechas = [(ahora - timedelta(hours=h)).strftime("%Y-%m-%d") for h in range(horas)]
    fechas = sorted(set(fechas))

    dfs = []
    archivos_total = 0
    for fecha in fechas:
        patron = f"{ADLS_CONTAINER}/bronze/{fuente}/fecha={fecha}"
        try:
            archivos = fs.glob(f"{patron}/**/*.parquet")
            for arch in archivos:
                with fs.open(arch, "rb") as f:
                    dfs.append(pd.read_parquet(f))
                archivos_total += 1
        except Exception:
            pass

    if not dfs:
        log(f"  {fuente}: sin archivos en ADLS bronze (últimas {horas}h)")
        return pd.DataFrame()

    df = pd.concat(dfs, ignore_index=True)
    if "_ingest_ts" in df.columns:
        df["_ingest_ts"] = (
            pd.to_datetime(df["_ingest_ts"], format="mixed", utc=True)
            .dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")
        )
    log(f"  {fuente}: {len(df):,} registros ({archivos_total} archivos)")
    return df


# ============================================================
# PASO 2 — SILVER (limpieza)
# ============================================================

def procesar_silver(bronces):
    """Aplica la misma lógica de silver_notebook.py sobre DataFrames locales."""
    silver = {}

    # Clima
    df = bronces.get("clima", pd.DataFrame())
    if not df.empty:
        df_s = (df
            .dropna(subset=["temperatura_c", "humedad_pct", "zona"])
            .drop_duplicates(subset=["zona", "_ingest_ts"])
            .astype({"temperatura_c": "float32", "humedad_pct": "float32",
                     "precipitacion_mm": "float32", "viento_kmh": "float32"})
            .sort_values(["zona", "_ingest_ts"]).reset_index(drop=True)
        )
        silver["clima"] = df_s
        log(f"  silver clima: {len(df_s):,} filas")

    # Sensores tráfico
    df = bronces.get("sensores_trafico", pd.DataFrame())
    if not df.empty:
        df_s = df.dropna(subset=["congestion_ratio", "zona"]).drop_duplicates(subset=["zona", "_ingest_ts"])
        for c in ["intensidad_veh_hora", "capacidad_veh_hora", "sensores_activos"]:
            if c in df_s.columns:
                df_s[c] = pd.to_numeric(df_s[c], errors="coerce").round().astype("Int32")
        df_s = (df_s
            .astype({"congestion_ratio": "float32", "ocupacion_pct": "float32",
                     "carga_pct": "float32"})
            .query("0 <= congestion_ratio <= 1")
            .sort_values(["zona", "_ingest_ts"]).reset_index(drop=True)
        )
        silver["sensores_trafico"] = df_s
        log(f"  silver sensores: {len(df_s):,} filas")

    # GPS / Rutas
    df = bronces.get("gps_rutas", pd.DataFrame())
    if not df.empty:
        df_s = df.dropna(subset=["velocidad_kmh", "congestion_factor", "zona"]).drop_duplicates(subset=["zona", "_ingest_ts"])
        for c in ["distancia_m", "duracion_libre_s", "duracion_trafico_s"]:
            if c in df_s.columns:
                df_s[c] = pd.to_numeric(df_s[c], errors="coerce").round().astype("Int32")
        df_s = (df_s
            .astype({"velocidad_kmh": "float32", "congestion_factor": "float32"})
            .query("1 <= velocidad_kmh <= 130 and 1.0 <= congestion_factor <= 5.0")
            .sort_values(["zona", "_ingest_ts"]).reset_index(drop=True)
        )
        silver["gps_rutas"] = df_s
        log(f"  silver gps: {len(df_s):,} filas")

    # Cámaras
    df = bronces.get("vision_camaras", pd.DataFrame())
    if not df.empty:
        df_s = (df
            .dropna(subset=["total_detectado", "zona", "camara_id"])
            .drop_duplicates(subset=["camara_id", "_ingest_ts"])
            .query("total_detectado >= 0")
            .astype({"total_detectado": "Int32"})
            .sort_values(["zona", "camara_id", "_ingest_ts"]).reset_index(drop=True)
        )
        silver["vision_camaras"] = df_s
        log(f"  silver camaras: {len(df_s):,} filas")

    # Eventos
    df = bronces.get("eventos", pd.DataFrame())
    if not df.empty:
        df_s = df.dropna(subset=["nombre", "fecha", "impacto_factor"]).drop_duplicates(subset=["nombre", "fecha"])
        df_s = (df_s
            .astype({"impacto_factor": "float32"})
            .query("1.0 <= impacto_factor <= 2.0")
            .sort_values(["fecha", "nombre"]).reset_index(drop=True)
        )
        silver["eventos"] = df_s
        log(f"  silver eventos: {len(df_s):,} filas")

    # Noticias
    df = bronces.get("redes_noticias", pd.DataFrame())
    if not df.empty:
        df_s = (df
            .dropna(subset=["titulo", "relevance_score"])
            .drop_duplicates(subset=["item_id"])
            .astype({"relevance_score": "float32"})
            .query("0 <= relevance_score <= 1")
            .sort_values(["_ingest_ts", "relevance_score"], ascending=[True, False])
            .reset_index(drop=True)
        )
        silver["redes_noticias"] = df_s
        log(f"  silver noticias: {len(df_s):,} filas")

    return silver


# ============================================================
# PASO 3 — GOLD (feature engineering)
# ============================================================

def agregar_tiempo(df):
    df = df.copy()
    df["_ts"]        = pd.to_datetime(df["_ingest_ts"], utc=True)
    df["fecha"]      = df["_ts"].dt.strftime("%Y-%m-%d")
    df["hora"]       = df["_ts"].dt.hour
    df["dia_semana"] = df["_ts"].dt.dayofweek
    return df


def procesar_gold(silver):
    """Aplica la misma lógica de gold_notebook.py sobre los DataFrames silver."""

    # Clima
    df_clima = agregar_tiempo(silver.get("clima", pd.DataFrame()))
    g_clima = (df_clima.groupby(["zona", "fecha", "hora", "dia_semana"])
        .agg(temperatura_c=("temperatura_c","mean"), humedad_pct=("humedad_pct","mean"),
             precipitacion_mm=("precipitacion_mm","mean"), viento_kmh=("viento_kmh","mean"),
             codigo_clima=("codigo_clima", lambda x: x.mode().iloc[0] if len(x) else np.nan))
        .round(2).reset_index()
    ) if not df_clima.empty else pd.DataFrame()

    # Sensores
    df_sensores = agregar_tiempo(silver.get("sensores_trafico", pd.DataFrame()))
    g_sensores = (df_sensores.groupby(["zona", "fecha", "hora"])
        .agg(congestion_ratio=("congestion_ratio","mean"),
             intensidad_veh_hora=("intensidad_veh_hora","mean"),
             nivel_servicio=("nivel_servicio", lambda x: x.mode().iloc[0] if len(x) else "-1"))
        .round(3).reset_index()
    ) if not df_sensores.empty else pd.DataFrame()

    # GPS
    df_gps = agregar_tiempo(silver.get("gps_rutas", pd.DataFrame()))
    g_gps = (df_gps.groupby(["zona", "fecha", "hora"])
        .agg(congestion_factor=("congestion_factor","mean"),
             velocidad_kmh=("velocidad_kmh","mean"),
             duracion_trafico_s=("duracion_trafico_s","mean"))
        .round(3).reset_index()
    ) if not df_gps.empty else pd.DataFrame()

    # Cámaras
    df_camaras = agregar_tiempo(silver.get("vision_camaras", pd.DataFrame()))
    if not df_camaras.empty:
        cnt_cols = [c for c in df_camaras.columns if c.startswith("cnt_")]
        g_cam_batch = (df_camaras
            .groupby(["zona", "fecha", "hora", "_ingest_ts"])[["total_detectado"] + cnt_cols]
            .sum().reset_index()
        )
        g_camaras = (g_cam_batch.groupby(["zona", "fecha", "hora"])[["total_detectado"] + cnt_cols]
            .mean().round(1).reset_index()
            .rename(columns={"total_detectado": "total_vehiculos_zona"})
        )
        total = g_camaras[cnt_cols].sum(axis=1).replace(0, np.nan)
        for c in cnt_cols:
            g_camaras[f"pct_{c.replace('cnt_','')}"] = (g_camaras[c] / total * 100).round(1)
        pct_bus   = g_camaras.get("pct_bus",          pd.Series(0.0, index=g_camaras.index))
        pct_combi = g_camaras.get("pct_combi_minibus", pd.Series(0.0, index=g_camaras.index))
        g_camaras["pct_transporte_publico"] = (pct_bus + pct_combi).round(1)
        g_camaras = g_camaras.drop(columns=cnt_cols)

        g_entrada = (df_camaras[df_camaras["posicion"]=="entrada"]
            .groupby(["zona","fecha","hora","_ingest_ts"])["total_detectado"].sum()
            .reset_index().rename(columns={"total_detectado":"v_entrada"}))
        g_salida  = (df_camaras[df_camaras["posicion"]=="salida"]
            .groupby(["zona","fecha","hora","_ingest_ts"])["total_detectado"].sum()
            .reset_index().rename(columns={"total_detectado":"v_salida"}))
        g_flujo = (g_entrada.merge(g_salida, on=["zona","fecha","hora","_ingest_ts"], how="outer")
            .fillna(0)
            .groupby(["zona","fecha","hora"])
            .agg(vehiculos_entrada=("v_entrada","mean"), vehiculos_salida=("v_salida","mean"))
            .round(1).reset_index()
        )
    else:
        g_camaras = pd.DataFrame()
        g_flujo   = pd.DataFrame()

    # Eventos
    df_eventos = silver.get("eventos", pd.DataFrame())
    if not df_eventos.empty:
        ev_todas = df_eventos[df_eventos["zona_afectada"] == "todas"]
        ev_zonas = df_eventos[df_eventos["zona_afectada"] != "todas"]
        if not ev_todas.empty:
            expandidas = pd.concat([ev_todas.assign(zona_afectada=z) for z in ZONAS], ignore_index=True)
            df_eventos = pd.concat([ev_zonas, expandidas], ignore_index=True)
        g_eventos = (df_eventos
            .sort_values("impacto_factor", ascending=False)
            .groupby(["zona_afectada","fecha"]).first().reset_index()
            [["zona_afectada","fecha","impacto_factor","tipo","severidad"]]
            .rename(columns={"zona_afectada":"zona","impacto_factor":"impacto_factor_evento","tipo":"tipo_evento"})
        )
        g_eventos["tiene_evento"] = True
        g_eventos["es_feriado"]   = g_eventos["tipo_evento"].isin(["feriado_oficial","feriado","feriado_especial"])
    else:
        g_eventos = pd.DataFrame()

    # Noticias
    df_noticias = agregar_tiempo(silver.get("redes_noticias", pd.DataFrame()))
    g_noticias = (df_noticias.groupby(["fecha","hora"])
        .agg(noticias_trafico_cnt=("es_trafico","sum"),
             sentimiento_negativo_cnt=("sentiment", lambda x: (x=="negativo").sum()),
             relevance_score_max=("relevance_score","max"))
        .reset_index()
    ) if not df_noticias.empty else pd.DataFrame()

    # Join
    if g_clima.empty:
        log("  [!] Sin datos clima — gold real vacío")
        return pd.DataFrame()

    gold = g_clima.copy()
    for df_right, keys in [
        (g_sensores, ["zona","fecha","hora"]),
        (g_gps,      ["zona","fecha","hora"]),
        (g_camaras,  ["zona","fecha","hora"]),
        (g_flujo,    ["zona","fecha","hora"]),
        (g_eventos,  ["zona","fecha"]),
        (g_noticias, ["fecha","hora"]),
    ]:
        if not df_right.empty:
            gold = gold.merge(df_right, on=keys, how="left")

    gold["tiene_evento"]             = gold.get("tiene_evento",             pd.Series(False, index=gold.index)).fillna(False).astype(int)
    gold["es_feriado"]               = gold.get("es_feriado",               pd.Series(False, index=gold.index)).fillna(False).astype(int)
    gold["impacto_factor_evento"]    = gold.get("impacto_factor_evento",    pd.Series(1.0,   index=gold.index)).fillna(1.0)
    gold["tipo_evento"]              = gold.get("tipo_evento",              pd.Series("ninguno", index=gold.index)).fillna("ninguno")
    gold["severidad"]                = gold.get("severidad",                pd.Series("ninguna", index=gold.index)).fillna("ninguna")
    gold["noticias_trafico_cnt"]     = gold.get("noticias_trafico_cnt",     pd.Series(0,     index=gold.index)).fillna(0).astype(int)
    gold["sentimiento_negativo_cnt"] = gold.get("sentimiento_negativo_cnt", pd.Series(0,     index=gold.index)).fillna(0).astype(int)
    gold["relevance_score_max"]      = gold.get("relevance_score_max",      pd.Series(0.0,   index=gold.index)).fillna(0.0)
    gold["vehiculos_entrada"]        = gold.get("vehiculos_entrada",        pd.Series(0.0,   index=gold.index)).fillna(0.0)
    gold["vehiculos_salida"]         = gold.get("vehiculos_salida",         pd.Series(0.0,   index=gold.index)).fillna(0.0)

    # Feature engineering
    gold["hora_sin"] = np.sin(2 * np.pi * gold["hora"] / 24).round(4)
    gold["hora_cos"] = np.cos(2 * np.pi * gold["hora"] / 24).round(4)
    gold["es_hora_punta"]    = gold["hora"].isin([7,8,9,17,18,19]).astype(int)
    gold["es_fin_de_semana"] = (gold["dia_semana"] >= 5).astype(int)
    gold["periodo_dia"] = pd.cut(gold["hora"], bins=[-1,5,11,16,20,23],
        labels=["madrugada","manana","tarde","noche_temprana","noche"]).astype(str)
    gold["lluvia_flag"]    = (gold.get("precipitacion_mm", 0) > 0.5).astype(int)
    gold["lluvia_intensa"] = (gold.get("precipitacion_mm", 0) > 5.0).astype(int)

    sensor_norm = gold.get("congestion_ratio",   pd.Series(0.0, index=gold.index)).fillna(0.0) + 1.0
    cam_max     = gold.get("total_vehiculos_zona", pd.Series(0.0, index=gold.index)).max()
    cam_norm    = (gold.get("total_vehiculos_zona", pd.Series(0.0, index=gold.index)).fillna(0) / cam_max + 1.0) if cam_max > 0 else 1.0
    gold["indice_congestion"] = (
        gold.get("congestion_factor", pd.Series(1.0, index=gold.index)).fillna(1.0) * 0.60 +
        sensor_norm * 0.30 + cam_norm * 0.10
    ).round(3)

    VEL_LIBRE = {"Via Expresa - Centro":80,"Javier Prado - San Isidro":60,
                 "Panamericana Norte - Independencia":80,"Carretera Central - Ate":80,
                 "Av. Brasil - Magdalena":40,"Costa Verde - Miraflores":70}
    gold["vel_libre_ref"]      = gold["zona"].map(VEL_LIBRE)
    gold["velocidad_relativa"] = (gold.get("velocidad_kmh", pd.Series(0.0, index=gold.index)) / gold["vel_libre_ref"]).round(3).clip(0, 1.5)
    gold = gold.drop(columns=["vel_libre_ref"])
    gold["presion_evento"]     = (gold["impacto_factor_evento"] - 1.0).round(3)
    gold["ratio_flujo_camara"] = (
        gold["vehiculos_entrada"] / gold["vehiculos_salida"].replace(0, 1)
    ).clip(0.3, 3.0).round(3)

    # Lag features
    gold = gold.sort_values(["zona","fecha","hora"]).reset_index(drop=True)
    gold["congestion_factor_lag1h"] = gold.groupby("zona")["congestion_factor"].shift(1)
    gold["indice_congestion_lag1h"] = gold.groupby("zona")["indice_congestion"].shift(1)
    gold["tendencia_congestion"]    = (gold["indice_congestion"] - gold["indice_congestion_lag1h"]).round(3)
    gold["congestion_factor_lag1h"] = gold["congestion_factor_lag1h"].fillna(gold.get("congestion_factor", 1.0))
    gold["indice_congestion_lag1h"] = gold["indice_congestion_lag1h"].fillna(gold["indice_congestion"])
    gold["tendencia_congestion"]    = gold["tendencia_congestion"].fillna(0.0)

    # Target
    def clasificar(row):
        cf  = row.get("congestion_factor")
        idx = row.get("indice_congestion", 1.0)
        if pd.notna(cf):
            return "bajo" if cf < 1.25 else "medio" if cf < 1.70 else "alto"
        return "bajo" if idx < 1.25 else "medio" if idx < 1.60 else "alto"

    gold["nivel_congestion"] = gold.apply(clasificar, axis=1)

    log(f"  gold real generado: {len(gold):,} filas x {len(gold.columns)} columnas")
    return gold


# ============================================================
# PASO 4 — MERGE con gold histórico de ADLS
# ============================================================

def merge_con_historico(gold_real):
    """Descarga gold histórico de ADLS, concatena y deduplica."""
    log("Descargando gold histórico desde ADLS...")
    try:
        ruta = f"abfs://{ADLS_CONTAINER}/gold/congestion_por_zona/datos.parquet"
        gold_hist = pd.read_parquet(ruta, storage_options=SO)
        log(f"  Histórico: {len(gold_hist):,} filas")
    except Exception as e:
        log(f"  [!] No se pudo leer gold histórico: {e}")
        log("  Se usará solo el gold real como gold combinado")
        return gold_real

    # Asegurar mismas columnas (el histórico puede tener algunas que no tiene el real y viceversa)
    cols_comunes = list(set(gold_hist.columns) & set(gold_real.columns))
    gold_hist  = gold_hist[cols_comunes]
    gold_real  = gold_real[[c for c in cols_comunes if c in gold_real.columns]]

    gold_merged = pd.concat([gold_hist, gold_real], ignore_index=True)

    # Dedup: si un registro real coincide con uno histórico en zona+fecha+hora, el real gana
    gold_merged = (gold_merged
        .sort_values("_ingest_ts" if "_ingest_ts" in gold_merged.columns else "fecha",
                     ascending=False)   # real > histórico (más reciente primero)
        .drop_duplicates(subset=["zona","fecha","hora"], keep="first")
        .sort_values(["zona","fecha","hora"])
        .reset_index(drop=True)
    )

    n_nuevos = len(gold_real)
    log(f"  Merge: {len(gold_hist):,} histórico + {n_nuevos} real = {len(gold_merged):,} total ({n_nuevos} nuevos/actualizados)")
    return gold_merged


# ============================================================
# PASO 4b — PREDICCION ML sobre gold real
# ============================================================

def predict_with_ml(gold_real_pd):
    """Carga prep_pipeline + mlp_classifier y predice nivel_congestion."""
    import json, tempfile, pathlib

    MODEL_DIR = ROOT / "05_ml_models" / "saved_model"
    if not (MODEL_DIR / "metadata.json").exists():
        log("  [ML] Modelo no encontrado — usando clasificacion por reglas")
        return gold_real_pd

    try:
        import os
        os.environ["PYSPARK_PYTHON"]        = sys.executable
        os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
        _hadoop = os.environ.get("HADOOP_HOME")
        if _hadoop:
            os.environ["PATH"] = os.path.join(_hadoop, "bin") + os.pathsep + os.environ.get("PATH", "")

        from pyspark.sql import SparkSession
        from pyspark.ml import PipelineModel
        from pyspark.ml.classification import MultilayerPerceptronClassificationModel
        from pyspark.sql.functions import when, col as scol

        spark_inf = (SparkSession.builder
            .appName("trafico_inferencia")
            .master("local[1]")
            .config("spark.driver.memory", "4g")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.python.worker.reuse", "true")
            .getOrCreate())

        with open(MODEL_DIR / "metadata.json") as f:
            meta = json.load(f)

        labels_target = meta["labels_target"]
        # Columnas a quitar: leakage + target (es lo que vamos a predecir)
        excluir_inf = set(meta["excluir"]) | {meta["target"]}

        # Preparar pandas DF para Spark: quitar leakage y target previo
        df_inf = gold_real_pd.drop(
            columns=[c for c in excluir_inf if c in gold_real_pd.columns],
            errors="ignore"
        )

        # Agregar _row_id para rastrear qué filas sobreviven VectorAssembler(handleInvalid=skip)
        df_inf = df_inf.reset_index(drop=True)
        df_inf["_row_id"] = range(len(df_inf))

        # Guardar a parquet local y leer con Spark
        tmp = pathlib.Path(tempfile.gettempdir()) / "pyspark_trafico"
        tmp.mkdir(exist_ok=True)
        inf_path = str(tmp / "gold_real_inf.parquet")
        df_inf.to_parquet(inf_path, index=False)

        df_spark = spark_inf.read.parquet(inf_path)

        # Cargar modelos via Java (evita sc.textFile() -> Python workers -> WinError 10038)
        def _to_uri(p):
            return "file:///" + str(p).replace("\\", "/")

        jvm = spark_inf._jvm
        java_prep = jvm.org.apache.spark.ml.PipelineModel.load(_to_uri(MODEL_DIR / "prep_pipeline"))
        prep_model = PipelineModel._from_java(java_prep)

        java_mlp = jvm.org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel.load(
            _to_uri(MODEL_DIR / "mlp_classifier")
        )
        mlp_model = MultilayerPerceptronClassificationModel._from_java(java_mlp)

        # Inferencia
        df_prep = prep_model.transform(df_spark)
        df_pred = mlp_model.transform(df_prep)

        # Mapear indice -> label string (sin Python UDFs)
        expr = when(scol("prediction") == 0.0, labels_target[0])
        for i, lbl in enumerate(labels_target[1:], start=1):
            expr = expr.when(scol("prediction") == float(i), lbl)
        expr = expr.otherwise(labels_target[0])

        # Extraer _row_id + nivel_congestion; _row_id identifica qué filas sobrevivieron
        df_result = (df_pred
            .withColumn("nivel_congestion", expr)
            .select("_row_id", "nivel_congestion")
            .toPandas()
        )

        spark_inf.stop()

        # Merge por _row_id (left join -> filas descartadas por nulls quedan con NaN)
        gold_out = gold_real_pd.copy().reset_index(drop=True)
        gold_out["_row_id"] = range(len(gold_out))
        gold_out = gold_out.drop(columns=["nivel_congestion"], errors="ignore")
        gold_out = gold_out.merge(df_result, on="_row_id", how="left")

        # Fallback reglas solo para filas que el VectorAssembler descartó (con nulls en features)
        mask = gold_out["nivel_congestion"].isna()
        if mask.any():
            def _clasificar(row):
                cf  = row.get("congestion_factor")
                idx = row.get("indice_congestion", 1.0)
                if pd.notna(cf):
                    return "bajo" if cf < 1.25 else "medio" if cf < 1.70 else "alto"
                return "bajo" if idx < 1.25 else "medio" if idx < 1.60 else "alto"
            gold_out.loc[mask, "nivel_congestion"] = gold_out[mask].apply(_clasificar, axis=1)

        gold_out = gold_out.drop(columns=["_row_id"])

        n_ml    = int((~mask).sum())
        n_rules = int(mask.sum())
        log(f"  [ML] {n_ml}/{len(gold_out)} MLP (F1=0.8915) | {n_rules} fallback reglas")
        return gold_out

    except Exception:
        log(f"  [ML] Error en inferencia, fallback a reglas:\n{traceback.format_exc()}")
        return gold_real_pd


# ============================================================
# PASO 5 — SUBIR GOLD COMBINADO A ADLS
# ============================================================

def subir_gold(gold_merged):
    ruta = f"abfs://{ADLS_CONTAINER}/gold/congestion_por_zona/datos.parquet"
    gold_merged.to_parquet(ruta, storage_options=SO, index=False)
    log(f"  Gold combinado subido: {len(gold_merged):,} filas -> ADLS")


# ============================================================
# PASO 6 — EXPORTAR CSVS DE POWER BI
# ============================================================

def exportar_powerbi():
    script = ROOT / "06_visualizacion" / "export_powerbi.py"
    result = subprocess.run([sys.executable, str(script)], capture_output=True, text=True, timeout=120)
    if result.returncode == 0:
        log("  Power BI CSVs actualizados")
    else:
        log(f"  [!] export_powerbi.py warning: {result.stderr.strip()[:300]}")


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-upload", action="store_true",
                        help="No subir gold a ADLS (solo procesamiento local)")
    args = parser.parse_args()

    sep = "=" * 60
    log(sep)
    log("  PIPELINE Bronze -> Silver -> Gold -> Power BI")
    log(sep)

    # 1. Bronze desde ADLS (últimas 24h)
    log("\n[1/6] Leyendo bronze desde ADLS (últimas 24h)...")
    fuentes = ["clima","sensores_trafico","gps_rutas","vision_camaras","eventos","redes_noticias"]
    bronces = {}
    for f in fuentes:
        df = leer_bronze_adls(f)
        if not df.empty:
            bronces[f] = df

    if not bronces:
        log("  ERROR: No hay datos bronze en ADLS. Corre el scheduler primero.")
        sys.exit(1)

    total_bronze = sum(len(v) for v in bronces.values())
    log(f"  Total bronze: {total_bronze:,} registros de {len(bronces)}/6 fuentes")

    # 2. Silver
    log("\n[2/6] Procesando silver...")
    silver = procesar_silver(bronces)
    log(f"  Silver OK: {len(silver)} fuentes procesadas")

    # 3. Gold real
    log("\n[3/6] Generando gold (datos reales)...")
    gold_real = procesar_gold(silver)

    if gold_real.empty:
        log("  Sin datos suficientes para generar gold real. Exportando con gold historico existente.")
        log("\n[4/6] Saltando merge (sin gold real)...")
        log("\n[5/6] Saltando upload (sin gold real)...")
    else:
        # 3b. Prediccion ML
        log("\n[3b] Prediciendo nivel_congestion con modelo MLP...")
        gold_real = predict_with_ml(gold_real)

        # 4. Merge con histórico
        log("\n[4/6] Mergeando con gold historico de ADLS...")
        gold_merged = merge_con_historico(gold_real)

        # 5. Subir a ADLS
        if args.skip_upload:
            log("\n[5/6] --skip-upload activo, no se sube a ADLS")
        else:
            log("\n[5/6] Subiendo gold combinado a ADLS...")
            try:
                subir_gold(gold_merged)
            except Exception:
                log(f"  [!] Error al subir:\n{traceback.format_exc()}")

    # 6. Power BI
    log("\n[6/6] Exportando KPIs para Power BI...")
    try:
        exportar_powerbi()
    except Exception:
        log(f"  [!] Error en export_powerbi:\n{traceback.format_exc()}")

    log(f"\n{sep}")
    log("  PIPELINE COMPLETADO")
    log(sep)


if __name__ == "__main__":
    main()
