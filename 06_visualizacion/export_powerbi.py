"""
Exporta KPIs de tráfico desde la capa Gold (ADLS) directamente a ADLS powerbi/.
Power BI Desktop conecta a ADLS — sin archivos locales.

Salida (ADLS trafico-lima/powerbi/):
    kpi_resumen.csv           — tarjetas globales
    kpi_resumen_por_zona.csv  — tarjetas filtradas por zona
    kpi_por_zona.csv          — mapa/barras por zona
    kpi_serie_temporal.csv    — evolución horaria
    kpi_composicion.csv       — composición vehicular
    kpi_ml_resultados.csv     — tabla comparativa de los 9 modelos ML
"""

import sys, io, os, warnings
warnings.filterwarnings("ignore")
from pathlib import Path
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

# --------------------------------------------------------------------------
# ADLS credentials — leídas del .env
# --------------------------------------------------------------------------
ADLS_ACCOUNT   = os.getenv("ADLS_ACCOUNT", "traficolima")
ADLS_KEY       = os.getenv("ADLS_KEY", "")
ADLS_CONTAINER = os.getenv("ADLS_CONTAINER", "trafico-lima")
SO = {"account_name": ADLS_ACCOUNT, "account_key": ADLS_KEY}

# --------------------------------------------------------------------------
# Leer Gold desde ADLS
# --------------------------------------------------------------------------
import pandas as pd
import numpy as np
import adlfs

_fs = adlfs.AzureBlobFileSystem(account_name=ADLS_ACCOUNT, account_key=ADLS_KEY)

def subir_csv_adls(df, nombre):
    """Sube un DataFrame como CSV directamente a ADLS sin tocar disco local."""
    buf = io.StringIO()
    df.to_csv(buf, index=False, encoding="utf-8")
    destino = f"{ADLS_CONTAINER}/powerbi/{nombre}"
    with _fs.open(destino, "w", encoding="utf-8") as f:
        f.write(buf.getvalue())
    print(f"  -> ADLS: powerbi/{nombre} ({len(df)} filas)")

print("Leyendo capa Gold desde ADLS...")
try:
    ruta_gold = f"abfs://{ADLS_CONTAINER}/gold/congestion_por_zona/datos.parquet"
    df = pd.read_parquet(ruta_gold, storage_options=SO)
    print(f"  Gold cargado: {len(df):,} filas x {df.shape[1]} columnas")
except Exception as e:
    print(f"  ERROR: No se pudo conectar a ADLS: {e}")
    sys.exit(1)

# Columna timestamp para serie temporal
if "fecha" in df.columns:
    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
else:
    # Crear fecha sintética si no existe (gold histórico generado con generate_historical_gold.py)
    df["fecha"] = pd.Timestamp("2026-06-28")

# Zonas reales de Lima para enriquecer el mapa
ZONAS_COORDS = {
    "Via Expresa - Centro":               {"lat": -12.071, "lon": -77.033},
    "Javier Prado - San Isidro":          {"lat": -12.092, "lon": -77.022},
    "Panamericana Norte - Independencia": {"lat": -11.990, "lon": -77.060},
    "Carretera Central - Ate":            {"lat": -12.030, "lon": -76.920},
    "Av. Brasil - Magdalena":             {"lat": -12.090, "lon": -77.060},
    "Costa Verde - Miraflores":           {"lat": -12.130, "lon": -77.030},
}

# ============================================================
# 1. KPI RESUMEN — tarjetas de cabecera del dashboard
# ============================================================
print("Calculando KPI resumen...")

velocidad_media = df["velocidad_kmh"].mean()
congestion_media = df["congestion_ratio"].mean()
pct_alto  = (df["nivel_congestion"] == "alto").mean()  * 100
pct_medio = (df["nivel_congestion"] == "medio").mean() * 100
pct_bajo  = (df["nivel_congestion"] == "bajo").mean()  * 100

# Zona más congestionada actualmente (último registro por zona)
if "zona" in df.columns:
    ultima_lectura = df.sort_values("fecha", ascending=False).drop_duplicates("zona")
    zona_critica   = ultima_lectura.loc[ultima_lectura["congestion_ratio"].idxmax(), "zona"]
    velocidad_min  = ultima_lectura["velocidad_kmh"].min()
else:
    zona_critica  = "N/D"
    velocidad_min = velocidad_media

eventos_activos = int(df["tiene_evento"].sum()) if "tiene_evento" in df.columns else 0

kpi_resumen = pd.DataFrame([{
    "KPI":   "Velocidad promedio (km/h)",       "Valor": round(velocidad_media, 1), "Unidad": "km/h"
},{
    "KPI":   "Congestión global (ratio)",        "Valor": round(congestion_media, 3), "Unidad": "ratio 0-1"
},{
    "KPI":   "% Nivel ALTO",                     "Valor": round(pct_alto, 1),  "Unidad": "%"
},{
    "KPI":   "% Nivel MEDIO",                    "Valor": round(pct_medio, 1), "Unidad": "%"
},{
    "KPI":   "% Nivel BAJO",                     "Valor": round(pct_bajo, 1),  "Unidad": "%"
},{
    "KPI":   "Zona más congestionada",           "Valor": zona_critica,        "Unidad": "texto"
},{
    "KPI":   "Velocidad mínima zona crítica (km/h)", "Valor": round(velocidad_min, 1), "Unidad": "km/h"
},{
    "KPI":   "Registros con evento activo",      "Valor": eventos_activos,     "Unidad": "registros"
}])
subir_csv_adls(kpi_resumen, "kpi_resumen.csv")

# ============================================================
# 2. KPI POR ZONA — barras + mapa
# ============================================================
print("Calculando KPI por zona...")

if "zona" in df.columns:
    por_zona = df.groupby("zona").agg(
        velocidad_kmh      = ("velocidad_kmh",    "mean"),
        congestion_ratio   = ("congestion_ratio", "mean"),
        intensidad_veh_hora= ("intensidad_veh_hora","mean"),
        total_vehiculos    = ("total_vehiculos_zona","mean"),
        pct_alto           = ("nivel_congestion", lambda x: (x == "alto").mean()  * 100),
        pct_medio          = ("nivel_congestion", lambda x: (x == "medio").mean() * 100),
        pct_bajo           = ("nivel_congestion", lambda x: (x == "bajo").mean()  * 100),
        registros          = ("velocidad_kmh",    "count"),
    ).round(2).reset_index()

    # Añadir coordenadas para mapa de Power BI
    por_zona["lat"] = por_zona["zona"].map(lambda z: ZONAS_COORDS.get(z, {}).get("lat", None))
    por_zona["lon"] = por_zona["zona"].map(lambda z: ZONAS_COORDS.get(z, {}).get("lon", None))

    # Etiqueta de nivel dominante por zona
    por_zona["nivel_dominante"] = por_zona[["pct_alto","pct_medio","pct_bajo"]].idxmax(axis=1)\
        .str.replace("pct_", "")
else:
    por_zona = pd.DataFrame({"zona": ["Sin datos zona"], "congestion_ratio": [congestion_media]})

subir_csv_adls(por_zona, "kpi_por_zona.csv")

# kpi_resumen_por_zona: mismas metricas de resumen pero con columna zona
# Permite que los KPI cards en Power BI se filtren por el slicer de zona
if "zona" in df.columns:
    ultima = df.sort_values("fecha", ascending=False).drop_duplicates("zona")
    filas_rpz = []
    for _, row in por_zona.iterrows():
        z = row["zona"]
        zona_ultima = ultima[ultima["zona"] == z]
        vel_min_z   = zona_ultima["velocidad_kmh"].min() if not zona_ultima.empty else row["velocidad_kmh"]
        filas_rpz.extend([
            {"zona": z, "KPI": "Velocidad promedio (km/h)",    "Valor": round(row["velocidad_kmh"], 1),    "Unidad": "km/h"},
            {"zona": z, "KPI": "Congestión global (ratio)",    "Valor": round(row["congestion_ratio"], 3), "Unidad": "ratio"},
            {"zona": z, "KPI": "% Nivel ALTO",                 "Valor": round(row["pct_alto"], 1),         "Unidad": "%"},
            {"zona": z, "KPI": "% Nivel MEDIO",                "Valor": round(row["pct_medio"], 1),        "Unidad": "%"},
            {"zona": z, "KPI": "% Nivel BAJO",                 "Valor": round(row["pct_bajo"], 1),         "Unidad": "%"},
            {"zona": z, "KPI": "Nivel dominante",              "Valor": row["nivel_dominante"],             "Unidad": "texto"},
            {"zona": z, "KPI": "Velocidad mínima (km/h)",      "Valor": round(vel_min_z, 1),               "Unidad": "km/h"},
            {"zona": z, "KPI": "Total vehiculos promedio",     "Valor": round(row["total_vehiculos"], 0),  "Unidad": "veh"},
        ])
    kpi_resumen_por_zona = pd.DataFrame(filas_rpz)
    subir_csv_adls(kpi_resumen_por_zona, "kpi_resumen_por_zona.csv")

# ============================================================
# 3. SERIE TEMPORAL — evolución horaria
# ============================================================
print("Calculando serie temporal...")

if "hora" in df.columns:
    serie = df.groupby("hora").agg(
        velocidad_kmh      = ("velocidad_kmh",     "mean"),
        congestion_ratio   = ("congestion_ratio",  "mean"),
        intensidad_veh_hora= ("intensidad_veh_hora","mean"),
        pct_alto           = ("nivel_congestion",  lambda x: (x == "alto").mean() * 100),
    ).round(3).reset_index()
    serie["hora_label"] = serie["hora"].apply(lambda h: f"{h:02d}:00")

    # Identificar horas punta
    serie["es_hora_punta"] = serie["hora"].isin([7, 8, 9, 17, 18, 19]).astype(int)
elif "fecha" in df.columns and df["fecha"].notna().any():
    df["hora"] = df["fecha"].dt.hour
    serie = df.groupby("hora").agg(
        velocidad_kmh    = ("velocidad_kmh",    "mean"),
        congestion_ratio = ("congestion_ratio", "mean"),
    ).round(3).reset_index()
    serie["hora_label"]   = serie["hora"].apply(lambda h: f"{h:02d}:00")
    serie["es_hora_punta"] = serie["hora"].isin([7, 8, 9, 17, 18, 19]).astype(int)
else:
    serie = pd.DataFrame({"hora": range(24), "velocidad_kmh": [velocidad_media]*24,
                          "congestion_ratio": [congestion_media]*24})
    serie["hora_label"]   = serie["hora"].apply(lambda h: f"{h:02d}:00")
    serie["es_hora_punta"] = serie["hora"].isin([7, 8, 9, 17, 18, 19]).astype(int)

subir_csv_adls(serie, "kpi_serie_temporal.csv")

# ============================================================
# 4. COMPOSICIÓN VEHICULAR — dona
# ============================================================
print("Calculando composición vehicular...")

tipo_cols = {
    "Auto/Taxi":            "pct_auto_taxi",
    "Combi/Minibus":        "pct_combi_minibus",
    "Moto/Mototaxi":        "pct_moto_mototaxi",
    "Bus":                  "pct_bus",
    "Camioneta/LGV":        "pct_camioneta_lgv",
    "Camion/HGV":           "pct_camion_hgv",
}

filas_comp = []
for nombre, col_name in tipo_cols.items():
    if col_name in df.columns:
        pct_prom = df[col_name].mean() * 100
        filas_comp.append({"tipo_vehiculo": nombre, "porcentaje": round(pct_prom, 2)})

if filas_comp:
    composicion = pd.DataFrame(filas_comp)
    total = composicion["porcentaje"].sum()
    composicion["porcentaje_norm"] = (composicion["porcentaje"] / total * 100).round(2)
else:
    composicion = pd.DataFrame({
        "tipo_vehiculo": ["Auto/Taxi","Combi/Minibus","Bus","Otros"],
        "porcentaje": [40, 25, 20, 15],
        "porcentaje_norm": [40, 25, 20, 15],
    })

subir_csv_adls(composicion, "kpi_composicion.csv")

# ============================================================
# 5. RESULTADOS ML — tabla comparativa de los 9 modelos
# ============================================================
print("Generando tabla de resultados ML...")

ml_resultados = pd.DataFrame([
    {"Modelo": "MLP",                 "Tipo": "Multiclase nativo", "Accuracy": 0.8908, "F1_score": 0.8897, "Precision": 0.8895, "Recall": 0.8908},
    {"Modelo": "LogisticRegression",  "Tipo": "Multiclase nativo", "Accuracy": 0.8898, "F1_score": 0.8886, "Precision": 0.8882, "Recall": 0.8898},
    {"Modelo": "GBT (OvR manual)",    "Tipo": "OvR manual",        "Accuracy": 0.8873, "F1_score": 0.8865, "Precision": 0.8864, "Recall": 0.8873},
    {"Modelo": "OvR + LR binaria",    "Tipo": "OvR manual",        "Accuracy": 0.8838, "F1_score": 0.8780, "Precision": 0.8786, "Recall": 0.8838},
    {"Modelo": "DecisionTree",        "Tipo": "Multiclase nativo", "Accuracy": 0.8835, "F1_score": 0.8810, "Precision": 0.8830, "Recall": 0.8835},
    {"Modelo": "RandomForest",        "Tipo": "Multiclase nativo", "Accuracy": 0.8779, "F1_score": 0.8718, "Precision": 0.8735, "Recall": 0.8779},
    {"Modelo": "FMClassifier (OvR)",  "Tipo": "OvR manual",        "Accuracy": 0.8615, "F1_score": 0.8566, "Precision": 0.8565, "Recall": 0.8615},
    {"Modelo": "LinearSVC (OvR)",     "Tipo": "OvR manual",        "Accuracy": 0.8544, "F1_score": 0.8186, "Precision": 0.8550, "Recall": 0.8544},
    {"Modelo": "NaiveBayes",          "Tipo": "Multiclase nativo", "Accuracy": 0.8381, "F1_score": 0.8361, "Precision": 0.8431, "Recall": 0.8381},
])
ml_resultados["es_mejor"] = (ml_resultados["F1_score"] == ml_resultados["F1_score"].max()).astype(int)
subir_csv_adls(ml_resultados, "kpi_ml_resultados.csv")

# ============================================================
# Resumen final
# ============================================================
print()
print("=" * 65)
print("  EXPORTACION COMPLETADA — todo en ADLS powerbi/")
print(f"  https://{ADLS_ACCOUNT}.dfs.core.windows.net/{ADLS_CONTAINER}/powerbi/")
print("=" * 65)
