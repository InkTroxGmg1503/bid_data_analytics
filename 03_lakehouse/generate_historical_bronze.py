# Databricks notebook — Generación histórica de datos Bronze
# Genera datos sintéticos calibrados para 6 fuentes (2015-2024)
# Guarda archivos parquet mensuales en ADLS bronze/
# Total aproximado: ~4 M filas en 720 archivos (10 años × 12 meses × 6 fuentes)
#
# Después de ejecutar este script, corre silver_notebook.py y gold_notebook.py normalmente.

# COMMAND ----------

# %pip install adlfs fsspec pyarrow pandas numpy
# Ejecutar solo la primera vez

# COMMAND ----------

import pandas as pd
import numpy as np
import adlfs
from datetime import timezone

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

# ── CONSTANTES COMPARTIDAS ────────────────────────────────────────────────────

ZONAS_LIMA = [
    {"nombre": "Via Expresa - Centro",               "lat": -12.071, "lon": -77.033, "base": 0.65, "tipo": "via_expresa",      "vel_libre": 80},
    {"nombre": "Javier Prado - San Isidro",          "lat": -12.092, "lon": -77.022, "base": 0.55, "tipo": "arteria_principal","vel_libre": 60},
    {"nombre": "Panamericana Norte - Independencia", "lat": -11.990, "lon": -77.060, "base": 0.50, "tipo": "acceso_autopista", "vel_libre": 80},
    {"nombre": "Carretera Central - Ate",            "lat": -12.030, "lon": -76.920, "base": 0.58, "tipo": "acceso_autopista", "vel_libre": 80},
    {"nombre": "Av. Brasil - Magdalena",             "lat": -12.090, "lon": -77.060, "base": 0.45, "tipo": "avenida",          "vel_libre": 40},
    {"nombre": "Costa Verde - Miraflores",           "lat": -12.130, "lon": -77.030, "base": 0.40, "tipo": "via_periferica",   "vel_libre": 70},
]

FACTOR_HORA = np.array([
    0.08, 0.05, 0.04, 0.04, 0.06, 0.14,
    0.38, 0.82, 1.00, 0.87, 0.66, 0.60,
    0.70, 0.75, 0.65, 0.62, 0.72, 0.92,
    1.00, 0.92, 0.72, 0.52, 0.36, 0.20,
])
FACTOR_DIA  = np.array([1.15, 1.10, 1.10, 1.12, 1.25, 0.78, 0.48])
FACTOR_MES  = np.array([1.00, 1.00, 1.02, 1.00, 0.98, 1.10, 1.12, 1.12, 1.10, 1.08, 1.00, 1.00])

CAPACIDAD = {"via_expresa": 85, "arteria_principal": 60, "acceso_autopista": 75, "avenida": 35, "via_periferica": 90}
NIVEL_SERVICIO = ["A", "B", "C", "D", "E", "F"]

COMPOSICION = {"auto_taxi": 0.55, "combi_minibus": 0.20, "moto_mototaxi": 0.12,
               "bus": 0.05, "camioneta_lgv": 0.06, "camion_hgv": 0.02}

TIPOS_EVENTO   = ["ninguno"] * 14 + ["deportivo", "cultural", "accidente", "obras", "marcha", "feriado"]
IMPACTO_EVENTO = {"ninguno": 1.0, "deportivo": 1.35, "cultural": 1.15,
                  "accidente": 1.55, "obras": 1.40, "marcha": 1.50, "feriado": 0.85}
SEVER_EVENTO   = {"ninguno": "ninguna", "deportivo": "media", "cultural": "baja",
                  "accidente": "alta",   "obras": "media",    "marcha": "alta",  "feriado": "baja"}

SENTIMIENTOS = ["positivo", "neutro", "negativo"]

def congestion_base(hora, dia_sem, mes_idx, anio, base_zona):
    f = FACTOR_HORA[hora] * FACTOR_DIA[dia_sem] * FACTOR_MES[mes_idx]
    f *= 1.0 + 0.04 * (anio - ANIO_INICIO)   # crecimiento anual MTC
    return np.clip(f * base_zona, 0, 1)

# COMMAND ----------

# ── GENERADORES POR FUENTE ────────────────────────────────────────────────────

def gen_sensores(ts: pd.Timestamp, rng):
    h, d, m, y = ts.hour, ts.dayofweek, ts.month - 1, ts.year
    registros = []
    for z in ZONAS_LIMA:
        cr  = congestion_base(h, d, m, y, z["base"]) + rng.normal(0, 0.04)
        cr  = float(np.clip(cr, 0, 1))
        cap = CAPACIDAD[z["tipo"]] * 12   # veh/h
        ivh = float(np.clip(cap * cr + rng.normal(0, 15), 0, None))
        ns  = NIVEL_SERVICIO[int(np.digitize(cr, [0.30, 0.50, 0.65, 0.80, 0.95]))]
        registros.append({
            "zona":                z["nombre"],
            "lat":                 z["lat"],
            "lon":                 z["lon"],
            "hora_local":          h,
            "congestion_ratio":    round(cr, 4),
            "intensidad_veh_hora": round(ivh, 1),
            "capacidad_veh_hora":  cap,
            "ocupacion_pct":       round(cr * 100, 1),
            "carga_pct":           round(float(np.clip(ivh / max(cap, 1) * 100, 0, 150)), 1),
            "sensores_activos":    int(rng.integers(4, 7)),
            "nivel_servicio":      ns,
            "_calidad_fuente":     "proxy",
            "_ingest_ts":          ts.isoformat(),
        })
    return registros


def gen_gps(ts: pd.Timestamp, rng):
    h, d, m, y = ts.hour, ts.dayofweek, ts.month - 1, ts.year
    registros = []
    ruta_id = 0
    for z in ZONAS_LIMA:
        cr  = congestion_base(h, d, m, y, z["base"]) + rng.normal(0, 0.05)
        cr  = float(np.clip(cr, 0, 1))
        cf  = float(np.clip(1.0 + 4.0 * cr + rng.normal(0, 0.1), 1, 5))
        vel = float(np.clip(z["vel_libre"] * (1 - 0.9 * cr) + rng.normal(0, 3), 3, 130))
        dist = round(rng.uniform(3, 18), 2)
        dur  = float(np.clip((dist / max(vel, 1)) * 3600 + rng.normal(0, 30), 60, None))
        dist_m      = int(dist * 1000)
        dur_libre_s = int(dist_m / max(z["vel_libre"] / 3.6, 1))
        for _ in range(3):   # 3 rutas por zona por batch
            ruta_id += 1
            registros.append({
                "zona":               z["nombre"],
                "ruta_id":            f"R{ruta_id:05d}",
                "origen":             z["nombre"],
                "destino":            f"Destino_{ruta_id % 6 + 1}",
                "velocidad_kmh":      round(vel + rng.normal(0, 2), 2),
                "congestion_factor":  round(cf, 4),
                "distancia_m":        dist_m,
                "duracion_libre_s":   dur_libre_s,
                "duracion_trafico_s": round(dur, 1),
                "_calidad_fuente":    "real",
                "_ingest_ts":         ts.isoformat(),
            })
    return registros


def gen_camaras(ts: pd.Timestamp, rng):
    h, d, m, y = ts.hour, ts.dayofweek, ts.month - 1, ts.year
    registros = []
    for z in ZONAS_LIMA:
        cr  = congestion_base(h, d, m, y, z["base"]) + rng.normal(0, 0.04)
        cr  = float(np.clip(cr, 0, 1))
        cap = CAPACIDAD[z["tipo"]]
        for pos in ["entrada", "salida"]:
            ruido  = float(rng.uniform(0.96, 1.04))
            total  = max(0, round(cap * FACTOR_HORA[h] * ruido))
            restante = total
            cnts = {}
            tipos = list(COMPOSICION.items())
            for tipo, prop in tipos[:-1]:
                cnts[f"cnt_{tipo}"] = round(total * prop)
                restante -= cnts[f"cnt_{tipo}"]
            cnts[f"cnt_{tipos[-1][0]}"] = max(0, restante)
            registros.append({
                "zona":              z["nombre"],
                "camara_id":         f"CAM_{z['nombre'][:6].replace(' ','').upper()}_{1 if pos=='entrada' else 2:02d}",
                "posicion":          pos,
                "lat":               z["lat"],
                "lon":               z["lon"],
                "hora_local":        h,
                "tipo_corredor":     z["tipo"],
                "total_detectado":   total,
                "tasa_deteccion_pct":round(ruido * 100, 1),
                "ventana_min":       5,
                **cnts,
                "_calidad_fuente":   "calibrado",
                "_ingest_ts":        ts.isoformat(),
            })
    return registros


def gen_clima(ts: pd.Timestamp, rng):
    h, m = ts.hour, ts.month - 1
    dia_anio = ts.dayofyear
    registros = []
    for z in ZONAS_LIMA:
        temp   = float(np.clip(20 + 3 * np.sin(2 * np.pi * (dia_anio - 45) / 365) + rng.normal(0, 1.2), -5, 45))
        hum    = float(np.clip(70 + 15 * FACTOR_MES[m] + rng.normal(0, 5), 0, 100))
        p_lluv = 0.08 if 5 <= m <= 9 else 0.02
        llueve = rng.random() < p_lluv
        prec   = float(np.clip(rng.exponential(2.0), 0, 50)) if llueve else 0.0
        viento = float(np.clip(12 + rng.normal(0, 3), 0, 60))
        codigo = 61 if llueve else (45 if 5 <= m <= 9 else 0)
        registros.append({
            "zona":             z["nombre"],
            "lat":              z["lat"],
            "lon":              z["lon"],
            "hora_local":       ts.strftime("%Y-%m-%dT%H:%M"),
            "temperatura_c":    round(temp, 2),
            "humedad_pct":      round(hum, 1),
            "precipitacion_mm": round(prec, 2),
            "viento_kmh":       round(viento, 1),
            "codigo_clima":     int(codigo),
            "_calidad_fuente":  "real",
            "_ingest_ts":       ts.isoformat(),
        })
    return registros


def gen_eventos(ts: pd.Timestamp, rng):
    # Un evento por zona por día (se llama solo una vez por día)
    fecha_str = ts.strftime("%Y-%m-%d")
    registros = []
    for z in ZONAS_LIMA:
        tipo = rng.choice(TIPOS_EVENTO)
        if tipo == "ninguno":
            continue
        registros.append({
            "zona_afectada":    z["nombre"],
            "fecha":            fecha_str,
            "nombre":           f"{tipo.capitalize()} en {z['nombre']}",
            "tipo":             tipo,
            "severidad":        SEVER_EVENTO[tipo],
            "impacto_factor":   round(IMPACTO_EVENTO[tipo] + rng.normal(0, 0.05), 3),
            "dias_para_evento": 0,
            "_calidad_fuente":  "real",
            "_ingest_ts":       ts.isoformat(),
        })
    return registros


def gen_noticias(ts: pd.Timestamp, rng):
    # ~5 noticias/tweets por hora globalmente
    registros = []
    n = rng.poisson(5)
    for _ in range(max(1, n)):
        zona = rng.choice([z["nombre"] for z in ZONAS_LIMA] + [None])
        es_trafico = bool(rng.random() < 0.6)
        sent = rng.choice(SENTIMIENTOS, p=[0.2, 0.5, 0.3] if es_trafico else [0.3, 0.5, 0.2])
        item_id = f"{ts.strftime('%Y%m%d%H%M')}_{_:03d}"
        registros.append({
            "item_id":         item_id,
            "titulo":          f"Reporte de {'tráfico' if es_trafico else 'ciudad'} Lima {ts.strftime('%H:%M')}",
            "zona":            zona if zona else "Lima",
            "es_trafico":      es_trafico,
            "sentiment":       sent,
            "relevance_score": round(float(rng.uniform(0.3, 1.0)) if es_trafico else float(rng.uniform(0.0, 0.4)), 3),
            "_calidad_fuente": "real",
            "_ingest_ts":      ts.isoformat(),
        })
    return registros

# COMMAND ----------

# ── GUARDAR EN ADLS ───────────────────────────────────────────────────────────

def guardar_bronze(df: pd.DataFrame, fuente: str, anio: int, mes: int):
    ruta = f"abfs://{ADLS_CONTAINER}/bronze/{fuente}/{anio}/{mes:02d}/datos.parquet"
    df.to_parquet(ruta, storage_options=SO, index=False)

# COMMAND ----------

# ── LOOP PRINCIPAL ────────────────────────────────────────────────────────────

FUENTES = {
    "sensores_trafico": (gen_sensores, "proxy",     False),  # (fn, calidad, solo_diario)
    "gps_rutas":        (gen_gps,      "real",      False),
    "vision_camaras":   (gen_camaras,  "calibrado", False),
    "clima":            (gen_clima,    "real",      False),
    "eventos":          (gen_eventos,  "real",      True),   # solo 1 vez por día
    "redes_noticias":   (gen_noticias, "real",      False),
}

total_filas = {f: 0 for f in FUENTES}

for anio in range(ANIO_INICIO, ANIO_FIN + 1):
    for mes in range(1, 13):
        # Timestamps horarios del mes
        inicio = pd.Timestamp(anio, mes, 1, tzinfo=timezone.utc)
        fin    = (inicio + pd.offsets.MonthEnd(1)).replace(hour=23)
        horas  = pd.date_range(inicio, fin, freq="h")

        buffers = {f: [] for f in FUENTES}

        for ts in horas:
            for fuente, (fn, _, solo_diario) in FUENTES.items():
                if solo_diario and ts.hour != 0:
                    continue
                buffers[fuente].extend(fn(ts, rng))

        for fuente, rows in buffers.items():
            if rows:
                df = pd.DataFrame(rows)
                guardar_bronze(df, fuente, anio, mes)
                total_filas[fuente] += len(df)

        print(f"  {anio}-{mes:02d} guardado")

    print(f"Año {anio} completado.")

print("\n=== RESUMEN ===")
for fuente, n in total_filas.items():
    print(f"  {fuente:25s}: {n:>10,} filas")
print(f"\nTotal: {sum(total_filas.values()):,} filas")
print("Ahora ejecuta silver_notebook.py → gold_notebook.py")
