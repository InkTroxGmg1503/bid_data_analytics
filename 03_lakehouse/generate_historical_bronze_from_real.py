"""
Genera datos históricos Bronze calibrados con patrones aprendidos de la data real.

Aprende de la data real en ADLS gold (últimos N días) y genera bronze histórico
para 2015-2024 con la misma estructura que el scheduler.py produce hoy.

Sube a ADLS bronze/{fuente}/fecha={fecha}/hora={hora}/ igual que _bronze.py.
Después corre silver_notebook.py y gold_notebook.py para procesar las capas.

Uso:
    python 03_lakehouse/generate_historical_bronze_from_real.py
    python 03_lakehouse/generate_historical_bronze_from_real.py --anio-inicio 2020 --anio-fin 2024
    python 03_lakehouse/generate_historical_bronze_from_real.py --dias 14   (mas dias para aprender)
"""

import sys
import argparse
import warnings
warnings.filterwarnings("ignore")
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from pathlib import Path
from datetime import datetime, timedelta, timezone

import os
import numpy as np
import pandas as pd
import adlfs
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

ADLS_ACCOUNT   = os.getenv("ADLS_ACCOUNT", "traficolima")
ADLS_KEY       = os.getenv("ADLS_KEY", "")
ADLS_CONTAINER = os.getenv("ADLS_CONTAINER", "trafico-lima")
SO = {"account_name": ADLS_ACCOUNT, "account_key": ADLS_KEY}

ZONAS_LIMA = [
    {"nombre": "Via Expresa - Centro",               "lat": -12.071, "lon": -77.033, "tipo": "via_expresa",       "vel_libre": 80, "capacidad": 85},
    {"nombre": "Javier Prado - San Isidro",          "lat": -12.092, "lon": -77.022, "tipo": "arteria_principal", "vel_libre": 60, "capacidad": 60},
    {"nombre": "Panamericana Norte - Independencia", "lat": -11.990, "lon": -77.060, "tipo": "acceso_autopista",  "vel_libre": 80, "capacidad": 75},
    {"nombre": "Carretera Central - Ate",            "lat": -12.030, "lon": -76.920, "tipo": "acceso_autopista",  "vel_libre": 80, "capacidad": 75},
    {"nombre": "Av. Brasil - Magdalena",             "lat": -12.090, "lon": -77.060, "tipo": "avenida",           "vel_libre": 40, "capacidad": 35},
    {"nombre": "Costa Verde - Miraflores",           "lat": -12.130, "lon": -77.030, "tipo": "via_periferica",    "vel_libre": 70, "capacidad": 90},
]

NIVEL_SERVICIO = ["A", "B", "C", "D", "E", "F"]
COMPOSICION = {"auto_taxi": 0.55, "combi_minibus": 0.20, "moto_mototaxi": 0.12,
               "bus": 0.05, "camioneta_lgv": 0.06, "camion_hgv": 0.02}
TIPOS_EVENTO   = ["ninguno"] * 14 + ["deportivo", "cultural", "accidente", "obras", "marcha", "feriado"]
IMPACTO_EVENTO = {"ninguno": 1.0, "deportivo": 1.35, "cultural": 1.15,
                  "accidente": 1.55, "obras": 1.40, "marcha": 1.50, "feriado": 0.85}
SEVER_EVENTO   = {"ninguno": "ninguna", "deportivo": "media", "cultural": "baja",
                  "accidente": "alta",   "obras": "media",    "marcha": "alta",  "feriado": "baja"}
SENTIMIENTOS   = ["positivo", "neutro", "negativo"]
FACTOR_MES_LIMA = np.array([1.00, 1.00, 1.02, 1.00, 1.06, 1.10, 1.12, 1.12, 1.10, 1.08, 1.02, 1.00])
CRECIMIENTO_ANUAL = 0.035
AÑO_BASE = 2026

def log(msg):
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


# ============================================================
# FASE 0 — APRENDER PATRONES DE LA DATA REAL
# ============================================================

def aprender_patrones(dias=7):
    """Lee gold real de ADLS y extrae patrones por zona x hora."""
    log(f"Leyendo gold real desde ADLS (ultimos {dias} dias)...")
    ruta = f"abfs://{ADLS_CONTAINER}/gold/congestion_por_zona/datos.parquet"
    df_all = pd.read_parquet(ruta, storage_options=SO)

    df_all["fecha"] = pd.to_datetime(df_all["fecha"], errors="coerce")
    corte = pd.Timestamp.now().normalize() - pd.Timedelta(days=dias)
    df_real = df_all[df_all["fecha"] >= corte].copy()

    if len(df_real) < 100:
        log(f"  Pocos datos ({len(df_real)}), ampliando a {dias*4} dias")
        corte = pd.Timestamp.now().normalize() - pd.Timedelta(days=dias * 4)
        df_real = df_all[df_all["fecha"] >= corte].copy()

    if "hora" not in df_real.columns:
        df_real["hora"] = df_real["fecha"].dt.hour
    df_real["dia_semana"] = df_real["fecha"].dt.dayofweek

    log(f"  Data real: {len(df_real):,} filas en {df_real['fecha'].dt.date.nunique()} dias")

    def safe_mean(s): v = pd.to_numeric(s, errors="coerce").mean();  return float(v) if pd.notna(v) else 0.0
    def safe_std(s):  v = pd.to_numeric(s, errors="coerce").std();   return max(float(v) if pd.notna(v) else 0.01, 0.01)

    # Patron por zona x hora
    patron = {}
    for z in ZONAS_LIMA:
        zn = z["nombre"]
        patron[zn] = {}
        dz = df_real[df_real["zona"] == zn]
        for h in range(24):
            dzh = dz[dz["hora"] == h]
            if len(dzh) >= 2:
                patron[zn][h] = {
                    "cr_mean": safe_mean(dzh.get("congestion_ratio")),
                    "cr_std":  safe_std(dzh.get("congestion_ratio")),
                    "vel_mean": safe_mean(dzh.get("velocidad_kmh")),
                    "vel_std":  safe_std(dzh.get("velocidad_kmh")),
                    "cf_mean":  safe_mean(dzh.get("congestion_factor")),
                    "cf_std":   safe_std(dzh.get("congestion_factor")),
                    "ivh_mean": safe_mean(dzh.get("intensidad_veh_hora")),
                    "ivh_std":  safe_std(dzh.get("intensidad_veh_hora")),
                    "temp_mean":safe_mean(dzh.get("temperatura_c")),
                    "temp_std": safe_std(dzh.get("temperatura_c")),
                    "hum_mean": safe_mean(dzh.get("humedad_pct")),
                    "hum_std":  safe_std(dzh.get("humedad_pct")),
                    "prec_mean":safe_mean(dzh.get("precipitacion_mm")),
                    "vien_mean":safe_mean(dzh.get("viento_kmh")),
                    "vien_std": safe_std(dzh.get("viento_kmh")),
                }
            else:
                patron[zn][h] = None  # interpolar luego

        # Interpolar horas sin datos
        for h in range(24):
            if patron[zn][h] is None:
                vecinos = [patron[zn][(h + d) % 24] for d in [-2, -1, 1, 2] if patron[zn].get((h + d) % 24)]
                if vecinos:
                    keys = [k for k in vecinos[0]]
                    patron[zn][h] = {k: np.mean([v[k] for v in vecinos]) for k in keys}
                else:
                    patron[zn][h] = {"cr_mean": 0.5, "cr_std": 0.06, "vel_mean": 35.0, "vel_std": 5.0,
                                     "cf_mean": 2.0, "cf_std": 0.3, "ivh_mean": 400.0, "ivh_std": 50.0,
                                     "temp_mean": 20.0, "temp_std": 1.5, "hum_mean": 75.0, "hum_std": 6.0,
                                     "prec_mean": 0.1, "vien_mean": 12.0, "vien_std": 3.0}

    # Factor por dia de semana
    if "congestion_ratio" in df_real.columns:
        media_global = float(pd.to_numeric(df_real["congestion_ratio"], errors="coerce").mean()) or 0.5
        factor_dia = np.ones(7)
        for d in range(7):
            v = pd.to_numeric(df_real[df_real["dia_semana"] == d]["congestion_ratio"], errors="coerce").mean()
            factor_dia[d] = float(v) / media_global if pd.notna(v) and media_global > 0 else 1.0
    else:
        factor_dia = np.array([1.10, 1.10, 1.10, 1.12, 1.20, 0.80, 0.50])

    log(f"  Patrones aprendidos: {len(ZONAS_LIMA)} zonas x 24 horas")
    log(f"  Factor dia semana: {[round(f, 2) for f in factor_dia]}")
    return patron, factor_dia


# ============================================================
# GENERADORES POR FUENTE (misma estructura que scheduler)
# ============================================================

def gen_sensores(ts, z, p, fd, fa, fm, rng):
    h   = ts.hour
    cr  = float(np.clip(p["cr_mean"] * fd * fa * fm + rng.normal(0, p["cr_std"]), 0.01, 0.99))
    cap = z["capacidad"] * 12
    ivh = float(np.clip(p["ivh_mean"] * fd * fa * fm + rng.normal(0, p["ivh_std"]), 0, None))
    ns  = NIVEL_SERVICIO[int(np.digitize(cr, [0.30, 0.50, 0.65, 0.80, 0.95]))]
    return {
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
    }


def gen_gps(ts, z, p, fd, fa, fm, rng):
    vel = float(np.clip(p["vel_mean"] / max(fd * fa * fm, 0.1) + rng.normal(0, p["vel_std"]), 3, 130))
    cf  = float(np.clip(p["cf_mean"] * fd * fa * fm + rng.normal(0, p["cf_std"]), 1.0, 5.0))
    registros = []
    for i in range(3):
        dist   = round(rng.uniform(3, 18), 2)
        dist_m = int(dist * 1000)
        dur_libre = int(dist_m / max(z["vel_libre"] / 3.6, 1))
        dur_traf  = float(np.clip(dur_libre * cf + rng.normal(0, 30), 60, None))
        registros.append({
            "zona":               z["nombre"],
            "ruta_id":            f"R{abs(hash(ts.isoformat() + z['nombre'] + str(i))) % 99999:05d}",
            "origen":             z["nombre"],
            "destino":            f"Destino_{i + 1}",
            "velocidad_kmh":      round(vel + rng.normal(0, 2), 2),
            "congestion_factor":  round(cf, 4),
            "distancia_m":        dist_m,
            "duracion_libre_s":   dur_libre,
            "duracion_trafico_s": round(dur_traf, 1),
            "_calidad_fuente":    "real",
            "_ingest_ts":         ts.isoformat(),
        })
    return registros


def gen_camaras(ts, z, p, fd, fa, fm, rng):
    cap = z["capacidad"]
    cr  = float(np.clip(p["cr_mean"] * fd * fa * fm + rng.normal(0, p["cr_std"]), 0.01, 0.99))
    registros = []
    for pos in ["entrada", "salida"]:
        ruido = float(rng.uniform(0.96, 1.04))
        total = max(0, round(cap * cr * 12 * ruido))
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
            "hora_local":        ts.hour,
            "tipo_corredor":     z["tipo"],
            "total_detectado":   total,
            "tasa_deteccion_pct":round(ruido * 100, 1),
            "ventana_min":       5,
            **cnts,
            "_calidad_fuente":   "calibrado",
            "_ingest_ts":        ts.isoformat(),
        })
    return registros


def gen_clima(ts, z, p, rng):
    temp  = float(np.clip(p["temp_mean"] + rng.normal(0, p["temp_std"]), 10, 35))
    hum   = float(np.clip(p["hum_mean"]  + rng.normal(0, p["hum_std"]),  30, 100))
    prec  = float(np.clip(abs(p["prec_mean"] + rng.normal(0, 0.3)),      0, 40))
    vien  = float(np.clip(p["vien_mean"] + rng.normal(0, p["vien_std"]), 0, 60))
    llueve = prec > 0.5
    return {
        "zona":             z["nombre"],
        "lat":              z["lat"],
        "lon":              z["lon"],
        "hora_local":       ts.strftime("%Y-%m-%dT%H:%M"),
        "temperatura_c":    round(temp, 2),
        "humedad_pct":      round(hum, 1),
        "precipitacion_mm": round(prec, 2),
        "viento_kmh":       round(vien, 1),
        "codigo_clima":     int(61 if llueve else (45 if 4 <= ts.month <= 9 else 0)),
        "_calidad_fuente":  "real",
        "_ingest_ts":       ts.isoformat(),
    }


def gen_eventos(ts, z, rng):
    tipo = rng.choice(TIPOS_EVENTO)
    if tipo == "ninguno":
        return None
    return {
        "zona_afectada":  z["nombre"],
        "fecha":          ts.strftime("%Y-%m-%d"),
        "nombre":         f"{tipo.capitalize()} en {z['nombre']}",
        "tipo":           tipo,
        "severidad":      SEVER_EVENTO[tipo],
        "impacto_factor": round(float(IMPACTO_EVENTO[tipo] + rng.normal(0, 0.05)), 3),
        "dias_para_evento": 0,
        "_calidad_fuente":"real",
        "_ingest_ts":     ts.isoformat(),
    }


def gen_noticias(ts, rng):
    registros = []
    n = int(rng.poisson(5))
    for i in range(max(1, n)):
        zona_n = rng.choice([z["nombre"] for z in ZONAS_LIMA] + ["Lima"])
        es_tr  = bool(rng.random() < 0.6)
        sent   = rng.choice(SENTIMIENTOS, p=[0.2, 0.5, 0.3] if es_tr else [0.3, 0.5, 0.2])
        registros.append({
            "item_id":         f"{ts.strftime('%Y%m%d%H%M')}_{i:03d}",
            "titulo":          f"Reporte de {'trafico' if es_tr else 'ciudad'} Lima {ts.strftime('%H:%M')}",
            "zona":            zona_n,
            "es_trafico":      es_tr,
            "sentiment":       sent,
            "relevance_score": round(float(rng.uniform(0.3, 1.0) if es_tr else rng.uniform(0.0, 0.4)), 3),
            "_calidad_fuente": "real",
            "_ingest_ts":      ts.isoformat(),
        })
    return registros


# ============================================================
# GUARDAR EN ADLS
# ============================================================

def guardar_bronze_adls(df, fuente, anio, mes, hora=None):
    if hora is not None:
        fecha_str = f"{anio}-{mes:02d}-01"
        ruta = (f"abfs://{ADLS_CONTAINER}/bronze/{fuente}"
                f"/fecha={fecha_str}/hora={hora:02d}/datos_{anio}{mes:02d}.parquet")
    else:
        ruta = f"abfs://{ADLS_CONTAINER}/bronze/{fuente}/{anio}/{mes:02d}/datos.parquet"
    df.to_parquet(ruta, storage_options=SO, index=False)


# ============================================================
# LOOP PRINCIPAL
# ============================================================

def generar(anio_inicio, anio_fin, patron, factor_dia, rng):
    total = {f: 0 for f in ["sensores_trafico","gps_rutas","vision_camaras","clima","eventos","redes_noticias"]}

    for anio in range(anio_inicio, anio_fin + 1):
        años_diff = AÑO_BASE - anio
        fa = 1.0 / ((1 + CRECIMIENTO_ANUAL) ** años_diff)

        for mes in range(1, 13):
            fm = float(FACTOR_MES_LIMA[mes - 1])
            inicio = datetime(anio, mes, 1, tzinfo=timezone.utc)
            if mes == 12:
                fin = datetime(anio + 1, 1, 1, tzinfo=timezone.utc) - timedelta(hours=1)
            else:
                fin = datetime(anio, mes + 1, 1, tzinfo=timezone.utc) - timedelta(hours=1)

            buf = {f: [] for f in total}
            ts = inicio
            while ts <= fin:
                dia_sem = ts.weekday()
                fd = float(factor_dia[dia_sem])

                for z in ZONAS_LIMA:
                    p = patron[z["nombre"]][ts.hour]
                    buf["sensores_trafico"].append(gen_sensores(ts, z, p, fd, fa, fm, rng))
                    buf["gps_rutas"].extend(gen_gps(ts, z, p, fd, fa, fm, rng))
                    buf["vision_camaras"].extend(gen_camaras(ts, z, p, fd, fa, fm, rng))
                    buf["clima"].append(gen_clima(ts, z, p, rng))

                    # Eventos: solo a las 00:00
                    if ts.hour == 0:
                        ev = gen_eventos(ts, z, rng)
                        if ev:
                            buf["eventos"].append(ev)

                # Noticias: por hora global
                buf["redes_noticias"].extend(gen_noticias(ts, rng))
                ts += timedelta(hours=1)

            # Guardar cada fuente como un parquet mensual
            for fuente, rows in buf.items():
                if rows:
                    df = pd.DataFrame(rows)
                    guardar_bronze_adls(df, fuente, anio, mes)
                    total[fuente] += len(df)

            log(f"  {anio}-{mes:02d}: guardado ({sum(len(b) for b in buf.values()):,} filas)")

        log(f"Año {anio} completado.")

    log("\n=== RESUMEN TOTAL ===")
    for fuente, n in total.items():
        log(f"  {fuente:<25}: {n:>10,} filas")
    log(f"  TOTAL: {sum(total.values()):,} filas")


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--anio-inicio", type=int, default=2015)
    parser.add_argument("--anio-fin",    type=int, default=2024)
    parser.add_argument("--dias",        type=int, default=7,
                        help="Dias de data real para aprender patrones")
    parser.add_argument("--seed",        type=int, default=42)
    args = parser.parse_args()

    rng = np.random.default_rng(args.seed)

    sep = "=" * 60
    log(sep)
    log("  GENERADOR HISTÓRICO BRONZE — calibrado con data real")
    log(f"  Anos: {args.anio_inicio}-{args.anio_fin} | Dias reales: {args.dias}")
    log(sep)

    patron, factor_dia = aprender_patrones(dias=args.dias)

    log(f"\nGenerando bronze {args.anio_inicio}-{args.anio_fin}...")
    log("(Despues ejecuta silver_notebook.py y gold_notebook.py)")
    generar(args.anio_inicio, args.anio_fin, patron, factor_dia, rng)

    log(sep)
    log("  COMPLETADO — revisa bronze en ADLS y corre silver/gold")
    log(sep)


if __name__ == "__main__":
    main()
