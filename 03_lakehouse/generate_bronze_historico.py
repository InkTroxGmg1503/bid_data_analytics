"""
Generador de bronze histórico en 2 fases, calibrado desde la data real.

FASE 1: Lee bronze REAL de ADLS -> aprende patrones -> genera sintetico
        24-Apr-2026 al 27-Jun-2026 (el hueco entre el historico y la data real)

FASE 2: Lee bronze REAL + FASE 1 -> aprende patrones enriquecidos -> genera
        historico completo Ene-2010 -> Mar-2026

Estructura de salida (igual que scheduler.py / _bronze.py):
    bronze/{fuente}/fecha={YYYY-MM-DD}/hora={HH}/sintetico_{YYYYMMDDTHH}.parquet

Uso:
    python 03_lakehouse/generate_bronze_historico.py
    python 03_lakehouse/generate_bronze_historico.py --solo-fase 1
    python 03_lakehouse/generate_bronze_historico.py --solo-fase 2
"""

import sys, argparse, warnings
warnings.filterwarnings("ignore")
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from datetime import datetime, timedelta, date, timezone
from pathlib import Path
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
SO  = {"account_name": ADLS_ACCOUNT, "account_key": ADLS_KEY}

FUENTES = ["sensores_trafico", "gps_rutas", "vision_camaras", "clima", "eventos", "redes_noticias"]

ZONAS = [
    {"nombre": "Via Expresa - Centro",               "lat": -12.071, "lon": -77.033, "tipo": "via_expresa",       "vel_libre": 80, "capacidad": 85},
    {"nombre": "Javier Prado - San Isidro",          "lat": -12.092, "lon": -77.022, "tipo": "arteria_principal", "vel_libre": 60, "capacidad": 60},
    {"nombre": "Panamericana Norte - Independencia", "lat": -11.990, "lon": -77.060, "tipo": "acceso_autopista",  "vel_libre": 80, "capacidad": 75},
    {"nombre": "Carretera Central - Ate",            "lat": -12.030, "lon": -76.920, "tipo": "acceso_autopista",  "vel_libre": 80, "capacidad": 75},
    {"nombre": "Av. Brasil - Magdalena",             "lat": -12.090, "lon": -77.060, "tipo": "avenida",           "vel_libre": 40, "capacidad": 35},
    {"nombre": "Costa Verde - Miraflores",           "lat": -12.130, "lon": -77.030, "tipo": "via_periferica",    "vel_libre": 70, "capacidad": 90},
]
ZONA_NOMBRES = [z["nombre"] for z in ZONAS]
ZONA_META    = {z["nombre"]: z for z in ZONAS}

# Lima: garua mayo-octubre sube humedad y congestion
FACTOR_MES_LIMA = np.array([1.00, 1.00, 1.02, 1.00, 1.06, 1.10, 1.12, 1.12, 1.10, 1.08, 1.02, 1.00])
CRECIMIENTO_ANUAL = 0.035   # MTC: trafico Lima crece ~3.5% anual
AÑO_REF = 2026

TIPOS_EVENTO   = ["ninguno"]*14 + ["deportivo","cultural","accidente","obras","marcha","feriado"]
IMPACTO_EVENTO = {"ninguno":1.0,"deportivo":1.35,"cultural":1.15,
                  "accidente":1.55,"obras":1.40,"marcha":1.50,"feriado":0.85}
SEVER_EVENTO   = {"ninguno":"ninguna","deportivo":"media","cultural":"baja",
                  "accidente":"alta","obras":"media","marcha":"alta","feriado":"baja"}
SENTIMIENTOS   = ["positivo","neutro","negativo"]
NS_UMBRALES    = [0.30, 0.50, 0.65, 0.80, 0.95]
NS_LABELS      = ["A","B","C","D","E","F"]

def log(msg):
    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {msg}", flush=True)

def safe_stat(s, stat="mean", default=0.0):
    v = pd.to_numeric(s, errors="coerce")
    r = v.mean() if stat == "mean" else v.std()
    return float(r) if pd.notna(r) else default


# ============================================================
# LEER BRONZE REAL DESDE ADLS
# ============================================================

def leer_bronze_fuente(fs, fuente, max_dias=14):
    """Lee los ultimos max_dias dias de una fuente desde ADLS bronze/.
    Limita la lectura para evitar leer cientos de archivos historicos."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    hoy = date.today()
    fechas = [(hoy - timedelta(days=d)).strftime("%Y-%m-%d") for d in range(max_dias)]

    archivos = []
    for fecha_str in fechas:
        patron = f"{ADLS_CONTAINER}/bronze/{fuente}/fecha={fecha_str}/**/*.parquet"
        try:
            found = fs.glob(patron)
            archivos.extend(found)
        except Exception:
            pass

    if not archivos:
        log(f"  {fuente}: sin archivos en los ultimos {max_dias} dias")
        return pd.DataFrame()

    # Limitar a 100 archivos max para no saturar
    archivos = archivos[:100]

    def _leer(arch):
        try:
            with fs.open(arch, "rb") as f:
                return pd.read_parquet(f)
        except Exception:
            return None

    dfs = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        futuros = {ex.submit(_leer, a): a for a in archivos}
        for fut in as_completed(futuros):
            r = fut.result()
            if r is not None:
                dfs.append(r)

    if not dfs:
        return pd.DataFrame()

    df = pd.concat(dfs, ignore_index=True)
    log(f"  {fuente}: {len(df):,} filas ({len(archivos)} archivos, ultimos {max_dias} dias)")
    return df


def leer_todo_el_bronze(fs):
    log("Leyendo bronze desde ADLS (ultimos 14 dias)...")
    bronze = {}
    for f in FUENTES:
        df = leer_bronze_fuente(fs, f)
        if not df.empty:
            bronze[f] = df
    total = sum(len(v) for v in bronze.values())
    log(f"  Total leido: {total:,} filas de {len(bronze)}/6 fuentes")
    return bronze


# ============================================================
# APRENDER PATRONES POR FUENTE
# ============================================================

def aprender_patrones(bronze):
    """Extrae estadisticas por zona x hora x dia_semana de cada fuente bronze."""
    pat = {}

    # ── SENSORES ──────────────────────────────────────────────
    df = bronze.get("sensores_trafico", pd.DataFrame())
    pat["sensores"] = {}
    if not df.empty:
        if "_ingest_ts" in df.columns:
            df["_ts"] = pd.to_datetime(df["_ingest_ts"], errors="coerce", utc=True)
            df["hora"] = df["_ts"].dt.hour
            df["dia_semana"] = df["_ts"].dt.dayofweek
        for z in ZONA_NOMBRES:
            pat["sensores"][z] = {}
            dz = df[df["zona"] == z] if "zona" in df.columns else pd.DataFrame()
            for h in range(24):
                dzh = dz[dz["hora"] == h] if not dz.empty else pd.DataFrame()
                pat["sensores"][z][h] = {
                    "cr_mean":  safe_stat(dzh.get("congestion_ratio"),   "mean", 0.50),
                    "cr_std":   max(safe_stat(dzh.get("congestion_ratio"),   "std",  0.06), 0.01),
                    "ivh_mean": safe_stat(dzh.get("intensidad_veh_hora"), "mean", 400.0),
                    "ivh_std":  max(safe_stat(dzh.get("intensidad_veh_hora"), "std", 50.0), 1.0),
                }

    # ── GPS ───────────────────────────────────────────────────
    df = bronze.get("gps_rutas", pd.DataFrame())
    pat["gps"] = {}
    if not df.empty:
        if "_ingest_ts" in df.columns:
            df["_ts"] = pd.to_datetime(df["_ingest_ts"], errors="coerce", utc=True)
            df["hora"] = df["_ts"].dt.hour
        for z in ZONA_NOMBRES:
            pat["gps"][z] = {}
            dz = df[df["zona"] == z] if "zona" in df.columns else pd.DataFrame()
            for h in range(24):
                dzh = dz[dz["hora"] == h] if not dz.empty else pd.DataFrame()
                pat["gps"][z][h] = {
                    "vel_mean": safe_stat(dzh.get("velocidad_kmh"),    "mean", 35.0),
                    "vel_std":  max(safe_stat(dzh.get("velocidad_kmh"),    "std",  5.0), 0.5),
                    "cf_mean":  safe_stat(dzh.get("congestion_factor"), "mean", 2.0),
                    "cf_std":   max(safe_stat(dzh.get("congestion_factor"), "std",  0.3), 0.05),
                }

    # ── CAMARAS ───────────────────────────────────────────────
    df = bronze.get("vision_camaras", pd.DataFrame())
    pat["camaras"] = {}
    if not df.empty:
        if "_ingest_ts" in df.columns:
            df["_ts"] = pd.to_datetime(df["_ingest_ts"], errors="coerce", utc=True)
            df["hora"] = df["_ts"].dt.hour
        for z in ZONA_NOMBRES:
            pat["camaras"][z] = {}
            dz = df[df["zona"] == z] if "zona" in df.columns else pd.DataFrame()
            for h in range(24):
                dzh = dz[dz["hora"] == h] if not dz.empty else pd.DataFrame()
                pat["camaras"][z][h] = {
                    "total_mean": safe_stat(dzh.get("total_detectado"), "mean", 200.0),
                    "total_std":  max(safe_stat(dzh.get("total_detectado"), "std",  30.0), 1.0),
                }

    # ── CLIMA ─────────────────────────────────────────────────
    df = bronze.get("clima", pd.DataFrame())
    pat["clima"] = {}
    if not df.empty:
        if "_ingest_ts" in df.columns:
            df["_ts"] = pd.to_datetime(df["_ingest_ts"], errors="coerce", utc=True)
            df["hora"] = df["_ts"].dt.hour
        for z in ZONA_NOMBRES:
            pat["clima"][z] = {}
            dz = df[df["zona"] == z] if "zona" in df.columns else pd.DataFrame()
            for h in range(24):
                dzh = dz[dz["hora"] == h] if not dz.empty else pd.DataFrame()
                pat["clima"][z][h] = {
                    "temp_mean": safe_stat(dzh.get("temperatura_c"),    "mean", 20.0),
                    "temp_std":  max(safe_stat(dzh.get("temperatura_c"),    "std",  1.5), 0.2),
                    "hum_mean":  safe_stat(dzh.get("humedad_pct"),      "mean", 75.0),
                    "hum_std":   max(safe_stat(dzh.get("humedad_pct"),      "std",  6.0), 0.5),
                    "prec_mean": safe_stat(dzh.get("precipitacion_mm"), "mean", 0.1),
                    "vien_mean": safe_stat(dzh.get("viento_kmh"),       "mean", 12.0),
                    "vien_std":  max(safe_stat(dzh.get("viento_kmh"),       "std",  3.0), 0.3),
                }

    # ── EVENTOS ───────────────────────────────────────────────
    df = bronze.get("eventos", pd.DataFrame())
    if not df.empty:
        total_dias = df["fecha"].nunique() if "fecha" in df.columns else 1
        total_ev   = len(df)
        prob_ev    = min(total_ev / max(total_dias * len(ZONA_NOMBRES), 1), 0.30)
        dist_tipo  = df["tipo"].value_counts(normalize=True).to_dict() if "tipo" in df.columns else {}
        tipos_validos = [t for t in TIPOS_EVENTO if t != "ninguno"]
        probs_tipo = np.array([dist_tipo.get(t, 1/len(tipos_validos)) for t in tipos_validos], dtype=float)
        probs_tipo /= probs_tipo.sum()
    else:
        prob_ev    = 0.15
        tipos_validos = [t for t in TIPOS_EVENTO if t != "ninguno"]
        probs_tipo = np.ones(len(tipos_validos)) / len(tipos_validos)
    pat["eventos"] = {"prob": prob_ev, "tipos": tipos_validos, "probs_tipo": probs_tipo}

    # ── NOTICIAS ──────────────────────────────────────────────
    df = bronze.get("redes_noticias", pd.DataFrame())
    if not df.empty:
        n_por_hora = len(df) / max(df["_ingest_ts"].nunique() if "_ingest_ts" in df.columns else 1, 1)
        n_por_hora = float(np.clip(n_por_hora, 2, 10))
        dist_sent  = df["sentiment"].value_counts(normalize=True).to_dict() if "sentiment" in df.columns else {}
        p_sent = np.array([dist_sent.get(s, 1/3) for s in SENTIMIENTOS], dtype=float)
        p_sent /= p_sent.sum()
        pct_trafico = float(df["es_trafico"].mean()) if "es_trafico" in df.columns else 0.6
    else:
        n_por_hora  = 5.0
        p_sent      = np.array([0.2, 0.5, 0.3])
        pct_trafico = 0.6
    pat["noticias"] = {"n_mean": n_por_hora, "p_sent": p_sent, "pct_trafico": pct_trafico}

    # ── FACTOR DIA SEMANA (de sensores) ───────────────────────
    df = bronze.get("sensores_trafico", pd.DataFrame())
    if not df.empty and "dia_semana" in df.columns and "congestion_ratio" in df.columns:
        media = safe_stat(df.get("congestion_ratio"), "mean", 0.5)
        factor_dia = np.ones(7)
        for d in range(7):
            v = safe_stat(df[df["dia_semana"] == d].get("congestion_ratio") if not df.empty else pd.Series(), "mean", media)
            factor_dia[d] = v / media if media > 0 else 1.0
    else:
        factor_dia = np.array([1.10, 1.10, 1.10, 1.12, 1.20, 0.80, 0.55])

    log(f"  Factor dia semana aprendido: {[round(f, 2) for f in factor_dia]}")
    _interpolar_horas_vacias(pat)
    return pat, factor_dia


def _interpolar_horas_vacias(pat):
    """Rellena horas con 0s usando vecinos."""
    for fuente in ["sensores", "gps", "camaras", "clima"]:
        if fuente not in pat:
            continue
        for z in ZONA_NOMBRES:
            for h in range(24):
                p = pat[fuente].get(z, {}).get(h, {})
                if not p or all(v == 0 for v in p.values()):
                    vecinos = []
                    for d in [-2, -1, 1, 2]:
                        vp = pat[fuente].get(z, {}).get((h + d) % 24, {})
                        if vp and any(v != 0 for v in vp.values()):
                            vecinos.append(vp)
                    if vecinos:
                        keys = vecinos[0].keys()
                        if z not in pat[fuente]:
                            pat[fuente][z] = {}
                        pat[fuente][z][h] = {k: float(np.mean([v[k] for v in vecinos])) for k in keys}


# ============================================================
# GENERAR UNA HORA DE BRONZE (todas las fuentes)
# ============================================================

def generar_hora(ts: datetime, pat, factor_dia, rng, fa=1.0, fm=1.0):
    """Genera registros de las 6 fuentes para un timestamp horario."""
    h   = ts.hour
    fd  = float(factor_dia[ts.weekday()])
    ts_iso = ts.replace(tzinfo=timezone.utc).isoformat()
    fecha_str = ts.strftime("%Y-%m-%d")
    out = {f: [] for f in FUENTES}

    for z in ZONAS:
        zn  = z["nombre"]
        ps  = pat["sensores"].get(zn, {}).get(h, {})
        pg  = pat["gps"].get(zn, {}).get(h, {})
        pc  = pat["camaras"].get(zn, {}).get(h, {})
        pcl = pat["clima"].get(zn, {}).get(h, {})

        cr  = float(np.clip(ps.get("cr_mean", 0.5) * fd * fa * fm + rng.normal(0, ps.get("cr_std", 0.06)), 0.01, 0.99))
        ivh = float(np.clip(ps.get("ivh_mean", 400) * fd * fa * fm + rng.normal(0, ps.get("ivh_std", 50)), 0, None))
        cap = z["capacidad"] * 12
        ns  = NS_LABELS[int(np.digitize(cr, NS_UMBRALES))]

        # Sensores
        out["sensores_trafico"].append({
            "zona": zn, "lat": z["lat"], "lon": z["lon"],
            "hora_local":          h,
            "congestion_ratio":    round(cr, 4),
            "intensidad_veh_hora": round(ivh, 1),
            "capacidad_veh_hora":  cap,
            "ocupacion_pct":       round(cr * 100, 1),
            "carga_pct":           round(float(np.clip(ivh / max(cap, 1) * 100, 0, 150)), 1),
            "sensores_activos":    int(rng.integers(4, 7)),
            "nivel_servicio":      ns,
            "_calidad_fuente":     "sintetico_calibrado",
            "_ingest_ts":          ts_iso,
        })

        # GPS (3 rutas por zona)
        vel = float(np.clip(pg.get("vel_mean", 35) / max(fd * fa * fm, 0.1) + rng.normal(0, pg.get("vel_std", 5)), 3, 130))
        cf  = float(np.clip(pg.get("cf_mean", 2.0) * fd * fa * fm + rng.normal(0, pg.get("cf_std", 0.3)), 1.0, 5.0))
        for i in range(3):
            dist   = round(float(rng.uniform(3, 18)), 2)
            dist_m = int(dist * 1000)
            dur_l  = int(dist_m / max(z["vel_libre"] / 3.6, 1))
            dur_t  = float(np.clip(dur_l * cf + rng.normal(0, 30), 60, None))
            out["gps_rutas"].append({
                "zona": zn,
                "ruta_id":            f"R{abs(hash(ts_iso + zn + str(i))) % 99999:05d}",
                "origen":             zn,
                "destino":            f"Destino_{i+1}",
                "velocidad_kmh":      round(vel + rng.normal(0, 2), 2),
                "congestion_factor":  round(cf, 4),
                "distancia_m":        dist_m,
                "duracion_libre_s":   dur_l,
                "duracion_trafico_s": round(dur_t, 1),
                "_calidad_fuente":    "sintetico_calibrado",
                "_ingest_ts":         ts_iso,
            })

        # Camaras (entrada + salida)
        for pos in ["entrada", "salida"]:
            total = max(0, int(pc.get("total_mean", 200) * fd * fa * fm + rng.normal(0, pc.get("total_std", 30))))
            restante = total
            cnts = {}
            comp_items = [("auto_taxi",0.55),("combi_minibus",0.20),("moto_mototaxi",0.12),
                          ("bus",0.05),("camioneta_lgv",0.06)]
            for tipo, prop in comp_items:
                cnts[f"cnt_{tipo}"] = round(total * prop)
                restante -= cnts[f"cnt_{tipo}"]
            cnts["cnt_camion_hgv"] = max(0, restante)
            out["vision_camaras"].append({
                "zona": zn,
                "camara_id":         f"CAM_{zn[:6].replace(' ','').upper()}_{1 if pos=='entrada' else 2:02d}",
                "posicion":          pos,
                "lat":               z["lat"], "lon": z["lon"],
                "hora_local":        h,
                "tipo_corredor":     z["tipo"],
                "total_detectado":   total,
                "tasa_deteccion_pct":round(float(rng.uniform(96, 104)), 1),
                "ventana_min":       5,
                **cnts,
                "_calidad_fuente":   "sintetico_calibrado",
                "_ingest_ts":        ts_iso,
            })

        # Clima
        temp = float(np.clip(pcl.get("temp_mean", 20) + rng.normal(0, pcl.get("temp_std", 1.5)), 10, 35))
        hum  = float(np.clip(pcl.get("hum_mean", 75)  + rng.normal(0, pcl.get("hum_std",  6.0)), 30, 100))
        prec = float(np.clip(abs(pcl.get("prec_mean", 0.1) + rng.normal(0, 0.3)), 0, 40))
        vien = float(np.clip(pcl.get("vien_mean", 12) + rng.normal(0, pcl.get("vien_std", 3.0)), 0, 60))
        out["clima"].append({
            "zona": zn, "lat": z["lat"], "lon": z["lon"],
            "hora_local":       ts.strftime("%Y-%m-%dT%H:%M"),
            "temperatura_c":    round(temp, 2),
            "humedad_pct":      round(hum, 1),
            "precipitacion_mm": round(prec, 2),
            "viento_kmh":       round(vien, 1),
            "codigo_clima":     int(61 if prec > 0.5 else (45 if 4 <= ts.month <= 9 else 0)),
            "_calidad_fuente":  "sintetico_calibrado",
            "_ingest_ts":       ts_iso,
        })

        # Eventos (solo a las 00:00)
        if h == 0:
            pe = pat["eventos"]
            if rng.random() < pe["prob"]:
                tipo = rng.choice(pe["tipos"], p=pe["probs_tipo"])
                out["eventos"].append({
                    "zona_afectada":  zn,
                    "fecha":          fecha_str,
                    "nombre":         f"{tipo.capitalize()} en {zn}",
                    "tipo":           tipo,
                    "severidad":      SEVER_EVENTO[tipo],
                    "impacto_factor": round(float(IMPACTO_EVENTO[tipo] + rng.normal(0, 0.05)), 3),
                    "dias_para_evento": 0,
                    "_calidad_fuente":"sintetico_calibrado",
                    "_ingest_ts":     ts_iso,
                })

    # Noticias (global, no por zona)
    pn = pat["noticias"]
    n  = int(rng.poisson(pn["n_mean"]))
    for i in range(max(1, n)):
        zona_n = rng.choice(ZONA_NOMBRES + ["Lima"])
        es_tr  = bool(rng.random() < pn["pct_trafico"])
        sent   = rng.choice(SENTIMIENTOS, p=pn["p_sent"])
        out["redes_noticias"].append({
            "item_id":         f"{ts.strftime('%Y%m%d%H%M')}_{i:03d}",
            "titulo":          f"Reporte {'trafico' if es_tr else 'ciudad'} Lima {ts.strftime('%H:%M')}",
            "zona":            zona_n,
            "es_trafico":      es_tr,
            "sentiment":       sent,
            "relevance_score": round(float(rng.uniform(0.3, 1.0) if es_tr else rng.uniform(0.0, 0.4)), 3),
            "_calidad_fuente": "sintetico_calibrado",
            "_ingest_ts":      ts_iso,
        })

    return out


# ============================================================
# GUARDAR EN ADLS — 1 parquet por DIA por fuente (no por hora)
# Reduce escrituras 24x y evita rate-limiting de ADLS.
# Checkpoint: si el archivo ya existe, el dia se saltea.
# ============================================================

def _dia_ya_guardado(fs, fuente, fecha_str):
    ruta = f"{ADLS_CONTAINER}/bronze/{fuente}/fecha={fecha_str}/sintetico_dia.parquet"
    try:
        return fs.exists(ruta)
    except Exception:
        return False

def guardar_dia_adls(fs, datos_dia, fecha_str, retries=3):
    """Escribe 1 parquet por fuente para el dia completo."""
    for fuente, rows in datos_dia.items():
        if not rows:
            continue
        df   = pd.DataFrame(rows)
        ruta = f"{ADLS_CONTAINER}/bronze/{fuente}/fecha={fecha_str}/sintetico_dia.parquet"
        for intento in range(retries):
            try:
                with fs.open(ruta, "wb") as f:
                    df.to_parquet(f, index=False)
                break
            except Exception as e:
                if intento == retries - 1:
                    log(f"  [ERROR] {fuente} {fecha_str} (intento {intento+1}): {e}")


# ============================================================
# GENERAR UN RANGO DE FECHAS — batch diario con checkpoint
# ============================================================

def generar_rango(fs, fecha_inicio: date, fecha_fin: date,
                  pat, factor_dia, rng,
                  fa_func=None, label=""):
    total_dias = (fecha_fin - fecha_inicio).days + 1
    log(f"  Generando {fecha_inicio} -> {fecha_fin} ({total_dias:,} dias)")

    conteo     = {f: 0 for f in FUENTES}
    saltados   = 0
    dia_actual = fecha_inicio

    while dia_actual <= fecha_fin:
        fecha_str = dia_actual.strftime("%Y-%m-%d")

        # Checkpoint: saltar dias ya guardados
        ya_listo = _dia_ya_guardado(fs, "sensores_trafico", fecha_str)
        if ya_listo:
            saltados += 1
            dia_actual += timedelta(days=1)
            continue

        # Acumular todas las horas del dia en memoria
        datos_dia = {f: [] for f in FUENTES}
        for h in range(24):
            ts = datetime.combine(dia_actual, datetime.min.time()).replace(hour=h)
            fm = float(FACTOR_MES_LIMA[ts.month - 1])
            fa = fa_func(ts) if fa_func else 1.0
            datos_h = generar_hora(ts, pat, factor_dia, rng, fa=fa, fm=fm)
            for fuente, rows in datos_h.items():
                datos_dia[fuente].extend(rows)

        # Una sola escritura por dia (6 archivos en vez de 144)
        guardar_dia_adls(fs, datos_dia, fecha_str)
        for fuente, rows in datos_dia.items():
            conteo[fuente] += len(rows)

        # Log cada 30 dias
        dias_hechos = (dia_actual - fecha_inicio).days + 1
        if dias_hechos % 30 == 0 or dia_actual == fecha_fin:
            total = sum(conteo.values())
            log(f"    {fecha_str} — dia {dias_hechos}/{total_dias} — {total:,} filas acum.")

        dia_actual += timedelta(days=1)

    if saltados:
        log(f"  (Checkpoint: {saltados} dias ya existian, saltados)")
    total = sum(conteo.values())
    log(f"  {label} completado: {total:,} filas")
    for f, n in conteo.items():
        log(f"    {f:<25}: {n:,}")
    return conteo


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--solo-fase", type=int, choices=[1, 2], default=0,
                        help="Ejecutar solo la fase indicada (default: ambas)")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    rng = np.random.default_rng(args.seed)

    fs = adlfs.AzureBlobFileSystem(account_name=ADLS_ACCOUNT, account_key=ADLS_KEY)

    sep = "=" * 62
    log(sep)
    log("  GENERADOR BRONZE HISTORICO — 2 fases calibradas")
    log(sep)

    # ──────────────────────────────────────────────────────────
    # FASE 1: Bronze real -> patrones -> sintetico 24-abr al 27-jun
    # ──────────────────────────────────────────────────────────
    if args.solo_fase in (0, 1):
        log("\n[FASE 1] Aprendiendo patrones del bronze REAL...")
        bronze_real = leer_todo_el_bronze(fs)
        pat1, fd1   = aprender_patrones(bronze_real)

        log("\n[FASE 1] Generando sintetico 24-Apr-2026 -> 27-Jun-2026...")
        generar_rango(
            fs,
            fecha_inicio = date(2026, 5, 24),
            fecha_fin    = date(2026, 6, 27),
            pat=pat1, factor_dia=fd1, rng=rng,
            fa_func=lambda ts: 1.0,   # mismo año de referencia, sin ajuste
            label="Fase 1 (hueco abr-jun 2026)"
        )
        log("[FASE 1] COMPLETADA")

    # ──────────────────────────────────────────────────────────
    # FASE 2: (real + fase1) -> patrones enriquecidos -> historico 2010-2026
    # ──────────────────────────────────────────────────────────
    if args.solo_fase in (0, 2):
        log("\n[FASE 2] Leyendo bronze completo (real + fase 1) para enriquecer patrones...")
        bronze_completo = leer_todo_el_bronze(fs)
        pat2, fd2       = aprender_patrones(bronze_completo)

        def factor_anual(ts):
            años_diff = AÑO_REF - ts.year
            return 1.0 / ((1 + CRECIMIENTO_ANUAL) ** años_diff)

        log("\n[FASE 2] Generando historico Ene-2010 -> Mar-2026...")
        generar_rango(
            fs,
            fecha_inicio = date(2010, 1, 1),
            fecha_fin    = date(2026, 3, 31),
            pat=pat2, factor_dia=fd2, rng=rng,
            fa_func=factor_anual,
            label="Fase 2 (historico 2010-2026)"
        )
        log("[FASE 2] COMPLETADA")

    log(sep)
    log("  TODO COMPLETADO")
    log("  Siguiente paso: correr silver_notebook.py y gold_notebook.py")
    log(sep)


if __name__ == "__main__":
    main()
