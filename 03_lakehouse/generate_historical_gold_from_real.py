"""
Genera datos históricos Gold calibrados con patrones aprendidos de la data real.

Nivel 1: 4 meses horarios  (feb-mayo 2026) usando patrones reales por zona×hora×día_semana
Nivel 2: 10 años (2015-2024) escalando el Nivel 1 con tendencia anual + estacionalidad Lima

Sube directamente a ADLS gold/ reemplazando el histórico existente.

Uso:
    python 03_lakehouse/generate_historical_gold_from_real.py            (ambos niveles)
    python 03_lakehouse/generate_historical_gold_from_real.py --nivel 1  (solo 4 meses)
    python 03_lakehouse/generate_historical_gold_from_real.py --nivel 2  (4 meses + 10 años)
    python 03_lakehouse/generate_historical_gold_from_real.py --dias 14  (ultimos N dias reales)
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
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

ADLS_ACCOUNT   = os.getenv("ADLS_ACCOUNT", "traficolima")
ADLS_KEY       = os.getenv("ADLS_KEY", "")
ADLS_CONTAINER = os.getenv("ADLS_CONTAINER", "trafico-lima")
SO = {"account_name": ADLS_ACCOUNT, "account_key": ADLS_KEY}

ZONAS = [
    {"nombre": "Via Expresa - Centro",               "lat": -12.071, "lon": -77.033, "vel_libre": 80, "capacidad": 85},
    {"nombre": "Javier Prado - San Isidro",          "lat": -12.092, "lon": -77.022, "vel_libre": 60, "capacidad": 60},
    {"nombre": "Panamericana Norte - Independencia", "lat": -11.990, "lon": -77.060, "vel_libre": 80, "capacidad": 75},
    {"nombre": "Carretera Central - Ate",            "lat": -12.030, "lon": -76.920, "vel_libre": 80, "capacidad": 75},
    {"nombre": "Av. Brasil - Magdalena",             "lat": -12.090, "lon": -77.060, "vel_libre": 40, "capacidad": 35},
    {"nombre": "Costa Verde - Miraflores",           "lat": -12.130, "lon": -77.030, "vel_libre": 70, "capacidad": 90},
]
ZONA_NOMBRES = [z["nombre"] for z in ZONAS]
ZONA_META    = {z["nombre"]: z for z in ZONAS}

NIVEL_SERVICIO_MAP = {0: "A", 1: "B", 2: "C", 3: "D", 4: "E", 5: "F"}

# Estacionalidad mensual Lima: garúa May-Oct sube congestión ~8-12%
FACTOR_MES = np.array([1.00, 1.00, 1.02, 1.00, 1.06, 1.10, 1.12, 1.12, 1.10, 1.08, 1.02, 1.00])

# Crecimiento anual tráfico Lima (MTC ~3.5% anual)
CRECIMIENTO_ANUAL = 0.035
AÑO_BASE = 2026   # año de la data real

COMPOSICION = {
    "pct_auto_taxi":     0.55,
    "pct_combi_minibus": 0.20,
    "pct_moto_mototaxi": 0.12,
    "pct_bus":           0.05,
    "pct_camioneta_lgv": 0.06,
    "pct_camion_hgv":    0.02,
}

TIPOS_EVENTO   = ["ninguno"] * 14 + ["deportivo", "cultural", "accidente", "obras", "marcha", "feriado"]
IMPACTO_EVENTO = {"ninguno": 1.0, "deportivo": 1.35, "cultural": 1.15,
                  "accidente": 1.55, "obras": 1.40, "marcha": 1.50, "feriado": 0.85}

def log(msg):
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


# ============================================================
# FASE 0 — APRENDER PATRONES DE LA DATA REAL
# ============================================================

def aprender_patrones(dias=7):
    """Lee gold real de ADLS y extrae patrones por zona×hora×día_semana."""
    log(f"Leyendo gold real desde ADLS (últimos {dias} días)...")
    ruta = f"abfs://{ADLS_CONTAINER}/gold/congestion_por_zona/datos.parquet"
    df_all = pd.read_parquet(ruta, storage_options=SO)
    log(f"  Gold total: {len(df_all):,} filas")

    # Filtrar solo los días recientes (data real del scheduler)
    df_all["fecha"] = pd.to_datetime(df_all["fecha"], errors="coerce")
    fecha_corte = pd.Timestamp.now(tz="UTC").tz_localize(None) - pd.Timedelta(days=dias)
    df_real = df_all[df_all["fecha"] >= fecha_corte].copy()

    if len(df_real) < 50:
        log(f"  [!] Solo {len(df_real)} filas reales — ampliando a {dias*3} días")
        fecha_corte = pd.Timestamp.now(tz="UTC").tz_localize(None) - pd.Timedelta(days=dias * 3)
        df_real = df_all[df_all["fecha"] >= fecha_corte].copy()

    log(f"  Data real usada: {len(df_real):,} filas en {df_real['fecha'].dt.date.nunique()} días")

    if "hora" not in df_real.columns and "fecha" in df_real.columns:
        df_real["hora"] = df_real["fecha"].dt.hour
    df_real["dia_semana"] = df_real["fecha"].dt.dayofweek   # 0=lunes

    # ── Patrones por zona × hora ──────────────────────────────
    METRICAS = ["congestion_ratio", "velocidad_kmh", "congestion_factor",
                "intensidad_veh_hora", "temperatura_c", "humedad_pct",
                "precipitacion_mm", "viento_kmh"]
    METRICAS = [c for c in METRICAS if c in df_real.columns]

    patron_hora = {}
    for zona in ZONA_NOMBRES:
        patron_hora[zona] = {}
        df_z = df_real[df_real["zona"] == zona]
        for h in range(24):
            df_zh = df_z[df_z["hora"] == h]
            if len(df_zh) < 2:
                # Interpolar desde horas vecinas después
                patron_hora[zona][h] = None
            else:
                def _mean(s):
                    v = pd.to_numeric(s, errors="coerce").mean()
                    return float(v) if pd.notna(v) else 0.0
                def _std(s):
                    v = pd.to_numeric(s, errors="coerce").std()
                    return max(float(v) if pd.notna(v) else 0.01, 0.01)
                patron_hora[zona][h] = {
                    "n": len(df_zh),
                    **{f"mean_{c}": _mean(df_zh[c]) for c in METRICAS if c in df_zh.columns},
                    **{f"std_{c}":  _std(df_zh[c])  for c in METRICAS if c in df_zh.columns},
                }

        # Interpolar horas sin datos usando vecinos
        for h in range(24):
            if patron_hora[zona][h] is None:
                vecinos = [patron_hora[zona][(h + d) % 24]
                           for d in [-2, -1, 1, 2]
                           if patron_hora[zona].get((h + d) % 24) is not None]
                if vecinos:
                    keys = [k for k in vecinos[0] if k != "n"]
                    patron_hora[zona][h] = {
                        "n": 1,
                        **{k: np.mean([v[k] for v in vecinos if k in v]) for k in keys}
                    }
                else:
                    # Fallback con valores genéricos
                    patron_hora[zona][h] = {
                        "n": 1,
                        "mean_congestion_ratio": 0.50, "std_congestion_ratio": 0.08,
                        "mean_velocidad_kmh": 35.0,    "std_velocidad_kmh": 5.0,
                        "mean_congestion_factor": 2.0, "std_congestion_factor": 0.3,
                    }

    # ── Factor día de semana (relativo al promedio de la semana) ──
    if "congestion_ratio" in df_real.columns:
        media_sem = df_real["congestion_ratio"].mean() or 0.5
        factor_dia = (
            df_real.groupby("dia_semana")["congestion_ratio"].mean() / media_sem
        ).reindex(range(7), fill_value=1.0).values
    else:
        factor_dia = np.array([1.10, 1.10, 1.10, 1.12, 1.20, 0.80, 0.50])

    # ── Probabilidades de nivel_congestion por zona × hora ──
    prob_nivel = {}
    for zona in ZONA_NOMBRES:
        prob_nivel[zona] = {}
        df_z = df_real[df_real["zona"] == zona]
        for h in range(24):
            df_zh = df_z[df_z["hora"] == h]
            if len(df_zh) >= 3 and "nivel_congestion" in df_zh.columns:
                vc = df_zh["nivel_congestion"].value_counts(normalize=True)
                prob_nivel[zona][h] = {
                    "alto":  vc.get("alto",  0.33),
                    "medio": vc.get("medio", 0.34),
                    "bajo":  vc.get("bajo",  0.33),
                }
            else:
                prob_nivel[zona][h] = {"alto": 0.33, "medio": 0.34, "bajo": 0.33}

    log(f"  Patrones aprendidos: {len(ZONA_NOMBRES)} zonas × 24 horas")
    log(f"  Factor día semana: {[round(f, 2) for f in factor_dia]}")
    return patron_hora, factor_dia, prob_nivel, METRICAS


# ============================================================
# GENERADOR DE UNA FILA GOLD
# ============================================================

def generar_fila(zona_meta, hora, dia_semana, fecha, patron, factor_dia,
                 prob_nivel, rng, factor_anual=1.0, factor_mes=1.0):
    zona = zona_meta["nombre"]
    p    = patron[zona][hora]

    fd = float(factor_dia[dia_semana])
    fm = float(factor_mes)
    fa = float(factor_anual)
    ruido = lambda std: rng.normal(0, std)

    # Métricas base aprendidas
    cr_base = p.get("mean_congestion_ratio", 0.5)
    cr  = float(np.clip(cr_base * fd * fm * fa + ruido(p.get("std_congestion_ratio", 0.06)), 0.01, 0.99))

    vel_base = p.get("mean_velocidad_kmh", 35.0)
    vel = float(np.clip(vel_base / (fd * fm * fa) + ruido(p.get("std_velocidad_kmh", 4.0)), 3, 130))

    cf_base = p.get("mean_congestion_factor", 2.0)
    cf  = float(np.clip(cf_base * fd * fm * fa + ruido(p.get("std_congestion_factor", 0.2)), 1.0, 5.0))

    cap = zona_meta["capacidad"] * 12
    ivh = float(np.clip(cap * cr + ruido(15), 0, None))

    # Nivel de servicio
    ns_idx = int(np.digitize(cr, [0.30, 0.50, 0.65, 0.80, 0.95]))
    ns = NIVEL_SERVICIO_MAP.get(ns_idx, "F")

    # Clima (Lima: temperatura estable, humedad varía con garúa)
    temp = float(np.clip(p.get("mean_temperatura_c", 20.0) + ruido(p.get("std_temperatura_c", 1.5)), 10, 35))
    hum  = float(np.clip(p.get("mean_humedad_pct",  75.0) + ruido(p.get("std_humedad_pct",  6.0)), 30, 100))
    prec = float(np.clip(p.get("mean_precipitacion_mm", 0.0) + max(0, ruido(p.get("std_precipitacion_mm", 0.5))), 0, 40))
    vien = float(np.clip(p.get("mean_viento_kmh", 12.0) + ruido(p.get("std_viento_kmh", 3.0)), 0, 60))

    # Evento
    tipo_ev = rng.choice(TIPOS_EVENTO)
    imp_ev  = round(float(IMPACTO_EVENTO[tipo_ev] + ruido(0.05)), 3)
    tiene_ev = int(tipo_ev != "ninguno")
    es_feriado = int(tipo_ev == "feriado")

    # Composición vehicular con ruido
    comp = {}
    total_comp = 0.0
    tipos_list = list(COMPOSICION.items())
    for k, base_pct in tipos_list[:-1]:
        v = float(np.clip(base_pct + ruido(0.02), 0.01, 1.0))
        comp[k] = v
        total_comp += v
    comp[tipos_list[-1][0]] = max(0, 1.0 - total_comp)

    total_veh = int(np.clip(ivh * rng.uniform(0.9, 1.1), 0, 5000))

    # Índices derivados
    vel_libre = zona_meta["vel_libre"]
    indice_cong = float(np.clip(cf, 1.0, 5.0))
    dur_libre_s = int((5000 / max(vel_libre / 3.6, 1)))
    dur_traf_s  = int(dur_libre_s * cf)

    # nivel_congestion aprendido de la data real
    pn = prob_nivel[zona][hora]
    probs = np.array([pn["alto"], pn["medio"], pn["bajo"]], dtype=float)
    probs /= probs.sum()
    nivel = rng.choice(["alto", "medio", "bajo"], p=probs)

    # Ajustar nivel según congestion_factor real (coherencia)
    if cf < 1.25:
        nivel = "bajo"
    elif cf < 1.70:
        nivel = "medio" if nivel != "bajo" else "bajo"
    else:
        nivel = "alto" if nivel != "medio" else "medio"

    # periodo_dia
    if 6 <= hora <= 9:
        periodo = "manana"
    elif 10 <= hora <= 14:
        periodo = "mediodia"
    elif 15 <= hora <= 19:
        periodo = "tarde"
    elif 20 <= hora <= 23:
        periodo = "noche"
    else:
        periodo = "madrugada"

    return {
        "zona":                    zona,
        "fecha":                   fecha.date().isoformat(),
        "hora":                    int(hora),
        "dia_semana":              int(dia_semana),
        "periodo_dia":             periodo,
        "lat":                     zona_meta["lat"],
        "lon":                     zona_meta["lon"],
        "congestion_ratio":        round(cr, 4),
        "velocidad_kmh":           round(vel, 2),
        "congestion_factor":       round(cf, 4),
        "intensidad_veh_hora":     round(ivh, 1),
        "capacidad_veh_hora":      cap,
        "ocupacion_pct":           round(cr * 100, 1),
        "carga_pct":               round(float(np.clip(ivh / max(cap, 1) * 100, 0, 150)), 1),
        "nivel_servicio":          ns,
        "velocidad_libre":         float(vel_libre),
        "indice_congestion":       round(indice_cong, 4),
        "duracion_libre_s":        dur_libre_s,
        "duracion_trafico_s":      dur_traf_s,
        "tendencia_congestion":    round(float(np.clip(ruido(0.05), -0.3, 0.3)), 4),
        "temperatura_c":           round(temp, 2),
        "humedad_pct":             round(hum, 1),
        "precipitacion_mm":        round(prec, 2),
        "viento_kmh":              round(vien, 1),
        "tiene_evento":            tiene_ev,
        "tipo_evento":             tipo_ev,
        "impacto_factor":          imp_ev,
        "es_feriado":              es_feriado,
        "total_vehiculos_zona":    total_veh,
        "pct_auto_taxi":           round(comp.get("pct_auto_taxi", 0.55), 4),
        "pct_combi_minibus":       round(comp.get("pct_combi_minibus", 0.20), 4),
        "pct_moto_mototaxi":       round(comp.get("pct_moto_mototaxi", 0.12), 4),
        "pct_bus":                 round(comp.get("pct_bus", 0.05), 4),
        "pct_camioneta_lgv":       round(comp.get("pct_camioneta_lgv", 0.06), 4),
        "pct_camion_hgv":          round(comp.get("pct_camion_hgv", 0.02), 4),
        "nivel_congestion":        nivel,
        "_fuente":                 "sintetico_calibrado",
        "_ingest_ts":              fecha.isoformat(),
    }


# ============================================================
# NIVEL 1 — 4 MESES HORARIOS
# ============================================================

def generar_4_meses(patron_hora, factor_dia, prob_nivel, rng):
    """Genera 4 meses de datos horarios calibrados con patrones reales."""
    hoy   = datetime.now(timezone.utc).replace(tzinfo=None)
    inicio = hoy - timedelta(days=120)   # ~4 meses atrás
    fin    = hoy - timedelta(days=1)     # hasta ayer (hoy lo genera el scheduler)

    log(f"Generando 4 meses: {inicio.date()} -> {fin.date()}")
    filas = []
    ts = inicio
    while ts <= fin:
        dia_sem = ts.weekday()
        fm = FACTOR_MES[ts.month - 1]
        for hora in range(24):
            fecha_h = ts.replace(hour=hora)
            for zona_meta in ZONAS:
                fila = generar_fila(zona_meta, hora, dia_sem, fecha_h,
                                    patron_hora, factor_dia, prob_nivel,
                                    rng, factor_anual=1.0, factor_mes=fm)
                filas.append(fila)
        ts += timedelta(days=1)

    df = pd.DataFrame(filas)
    log(f"  Nivel 1 generado: {len(df):,} filas ({ts - inicio} → {len(ZONA_NOMBRES)} zonas × 24h)")
    return df


# ============================================================
# NIVEL 2 — 10 AÑOS (2015-2024)
# ============================================================

def generar_10_años(patron_hora, factor_dia, prob_nivel, rng):
    """Genera 10 años escalando patrones reales con tendencia anual."""
    AÑO_INICIO = 2015
    AÑO_FIN    = 2024

    log(f"Generando {AÑO_FIN - AÑO_INICIO + 1} años: {AÑO_INICIO} → {AÑO_FIN}")
    dfs = []

    for anio in range(AÑO_INICIO, AÑO_FIN + 1):
        años_diff  = AÑO_BASE - anio
        fa = 1.0 / ((1 + CRECIMIENTO_ANUAL) ** años_diff)   # menos congestion en el pasado

        filas_anio = []
        # Generar un día representativo por mes (luego replicar con variación diaria)
        # Para rendimiento: muestreo de 4 semanas por mes (28 días), no 365 días
        for mes in range(1, 13):
            fm = FACTOR_MES[mes - 1]
            # 4 semanas representativas del mes
            for semana in range(4):
                for dia_sem in range(7):
                    # Fecha sintética (solo para .weekday() y .month)
                    dia_num = semana * 7 + dia_sem + 1
                    if dia_num > 28:
                        continue
                    try:
                        fecha = datetime(anio, mes, dia_num)
                    except ValueError:
                        continue

                    for hora in range(24):
                        fecha_h = fecha.replace(hour=hora)
                        for zona_meta in ZONAS:
                            fila = generar_fila(zona_meta, hora, dia_sem, fecha_h,
                                                patron_hora, factor_dia, prob_nivel,
                                                rng, factor_anual=fa, factor_mes=fm)
                            filas_anio.append(fila)

        df_anio = pd.DataFrame(filas_anio)
        dfs.append(df_anio)
        log(f"  {anio}: {len(df_anio):,} filas generadas")

    df_total = pd.concat(dfs, ignore_index=True)
    log(f"  Nivel 2 total: {len(df_total):,} filas")
    return df_total


# ============================================================
# SUBIR A ADLS
# ============================================================

def subir_a_adls(df, nombre_archivo):
    ruta = f"abfs://{ADLS_CONTAINER}/gold/congestion_por_zona/{nombre_archivo}"
    df.to_parquet(ruta, storage_options=SO, index=False)
    log(f"  Subido: {nombre_archivo} ({len(df):,} filas)")


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--nivel",  type=int, default=2,  help="1=4 meses, 2=4 meses+10 años (default: 2)")
    parser.add_argument("--dias",   type=int, default=7,  help="Días de data real para aprender patrones (default: 7)")
    parser.add_argument("--seed",   type=int, default=42, help="Semilla aleatoria")
    args = parser.parse_args()

    rng = np.random.default_rng(args.seed)

    sep = "=" * 60
    log(sep)
    log("  GENERADOR HISTÓRICO GOLD — calibrado con data real")
    log(f"  Nivel: {args.nivel} | Días reales: {args.dias} | Seed: {args.seed}")
    log(sep)

    # Fase 0: aprender patrones
    patron_hora, factor_dia, prob_nivel, _ = aprender_patrones(dias=args.dias)

    # Nivel 1: 4 meses
    log("\n[NIVEL 1] Generando 4 meses horarios...")
    df_4m = generar_4_meses(patron_hora, factor_dia, prob_nivel, rng)

    if args.nivel == 1:
        log("\nSubiendo Nivel 1 a ADLS (reemplaza histórico)...")
        subir_a_adls(df_4m, "datos.parquet")
        log(sep)
        log(f"  COMPLETADO — {len(df_4m):,} filas en ADLS gold/")
        log(sep)
        return

    # Nivel 2: 10 años
    log("\n[NIVEL 2] Generando 10 años (2015-2024)...")
    df_10a = generar_10_años(patron_hora, factor_dia, prob_nivel, rng)

    # Combinar: 10 años + 4 meses recientes
    log("\nCombinando y subiendo a ADLS...")
    df_final = pd.concat([df_10a, df_4m], ignore_index=True)
    df_final = df_final.drop_duplicates(subset=["zona", "fecha", "hora"])
    df_final = df_final.sort_values(["fecha", "zona", "hora"]).reset_index(drop=True)

    subir_a_adls(df_final, "datos.parquet")

    log(sep)
    log(f"  COMPLETADO")
    log(f"  Total filas: {len(df_final):,}")
    log(f"  Rango: {df_final['fecha'].min()} → {df_final['fecha'].max()}")
    dist = df_final["nivel_congestion"].value_counts(normalize=True).round(3)
    log(f"  Distribucion: {dist.to_dict()}")
    log(sep)


if __name__ == "__main__":
    main()
