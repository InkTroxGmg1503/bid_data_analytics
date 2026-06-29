"""
Fuente #6 — EVENTOS (semi-estructurado).

Dos sub-fuentes reales combinadas:
  1. Nager.Date API  → feriados oficiales del Perú (sin key, JSON).
  2. CSV interno     → eventos grandes de Lima 2026 con impacto en tráfico
                       (partidos Liga 1, conciertos, ferias, maratones).
                       Fuente: calendarios oficiales FPF / organizadores.

Cada micro-batch reporta los eventos ACTIVOS HOY y los próximos 7 días,
con su impacto estimado en tráfico (zona afectada, hora inicio/fin, severidad).

Uso:
    python 01_ingesta/eventos.py
"""
import sys
from pathlib import Path
import io

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from datetime import datetime, timezone, timedelta
import requests
import pandas as pd
from _bronze import guardar_bronze

API_FERIADOS = "https://date.nager.at/api/v3/PublicHolidays/{year}/PE"

# --------------------------------------------------------------------------
# Eventos grandes Lima 2026 con impacto en tráfico.
# Fuente: calendarios FPF Liga 1, APDAYC, Municipalidad de Lima.
# Zona afectada = zona de ZONAS_LIMA más cercana al venue.
# Severidad: alta / media / baja (impacto en congestion_ratio esperado).
# --------------------------------------------------------------------------
EVENTOS_LIMA_CSV = """fecha,hora_inicio,hora_fin,nombre,tipo,venue,zona_afectada,severidad
2026-07-28,08:00,14:00,Desfile Fiestas Patrias,feriado_especial,Av. Brasil - Magdalena,Av. Brasil - Magdalena,alta
2026-07-29,00:00,23:59,Feriado Independencia (día 2),feriado,Lima Centro,Via Expresa - Centro,media
2026-06-29,10:00,22:00,San Pedro y San Pablo - feriado largo,feriado,Lima Norte,Panamericana Norte - Independencia,media
2026-08-30,08:00,20:00,Santa Rosa de Lima - procesión,procesion,Lima Centro,Via Expresa - Centro,alta
2026-10-18,15:00,22:00,Señor de los Milagros - procesión,procesion,Lima Centro,Via Expresa - Centro,alta
2026-04-05,08:00,20:00,Semana Santa - Domingo de Resurrección,feriado_especial,Lima,Via Expresa - Centro,alta
2026-07-04,20:00,23:00,Universitario vs Alianza Lima - Estadio Nacional,futbol,Estadio Nacional,Av. Brasil - Magdalena,alta
2026-08-15,16:00,19:00,Sporting Cristal vs Universitario - Estadio Nacional,futbol,Estadio Nacional,Av. Brasil - Magdalena,alta
2026-09-12,19:00,22:00,Concierto - Estadio Nacional,concierto,Estadio Nacional,Av. Brasil - Magdalena,media
2026-11-07,16:00,19:00,Alianza Lima vs Sporting Cristal - Matute,futbol,Estadio Matute,Javier Prado - San Isidro,media
2026-12-06,07:00,10:00,Maratón de Lima,maraton,Miraflores,Costa Verde - Miraflores,alta
2026-10-08,08:00,20:00,Combate de Angamos - feriado,feriado,Lima,Via Expresa - Centro,baja
2026-12-08,08:00,20:00,Inmaculada Concepción - feriado,feriado,Lima,Via Expresa - Centro,baja
2026-12-24,18:00,23:59,Nochebuena - tráfico salida,festividad,Lima,Via Expresa - Centro,alta
2026-12-31,18:00,23:59,Fin de año - tráfico salida,festividad,Lima,Costa Verde - Miraflores,alta
"""

SEVERIDAD_FACTOR = {"alta": 1.45, "media": 1.20, "baja": 1.08}


def _cargar_feriados(año):
    """Descarga feriados oficiales del Perú desde Nager.Date."""
    try:
        r = requests.get(API_FERIADOS.format(year=año), timeout=10)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        print(f"[eventos] error al obtener feriados {año}: {e}")
        return []


def _cargar_eventos_csv():
    """Lee el CSV de eventos Lima embebido en este módulo."""
    return pd.read_csv(io.StringIO(EVENTOS_LIMA_CSV))


def extraer():
    hoy = datetime.now(timezone.utc).date()
    ventana_fin = hoy + timedelta(days=7)

    registros = []

    # --- Sub-fuente 1: Feriados Nager.Date ---
    for año in {hoy.year, ventana_fin.year}:
        for f in _cargar_feriados(año):
            fecha = datetime.strptime(f["date"], "%Y-%m-%d").date()
            if hoy <= fecha <= ventana_fin:
                registros.append({
                    "fecha":          str(fecha),
                    "hora_inicio":    "00:00",
                    "hora_fin":       "23:59",
                    "nombre":         f["localName"],
                    "tipo":           "feriado_oficial",
                    "venue":          "Nacional",
                    "zona_afectada":  "todas",
                    "severidad":      "media",
                    "impacto_factor": SEVERIDAD_FACTOR["media"],
                    "dias_para_evento": (fecha - hoy).days,
                    "sub_fuente":     "Nager.Date / gobierno Peru",
                })

    # --- Sub-fuente 2: Eventos Lima CSV ---
    df = _cargar_eventos_csv()
    df["fecha_dt"] = pd.to_datetime(df["fecha"]).dt.date
    df_ventana = df[(df["fecha_dt"] >= hoy) & (df["fecha_dt"] <= ventana_fin)]

    for _, row in df_ventana.iterrows():
        registros.append({
            "fecha":          str(row["fecha_dt"]),
            "hora_inicio":    row["hora_inicio"],
            "hora_fin":       row["hora_fin"],
            "nombre":         row["nombre"],
            "tipo":           row["tipo"],
            "venue":          row["venue"],
            "zona_afectada":  row["zona_afectada"],
            "severidad":      row["severidad"],
            "impacto_factor": SEVERIDAD_FACTOR.get(row["severidad"], 1.0),
            "dias_para_evento": (row["fecha_dt"] - hoy).days,
            "sub_fuente":     "Calendario Lima 2026 (FPF / organizadores)",
        })

    if not registros:
        # Si no hay eventos en la ventana, registra igualmente el estado "sin eventos"
        registros.append({
            "fecha":          str(hoy),
            "hora_inicio":    None,
            "hora_fin":       None,
            "nombre":         "Sin eventos relevantes",
            "tipo":           "ninguno",
            "venue":          None,
            "zona_afectada":  "ninguna",
            "severidad":      "ninguna",
            "impacto_factor": 1.0,
            "dias_para_evento": 0,
            "sub_fuente":     "sistema",
        })

    return registros


if __name__ == "__main__":
    registros = extraer()
    guardar_bronze(registros, "eventos")
