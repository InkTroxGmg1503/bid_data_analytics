"""
Fuente #3 — GPS / RUTAS (semi-estructurado). DATOS REALES vía Google Distance Matrix API.

Estrategia híbrida en dos fases:
  Fase colección (hoy/mañana): consulta los 6 corredores de Lima en distintas horas
  del día y acumula snapshots reales en bronze. Con 2 días de capturas se obtienen
  patrones de hora punta mañana, mediodía, tarde y noche.

  Fase escala: los patrones reales observados se usan para poblar data histórica
  sintética calibrada — no inventada desde cero.

Métrica clave:
  congestion_factor = duration_in_traffic / duration   (1.0 = flujo libre, 2.0 = doble)

Uso:
    python 01_ingesta/gps_rutas.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

import os
import requests
from _bronze import guardar_bronze

API_URL = "https://maps.googleapis.com/maps/api/distancematrix/json"
API_KEY = os.getenv("GOOGLE_MAPS_API_KEY", "")

# Corredores reales de Lima: origen → destino representativo de cada zona.
# Cada par captura el flujo direccional más relevante del corredor en hora punta.
CORREDORES = [
    {
        "zona":    "Via Expresa - Centro",
        "origen":  "-12.1391,-77.0283",   # Chorrillos (sur)
        "destino": "-12.0464,-77.0428",   # Lima Centro
        "tipo_corredor": "via_expresa",
    },
    {
        "zona":    "Javier Prado - San Isidro",
        "origen":  "-12.0848,-76.9720",   # La Molina
        "destino": "-12.0921,-77.0222",   # San Isidro
        "tipo_corredor": "arteria_principal",
    },
    {
        "zona":    "Panamericana Norte - Independencia",
        "origen":  "-11.9900,-77.0600",   # Independencia norte
        "destino": "-12.0464,-77.0428",   # Lima Centro
        "tipo_corredor": "acceso_autopista",
    },
    {
        "zona":    "Carretera Central - Ate",
        "origen":  "-12.0300,-76.9200",   # Ate este
        "destino": "-12.0464,-77.0428",   # Lima Centro
        "tipo_corredor": "acceso_autopista",
    },
    {
        "zona":    "Av. Brasil - Magdalena",
        "origen":  "-12.0766,-77.0839",   # Pueblo Libre
        "destino": "-12.0921,-77.0596",   # Magdalena
        "tipo_corredor": "avenida",
    },
    {
        "zona":    "Costa Verde - Miraflores",
        "origen":  "-12.1526,-77.0218",   # Barranco
        "destino": "-12.1219,-77.0282",   # Miraflores
        "tipo_corredor": "via_periferica",
    },
]


def _consultar_corredor(corredor):
    """Consulta Google Distance Matrix para un corredor y devuelve el registro."""
    params = {
        "origins":        corredor["origen"],
        "destinations":   corredor["destino"],
        "departure_time": "now",
        "traffic_model":  "best_guess",
        "key":            API_KEY,
    }
    try:
        r = requests.get(API_URL, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
    except requests.RequestException as e:
        print(f"[gps] error en {corredor['zona']}: {e}")
        return None

    status = data.get("status")
    if status != "OK":
        print(f"[gps] API status={status} en {corredor['zona']}")
        return None

    elemento = data["rows"][0]["elements"][0]
    if elemento.get("status") != "OK":
        print(f"[gps] elemento status={elemento.get('status')} en {corredor['zona']}")
        return None

    distancia_m      = elemento["distance"]["value"]
    duration_s       = elemento["duration"]["value"]
    duration_traf_s  = elemento.get("duration_in_traffic", {}).get("value", duration_s)

    congestion_factor = round(duration_traf_s / duration_s, 3) if duration_s > 0 else 1.0
    velocidad_kmh     = round((distancia_m / duration_traf_s) * 3.6, 1) if duration_traf_s > 0 else 0.0

    return {
        "zona":              corredor["zona"],
        "tipo_corredor":     corredor["tipo_corredor"],
        "origen":            corredor["origen"],
        "destino":           corredor["destino"],
        "distancia_m":       distancia_m,
        "duracion_libre_s":  duration_s,
        "duracion_trafico_s": duration_traf_s,
        "congestion_factor": congestion_factor,   # métrica clave: 1.0=libre, 2.0=doble
        "velocidad_kmh":     velocidad_kmh,
        "fuente":            "Google Distance Matrix",
    }


def extraer():
    if not API_KEY:
        print("[gps] ERROR: GOOGLE_MAPS_API_KEY no definida en .env")
        return []

    registros = []
    for corredor in CORREDORES:
        registro = _consultar_corredor(corredor)
        if registro:
            registros.append(registro)

    n_antes = len(registros)
    registros = [r for r in registros
                 if 1 <= r.get("velocidad_kmh", 0) <= 130
                 and 1.0 <= r.get("congestion_factor", 0) <= 5.0]
    if len(registros) < n_antes:
        print(f"[gps] {n_antes - len(registros)} registros descartados por rango invalido")

    return registros


if __name__ == "__main__":
    registros = extraer()
    guardar_bronze(registros, "gps_rutas")
