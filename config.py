"""
Configuración central del proyecto.
Rutas, parámetros del streaming falso y credenciales (vía variables de entorno).

Uso:
    from config import RUTAS, INTERVALO_MIN, FUENTES
"""
from pathlib import Path
import os

# --------------------------------------------------------------------------
# Rutas base — Fase 1 (local). En Fase 2 se reemplazan por rutas ADLS Gen2:
#   abfss://<container>@<cuenta>.dfs.core.windows.net/<capa>
# --------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
LAKEHOUSE = BASE_DIR / "03_lakehouse"

RUTAS = {
    "bronze": LAKEHOUSE / "bronze",
    "silver": LAKEHOUSE / "silver",
    "gold":   LAKEHOUSE / "gold",
    "temp":   BASE_DIR / "07_temp",
}

# --------------------------------------------------------------------------
# Streaming falso: cada cuántos minutos se dispara un micro-batch (Velocidad)
# --------------------------------------------------------------------------
INTERVALO_MIN = 5

# --------------------------------------------------------------------------
# Catálogo de fuentes (Variedad). 'tipo' documenta la 5V correspondiente.
# --------------------------------------------------------------------------
FUENTES = {
    "sensores_trafico": {"tipo": "estructurado",      "modulo": "sensores_trafico"},
    "gps_rutas":        {"tipo": "semiestructurado",  "modulo": "gps_rutas"},
    "vision_camaras":   {"tipo": "estructurado",      "modulo": "vision_camaras"},
    "redes_twitter":    {"tipo": "no_estructurado",   "modulo": "redes_twitter"},
    "clima":            {"tipo": "estructurado",      "modulo": "clima"},
    "eventos":          {"tipo": "semiestructurado",  "modulo": "eventos"},
}

# --------------------------------------------------------------------------
# Credenciales — NUNCA hardcodear. Definir como variables de entorno.
# --------------------------------------------------------------------------
TWITTER_BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN", "")
GOOGLE_MAPS_API_KEY  = os.getenv("GOOGLE_MAPS_API_KEY", "")
CLIMA_API_KEY        = os.getenv("CLIMA_API_KEY", "")

# Zona geográfica de estudio (enfoque híbrido: narrativa Lima, calibrable con datos reales)
CIUDAD = "Lima"
BBOX = {"lat_min": -12.20, "lat_max": -11.95, "lon_min": -77.15, "lon_max": -76.90}

# Puntos/corredores reales de Lima usados como "estaciones" de cada fuente.
# (coords aproximadas de avenidas clave; sirven para clima, sensores, GPS, cámaras)
ZONAS_LIMA = [
    {"nombre": "Via Expresa - Centro",               "lat": -12.071, "lon": -77.033},
    {"nombre": "Javier Prado - San Isidro",          "lat": -12.092, "lon": -77.022},
    {"nombre": "Panamericana Norte - Independencia", "lat": -11.990, "lon": -77.060},
    {"nombre": "Carretera Central - Ate",            "lat": -12.030, "lon": -76.920},
    {"nombre": "Av. Brasil - Magdalena",             "lat": -12.090, "lon": -77.060},
    {"nombre": "Costa Verde - Miraflores",           "lat": -12.130, "lon": -77.030},
]
