"""
Fuente #5 — CLIMA (estructurado). DATOS REALES vía Open-Meteo (sin API key).

Por cada micro-batch jala el clima actual de cada zona de Lima definida en config.
El clima es un driver clave del tráfico (lluvia => congestión), por eso es fuente propia.

Doc API: https://open-meteo.com/en/docs
Uso:
    python 01_ingesta/clima.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import ZONAS_LIMA

import requests
from _bronze import guardar_bronze

API = "https://api.open-meteo.com/v1/forecast"
CAMPOS = "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m,weather_code"


def extraer():
    """Devuelve una lista de dicts, un registro de clima por zona."""
    registros = []
    for zona in ZONAS_LIMA:
        params = {
            "latitude": zona["lat"],
            "longitude": zona["lon"],
            "current": CAMPOS,
            "timezone": "America/Lima",
        }
        try:
            r = requests.get(API, params=params, timeout=15)
            r.raise_for_status()
            cur = r.json().get("current", {})
        except requests.RequestException as e:
            print(f"[clima] error en zona {zona['nombre']}: {e}")
            continue

        registros.append({
            "zona": zona["nombre"],
            "lat": zona["lat"],
            "lon": zona["lon"],
            "hora_local": cur.get("time"),
            "temperatura_c": cur.get("temperature_2m"),
            "humedad_pct": cur.get("relative_humidity_2m"),
            "precipitacion_mm": cur.get("precipitation"),
            "viento_kmh": cur.get("wind_speed_10m"),
            "codigo_clima": cur.get("weather_code"),
        })
    n_antes = len(registros)
    registros = [r for r in registros
                 if r.get("temperatura_c") is not None
                 and -5 <= r.get("temperatura_c", 999) <= 45
                 and 0 <= r.get("humedad_pct", -1) <= 100]
    if len(registros) < n_antes:
        print(f"[clima] {n_antes - len(registros)} registros descartados por rango invalido")

    return registros


if __name__ == "__main__":
    registros = extraer()
    guardar_bronze(registros, "clima")
