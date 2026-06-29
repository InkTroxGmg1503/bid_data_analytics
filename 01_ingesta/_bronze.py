"""
Utilidad compartida de ingesta — escribe registros CRUDOS en la capa bronze.

Soporta dos modos según variables de entorno en .env:
  - Local  (default): escribe Parquet en 03_lakehouse/bronze/  (Fase 1)
  - ADLS   (si ADLS_ACCOUNT definido): escribe directo a Azure Data Lake Gen2 (Fase 2)

Bronze = datos tal cual llegan + metadatos de ingesta (_fuente, _ingest_ts) => Veracidad.
Particionado por fuente/fecha/hora => Volumen.
"""
import sys
from pathlib import Path
from datetime import datetime, timezone
import os

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

from config import RUTAS
import pandas as pd

# --------------------------------------------------------------------------
# Detectar modo: ADLS o local
# --------------------------------------------------------------------------
_ADLS_ACCOUNT   = os.getenv("ADLS_ACCOUNT", "")
_ADLS_KEY       = os.getenv("ADLS_KEY", "")
_ADLS_CONTAINER = os.getenv("ADLS_CONTAINER", "trafico-lima")
_MODO_ADLS      = bool(_ADLS_ACCOUNT and _ADLS_KEY)

_SO = {"account_name": _ADLS_ACCOUNT, "account_key": _ADLS_KEY} if _MODO_ADLS else {}

# Veracidad: calidad declarada por fuente (real / proxy / calibrado)
CALIDAD_FUENTE = {
    "clima":            "real",       # Open-Meteo API — Lima real
    "sensores_trafico": "proxy",      # Sensores Madrid → mapeados a Lima
    "gps_rutas":        "real",       # Google Distance Matrix — Lima real
    "vision_camaras":   "calibrado",  # Sintético calibrado con patrones Madrid + UK DfT
    "eventos":          "real",       # Nager.Date Perú + calendario Lima oficial
    "redes_noticias":   "real",       # RSS RPP + Andina — Lima real
}


def guardar_bronze(registros, fuente, ts=None):
    """Guarda un micro-batch como Parquet en bronze (local o ADLS según .env).

    registros: list[dict]
    fuente:    str   — nombre lógico de la fuente
    ts:        datetime — momento de ingesta (default: ahora UTC)
    """
    if not registros:
        print(f"[bronze] {fuente}: 0 registros, nada que guardar.")
        return None

    ts = ts or datetime.now(timezone.utc)
    df = pd.DataFrame(registros)
    df["_fuente"]         = fuente
    df["_ingest_ts"]      = ts.isoformat()
    df["_calidad_fuente"] = CALIDAD_FUENTE.get(fuente, "desconocido")

    fecha = ts.strftime("%Y-%m-%d")
    hora  = ts.strftime("%H")
    nombre_archivo = f"{fuente}_{ts.strftime('%Y%m%dT%H%M%SZ')}.parquet"

    if _MODO_ADLS:
        ruta = (
            f"abfs://{_ADLS_CONTAINER}"
            f"/bronze/{fuente}/fecha={fecha}/hora={hora}/{nombre_archivo}"
        )
        df.to_parquet(ruta, storage_options=_SO, index=False)
    else:
        destino = Path(RUTAS["bronze"]) / fuente / f"fecha={fecha}" / f"hora={hora}"
        destino.mkdir(parents=True, exist_ok=True)
        ruta = str(destino / nombre_archivo)
        df.to_parquet(ruta, index=False)

    modo = "ADLS" if _MODO_ADLS else "local"
    print(f"[bronze:{modo}] {fuente}: {len(df)} registros -> {nombre_archivo}")
    return ruta
