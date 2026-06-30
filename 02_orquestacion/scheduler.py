"""
Orquestador local — streaming falso cada 5 min (Fase 1).
Dispara las 6 fuentes en orden, muestra resumen por ciclo y loguea en 07_temp/.

En Fase 2 este archivo se reemplaza por un pipeline de Azure Data Factory.

Uso:
    python 02_orquestacion/scheduler.py
    python 02_orquestacion/scheduler.py --once     (un solo ciclo, para pruebas)
"""
import sys
import time
import argparse
import importlib
import traceback
import logging

# Silenciar el verbose HTTP de Azure SDK
logging.getLogger("azure").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
from pathlib import Path
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from config import INTERVALO_MIN, RUTAS

# --------------------------------------------------------------------------
# Logging: consola + archivo en 07_temp/
# --------------------------------------------------------------------------
LOG_FILE = RUTAS["temp"] / "scheduler.log"
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
# Fuerza UTF-8 en la consola de Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
log = logging.getLogger("scheduler")

# Orden de ejecución de fuentes (módulos en 01_ingesta/)
FUENTES = [
    ("clima",           "Clima (Open-Meteo)"),
    ("sensores_trafico","Sensores tráfico (Madrid Open Data)"),
    ("gps_rutas",       "GPS / Rutas (Google Distance Matrix)"),
    ("vision_camaras",  "Cámaras / Visión"),
    ("eventos",         "Eventos (Nager.Date + calendario Lima)"),
    ("redes_noticias",  "Noticias (RPP + Andina RSS)"),
]


def _ejecutar_fuente(modulo_nombre, etiqueta):
    """Importa y ejecuta extraer() + guardar_bronze() de una fuente."""
    try:
        ruta_modulo = str(ROOT / "01_ingesta")
        if ruta_modulo not in sys.path:
            sys.path.insert(0, ruta_modulo)

        mod = importlib.import_module(modulo_nombre)
        importlib.reload(mod)   # recarga por si cambia en caliente

        registros = mod.extraer()
        if registros:
            from _bronze import guardar_bronze
            archivo = guardar_bronze(registros, modulo_nombre)
            return len(registros), archivo
        else:
            log.warning(f"  {etiqueta}: 0 registros, nada guardado.")
            return 0, None
    except Exception:
        log.error(f"  {etiqueta}: ERROR\n{traceback.format_exc()}")
        return -1, None


def ejecutar_ciclo(ciclo_num):
    ts = datetime.now(timezone.utc)
    sep = "-" * 60
    log.info(sep)
    log.info(f"CICLO #{ciclo_num:03d}  |  {ts.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    log.info(sep)

    resumen = []
    for modulo, etiqueta in FUENTES:
        t0 = time.time()
        n, archivo = _ejecutar_fuente(modulo, etiqueta)
        elapsed = round(time.time() - t0, 1)

        if n > 0:
            nombre_archivo = Path(archivo).name if archivo else "?"
            log.info(f"  [OK]  {etiqueta:<42} {n:>3} registros  ({elapsed}s)  -> {nombre_archivo}")
            resumen.append((etiqueta, n, "OK"))
        elif n == 0:
            log.info(f"  [--]  {etiqueta:<42}   0 registros  ({elapsed}s)")
            resumen.append((etiqueta, 0, "VACIO"))
        else:
            log.info(f"  [ERR] {etiqueta:<42}  ERROR         ({elapsed}s)")
            resumen.append((etiqueta, -1, "ERROR"))

    total_ok = sum(1 for _, _, s in resumen if s == "OK")
    log.info(sep)
    log.info(f"  Ciclo #{ciclo_num:03d} completado -- {total_ok}/{len(FUENTES)} fuentes OK")


    log.info(f"  Proximo ciclo en {INTERVALO_MIN} min")
    log.info(sep)
    return resumen


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="Ejecutar un solo ciclo y salir")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("  SCHEDULER - Sistema Optimizacion Trafico Urbano Lima")
    log.info(f"  Intervalo: {INTERVALO_MIN} min  |  Fuentes: {len(FUENTES)}")
    log.info(f"  Log: {LOG_FILE}")
    log.info("=" * 60)

    ciclo = 1
    while True:
        ejecutar_ciclo(ciclo)
        if args.once:
            break
        ciclo += 1
        time.sleep(INTERVALO_MIN * 60)


if __name__ == "__main__":
    main()
