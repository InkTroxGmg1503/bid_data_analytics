"""
Pipeline Scheduler — Bronze -> Silver -> Gold -> ML -> ADLS -> Power BI
Se ejecuta cada INTERVALO_PIPELINE minutos de forma continua.

Uso:
    python 02_orquestacion/pipeline_scheduler.py
    python 02_orquestacion/pipeline_scheduler.py --once   (un solo ciclo, para pruebas)
"""
import sys
import time
import argparse
import subprocess
import logging
import traceback

from pathlib import Path
from datetime import datetime, timezone

# Silenciar Azure SDK verbose
logging.getLogger("azure").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from config import RUTAS

INTERVALO_PIPELINE = 10   # minutos entre cada ejecucion del pipeline
PIPELINE_PY = ROOT / "02_orquestacion" / "pipeline.py"

# --------------------------------------------------------------------------
# Logging: consola + archivo en 07_temp/
# --------------------------------------------------------------------------
LOG_FILE = RUTAS["temp"] / "pipeline_scheduler.log"
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
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
log = logging.getLogger("pipeline_scheduler")


def ejecutar_pipeline(ciclo_num):
    ts = datetime.now(timezone.utc)
    sep = "-" * 60
    log.info(sep)
    log.info(f"CICLO #{ciclo_num:03d}  |  {ts.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    log.info("  Bronze -> Silver -> Gold -> ML -> ADLS -> Power BI")
    log.info(sep)

    t0 = time.time()
    try:
        result = subprocess.run(
            [sys.executable, str(PIPELINE_PY)],
            cwd=str(ROOT),
            capture_output=False,   # muestra salida en tiempo real
            text=True,
        )
        elapsed = round(time.time() - t0, 1)

        if result.returncode == 0:
            log.info(f"  [OK]  Pipeline completado en {elapsed}s")
        else:
            log.error(f"  [ERR] Pipeline termino con codigo {result.returncode} ({elapsed}s)")

    except Exception:
        elapsed = round(time.time() - t0, 1)
        log.error(f"  [ERR] Error al lanzar pipeline ({elapsed}s):\n{traceback.format_exc()}")

    log.info(sep)
    log.info(f"  Proximo ciclo en {INTERVALO_PIPELINE} min")
    log.info(sep)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="Ejecutar un solo ciclo y salir")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("  PIPELINE SCHEDULER - Trafico Urbano Lima")
    log.info(f"  Intervalo: {INTERVALO_PIPELINE} min")
    log.info(f"  Pipeline: {PIPELINE_PY}")
    log.info(f"  Log: {LOG_FILE}")
    log.info("=" * 60)

    ciclo = 1
    while True:
        ejecutar_pipeline(ciclo)
        if args.once:
            break
        ciclo += 1
        time.sleep(INTERVALO_PIPELINE * 60)


if __name__ == "__main__":
    main()
