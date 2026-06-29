"""
Fuente #1 — SENSORES DE TRÁFICO (estructurado). DATOS REALES vía Madrid Open Data.

Estrategia híbrida:
  - Madrid publica 4890 sensores cada 5 min (mismo ritmo que nuestro micro-batch).
  - Cada zona de Lima se ancla a un cluster de sensores Madrid del mismo TIPO de
    corredor (vía expresa, arteria, acceso de autopista, avenida, vía periférica).
  - Se agrega por cluster → se traduce a coords de Lima → se salva en bronze.
  - La métrica clave es congestion_ratio = intensidad / intensidadSat, que es
    independiente de la ciudad y directamente comparable.

Niveles de servicio Madrid: 0=Fluido, 1=Lento, 2=Retenido, 3=Cortado, -1=Sin datos

Uso:
    python 01_ingesta/sensores_trafico.py
"""
import sys
from pathlib import Path
import xml.etree.ElementTree as ET

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import ZONAS_LIMA

import requests
from _bronze import guardar_bronze

API_MADRID = "https://informo.madrid.es/informo/tmadrid/pm.xml"

NIVEL_LABEL = {
    "0": "Fluido",
    "1": "Lento",
    "2": "Retenido",
    "3": "Cortado",
    "-1": "Sin datos",
}

# Mapeo: cada zona de Lima → cluster de sensores Madrid del mismo tipo de corredor.
# Seleccionados por función (vía expresa, arteria ppal, acceso autopista, etc.)
# y capacidad (intensidadSat), no por nombre geográfico.
MAPA_SENSORES = {
    "Via Expresa - Centro": {
        "ids": ["5423", "5520"],          # M-30 (anillo rápido urbano)
        "tipo_corredor": "via_expresa",
    },
    "Javier Prado - San Isidro": {
        "ids": ["5451", "5452", "5453"],  # Pº Castellana (arteria financiera)
        "tipo_corredor": "arteria_principal",
    },
    "Panamericana Norte - Independencia": {
        "ids": ["5429", "10463"],         # Nudo Norte A-1 (acceso norte)
        "tipo_corredor": "acceso_autopista",
    },
    "Carretera Central - Ate": {
        "ids": ["5486", "9991"],          # Acceso este A-2 (carretera este)
        "tipo_corredor": "acceso_autopista",
    },
    "Av. Brasil - Magdalena": {
        "ids": ["5641", "5011", "10572"], # Av. Portugal/Brasil (avenida resi/comercial)
        "tipo_corredor": "avenida",
    },
    "Costa Verde - Miraflores": {
        "ids": ["4823", "4824", "4822"],  # M-40/Av. Andalucía (periférica alta cap.)
        "tipo_corredor": "via_periferica",
    },
}

# índice coords Lima por nombre de zona (de config.py)
_COORDS_LIMA = {z["nombre"]: (z["lat"], z["lon"]) for z in ZONAS_LIMA}


def _parsear_sensor(pm):
    """Extrae campos numéricos de un elemento <pm> del XML de Madrid."""
    def _int(tag, default=0):
        val = pm.findtext(tag)
        try:
            return int(val)
        except (TypeError, ValueError):
            return default

    return {
        "id": pm.findtext("idelem"),
        "intensidad": _int("intensidad"),       # veh/hora
        "ocupacion": _int("ocupacion"),         # % ocupación detector
        "carga": _int("carga"),                 # % carga
        "isat": _int("intensidadSat"),          # capacidad máxima veh/hora
        "nivel": pm.findtext("nivelServicio", "-1"),
        "error": pm.findtext("error", "S"),
    }


def _agregar_cluster(sensores):
    """Promedia un cluster de sensores. Excluye los que tienen error o sin datos."""
    validos = [s for s in sensores if s["error"] == "N" and s["nivel"] != "-1"]
    if not validos:
        return None

    n = len(validos)
    intensidad_prom = sum(s["intensidad"] for s in validos) / n
    ocupacion_prom  = sum(s["ocupacion"]  for s in validos) / n
    carga_prom      = sum(s["carga"]      for s in validos) / n
    isat_prom       = sum(s["isat"]       for s in validos) / n

    congestion_ratio = round(intensidad_prom / isat_prom, 3) if isat_prom > 0 else 0.0

    # nivel dominante: el más restrictivo
    niveles = [int(s["nivel"]) for s in validos if s["nivel"].lstrip("-").isdigit()]
    nivel_dom = str(max(niveles)) if niveles else "-1"

    return {
        "intensidad_veh_hora": round(intensidad_prom),
        "ocupacion_pct": round(ocupacion_prom, 1),
        "carga_pct": round(carga_prom, 1),
        "capacidad_veh_hora": round(isat_prom),
        "congestion_ratio": congestion_ratio,      # 0-1, la métrica clave
        "nivel_servicio": nivel_dom,
        "nivel_label": NIVEL_LABEL.get(nivel_dom, "Desconocido"),
        "sensores_activos": n,
        "fuente_patron": "Madrid Open Data",
    }


def extraer():
    """Jala el XML de Madrid y devuelve una lista de dicts, uno por zona de Lima."""
    try:
        r = requests.get(API_MADRID, timeout=15)
        r.raise_for_status()
    except requests.RequestException as e:
        print(f"[sensores] error al contactar Madrid Open Data: {e}")
        return []

    root = ET.fromstring(r.content)

    # Indexar todos los sensores por ID para búsqueda O(1)
    indice = {}
    for pm in root.findall("pm"):
        sensor_id = pm.findtext("idelem")
        if sensor_id:
            indice[sensor_id] = _parsear_sensor(pm)

    registros = []
    for nombre_zona, config_zona in MAPA_SENSORES.items():
        sensores = [indice[sid] for sid in config_zona["ids"] if sid in indice]
        agregado = _agregar_cluster(sensores)
        if agregado is None:
            print(f"[sensores] zona '{nombre_zona}': todos los sensores con error, se omite.")
            continue

        coords = _COORDS_LIMA.get(nombre_zona, (None, None))
        registro = {
            "zona": nombre_zona,
            "lat": coords[0],
            "lon": coords[1],
            "tipo_corredor": config_zona["tipo_corredor"],
            **agregado,
            "ids_madrid": ",".join(config_zona["ids"]),
        }
        registros.append(registro)

    n_antes = len(registros)
    registros = [r for r in registros
                 if 0 <= r.get("congestion_ratio", -1) <= 1
                 and r.get("intensidad_veh_hora", -1) >= 0]
    if len(registros) < n_antes:
        print(f"[sensores] {n_antes - len(registros)} registros descartados por rango invalido")

    return registros


if __name__ == "__main__":
    registros = extraer()
    guardar_bronze(registros, "sensores_trafico")
