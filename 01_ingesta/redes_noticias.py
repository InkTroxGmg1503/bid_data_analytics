"""
Fuente #6 — NOTICIAS / SOCIAL (no estructurado → estructurado tras scoring).

Dos feeds RSS reales de Lima sin API key:
  - RPP Noticias   https://rpp.pe/rss           (noticias generales Perú, ~60 items)
  - Andina         https://andina.pe/agencia/rss.aspx  (agencia oficial Estado)

Estrategia: se ingesta TODO (no solo noticias de tráfico).
  - Se calcula un relevance_score (0-1) por coincidencia de keywords de tráfico.
  - Un sentiment_flag aproxima el tono (incidente negativo vs. mejora/obra positiva).
  - Bronze guarda el lote completo. Silver filtrará relevance_score > 0.
  - La ausencia de noticias de tráfico también es un dato (día sin incidentes).

Uso:
    python 01_ingesta/redes_noticias.py
"""
import sys
from pathlib import Path
import re
import hashlib
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import requests
from _bronze import guardar_bronze

FEEDS = {
    "rpp":    "https://rpp.pe/rss",
    "andina": "https://andina.pe/agencia/rss.aspx",
}

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; TraficoLimaBot/1.0)"}

# Keywords de tráfico en español (para relevance_score)
KW_TRAFICO = [
    "tráfico", "trafico", "tránsito", "transito", "congestión", "congestion",
    "accidente", "choque", "colisión", "colision", "atropello",
    "vía", "via", "avenida", "carretera", "autopista", "pista",
    "desvío", "desvio", "cierre vial", "obra vial", "bloqueo",
    "manifestación", "manifestacion", "marcha", "protesta", "paro",
    "panamericana", "javier prado", "vía expresa", "via expresa",
    "costa verde", "av. brasil", "carretera central",
]

# Keywords de sentimiento negativo (incidente) vs positivo (mejora)
KW_NEGATIVO = ["accidente", "choque", "colisión", "atropello", "bloqueo",
               "congestión", "congestion", "cierre", "interrupción", "caos"]
KW_POSITIVO = ["mejora", "habilitó", "apertura", "fluidez", "solución",
               "nuevo acceso", "ampliación"]


def _score_relevancia(texto):
    """Calcula relevance_score 0-1 según densidad de keywords de tráfico."""
    texto_lower = texto.lower()
    hits = sum(1 for kw in KW_TRAFICO if kw in texto_lower)
    return round(min(hits / 3, 1.0), 2)   # 3 keywords = score 1.0


def _sentiment(texto):
    texto_lower = texto.lower()
    neg = sum(1 for kw in KW_NEGATIVO if kw in texto_lower)
    pos = sum(1 for kw in KW_POSITIVO if kw in texto_lower)
    if neg > pos:
        return "negativo"
    if pos > neg:
        return "positivo"
    return "neutro"


def _parsear_feed_rss(texto, fuente):
    """Extrae items de un feed RSS/Atom con o sin CDATA."""
    # Soporte RSS 2.0 (RPP) y Atom (Andina)
    titles  = re.findall(r'<title[^>]*>(?:<!\[CDATA\[)?\s*(.*?)\s*(?:\]\]>)?</title>', texto, re.DOTALL)
    descs   = re.findall(r'<description[^>]*>(?:<!\[CDATA\[)?\s*(.*?)\s*(?:\]\]>)?</description>', texto, re.DOTALL)
    links   = re.findall(r'<link[^>]*>(?:<!\[CDATA\[)?\s*(https?://[^\s<]+)\s*(?:\]\]>)?</link>', texto)
    fechas  = re.findall(r'<(?:pubDate|published|updated)>(.*?)</(?:pubDate|published|updated)>', texto)

    items = []
    # el primer title/description suele ser el canal; los saltamos
    for i, title in enumerate(titles[1:], start=1):
        title = re.sub(r'<[^>]+>', '', title).strip()
        if not title:
            continue
        desc  = re.sub(r'<[^>]+>', '', descs[i] if i < len(descs) else "").strip()[:300]
        link  = links[i] if i < len(links) else ""
        fecha = fechas[i - 1] if (i - 1) < len(fechas) else ""
        texto_completo = f"{title} {desc}"

        items.append({
            "fuente":           fuente,
            "item_id":          hashlib.md5(link.encode()).hexdigest()[:12] if link else hashlib.md5(title.encode()).hexdigest()[:12],
            "titulo":           title[:200],
            "descripcion":      desc,
            "url":              link,
            "fecha_publicacion": fecha[:50],
            "relevance_score":  _score_relevancia(texto_completo),
            "sentiment":        _sentiment(texto_completo),
            "es_trafico":       _score_relevancia(texto_completo) > 0,
        })
    return items


def extraer():
    registros = []
    for fuente, url in FEEDS.items():
        try:
            r = requests.get(url, timeout=15, headers=HEADERS)
            r.raise_for_status()
        except requests.RequestException as e:
            print(f"[noticias] error en {fuente}: {e}")
            continue

        items = _parsear_feed_rss(r.text, fuente)
        print(f"[noticias] {fuente}: {len(items)} items ({sum(1 for i in items if i['es_trafico'])} de tráfico)")
        registros.extend(items)

    return registros


if __name__ == "__main__":
    registros = extraer()
    guardar_bronze(registros, "redes_noticias")
