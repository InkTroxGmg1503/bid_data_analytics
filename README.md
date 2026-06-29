# Sistema de Optimización Dinámica del Tráfico Urbano con Datos en Tiempo Real

Proyecto de curso (ESAN — Big Data). Ingiere tráfico urbano de **6 fuentes heterogéneas**
cada ~5 min (*streaming falso* = micro-batch), las unifica en un **lakehouse medallion**,
predice congestión y recomienda rutas, y lo presenta en **PowerBI**.

> Filosofía: **simple, pero cumpliendo las 5 V's de Big Data.**

---

## 🧱 Arquitectura

```
Fuentes (6)  →  Ingesta (Python)  →  Orquestación (ADF / Prefect)  →  Lakehouse (ADLS Gen2 + Delta)
                                                                          bronze → silver → gold
                                                                                              ↓
                                                              Procesamiento (PySpark / spark.sql)
                                                                                              ↓
                                                              ML (congestión + rutas)  →  PowerBI
```

| Capa | Tecnología | Plan B |
|------|-----------|--------|
| Ingesta | Python | Azure Data Factory |
| Orquestación | Azure Data Factory | Prefect |
| Lakehouse | ADLS Gen2 + Databricks/Delta Lake | Local: Delta/Parquet |
| Ciencia de datos | Jupyter + PySpark | — |
| Modelos IA | scikit-learn / MLlib (+ RPA si sobra tiempo) | — |
| Visualización | PowerBI | Tableau |
| Query | spark.sql (pipelines), dbutils (consultas) | — |

## 📂 Estructura

| Carpeta | Contenido |
|---------|-----------|
| `00_docs/` | Diagrama de arquitectura, mapeo 5V, sílabo, diccionario de datos |
| `01_ingesta/` | Un generador por fuente (el *streaming falso*) |
| `02_orquestacion/` | Pipelines ADF (JSON) / flows Prefect / `scheduler.py` local |
| `03_lakehouse/` | `bronze/` (crudo) → `silver/` (limpio) → `gold/` (modelado) |
| `04_procesamiento/` | Notebooks PySpark + scripts bronze→silver→gold |
| `05_ml_models/` | Predicción de congestión + optimización de rutas |
| `06_visualizacion/` | PowerBI (.pbix) + datasets exportados de gold |
| `07_temp/` | Cache, logs, checkpoints |

## 🔢 Las 5 V's

| V | Dónde | Cómo |
|---|-------|------|
| **Volumen** | `01_ingesta` + `bronze` | 6 fuentes × cada 5 min, particionado por fecha/hora |
| **Velocidad** | `02_orquestacion` | Micro-batch cada 5 min (streaming falso) |
| **Variedad** | las 6 fuentes | Estructurado, semi-estructurado (JSON) y no estructurado (texto) |
| **Veracidad** | capa `silver` | Validación de esquema, dedup, sensores caídos/nulos, normalización |
| **Valor** | `gold` + `05_ml` + PowerBI | Predicción, ruta óptima, dashboard accionable |

## 🪜 Estrategia incremental (cuidando créditos de Azure)

1. **Fase 1 — Local:** todo en Python; lakehouse como carpetas locales (Delta/Parquet). End-to-end en la PC.
2. **Fase 2 — Nube:** mismas rutas → ADLS Gen2; procesamiento en Databricks; ADF orquesta.
3. **Fase 3 — Extras:** ML + PowerBI + (si sobra tiempo) RPA.

## 🚀 Cómo correr (Fase 1)

```bash
pip install -r requirements.txt
python 02_orquestacion/scheduler.py     # dispara la ingesta cada 5 min
```

## 📡 Fuentes de datos

| # | Fuente | Tipo | Script |
|---|--------|------|--------|
| 1 | Sensores de tráfico / flujo vehicular | Estructurado | `01_ingesta/sensores_trafico.py` |
| 2 | GPS (Google Maps / Waze) | Semi-estructurado | `01_ingesta/gps_rutas.py` |
| 3 | Cámaras / visión computacional (simulada) | Estructurado | `01_ingesta/vision_camaras.py` |
| 4 | Redes sociales (Twitter API) | No estructurado | `01_ingesta/redes_twitter.py` |
| 5 | Clima | Estructurado | `01_ingesta/clima.py` |
| 6 | Eventos no recurrentes (pasados/futuros) | Semi-estructurado | `01_ingesta/eventos.py` |
