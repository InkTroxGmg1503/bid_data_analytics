# Guía Power BI Desktop — Dashboard Tráfico Lima
## Paso a paso completo

Tiempo estimado: 45–60 minutos  
Archivos necesarios (todos en `06_visualizacion/`):
- `data/kpi_resumen.csv`
- `data/kpi_por_zona.csv`
- `data/kpi_serie_temporal.csv`
- `data/kpi_composicion.csv`
- `data/kpi_ml_resultados.csv`
- `tema_trafico_lima.json`
- `medidas_dax.txt`

---

## PASO 0 — Configuración inicial

1. Abre **Power BI Desktop**.
2. Menú **Vista** → **Temas** → **Buscar temas** → selecciona `tema_trafico_lima.json`.  
   Verás que el fondo, colores y tipografía cambian automáticamente.
3. Menú **Ver** → activa **Líneas de cuadrícula** y **Ajustar al cuadrícula** para alinear mejor.
4. Menú **Vista** → **Tamaño de página** → selecciona **16:9** (si no está ya).

---

## PASO 1 — Conectar los 5 CSVs

1. **Inicio → Obtener datos → Texto/CSV**.
2. Selecciona `kpi_resumen.csv` → **Cargar** (no Transform).
3. Repite para los 4 restantes: `kpi_por_zona`, `kpi_serie_temporal`, `kpi_composicion`, `kpi_ml_resultados`.
4. En el panel **Datos** (derecha) verás las 5 tablas. Verifica que aparezcan.

> Tip: si alguna columna numérica aparece como texto, ve a **Transformar datos** → selecciona la columna → **Tipo: Número decimal**.

---

## PASO 2 — Crear las medidas DAX

1. En el panel **Datos**, haz clic sobre la tabla `kpi_resumen`.
2. **Inicio → Nueva medida**.
3. Abre el archivo `medidas_dax.txt` y copia/pega cada bloque de medida.
4. Crea todas las medidas en la tabla que corresponde (el archivo indica en qué tabla va cada grupo).

> Las medidas aparecen con el ícono de calculadora (fx) en el panel de Datos.

---

## PASO 3 — Crear las 5 páginas

Clic derecho en la pestaña inferior → **Nueva página**. Repite hasta tener 5 páginas.  
Renombra haciendo doble clic en cada pestaña:
1. `Portada`
2. `Mapa Operativo`
3. `Tendencia Horaria`
4. `Composición Vehicular`
5. `Modelos ML`

---

## PÁGINA 1 — Portada ejecutiva

**Objetivo:** responder "¿cómo está Lima ahora?" de un vistazo.

### Franja de encabezado (arriba de todo)
- Inserta un **Rectángulo** (Insertar → Formas → Rectángulo).
- Tamaño: ancho completo, alto ~60 px.
- Relleno: color `#1A3C5E` (azul oscuro).
- Encima coloca un **Cuadro de texto**:
  - Texto: `Sistema de Optimización de Tráfico Urbano — Lima`
  - Fuente: Segoe UI, 18 pt, blanco, negrita.
  - A la derecha del título, otro Cuadro de texto más pequeño: `Última actualización: [fecha]`

### Fila de KPI cards (justo debajo del encabezado)
Inserta 4 **cards** uno al lado del otro horizontalmente.  
Para insertar: **Visualizaciones → Card (tarjeta)**.

| Card | Campo | Tabla |
|------|-------|-------|
| 1 | Medida `Velocidad Media` | kpi_resumen |
| 2 | Medida `Congestion Ratio` | kpi_resumen |
| 3 | Medida `Pct Alto` | kpi_resumen |
| 4 | Medida `Zona Critica` | kpi_resumen |

Para cada card:
- Panel **Formato** → **Etiqueta de categoría** → escribe el nombre manualmente (Velocidad Media, etc.)
- **Borde** → activar, radio 6.
- **Sombra** → activar.

### Semáforo global (centro izquierdo)
- Inserta un **Card** con la medida `Icono Semaforo`.
- Formato → **Valor de llamada** → color de fuente: **Valor de campo** → selecciona `Color Congestion`.
- Esto hace que el card cambie de color automáticamente (verde/amarillo/rojo).
- Ponlo bien visible, fuente 24 pt.

### Mini gráfico de tendencia (centro derecha)
- Inserta un **Gráfico de líneas**.
- Eje X: `hora_label` de `kpi_serie_temporal`.
- Valores: `congestion_ratio`.
- Hazlo pequeño (~400×180 px) a la derecha del semáforo.
- Título: "Congestión por hora".

### Slicer de zona (esquina superior derecha)
- Inserta un **Segmentador**.
- Campo: `zona` de `kpi_por_zona`.
- Formato → Estilo: **Lista** o **Menú desplegable**.

---

## PÁGINA 2 — Mapa operativo

**Objetivo:** "torre de control" — ver de inmediato qué zonas están en rojo.

### Mapa principal (ocupa ~70% del ancho)
- Visualizaciones → **Mapa** (el globo terráqueo básico de Power BI).
- Configura los campos:
  - **Latitud**: `lat` de `kpi_por_zona`
  - **Longitud**: `lon` de `kpi_por_zona`
  - **Tamaño de burbuja**: `congestion_ratio`
  - **Leyenda**: `nivel_dominante`
  - **Información sobre herramientas**: arrastra `zona`, `velocidad_kmh`, `congestion_ratio`, `nivel_dominante`
- Formato → **Colores de datos**: asigna manualmente:
  - alto → `#E74C3C` (rojo)
  - medio → `#F1C40F` (amarillo)
  - bajo → `#2ECC71` (verde)

### Panel lateral derecho
Columna derecha (~30% del ancho), de arriba a abajo:

**Card — Zona más crítica:**
- Medida `Zona Mas Congestionada`.
- Título: "Zona crítica ahora".

**Tabla — Top 3 zonas:**
- Visualizaciones → **Tabla**.
- Columnas: `zona`, `congestion_ratio`, `velocidad_kmh`, `nivel_dominante`.
- Ordena por `congestion_ratio` descendente.
- Formato → filas alternadas activado.
- Formato condicional en `congestion_ratio`: fondo por escala de colores (verde → rojo).

**Slicer de nivel:**
- Segmentador con `nivel_dominante`.
- Estilo: botones para seleccionar alto/medio/bajo.

---

## PÁGINA 3 — Tendencia horaria

**Objetivo:** mostrar cuándo ocurre la congestión (hora valle vs hora pico).

### Gráfico de línea doble (ocupa ~65% del ancho, centrado)
- Visualizaciones → **Gráfico de líneas**.
- Eje X: `hora_label` de `kpi_serie_temporal`.
- Eje Y (izquierdo): `velocidad_kmh`.
- Eje Y secundario: `congestion_ratio`.
- Colores: velocidad en `#00B4D8` (cyan), congestión en `#E74C3C` (rojo).
- Formato → activa **Marcadores** en ambas líneas.
- Título: "Velocidad vs Congestión por hora del día".

### Resaltar horas punta
- Con el gráfico seleccionado, ve a **Análisis** (ícono lupa en el panel de formato).
- **Línea constante** → agrega una en X = 8 (etiqueta: "Hora punta mañana") color #E74C3C.
- Agrega otra en X = 18 (etiqueta: "Hora punta tarde") color #E74C3C.

### Panel superior (fila de KPIs sobre el gráfico)
3 cards pequeños:

| Card | Medida | Título |
|------|--------|--------|
| 1 | `Hora Pico` | "Hora más congestionada" |
| 2 | `Congestion En Hora Pico` | "Congestión en hora pico" |
| 3 | `Variacion Velocidad %` | "Variación vs hora anterior" |

Para el card de Variación %:
- Formato → **Valor de llamada** → Formato: porcentaje con 1 decimal.

### Slicer
- Segmentador con `es_hora_punta` → sirve para filtrar solo horas punta (valor 1) o todo.

---

## PÁGINA 4 — Composición vehicular

**Objetivo:** demostrar la V de Variedad — qué mezcla de vehículos compone el tráfico.

### Gráfico de barras horizontales (izquierda, ~55% del ancho)
- Visualizaciones → **Gráfico de barras apiladas** (horizontal).
- Eje Y: `tipo_vehiculo` de `kpi_composicion`.
- Eje X: `porcentaje_norm`.
- Ordena por valor descendente: clic en los 3 puntos del visual → **Ordenar por → porcentaje_norm → Descendente**.
- Formato → **Etiquetas de datos** → activar, color blanco o contraste.
- Colores de datos: asigna un color distinto a cada tipo (paleta del tema).
- Título: "Distribución por tipo de vehículo (%)".

### Gráfico de dona (derecha, ~35% del ancho)
- Visualizaciones → **Gráfico de anillos**.
- Leyenda: `tipo_vehiculo`.
- Valores: `porcentaje_norm`.
- Formato → **Etiquetas de detalle** → "Categoría + porcentaje".
- Título: "Composición modal".

### Card central (entre los dos gráficos)
- Card con el tipo de vehículo dominante.
- Para obtenerlo, crea una medida rápida en `kpi_composicion`:
  ```
  Tipo Dominante =
  CALCULATE(
      SELECTEDVALUE(kpi_composicion[tipo_vehiculo]),
      TOPN(1, kpi_composicion, kpi_composicion[porcentaje_norm], DESC)
  )
  ```
- Título del card: "Tipo dominante".

### Cuadro de texto interpretativo (panel derecho o debajo)
- Insertar → **Cuadro de texto**.
- Escribe 2-3 líneas de interpretación, por ejemplo:
  > "El tráfico de Lima está dominado por autos y taxis (XX%), seguido de combis y minibuses (XX%). Esta mezcla explica la alta densidad en corredores como Javier Prado y Via Expresa."
- Fondo blanco, borde sutil, fuente 11 pt Segoe UI.

---

## PÁGINA 5 — Modelos ML

**Objetivo:** demostrar la V de Valor — los 9 modelos comparados, el mejor resaltado.

### Card del mejor modelo (arriba centrado)
- Card con medida `Mejor Modelo`.
- Título: "Mejor modelo por F1-score".
- Segundo card al lado: medida `Mejor F1`.

### Gráfico de barras agrupadas (centro)
- Visualizaciones → **Gráfico de barras agrupadas**.
- Eje Y: `Modelo` de `kpi_ml_resultados`.
- Eje X: `F1_score`, `Accuracy` (arrastra ambos a Valores).
- Ordena por `F1_score` descendente.
- Formato → colores: F1 en `#00B4D8`, Accuracy en `#1A3C5E`.
- Título: "Comparativa de métricas por modelo".

### Tabla completa (abajo del gráfico)
- Visualizaciones → **Tabla**.
- Columnas: `Modelo`, `Tipo`, `Accuracy`, `F1_score`, `Precision`, `Recall`.
- Formato condicional en `F1_score`:
  - Clic en la flecha de la columna `F1_score` → **Formato condicional → Color de fondo**.
  - Selecciona **Formatear por: Escalas de colores**.
  - Mínimo: `#E74C3C` (rojo), Medio: `#F1C40F` (amarillo), Máximo: `#2ECC71` (verde).
- Activa **Totales** si lo deseas (mostrará el promedio).

### Nota de conclusión (cuadro de texto, abajo)
- Texto ejemplo:
  > "El modelo MLP alcanza el mejor F1-score (0.8897), demostrando la capacidad predictiva del pipeline. El pipeline Bronze→Silver→Gold garantiza Veracidad en los datos de entrenamiento."

---

## PASO 4 — Navegación entre páginas (botones)

En **cada página**, agrega botones de navegación:

1. **Insertar → Botones → En blanco**.
2. Crea 5 botones pequeños, uno por página.
3. En cada botón: Panel **Formato → Acción → Tipo: Navegación de página → selecciona la página destino**.
4. Etiqueta los botones: `Portada` | `Mapa` | `Tendencia` | `Flota` | `ML`.
5. Alinea los 5 botones horizontalmente en el encabezado de cada página.
6. Copia el grupo de botones y pégalo en las demás páginas (Ctrl+C / Ctrl+V).

---

## PASO 5 — Guardar y actualizar

1. **Archivo → Guardar como** → nombre: `dashboard_trafico_lima.pbix`.
2. Guárdalo en `06_visualizacion/`.
3. Para actualizar datos: vuelve a correr `python export_powerbi.py` y luego en Power BI Desktop presiona **Inicio → Actualizar**.

---

## Paleta de colores de referencia

| Color | HEX | Uso |
|-------|-----|-----|
| Azul oscuro | `#1A3C5E` | Encabezados, fondos de tabla |
| Cyan eléctrico | `#00B4D8` | Líneas, barras principales |
| Verde | `#2ECC71` | Nivel BAJO / estado FLUIDO |
| Amarillo | `#F1C40F` | Nivel MEDIO / estado MODERADO |
| Rojo | `#E74C3C` | Nivel ALTO / estado CRÍTICO |
| Gris claro | `#F0F4F8` | Fondo de página |
| Texto | `#2C3E50` | Texto general |
