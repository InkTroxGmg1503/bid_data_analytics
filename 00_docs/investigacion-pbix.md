<img src="https://r2cdn.perplexity.ai/pplx-full-logo-primary-dark%402x.png" style="height:64px;margin-right:32px"/>

# Estoy desarrollando un dashboard profesional en Power BI Desktop (.pbix) para un sistema de optimización de tráfico urbano en Lima, Perú. Los datos vienen de una arquitectura medallion lakehouse (Bronze → Silver → Gold) en Azure ADLS Gen2, procesados con PySpark/Databricks.

Tengo 5 tablas CSV ya preparadas:
kpi_resumen.csv — 8 KPIs escalares (velocidad media, ratio de congestión, % por nivel)
kpi_por_zona.csv — métricas agregadas por 6 zonas de Lima con lat/lon
kpi_serie_temporal.csv — evolución horaria (0-23h) de velocidad y congestión
kpi_composicion.csv — % de 6 tipos de vehículos (auto, bus, moto, etc.)
kpi_ml_resultados.csv — comparativa de 9 modelos de clasificación PySpark ML con Accuracy, F1, Precision y Recall
Necesito saber:
¿Cuál es la estructura profesional de páginas para un dashboard de tráfico en tiempo real? (layout, cantidad de páginas, jerarquía de información)
¿Cómo se crean KPIs con semáforo de colores (verde/amarillo/rojo) para niveles de congestión usando formato condicional?
¿Cómo usar el visual de Mapa con coordenadas lat/lon para mostrar zonas críticas con burbujas de calor?
¿Cómo configurar actualización automática del dataset desde archivos CSV locales para simular tiempo real?
¿Qué mejores prácticas de diseño (tema, tipografía, colores) se usan en dashboards de tráfico/smart city a nivel profesional?
¿Cómo crear medidas DAX para calcular variación porcentual entre la hora actual y la hora pico anterior?
El objetivo es un dashboard ejecutivo que demuestre las 5 V's del Big Data (Volumen, Velocidad, Variedad, Veracidad, Valor) para una presentación universitaria.

Sí: para tu caso conviene un dashboard **ejecutivo de 4 a 5 páginas**, con una portada de KPI y luego páginas analíticas por tiempo, zonas, composición vehicular y ML. Para tráfico urbano en tiempo real, la jerarquía ideal es: visión general, alertas críticas, análisis espacial, evolución temporal, y soporte técnico/modelos.[^1][^2]

## Estructura profesional

Una estructura sólida para Power BI Desktop sería esta:

1. **Portada ejecutiva.** KPIs escalarizados, estado general de congestión, semáforo global, velocidad media, ratio de congestión, y % de niveles.
2. **Mapa operativo.** Zonas de Lima con burbujas/colores por criticidad, filtro por zona y lectura geográfica rápida.
3. **Tendencia horaria.** Serie 0–23h con velocidad y congestión, para identificar hora valle y hora pico.
4. **Flota y composición.** Distribución de tipos de vehículos para explicar presión vial y mezcla modal.
5. **Modelos ML.** Comparativa de Accuracy, F1, Precision y Recall para justificar valor analítico y veracidad del pipeline.

Si tu audiencia es muy ejecutiva, puedes compactarlo a **4 páginas** y dejar ML como página final opcional o anexo. La regla práctica es: una página = una pregunta de negocio.

## KPIs con semáforo

La forma más limpia es crear una medida DAX que devuelva un estado o color según el nivel de congestión, y luego usar **formato condicional** en la tarjeta, tabla o matriz. En Power BI, el patrón habitual usa `SWITCH(TRUE())` para definir umbrales y luego asigna color por regla o por valor de campo.[^3][^4]

Ejemplo de medida:

```DAX
Semaforo Congestion =
SWITCH(
    TRUE(),
    [Ratio Congestion] < 0.40, "Verde",
    [Ratio Congestion] < 0.70, "Amarillo",
    "Rojo"
)
```

Y otra medida para color HEX:

```DAX
Color Congestion =
SWITCH(
    [Semaforo Congestion],
    "Verde", "#2ECC71",
    "Amarillo", "#F1C40F",
    "Rojo", "#E74C3C"
)
```

Luego en la visual: **Formato condicional → Color de fondo o color de fuente → Formatear por: Valor de campo → `Color Congestion`**. El patrón también funciona bien con un círculo `UNICHAR(9679)` en una tabla para simular el semáforo.[^5][^6]

## Mapa con lat/lon

Para mostrar zonas críticas, usa el visual de **Map** o mejor **Azure Maps** si quieres más control geoespacial; con latitud y longitud puedes ubicar puntos exactos sin depender de nombres de distrito. En este tipo de mapa puedes ajustar tamaño de burbuja y activar modo de calor para resaltar concentración de congestión.[^7][^1]

Recomendación práctica:

- **Latitude** → campo latitud.
- **Longitude** → campo longitud.
- **Size** → `Ratio Congestion` o volumen vehicular.
- **Tooltips** → zona, velocidad media, nivel de congestión, hora pico.
- **Color** → severidad o nivel de congestión.

Si quieres que el mapa “cuente una historia”, usa burbujas para comparar zonas y heatmap para mostrar intensidad agregada; eso ayuda mucho en una presentación universitaria. El visual de shape map no sirve bien para lat/lon porque está pensado para áreas, no puntos.[^8][^1]

## Actualización automática

Si los CSV son locales, Power BI Desktop por sí solo no te da una actualización programada real; para eso debes **publicar al servicio** y usar **On-premises Data Gateway** o mover los archivos a OneDrive/SharePoint para refresco directo en la nube. En fuentes locales, el gateway exige que coincidan ruta, servidor o carpeta y credenciales Windows válidas.[^2][^9][^10]

Opciones recomendadas:

- **Opción profesional:** publicar el `.pbix`, configurar gateway, y programar refresh.
- **Opción más simple:** guardar los CSV en OneDrive/SharePoint y conectar desde ahí.
- **Opción de simulación local:** mantener Power BI Desktop y refrescar manualmente o con automatización externa, pero no es refresh “en tiempo real” nativo.

Para una demo universitaria, la mejor narrativa es: “carga incremental desde Bronze/Silver/Gold, publicación al servicio y refresco programado”. Eso conecta bien con tu arquitectura lakehouse.[^10][^2]

## Diseño profesional

En dashboards de tráfico y smart city funciona muy bien una estética sobria, tecnológica y de alta legibilidad: fondo claro o gris muy suave, una sola familia tipográfica sans-serif, y colores reservados para estado/alerta. La tipografía debe priorizar lectura rápida; evita fuentes decorativas y usa tamaños consistentes para títulos, subtítulos y métricas.[^11]

Buenas prácticas:

- **Paleta:** verde, ámbar y rojo solo para estado; azules o grises para el resto.
- **Fondo:** blanco, gris claro o azul muy tenue.
- **Tipografía:** una sola familia, con jerarquía clara.
- **Espaciado:** bloques alineados y aire suficiente entre visuales.
- **Accento visual:** resalta solo lo crítico; no todo debe competir.

Para tráfico urbano, suele funcionar un estilo “control room”: limpio, técnico, con indicadores grandes y mapa dominante. Así el dashboard transmite velocidad, control y confiabilidad.[^12][^11]

## Medidas DAX útiles

Para variación porcentual entre la hora actual y la hora pico anterior, necesitas una medida que identifique la hora de referencia y otra que compare contra la hora previa relevante. Si tu tabla temporal está por hora, puedes usar `MAX('Tabla'[Hora])` o una selección de contexto con `SELECTEDVALUE`.

Ejemplo base:

```DAX
Velocidad Actual = 
CALCULATE(
    AVERAGE(kpi_serie_temporal[velocidad]),
    kpi_serie_temporal[hora] = MAX(kpi_serie_temporal[hora])
)
```

```DAX
Velocidad Hora Anterior =
CALCULATE(
    AVERAGE(kpi_serie_temporal[velocidad]),
    kpi_serie_temporal[hora] = MAX(kpi_serie_temporal[hora]) - 1
)
```

```DAX
Variacion % =
DIVIDE(
    [Velocidad Actual] - [Velocidad Hora Anterior],
    [Velocidad Hora Anterior]
)
```

Si por “hora pico anterior” te refieres al máximo anterior dentro del mismo día, entonces conviene una medida con `TOPN` o una tabla auxiliar de horas ordenadas. Esa variante es mejor para análisis ejecutivo porque compara contra el último pico real, no solo contra la hora inmediatamente anterior.

## Valor para tu caso

Con tus 5 CSV ya tienes una historia muy completa para las 5 V’s del Big Data: **Volumen** en agregados y series, **Velocidad** en actualización horaria, **Variedad** en zonas, vehículos y ML, **Veracidad** en el pipeline Bronze/Silver/Gold, y **Valor** en los KPIs ejecutivos y la predicción. El diseño debe dejar eso visible sin saturar: una portada fuerte, un mapa convincente, una serie temporal clara y una página final con modelos.[^12][^2]

Si quieres, puedo proponerte la **estructura exacta de páginas con nombres, visuales por página, medidas DAX y esquema de colores** lista para construir en Power BI.
<span style="display:none">[^13][^14][^15][^16][^17][^18][^19][^20][^21][^22][^23][^24][^25][^26][^27][^28][^29]</span>

<div align="center">⁂</div>

[^1]: https://www.youtube.com/watch?v=m2Rov1x3mIE

[^2]: https://learn.microsoft.com/en-us/power-bi/connect-data/service-gateway-enterprise-manage-scheduled-refresh

[^3]: https://stackoverflow.com/questions/57202628/traffic-lights-in-power-bi-which-work-also-for-totals

[^4]: https://www.nemoudar.com/blog/traffic-light-custom-visual/

[^5]: https://www.youtube.com/watch?v=fcv3CHBItJo

[^6]: https://www.youtube.com/watch?v=7aDdOK_k-Pc

[^7]: https://www.youtube.com/watch?v=SIU4mZ9ZyTo

[^8]: https://community.powerbi.com/t5/Desktop/Shape-Map-with-Bubbles/td-p/1338803

[^9]: https://community.fabric.microsoft.com/t5/Service/Read-csv-from-network-location-in-service/m-p/1189095/highlight/true

[^10]: https://community.fabric.microsoft.com/t5/Power-Query/on-premise-csv-file-gt-gateway-service-gt-dataset-in-powerbi/td-p/153914

[^11]: https://www.fastercapital.com/content/Font-Styles--Font-Finesse--Selecting-the-Right-Typography-for-Power-BI-Themes.html

[^12]: https://www.epcgroup.net/power-bi-kpi-visuals-dashboard-guide-2026

[^13]: https://www.youtube.com/watch?v=LFMn3wnruNc

[^14]: https://www.linkedin.com/posts/mayilsamy-murugesan-18513620a_power-bi-scheduled-refresh-with-local-activity-7384302514256592896-HoPu

[^15]: https://ramvudugula9.medium.com/power-bi-disable-power-query-background-refresh-for-local-data-source-df77a96748c0

[^16]: https://forum.enterprisedna.co/t/only-refresh-new-files-from-folder-in-power-query/27667

[^17]: https://www.scribd.com/document/1019650179/Automating-Daily-Power-BI-Report-Refresh

[^18]: https://community.fabric.microsoft.com/t5/Desktop/How-to-schedule-local-data-refresh/td-p/3435116

[^19]: https://community.fabric.microsoft.com/t5/Power-Query/Automatic-dataset-refresh-from-Excel-data-source-on-Sharepoint/td-p/638937

[^20]: https://community.fabric.microsoft.com/t5/Desktop/How-to-auto-update-the-data-from-an-CSV-file-in-Power-BI-Desktop/td-p/1350922

[^21]: https://www.thebricks.com/resources/guide-how-to-set-power-bi-to-automatically-refresh

[^22]: https://www.reddit.com/r/PowerBI/comments/1lgvxne/automate_csv_file_refresh_in_power_bi/

[^23]: https://www.youtube.com/watch?v=KRZDDOLyTKw

[^24]: https://www.datacamp.com/ko/tutorial/power-bi-kpi

[^25]: https://community.fabric.microsoft.com/t5/Desktop/Traffic-light-KPI-with-an-threshold-input/td-p/1402267

[^26]: http://bsachamber.com/ckeditor/uploads/files/pikode_zekijopiludarin.pdf

[^27]: https://www.youtube.com/watch?v=mfdn26xyGKI

[^28]: https://community.fabric.microsoft.com/t5/Desktop/Upload-Static-CSV-and-do-not-Refresh/m-p/250400

[^29]: https://www.youtube.com/watch?v=ojFx23WQBCY

