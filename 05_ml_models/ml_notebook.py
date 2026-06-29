# Databricks notebook — Capa ML
# Sistema de Optimización Dinámica del Tráfico Urbano — Lima
#
# Problema: Clasificación multiclase
# Target:   nivel_congestion  →  bajo / medio / alto

# COMMAND ----------

# %pip install adlfs fsspec pyarrow pandas numpy matplotlib seaborn
# Ejecutar solo la primera vez

# COMMAND ----------

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import adlfs

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, lit
from pyspark.sql.types import StringType

from pyspark.ml.stat import Correlation
from pyspark.ml.feature import (StringIndexer, OneHotEncoder,
                                 VectorAssembler, StandardScaler)
from pyspark.ml import Pipeline
from pyspark.ml.feature import UnivariateFeatureSelector

from pyspark.ml.classification import (
    DecisionTreeClassifier, RandomForestClassifier,
    MultilayerPerceptronClassifier, LogisticRegression,
    GBTClassifier,
)
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

spark = SparkSession.builder.appName("trafico_lima_ml").getOrCreate()
print("Spark version:", spark.version)

ADLS_ACCOUNT   = "traficolima"
ADLS_KEY       = ""          # <-- pega tu ADLS_KEY aquí
ADLS_CONTAINER = "trafico-lima"
SO = {"account_name": ADLS_ACCOUNT, "account_key": ADLS_KEY}

assert ADLS_KEY, "ERROR: pega tu ADLS_KEY antes de continuar"

# COMMAND ----------

# ── Funciones (misma estructura que el ejemplo del curso) ─────────────────────

def label_indexed(df: DataFrame, col_label: str):
    indexer = StringIndexer(inputCol=col_label, outputCol=f"{col_label}_idx",
                            handleInvalid="keep")
    modelo  = indexer.fit(df)
    df_out  = (modelo.transform(df)
               .withColumnRenamed(col_label,           f"{col_label}_cat")
               .withColumnRenamed(f"{col_label}_idx",  col_label))
    return df_out, modelo.labels


def division_columnas(df: DataFrame, col_label: str):
    """Detecta tipos desde el schema de Spark — igual que en el ejemplo."""
    cols_cat = [c.name for c in df.schema
                if c.dataType == StringType()
                and c.name != col_label
                and c.name != f"{col_label}_cat"]

    cols_num = [c.name for c in df.schema
                if c.dataType.simpleString() in
                ("int", "bigint", "double", "float", "decimal", "long", "short")
                and c.name != col_label
                and c.name != f"{col_label}_cat"]

    return cols_cat, cols_num


def get_multicolinealidad(df: DataFrame, cols_nums: list, umbral: float = 0.95):
    if len(cols_nums) < 2:
        import numpy as np
        return cols_nums, [], np.array([[1.0]])

    assembler = VectorAssembler(inputCols=cols_nums, outputCol="num_vec",
                                handleInvalid="skip")
    df_num    = assembler.transform(df).select("num_vec")
    corr_mat  = Correlation.corr(df_num, "num_vec", "pearson").head()[0].toArray()

    to_drop = set()
    for i in range(len(cols_nums)):
        for j in range(i + 1, len(cols_nums)):
            if abs(corr_mat[i, j]) > umbral:
                to_drop.add(cols_nums[j])

    cols_filtradas = [c for c in cols_nums if c not in to_drop]

    # Con pocos datos las correlaciones son muy ruidosas — si quedaran < 2 columnas
    # devolvemos la lista original para no dejar ANOVA sin features.
    if len(cols_filtradas) < 2:
        print(f"[!] Pearson dejó {len(cols_filtradas)} columna(s) con umbral {umbral}. "
              f"Se usa la lista original ({len(cols_nums)} cols).")
        return cols_nums, [], corr_mat

    return cols_filtradas, sorted(list(to_drop)), corr_mat


def get_cols_selected(df: DataFrame, featureTypeCat: bool, cols: list,
                      label_col: str, threshold: float = 0.05, mode: str = "fpr"):
    if featureTypeCat:
        indexers = [StringIndexer(inputCol=c, outputCol=f"{c}_idx",
                                  handleInvalid="keep") for c in cols]
        for ix in indexers:
            df = ix.fit(df).transform(df)
        cols_vec  = [f"{c}_idx" for c in cols]
        feat_type = "categorical"
    else:
        cols_vec  = cols
        feat_type = "continuous"

    assembler = VectorAssembler(inputCols=cols_vec, outputCol="vec",
                                handleInvalid="skip")
    df_a      = assembler.transform(df)
    selector  = UnivariateFeatureSelector(
        featuresCol="vec", labelCol=label_col,
        selectionMode=mode, outputCol="selectedFeatures"
    )
    selector.setFeatureType(feat_type).setLabelType("categorical").setSelectionThreshold(threshold)
    model = selector.fit(df_a)
    return [cols[i] for i in model.selectedFeatures]

# COMMAND ----------

# ── 1. LEER GOLD ──────────────────────────────────────────────────────────────

TARGET    = "nivel_congestion"
ruta_gold = f"abfs://{ADLS_CONTAINER}/gold/congestion_por_zona/datos.parquet"

df_pd = pd.read_parquet(ruta_gold, storage_options=SO)
df_pd = df_pd.dropna(subset=[TARGET])

print(f"Gold: {len(df_pd)} filas x {len(df_pd.columns)} columnas")
print(f"\nDistribución target:")
print(df_pd[TARGET].value_counts().to_string())

df = spark.createDataFrame(df_pd)
print(f"\nSpark DataFrame listo.")
df.printSchema()

# COMMAND ----------

# ── 2. EXCLUIR COLUMNAS NO APTAS PARA ML ──────────────────────────────────────

# Solo definimos lo que NO debe entrar y por qué —
# division_columnas() detecta el resto automáticamente del schema.
EXCLUIR = [
    # Data leakage: son la base directa del target
    "congestion_factor",
    "indice_congestion",
    "tendencia_congestion",
    "duracion_trafico_s",
    # Identificadores: no son predictores
    "fecha",
]

df_clean = df.drop(*EXCLUIR)
print(f"Columnas tras exclusión: {len(df_clean.columns)}  (excluidas: {len(EXCLUIR)})")
print(f"Excluidas: {EXCLUIR}")

# COMMAND ----------

# ── 3. INDEXAR TARGET ─────────────────────────────────────────────────────────

df_clean, labels_target = label_indexed(df_clean, TARGET)

print("Mapping del target:")
for i, lbl in enumerate(labels_target):
    print(f"  {lbl} → {i:.0f}")

# COMMAND ----------

# ── 4. SEPARACIÓN AUTOMÁTICA POR TIPO DE DATOS ───────────────────────────────

cols_cat, cols_num = division_columnas(df_clean, TARGET)

print(f"Columnas CATEGÓRICAS detectadas del schema ({len(cols_cat)}):")
for c in cols_cat:
    print(f"  {c}")

print(f"\nColumnas NUMÉRICAS detectadas del schema ({len(cols_num)}):")
for c in cols_num:
    print(f"  {c}")

# COMMAND ----------

# ── 5. MULTICOLINEALIDAD — CORRELACIÓN DE PEARSON ────────────────────────────

cols_num_filtradas, cols_drop, corr_mat = get_multicolinealidad(df_clean, cols_num)

print(f"Numéricas originales:             {len(cols_num)}")
print(f"Eliminadas por multicolinealidad: {len(cols_drop)}  →  {cols_drop}")
print(f"Numéricas retenidas:              {len(cols_num_filtradas)}")

# COMMAND ----------

top = cols_num_filtradas[:15]
idx = [cols_num.index(c) for c in top if c in cols_num]
sub = corr_mat[np.ix_(idx, idx)]

plt.figure(figsize=(12, 10))
sns.heatmap(sub, annot=True, fmt=".2f", cmap="coolwarm",
            xticklabels=top, yticklabels=top, center=0, vmin=-1, vmax=1)
plt.title("Correlación de Pearson — Features numéricas")
plt.tight_layout()
plt.show()

# COMMAND ----------

# ── 6. SELECCIÓN — ANOVA Y CHI² ───────────────────────────────────────────────

if len(cols_num_filtradas) >= 2:
    cols_num_sel = get_cols_selected(df_clean, False, cols_num_filtradas, TARGET)
else:
    cols_num_sel = cols_num_filtradas
    print("[!] Muy pocas columnas numéricas para ANOVA — se usan todas las retenidas.")

print(f"Numéricas seleccionadas por ANOVA (p<0.05): {len(cols_num_sel)}")
print(f"  {cols_num_sel}")

if len(cols_cat) >= 1:
    cols_cat_sel = get_cols_selected(df_clean, True, cols_cat, TARGET)
else:
    cols_cat_sel = cols_cat
    print("[!] Sin columnas categóricas para Chi².")

print(f"\nCategóricas seleccionadas por Chi² (p<0.05): {len(cols_cat_sel)}")
print(f"  {cols_cat_sel}")

# COMMAND ----------

# ── 7. FEATURE ENGINEERING — PIPELINE ────────────────────────────────────────

cat_indexers    = [StringIndexer(inputCol=c, outputCol=f"{c}_idx",
                                  handleInvalid="keep") for c in cols_cat_sel]

num_assembler   = VectorAssembler(inputCols=cols_num_sel, outputCol="num_features",
                                   handleInvalid="skip")
scaler          = StandardScaler(inputCol="num_features", outputCol="num_scaled",
                                  withMean=True, withStd=True)

if cols_cat_sel:
    encoder         = OneHotEncoder(inputCols  = [f"{c}_idx" for c in cols_cat_sel],
                                     outputCols = [f"{c}_vec" for c in cols_cat_sel])
    cat_assembler   = VectorAssembler(inputCols=[f"{c}_vec" for c in cols_cat_sel],
                                       outputCol="cat_features")
    final_assembler = VectorAssembler(inputCols=["num_scaled", "cat_features"],
                                       outputCol="features_scaled")
    stages_cat = [encoder, cat_assembler]
else:
    print("[!] Sin categóricas — features_scaled usará solo numéricas.")
    final_assembler = VectorAssembler(inputCols=["num_scaled"],
                                       outputCol="features_scaled")
    stages_cat = []

pipeline_prep = Pipeline(stages=cat_indexers + [num_assembler, scaler]
                                + stages_cat + [final_assembler])
model_prep = pipeline_prep.fit(df_clean)
df_prep    = model_prep.transform(df_clean)

n_input = len(df_prep.select("features_scaled").first()[0])
print(f"Vector final: {n_input} dimensiones")
df_prep.select(TARGET, "features_scaled").show(5, truncate=False)

# COMMAND ----------

# ── 8. SPLIT TRAIN / TEST ─────────────────────────────────────────────────────

train_df, test_df = df_prep.randomSplit([0.80, 0.20], seed=700)
print(f"Train: {train_df.count()} filas  |  Test: {test_df.count()} filas")
train_df.groupBy(TARGET).count().orderBy("count", ascending=False).show()

# COMMAND ----------

# ── 9. DEFINIR MODELOS ────────────────────────────────────────────────────────

N_CLASES = len(labels_target)
feats    = "features_scaled"
etiqueta = TARGET

dt  = DecisionTreeClassifier(featuresCol=feats, labelCol=etiqueta, maxDepth=5, seed=700)
rf  = RandomForestClassifier(featuresCol=feats, labelCol=etiqueta, numTrees=100, seed=700)
mlp = MultilayerPerceptronClassifier(featuresCol=feats, labelCol=etiqueta,
                                      layers=[n_input, 16, 8, N_CLASES], seed=700)
lr  = LogisticRegression(featuresCol=feats, labelCol=etiqueta,
                          family="multinomial", maxIter=100)
gbt = GBTClassifier(featuresCol=feats, labelCol=etiqueta,
                    maxIter=50, maxDepth=5, seed=700)

# Excluidos por limitaciones de Databricks serverless + Unity Catalog:
#   NaiveBayes    → UC bloquea higher-order functions internas
#   LinearSVC     → binario, necesita OneVsRest
#   FMClassifier  → binario, necesita OneVsRest
#   OneVsRest     → Delta temp tables no soportado en serverless

MODELOS = [
    ("DecisionTree",       dt),
    ("RandomForest",       rf),
    ("MLP",                mlp),
    ("LogisticRegression", lr),
    ("GBT",                gbt),
]
print(f"Modelos: {len(MODELOS)}  |  Clases: {N_CLASES}  |  Input dim: {n_input}")

# COMMAND ----------

# ── 10. ENTRENAMIENTO Y MÉTRICAS ──────────────────────────────────────────────

eval_mc  = MulticlassClassificationEvaluator(labelCol=etiqueta, predictionCol="prediction")
metricas = [("accuracy","Accuracy"), ("f1","F1-score"),
            ("weightedPrecision","Precision"), ("weightedRecall","Recall")]

print(f"{'Modelo':22s} | Accuracy | F1-score | Precision | Recall")
print("-" * 65)

predicciones = {}
resultados   = []

for nombre, modelo in MODELOS:
    try:
        pred = modelo.fit(train_df).transform(test_df)
        predicciones[nombre] = pred
        fila = {"Modelo": nombre}
        for key, label in metricas:
            fila[label] = round(eval_mc.setMetricName(key).evaluate(pred), 4)
        resultados.append(fila)
        print(f"{nombre:22s} | {fila['Accuracy']:.4f}   | {fila['F1-score']:.4f}   | "
              f"{fila['Precision']:.4f}    | {fila['Recall']:.4f}")
    except Exception as e:
        print(f"{nombre:22s} | ERROR: {e}")
        predicciones[nombre] = None

display(pd.DataFrame(resultados).set_index("Modelo"))

# COMMAND ----------

# ── 11. MATRICES DE CONFUSIÓN ─────────────────────────────────────────────────

tick_labels = labels_target

def get_confusion_matrix(pred_df):
    cm = (pred_df
          .groupBy(etiqueta, "prediction")
          .agg(count(lit(1)).alias("count"))
          .toPandas()
          .pivot(index=etiqueta, columns="prediction", values="count")
          .fillna(0).astype(int))
    indices = list(range(N_CLASES))
    return cm.reindex(index=indices, columns=indices, fill_value=0).values

validos    = [(n, p) for n, p in predicciones.items() if p is not None]
cols_plot  = 4
filas_plot = (len(validos) + cols_plot - 1) // cols_plot

plt.figure(figsize=(cols_plot * 4, filas_plot * 3.5))
for i, (nombre, pred) in enumerate(validos, 1):
    try:
        cm = get_confusion_matrix(pred)
        plt.subplot(filas_plot, cols_plot, i)
        sns.heatmap(cm, annot=True, fmt="d", cmap="Greens", cbar=False,
                    xticklabels=tick_labels, yticklabels=tick_labels)
        plt.title(nombre, fontsize=10)
        plt.ylabel("Real")
        plt.xlabel("Predicción")
    except Exception as e:
        print(f"[!] CM {nombre}: {e}")

plt.suptitle("Matrices de Confusión — nivel_congestion (bajo / medio / alto)",
             fontsize=12, y=1.02)
plt.tight_layout()
plt.show()

# COMMAND ----------

# ── 12. CONCLUSIÓN ────────────────────────────────────────────────────────────

df_res = pd.DataFrame(resultados).set_index("Modelo")
mejor  = df_res["F1-score"].idxmax()

print("=" * 65)
print("  CONCLUSIÓN")
print("=" * 65)
print(f"  Mejor modelo (F1-score): {mejor}")
print(f"  F1-score:  {df_res.loc[mejor, 'F1-score']:.4f}")
print(f"  Accuracy:  {df_res.loc[mejor, 'Accuracy']:.4f}")
print()
print(f"  Features numéricas finales  ({len(cols_num_sel)}): {cols_num_sel}")
print(f"  Features categóricas finales ({len(cols_cat_sel)}): {cols_cat_sel}")
print()
print("  6 de 9 modelos PySpark funcionan en serverless con multiclase nativo.")
print("  LinearSVC y FMClassifier excluidos: binarios, requieren OneVsRest")
print("  que no soporta Databricks serverless (Delta temp table limitation).")
print("=" * 65)
