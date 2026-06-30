# Databricks notebook — Capa ML
# Sistema de Optimización Dinámica del Tráfico Urbano — Lima
#
# Problema: Clasificación multiclase
# Target:   nivel_congestion  ->  bajo / medio / alto

# COMMAND ----------

# Instalación (ejecutar en terminal antes de correr este script):
#   pip install pyspark adlfs fsspec pyarrow pandas numpy matplotlib seaborn
# Requisito: Java 11+  ->  https://adoptium.net/

# COMMAND ----------

import sys
import os

# Apunta a Spark al mismo Python que está corriendo este script.
# En Windows el alias "python" del Microsoft Store rompe los workers de Spark.
os.environ["PYSPARK_PYTHON"]        = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import winreg
def _get_reg(name):
    try:
        k = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE,
                           r"SYSTEM\CurrentControlSet\Control\Session Manager\Environment")
        v, _ = winreg.QueryValueEx(k, name)
        return v
    except Exception:
        return None

_hadoop = os.environ.get("HADOOP_HOME") or _get_reg("HADOOP_HOME")
if _hadoop:
    os.environ["HADOOP_HOME"] = _hadoop
    os.environ["PATH"] = os.path.join(_hadoop, "bin") + os.pathsep + os.environ.get("PATH", "")

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")   # backend sin ventana — evita conflicto entre Tk y PySpark en Windows
import matplotlib.pyplot as plt
import seaborn as sns
import adlfs

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, lit
from pyspark.sql.types import StringType

from pyspark.ml.stat import Correlation
from pyspark.ml.feature import (StringIndexer, OneHotEncoder,
                                 VectorAssembler, StandardScaler,
                                 MinMaxScaler)
from pyspark.ml import Pipeline
from pyspark.ml.feature import UnivariateFeatureSelector

from pyspark.ml.classification import (
    DecisionTreeClassifier, RandomForestClassifier,
    MultilayerPerceptronClassifier, LogisticRegression,
    GBTClassifier, LinearSVC, NaiveBayes, FMClassifier,
)
from pyspark.ml.functions import vector_to_array
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

spark = (SparkSession.builder
         .appName("trafico_lima_ml")
         .master("local[1]")
         .config("spark.driver.memory", "8g")
         .config("spark.sql.shuffle.partitions", "8")
         .config("spark.python.worker.reuse", "true")
         .getOrCreate())
print("Spark version:", spark.version)

ADLS_ACCOUNT   = "traficolima"
ADLS_KEY       = ""          # <-- pega tu ADLS_KEY aquí (ver .env local)
ADLS_CONTAINER = "trafico-lima"
SO = {"account_name": ADLS_ACCOUNT, "account_key": ADLS_KEY}

assert ADLS_KEY, "ERROR: pega tu ADLS_KEY antes de continuar"

# COMMAND ----------

# ── Funciones (misma estructura que el ejemplo del curso) ─────────────────────

def ovr_fit_predict(clf_factory, feats_col, label_col, n_classes, tr, te):
    """OvR manual sin Python UDFs — usa SQL expressions para binarizar etiquetas.
    clf_factory(k, bin_col) devuelve un clasificador binario ya configurado.
    """
    from pyspark.sql.functions import monotonically_increasing_id, when, col as scol

    te_id  = te.withColumn("_rid", monotonically_increasing_id())
    result = te_id.select("_rid", scol(label_col))

    for k in range(n_classes):
        bin_col = f"_bin{k}"
        tr_k = tr.withColumn(bin_col, when(scol(label_col) == float(k), 1.0).otherwise(0.0))
        te_k = te_id.withColumn(bin_col, when(scol(label_col) == float(k), 1.0).otherwise(0.0))

        fitted  = clf_factory(k, bin_col).fit(tr_k)
        scored  = fitted.transform(te_k)

        # P(clase k): usar probability[1] si existe, si no rawPrediction[1] (LinearSVC)
        vec_col = "probability" if "probability" in scored.columns else "rawPrediction"
        score   = vector_to_array(scol(vec_col)).getItem(1)
        result  = result.join(scored.select("_rid", score.alias(f"_s{k}")), "_rid")

    # Argmax con expresiones SQL puras (sin Python UDF)
    result = result.withColumn(
        "prediction",
        when((scol("_s0") >= scol("_s1")) & (scol("_s0") >= scol("_s2")), 0.0)
        .when(scol("_s1") >= scol("_s2"), 1.0)
        .otherwise(2.0)
    )
    return result.select(scol(label_col), scol("prediction"))


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

TARGET = "nivel_congestion"
ruta_gold = f"abfs://{ADLS_CONTAINER}/gold/congestion_por_zona/datos.parquet"

# Leer con pandas (adlfs), escribir a disco local y que Spark lea desde ahí.
# spark.createDataFrame(525k filas) crea una tarea de ~25 MB y el worker Python se cae.
# spark.read.parquet(ruta_local) parte el archivo en splits — sin tarea gigante.
import pathlib, tempfile

df_pd = pd.read_parquet(ruta_gold, storage_options=SO).dropna(subset=[TARGET])

_tmp = pathlib.Path(tempfile.gettempdir()) / "pyspark_trafico"
_tmp.mkdir(exist_ok=True)
_local_parquet = str(_tmp / "gold_ml.parquet")
df_pd.to_parquet(_local_parquet, index=False)

df = spark.read.parquet(_local_parquet).sample(fraction=0.05, seed=700)

print(f"Gold: {df.count()} filas x {len(df.columns)} columnas")
print(f"\nDistribución target:")
df.groupBy(TARGET).count().orderBy("count", ascending=False).show()
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
    print(f"  {lbl} -> {i:.0f}")

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
print(f"Eliminadas por multicolinealidad: {len(cols_drop)}  ->  {cols_drop}")
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
_plot_pearson = str(_tmp / "pearson_heatmap.png")
plt.savefig(_plot_pearson, dpi=120, bbox_inches="tight")
plt.close()
print(f"Gráfico guardado: {_plot_pearson}")

# COMMAND ----------

# ── 6. SELECCIÓN — ANOVA Y CHI² ───────────────────────────────────────────────

if len(cols_num_filtradas) >= 2:
    try:
        cols_num_sel = get_cols_selected(df_clean, False, cols_num_filtradas, TARGET)
    except Exception as e:
        print(f"[!] ANOVA falló ({type(e).__name__}): dataset muy pequeño. Se usan todas las numéricas.")
        cols_num_sel = cols_num_filtradas
else:
    cols_num_sel = cols_num_filtradas
    print("[!] Muy pocas columnas numéricas para ANOVA — se usan todas las retenidas.")

print(f"Numéricas seleccionadas por ANOVA (p<0.05): {len(cols_num_sel)}")
print(f"  {cols_num_sel}")

if len(cols_cat) >= 1:
    try:
        cols_cat_sel = get_cols_selected(df_clean, True, cols_cat, TARGET)
    except Exception as e:
        print(f"[!] Chi² falló ({type(e).__name__}): dataset muy pequeño. Se usan todas las categóricas.")
        cols_cat_sel = cols_cat
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

# Pipeline adicional para NaiveBayes — requiere features no-negativas -> MinMaxScaler
# Reutiliza las columnas intermedias ya calculadas en df_prep (num_features, cat_features)
mms = MinMaxScaler(inputCol="num_features", outputCol="num_mms")
mms_model = mms.fit(df_prep)
df_prep_nb = mms_model.transform(df_prep)

if cols_cat_sel:
    final_nb = VectorAssembler(inputCols=["num_mms", "cat_features"],
                               outputCol="features_nb", handleInvalid="skip")
else:
    final_nb = VectorAssembler(inputCols=["num_mms"],
                               outputCol="features_nb", handleInvalid="skip")
df_prep_nb = final_nb.transform(df_prep_nb)
print(f"Pipeline NaiveBayes (MinMaxScaler): vector features_nb listo")

# COMMAND ----------

# ── 8. SPLIT TRAIN / TEST ─────────────────────────────────────────────────────

train_df, test_df = df_prep.randomSplit([0.80, 0.20], seed=700)
train_nb, test_nb = df_prep_nb.randomSplit([0.80, 0.20], seed=700)

# Cache — OvR re-ejecutaría el pipeline completo (incluyendo Python UDFs del
# UnivariateFeatureSelector) para cada clasificador binario. Con cache, el
# preprocessing corre una sola vez y los modelos leen desde memoria.
# (spark.write.parquet falla en Windows sin winutils.exe instalado)
print("Cacheando train/test en memoria...")
train_df.cache()
test_df.cache()
train_nb.cache()
test_nb.cache()
n_train = train_df.count()   # dispara la materialización
n_test  = test_df.count()
train_nb.count()
test_nb.count()

print(f"Train: {n_train} filas  |  Test: {n_test} filas")
train_df.groupBy(TARGET).count().orderBy("count", ascending=False).show()

# COMMAND ----------

# ── 9. DEFINIR MODELOS ────────────────────────────────────────────────────────

N_CLASES = len(labels_target)
feats    = "features_scaled"
feats_nb = "features_nb"      # MinMaxScaler — requerido por NaiveBayes
etiqueta = TARGET

# ── Modelos con soporte multiclase nativo ──────────────────────────────────────
dt  = DecisionTreeClassifier(featuresCol=feats, labelCol=etiqueta,
                              maxDepth=5, seed=700)
rf  = RandomForestClassifier(featuresCol=feats, labelCol=etiqueta,
                              numTrees=100, seed=700)
lr  = LogisticRegression(featuresCol=feats, labelCol=etiqueta,
                         family="multinomial", maxIter=100)
mlp = MultilayerPerceptronClassifier(featuresCol=feats, labelCol=etiqueta,
                                     layers=[n_input, 64, 32, N_CLASES], seed=700,
                                     maxIter=100)
nb  = NaiveBayes(featuresCol=feats_nb, labelCol=etiqueta, smoothing=1.0)

# ── Factories para modelos OvR manual (evitan Python UDFs de OneVsRest) ────────
# GBT: clasificacion binaria en Spark 3.5 -> necesita OvR manual
def _factory_gbt(k, bin_col):
    return GBTClassifier(featuresCol=feats, labelCol=bin_col,
                         maxIter=20, maxDepth=4, seed=700)

# LinearSVC: solo binario, no da probability -> usa rawPrediction[1] como score
def _factory_svc(k, bin_col):
    return LinearSVC(featuresCol=feats, labelCol=bin_col, maxIter=30)

# FMClassifier: factorization machines, solo binario
def _factory_fm(k, bin_col):
    return FMClassifier(featuresCol=feats, labelCol=bin_col,
                        maxIter=20, seed=700)

# LR binaria OvR (vs multinomial nativo de lr arriba)
def _factory_lr_ovr(k, bin_col):
    return LogisticRegression(featuresCol=feats, labelCol=bin_col, maxIter=30)

# Tupla: (nombre, modelo_nativo, usar_nb, ovr_factory)
# modelo_nativo=None  -> se usa ovr_factory con ovr_fit_predict
# usar_nb=True        -> fit/transform sobre train_nb/test_nb (MinMaxScaler)
MODELOS = [
    ("DecisionTree",       dt,   False, None),
    ("RandomForest",       rf,   False, None),
    ("GBT (OvR manual)",   None, False, _factory_gbt),
    ("LogisticRegression", lr,   False, None),
    ("MLP",                mlp,  False, None),
    ("NaiveBayes",         nb,   True,  None),
    ("LinearSVC (OvR)",    None, False, _factory_svc),
    ("FMClassifier (OvR)", None, False, _factory_fm),
    ("OvR + LR binaria",   None, False, _factory_lr_ovr),
]
print(f"Modelos: {len(MODELOS)}  |  Clases: {N_CLASES}  |  Input dim: {n_input}")

# COMMAND ----------

# ── 10. ENTRENAMIENTO Y MÉTRICAS ──────────────────────────────────────────────

eval_mc      = MulticlassClassificationEvaluator(labelCol=etiqueta, predictionCol="prediction")
metricas_kv  = [("accuracy","Accuracy"), ("f1","F1-score"),
                ("weightedPrecision","Precision"), ("weightedRecall","Recall")]

print(f"{'Modelo':22s} | Accuracy | F1-score | Precision | Recall")
print("-" * 65)

predicciones = {}
resultados   = []

for nombre, modelo, usar_nb, ovr_factory in MODELOS:
    tr = train_nb if usar_nb else train_df
    te = test_nb  if usar_nb else test_df
    try:
        if ovr_factory is not None:
            # OvR manual: evita Python UDFs de pyspark.ml.classification.OneVsRest
            pred = ovr_fit_predict(ovr_factory, feats, etiqueta, N_CLASES, tr, te)
        else:
            pred = modelo.fit(tr).transform(te)
        predicciones[nombre] = pred
        fila = {"Modelo": nombre}
        for key, label in metricas_kv:
            fila[label] = round(eval_mc.setMetricName(key).evaluate(pred), 4)
        resultados.append(fila)
        print(f"{nombre:22s} | {fila['Accuracy']:.4f}   | {fila['F1-score']:.4f}   | "
              f"{fila['Precision']:.4f}    | {fila['Recall']:.4f}")
    except Exception as e:
        print(f"{nombre:22s} | ERROR: {type(e).__name__}: {e}")
        predicciones[nombre] = None

df_res_tabla = pd.DataFrame(resultados).set_index("Modelo")
try:
    display(df_res_tabla)
except NameError:
    print(df_res_tabla.to_string())

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
_plot_cm = str(_tmp / "confusion_matrices.png")
plt.savefig(_plot_cm, dpi=120, bbox_inches="tight")
plt.close()
print(f"Gráfico guardado: {_plot_cm}")

# COMMAND ----------

# ── 12. CONCLUSIÓN ────────────────────────────────────────────────────────────

mejor = df_res_tabla["F1-score"].idxmax()

print("=" * 65)
print("  CONCLUSIÓN")
print("=" * 65)
print(f"  Mejor modelo (F1-score): {mejor}")
print(f"  F1-score:  {df_res_tabla.loc[mejor, 'F1-score']:.4f}")
print(f"  Accuracy:  {df_res_tabla.loc[mejor, 'Accuracy']:.4f}")
print()
print(f"  Features numericas finales   ({len(cols_num_sel)}): {cols_num_sel}")
print(f"  Features categoricas finales ({len(cols_cat_sel)}): {cols_cat_sel}")
print()
print("  9 modelos PySpark ML ejecutados en modo local (local[*]):")
print("    Multiclase nativo : DT, RF, LR-multinomial, MLP, NaiveBayes")
print("    OvR (binario->multi): GBT, LinearSVC, FMClassifier, OneVsRest+LR")
print("  NaiveBayes usa MinMaxScaler (requiere features >= 0).")
print("  OvR entrena 1 clasificador binario por clase (3 x modelo base).")
print("=" * 65)

# COMMAND ----------

# ── 13. GUARDAR MODELO PARA INFERENCIA ────────────────────────────────────────

import json, pathlib

MODEL_DIR = pathlib.Path(__file__).resolve().parent / "saved_model"
MODEL_DIR.mkdir(exist_ok=True)

print(f"\nGuardando modelos en {MODEL_DIR} ...")

# Pipeline de preprocesamiento completo (StringIndexers + OHE + Assemblers + Scaler)
model_prep.write().overwrite().save(str(MODEL_DIR / "prep_pipeline"))
print("  prep_pipeline guardado")

# MLP re-entrenado sobre todo el train (ya esta en memoria cacheada)
print("  Entrenando MLP final sobre train completo...")
mlp_save = MultilayerPerceptronClassifier(
    featuresCol=feats, labelCol=etiqueta,
    layers=[n_input, 64, 32, N_CLASES], seed=700, maxIter=100
)
mlp_save.fit(train_df).write().overwrite().save(str(MODEL_DIR / "mlp_classifier"))
print("  mlp_classifier guardado")

# Metadata: todo lo necesario para reproducir el preprocessing en inferencia
meta = {
    "labels_target": labels_target,   # ["alto","bajo","medio"] -> indice 0,1,2
    "cols_num_sel":  cols_num_sel,
    "cols_cat_sel":  cols_cat_sel,
    "excluir":       EXCLUIR,
    "n_input":       n_input,
    "target":        TARGET,
}
with open(MODEL_DIR / "metadata.json", "w") as f:
    json.dump(meta, f, indent=2)
print("  metadata.json guardado")

print(f"\nModelo listo para inferencia.")
print(f"  Labels: {labels_target}")
print(f"  Usar: pipeline.py carga prep_pipeline + mlp_classifier automaticamente")
