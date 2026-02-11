from pyspark.sql import SparkSession
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.feature import HashingTF, Tokenizer
from pyspark.ml import Pipeline
from pyspark.sql.functions import col, when

# 1. Initialisation
spark = SparkSession.builder \
    .appName("TwitterSentimentAnalysis") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# 2. Chemins
base_path = "/home/aboubakr/projects/Twitter_sentiment_analysis"
data_path = "/home/aboubakr/.cache/kagglehub/datasets/kazanova/sentiment140/versions/2/*.csv"
model_save_path = f"{base_path}/models/naive_bayes_sentiment"
data_test_path = f"{base_path}/data/test_data_split.csv"

# 3. Chargement
print(f"Chargement des données depuis : {data_path}")
df = spark.read.csv(data_path, inferSchema=True) \
    .toDF("polarity", "id", "date", "query", "user", "text")

# 4. Nettoyage et MAPPING MANUEL (C'est ici la correction !)
# On force : 4 -> 1.0 (Positif), 0 -> 0.0 (Négatif)
df_clean = df.select("polarity", "text") \
    .withColumn("label", when(col("polarity") == 4, 1.0).otherwise(0.0))

# Vérification rapide (Optionnel, juste pour être sûr)
print("Distribution des labels après correction :")
df_clean.groupBy("label").count().show()

# 5. Split Train/Test (80/20)
train_raw, test_raw = df_clean.randomSplit([0.8, 0.2], seed=42)

# ============================================================
# PIPELINE CORRIGÉ (Sans StringIndexer)
# ============================================================

# Étape 1 : Tokenizer (Texte -> Mots)
tokenizer = Tokenizer(inputCol="text", outputCol="words")

# Étape 2 : HashingTF (Mots -> Vecteurs)
hashingTF = HashingTF(inputCol="words", outputCol="features")

# Étape 3 : Le Modèle Naive Bayes
# Il va utiliser la colonne "label" qu'on a créée manuellement juste au-dessus
nb = NaiveBayes(smoothing=1.0, modelType="multinomial", labelCol="label", featuresCol="features")

# Création du Pipeline
# IMPORTANT : On a retiré 'indexer' de la liste !
pipeline = Pipeline(stages=[tokenizer, hashingTF, nb])

print("Entraînement du modèle avec Pipeline...")
model = pipeline.fit(train_raw)

# 6. Sauvegarde du Pipeline complet
model.write().overwrite().save(model_save_path)
print(f"✅ Modèle Pipeline corrigé sauvegardé dans : {model_save_path}")

# 7. Sauvegarde des données de TEST
print(f"Sauvegarde des données de test...")
test_raw \
    .coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(data_test_path)

print(f"✅ Données de test sauvegardées !")