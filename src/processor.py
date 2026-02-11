import sys
import os

# Configuration de l'environnement (au cas où)
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

# 1. Initialisation de la Session Spark
print("🚀 Démarrage du moteur Spark Streaming...")
spark = SparkSession.builder \
    .appName("TwitterSentimentAnalysis_RealTime") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

# Réduire le bruit dans les logs
spark.sparkContext.setLogLevel("WARN")

# 2. Chemins
base_path = "/home/aboubakr/projects/Twitter_sentiment_analysis"
model_path = f"{base_path}/models/naive_bayes_sentiment"

# 3. Chargement du Pipeline Entraîné
print(f"⏳ Chargement du modèle depuis : {model_path}")
try:
    # On charge le PipelineModel car il contient le StringIndexer et le Tokenizer
    loaded_pipeline = PipelineModel.load(model_path)
    print("✅ Modèle chargé avec succès !")
except Exception as e:
    print(f"❌ ERREUR CRITIQUE : Impossible de charger le modèle.\n{e}")
    sys.exit(1)

# 4. Connexion au flux (Socket)
print("📡 Tentative de connexion au Producer sur localhost:9999...")

# Spark lit le flux ligne par ligne. La colonne s'appelle par défaut "value".
raw_stream = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# IMPORTANT : Le modèle attend une colonne nommée "text" (comme lors de l'entraînement)
# On renomme "value" -> "text"
tweet_stream = raw_stream.select(col("value").alias("text"))

# 5. Prédiction en Temps Réel
# Le pipeline fait tout : Tokenization -> HashingTF -> Classification
predictions = loaded_pipeline.transform(tweet_stream)

# 6. Embellissement du résultat (Mapping 0.0 -> Négatif, 1.0 -> Positif)
# Rappel : StringIndexer a transformé 0->0.0 (Négatif) et 4->1.0 (Positif)
def map_label(prediction):
    if prediction == 1.0:
        return "😃 Positif"
    else:
        return "😡 Négatif"

# On enregistre cette fonction pour que Spark puisse l'utiliser (UDF)
label_udf = udf(map_label, StringType())

# Sélection finale pour l'affichage
final_output = predictions.select(
    col("text"),
    label_udf(col("prediction")).alias("sentiment"),
    col("probability")
)

# 7. Affichage dans la console
# Trigger "processingTime='2 seconds'" pour mettre à jour l'affichage toutes les 2s
print("🎬 Streaming lancé ! Regardez les prédictions défiler ci-dessous :")
print("---------------------------------------------------------------")

query = final_stream = final_output.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="2 seconds") \
    .start()

query.awaitTermination()