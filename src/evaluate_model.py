from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# 1. Initialisation
spark = SparkSession.builder \
    .appName("TwitterSentimentAnalysis_Eval") \
    .getOrCreate()

# 2. Chemins
base_path = "/home/aboubakr/projects/Twitter_sentiment_analysis"
model_path = f"{base_path}/models/naive_bayes_sentiment"
test_data_path = f"{base_path}/data/test_data_split.csv"

# 3. Chargement des données de TEST (BRUTES)
# IMPORTANT : On charge les données brutes ! On ne fait aucun Tokenizer/HashingTF ici.
# Le Pipeline sauvegardé s'en chargera.
print(f"Chargement des données de test depuis : {test_data_path}")
df_test = spark.read.csv(test_data_path, header=True, inferSchema=True)

# 4. Chargement du Pipeline Entraîné
# Notez qu'on importe PipelineModel et non plus NaiveBayesModel
print(f"Chargement du Pipeline complet depuis : {model_path}")
loaded_pipeline = PipelineModel.load(model_path)

# 5. Prédiction
# La magie opère ici : le pipeline va automatiquement :
# a) Convertir polarity -> label
# b) Tokenizer le text -> words
# c) HashingTF words -> features
# d) Prédire -> prediction
predictions = loaded_pipeline.transform(df_test)

# 6. Évaluation
# On compare la colonne 'label' (créée par le pipeline) avec 'prediction'
evaluator = MulticlassClassificationEvaluator(
    labelCol="label", 
    predictionCol="prediction", 
    metricName="accuracy"
)

accuracy = evaluator.evaluate(predictions)

print("\n===========================================")
print(f"🎯 Précision du modèle (Accuracy) : {accuracy:.2%}")
print("===========================================")

# Afficher quelques exemples pour le plaisir
predictions.select("text", "label", "prediction", "probability").show(5)