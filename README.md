Here's your text formatted cleanly in Markdown for a `README.md` file:

```md
# Twitter Sentiment Analysis avec PySpark 🚀

Un pipeline Big Data complet pour l'analyse de sentiment en temps réel sur des flux de tweets, utilisant **Apache Spark Structured Streaming** et l'algorithme **Naive Bayes**.

---

## 🏗 Architecture du Projet

Le projet suit une architecture Lambda simplifiée pour le traitement en temps réel :

- **Ingestion & Training** : Le modèle est entraîné sur le dataset [Sentiment140](https://www.kaggle.com/kazanova/sentiment140) (1.6 millions de tweets) via **Spark MLlib**.
- **Producer (Simulation)** : Un script Python simule un flux de données en direct via des **Sockets TCP**.
- **Processor (Streaming)** : **Spark Structured Streaming** charge le modèle entraîné, écoute le flux, prédit le sentiment (**Positif/Négatif**) et affiche le résultat en temps réel.

---

## 📂 Structure du Projet


Twitter_sentiment_analysis/
├── data/               # Dossier pour les datasets (non inclus sur GitHub)
├── models/             # Dossier de sauvegarde du modèle Pipeline
├── notebooks/          # Scripts d'entraînement et notebooks
│   └── training.py     # Script principal pour entraîner et sauvegarder le modèle
├── src/                # Code source de l'application
│   ├── producer.py     # Serveur Socket qui envoie les tweets (Simulation)
│   ├── processor.py    # Client Spark Streaming qui prédit les sentiments
│   └── evaluate_model.py # Script de validation de l'accuracy
├── .gitignore          # Fichiers à ignorer par Git
├── requirements.txt    # Liste des dépendances Python
└── README.md           # Documentation du projet
```

---

## 🛠 Prérequis

- **Python 3.12** (ou supérieur)
- **Java 17** (OpenJDK) — indispensable pour Spark
- **Apache Spark 3.x**
- **WSL2** (si vous êtes sous Windows)

---

## 📦 Installation

### 1. Cloner le projet
```bash
git clone https://github.com/VOTRE_USER/Twitter_sentiment_analysis.git
cd Twitter_sentiment_analysis
```

### 2. Créer l’environnement virtuel
```bash
python3 -m venv .venv
source .venv/bin/activate  # Sur Windows : .venv\Scripts\activate
```

### 3. Installer les dépendances
```bash
pip install -r requirements.txt
```

---

## 🚀 Utilisation

### 1. Entraînement du Modèle

Avant de lancer le streaming, générez le modèle (Pipeline Naive Bayes), qui sera sauvegardé dans `models/` :

```bash
python3 notebooks/training.py
```

> **Note** : L’accuracy attendue est d’environ **77–78%**.

### 2. Validation (Optionnel)

Pour vérifier la précision du modèle sur des données de test :
```bash
python3 src/evaluate_model.py
```

### 3. Lancer le Streaming

Ouvrez **deux terminaux séparés** (activez `.venv` dans les deux).

#### Terminal 1 : Le Producteur (Serveur)
Lit les données de test et les envoie sur le port `9999` :
```bash
source .venv/bin/activate
python3 src/producer.py
```
Attendez le message : `"En attente de la connexion de Spark..."`

#### Terminal 2 : Le Processeur (Spark Streaming)
Écoute le port `9999`, charge le modèle et prédit en direct :
```bash
source .venv/bin/activate
python3 src/processor.py
```

---

## 📊 Résultats

Une fois connectés, le processeur affiche les prédictions par batch toutes les 2 secondes :

```
-------------------------------------------
Batch: 5
-------------------------------------------
+-----------------------+-----------+
|text                   |sentiment  |
+-----------------------+-----------+
|I love this project!   |😃 Positif |
|My code is broken...   |😡 Négatif |
+-----------------------+-----------+
```

---

## 👤 Auteur

**Aboubakr Tahir**  
Étudiant Ingénieur en Big Data & Systèmes d'Information  
ENSA Berrechid, Maroc
