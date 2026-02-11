import socket
import time
import pandas as pd
import random
import glob
import os

# 1. Configuration
HOST = 'localhost'
PORT = 9999
FOLDER_PATH = "/home/aboubakr/projects/Twitter_sentiment_analysis/data/test_data_split.csv"

def get_csv_file(folder):
    """Trouve le fichier part-*.csv à l'intérieur du dossier Spark"""
    search_path = os.path.join(folder, "part-*.csv")
    files = glob.glob(search_path)
    if not files:
        raise FileNotFoundError(f"Aucun fichier CSV trouvé dans {folder}")
    return files[0]

def start_server():
    print("📚 Recherche du fichier de données...")
    try:
        csv_file = get_csv_file(FOLDER_PATH)
        print(f"📄 Lecture du fichier : {csv_file}")
        
        # --- CORRECTION MAJEURE ICI ---
        # 1. engine='python' : Utilise le moteur Python, plus lent mais gère mieux les caractères bizarres (Line Terminators)
        # 2. on_bad_lines='skip' : Si une ligne est mal formée (ex: 4 colonnes au lieu de 2), on l'ignore au lieu de planter.
        # 3. quotechar='"' : Aide pandas à comprendre que les virgules à l'intérieur des guillemets font partie du texte.
        df = pd.read_csv(
            csv_file, 
            engine='python', 
            on_bad_lines='skip', 
            quotechar='"',
            encoding='utf-8' # Force l'encodage
        )
        
        # On ne garde que les tweets valides
        tweets = df['text'].dropna().tolist()
        print(f"✅ {len(tweets)} tweets chargés et prêts !")
        
    except Exception as e:
        print(f"❌ Erreur critique lors de la lecture : {e}")
        return

    # Le reste du code serveur ne change pas...
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((HOST, PORT))
    s.listen(1)
    
    print(f"\n📡 Serveur Producer démarré sur {HOST}:{PORT}")
    print("⏳ En attente de la connexion de Spark (Lancez processor.py maintenant)...")

    conn, addr = s.accept()
    print(f"🔗 Connecté à Spark via : {addr}")

    try:
        for i, tweet in enumerate(tweets):
            # Nettoyage vital pour le streaming socket
            clean_tweet = str(tweet).replace('\n', ' ').replace('\r', '')
            
            message = clean_tweet + "\n"
            conn.send(message.encode('utf-8'))
            
            # Affichage allégé
            print(f"[{i+1}/{len(tweets)}] 📤 Envoyé : {clean_tweet[:60]}...") 
            
            time.sleep(random.uniform(0.5, 1.5))
            
    except BrokenPipeError:
        print("\n❌ La connexion avec Spark a été rompue.")
    except KeyboardInterrupt:
        print("\n🛑 Arrêt manuel.")
    finally:
        conn.close()
        s.close()
        print("Serveur fermé.")

if __name__ == "__main__":
    start_server()