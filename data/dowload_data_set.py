import kagglehub
import os
import pandas as pd

path_folder = kagglehub.dataset_download("kazanova/sentiment140")

print("path_folder to dataset files:", path_folder)

csv_path = os.path.join(path_folder, "training.1600000.processed.noemoticon.csv")
df = pd.read_csv(
    csv_path,
    encoding="latin1",
    header=None,
    names=["target", "ids", "date", "flag", "user", "text"], # Noms de colonnes officiels
)

print("First 5 records:")
print(df.head())