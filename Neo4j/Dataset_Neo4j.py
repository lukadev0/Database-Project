import pandas as pd

# Carico il dataset completo da dataset_frodi_100.csv
df_100 = pd.read_csv('dataset_frodi_100.csv')

# Calcolo quello che è il numero di righe da includere nei nuovi dataset
num_rows_75 = int(len(df_100) * 0.75)
num_rows_50 = int(len(df_100) * 0.50)
num_rows_25 = int(len(df_100) * 0.25)

# Eseguo il campionamento per creare i nuovi dataset
df_75 = df_100.sample(n=num_rows_75, random_state=1)
df_50 = df_100.sample(n=num_rows_50, random_state=2)
df_25 = df_100.sample(n=num_rows_25, random_state=3)

# Momento salvataggio
df_75.to_csv('dataset_frodi_75.csv', index=False)
df_50.to_csv('dataset_frodi_50.csv', index=False)
df_25.to_csv('dataset_frodi_25.csv', index=False)

print("File CSV 'dataset_frodi_75.csv', 'dataset_frodi_50.csv', e 'dataset_frodi_25.csv' creati con successo.")
