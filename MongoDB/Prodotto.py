import pandas as pd
import pymongo
import random

# Connessione al database MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["Frodi"]
collection_name = 'Prodotto'
collection_name_75 = 'Prodotto 75%'
collection_name_50 = 'Prodotto 50%'
collection_name_25 = 'Prodotto 25%'
collection_name_100 = 'Prodotto 100%'


# Carico il dataset CSV utilizzando pandas
csv_filename = 'dataset_prodotti.csv'
df = pd.read_csv(csv_filename, encoding='ISO-8859-1')

# Calcolo il numero totale di documenti nel Dataframe
total_documents = df.shape[0]


# Calcolo il numero di documenti per ciascuna percentuale
n_100 = int(100*total_documents)
n_75 = int(0.75*total_documents)
n_50 = int(0.50*total_documents)
n_25 = int(0.25*total_documents)

# Genero tre liste di indicie casuali per suddividere i dati
indices = list(range(total_documents))
random.shuffle(indices)
indices_100 = indices[:n_100]
indices_75 = indices[:n_75]
indices_50 = indices[:n_50]
indices_25 = indices[:n_25]


# Suddivido il DataFrame in base agli indici generati
df_100 = df.iloc[indices_100]
df_50 = df.iloc[indices_50]
df_75 = df.iloc[indices_75]
df_25 = df.iloc[indices_25]


# Converto i Dataframe pandas in elenchi di dizionari
data_100 = df_100.to_dict(orient='records')
data_75 = df_75.to_dict(orient='records')
data_50 = df_50.to_dict(orient='records')
data_25 = df_25.to_dict(orient='records')


# Inserisco i vari dati nelle collezioni MongoDB separate
db[collection_name_100].insert_many(data_100)
db[collection_name_75].insert_many(data_75)
db[collection_name_50].insert_many(data_50)
db[collection_name_25].insert_many(data_25)


print("Dati caricati in MongoDB con successo.")
