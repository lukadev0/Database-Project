import os
import pandas as pd
from py2neo import Graph, Node, Relationship

# Directory in cui si trovano i file CSV
csv_directory = r'C:\Users\lucac\PycharmProjects\pythonProject\Progetto Database 2'


graph100 = Graph("bolt://localhost:7687", user="neo4j", password="12345678", name="dataset100")
graph75 = Graph("bolt://localhost:7687", user="neo4j", password="12345678", name="dataset75")
graph50 = Graph("bolt://localhost:7687", user="neo4j", password="12345678", name="dataset50")
graph25 = Graph("bolt://localhost:7687", user="neo4j", password="12345678", name="dataset25")

# Dizionario per mappare le percentuali ai grafi
graphs_by_percentage = {
    100: graph100,
    75: graph75,
    50: graph50,
    25: graph25
}

# Nomi dei file CSV
csv_files = [
    "dataset_commerciante.csv",
    "dataset_utenti.csv",
    "dataset_prodotti.csv",
    "dataset_transazioni.csv"
]

# Importo i dati dai file CSV nei grafi Neo4j e crea le relazioni
for csv_file in csv_files:
    for percentage, graph in graphs_by_percentage.items():
        # Percorso completo al file CSV
        csv_path = os.path.join(csv_directory, csv_file)

        # Lettura dei dati dal file CSV utilizzando pandas
        data = pd.read_csv(csv_path, encoding='ISO-8859-1', dtype={'transaction_id': int, 'user_id': int, 'merchant_id': int, 'product_id': int, 'amount': float})

        # Calcola il numero di righe da inserire per la percentuale specifica
        rows_to_insert = int(len(data) * (percentage / 100))

        # Prendi solo le prime "rows_to_insert" righe
        data = data.head(rows_to_insert)

        # Inserisci i dati nel grafo
        for index, row in data.iterrows():
            node = Node(csv_file.split("_")[1].split(".")[0], **row.to_dict())
            graph.create(node)

            if "transazioni" in csv_file:
                # Creazione relazione con utente mittente
                user_id = int(row['user_id'])
                user_node = graph.nodes.match('utenti', user_id=user_id).first()
                if user_node:
                    transaction_to_user = Relationship(user_node, 'EFFETTUA', node)
                    graph.create(transaction_to_user)

                # Creazione relazione con prodotto
                product_id = int(row['product_id'])
                product_node = graph.nodes.match('prodotti', product_id=product_id).first()
                if product_node:
                    transaction_to_product = Relationship(node, 'CONCERNE', product_node)
                    graph.create(transaction_to_product)

                # Creazione relazione con commerciante
                merchant_id = int(row['merchant_id'])
                merchant_node = graph.nodes.match('commerciante', merchant_id=merchant_id).first()
                if merchant_node:
                    transaction_to_merchant = Relationship(node, 'RIGUARDA', merchant_node)
                    graph.create(transaction_to_merchant)

        print(f"{percentage}% dei dati del dataset {csv_file} inseriti in Neo4j con successo nel dataset {percentage}%.")

print("Inserimento dati e creazione relazioni completato per tutti i dataset.")
