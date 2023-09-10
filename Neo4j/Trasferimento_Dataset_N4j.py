import os
import pandas as pd
from py2neo import Graph, Node

# Cartella in cui si trova lo script Python
script_directory = os.path.dirname(os.path.abspath(__file__))


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

# Tipi di dati
data_types = ['transazioni']

for data_type in data_types:
    for percentage, graph in graphs_by_percentage.items():
        csv_filename = f'dataset_frodi_{percentage}.csv'

        # Percorso completo al file CSV
        csv_path = os.path.join(script_directory, csv_filename)

        # Leggo i dati dal file CSV utilizzando pandas
        data = pd.read_csv(csv_path)

        # Inserisco i dati nel grafo
        for index, row in data.iterrows():
            transaction_id = row['transaction_id']
            timestamp = row['timestamp']
            amount = row['amount']
            transaction_type = row['transaction_type']
            subject = row['subject']
            description = row['description']
            status = row['status']
            country = row['country']

            node = Node(data_type, transaction_id=transaction_id, timestamp=timestamp,
                        amount=amount, transaction_type=transaction_type, subject=subject,
                        description=description, status=status, country=country)
            graph.create(node)
            print(f"Elemento inserito correttamente nel dataset {percentage}%.")

        print(f"Dati del dataset {data_type} al {percentage}% inseriti in Neo4j con successo.")

print("Inserimento completato per tutti i dataset.")
