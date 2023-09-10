import time
import csv
import numpy as np
import scipy.stats as stats
from py2neo import Graph

# Dizionario per i tempi di risposta medi della prima esecuzione per ogni percentuale
tempi_di_risposta_prima_esecuzione = {}

# Dizionario per i tempi di risposta medi delle 30 esecuzioni successive per ogni percentuale
tempi_di_risposta_media_intervallo = {}

percentuali = ['100', '75', '50', '25']


def calculate_confidence_interval(data):
    mean_val = np.mean(data)
    std_dev = np.std(data)
    margin_of_error = 1.96 * (std_dev / np.sqrt(len(data)))  # 1.96 è lo z-score per avere l'intervallo di confidenza al 95%
    return mean_val, margin_of_error


for percentuale in percentuali:
    db_name = f"dataset{percentuale}"
    graph = Graph(f"bolt://localhost:7687/{db_name}", user="neo4j", password="12345678", name=db_name)

    print(f"\nAnalisi per la percentuale: {percentuale}\n")

    selected_country = 'Italy'
    selected_username = 'Heather James'
    dataset_name = f"dataset{percentuale}"

    # Calcolo il tempo medio della prima esecuzione per la prima query
    start_time = time.time()
    result = graph.run("MATCH (t:transazioni {country: $country}) RETURN count(t) as num_transactions", country=selected_country)
    record = result.next()  # Ottengo il record restituito dalla query
    end_time = time.time()
    tempo_prima_esecuzione = round((end_time - start_time) * 1000, 2)

    num_transactions = record['num_transactions']
    print(f"Numero totale di transazioni in {selected_country}: {num_transactions}\n")
    tempi_di_risposta_prima_esecuzione[f"{dataset_name} - Query 1"] = tempo_prima_esecuzione

    # Calcolo il tempo medio delle 30 esecuzioni successive per la prima query
    tempi_successivi = []
    for _ in range(30):
        start_time = time.time()
        result = graph.run("MATCH (t:transazioni {country: $country}) RETURN count(t) as num_transactions", country=selected_country)
        record = result.next()  # Ottengo il record restituito dalla query
        end_time = time.time()
        tempo_esecuzione = round((end_time - start_time) * 1000, 2)

        num_transactions = record['num_transactions']
        tempi_successivi.append(tempo_esecuzione)

    tempo_medio_successive = round(sum(tempi_successivi) / len(tempi_successivi), 2)
    mean, interval = calculate_confidence_interval(tempi_successivi)
    tempi_di_risposta_media_intervallo[f"{dataset_name} - Query 1"] = (tempo_medio_successive, mean, interval)

    # Calcolo il tempo medio della prima esecuzione per la seconda query
    start_time = time.time()
    result = graph.run("MATCH (t:transazioni {country: $country, status: 'sospetta'}) RETURN sum(t.amount) as total_amount", country=selected_country)
    record = result.next()  # Ottengo il record restituito dalla query
    end_time = time.time()
    tempo_prima_esecuzione = round((end_time - start_time) * 1000, 2)

    total_amount = record['total_amount']
    print(f"Totale degli importi delle transazioni fraudolente in {selected_country}: {total_amount}\n")
    tempi_di_risposta_prima_esecuzione[f"{dataset_name} - Query 2"] = tempo_prima_esecuzione

    # Calcolo il tempo medio delle 30 esecuzioni successive per la seconda query
    tempi_successivi = []
    for _ in range(30):
        start_time = time.time()
        result = graph.run("MATCH (t:transazioni {country: $country, status: 'sospetta'}) RETURN sum(t.amount) as total_amount", country=selected_country)
        record = result.next()  # Ottengo il record restituito dalla query
        end_time = time.time()
        tempo_esecuzione = round((end_time - start_time) * 1000, 2)

        total_amount = record['total_amount']
        tempi_successivi.append(tempo_esecuzione)

    tempo_medio_successive = round(sum(tempi_successivi) / len(tempi_successivi), 2)
    mean, interval = calculate_confidence_interval(tempi_successivi)
    tempi_di_risposta_media_intervallo[f"{dataset_name} - Query 2"] = (tempo_medio_successive, mean, interval)

    # Calcolo il tempo medio della prima esecuzione per la terza query
    start_time = time.time()
    result = graph.run("MATCH (t:transazioni {subject: $username, status: 'sospetta'}) RETURN count(t) as num_transactions", username=selected_username)
    record = result.next()  # Ottengo il record restituito dalla query
    end_time = time.time()
    tempo_prima_esecuzione = round((end_time - start_time) * 1000, 2)

    num_transactions = record['num_transactions']
    print(f"Numero totale di transazioni fraudolente per l'utente {selected_username}: {num_transactions}\n")
    tempi_di_risposta_prima_esecuzione[f"{dataset_name} - Query 3"] = tempo_prima_esecuzione

    # Calcolo il tempo medio delle 30 esecuzioni successive per la terza query
    tempi_successivi = []
    for _ in range(30):
        start_time = time.time()
        result = graph.run("MATCH (t:transazioni {subject: $username, status: 'sospetta'}) RETURN count(t) as num_transactions", username=selected_username)
        record = result.next()  # Ottengo il record restituito dalla query
        end_time = time.time()
        tempo_esecuzione = round((end_time - start_time) * 1000, 2)

        num_transactions = record['num_transactions']
        tempi_successivi.append(tempo_esecuzione)

    tempo_medio_successive = round(sum(tempi_successivi) / len(tempi_successivi), 2)
    mean, interval = calculate_confidence_interval(tempi_successivi)
    tempi_di_risposta_media_intervallo[f"{dataset_name} - Query 3"] = (tempo_medio_successive, mean, interval)

    # Calcolo il tempo medio della prima esecuzione per la quarta query
    start_time = time.time()
    result = graph.run("MATCH (t:transazioni {status: 'sospetta'}) RETURN count(t) as num_transactions")
    record = result.next()  # Ottengo il record restituito dalla query
    end_time = time.time()
    tempo_prima_esecuzione = round((end_time - start_time) * 1000, 2)

    num_transactions = record['num_transactions']
    print(f"Numero totale di transazioni fraudolente nel dataset {dataset_name}: {num_transactions}\n")
    tempi_di_risposta_prima_esecuzione[f"{dataset_name} - Query 4"] = tempo_prima_esecuzione

    # Calcolo il tempo medio delle 30 esecuzioni successive per la quarta query
    tempi_successivi = []
    for _ in range(30):
        start_time = time.time()
        result = graph.run("MATCH (t:transazioni {status: 'sospetta'}) RETURN count(t) as num_transactions")
        record = result.next()  # Ottengo il record restituito dalla query
        end_time = time.time()
        tempo_esecuzione = round((end_time - start_time) * 1000, 2)

        num_transactions = record['num_transactions']
        tempi_successivi.append(tempo_esecuzione)

    tempo_medio_successive = round(sum(tempi_successivi) / len(tempi_successivi), 2)
    mean, interval = calculate_confidence_interval(tempi_successivi)
    tempi_di_risposta_media_intervallo[f"{dataset_name} - Query 4"] = (tempo_medio_successive, mean, interval)

    print("-" * 70)

# Scrivo i tempi di risposta medi della prima esecuzione in un file CSV
with open('tempi_di_risposta_prima_esecuzione.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['Dataset', 'Query', 'Millisecondi'])

    # Scrivo i dati
    for query, tempo_prima_esecuzione in tempi_di_risposta_prima_esecuzione.items():
        dataset, query = query.split(' - ')
        writer.writerow([dataset, query, tempo_prima_esecuzione])

# Scrivo i tempi di risposta medi delle 30 esecuzioni successive in un file CSV
with open('tempi_di_risposta_media_30.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['Dataset', 'Query', 'Millisecondi', 'Media', 'Intervallo di Confidenza'])

    # Scrivo i dati
    for query, (tempo_medio_successive, mean, interval) in tempi_di_risposta_media_intervallo.items():
        dataset, query = query.split(' - ')
        writer.writerow([dataset, query, tempo_medio_successive, round(mean, 2), round(interval, 2)])

print("I tempi di risposta medi sono stati scritti nei file 'tempi_di_risposta_prima_esecuzione.csv' e 'tempi_di_risposta_media_30.csv'.")
