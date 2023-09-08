from pymongo import MongoClient
import time
import csv
import scipy.stats as stats
import numpy as np


def calculate_confidence_interval(data):
    confidence = 0.95
    n = len(data)
    mean_value = np.mean(data)
    stderr = stats.sem(data)
    margin_of_error = stderr * stats.t.ppf((1 + confidence) / 2, n - 1)
    return mean_value, margin_of_error


client = MongoClient('mongodb://localhost:27017/')
db = client['Frodi']

percentuali = ['100%', '75%', '50%', '25%']

# Verifico la connessione al database
print(f"Connesso al database: {db.name}")

# Dizionario per i tempi di risposta medi della prima esecuzione per ogni percentuale
tempi_di_risposta_prima_esecuzione = {}

# Dizionario per i tempi di risposta medi delle 30 esecuzioni successive per ogni percentuale
tempi_di_risposta_media = {}

for percentuale in percentuali:
    print(f"\nAnalisi per la percentuale: {percentuale}\n")

    selected_country = 'Italy'  # Cambia con il paese desiderato

    # Calcolo il tempo medio della prima esecuzione per la prima query
    start_time = time.time()
    count = db[percentuale].count_documents({'country': selected_country})
    end_time = time.time()
    tempo_prima_esecuzione = round((end_time - start_time) * 1000, 2)
    print(f"Numero totale di transazioni in {selected_country}: {count}")
    print(f"Tempo di risposta (prima esecuzione - Query 1): {tempo_prima_esecuzione} ms\n")

    # Qui aggiungo il tempo di risposta medio della prima esecuzione al dizionario per la prima query
    tempi_di_risposta_prima_esecuzione[f"{percentuale} - Query 1"] = tempo_prima_esecuzione

    # Calcolo il tempo medio delle 30 esecuzioni successive per la prima query
    tempi_successivi = []
    for _ in range(30):
        start_time = time.time()
        count = db[percentuale].count_documents({'country': selected_country})
        end_time = time.time()
        tempo_esecuzione = round((end_time - start_time) * 1000, 2)
        tempi_successivi.append(tempo_esecuzione)

    tempo_medio_successive = round(sum(tempi_successivi) / len(tempi_successivi), 2)
    mean, margin_of_error = calculate_confidence_interval(tempi_successivi)
    print(f"Tempo medio di 30 esecuzioni successive (Query 1): {tempo_medio_successive} ms")
    print(f"Intervallo di Confidenza (Query 1): [{round(mean - margin_of_error, 2)}, {round(mean + margin_of_error, 2)}] ms\n")

    tempi_di_risposta_media[f"{percentuale} - Query 1"] = (tempo_medio_successive, mean, margin_of_error)

    selected_country = 'Italy'  # Cambia con il paese desiderato

    # Calcolo il tempo medio della prima esecuzione per la seconda query
    start_time = time.time()
    pipeline = [
        {'$match': {'country': selected_country, 'status': 'sospetta'}},
        {'$group': {'_id': None, 'total_amount': {'$sum': '$amount'}}}
    ]
    result = list(db[percentuale].aggregate(pipeline))
    end_time = time.time()
    total_amount = result[0]['total_amount'] if result else 0
    tempo_prima_esecuzione = round((end_time - start_time) * 1000, 2)
    print(f"Totale degli importi delle transazioni fraudolente in {selected_country}: {total_amount}")
    print(f"Tempo di risposta (prima esecuzione - Query 2): {tempo_prima_esecuzione} ms\n")

    tempi_di_risposta_prima_esecuzione[f"{percentuale} - Query 2"] = tempo_prima_esecuzione

    # Calcolo il tempo medio delle 30 esecuzioni successive per la seconda query
    tempi_successivi = []

    for _ in range(30):
        start_time = time.time()
        pipeline = [
            {'$match': {'country': selected_country, 'status': 'sospetta'}},
            {'$group': {'_id': None, 'total_amount': {'$sum': '$amount'}}}
        ]
        result = list(db[percentuale].aggregate(pipeline))
        end_time = time.time()
        total_amount = result[0]['total_amount'] if result else 0
        tempo_esecuzione = round((end_time - start_time) * 1000, 2)
        tempi_successivi.append(tempo_esecuzione)

    tempo_medio_successive = round(sum(tempi_successivi) / len(tempi_successivi), 2)
    mean_value, margin_of_error = calculate_confidence_interval(tempi_successivi)
    print(f"Tempo medio di 30 esecuzioni successive (Query 2): {tempo_medio_successive} ms")
    print(f"Intervallo di Confidenza (Query 2): [{round(mean_value - margin_of_error, 2)}, {round(mean_value + margin_of_error, 2)}] ms\n")

    tempi_di_risposta_media[f"{percentuale} - Query 2"] = (tempo_medio_successive, mean_value, margin_of_error)

    selected_username = 'Jennifer Rodgers'  # Qui bisogna cambiare il nome dell'utente desiderato

    # Calcola il tempo medio della prima esecuzione per la terza query
    start_time = time.time()
    pipeline = [
        {'$match': {'subject': selected_username, 'status': 'sospetta'}},
        {'$count': 'fraudulent_transactions'}
    ]
    result = list(db[percentuale].aggregate(pipeline))
    end_time = time.time()
    fraudulent_transactions = result[0]['fraudulent_transactions'] if result else 0
    tempo_prima_esecuzione = round((end_time - start_time) * 1000, 2)
    print(f"Numero totale di transazioni fraudolente per l'utente {selected_username}: {fraudulent_transactions}")
    print(f"Tempo di risposta (prima esecuzione - Query 3): {tempo_prima_esecuzione} ms\n")

    # Aggiungo il tempo di risposta medio della prima esecuzione al dizionario per la terza query
    tempi_di_risposta_prima_esecuzione[f"{percentuale} - Query 3"] = tempo_prima_esecuzione

    # Calcolo il tempo medio delle 30 esecuzioni successive per la terza query
    tempi_successivi = []
    for _ in range(30):
        start_time = time.time()
        pipeline = [
            {'$match': {'subject': selected_username, 'status': 'sospetta'}},
            {'$count': 'fraudulent_transactions'}
        ]
        result = list(db[percentuale].aggregate(pipeline))
        end_time = time.time()
        fraudulent_transactions = result[0]['fraudulent_transactions'] if result else 0
        tempo_esecuzione = round((end_time - start_time) * 1000, 2)
        tempi_successivi.append(tempo_esecuzione)

    tempo_medio_successive = round(sum(tempi_successivi) / len(tempi_successivi), 2)
    mean_value, margin_of_error = calculate_confidence_interval(tempi_successivi)
    print(f"Tempo medio di 30 esecuzioni successive (Query 3): {tempo_medio_successive} ms")
    print(f"Intervallo di Confidenza (Query 3): [{round(mean_value - margin_of_error, 2)}, {round(mean_value + margin_of_error, 2)}] ms\n")

    tempi_di_risposta_media[f"{percentuale} - Query 3"] = (tempo_medio_successive, mean_value, margin_of_error)

    # Calcolo il tempo medio della prima esecuzione per la quarta query
    start_time = time.time()
    count = db[percentuale].count_documents({'status': 'sospetta'})
    end_time = time.time()
    tempo_prima_esecuzione = round((end_time - start_time) * 1000, 2)
    print(f"Numero totale di transazioni fraudolente nel dataset {percentuale}: {count}")
    print(f"Tempo di risposta (prima esecuzione - Query 4): {tempo_prima_esecuzione} ms\n")

    # Aggiungi il tempo di risposta medio della prima esecuzione al dizionario per la quarta query
    tempi_di_risposta_prima_esecuzione[f"{percentuale} - Query 4"] = tempo_prima_esecuzione

    # Calcola il tempo medio delle 30 esecuzioni successive per la quarta query
    tempi_successivi = []
    for _ in range(30):
        start_time = time.time()
        count = db[percentuale].count_documents({'status': 'sospetta'})
        end_time = time.time()
        tempo_esecuzione = round((end_time - start_time) * 1000, 2)
        tempi_successivi.append(tempo_esecuzione)

    tempo_medio_successive = round(sum(tempi_successivi) / len(tempi_successivi), 2)
    mean_value, margin_of_error = calculate_confidence_interval(tempi_successivi)
    print(f"Tempo medio di 30 esecuzioni successive (Query 4): {tempo_medio_successive} ms")
    print(f"Intervallo di Confidenza (Query 4): [{round(mean_value - margin_of_error, 2)}, {round(mean_value + margin_of_error, 2)}] ms\n")

    # Aggiungi il tempo di risposta medio della prima esecuzione al dizionario per la quarta query
    tempi_di_risposta_media[f"{percentuale} - Query 4"] = (tempo_medio_successive, mean_value, margin_of_error)

    print("-" * 60)  # Separatore tra le diverse percentuali

# Scrivi i tempi di risposta medi della prima esecuzione in un file CSV
with open('tempi_di_risposta_prima_esecuzione_MDB.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['Dataset', 'Query', 'Millisecondi'])

    # Scrivi i dati
    for query, tempo_prima_esecuzione in tempi_di_risposta_prima_esecuzione.items():
        dataset, query = query.split(' - ')
        writer.writerow([dataset, query, tempo_prima_esecuzione])

# Scrivi i tempi di risposta medi delle 30 esecuzioni successive in un file CSV
with open('tempi_di_risposta_media_30_MDB.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['Dataset', 'Query', 'Millisecondi', 'Media', 'Intervallo di Confidenza'])

    # Scrivi i dati
    for query, (tempo_medio_successive, mean_value, margin_of_error) in tempi_di_risposta_media.items():
        dataset, query = query.split(' - ')
        writer.writerow([dataset, query, tempo_medio_successive, round(mean_value, 2), round(margin_of_error, 2)])

print("I tempi di risposta medi sono stati scritti nei file 'tempi_di_risposta_prima_esecuzione_MDB.csv' e 'tempi_di_risposta_media_30_MDB.csv'.")
