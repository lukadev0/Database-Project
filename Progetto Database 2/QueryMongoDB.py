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

    # Query 1: Ricerca dei commercianti con un determinato merchant_name
    selected_lastname = 'Ross-Williams'
    start_time = time.time()
    merchant = db[f'Commerciante {percentuale}'].find_one({'merchant_name': selected_lastname})
    if merchant:
        print(f"Sede dell'azienda del commerciante con il cognome specificato: {merchant['merchant_location']}")
    else:
        print(f"Nessun commerciante trovato con il cognome specificato: {selected_lastname}")
    end_time = time.time()
    tempo_prima_esecuzione = round((end_time - start_time) * 1000, 2)
    print(f"Tempo di risposta (prima esecuzione - Query 1): {tempo_prima_esecuzione} ms")

    # Aggiungi il tempo di risposta medio della prima esecuzione al dizionario per la prima query
    tempi_di_risposta_prima_esecuzione[f"{percentuale} - Query 1"] = tempo_prima_esecuzione

    # Calcolo il tempo medio delle 30 esecuzioni successive per la prima query
    tempi_successivi = []
    for _ in range(30):
        start_time = time.time()
        merchant = db[f'Commerciante {percentuale}'].find_one({'merchant_name': selected_lastname})
        end_time = time.time()
        tempo_esecuzione = round((end_time - start_time) * 1000, 2)
        tempi_successivi.append(tempo_esecuzione)

    tempo_medio_successive = round(sum(tempi_successivi) / len(tempi_successivi), 2)
    mean, margin_of_error = calculate_confidence_interval(tempi_successivi)
    print(f"Tempo medio di 30 esecuzioni successive (Query 1): {tempo_medio_successive} ms")
    print(f"Intervallo di Confidenza (Query 1): [{round(mean - margin_of_error, 2)}, {round(mean + margin_of_error, 2)}] ms\n")

    tempi_di_risposta_media[f"{percentuale} - Query 1"] = (tempo_medio_successive, mean, margin_of_error)

    # Query 2: Ricerca del numero di commercianti in una determinata nazione
    selected_country = 'Norway'
    start_time = time.time()
    count = db[f'Commerciante {percentuale}'].count_documents({'merchant_location': selected_country})
    end_time = time.time()
    tempo_prima_esecuzione = round((end_time - start_time) * 1000, 2)
    print(f"Numero di commercianti in {selected_country}: {count}")
    print(f"Tempo di risposta (prima esecuzione - Query 2): {tempo_prima_esecuzione} ms")

    # Aggiungi il tempo di risposta medio della prima esecuzione al dizionario per la seconda query
    tempi_di_risposta_prima_esecuzione[f"{percentuale} - Query 2"] = tempo_prima_esecuzione

    # Calcolo il tempo medio delle 30 esecuzioni successive per la seconda query
    tempi_successivi = []
    for _ in range(30):
        start_time = time.time()
        count = db[f'Commerciante {percentuale}'].count_documents({'merchant_location': selected_country})
        end_time = time.time()
        tempo_esecuzione = round((end_time - start_time) * 1000, 2)
        tempi_successivi.append(tempo_esecuzione)

    tempo_medio_successive = round(sum(tempi_successivi) / len(tempi_successivi), 2)
    mean, margin_of_error = calculate_confidence_interval(tempi_successivi)
    print(f"Tempo medio di 30 esecuzioni successive (Query 2): {tempo_medio_successive} ms")
    print(f"Intervallo di Confidenza (Query 2): [{round(mean - margin_of_error, 2)}, {round(mean + margin_of_error, 2)}] ms\n")

    tempi_di_risposta_media[f"{percentuale} - Query 2"] = (tempo_medio_successive, mean, margin_of_error)

    # Query 3: Ricerca del nome e del costo del prodotto associato a un cliente
    selected_user_id = 9182
    start_time = time.time()
    transactions = list(db[f'Transazioni {percentuale}'].find({'user_id': selected_user_id}))
    if transactions:
        print(f"Prodotti associati all'ID cliente {selected_user_id}:")
        for transaction in transactions:
            product_id = transaction['product_id']
            product = db[f'Prodotto {percentuale}'].find_one({'product_id': product_id})
            product_name = product.get('product_name', 'N/A')
            product_amount = product.get('amount', 'N/A')
            print(f"- {product_name} : {product_amount} euro")
    else:
        print(f"Nessuna transazione trovata per il cliente con ID {selected_user_id}.")
    end_time = time.time()
    tempo_prima_esecuzione = round((end_time - start_time) * 1000, 2)
    print(f"Tempo di risposta (prima esecuzione - Query 3): {tempo_prima_esecuzione} ms")

    # Aggiungi il tempo di risposta medio della prima esecuzione al dizionario per la terza query
    tempi_di_risposta_prima_esecuzione[f"{percentuale} - Query 3"] = tempo_prima_esecuzione

    # Calcolo il tempo medio delle 30 esecuzioni successive per la terza query
    tempi_successivi = []
    for _ in range(30):
        start_time = time.time()
        transaction = db[f'Transazioni {percentuale}'].find_one({'user_id': selected_user_id})
        if transaction:
            product_id = transaction['product_id']
            product = db[f'Prodotto {percentuale}'].find_one({'product_id': product_id})
            product_name = product.get('product_name', 'N/A')
            product_amount = product.get('amount', 'N/A')
        else:
            product_name = 'N/A'
            product_amount = 'N/A'
        end_time = time.time()
        tempo_esecuzione = round((end_time - start_time) * 1000, 2)
        tempi_successivi.append(tempo_esecuzione)

    tempo_medio_successive = round(sum(tempi_successivi) / len(tempi_successivi), 2)
    mean, margin_of_error = calculate_confidence_interval(tempi_successivi)
    print(f"Tempo medio di 30 esecuzioni successive (Query 3): {tempo_medio_successive} ms")
    print(f"Intervallo di Confidenza (Query 3): [{round(mean - margin_of_error, 2)}, {round(mean + margin_of_error, 2)}] ms\n")

    tempi_di_risposta_media[f"{percentuale} - Query 3"] = (tempo_medio_successive, mean, margin_of_error)

    # Query 4: Ricerca della quantità di prodotti con costo superiore a quello selezionato e del prodotto più costoso
    selected_amount = 990
    start_time = time.time()
    products_above_selected_amount = list(db[f'Prodotto {percentuale}'].find({'amount': {'$gt': selected_amount}}))
    count_products_above_selected_amount = len(products_above_selected_amount)

    # Trova il prodotto più costoso
    most_expensive_product = db[f'Prodotto {percentuale}'].find_one({'amount': {'$gt': selected_amount}}, sort=[('amount', -1)])
    end_time = time.time()
    tempo_prima_esecuzione = round((end_time - start_time) * 1000, 2)

    print(f"Quantità di prodotti con costo superiore a {selected_amount} euro: {count_products_above_selected_amount}")
    if most_expensive_product:
        print(f"Prodotto più costoso con costo di {most_expensive_product['amount']} euro: {most_expensive_product['product_name']}")
    else:
        print("Nessun prodotto con costo superiore a quello selezionato trovato.")

    print(f"Tempo di risposta (prima esecuzione - Query 4): {tempo_prima_esecuzione} ms")

    # Aggiungi il tempo di risposta medio della prima esecuzione al dizionario per la quarta query
    tempi_di_risposta_prima_esecuzione[f"{percentuale} - Query 4"] = tempo_prima_esecuzione

    # Calcolo il tempo medio delle 30 esecuzioni successive per la quarta query

    tempi_successivi = []
    for _ in range(30):
        start_time = time.time()
        pipeline = [
            {'$match': {'amount': {'$gt': selected_amount}}},
            {'$lookup': {
                'from': f'Prodotto {percentuale}',
                'localField': 'product_id',
                'foreignField': 'product_id',
                'as': 'product_info'
            }},
            {'$project': {'product_name': '$product_info.product_name', 'amount': '$product_info.amount'}}
        ]
        products_over_amount = list(db[f'Prodotto {percentuale}'].aggregate(pipeline))

        end_time = time.time()
        tempo_esecuzione = round((end_time - start_time) * 1000, 2)
        tempi_successivi.append(tempo_esecuzione)

    tempo_medio_successive = round(sum(tempi_successivi) / len(tempi_successivi), 2)
    mean, margin_of_error = calculate_confidence_interval(tempi_successivi)
    print(f"Tempo medio di 30 esecuzioni successive (Query 4): {tempo_medio_successive} ms")
    print(f"Intervallo di Confidenza (Query 4): [{round(mean - margin_of_error, 2)}, {round(mean + margin_of_error, 2)}] ms\n")

    tempi_di_risposta_media[f"{percentuale} - Query 4"] = (tempo_medio_successive, mean, margin_of_error)

    print("-" * 70)  # Separatore tra le diverse percentuali

# Scrivo i tempi di risposta medi della prima esecuzione in un file CSV
with open('tempi_di_risposta_prima_esecuzione_MDB.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['Dataset', 'Query', 'Millisecondi'])

    # Scrivo i dati
    for query, tempo_prima_esecuzione in tempi_di_risposta_prima_esecuzione.items():
        dataset, query = query.split(' - ')
        writer.writerow([dataset, query, tempo_prima_esecuzione])

# Scrivo i tempi di risposta medi delle 30 esecuzioni successive in un file CSV
with open('tempi_di_risposta_media_30_MDB.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['Dataset', 'Query', 'Millisecondi', 'Media', 'Intervallo di Confidenza (Min, Max)'])

    # Scrivo i dati
    for query, (tempo_medio_successive, mean_value, margin_of_error) in tempi_di_risposta_media.items():
        dataset, query = query.split(' - ')
        min_interval = round(mean_value - margin_of_error, 2)
        max_interval = round(mean_value + margin_of_error, 2)
        intervallo_di_confidenza = (min_interval, max_interval)  # Creo una tupla con minimo e massimo
        writer.writerow([dataset, query, tempo_medio_successive, round(mean_value, 2), intervallo_di_confidenza])

print("I tempi di risposta medi sono stati scritti nei file 'tempi_di_risposta_prima_esecuzione_MDB.csv' e 'tempi_di_risposta_media_30_MDB.csv'.")
