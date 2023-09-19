import time
import csv
import numpy as np
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

    selected_country = 'Norway'
    selected_user_id = 2504  # Sostituisci con l'ID dell'utente desiderato per la seconda query
    dataset_name = f"{percentuale}%"

    # Query 1: Ricerca dei commercianti con un determinato merchant_name
    print("Query 1:")

    start_time = time.time()
    result = graph.run("MATCH (c:commerciante {merchant_name: 'Ross-Williams'}) RETURN c.merchant_name AS merchant_name, c.merchant_location AS merchant_location")
    records = list(result)
    end_time = time.time()
    tempo_prima_esecuzione = round((end_time - start_time) * 1000, 2)

    if records:
        for record in records:
            merchant_name = record['merchant_name']
            merchant_location = record['merchant_location']
            print(f"Sede dell'azienda del commerciante {merchant_name}: {merchant_location}\n")
    else:
        print(f"Nessun commerciante trovato con il merchant_name cercato.")

    print(f"Tempo di risposta (prima esecuzione - Query 1): {tempo_prima_esecuzione} ms")

    # Salva il tempo di risposta della prima esecuzione nel dizionario
    tempi_di_risposta_prima_esecuzione[f"{percentuale}% - Query 1"] = tempo_prima_esecuzione

    # Calcolo il tempo medio delle 30 esecuzioni successive per la prima query
    tempi_successivi = []
    for _ in range(30):
        start_time = time.time()
        result = graph.run("MATCH (c:commerciante {merchant_name: 'Ross-Williams'}) RETURN  c.merchant_name AS merchant_name, c.merchant_location AS merchant_location")
        records = list(result)
        end_time = time.time()
        tempo_esecuzione = round((end_time - start_time) * 1000, 2)

        tempi_successivi.append(tempo_esecuzione)

    tempo_medio_successive = round(sum(tempi_successivi) / len(tempi_successivi), 2)
    mean, interval = calculate_confidence_interval(tempi_successivi)
    tempi_di_risposta_media_intervallo[f"{dataset_name} - Query 1"] = (tempo_medio_successive, mean, interval)

    print(f"Tempo medio di 30 esecuzioni successive (Query 1): {tempo_medio_successive} ms")
    print(f"Intervallo di Confidenza (Query 1): [{round(mean - interval, 2)}, {round(mean + interval, 2)}] ms\n")

    # Query 2: Ricerca del numero di commercianti in una determinata nazione
    print("Query 2:")

    start_time = time.time()
    result = graph.run("MATCH (c:commerciante) WHERE c.merchant_location = $country RETURN count(c) AS num_merchants", country=selected_country)
    record = result.next()  # Ottengo il record restituito dalla query
    num_merchants = record['num_merchants']
    end_time = time.time()

    tempo_prima_esecuzione = round((end_time - start_time) * 1000, 2)
    print(f"Numero totale di commercianti in {selected_country}: {num_merchants}\n")
    print(f"Tempo di risposta (prima esecuzione - Query 2): {tempo_prima_esecuzione} ms")

    # Salva il tempo di risposta della prima esecuzione nel dizionario
    tempi_di_risposta_prima_esecuzione[f"{percentuale}% - Query 2"] = tempo_prima_esecuzione

    # Calcolo il tempo medio delle 30 esecuzioni successive per la terza query
    tempi_successivi = []
    for _ in range(30):
        start_time = time.time()
        result = graph.run("MATCH (c:commerciante) WHERE c.merchant_location = $country RETURN count(c) AS num_merchants", country=selected_country)
        record = result.next()  # Ottengo il record restituito dalla query
        end_time = time.time()
        tempo_esecuzione = round((end_time - start_time) * 1000, 2)

        tempi_successivi.append(tempo_esecuzione)

    tempo_medio_successive = round(sum(tempi_successivi) / len(tempi_successivi), 2)
    mean, interval = calculate_confidence_interval(tempi_successivi)
    tempi_di_risposta_media_intervallo[f"{dataset_name} - Query 2"] = (tempo_medio_successive, mean, interval)

    print(f"Tempo medio di 30 esecuzioni successive (Query 2): {tempo_medio_successive} ms")
    print(f"Intervallo di Confidenza (Query 2): [{round(mean - interval, 2)}, {round(mean + interval, 2)}] ms\n")

    # Query 3: Ricerca del nome e del costo del prodotto associato a un cliente
    print("Query 3:")

    start_time = time.time()
    result = graph.run("MATCH(u:utenti {user_id: $user_id})-[:EFFETTUA]->(t:transazioni)-[:CONCERNE]->(p:prodotti) RETURN p.product_name AS product_name, toFloat(t.amount) AS amount", user_id=selected_user_id)
    records = list(result)
    end_time = time.time()
    tempo_prima_esecuzione = round((end_time - start_time) * 1000, 2)

    if records:
        print(f"Prodotti associati all'utente con ID {selected_user_id}:")
        for record in records:
            product_name = record['product_name']
            amount = record['amount']
            print(f"- Prodotto: {product_name}, Costo: {amount} euro")
    else:
        print(f"Nessuna transazione trovata per l'utente con ID {selected_user_id}.")

    print(f"\nTempo di risposta (prima esecuzione - Query 3): {tempo_prima_esecuzione} ms")

    # Salva il tempo di risposta della prima esecuzione nel dizionario
    tempi_di_risposta_prima_esecuzione[f"{percentuale}% - Query 3"] = tempo_prima_esecuzione

    # Calcolo il tempo medio delle 30 esecuzioni successive per la seconda query
    tempi_successivi = []
    for _ in range(30):
        start_time = time.time()
        result = graph.run("MATCH (u:utenti {user_id: $user_id})-[:EFFETTUA]->(t:transazioni)-[:CONCERNE]->(p:prodotti) RETURN p.product_name AS product_name, toFloat(t.amount) AS amount", user_id=selected_user_id)

        records = list(result)
        end_time = time.time()
        tempo_esecuzione = round((end_time - start_time) * 1000, 2)

        tempi_successivi.append(tempo_esecuzione)

    tempo_medio_successive = round(sum(tempi_successivi) / len(tempi_successivi), 2)
    mean, interval = calculate_confidence_interval(tempi_successivi)
    tempi_di_risposta_media_intervallo[f"{dataset_name} - Query 3"] = (tempo_medio_successive, mean, interval)

    print(f"Tempo medio di 30 esecuzioni successive (Query 3): {tempo_medio_successive} ms")
    print(f"Intervallo di Confidenza (Query 3): [{round(mean - interval, 2)}, {round(mean + interval, 2)}] ms\n")

    # Query 4: Ricerca della quantità di prodotti con costo superiore a quello selezionato e del prodotto più costoso
    print("Query 4:")

    selected_amount = 990  # Sostituisci con il costo desiderato
    start_time = time.time()
    result = graph.run("""
        MATCH (t:transazioni)-[:CONCERNE]->(p:prodotti)
        WHERE t.amount > $amount
        WITH t, p
        ORDER BY t.amount DESC
        LIMIT 1
        RETURN count(t) AS num_transactions, max(t.amount) AS max_amount, p.product_name AS most_expensive_product
        """, amount=selected_amount)

    record = result.next()  # Ottengo il record restituito dalla query
    max_amount = record['max_amount']
    num_transactions = record['num_transactions']
    most_expensive_product = record['most_expensive_product']
    end_time = time.time()

    tempo_prima_esecuzione = round((end_time - start_time) * 1000, 2)

    print(f"Numero di transazioni con costo superiore a {selected_amount} euro: {num_transactions}")
    print(f"Prodotto più costoso tra le transazioni: {most_expensive_product} - {max_amount} euro\n")
    print(f"Tempo di risposta (prima esecuzione - Query 4): {tempo_prima_esecuzione} ms")

    # Salva il tempo di risposta della prima esecuzione nel dizionario
    tempi_di_risposta_prima_esecuzione[f"{percentuale}% - Query 4"] = tempo_prima_esecuzione

    # Calcolo il tempo medio delle 30 esecuzioni successive per la quarta query
    tempi_successivi = []
    for _ in range(30):
        start_time = time.time()
        result = graph.run("""
                MATCH (t:transazioni)-[:CONCERNE]->(p:prodotti)
                WHERE t.amount > $amount
                WITH t, p
                ORDER BY t.amount DESC
                LIMIT 1
                RETURN count(t) AS num_transactions, max(t.amount) AS max_amount, p.product_name AS most_expensive_product
                """, amount=selected_amount)
        record = result.next()  # Ottengo il record restituito dalla query
        end_time = time.time()
        tempo_esecuzione = round((end_time - start_time) * 1000, 2)

        tempi_successivi.append(tempo_esecuzione)

    tempo_medio_successive = round(sum(tempi_successivi) / len(tempi_successivi), 2)
    mean, interval = calculate_confidence_interval(tempi_successivi)
    tempi_di_risposta_media_intervallo[f"{dataset_name} - Query 4"] = (tempo_medio_successive, mean, interval)

    print(f"Tempo medio di 30 esecuzioni successive (Query 4): {tempo_medio_successive} ms")
    print(f"Intervallo di Confidenza (Query 4): [{round(mean - interval, 2)}, {round(mean + interval, 2)}] ms\n")

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
    writer.writerow(['Dataset', 'Query', 'Millisecondi', 'Media', 'Intervallo di Confidenza (Min, Max)'])

    # Scrivo i dati
    for query, (tempo_medio_successive, mean, interval) in tempi_di_risposta_media_intervallo.items():
        dataset, query = query.split(' - ')
        min_interval = round(mean - interval, 2)
        max_interval = round(mean + interval, 2)
        intervallo_di_confidenza = (min_interval, max_interval)  # Creo una tupla con minimo e massimo
        writer.writerow([dataset, query, tempo_medio_successive, round(mean, 2), intervallo_di_confidenza])

print("I tempi di risposta medi sono stati scritti nei file 'tempi_di_risposta_prima_esecuzione.csv' e 'tempi_di_risposta_media_30.csv'.")