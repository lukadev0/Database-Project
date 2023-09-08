import matplotlib.pyplot as plt
import pandas as pd

# Leggi i dati dai file CSV
mdb_prima_esecuzione = pd.read_csv("tempi_di_risposta_prima_esecuzione_MDB.csv")
mdb_media_30 = pd.read_csv("tempi_di_risposta_media_30_MDB.csv")
neo4j_prima_esecuzione = pd.read_csv("C:\\Users\\lucac\\PycharmProjects\\pythonProject\\Neo4j\\tempi_di_risposta_prima_esecuzione.csv")
neo4j_media_30 = pd.read_csv("C:\\Users\\lucac\\PycharmProjects\\pythonProject\\Neo4j\\tempi_di_risposta_media_30.csv")

# Prendi i dati per il confronto della prima esecuzione
mdb_prima_esecuzione_queries = mdb_prima_esecuzione['Query']
mdb_prima_esecuzione_tempi = mdb_prima_esecuzione['Millisecondi']

neo4j_prima_esecuzione_queries = neo4j_prima_esecuzione['Query']
neo4j_prima_esecuzione_tempi = neo4j_prima_esecuzione['Millisecondi']

# Prendi i dati per il confronto dei tempi medi e degli intervalli di confidenza
mdb_media_30_queries = mdb_media_30['Query']
mdb_media_30_tempi = mdb_media_30['Media']
mdb_media_30_intervallo = mdb_media_30['Intervallo di Confidenza']

neo4j_media_30_queries = neo4j_media_30['Query']
neo4j_media_30_tempi = neo4j_media_30['Media']
neo4j_media_30_intervallo = neo4j_media_30['Intervallo di Confidenza']

# Colore per MongoDB (verde) e Neo4j (celeste)
colore_mdb = 'coral'
colore_neo4j = 'purple'

# Istogramma per il confronto dei tempi di risposta della prima esecuzione tra MongoDB e Neo4j
plt.figure(figsize=(12, 6))
bar_width = 0.35
index = range(len(mdb_prima_esecuzione_queries))

# Barre per MongoDB
plt.bar([i - bar_width/2 for i in index], mdb_prima_esecuzione_tempi, bar_width, label='MongoDB', alpha=0.7, color=colore_mdb)
# Barre per Neo4j
plt.bar([i + bar_width/2 for i in index], neo4j_prima_esecuzione_tempi, bar_width, label='Neo4j', alpha=0.7, color=colore_neo4j)
plt.xlabel('Query')
plt.ylabel('Tempo di risposta (ms)')
plt.title('Confronto dei tempi di risposta della prima esecuzione tra MongoDB e Neo4j')
plt.xticks(index, mdb_prima_esecuzione_queries, rotation=45, ha="right")  # Aggiunta delle etichette delle query
plt.legend()
plt.tight_layout()
plt.savefig('confronto_prima_esecuzione.png')
plt.show()

# Istogramma per il confronto dei tempi medi e degli intervalli di confidenza tra MongoDB e Neo4j
plt.figure(figsize=(12, 6))
bar_width = 0.35
index = range(len(mdb_media_30_queries))

plt.bar(index, mdb_media_30_tempi, bar_width, label='Media MongoDB', alpha=0.7, color=colore_mdb)
plt.bar([i + bar_width for i in index], neo4j_media_30_tempi, bar_width, label='Media Neo4j', alpha=0.7, color=colore_neo4j)

# Aggiungi gli intervalli di confidenza con i pallini
plt.errorbar(
    [i + bar_width / 2 for i in index],
    mdb_media_30_tempi,
    yerr=mdb_media_30_intervallo,
    fmt='o',
    label='Intervallo di Confidenza MongoDB',
    alpha=0.7,
    color=colore_mdb,
    capsize=4,
    errorevery=1,
)
plt.errorbar(
    [i + bar_width * 1.5 for i in index],
    neo4j_media_30_tempi,
    yerr=neo4j_media_30_intervallo,
    fmt='o',
    label='Intervallo di Confidenza Neo4j',
    alpha=0.7,
    color=colore_neo4j,
    capsize=4,
    errorevery=1,
)

plt.xlabel('Query')
plt.ylabel('Tempo di risposta (ms)')
plt.title('Confronto dei tempi medi e degli intervalli di confidenza tra MongoDB e Neo4j')
plt.xticks([i + bar_width for i in index], mdb_media_30_queries, rotation=45, ha="right")
plt.legend()
plt.tight_layout()
plt.savefig('confronto_media_30.png')
plt.show()
