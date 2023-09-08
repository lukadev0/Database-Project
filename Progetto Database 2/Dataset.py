from faker import Faker
import random
import csv

fake = Faker()

european_countries = ['Austria', 'Belgium', 'Bulgaria', 'Croatia', 'Cyprus', 'Czech Republic', 'Denmark', 'Estonia',
                     'Finland', 'France', 'Germany', 'Greece', 'Hungary', 'Ireland', 'Italy', 'Latvia', 'Lithuania',
                     'Luxembourg', 'Malta', 'Netherlands', 'Norway', 'Poland', 'Portugal', 'Romania', 'Slovakia',
                     'Slovenia', 'Spain', 'Sweden', 'Switzerland', 'United Kingdom']

#Definizione delle costanti
NUM_TRANSACTIONS = 10000
FRAUD_PROBABILITY = 0.05  # Probabilità del 5% di frode

# Cosi apro il file CSV per la scrittura dati
with open('dataset_frodi.csv', 'w', newline='') as csvfile:
    fieldnames = ['transaction_id', 'timestamp', 'amount', 'transaction_type', 'subject', 'description', 'status', 'country']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for i in range(NUM_TRANSACTIONS):
        transaction_id = i + 1
        timestamp = fake.date_time_this_year()
        amount = round(random.uniform(10, 1000), 2)
        transaction_type = random.choice(['acquisto', 'prelievo', 'trasferimento'])
        subject = fake.name()
        description = fake.sentence()
        country = fake.country()  # Genera una nazione casuale

        # Determina se è un caso di frode
        is_fraud = country in european_countries
        status = 'sospetta' if is_fraud else 'autorizzata'

        writer.writerow({
            'transaction_id': transaction_id,
            'timestamp': timestamp,
            'amount': amount,
            'transaction_type': transaction_type,
            'subject': subject,
            'description': description,
            'status': status,
            'country': country  # Includi il campo "country" con il valore casuale
        })

print("File CSV 'dataset_frodi.csv' creato con successo.")
