import random
import csv
from faker import Faker

fake = Faker()


product_names = ['palla', 'cellulare', 'computer', 'libro', 'orologio', 'scarpe', 'borsa', 'giocattolo', 'televisore', 'occhiali',
                 'bracciale', 'collana', 'maglia', 'pantaloni', 'automobile', 'tablet', 'console', 'ocarina', 'frullatore', 'tappeto',
                 'bicicletta', 'stampante', 'ventilatore', 'occhiali da sole', 'telecamera', 'microfono', 'altoparlante', 'radio',
                 'macchina fotografica', 'radiatore', 'ventola', 'trapano', 'frigorifero', 'tostapane', 'forno', 'cuffie', 'piatto',
                 'bicchiere', 'cucchiaio', 'forchetta', 'coltello', 'tazza', 'tavolo', 'sedia', 'divano', 'letto', 'lampada', 'Auricolari wireless',
                 'Videogiochi', 'Stampante laser', 'Forno a microonde', 'Aspirapolvere robot', 'Macchina da caffè', 'Tablet Android', 'Monitor PC',
                 'Giocattoli per bambini', 'Frigorifero doppia porta', 'Telefono cellulare', 'Smart TV 4K', 'Tostapane', 'Borsa da viaggio',
                 'Cuffie Bluetooth', 'Macchina per il fitness', 'Videocamera HD', 'Piumino', 'Sedia da ufficio', 'Bicicletta da corsa',
                 'Orologio da polso', 'Scarpe da ginnastica', 'Libri per bambini', 'Telefono cordless', 'Macchina fotografica digitale',
                 'Tavolo da pranzo', 'Abbigliamento sportivo', 'Altoparlante Bluetooth', 'Letto a baldacchino', 'Lavatrice', 'Tappeto persiano',
                 'Penna stilografica', 'Fornello a gas', 'Pentola a pressione', 'Portatile', 'Orologio da parete', 'Pantaloni in jeans',
                 'Ombrello pieghevole', 'Olio di oliva extra vergine', 'Set di pentole antiaderenti',  'Set di posate in acciaio inox',
                 'Tavolo da gioco', 'Kit per il trucco', 'Cuscino memory foam', 'Pantofole da casa', 'Videoproiettore HD', 'Valigia rigida',
                 'Lavastoviglie', 'Vino rosso pregiato', 'Smartwatch']

# Definizione delle costanti
NUM_UTENTI = 10000
NUM_COMMERCIANTI = 10000
NUM_PRODOTTI = 10000
NUM_TRANSAZIONI = 20000

# Inizializza le liste per tracciare gli ID già assegnati
used_user_ids = set()
used_merchant_ids = set()
used_product_ids = set()

# Creazione della collezione 'Utenti'
with open('dataset_utenti.csv', 'w', newline='') as csvfile:
    fieldnames = ['user_id', 'user_name', 'user_email', 'user_address']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for user_id in range(1, NUM_UTENTI + 1):
        user_name = fake.name()
        user_email = fake.email()
        user_address = fake.address()

        writer.writerow({
            'user_id': user_id,
            'user_name': user_name,
            'user_email': user_email,
            'user_address': user_address,
        })
        used_user_ids.add(user_id)  # Aggiungi user_id alla lista degli ID già assegnati

print("File CSV 'dataset_utenti.csv' creato con successo.")

# Creazione della collezione 'Commercianti'
with open('dataset_commerciante.csv', 'w', newline='') as csvfile:
    fieldnames = ['merchant_id', 'merchant_name', 'merchant_category', 'merchant_location', 'merchant_description']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for merchant_id in range(1, NUM_COMMERCIANTI + 1):
        merchant_name = fake.company()
        merchant_category = fake.bs()
        merchant_location = fake.country()
        merchant_description = fake.catch_phrase()

        writer.writerow({
            'merchant_id': merchant_id,
            'merchant_name': merchant_name,
            'merchant_category': merchant_category,
            'merchant_location': merchant_location,
            'merchant_description': merchant_description,
        })
        used_merchant_ids.add(merchant_id)  # Aggiungi merchant_id alla lista degli ID già assegnati

print("File CSV 'dataset_commerciante.csv' creato con successo.")

# Creazione della collezione 'Prodotti'
with open('dataset_prodotti.csv', 'w', newline='') as csvfile:
    fieldnames = ['product_id', 'product_name', 'amount', 'product_quantity']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for product_id in range(1, NUM_PRODOTTI + 1):
        product_name = random.choice(product_names)  # Genera un nome di prodotto casuale
        amount = round(random.uniform(1, 1000), 2)  # Prezzo casuale tra 1 e 1000 con due decimali
        product_quantity = random.randint(1, 5000)  # Quantità casuale tra 1 e 5000

        writer.writerow({
            'product_id': product_id,
            'product_name': product_name,
            'amount': amount,
            'product_quantity': product_quantity,
        })
        used_product_ids.add(product_id)  # Aggiungi product_id alla lista degli ID già assegnati

print("File CSV 'dataset_prodotti.csv' creato con successo.")

# Creazione della collezione 'Transazioni'
with open('dataset_transazioni.csv', 'w', newline='') as csvfile:
    fieldnames = ['transaction_id', 'user_id', 'merchant_id', 'product_id', 'amount']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for transaction_id in range(1, NUM_TRANSAZIONI + 1):
        user_id = random.choice(list(used_user_ids))
        merchant_id = random.choice(list(used_merchant_ids))
        product_id = random.choice(list(used_product_ids))
        amount = 0.0

        # Trova l'amount (prezzo) corrispondente all'ID del prodotto
        with open('dataset_prodotti.csv', newline='') as product_csv:
            product_reader = csv.DictReader(product_csv)
            for row in product_reader:
                if int(row['product_id']) == product_id:
                    amount = float(row['amount'])
                    break

        writer.writerow({
            'transaction_id': transaction_id,
            'user_id': user_id,
            'merchant_id': merchant_id,
            'product_id': product_id,
            'amount': amount,
        })

print("File CSV 'dataset_transazioni.csv' creato con successo.")