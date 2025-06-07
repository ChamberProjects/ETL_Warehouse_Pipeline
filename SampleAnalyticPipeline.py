import zipfile
import json
import sqlite3
import os
from datetime import datetime

#Lectura y extracción de archivos JSON desde un archivo ZIP 
def extract_json_from_zip(zip_path):
    data = {}
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        for file_name in zip_ref.namelist():
            if file_name.endswith('.json') and '__MACOSX' not in file_name:
                with zip_ref.open(file_name) as f:
                    base = os.path.basename(file_name)
                    if base == 'sample_analytics.accounts.json':
                        data['accounts'] = json.load(f)
                    elif base == 'sample_analytics.customers.json':
                        data['customers'] = json.load(f)
                    elif base == 'sample_analytics.transactions.json':
                        data['transactions'] = json.load(f)
    return data['accounts'], data['customers'], data['transactions']

#Limpieza, enriquecimiento y normalización de los datos 
def transform_data(accounts, customers, transactions):
    # Transformación de cuentas: asignación de IDs artificiales y descomposición de productos
    dim_accounts = {}
    for idx, account in enumerate(accounts, start=1):
        dim_accounts[idx] = (
            account.get('limit', 0),
            ','.join(account.get('products', []))
        )
        account['__generated_account_id'] = idx

    # Transformación de clientes: asignación de IDs, manejo de fechas y estructura
    dim_customers = {}
    account_customer_mapping = []
    next_customer_id = 1

    for customer in customers:
        username = customer.get('username') or f"user{next_customer_id}"
        birthdate_raw = customer.get('birthdate', '')
        birth_date = ''

        if isinstance(birthdate_raw, dict):
            birthdate_raw = birthdate_raw.get('$date', '')
        try:
            dt = datetime.fromisoformat(birthdate_raw.replace('Z', ''))
            birth_date = dt.date().isoformat()
        except:
            birth_date = ''

        dim_customers[next_customer_id] = {
            'customer_id': next_customer_id,
            'name': customer.get('name', ''),
            'username': username,
            'birth_date': birth_date,
            'accounts': customer.get('accounts', [])
        }

        next_customer_id += 1

    #Relación muchos a muchos: mapeo entre clientes y cuentas
    for customer in dim_customers.values():
        for acc_id in customer['accounts']:
            matching_account_id = next((a['__generated_account_id'] for a in accounts if a['account_id'] == acc_id), None)
            if matching_account_id:
                account_customer_mapping.append((customer['customer_id'], matching_account_id))

    #Dimensión de fechas y hechos de transacciones: estructuración y mapeo de relaciones
    dim_dates = []
    fact_transactions = []
    date_ids = {}
    next_date_id = 1
    transaction_id = 1

    for transaction_group in transactions:
        original_account_id = transaction_group.get('account_id')
        mapped_account_id = next((a['__generated_account_id'] for a in accounts if a['account_id'] == original_account_id), None)
        if not mapped_account_id:
            continue

        transaction_count = transaction_group.get('transaction_count', 0)
        for transaction in transaction_group.get('transactions', []):
            try:
                date_str = transaction['date'].get('$date') if isinstance(transaction['date'], dict) else transaction['date']
                dt = datetime.fromisoformat(date_str.replace('Z', '')) if date_str else None
                date_key = dt.date().isoformat() if dt else None

                if not date_key:
                    continue

                if date_key not in date_ids:
                    date_ids[date_key] = next_date_id
                    dim_dates.append((next_date_id, date_key))
                    next_date_id += 1

                fact_transactions.append((
                    transaction_id,
                    mapped_account_id,
                    date_ids[date_key],
                    transaction_count,
                    transaction.get('amount', 0.0),
                    transaction.get('transaction_code', ''),
                    transaction.get('symbol', ''),
                    float(transaction.get('price', 0.0)),
                    float(transaction.get('total', 0.0))
                ))
                transaction_id += 1
            except:
                continue

    return dim_accounts, dim_customers, account_customer_mapping, dim_dates, fact_transactions

#Carga de datos transformados en base de datos SQLite (Data Warehouse)
def load_to_sqlite(dim_accounts, dim_customers, account_customer_mapping, dim_dates, fact_transactions, db_name='analytics_dw.db'):
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()

    #Creación de esquema dimensional en SQLite (modelo estrella)
    cur.executescript("""
    DROP TABLE IF EXISTS dim_accounts;
    DROP TABLE IF EXISTS dim_customers;
    DROP TABLE IF EXISTS account_customers;
    DROP TABLE IF EXISTS dim_dates;
    DROP TABLE IF EXISTS fact_transactions;

    CREATE TABLE dim_accounts (
        account_id INTEGER PRIMARY KEY,
        limit_amount INTEGER,
        products TEXT
    );

    CREATE TABLE dim_customers (
        customer_id INTEGER PRIMARY KEY,
        name TEXT,
        username TEXT,
        birth_date TEXT
    );

    CREATE TABLE account_customers (
        customer_id INTEGER,
        account_id INTEGER
    );

    CREATE TABLE dim_dates (
        date_id INTEGER PRIMARY KEY,
        date TEXT
    );

    CREATE TABLE fact_transactions (
        transaction_id INTEGER PRIMARY KEY,
        account_id INTEGER,
        date_id INTEGER,
        transaction_count INTEGER,
        amount REAL,
        transaction_type TEXT,
        symbol TEXT,
        price REAL,
        total REAL
    );
    """)

    #Inserción de datos en tablas dimensionales y tabla de hechos
    for acc_id, (limit_amount, products) in dim_accounts.items():
        cur.execute("INSERT INTO dim_accounts VALUES (?, ?, ?)", (acc_id, limit_amount, products))

    for customer_data in dim_customers.values():
        cur.execute("INSERT INTO dim_customers VALUES (?, ?, ?, ?)", (
            customer_data['customer_id'], customer_data['name'], customer_data['username'], customer_data['birth_date']
        ))

    cur.executemany("INSERT INTO account_customers VALUES (?, ?)", account_customer_mapping)
    cur.executemany("INSERT INTO dim_dates VALUES (?, ?)", dim_dates)
    cur.executemany("""
        INSERT INTO fact_transactions (
            transaction_id, account_id, date_id, transaction_count,
            amount, transaction_type, symbol, price, total
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""", fact_transactions)

    conn.commit()

    #Reporte de registros cargados por tabla
    for table in ['dim_accounts', 'dim_customers', 'account_customers', 'dim_dates', 'fact_transactions']:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        print(f"📊 {table}: {count} rows")

    conn.close()
    print("ETL completed successfully.")
s
#Ejecutamos el codigo
zip_path = 'sample_analytics_dataset.zip'
if not os.path.isfile(zip_path):
    print(f"Archivo no encontrado: {zip_path}")
else:
    accounts, customers, transactions = extract_json_from_zip(zip_path)
    dim_account, dim_customer, account_client, dim_date, fact_transactions = transform_data(accounts, customers, transactions)
    load_to_sqlite(dim_account, dim_customer, account_client, dim_date, fact_transactions)
