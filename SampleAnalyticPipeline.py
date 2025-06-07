# Importación de librerías necesarias
import zipfile  # Para descomprimir archivos .zip
import json     # Para trabajar con archivos JSON
import sqlite3  # Para conectarnos a la base de datos SQLite
import os       # Para validar rutas y archivos
from datetime import datetime  # Para manejar fechas

# Extraer Zip
def extract_json_from_zip(zip_path):
    data = {}
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        for file_name in zip_ref.namelist():
            if file_name.endswith('.json') and '__MACOSX' not in file_name:
                with zip_ref.open(file_name) as f:
                    base = os.path.basename(file_name)
                    # Clasificamos los archivos según su nombre
                    if base == 'sample_analytics.accounts.json':
                        data['accounts'] = json.load(f)
                    elif base == 'sample_analytics.customers.json':
                        data['customers'] = json.load(f)
                    elif base == 'sample_analytics.transactions.json':
                        data['transactions'] = json.load(f)
    return data['accounts'], data['customers'], data['transactions']

# Transformar los datos
def transform_data(accounts, customers, transactions):
    dim_accounts = {}
    for idx, account in enumerate(accounts, start=1):
        # Creamos IDs propios y extraemos campos relevantes
        dim_accounts[idx] = (
            account.get('limit', 0),
            ','.join(account.get('products', []))
        )
        # Guardamos el nuevo ID en el objeto original
        account['__generated_account_id'] = idx

    dim_customers = {}
    account_customer_mapping = []
    next_customer_id = 1

    for customer in customers:
        username = customer.get('username') or f"user{next_customer_id}"
        birthdate_raw = customer.get('birthdate', '')
        if isinstance(birthdate_raw, dict):
            birthdate_raw = birthdate_raw.get('$date', '')
        try:
            dt = datetime.fromisoformat(birthdate_raw.replace('Z', ''))
            birth_date = dt.date().isoformat()
        except:
            birth_date = ''

        # Guardamos cliente con nuevo ID
        dim_customers[next_customer_id] = {
            'customer_id': next_customer_id,
            'name': customer.get('name', ''),
            'username': username,
            'birth_date': birth_date,
            'accounts': customer.get('accounts', [])
        }
        next_customer_id += 1

    # Mapeamos la relación customer -> account
    for customer in dim_customers.values():
        for acc_id in customer['accounts']:
            matching_account_id = next((a['__generated_account_id'] for a in accounts if a['account_id'] == acc_id), None)
            if matching_account_id:
                account_customer_mapping.append((customer['customer_id'], matching_account_id))

    dim_dates = []
    fact_transactions = []
    date_ids = {}
    next_date_id = 1
    transaction_id = 1

    for transaction_group in transactions:
        original_account_id = transaction_group.get('account_id')
        mapped_account_id = next((a['__generated_account_id'] for a in accounts if a['account_id'] == original_account_id), None)
        if not mapped_account_id:
            continue  # Saltar si no se encuentra

        transaction_count = transaction_group.get('transaction_count', 0)
        for transaction in transaction_group.get('transactions', []):
            try:
                # Convertimos la fecha al formato ISO estándar
                date_str = transaction['date'].get('$date') if isinstance(transaction['date'], dict) else transaction['date']
                dt = datetime.fromisoformat(date_str.replace('Z', '')) if date_str else None
                date_key = dt.date().isoformat() if dt else None

                if not date_key:
                    continue

                if date_key not in date_ids:
                    # Si la fecha no existe aún, la agregamos
                    date_ids[date_key] = next_date_id
                    dim_dates.append((next_date_id, date_key))
                    next_date_id += 1

                # Agregamos registro a la tabla de hechos
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
                continue  # Saltar errores

    return dim_accounts, dim_customers, account_customer_mapping, dim_dates, fact_transactions

# Crea Tablas
def create_tables_if_not_exist(db_name):
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()

    # Eliminamos las tablas si ya existen (para evitar duplicación)
    cur.execute("DROP TABLE IF EXISTS fact_transactions")
    cur.execute("DROP TABLE IF EXISTS dim_dates")
    cur.execute("DROP TABLE IF EXISTS account_customers")
    cur.execute("DROP TABLE IF EXISTS dim_customers")
    cur.execute("DROP TABLE IF EXISTS dim_accounts")

    # Creamos tabla de cuentas
    cur.execute("""
    CREATE TABLE IF NOT EXISTS dim_accounts (
        account_id INTEGER PRIMARY KEY,
        limit_amount REAL,
        products TEXT
    )
    """)

    # Tabla de clientes
    cur.execute("""
    CREATE TABLE IF NOT EXISTS dim_customers (
        customer_id INTEGER PRIMARY KEY,
        name TEXT,
        username TEXT,
        birth_date TEXT
    )
    """)

    # Tabla relacional entre clientes y cuentas
    cur.execute("""
    CREATE TABLE IF NOT EXISTS account_customers (
        customer_id INTEGER,
        account_id INTEGER,
        PRIMARY KEY (customer_id, account_id)
    )
    """)

    # Tabla de fechas
    cur.execute("""
    CREATE TABLE IF NOT EXISTS dim_dates (
        date_id INTEGER PRIMARY KEY,
        date TEXT
    )
    """)

    # Tabla de hechos con las transacciones
    cur.execute("""
    CREATE TABLE IF NOT EXISTS fact_transactions (
        transaction_id INTEGER PRIMARY KEY,
        account_id INTEGER,
        date_id INTEGER,
        transaction_count INTEGER,
        amount REAL,
        transaction_type TEXT,
        symbol TEXT,
        price REAL,
        total REAL
    )
    """)

    conn.commit()
    conn.close()

# Carga
def load_to_sqlite_kimball(dim_accounts, dim_customers, account_customer_mapping, dim_dates, fact_transactions, db_name):
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()

    # Insertamos los registros transformados a cada tabla
    cur.executemany("INSERT INTO dim_accounts (account_id, limit_amount, products) VALUES (?, ?, ?)",
                    [(k, v[0], v[1]) for k, v in dim_accounts.items()])

    cur.executemany("INSERT INTO dim_customers (customer_id, name, username, birth_date) VALUES (?, ?, ?, ?)",
                    [(v['customer_id'], v['name'], v['username'], v['birth_date']) for v in dim_customers.values()])

    cur.executemany("INSERT INTO account_customers (customer_id, account_id) VALUES (?, ?)", account_customer_mapping)
    cur.executemany("INSERT INTO dim_dates (date_id, date) VALUES (?, ?)", dim_dates)

    cur.executemany("""
    INSERT INTO fact_transactions (
        transaction_id, account_id, date_id, transaction_count,
        amount, transaction_type, symbol, price, total
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, fact_transactions)

    conn.commit()

    # Mostrar cuántos registros se insertaron en cada tabla
    print("\nResumen de carga de datos:")
    for table in ['dim_accounts', 'dim_customers', 'account_customers', 'dim_dates', 'fact_transactions']:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        print(f"{table}: {count} filas insertadas")

    conn.close()

# Ejecuta
if __name__ == "__main__":
    # Ruta al archivo ZIP de entrada y a la base de datos
    zip_path = 'C:/Users/PC/Downloads/sample_analytics_dataset.zip'
    db_name = 'C:/Users/PC/Desktop/analytics_etl.db'

    # Verificamos que el archivo exista
    if not os.path.isfile(zip_path):
        print(f"Archivo no encontrado: {zip_path}")
    else:
        print("Extrayendo datos...")
        accounts, customers, transactions = extract_json_from_zip(zip_path)

        print("Transformando datos...")
        dim_account, dim_customer, account_client, dim_date, fact_transactions = transform_data(accounts, customers, transactions)

        print("Creando tablas...")
        create_tables_if_not_exist(db_name)

        print("Cargando datos...")
        load_to_sqlite_kimball(dim_account, dim_customer, account_client, dim_date, fact_transactions, db_name)

        print("\nETL completado correctamente.")
