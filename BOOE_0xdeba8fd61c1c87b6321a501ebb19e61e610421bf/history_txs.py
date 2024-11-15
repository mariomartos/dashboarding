import pyodbc
import requests
import os
from datetime import datetime

# Configuraciones
SQL_SERVER = 'A7\\SQLEXPRESS'
DATABASE_NAME = 'dashboards'
CONTRACT_ADDRESS = '0x289Ff00235D2b98b0145ff5D4435d3e92f9540a6'  
BLOCK_RANGE = 1000  # Número de bloques a procesar en cada solicitud

# Ruta del archivo de API key (relativa al script)
API_KEY_PATH = os.path.join(os.path.dirname(__file__), "api_key.txt")

# Función para cargar la API key desde el archivo
def load_api_key():
    try:
        with open(API_KEY_PATH, 'r') as file:
            api_key = file.read().strip()
        return api_key
    except Exception as e:
        print(f"Error al leer el archivo de API key: {e}")
        return None

# Conectar a SQL Server
def connect_to_sql_server():
    connection_string = f'DRIVER={{SQL Server}};SERVER={SQL_SERVER};DATABASE={DATABASE_NAME};Trusted_Connection=yes;'
    try:
        conn = pyodbc.connect(connection_string)
        print("Conexión a SQL Server establecida correctamente.")
        return conn
    except Exception as e:
        print(f"Error al conectar a SQL Server: {e}")
        return None

# Obtener el primer número de bloque de un contrato
def get_first_block(contract_address, api_key):
    api_url = f"https://api.etherscan.io/api?module=account&action=tokentx&contractaddress={contract_address}&startblock=0&endblock=99999999&sort=asc&apikey={api_key}"
    try:
        response = requests.get(api_url, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        if data.get('status') != '1':
            print(f"Error en la respuesta de Etherscan: {data.get('message')}")
            return None

        tokens = data.get('result', [])
        if not tokens:
            print("No se encontraron transacciones para este contrato.")
            return None
        
        first_block = int(tokens[0].get("blockNumber"))
        return first_block

    except requests.exceptions.RequestException as e:
        print(f"Error al obtener el primer bloque: {e}")
        return None

# Obtener el número de bloque actual
def get_current_block(api_key):
    api_url = f"https://api.etherscan.io/api?module=proxy&action=eth_blockNumber&apikey={api_key}"
    try:
        response = requests.get(api_url, timeout=30)
        response.raise_for_status()
        data = response.json()
        current_block = int(data["result"], 16)
        return current_block
    except requests.exceptions.RequestException as e:
        print(f"Error al obtener el bloque actual: {e}")
        return None

# Insertar una transacción en SQL Server si no existe duplicado
def insert_transaction(conn, tx_data):
    cursor = conn.cursor()
    
    # Comprobación de duplicados
    check_query = """
    SELECT 1 FROM transactions
    WHERE hash = ? AND date = ? AND block_number = ? AND [from] = ? AND [to] = ? AND amount = ?
    """
    cursor.execute(check_query, tx_data["hash"], tx_data["date"], tx_data["block_number"], tx_data["from"], tx_data["to"], tx_data["amount"])
    if cursor.fetchone():
        return False

    # Inserción de la transacción con contract_address
    insert_query = """
    INSERT INTO transactions (id, contract_address, hash, date, block_number, [from], [to], amount)
    VALUES (NEWID(), ?, ?, ?, ?, ?, ?, ?)
    """
    cursor.execute(insert_query, tx_data["contract_address"], tx_data["hash"], tx_data["date"], tx_data["block_number"], tx_data["from"], tx_data["to"], tx_data["amount"])
    conn.commit()
    return True

# Insertar un registro de log y devolver el log_id
def insert_log(conn, log_data):
    cursor = conn.cursor()
    
    insert_query = """
    INSERT INTO logs (id, insert_date, block_from, block_to, txs_insert, txs_amount, refreshed, refreshed_date, refreshed_id)
    OUTPUT INSERTED.id
    VALUES (NEWID(), GETDATE(), ?, ?, ?, ?, 0, NULL, NULL)
    """
    cursor.execute(insert_query, log_data["block_from"], log_data["block_to"], log_data["txs_insert"], log_data["txs_amount"])
    log_id = cursor.fetchone()[0]
    conn.commit()
    
    print(f"Log insertado para bloques {log_data['block_from']} - {log_data['block_to']}")
    
    return log_id

# Obtener transacciones en bucle hasta el bloque actual y registrar en SQL Server
def get_transactions_in_loop(contract_address, start_block, api_key, conn):
    while True:
        current_block = get_current_block(api_key)
        if not current_block:
            print("No se pudo obtener el bloque actual.")
            break
        
        end_block = min(start_block + BLOCK_RANGE - 1, current_block)
        
        # Salir del bucle si hemos alcanzado el bloque actual
        if start_block > current_block:
            print("Se alcanzó el bloque actual.")
            break

        # API request para el rango actual de bloques
        api_url = f"https://api.etherscan.io/api?module=account&action=tokentx&contractaddress={contract_address}&startblock={start_block}&endblock={end_block}&sort=asc&apikey={api_key}"
        try:
            response = requests.get(api_url, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            tx_count = 0
            inserted_count = 0
            
            # Solo procesar transacciones si se encuentran
            if data.get('status') == '1' and data.get('result'):
                transactions = data.get('result', [])
                tx_count = len(transactions)
                
                # Insertar cada transacción
                for tx in transactions:
                    tx_data = {
                        "contract_address": contract_address,
                        "hash": tx.get("hash"),
                        "date": datetime.fromtimestamp(int(tx.get("timeStamp"))),
                        "block_number": int(tx.get("blockNumber")),
                        "from": tx.get("from"),
                        "to": tx.get("to"),
                        "amount": float(tx.get("value")) / (10 ** int(tx.get("tokenDecimal", 18)))
                    }
                    if insert_transaction(conn, tx_data):
                        inserted_count += 1

            # Insertar el log, incluso si no se encontraron transacciones
            log_data = {
                "block_from": start_block,
                "block_to": end_block,
                "txs_insert": inserted_count,
                "txs_amount": tx_count
            }
            insert_log(conn, log_data)

            # Avanzar al siguiente bloque
            start_block = end_block + 1

        except requests.exceptions.RequestException as e:
            print(f"Error al obtener las transacciones: {e}")
            break

# Ejecución principal
if __name__ == "__main__":
    api_key = load_api_key()
    if not api_key:
        print("No se pudo cargar la API key.")
    else:
        conn = connect_to_sql_server()
        if conn:
            first_block = get_first_block(CONTRACT_ADDRESS, api_key)
            if first_block:
                print(f"Primer bloque {CONTRACT_ADDRESS}: {first_block}")
                get_transactions_in_loop(CONTRACT_ADDRESS, first_block, api_key, conn)
            conn.close()
