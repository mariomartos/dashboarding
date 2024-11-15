import pyodbc
import requests
import os
import time
from datetime import datetime

# Configuraciones
SQL_SERVER = 'A7\\SQLEXPRESS'
DATABASE_NAME = 'dashboards'
CONTRACT_ADDRESS = '0x289Ff00235D2b98b0145ff5D4435d3e92f9540a6'  
BLOCK_RANGE = 1000  # Número de bloques a procesar en cada solicitud
RETRY_INTERVAL = 10  # Tiempo en minutos entre intentos de reintento
REFRESH_THRESHOLD = 15  # Tiempo mínimo en minutos desde el último refresh para ejecutar

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

# Obtener el tiempo desde el último refresh para el contrato
def get_time_since_last_refresh(conn, contract_address):
    try:
        cursor = conn.cursor()
        query = """
        SELECT DATEDIFF(MINUTE, last_refresh, GETDATE()) AS diff_in_minutes
        FROM contract
        WHERE contract_address = ?
        """
        cursor.execute(query, contract_address)
        result = cursor.fetchone()
        return result[0] if result else None
    except Exception as e:
        print(f"Error al obtener el tiempo desde el último refresh: {e}")
        return None

# Actualizar el campo last_refresh en la tabla contract
def update_last_refresh(conn, contract_address):
    try:
        cursor = conn.cursor()
        update_query = """
        UPDATE contract
        SET last_refresh = GETDATE()
        WHERE contract_address = ?
        """
        cursor.execute(update_query, contract_address)
        conn.commit()
        print(f"last_refresh actualizado para el contrato {contract_address}.")
    except Exception as e:
        print(f"Error al actualizar last_refresh para el contrato {contract_address}: {e}")

# Obtener el primer número de bloque desde la base de datos SQL o desde la API si no hay registros
def get_first_block(conn, contract_address, api_key):
    try:
        cursor = conn.cursor()
        query = "SELECT ISNULL(MAX(block_from), 0) FROM logs"
        cursor.execute(query)
        result = cursor.fetchone()
        first_block = result[0] if result else 0

        if first_block == 0:
            print("No se encontraron registros en logs. Consultando el primer bloque desde la API...")
            api_url = f"https://api.etherscan.io/api?module=account&action=tokentx&contractaddress={contract_address}&startblock=0&endblock=99999999&sort=asc&apikey={api_key}"
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
            print(f"Primer bloque obtenido desde la API: {first_block}")
            return first_block

        print(f"Primer bloque obtenido de SQL: {first_block}")
        return first_block

    except Exception as e:
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
    INSERT INTO logs (id, insert_date, block_from, block_to, txs_insert, txs_amount, refreshed, refreshed_date, refreshed_id, contract_address)
    OUTPUT INSERTED.id
    VALUES (NEWID(), GETDATE(), ?, ?, ?, ?, 0, NULL, NULL, ?)
    """
    cursor.execute(insert_query, log_data["block_from"], log_data["block_to"], log_data["txs_insert"], log_data["txs_amount"], log_data["contract_address"])
    log_id = cursor.fetchone()[0]
    conn.commit()
    
    print(f"Log insertado para bloques {log_data['block_from']} - {log_data['block_to']} (Contract: {log_data['contract_address']})")
    
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
            
            # Procesar transacciones si se encuentran
            if data.get('status') == '1' and data.get('result'):
                transactions = data.get('result', [])
                tx_count = len(transactions)
                
                # Insertar cada transacción
                for tx in transactions:
                    try:
                        tx_data = {
                            "contract_address": contract_address,
                            "hash": tx.get("hash"),
                            "date": datetime.fromtimestamp(int(tx.get("timeStamp"))),
                            "block_number": int(tx.get("blockNumber")),
                            "from": tx.get("from"),
                            "to": tx.get("to"),
                            "amount": float(tx.get("value")) / (10 ** int(tx.get("tokenDecimal", 18)))
                        }
                        # Verificar si se inserta correctamente
                        success = insert_transaction(conn, tx_data)
                        if success:
                            inserted_count += 1
                    except Exception as e:
                        print(f"Error al procesar transacción: {e}")

                    # Insertar el log, incluso si no se encontraron transacciones
                    try:
                        log_data = {
                            "block_from": start_block,
                            "block_to": end_block,
                            "txs_insert": inserted_count,
                            "txs_amount": tx_count,
                            "contract_address": contract_address  # Nuevo campo añadido
                        }
                        insert_log(conn, log_data)
                    except Exception as e:
                        print(f"Error al insertar el log para bloques {start_block} - {end_block}: {e}")


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
        while True:
            conn = connect_to_sql_server()
            if conn:
                time_since_last_refresh = get_time_since_last_refresh(conn, CONTRACT_ADDRESS)
                if time_since_last_refresh is not None and time_since_last_refresh >= REFRESH_THRESHOLD:
                    first_block = get_first_block(conn, CONTRACT_ADDRESS, api_key)
                    if first_block:
                        print(f"Primer bloque {CONTRACT_ADDRESS}: {first_block}")
                        get_transactions_in_loop(CONTRACT_ADDRESS, first_block, api_key, conn)
                        update_last_refresh(conn, CONTRACT_ADDRESS)
                else:
                    print(f"No se cumple el umbral de refresco ({REFRESH_THRESHOLD} minutos). Esperando {RETRY_INTERVAL} minutos...")
                conn.close()
            time.sleep(RETRY_INTERVAL * 60)
