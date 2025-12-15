
import mysql.connector

DB_CONFIG = {
    "host": "localhost",
    "user": "admin",
    "password": "filomena",
    "database": "my_karting_app",
    "ssl_disabled": True
}

def get_mysql_conn():
    """Retorna uma conexão MySQL usando a configuração padrão."""
    return mysql.connector.connect(**DB_CONFIG)

def get_app_config():
    """
    Lê api_token e race_id da tabela app_config (id=1).
    Retorna dict: {"api_token": str, "race_id": int}
    """
    conn = get_mysql_conn()
    cur = conn.cursor()
    cur.execute("SELECT api_token, race_id FROM app_config WHERE id = 1")
    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row:
        raise RuntimeError("Configuração ausente: insira um registro em app_config com id=1.")
    api_token, race_id = row
    return {"api_token": api_token, "race_id": int(race_id)}
