
import mysql.connector
from datetime import datetime

from db_config import get_mysql_conn
from race_monitor_worker import fetch_racer, update_database

INTERVAL_A = 120  # 2 min
INTERVAL_B = 240  # 4 min

TABLES = [
    ("update_group_2min", INTERVAL_A),
    ("update_group_4min", INTERVAL_B),
    ("update_group_rest", 0)  # sem intervalo mínimo específico
]

def get_next_record():
    """
    Lê o candidato mais antigo de cada tabela (ORDER BY last_update ASC, racer_id ASC),
    e escolhe o mais antigo geral (em empate, menor racer_id).
    """
    conn_db = get_mysql_conn()
    cur = conn_db.cursor()

    candidates = []
    for table, interval in TABLES:
        cur.execute(f"""
            SELECT racer_id, COALESCE(last_update, '1970-01-01 00:00:00') AS last_update
            FROM {table}
            ORDER BY last_update ASC, racer_id ASC
            LIMIT 1
        """)
        row = cur.fetchone()
        if row:
            racer_id, last_update = row
            candidates.append((table, interval, racer_id, last_update))

    cur.close()
    conn_db.close()

    candidates.sort(key=lambda x: (x[3], x[2]))
    return candidates[0] if candidates else None

def parse_dt(dt_str):
    """Converte 'YYYY-MM-DD HH:MM:SS' para datetime (garante hora caso falte)."""
    if " " not in dt_str:
        dt_str = dt_str + " 00:00:00"
    return datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")

def should_update(last_update_str, interval_seconds):
    """True se já passou o intervalo mínimo desde last_update."""
    if interval_seconds <= 0:
        return True
    last_update_dt = parse_dt(last_update_str)
    now = datetime.now()
    return (now - last_update_dt).total_seconds() >= interval_seconds

def update_last_update(table_name, racer_id):
    """Marca last_update = NOW() na tabela auxiliar."""
    conn_db = get_mysql_conn()
    cur = conn_db.cursor()
    cur.execute(f"UPDATE {table_name} SET last_update = NOW() WHERE racer_id = %s", (racer_id,))
    conn_db.commit()
    cur.close()
    conn_db.close()

def update_racer_once(racer_id, table_name):
    """Chama API, atualiza DB principal e marca last_update."""
    data = fetch_racer(racer_id)
    if data.get("Successful"):
        update_database(data["Details"]["Competitor"], data["Details"]["Laps"])
        update_last_update(table_name, racer_id)
        print(f"[{table_name}] Atualizado racer_id={racer_id} às {datetime.now().strftime('%H:%M:%S')}")
    else:
        print(f"Falha API para racer_id={racer_id}: {data.get('Message')}")

if __name__ == "__main__":
    record = get_next_record()
    if record:
        table, interval, racer_id, last_update = record
        if should_update(last_update, interval):
            update_racer_once(racer_id, table)
        else:
            print(f"Registro {racer_id} ({table}) ainda não atingiu intervalo mínimo.")
    else:
        print("Nenhum registro encontrado nas tabelas auxiliares.")
