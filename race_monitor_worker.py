
import http.client
import json
import logging
import os
from datetime import datetime
import mysql.connector
from db_config import get_mysql_conn

# ====================== LOG ======================
os.makedirs("/home/ubuntu/mykartapp", exist_ok=True)
logging.basicConfig(
    filename='/home/ubuntu/mykartapp/race_monitor.log',
    filemode='a',
    level=logging.INFO,
    format='%(asctime)s %(message)s',
    datefmt='%H:%M:%S'
)

def safe_int(value, default=0):
    try:
        return int(value)
    except (ValueError, TypeError):
        return default

def format_racer_id(racer_id):
    """Formata o racer_id para sempre ter 3 caracteres (com zeros à esquerda)."""
    try:
        return str(int(racer_id)).zfill(3)
    except (ValueError, TypeError):
        return "000"  # fallback seguro

# ====================== NOVAS FUNÇÕES ======================
def get_least_used_api_key():
    """
    Busca a chave de API menos utilizada na tabela app_config.
    Assume colunas: id, api_token, race_id, last_used (DATETIME).
    """
    conn_db = get_mysql_conn()
    cur = conn_db.cursor(dictionary=True)
    cur.execute("""
        SELECT id, api_token, race_id, COALESCE(last_used, '1970-01-01 00:00:00') AS last_used
        FROM app_config
        ORDER BY last_used ASC
        LIMIT 1
    """)
    row = cur.fetchone()
    cur.close()
    conn_db.close()
    return row

def update_api_key_usage(api_id):
    """Atualiza o timestamp da chave usada."""
    conn_db = get_mysql_conn()
    cur = conn_db.cursor()
    cur.execute("UPDATE app_config SET last_used = NOW() WHERE id = %s", (api_id,))
    conn_db.commit()
    cur.close()
    conn_db.close()

# ====================== FUNÇÃO AJUSTADA ======================
def fetch_racer(racer_id):
    """Faz chamada à API Race Monitor (GetRacer) usando a chave menos utilizada."""
    api_info = get_least_used_api_key()
    if not api_info:
        raise Exception("Nenhuma chave de API encontrada na tabela app_config.")

    api_token = api_info["api_token"]
    race_id = api_info["race_id"]
    api_id = api_info["id"]

    # Atualiza last_used para esta chave
    update_api_key_usage(api_id)

    # Ajusta racer_id para 3 dígitos
    racer_id = format_racer_id(racer_id)

    conn = http.client.HTTPSConnection("api.race-monitor.com")
    endpoint = f"/v2/Live/GetRacer?apiToken={api_token}&raceID={race_id}&racerID={racer_id}"
    headers = {"Content-Type": "application/json"}
    conn.request("POST", endpoint, '', headers)
    res = conn.getresponse()
    raw_data = res.read().decode("utf-8")
    conn.close()

    # ✅ Log da API utilizada
    logging.info(f"API usada → ID:{api_id}, Token:{api_token[:6]}..., RaceID:{race_id}")
    print(f"API usada → ID:{api_id}, Token:{api_token[:6]}..., RaceID:{race_id}")

    return json.loads(raw_data)

# ====================== RESTANTE DO CÓDIGO (update_database) ======================
def update_database(comp, laps):
    """Atualiza dados do competidor e voltas no banco MySQL."""
    cfg = get_least_used_api_key()  # Pode usar race_id daqui se necessário
    race_id = cfg["race_id"]

    conn_db = get_mysql_conn()
    cur = conn_db.cursor()

    racer_id = safe_int(comp.get("RacerID"))
    number = comp.get("Number") or ""
    transponder = comp.get("Transponder") or ""
    first_name = comp.get("FirstName") or ""
    last_name = comp.get("LastName") or ""
    nationality = comp.get("Nationality") or ""
    additional_data = comp.get("AdditionalData") or ""
    class_id = safe_int(comp.get("ClassID"))
    position = safe_int(comp.get("Position"))
    laps_completed = safe_int(comp.get("Laps"))
    total_time = comp.get("TotalTime") or "00:00.000"
    best_position = safe_int(comp.get("BestPosition"))
    best_lap = safe_int(comp.get("BestLap"))
    best_lap_time = comp.get("BestLapTime") or "00:00.000"
    last_lap_time = comp.get("LastLapTime") or "00:00.000"

    cur.execute("""
        INSERT INTO competitors (
            racer_id, race_id, number, transponder, first_name, last_name,
            nationality, additional_data, class_id, position, laps_completed,
            total_time, best_position, best_lap, best_lap_time, last_lap_time, updated_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, NOW())
        ON DUPLICATE KEY UPDATE
            position=VALUES(position), laps_completed=VALUES(laps_completed),
            total_time=VALUES(total_time), best_position=VALUES(best_position),
            best_lap=VALUES(best_lap), best_lap_time=VALUES(best_lap_time),
            last_lap_time=VALUES(last_lap_time), updated_at=NOW()
    """, (
        racer_id, race_id, number, transponder, first_name, last_name,
        nationality, additional_data, class_id, position, laps_completed,
        total_time, best_position, best_lap, best_lap_time, last_lap_time
    ))

    laps_data = []
    for lap in laps:
        lap_number = safe_int(lap.get("Lap"))
        lap_position = safe_int(lap.get("Position"))
        lap_time = lap.get("LapTime") or "00:00.000"
        flag_status = lap.get("FlagStatus") or ""
        total_time_lap = lap.get("TotalTime") or "00:00.000"
        laps_data.append((race_id, racer_id, lap_number, lap_position, lap_time, flag_status, total_time_lap))

    if laps_data:
        cur.executemany("""
            INSERT IGNORE INTO competitor_laps
            (race_id, racer_id, lap_number, position, lap_time, flag_status, total_time)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
        """, laps_data)

    conn_db.commit()
    cur.close()
    conn_db.close()

    logging.info(f"OK → {racer_id} {first_name} {last_name} Pos {position} {len(laps)} voltas")
    print(f"OK → {racer_id} {first_name} {last_name} sincronizado às {datetime.now().strftime('%H:%M:%S')}")
