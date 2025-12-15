
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import time
import http.client
import json
import logging
import os
from datetime import datetime
import mysql.connector
from tqdm import tqdm  # barra de progresso

from db_config import get_mysql_conn

# ====================== CONFIG / LOG ======================
LOG_DIR = "/home/ubuntu/mykartapp"
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(LOG_DIR, "results_ingest.log"),
    filemode="a",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)

TABLE_COMPETITORS = "competitors"
TABLE_LAPS_NAME = "competitor_laps"  # ajuste se necessário
API_HOST = "api.race-monitor.com"
MAX_CALLS_PER_MINUTE = 10  # <<<<<<<<<<<<<< AJUSTE: agora 10/min

# ====================== HELPERS ======================
def safe_int(value, default=0):
    try:
        return int(value)
    except (ValueError, TypeError):
        return default

def mask_token(token: str, head=6):
    return (token or "")[:head] + "..." if token else "None"

# ====================== API KEY ROTATION ======================
def get_least_used_api_key():
    conn = get_mysql_conn()
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT id, api_token, COALESCE(last_used, '1970-01-01 00:00:00') AS last_used
        FROM app_config
        WHERE api_token IS NOT NULL AND api_token <> ''
        ORDER BY last_used ASC
        LIMIT 1
    """)
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row

def update_api_key_usage(api_id):
    conn = get_mysql_conn()
    cur = conn.cursor()
    cur.execute("UPDATE app_config SET last_used = NOW() WHERE id = %s", (api_id,))
    conn.commit()
    cur.close()
    conn.close()

# ====================== RATE LIMIT CONTROL (10/min) ======================
call_timestamps = []  # timestamps (epoch seconds) das últimas chamadas

def enforce_rate_limit():
    """Garante no máximo MAX_CALLS_PER_MINUTE chamadas dentro de uma janela móvel de 60s."""
    global call_timestamps
    now = time.time()
    # mantém apenas chamadas nos últimos 60s
    call_timestamps = [t for t in call_timestamps if now - t < 60.0]
    if len(call_timestamps) >= MAX_CALLS_PER_MINUTE:
        sleep_time = 60.0 - (now - call_timestamps[0])
        if sleep_time > 0:
            logging.info(f"[RATE LIMIT] Aguardando {sleep_time:.1f}s para não exceder {MAX_CALLS_PER_MINUTE} chamadas/min")
            time.sleep(sleep_time)
        # após dormir, lista é podada novamente na próxima chamada

# ====================== API CALL ======================
def api_call_with_rotation(path: str) -> dict:
    # Aplica rate limit antes de escolher a chave (regras claras e uniformes)
    enforce_rate_limit()

    api_info = get_least_used_api_key()
    if not api_info:
        raise RuntimeError("Nenhuma API key disponível em app_config.")

    api_id = api_info["id"]
    token_mask = mask_token(api_info["api_token"])

    # Atualiza last_used antes da chamada para mitigar corrida entre processos
    update_api_key_usage(api_id)

    start = time.time()
    conn = http.client.HTTPSConnection(API_HOST, timeout=30)
    headers = {"Content-Type": "application/json"}

    try:
        conn.request("POST", path, body="", headers=headers)
        resp = conn.getresponse()
        raw = resp.read().decode("utf-8", errors="replace")
        status = resp.status
    except Exception as e:
        elapsed = (time.time() - start) * 1000.0
        logging.error(
            f"[API CALL FAIL] app_config.id={api_id} token={token_mask} path={path} "
            f"error={repr(e)} elapsed_ms={elapsed:.1f}"
        )
        raise
    finally:
        conn.close()

    elapsed = (time.time() - start) * 1000.0
    logging.info(
        f"[API CALL] app_config.id={api_id} token={token_mask} path={path} "
        f"status={status} elapsed_ms={elapsed:.1f}"
    )

    # registra a chamada para controle da janela
    call_timestamps.append(time.time())

    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        logging.error(
            f"[API JSON ERROR] path={path} status={status} error={repr(e)} payload_head={raw[:300]!r}"
        )
        raise RuntimeError(f"Falha ao decodificar JSON para {path}: {e}")

# ====================== WRAPPERS ======================
def fetch_session_details(session_id: int) -> dict:
    api_info = get_least_used_api_key()
    token = api_info["api_token"]
    path = f"/v2/Results/SessionDetails?apiToken={token}&sessionID={session_id}"
    return api_call_with_rotation(path)

def fetch_competitor_details(competitor_id: int) -> dict:
    api_info = get_least_used_api_key()
    token = api_info["api_token"]
    path = f"/v2/Results/CompetitorDetails?apiToken={token}&competitorID={competitor_id}"
    return api_call_with_rotation(path)

# ====================== DB OPS ======================
def upsert_competitor(conn, comp: dict):
    racer_id = safe_int(comp.get("ID"))
    race_id = safe_int(comp.get("RaceID"))
    number = comp.get("Number") or ""
    transponder = comp.get("Transponder") or ""
    first_name = comp.get("FirstName") or ""
    last_name = comp.get("LastName") or ""
    nationality = comp.get("Nationality") or ""
    additional_data = comp.get("AdditionalData") or ""
    class_id = safe_int(comp.get("Category"))
    position = safe_int(comp.get("Position"))
    laps_completed = safe_int(comp.get("Laps"))
    total_time = comp.get("TotalTime") or "00:00.000"
    best_position = safe_int(comp.get("BestPosition"))
    best_lap = safe_int(comp.get("BestLap"))
    best_lap_time = comp.get("BestLapTime") or "00:00.000"
    last_lap_time = comp.get("LastLapTime") or "00:00.000"

    sql = f"""
        INSERT INTO {TABLE_COMPETITORS} (
            racer_id, race_id, number, transponder, first_name, last_name,
            nationality, additional_data, class_id, position, laps_completed,
            total_time, best_position, best_lap, best_lap_time, last_lap_time, updated_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, NOW())
        ON DUPLICATE KEY UPDATE
            position=VALUES(position), laps_completed=VALUES(laps_completed),
            total_time=VALUES(total_time), best_position=VALUES(best_position),
            best_lap=VALUES(best_lap), best_lap_time=VALUES(best_lap_time),
            last_lap_time=VALUES(last_lap_time), updated_at=NOW()
    """
    vals = (
        racer_id, race_id, number, transponder, first_name, last_name,
        nationality, additional_data, class_id, position, laps_completed,
        total_time, best_position, best_lap, best_lap_time, last_lap_time
    )
    cur = conn.cursor()
    cur.execute(sql, vals)
    cur.close()

def insert_laps(conn, race_id: int, racer_id: int, laps: list):
    if not laps:
        return
    data = []
    for lap in laps:
        lap_number = safe_int(lap.get("Lap"))
        lap_position = safe_int(lap.get("Position"))
        lap_time = lap.get("LapTime") or "00:00.000"
        flag_status = safe_int(lap.get("FlagStatus"))
        total_time_lap = lap.get("TotalTime") or "00:00.000"
        data.append((race_id, racer_id, lap_number, lap_position, lap_time, flag_status, total_time_lap))

    sql = f"""
        INSERT IGNORE INTO {TABLE_LAPS_NAME}
        (race_id, racer_id, lap_number, position, lap_time, flag_status, total_time)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
    """
    cur = conn.cursor()
    cur.executemany(sql, data)
    cur.close()

# ====================== MAIN FLOW ======================
def process_session(session_id: int):
    logging.info(f"Iniciando ingestão da sessão {session_id}")
    session_json = fetch_session_details(session_id)
    if not session_json.get("Successful"):
        raise RuntimeError(f"SessionDetails falhou: {session_json}")

    session = session_json.get("Session") or {}
    sorted_competitors = session.get("SortedCompetitors") or []
    total = len(sorted_competitors)
    logging.info(f"Session {session_id}: {total} competidores")

    conn = get_mysql_conn()
    try:
        with tqdm(total=total, desc="Processando competidores", unit="comp") as pbar:
            for sc in sorted_competitors:
                competitor_id = safe_int(sc.get("ID"))
                if competitor_id <= 0:
                    pbar.update(1)
                    continue

                details_json = fetch_competitor_details(competitor_id)
                if not details_json.get("Successful"):
                    logging.warning(f"CompetitorDetails falhou para ID={competitor_id}: {details_json}")
                    pbar.update(1)
                    continue

                comp = details_json.get("Competitor") or {}
                upsert_competitor(conn, comp)

                race_id = safe_int(comp.get("RaceID"))
                racer_id = safe_int(comp.get("ID"))
                laps = comp.get("LapTimes") or []
                insert_laps(conn, race_id, racer_id, laps)

                conn.commit()
                logging.info(f"OK → racer_id={racer_id}, laps={len(laps)}")
                pbar.update(1)
    finally:
        conn.close()

    logging.info(f"Finalizado sessão {session_id}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python3 results_ingest.py <SESSION_ID>")
        sys.exit(1)
    session_id = safe_int(sys.argv[1])
    if session_id <= 0:
        print("SESSION_ID inválido.")
        sys.exit(1)
    process_session(session_id)
    print(f"✅ Ingestão concluída para session_id={session_id} às {datetime.now().strftime('%H:%M:%S')}")
