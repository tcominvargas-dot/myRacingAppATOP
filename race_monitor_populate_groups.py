
import http.client
import json
from datetime import datetime

from db_config import get_mysql_conn, get_app_config

def safe_int(value, default=0):
    try:
        return int(value)
    except (ValueError, TypeError):
        return default

def fetch_session():
    """Busca dados da sessão (competidores) na API Race Monitor."""
    cfg = get_app_config()
    api_token = cfg["api_token"]
    race_id = cfg["race_id"]

    conn = http.client.HTTPSConnection("api.race-monitor.com")
    endpoint = f"/v2/Live/GetSession?apiToken={api_token}&raceID={race_id}"
    headers = {"Content-Type": "application/json"}
    conn.request("POST", endpoint, '', headers)
    res = conn.getresponse()
    raw = res.read().decode("utf-8")
    conn.close()
    return json.loads(raw)

def get_group_2min_ids():
    """Lê racer_ids da tabela de 2 minutos (não alterada por este script)."""
    conn_db = get_mysql_conn()
    cur = conn_db.cursor()
    cur.execute("SELECT racer_id FROM update_group_2min")
    ids = {row[0] for row in cur.fetchall()}
    cur.close()
    conn_db.close()
    return ids

def reset_and_fill_aux_tables(top5_ids, other_ids):
    """
    Limpa as tabelas de 4 minutos e restante, depois insere IDs.
    Não toca na 2 minutos (manual).
    """
    conn_db = get_mysql_conn()
    cur = conn_db.cursor()

    cur.execute("TRUNCATE TABLE update_group_4min")
    cur.execute("TRUNCATE TABLE update_group_rest")

    for racer_id in top5_ids:
        cur.execute("""
            INSERT INTO update_group_4min (racer_id, last_update)
            VALUES (%s, NULL)
        """, (racer_id,))

    for racer_id in other_ids:
        cur.execute("""
            INSERT INTO update_group_rest (racer_id, last_update)
            VALUES (%s, NULL)
        """, (racer_id,))

    conn_db.commit()
    cur.close()
    conn_db.close()

if __name__ == "__main__":
    data = fetch_session()
    if not data.get("Successful"):
        print("Erro da API:", data.get("Message", "Falha desconhecida"))
        raise SystemExit(1)

    competitors_dict = data["Session"]["Competitors"]  # dicionário: chaves são RacerID
    # Lista ordenada por posição crescente
    competitors_sorted = sorted(
        competitors_dict.values(),
        key=lambda c: safe_int(c.get("Position"), default=999999)
    )

    # IDs que já estão na tabela 2min (excluídos)
    group2_ids = get_group_2min_ids()

    filtered = [c for c in competitors_sorted if safe_int(c.get("RacerID")) not in group2_ids]

    top5_ids = [safe_int(c.get("RacerID")) for c in filtered[:5]]
    other_ids = [safe_int(c.get("RacerID")) for c in filtered[5:]]

    reset_and_fill_aux_tables(top5_ids, other_ids)

    print(f"✅ População concluída às {datetime.now().strftime('%H:%M:%S')}")
    print(f"➡️ Ignorados (2min): {sorted(group2_ids)}")
    print(f"➡️ 4min (top5): {top5_ids}")
    print(f"➡️ REST (outros): {other_ids}")
