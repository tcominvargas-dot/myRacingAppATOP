
#!/usr/bin/env python3
import argparse
import sys

from db_config import get_mysql_conn

def run_sql(conn, sql, params=None):
    cur = conn.cursor()
    cur.execute(sql, params or ())
    conn.commit()
    cur.close()

def main():
    parser = argparse.ArgumentParser(
        description="Limpa as tabelas competitors e competitor_laps sem confirmação."
    )
    parser.add_argument(
        "--method",
        choices=["truncate", "delete"],
        default="truncate",
        help="Método de limpeza: TRUNCATE (mais rápido, zera AUTO_INCREMENT) ou DELETE (mantém AUTO_INCREMENT)."
    )
    parser.add_argument(
        "--only-competitors",
        action="store_true",
        help="Limpa apenas a tabela competitors."
    )
    parser.add_argument(
        "--only-laps",
        action="store_true",
        help="Limpa apenas a tabela competitor_laps."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Mostra as queries que seriam executadas sem aplicar mudanças."
    )

    args = parser.parse_args()

    # Regras de segurança básicas
    if args.only_competitors and args.only_laps:
        print("Erro: flags conflitantes (--only-competitors e --only-laps).")
        sys.exit(1)

    alvo = (
        "competitors" if args.only_competitors else
        "competitor_laps" if args.only_laps else
        "competitors + competitor_laps"
    )

    print("⚠️ Executando limpeza direta (sem confirmação).")
    print(f"→ Método: {args.method.upper()}")
    print(f"→ Tabelas a limpar: {alvo}")

    sql_truncate = {
        "competitors": "TRUNCATE TABLE competitors",
        "competitor_laps": "TRUNCATE TABLE competitor_laps",
    }
    sql_delete = {
        "competitors": "DELETE FROM competitors",
        "competitor_laps": "DELETE FROM competitor_laps",
    }

    # Determina as queries a executar
    queries = []
    if args.method == "truncate":
        if args.only_competitors:
            queries.append(sql_truncate["competitors"])
        elif args.only_laps:
            queries.append(sql_truncate["competitor_laps"])
        else:
            queries.extend([sql_truncate["competitor_laps"], sql_truncate["competitors"]])
    else:  # delete
        if args.only_competitors:
            queries.append(sql_delete["competitors"])
        elif args.only_laps:
            queries.append(sql_delete["competitor_laps"])
        else:
            queries.extend([sql_delete["competitor_laps"], sql_delete["competitors"]])

    if args.dry_run:
        print("🔍 Modo DRY-RUN: As seguintes queries seriam executadas:")
        for q in queries:
            print(f"→ {q}")
        sys.exit(0)

    conn = get_mysql_conn()

    try:
        for q in queries:
            run_sql(conn, q)
        print("✅ Limpeza concluída com sucesso.")
    except Exception as e:
        conn.rollback()
        print("❌ Erro na limpeza:", e)
        sys.exit(1)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
