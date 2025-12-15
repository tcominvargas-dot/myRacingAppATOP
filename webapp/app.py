
# -*- coding: utf-8 -*-
import os, sys, re, logging, subprocess, threading
from logging.handlers import RotatingFileHandler
from datetime import datetime
from statistics import mean
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, session

# Caminhos base
ROOT_DIR = '/home/ubuntu/mykartapp'
WEBAPP_DIR = os.path.dirname(__file__)
LOGS_DIR = os.path.join(WEBAPP_DIR, 'logs')

# Acesso ao db_config
sys.path.append(ROOT_DIR)
try:
    from db_config import get_mysql_conn
except Exception:
    get_mysql_conn = None

APP_TITLE = "MyKartApp – Controle"
SCRIPTS_DIR = os.environ.get('MYKART_SCRIPTS_DIR', ROOT_DIR)
CLEANUP_SCRIPT = os.path.join(SCRIPTS_DIR, 'cleanup_tables.py')
POPULATE_SCRIPT = os.path.join(SCRIPTS_DIR, 'race_monitor_populate_groups.py')
SCHEDULER_SCRIPT = os.path.join(SCRIPTS_DIR, 'race_monitor_scheduler.py')
RACE_LOG_FILE = os.path.join(SCRIPTS_DIR, 'race_monitor.log')

# Thresholds de cor (ms de delta vs média global)
try:
    DELTA_FAST_MAX = int(os.environ.get('DELTA_FAST_MAX', -1))
    DELTA_GOOD_MAX = int(os.environ.get('DELTA_GOOD_MAX', 200))
    DELTA_WARN_MAX = int(os.environ.get('DELTA_WARN_MAX', 700))
except Exception:
    DELTA_FAST_MAX, DELTA_GOOD_MAX, DELTA_WARN_MAX = -1, 200, 700

# Flask app
app = Flask(__name__, template_folder=os.path.join(WEBAPP_DIR, 'templates'), static_folder=os.path.join(WEBAPP_DIR, 'static'))
app.secret_key = os.environ.get('FLASK_SECRET', 'change-me')

# Logging
os.makedirs(LOGS_DIR, exist_ok=True)

def _make_logger(name, filename):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    handler = RotatingFileHandler(os.path.join(LOGS_DIR, filename), maxBytes=1_000_000, backupCount=3, encoding='utf-8')
    fmt = logging.Formatter('[%(asctime)s] %(levelname)s %(name)s: %(message)s')
    handler.setFormatter(fmt)
    if not any(getattr(h, 'baseFilename', None) == handler.baseFilename for h in logger.handlers):
        logger.addHandler(handler)
    stdout_h = logging.StreamHandler(sys.stdout)
    stdout_h.setFormatter(fmt)
    if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        logger.addHandler(stdout_h)
    logger.propagate = False
    return logger

boot_logger = _make_logger('app_boot', 'app_boot.log')
box_logger  = _make_logger('box_eval', 'box_eval.log')

# ---------------------- Scheduler ----------------------
class SchedulerManager:
    def __init__(self):
        self._thread = None
        self._stop_evt = threading.Event()
        self.interval_seconds = None
        self.running = False
        self.start_time = None
        self.last_run = None
        self.run_count = 0
        self.last_returncode = None
        self.last_stdout = ''
        self.last_stderr = ''

    def _loop(self):
        while not self._stop_evt.is_set():
            try:
                proc = subprocess.run(['/usr/bin/python3', SCHEDULER_SCRIPT], capture_output=True, text=True, timeout=120)
                self.last_returncode = proc.returncode
                self.last_stdout = proc.stdout[-4000:]
                self.last_stderr = proc.stderr[-4000:]
                self.run_count += 1
                self.last_run = datetime.now()
            except Exception as e:
                self.last_stderr = f"Exception: {e}"
                self.last_returncode = -1
                self.last_run = datetime.now()
            self._stop_evt.wait(self.interval_seconds)
        self.running = False

    def start(self, interval_seconds: int):
        if not interval_seconds or interval_seconds <= 0:
            raise ValueError("Intervalo deve ser > 0 segundos")
        self.interval_seconds = int(interval_seconds)
        if self._thread and self._thread.is_alive():
            return
        self._stop_evt.clear()
        self.run_count = 0
        self.start_time = datetime.now()
        self.running = True
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self):
        if self._thread and self._thread.is_alive():
            self._stop_evt.set()
            self._thread.join(timeout=10)
        self.running = False

    def status(self):
        return {
            'running': self.running,
            'interval_seconds': self.interval_seconds,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'last_run': self.last_run.isoformat() if self.last_run else None,
            'run_count': self.run_count,
            'last_returncode': self.last_returncode,
        }

sched = SchedulerManager()

# ---------------------- Helpers ----------------------

def parse_ms(s: str):
    if not s: return None
    s = s.strip()
    try:
        if s.count(':') == 2:
            h, m, rest = s.split(':')
            sec, ms = rest.split('.') if '.' in rest else (rest, '0')
            return int(h)*3600000 + int(m)*60000 + int(sec)*1000 + int(ms.ljust(3,'0')[:3])
        elif s.count(':') == 1:
            m, rest = s.split(':')
            sec, ms = rest.split('.') if '.' in rest else (rest, '0')
            return int(m)*60000 + int(sec)*1000 + int(ms.ljust(3,'0')[:3])
        else:
            return None
    except Exception:
        return None


def fmt_ms(ms):
    if ms is None: return '—'
    total_seconds = ms // 1000
    m = (total_seconds // 60) % 60
    h = total_seconds // 3600
    s = total_seconds % 60
    frac = ms % 1000
    return (f"{h:02d}:{m:02d}:{s:02d}.{frac:03d}" if h else f"{m:02d}:{s:02d}.{frac:03d}")


def get_color_class(delta):
    if delta is None:
        return ''
    try:
        d = int(delta)
    except Exception:
        return ''
    if d <= DELTA_FAST_MAX:
        return 'cell-fast'
    if d <= DELTA_GOOD_MAX:
        return 'cell-good'
    if d <= DELTA_WARN_MAX:
        return 'cell-warn'
    return 'cell-slow'

app.jinja_env.globals['fmt_ms'] = fmt_ms
app.jinja_env.globals['get_color_class'] = get_color_class

# ---------------------- DB helpers ----------------------

def _conn_or_flash():
    if get_mysql_conn is None:
        flash('db_config não carregado. Verifique se o arquivo existe em {}.'.format(ROOT_DIR))
        return None
    try:
        return get_mysql_conn()
    except Exception as e:
        flash('Falha ao conectar ao MySQL: {}'.format(e))
        boot_logger.error('MySQL connection error: %s', e)
        return None


def get_current_race_id(conn):
    try:
        cur = conn.cursor(); cur.execute("SELECT race_id FROM app_config WHERE id = 1")
        row = cur.fetchone(); cur.close()
        if row and row[0]: return row[0]
        cur = conn.cursor(); cur.execute("SELECT MAX(race_id) FROM competitors")
        row = cur.fetchone(); cur.close()
        return row[0] if row and row[0] else None
    except Exception as e:
        boot_logger.error('get_current_race_id error: %s', e)
        return None


def fetch_competitor_basic(conn, race_id, racer_id):
    cur = conn.cursor()
    try:
        cur.execute("SELECT racer_id, number, first_name, last_name, last_lap_time FROM competitors WHERE race_id=%s AND racer_id=%s", (race_id, racer_id))
        row = cur.fetchone()
        return None if not row else {
            'racer_id': row[0], 'number': row[1], 'first_name': row[2], 'last_name': row[3], 'last_lap_ms': parse_ms(row[4])
        }
    finally:
        cur.close()


def fetch_last_n_laps_ms(conn, race_id, racer_id, n=5):
    cur = conn.cursor()
    try:
        cur.execute("SELECT lap_number, lap_time FROM competitor_laps WHERE race_id=%s AND racer_id=%s ORDER BY lap_number DESC LIMIT %s", (race_id, racer_id, n))
        rows = cur.fetchall(); laps = [parse_ms(t[1]) for t in rows]
        return laps[::-1]
    finally:
        cur.close()


def fetch_global_lastlap_mean_ms(conn, race_id):
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT cl.lap_time
            FROM competitor_laps cl
            JOIN (
                SELECT racer_id, MAX(lap_number) AS max_lap
                FROM competitor_laps
                WHERE race_id = %s
                GROUP BY racer_id
            ) t ON t.racer_id = cl.racer_id AND t.max_lap = cl.lap_number
            WHERE cl.race_id = %s
            """,
            (race_id, race_id)
        )
        vals = []
        for (lap_time_str,) in cur.fetchall():
            ms = parse_ms(lap_time_str)
            if ms is not None and ms <= 120000:  # ignora > 2:00
                vals.append(ms)
        return int(mean(vals)) if vals else None
    finally:
        cur.close()


def fetch_top_positions(conn, race_id, positions=(1,2,3)):
    cur = conn.cursor()
    try:
        data = []
        for pos in positions:
            cur.execute("SELECT racer_id FROM competitors WHERE race_id=%s AND position=%s", (race_id, pos))
            racers = [r[0] for r in cur.fetchall()]
            if not racers: continue
            max_lap, chosen = -1, None
            for rid in racers:
                cur.execute("SELECT MAX(lap_number) FROM competitor_laps WHERE race_id=%s AND racer_id=%s", (race_id, rid))
                ml = cur.fetchone()[0] or 0
                if ml > max_lap: max_lap, chosen = ml, rid
            if chosen: data.append({'position': pos, 'racer_id': chosen})
        return data
    finally:
        cur.close()


def fetch_fastest_slowest_lastlaps(conn, race_id, top=5):
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT cl.racer_id, cl.lap_time
            FROM competitor_laps cl
            JOIN (
                SELECT racer_id, MAX(lap_number) AS max_lap
                FROM competitor_laps
                WHERE race_id = %s
                GROUP BY racer_id
            ) t ON t.racer_id = cl.racer_id AND t.max_lap = cl.lap_number
            WHERE cl.race_id = %s
            """,
            (race_id, race_id)
        )
        rows = [(r, parse_ms(t)) for r, t in cur.fetchall()]
        rows = [(r, ms) for r, ms in rows if ms is not None]
        fast = sorted(rows, key=lambda x: x[1])[:top]
        rows90 = [(r, ms) for r, ms in rows if ms <= 90000]  # slowest ignora >1:30
        slow = sorted(rows90, key=lambda x: x[1], reverse=True)[:top]
        avg_ms = int(mean([ms for _, ms in rows if ms <= 120000])) if rows else None
        return fast, slow, avg_ms
    finally:
        cur.close()


def build_comp_row(conn, race_id, racer_id):
    base = fetch_competitor_basic(conn, race_id, racer_id)
    if not base: return None
    last5 = fetch_last_n_laps_ms(conn, race_id, racer_id, 5)
    last10 = fetch_last_n_laps_ms(conn, race_id, racer_id, 10)
    return {
        'racer_id': base['racer_id'], 'number': base['number'], 'first_name': base['first_name'], 'last_name': base['last_name'],
        'last_lap_ms': last5[-1] if last5 else base['last_lap_ms'], 'last5_ms': last5,
        'avg5_ms': int(mean([x for x in last5 if x is not None])) if last5 else None,
        'avg10_ms': int(mean([x for x in last10 if x is not None])) if last10 else None,
    }

# ---------------------- Health ----------------------
@app.route('/healthz')
def healthz():
    return jsonify({'status': 'ok', 'time': datetime.now().isoformat(), 'templates': app.template_folder, 'static': app.static_folder})

# ---------------------- Home ----------------------
@app.route('/')
def home():
    return render_template('home.html', app_title=APP_TITLE)

# ---------------------- Dashboard ----------------------
@app.route('/dashboard', methods=['GET'])
def dashboard():
    conn = _conn_or_flash()
    if conn is None:
        return render_template('dashboard.html', app_title=APP_TITLE,
                               race_id=None, main_rows=[], global_avg_ms=None,
                               pos_rows=[], fastest_rows=[], slowest_rows=[], avg_last_ms=None,
                               chosen_rows=[], chosen_numbers='', auto_refresh=False)
    try:
        race_id = request.args.get('race_id', type=int) or get_current_race_id(conn)
        if not race_id:
            flash('Nenhum race_id encontrado. Preencha o app_config (id=1) ou garanta dados em competitors.')
            return render_template('dashboard.html', app_title=APP_TITLE,
                                   race_id=None, main_rows=[], global_avg_ms=None,
                                   pos_rows=[], fastest_rows=[], slowest_rows=[], avg_last_ms=None,
                                   chosen_rows=[], chosen_numbers='', auto_refresh=False)
        auto_refresh = request.args.get('auto', 'off') == 'on'

        # Grupo principal 2min
        cur = conn.cursor(); cur.execute("SELECT racer_id FROM update_group_2min ORDER BY racer_id ASC")
        main_ids = [r[0] for r in cur.fetchall()]; cur.close()
        main_rows = [r for rid in main_ids if (r:=build_comp_row(conn, race_id, rid))]

        global_avg_ms = fetch_global_lastlap_mean_ms(conn, race_id)
        pos_rows = []
        for info in fetch_top_positions(conn, race_id, positions=(1,2,3)):
            r = build_comp_row(conn, race_id, info['racer_id']);
            if r: r['position'] = info['position']; pos_rows.append(r)

        fastest, slowest, avg_last_ms = fetch_fastest_slowest_lastlaps(conn, race_id, top=5)
        fastest_rows = [build_comp_row(conn, race_id, rid) for rid, _ in fastest]
        slowest_rows = [build_comp_row(conn, race_id, rid) for rid, _ in slowest]

        # Karts selecionados (múltiplos)
        chosen_numbers = (request.args.get('kart_numbers') or '').strip()
        chosen_rows = []
        if chosen_numbers:
            nums = [n.strip() for n in chosen_numbers.replace(';', ',').split(',') if n.strip()]
            if nums:
                cur = conn.cursor()
                for num in nums:
                    cur.execute("SELECT racer_id FROM competitors WHERE race_id=%s AND number=%s", (race_id, num))
                    row = cur.fetchone()
                    if row:
                        r = build_comp_row(conn, race_id, row[0])
                        if r: chosen_rows.append(r)
                cur.close()

        if not main_rows:
            flash('Grupo 2min vazio ou sem dados para o race_id atual. Use Configuração → Popular grupos.')
        if global_avg_ms is None:
            flash('Não foi possível calcular a média da última volta (verifique competitor_laps).')

        return render_template('dashboard.html', app_title=APP_TITLE,
                               race_id=race_id, main_rows=main_rows, global_avg_ms=global_avg_ms,
                               pos_rows=pos_rows, fastest_rows=fastest_rows, slowest_rows=slowest_rows,
                               avg_last_ms=avg_last_ms, chosen_rows=chosen_rows, chosen_numbers=chosen_numbers,
                               auto_refresh=auto_refresh)
    finally:
        try: conn.close()
        except Exception: pass

# ---------------------- Box Eval (NULL no 8º param por padrão) ----------------------
ALLOWED_PROCS = {
    'sp_kart_box_ranking', 'sp_kart_box_summary',
    'my_karting_app.sp_kart_box_ranking', 'my_karting_app.sp_kart_box_summary',
}

BASE_PARAMS = [None, None, None, None, None, 1, 1, None, None]


def callproc_with(conn, sp_name, params):
    sets = []
    try:
        try:
            cur = conn.cursor(buffered=True)
        except Exception:
            cur = conn.cursor()
        box_logger.info('CALLPROC: %s params=%s', sp_name, params)
        cur.callproc(sp_name, params)
        # mysql-connector path
        try:
            stored = list(cur.stored_results())
        except Exception:
            stored = []
        if stored:
            for rs in stored:
                cols = [d[0] for d in (rs.description or [])]
                rows = rs.fetchall() if rs.description else []
                sets.append((cols, rows))
                box_logger.info('CALLPROC stored_results: cols=%s rows=%d', cols, len(rows))
        else:
            while True:
                if getattr(cur, 'description', None):
                    cols = [d[0] for d in cur.description]
                    rows = cur.fetchall(); sets.append((cols, rows))
                    box_logger.info('CALLPROC nextset: cols=%s rows=%d', cols, len(rows))
                if not cur.nextset(): break
        conn.commit()
    finally:
        try:
            while True:
                if getattr(cur, 'description', None):
                    _ = cur.fetchall()
                if not cur.nextset(): break
            cur.close()
        except Exception:
            pass
    return sets


def run_both_procs_for_intervals(intervals):
    out = {'ranking': [], 'summary': []}
    for (mn, mx) in intervals:
        p = list(BASE_PARAMS)
        p[1] = int(mn)
        p[2] = int(mx)
        conn_a = get_mysql_conn(); rank_sets = callproc_with(conn_a, 'my_karting_app.sp_kart_box_ranking', p)
        try: conn_a.close()
        except Exception: pass
        conn_b = get_mysql_conn(); sum_sets  = callproc_with(conn_b, 'my_karting_app.sp_kart_box_summary', p)
        try: conn_b.close()
        except Exception: pass
        out['ranking'].extend([(c, r, f"{mn}-{mx}") for (c, r) in rank_sets])
        out['summary'].extend([(c, r, f"{mn}-{mx}") for (c, r) in sum_sets])
    return out

BOX_OPTIONS = {
    'opt_230_250': [(230, 250)],
    'opt_two_windows': [(290, 310), (410, 430)],
    'opt_custom': []  # definidas pelo usuário
}

@app.route('/box_eval', methods=['GET', 'POST'])
def box_eval():
    # Restaurar da sessão (lembrar escolhas)
    choice = session.get('box_choice', 'opt_230_250')
    custom_min1 = session.get('custom_min1', '')
    custom_max1 = session.get('custom_max1', '')
    custom_min2 = session.get('custom_min2', '')
    custom_max2 = session.get('custom_max2', '')

    results = {'ranking': [], 'summary': []}
    err = None

    if request.method == 'POST':
        choice = request.form.get('box_choice', choice)
        custom_min1 = request.form.get('custom_min1', custom_min1).strip()
        custom_max1 = request.form.get('custom_max1', custom_max1).strip()
        custom_min2 = request.form.get('custom_min2', custom_min2).strip()
        custom_max2 = request.form.get('custom_max2', custom_max2).strip()

        # Persistir na sessão
        session['box_choice'] = choice
        session['custom_min1'] = custom_min1
        session['custom_max1'] = custom_max1
        session['custom_min2'] = custom_min2
        session['custom_max2'] = custom_max2

        # Montar intervalos conforme escolha
        try:
            if choice == 'opt_custom':
                intervals = []
                if custom_min1 and custom_max1:
                    a, b = int(custom_min1), int(custom_max1)
                    if a > b: a, b = b, a
                    intervals.append((a, b))
                else:
                    raise ValueError('Informe pelo menos o primeiro intervalo (min1 e max1).')
                # segundo intervalo é opcional, mas se um dos dois vier, precisa dos dois
                if any([custom_min2, custom_max2]):
                    if not (custom_min2 and custom_max2):
                        raise ValueError('Para o segundo intervalo, preencha min2 e max2 ou deixe ambos vazios.')
                    a2, b2 = int(custom_min2), int(custom_max2)
                    if a2 > b2: a2, b2 = b2, a2
                    intervals.append((a2, b2))
            else:
                intervals = BOX_OPTIONS.get(choice, BOX_OPTIONS['opt_230_250'])

            # Executar ambas as SPs
            results = run_both_procs_for_intervals(intervals)
        except Exception as e:
            err = str(e)
            box_logger.error('box_eval error: %s', err)

    return render_template('box_eval.html', app_title=APP_TITLE,
                           choice=choice, results=results, err=err,
                           custom_min1=custom_min1, custom_max1=custom_max1,
                           custom_min2=custom_min2, custom_max2=custom_max2)

# ---------------------- Logs viewer ----------------------
@app.route('/box_eval/logs')
def box_eval_logs():
    lines = int(request.args.get('lines', 300))
    log_path = os.path.join(LOGS_DIR, 'box_eval.log')
    content = ''
    try:
        if os.path.exists(log_path):
            with open(log_path, 'r', encoding='utf-8') as f:
                content = ''.join(f.readlines()[-lines:])
        else:
            content = 'Arquivo de log não encontrado: ' + log_path
    except Exception as e:
        content = 'Erro ao ler log: ' + str(e)
    return render_template('box_eval_logs.html', app_title=APP_TITLE, log_content=content, log_path=log_path, lines=lines)

# ---------------------- Config ----------------------
@app.route('/config')
def config():
    return render_template('config.html', app_title=APP_TITLE, scheduler_status=sched.status())

# Utils scripts

def run_script(path, args=None):
    cmd = ['/usr/bin/python3', path]
    if args: cmd.extend(args)
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    return proc.returncode, proc.stdout, proc.stderr

@app.route('/actions/cleanup', methods=['POST'])
def action_cleanup():
    method = request.form.get('method', 'truncate')
    only_comp = request.form.get('only_competitors') == 'on'
    only_laps = request.form.get('only_laps') == 'on'
    dry_run = request.form.get('dry_run') == 'on'
    args = ['--method', method]
    if only_comp: args.append('--only-competitors')
    if only_laps: args.append('--only-laps')
    if dry_run: args.append('--dry-run')
    code, out, err = run_script(CLEANUP_SCRIPT, args)
    flash(f"cleanup_tables.py retornou {code}")
    if out: flash(out)
    if err: flash(err)
    return redirect(url_for('config'))

@app.route('/actions/populate', methods=['POST'])
def action_populate():
    code, out, err = run_script(POPULATE_SCRIPT)
    flash(f"race_monitor_populate_groups.py retornou {code}")
    if out: flash(out)
    if err: flash(err)
    return redirect(url_for('config'))

@app.route('/scheduler/start', methods=['POST'])
def scheduler_start():
    interval = request.form.get('interval_seconds', type=int)
    try:
        sched.start(interval)
        flash(f"Scheduler iniciado com intervalo de {interval}s")
    except Exception as e:
        flash(f"Erro ao iniciar scheduler: {e}")
    return redirect(url_for('config'))

@app.route('/scheduler/stop', methods=['POST'])
def scheduler_stop():
    sched.stop(); flash("Scheduler parado")
    return redirect(url_for('config'))

@app.route('/scheduler/status')
def scheduler_status():
    return jsonify(sched.status())

# ---------------------- Grupos/Logs/app_config ----------------------

def query_group(table_name):
    conn = _conn_or_flash()
    if not conn: return []
    cur = conn.cursor(); cur.execute(f"SELECT racer_id, COALESCE(last_update, 'NULL') FROM {table_name} ORDER BY racer_id ASC")
    rows = cur.fetchall(); cur.close(); conn.close(); return rows

@app.route('/groups/<group_name>')
def groups_view(group_name):
    table_map = {'2min': 'update_group_2min', '4min': 'update_group_4min', 'rest': 'update_group_rest'}
    if group_name not in table_map:
        flash('Grupo inválido'); return redirect(url_for('config'))
    rows = query_group(table_map[group_name])
    return render_template('groups.html', app_title=APP_TITLE, group_name=group_name, rows=rows)

from db_config import get_mysql_conn as _get_conn

def add_to_group(table_name, racer_id):
    conn = _get_conn(); cur = conn.cursor()
    cur.execute(f"INSERT IGNORE INTO {table_name} (racer_id, last_update) VALUES (%s, NULL)", (int(racer_id),))
    conn.commit(); cur.close(); conn.close()

@app.route('/groups/<group_name>/add', methods=['POST'])
def groups_add(group_name):
    table_map = {'2min': 'update_group_2min', '4min': 'update_group_4min', 'rest': 'update_group_rest'}
    racer_id = request.form.get('racer_id', type=int)
    if group_name in table_map and racer_id:
        add_to_group(table_map[group_name], racer_id)
        flash(f"Adicionado {racer_id} ao grupo {group_name}")
    else:
        flash("Parâmetros inválidos")
    return redirect(url_for('groups_view', group_name=group_name))


def remove_from_group(table_name, racer_id):
    conn = _get_conn(); cur = conn.cursor()
    cur.execute(f"DELETE FROM {table_name} WHERE racer_id = %s", (int(racer_id),))
    conn.commit(); cur.close(); conn.close()

@app.route('/groups/<group_name>/remove', methods=['POST'])
def groups_remove(group_name):
    table_map = {'2min': 'update_group_2min', '4min': 'update_group_4min', 'rest': 'update_group_rest'}
    racer_id = request.form.get('racer_id', type=int)
    if group_name in table_map and racer_id:
        remove_from_group(table_map[group_name], racer_id)
        flash(f"Removido {racer_id} do grupo {group_name}")
    else:
        flash("Parâmetros inválidos")
    return redirect(url_for('groups_view', group_name=group_name))


def set_group_time(table_name, racer_id, to_now=True):
    conn = _get_conn(); cur = conn.cursor()
    if to_now: cur.execute(f"UPDATE {table_name} SET last_update = NOW() WHERE racer_id = %s", (int(racer_id),))
    else: cur.execute(f"UPDATE {table_name} SET last_update = NULL WHERE racer_id = %s", (int(racer_id),))
    conn.commit(); cur.close(); conn.close()

@app.route('/groups/<group_name>/touch', methods=['POST'])
def groups_touch(group_name):
    table_map = {'2min': 'update_group_2min', '4min': 'update_group_4min', 'rest': 'update_group_rest'}
    racer_id = request.form.get('racer_id', type=int)
    action = request.form.get('action', 'now')
    if group_name in table_map and racer_id:
        set_group_time(table_map[group_name], racer_id, to_now=(action == 'now'))
        flash(f"Atualizado last_update de {racer_id} ({action}) no grupo {group_name}")
    else:
        flash("Parâmetros inválidos")
    return redirect(url_for('groups_view', group_name=group_name))

@app.route('/logs')
def logs_view():
    lines = int(request.args.get('lines', 200))
    content = ""
    try:
        if os.path.exists(RACE_LOG_FILE):
            with open(RACE_LOG_FILE, 'r', encoding='utf-8') as f:
                data = f.readlines()
            content = ''.join(data[-lines:])
        else:
            content = f"Arquivo de log não encontrado: {RACE_LOG_FILE}"
    except Exception as e:
        content = f"Erro ao ler log: {e}"
    return render_template('logs.html', app_title=APP_TITLE, log_content=content, log_path=RACE_LOG_FILE, lines=lines)

@app.route('/config/app_config')
def app_config_list():
    conn = _conn_or_flash(); rows = []
    if conn:
        cur = conn.cursor(); cur.execute("SELECT id, api_token, race_id FROM app_config ORDER BY id ASC")
        rows = cur.fetchall(); cur.close(); conn.close()
    return render_template('app_config.html', app_title=APP_TITLE, rows=rows)

@app.route('/config/app_config/add', methods=['POST'])
def app_config_add():
    id_ = request.form.get('id', type=int)
    token = request.form.get('api_token')
    race_id = request.form.get('race_id', type=int)
    if not id_ or not token or not race_id:
        flash("Preencha ID, api_token e race_id"); return redirect(url_for('app_config_list'))
    conn = _conn_or_flash();
    if not conn:
        flash('Sem conexão com DB'); return redirect(url_for('app_config_list'))
    cur = conn.cursor()
    try:
        cur.execute("INSERT INTO app_config (id, api_token, race_id, last_used, updated_at) VALUES (%s, %s, %s, NULL, NOW())", (id_, token, race_id))
        conn.commit(); flash("Registro criado")
    except Exception as e:
        conn.rollback(); flash(f"Erro ao criar: {e}")
    finally:
        cur.close(); conn.close()
    return redirect(url_for('app_config_list'))

@app.route('/config/app_config/update', methods=['POST'])
def app_config_update():
    id_ = request.form.get('id', type=int)
    token = request.form.get('api_token')
    race_id = request.form.get('race_id', type=int)
    if not id_:
        flash("ID é obrigatório"); return redirect(url_for('app_config_list'))
    conn = _conn_or_flash();
    if not conn:
        flash('Sem conexão com DB'); return redirect(url_for('app_config_list'))
    cur = conn.cursor()
    try:
        cur.execute("UPDATE app_config SET api_token=%s, race_id=%s, updated_at=NOW() WHERE id=%s", (token, race_id, id_))
        conn.commit(); flash("Registro atualizado")
    except Exception as e:
        conn.rollback(); flash(f"Erro ao atualizar: {e}")
    finally:
        cur.close(); conn.close()
    return redirect(url_for('app_config_list'))

@app.route('/config/app_config/delete', methods=['POST'])
def app_config_delete():
    id_ = request.form.get('id', type=int)
    conn = _conn_or_flash();
    if not conn:
        flash('Sem conexão com DB'); return redirect(url_for('app_config_list'))
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM app_config WHERE id=%s", (id_,))
        conn.commit(); flash("Registro removido")
    except Exception as e:
        conn.rollback(); flash(f"Erro ao remover: {e}")
    finally:
        cur.close(); conn.close()
    return redirect(url_for('app_config_list'))

# ---------------------- Run ----------------------
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    boot_logger.info('Running Flask on 0.0.0.0:%s', port)
    app.run(host='0.0.0.0', port=port)
