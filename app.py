"""
app.py  –  Skipperplan Backend (xlsxwriter)
"""
from flask import Flask, jsonify, request, Response
from flask_cors import CORS
import requests, os, io, re, datetime, json, base64, hmac, time
import xlsxwriter

app = Flask(__name__)
CORS(app, origins=["https://wolkeyachting.github.io"])

JTC_BASE       = "https://api-aws.join-the-crew.com"
JTC_HEADERS    = {"Accept":"application/json, text/plain, */*","Accept-Language":"de,en-US;q=0.9,en;q=0.8","Origin":"https://join-the-crew.com","Referer":"https://join-the-crew.com/","X-Vendor":"jtc"}
ALGOLIA_HOST   = "https://m6n4q601zw-dsn.algolia.net"
ALGOLIA_APP_ID = "M6N4Q601ZW"
ALGOLIA_INDEX  = "prod_trip_dates_skipperplan_de"
HITS_PER_PAGE  = 100
GITHUB_REPO    = "wolkeyachting/Skipperplan"

JTC_USER      = os.environ.get("JTC_USER",      "")
JTC_PASSWORD  = os.environ.get("JTC_PASSWORD",  "")
APP_PASSWORD  = os.environ.get("APP_PASSWORD",  "")
GITHUB_TOKEN  = os.environ.get("GITHUB_TOKEN",  "")
RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "")
NOTIFY_EMAILS  = os.environ.get("NOTIFY_EMAILS",  "")

COLUMN_ORDER = [
    'Startdatum','Enddatum','Reisename','Typ','Tage','Altersgruppe',
    'Skipper','ID Skipper','Plätze gesamt','Plätze belegt',
    'Saison','Preisspanne (€)','Land','Kontinent','Anbieter',
    'Trip ID','Trip Date ID','Yacht ID','Yachtmodell','Yachtname',
    'Bootstyp','Baujahr','Yacht Status','Skipper Status','Flotillenführer','Bewerbungen',
]
COL_WIDTHS = {
    'Startdatum':12,'Enddatum':12,'Reisename':28,'Typ':10,'Tage':7,'Altersgruppe':13,
    'Skipper':20,'ID Skipper':10,'Plätze gesamt':14,'Plätze belegt':14,'Saison':9,
    'Preisspanne (€)':15,'Land':12,'Kontinent':11,'Anbieter':10,'Trip ID':9,
    'Trip Date ID':13,'Yacht ID':10,'Yachtmodell':22,'Yachtname':18,'Bootstyp':12,
    'Baujahr':9,'Yacht Status':15,'Skipper Status':22,'Flotillenführer':15,'Bewerbungen':40,
}

SKIPPER_STATUS_LABELS = {
    'confirmed_by_admin_and_skipper': 'Bestätigt',
    'confirmed_by_admin':             'Vom Admin bestätigt',
    'assigned':                       'Zugeteilt (offen)',
}

# ── Hilfsfunktionen ───────────────────────────────────────────────────────────
def ts_to_date(ts):
    if not ts: return ''
    try: return datetime.datetime.fromtimestamp(int(ts), datetime.UTC).strftime('%d.%m.%Y')
    except: return ''

def ts_to_iso(ts):
    if not ts: return ''
    try: return datetime.datetime.fromtimestamp(int(ts), datetime.UTC).strftime('%Y-%m-%d')
    except: return ''

def first(lst, default=''):
    return lst[0] if lst else default

def days_to_weeks(days):
    return round(days / 7)

def parse_days(s):
    m = re.search(r'(\d+)', str(s or ''))
    return int(m.group(1)) if m else 7

def today_iso():
    return datetime.datetime.now(datetime.UTC).strftime('%Y-%m-%d')

# ── GitHub API ────────────────────────────────────────────────────────────────
def gh_headers():
    return {"Authorization": f"Bearer {GITHUB_TOKEN}", "Accept": "application/vnd.github+json"}

def gh_url(path):
    return f"https://api.github.com/repos/{GITHUB_REPO}/contents/{path}"

def gh_get_sha(path):
    """Nur die SHA einer Datei (None, wenn sie nicht existiert). Funktioniert auch über 1 MB."""
    resp = requests.get(gh_url(path), headers=gh_headers(), timeout=30)
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return resp.json()['sha']

def gh_get_file(path):
    """Vollständiger Dateiinhalt + SHA, auch über 1 MB (Raw-Download, bis 100 MB).
    Die normale Contents-API liefert ab 1 MB ein leeres 'content'-Feld – daher der Raw-Header."""
    sha = gh_get_sha(path)
    if sha is None:
        return None, None
    resp = requests.get(gh_url(path),
                        headers={**gh_headers(), "Accept": "application/vnd.github.raw+json"},
                        timeout=60)
    resp.raise_for_status()
    return resp.content, sha

def gh_put_file(path, content_bytes, sha, message):
    payload = {"message": message, "content": base64.b64encode(content_bytes).decode()}
    if sha:
        payload["sha"] = sha
    resp = requests.put(f"https://api.github.com/repos/{GITHUB_REPO}/contents/{path}",
                        headers=gh_headers(), json=payload, timeout=30)
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"GitHub Fehler {resp.status_code}: {resp.text[:200]}")

def compact_hits(hits):
    """Reduziert Hits auf die für den Changelog und Fahrstatus relevanten Felder."""
    compact = []
    for h in hits:
        dest      = h.get('trip_destination', {}) or {}
        start_ts  = h.get('start_date_min') or first(h.get('start_date', []))
        end_ts    = first(h.get('end_date', []))
        proposals = [p['name'] for p in h.get('yacht_skipper_proposals', []) if p.get('name')]
        for y in h.get('yachts', []):
            sk = y.get('skipper') or {}
            compact.append({
                'k':   f"{h.get('trip_date_id')}_{y.get('id')}",
                'tn':  h.get('trip_date_id'),
                'ti':  h.get('trip_id', ''),
                'yi':  y.get('id'),
                'rn':  dest.get('name', ''),
                'sd':  ts_to_date(start_ts),
                'si':  ts_to_iso(start_ts),
                'ed':  ts_to_date(end_ts),
                'ei':  ts_to_iso(end_ts),
                'ym':  y.get('accomodation_details_name', ''),
                'yn':  y.get('yacht_name', ''),
                'yt':  y.get('accomodation_details_type', ''),
                'yy':  y.get('yacht_year', ''),
                'ys':  y.get('status', ''),
                'sn':  sk.get('name', ''),
                'si2': sk.get('id', ''),
                'sst': sk.get('status', ''),
                'sfl': sk.get('is_flotilla_leader', False),
                'pl':  y.get('places', ''),
                'po':  y.get('occupied_places', ''),
                'pr':  proposals,
                'tg':  first(h.get('trip_days', [])),
                'ag':  first(h.get('age_range', [])),
                'sa':  first(h.get('season', [])),
                'pp':  first(h.get('price_range', [])),
                'la':  dest.get('country', ''),
                'co':  dest.get('continent', ''),
                'an':  h.get('vendor', ''),
                'ty':  h.get('type', ''),
            })
    return compact

def expand_compact(compact_list):
    """Wandelt kompakte Hits zurück in das für detect_changes erwartete Format."""
    hits_by_trip = {}
    for c in compact_list:
        tid = c['tn']
        if tid not in hits_by_trip:
            hits_by_trip[tid] = {
                'trip_date_id': tid,
                'trip_destination': {'name': c['rn']},
                'start_date': [],
                'start_date_min': None,
                'end_date': [],
                'yacht_skipper_proposals': [{'name': p} for p in c.get('pr', [])],
                'yachts': [],
            }
        hits_by_trip[tid]['yachts'].append({
            'id': c['yi'],
            'accomodation_details_name': c['ym'],
            'yacht_name': c['yn'],
            'status': c['ys'],
            'skipper': {'name': c['sn'], 'id': c['si2']},
        })
        # Datum aus erstem Yacht-Eintrag nehmen
        if not hits_by_trip[tid]['start_date_min']:
            hits_by_trip[tid]['_sd'] = c['sd']
            hits_by_trip[tid]['_si'] = c['si']
            hits_by_trip[tid]['_ed'] = c['ed']
            hits_by_trip[tid]['_ei'] = c['ei']
    return list(hits_by_trip.values())


# ── Gespeicherter Zustand im Repo ─────────────────────────────────────────────
HISTORY_FILE   = "history.json"          # daily_hits + currently_sailing
HISTORY_BACKUP = "history_backup.json"
PAST_FILE      = "past_trips.json"       # vergangene Törns (wird nie automatisch gekürzt)
PAST_BACKUP    = "past_trips_backup.json"

def load_json(path, backup_path, default):
    """Lädt eine JSON-Datei aus dem Repo.
    Rückgabe: (daten, roh_bytes_wenn_gültig, sha).
    - Datei fehlt                → Backup, sonst Standardwert
    - Datei vorhanden, gültig    → Inhalt
    - Datei vorhanden, unlesbar  → Backup; ist auch das unlesbar: Abbruch.
      So wird nie mehr ein Standardwert über echte Daten geschrieben."""
    content, sha = gh_get_file(path)
    if content:
        try:
            return json.loads(content.decode('utf-8')), content, sha
        except Exception:
            print(f"[load_json] {path} ist unlesbar – versuche {backup_path}")
    backup, _ = gh_get_file(backup_path)
    if backup:
        try:
            data = json.loads(backup.decode('utf-8'))
            print(f"[load_json] {path} aus {backup_path} geladen")
            return data, None, sha
        except Exception:
            pass
    if content is not None:
        raise RuntimeError(f"{path} existiert, ist aber nicht lesbar (Backup ebenfalls) – "
                           f"Lauf abgebrochen, damit keine Daten überschrieben werden.")
    return default, None, sha

def save_json(path, backup_path, data, previous_raw, sha, message):
    """Sichert zuerst die bisherige gültige Version als Backup, dann wird gespeichert.
    Die SHA stammt vom Laden – hat jemand die Datei zwischenzeitlich geändert,
    lehnt GitHub das Speichern ab statt sie still zu überschreiben."""
    if previous_raw:
        gh_put_file(backup_path, previous_raw, gh_get_sha(backup_path), f"Backup: {message}")
    gh_put_file(path, json.dumps(data, ensure_ascii=False).encode('utf-8'), sha, message)

# ── Daten laden ───────────────────────────────────────────────────────────────
def login():
    resp = requests.post(f"{JTC_BASE}/de/api/users/login",
                         json={"user": JTC_USER, "password": JTC_PASSWORD},
                         headers=JTC_HEADERS, timeout=15)
    resp.raise_for_status()
    key = (resp.json().get("api_keys") or {}).get("algolia", {}).get("skipperplan")
    if not key: raise ValueError("Algolia-Key nicht gefunden")
    return key

def fetch_algolia(api_key):
    hdrs = {"x-algolia-application-id": ALGOLIA_APP_ID, "x-algolia-api-key": api_key, "Content-Type": "application/json"}
    all_hits, page, total = [], 0, None
    while True:
        body = {"requests": [{"indexName": ALGOLIA_INDEX, "params": f"hitsPerPage={HITS_PER_PAGE}&page={page}&query="}]}
        resp = requests.post(f"{ALGOLIA_HOST}/1/indexes/*/queries", headers=hdrs, json=body, timeout=30)
        resp.raise_for_status()
        result = resp.json()["results"][0]
        hits   = result.get("hits", [])
        all_hits.extend(hits)
        if total is None: total = result.get("nbHits", 0)
        if len(all_hits) >= total or len(hits) < HITS_PER_PAGE: break
        page += 1
    seen, unique = set(), []
    for h in all_hits:
        tid = h.get("trip_date_id") or h.get("objectID")
        if tid not in seen:
            seen.add(tid); unique.append(h)
    return unique

# ── Zeilen extrahieren ────────────────────────────────────────────────────────
def make_trip_row(h, y, i, proposals):
    dest      = h.get('trip_destination', {}) or {}
    start_ts  = h.get('start_date_min') or first(h.get('start_date', []))
    end_ts    = first(h.get('end_date', []))
    sk        = (y.get('skipper') or {})
    sk_status = sk.get('status', '')
    sk_name   = sk.get('name', '')
    ys        = y.get('status', '')
    if ys == 'should_be_canceled':  color = 'cancel'
    elif not sk_name:               color = 'no_skipper'
    elif sk_status == 'assigned':   color = 'unconfirmed'
    else:                           color = None
    return {
        'Startdatum':      ts_to_date(start_ts),
        'Enddatum':        ts_to_date(end_ts),
        '_start_iso':      ts_to_iso(start_ts),
        '_end_iso':        ts_to_iso(end_ts),
        '_yacht_key':      f"{h.get('trip_date_id')}_{y.get('id')}",
        '_color':          color,
        'Reisename':       dest.get('name', ''),
        'Typ':             h.get('type', ''),
        'Tage':            first(h.get('trip_days', [])),
        'Altersgruppe':    first(h.get('age_range', [])),
        'Saison':          first(h.get('season', [])),
        'Preisspanne (€)': first(h.get('price_range', [])),
        'Land':            dest.get('country', ''),
        'Kontinent':       dest.get('continent', ''),
        'Anbieter':        h.get('vendor', ''),
        'Trip ID':         str(h.get('trip_id', '')),
        'Trip Date ID':    str(h.get('trip_date_id', '')),
        'Skipper':         sk_name if sk_name else '⚠ Kein Skipper',
        'ID Skipper':      str(sk.get('id', '')),
        'Plätze gesamt':   y.get('places', ''),
        'Plätze belegt':   y.get('occupied_places', ''),
        'Yacht ID':        str(y.get('id', '')),
        'Yachtmodell':     y.get('accomodation_details_name', ''),
        'Yachtname':       y.get('yacht_name', ''),
        'Bootstyp':        y.get('accomodation_details_type', ''),
        'Baujahr':         y.get('yacht_year', ''),
        'Yacht Status':    'Zu stornieren' if ys == 'should_be_canceled' else 'Bestätigt',
        'Skipper Status':  SKIPPER_STATUS_LABELS.get(sk_status, sk_status or '—'),
        'Flotillenführer': 'Ja' if sk.get('is_flotilla_leader') else 'Nein',
        'Bewerbungen':     ', '.join(proposals) if i == 0 else '',
    }

def extract_rows(hits):
    rows = []
    for h in hits:
        proposals = [p['name'] for p in h.get('yacht_skipper_proposals', []) if p.get('name')]
        for i, y in enumerate(h.get('yachts', [])):
            rows.append(make_trip_row(h, y, i, proposals))
    return rows

# ── Saisons ───────────────────────────────────────────────────────────────────
# saisons.json im Repo ordnet jede Reise (Trip ID) dem Sommer oder Winter zu:
#   { "40": {"name": "Polynesien", "saison": "Winter"}, ... }
# Das Saisonjahr ergibt sich aus dem Startdatum:
#   Sommer         → "SS 26"
#   Winter ab Juli → "WS 26-27", Winter Jan–Juni → "WS 25-26"
# ("/" ist in Excel-Blattnamen nicht erlaubt, daher "-")
SEASON_FILE = "saisons.json"

def load_season_map():
    """Liefert (zuordnung, fehlermeldung). Eine fehlerhafte Datei bricht den Lauf nicht ab,
    sondern erscheint in der Mail unter 'Aktion erforderlich'."""
    content, _ = gh_get_file(SEASON_FILE)
    if content is None:
        return {}, f"{SEASON_FILE} fehlt im Repo – keine Reise ist einer Saison zugeordnet."
    try:
        data = json.loads(content.decode('utf-8'))
        return {str(k): v for k, v in data.items()}, None
    except Exception as e:
        return {}, f"{SEASON_FILE} ist fehlerhaft und wurde ignoriert ({e})."

def season_of(row, season_map):
    """(Blattname-Kürzel, Sortierschlüssel) oder None, wenn die Reise nicht zugeordnet ist."""
    entry = season_map.get(str(row.get('Trip ID', '')))
    kind  = (entry or {}).get('saison') if isinstance(entry, dict) else None
    start = row.get('_start_iso', '')
    if kind not in ('Sommer', 'Winter') or len(start) < 7:
        return None
    y, m = int(start[:4]), int(start[5:7])
    if kind == 'Sommer':
        return f"SS {y % 100:02d}", y * 2
    a = y if m >= 7 else y - 1
    return f"WS {a % 100:02d}-{(a + 1) % 100:02d}", a * 2 + 1

def has_skipper(row):
    sid = str(row.get('ID Skipper', '') or '').strip()
    return bool(sid) and sid != 'None'

def build_season_data(rows, currently_sailing, past_trips, season_map):
    """Teilt alle Törns auf Saisons auf.
    Rückgabe:
      seasons       {kürzel: {'key', 'past': [...], 'skipper': [(name, id, törns, wochen), ...]}}
      past_unassigned  vergangene Törns ohne Zuordnung (Reiter 'Ohne Saison')
      unassigned_trips Reisen ohne Zuordnung für die Mail (alle Zeiträume)"""
    from collections import defaultdict
    seasons = {}
    sk_count = defaultdict(lambda: defaultdict(lambda: {'name': '', 'törns': 0, 'wochen': 0}))
    past_unassigned = []
    unassigned = {}

    def handle(row, is_past):
        s = season_of(row, season_map)
        if s is None:
            tid = str(row.get('Trip ID', '') or '?')
            u = unassigned.setdefault(tid, {'trip_id': tid, 'namen': set(), 'von': '9999', 'bis': '0000', 'termine': set()})
            u['namen'].add(row.get('Reisename', ''))
            u['von'] = min(u['von'], row.get('_start_iso', '') or '9999')
            u['bis'] = max(u['bis'], row.get('_end_iso', '') or '0000')
            u['termine'].add(row.get('Trip Date ID', ''))
            if is_past:
                past_unassigned.append(row)
            return
        label, key = s
        season = seasons.setdefault(label, {'key': key, 'past': [], 'skipper': []})
        if is_past:
            season['past'].append(row)
        if has_skipper(row):
            d = sk_count[label][str(row['ID Skipper'])]
            d['name']    = row.get('Skipper', '')
            d['törns']  += 1
            d['wochen'] += days_to_weeks(parse_days(row.get('Tage', '')))

    for r in past_trips:        handle(r, True)
    for r in currently_sailing: handle(r, False)
    for r in rows:              handle(r, False)

    for label, skippers in sk_count.items():
        seasons[label]['skipper'] = sorted(
            ((d['name'], int(sid) if sid.isdigit() else sid, d['törns'], d['wochen'])
             for sid, d in skippers.items()),
            key=lambda x: (-x[2], x[0]))

    unassigned_trips = sorted(
        ({'trip_id': u['trip_id'], 'name': ', '.join(sorted(n for n in u['namen'] if n)),
          'von': u['von'], 'bis': u['bis'], 'termine': len(u['termine'])}
         for u in unassigned.values()),
        key=lambda x: x['von'])
    return seasons, past_unassigned, unassigned_trips

# ── Changelog ────────────────────────────────────────────────────────────────
def detect_changes(new_hits, prev_compact, today):
    """Vergleicht neue Hits mit kompakt gespeicherten vorherigen."""
    def index_new(hits):
        idx = {}
        for h in hits:
            dest      = h.get('trip_destination', {}) or {}
            start_ts  = h.get('start_date_min') or first(h.get('start_date', []))
            end_ts    = first(h.get('end_date', []))
            proposals = [p['name'] for p in h.get('yacht_skipper_proposals', []) if p.get('name')]
            for y in h.get('yachts', []):
                key = f"{h.get('trip_date_id')}_{y.get('id')}"
                sk  = y.get('skipper') or {}
                idx[key] = {
                    'typ': '', 'reisename': dest.get('name', ''),
                    'startdatum': ts_to_date(start_ts), 'start_iso': ts_to_iso(start_ts),
                    'enddatum': ts_to_date(end_ts),
                    'yachtmodell': y.get('accomodation_details_name', ''),
                    'yachtname': y.get('yacht_name', ''),
                    'skipper_name': sk.get('name', ''), 'skipper_id': sk.get('id', ''),
                    'yacht_status': y.get('status', ''), 'proposals': proposals,
                    'skipper_alt': '', 'neue_bewerbungen': '',
                }
        return idx

    def index_compact(compact_list):
        idx = {}
        for c in compact_list:
            idx[c['k']] = {
                'typ': '', 'reisename': c['rn'],
                'startdatum': c['sd'], 'start_iso': c['si'],
                'enddatum': c['ed'],
                'yachtmodell': c['ym'], 'yachtname': c['yn'],
                'skipper_name': c['sn'], 'skipper_id': c['si2'],
                'yacht_status': c['ys'], 'proposals': c.get('pr', []),
                'skipper_alt': '', 'neue_bewerbungen': '',
            }
        return idx

    new_idx  = index_new(new_hits)
    prev_idx = index_compact(prev_compact)
    changes  = []

    for key, nd in new_idx.items():
        if key not in prev_idx:
            entry = dict(nd); entry['typ'] = 'Neues Boot'
            changes.append(entry)

    for key, pd in prev_idx.items():
        if key not in new_idx:
            if pd['start_iso'] > today:
                entry = dict(pd); entry['typ'] = 'Storniert'; entry['skipper_alt'] = pd['skipper_name']
                changes.append(entry)
        else:
            nd = new_idx[key]
            entry = dict(nd)
            changed = False
            if pd['skipper_id'] != nd['skipper_id']:
                entry['typ'] = 'Skipper geändert'
                entry['skipper_alt'] = pd['skipper_name'] or '(Kein Skipper)'
                changed = True
            new_proposals = [p for p in nd['proposals'] if p not in pd['proposals']]
            if new_proposals:
                entry['neue_bewerbungen'] = ', '.join(new_proposals)
                if not changed: entry['typ'] = 'Neue Bewerbung(en)'
                changed = True
            if changed:
                changes.append(entry)

    # Neue Bewerbungen pro Termin zusammenfassen (nicht pro Boot)
    proposal_by_trip = {}
    other_changes = []
    for ch in changes:
        if ch.get('typ') == 'Neue Bewerbung(en)':
            tid = ch.get('start_iso','') + '_' + ch.get('reisename','')
            if tid not in proposal_by_trip:
                proposal_by_trip[tid] = dict(ch)
                proposal_by_trip[tid]['_names'] = set(ch.get('neue_bewerbungen','').split(', '))
                # Prüfen ob mind. ein Boot keinen Skipper hat
                proposal_by_trip[tid]['_has_free'] = (ch.get('skipper_name','') == '⚠ Kein Skipper')
            else:
                proposal_by_trip[tid]['_names'].update(ch.get('neue_bewerbungen','').split(', '))
                if ch.get('skipper_name','') == '⚠ Kein Skipper':
                    proposal_by_trip[tid]['_has_free'] = True
        else:
            other_changes.append(ch)

    for entry in proposal_by_trip.values():
        names = ', '.join(sorted(entry['_names']))
        free  = ' (⚠ freies Boot im Termin)' if entry.get('_has_free') else ''
        entry['neue_bewerbungen'] = names + free
        entry['yachtmodell'] = ''  # kein spezifisches Boot mehr
        other_changes.append(entry)

    return sorted(other_changes, key=lambda x: x.get('start_iso', ''))

# ── Fahrstatus ────────────────────────────────────────────────────────────────
def compact_to_row(c):
    """Kompakten daily_hits-Eintrag in eine vollständige Tabellenzeile umwandeln."""
    return {
                    '_yacht_key':    c['k'],
                    '_start_iso':    c.get('si', ''),
                    '_end_iso':      c.get('ei', ''),
                    'Startdatum':    c.get('sd', ''),
                    'Enddatum':      c.get('ed', ''),
                    'Reisename':     c.get('rn', ''),
                    'Typ':           c.get('ty', ''),
                    'Tage':          c.get('tg', ''),
                    'Altersgruppe':  c.get('ag', ''),
                    'Saison':        c.get('sa', ''),
                    'Preisspanne (€)': c.get('pp', ''),
                    'Land':          c.get('la', ''),
                    'Kontinent':     c.get('co', ''),
                    'Anbieter':      c.get('an', ''),
                    'Trip ID':       str(c.get('ti', '')),
                    'Trip Date ID':  str(c.get('tn', '')),
                    'Skipper':       c.get('sn', '') or '—',
                    'ID Skipper':    str(c.get('si2', '')),
                    'Plätze gesamt': c.get('pl', ''),
                    'Plätze belegt': c.get('po', ''),
                    'Yacht ID':      str(c.get('yi', '')),
                    'Yachtmodell':   c.get('ym', ''),
                    'Yachtname':     c.get('yn', ''),
                    'Bootstyp':      c.get('yt', ''),
                    'Baujahr':       c.get('yy', ''),
                    'Yacht Status':  'Bestätigt',
                    'Skipper Status': SKIPPER_STATUS_LABELS.get(c.get('sst',''), c.get('sst','') or '—'),
                    'Flotillenführer': 'Ja' if c.get('sfl') else 'Nein',
                    'Bewerbungen':   ', '.join(c.get('pr', [])),
    }

def update_sailing_status(history, past_trips, new_hits, prev_compact, today):
    """Boote, die seit dem letzten Tageslauf aus dem Skipperplan verschwunden sind:
    - Start in der Zukunft          → Storno (nur Changelog, hier ignoriert)
    - gestartet, Ende noch offen    → currently_sailing
    - gestartet UND schon beendet   → direkt past_trips (z.B. nach einem Ausfall)
    Danach wandern beendete Törns aus currently_sailing nach past_trips."""
    new_idx = {f"{h.get('trip_date_id')}_{y.get('id')}"
               for h in new_hits for y in h.get('yachts', [])}
    sailing   = history.setdefault('currently_sailing', [])
    past_keys = {r['_yacht_key'] for r in past_trips}
    known     = {r['_yacht_key'] for r in sailing} | past_keys

    for c in prev_compact:
        key = c['k']
        if key in new_idx or key in known:
            continue
        start_iso, end_iso = c.get('si', ''), c.get('ei', '')
        if not start_iso or start_iso > today:
            continue
        row = compact_to_row(c)
        if end_iso and end_iso < today:
            past_trips.append(row)
            past_keys.add(key)
        else:
            sailing.append(row)
        known.add(key)

    still = []
    for row in sailing:
        if row.get('_end_iso', '') < today:
            if row['_yacht_key'] not in past_keys:
                past_trips.append(row)
                past_keys.add(row['_yacht_key'])
        else:
            still.append(row)
    history['currently_sailing'] = still
    return history, past_trips

# ── Excel bauen (xlsxwriter) ──────────────────────────────────────────────────
def build_excel(rows, changelog, currently_sailing, seasons, past_unassigned):
    buf = io.BytesIO()
    wb  = xlsxwriter.Workbook(buf, {'in_memory': True})

    # Formate definieren
    def fmt(bg='FFFFFF', bold=False, center=False, font_color='000000'):
        return wb.add_format({
            'font_name':  'Arial',
            'font_size':  11 if bold else 10,
            'bold':       bold,
            'font_color': font_color,
            'bg_color':   f'#{bg}',
            'align':      'center' if center else 'left',
            'valign':     'vcenter',
            'border':     1,
            'border_color': '#BFBFBF',
            'text_wrap':  bold,
        })

    hdr      = fmt('1F4E79', bold=True, center=True, font_color='FFFFFF')
    row_a    = fmt('D6E4F0')
    row_b    = fmt('FFFFFF')
    no_skip  = fmt('B4F7B4')
    cancel   = fmt('FFCCCC')
    unconf   = fmt('FCE4D6')
    cl_chg   = fmt('FFF2CC')
    cl_new   = fmt('E2EFDA')
    cl_can   = fmt('FCE4D6')
    cl_bew   = fmt('D6E4F0')
    center_a = fmt('D6E4F0', center=True)
    center_b = fmt('FFFFFF', center=True)

    color_fmt = {'cancel': cancel, 'no_skipper': no_skip, 'unconfirmed': unconf}

    def write_trip_sheet(ws_name, trip_rows, sort_key=None, reverse=False):
        ws = wb.add_worksheet(ws_name)
        ws.freeze_panes(1, 0)
        ws.autofilter(0, 0, 0, len(COLUMN_ORDER)-1)
        for c, col in enumerate(COLUMN_ORDER):
            ws.set_column(c, c, COL_WIDTHS.get(col, 14))
            ws.write(0, c, col, hdr)
        ws.set_row(0, 32)

        data = sorted(trip_rows, key=lambda x: x.get(sort_key,''), reverse=reverse) if sort_key else trip_rows
        for r, row in enumerate(data, 1):
            cf = color_fmt.get(row.get('_color')) or (row_a if r%2==1 else row_b)
            for c, col in enumerate(COLUMN_ORDER):
                ws.write(r, c, row.get(col, ''), cf)
        return ws

    # Segelreisen
    ws_main = write_trip_sheet('Segelreisen', rows)
    lc = len(COLUMN_ORDER) + 1
    ws_main.write(0, lc, 'Legende', fmt('FFFFFF', bold=True))
    ws_main.set_column(lc, lc, 26)
    for i, (bg, label) in enumerate([(no_skip,'⚠ Kein Skipper'),(cancel,'🚫 Zu stornieren'),(unconf,'⏳ Nicht bestätigt')], 1):
        ws_main.write(i, lc, label, bg)

    # Aktuell fahrend
    write_trip_sheet('Aktuell fahrend', currently_sailing or [], sort_key='_start_iso')

    # Changelog
    if changelog is not None:
        ws_cl = wb.add_worksheet('Changelog')
        cl_cols = ['Typ','Reisename','Startdatum','Enddatum','Yachtmodell','Yachtname',
                   'Skipper (neu)','Skipper (alt)','Neue Bewerbungen']
        cl_widths = [18,28,12,12,22,18,20,20,40]
        ws_cl.freeze_panes(1, 0)
        ws_cl.autofilter(0, 0, 0, len(cl_cols)-1)
        for c, (col, w) in enumerate(zip(cl_cols, cl_widths)):
            ws_cl.set_column(c, c, w)
            ws_cl.write(0, c, col, hdr)
        ws_cl.set_row(0, 32)
        type_fmt = {'Skipper geändert': cl_chg, 'Neues Boot': cl_new,
                    'Storniert': cl_can, 'Neue Bewerbung(en)': cl_bew}
        for r, ch in enumerate(changelog, 1):
            cf = type_fmt.get(ch.get('typ',''), row_b)
            vals = [ch.get('typ',''), ch.get('reisename',''), ch.get('startdatum',''),
                    ch.get('enddatum',''), ch.get('yachtmodell',''), ch.get('yachtname',''),
                    ch.get('skipper_name',''), ch.get('skipper_alt',''), ch.get('neue_bewerbungen','')]
            for c, val in enumerate(vals):
                ws_cl.write(r, c, val, cf)

    # Vergangene Törns ohne Saison-Zuordnung (nur wenn vorhanden)
    if past_unassigned:
        write_trip_sheet('Ohne Saison', past_unassigned, sort_key='_end_iso', reverse=True)

    # Pro Saison (neueste zuerst): Skipper, dann vergangene Törns
    sk_cols   = ['Skipper', 'ID', 'Törns', 'Wochen']
    sk_widths = [24, 10, 8, 9]
    for label, season in sorted(seasons.items(), key=lambda kv: -kv[1]['key']):
        ws_sk = wb.add_worksheet(f'Skipper {label}')
        ws_sk.freeze_panes(1, 0)
        ws_sk.autofilter(0, 0, 0, len(sk_cols) - 1)
        for c, (col, w) in enumerate(zip(sk_cols, sk_widths)):
            ws_sk.set_column(c, c, w)
            ws_sk.write(0, c, col, hdr)
        ws_sk.set_row(0, 32)
        for r, (name, sid, toerns, wochen) in enumerate(season['skipper'], 1):
            def_fmt = row_a if r % 2 == 1 else row_b
            def_ctr = center_a if r % 2 == 1 else center_b
            ws_sk.write(r, 0, name,   def_fmt)
            ws_sk.write(r, 1, sid,    def_ctr)
            ws_sk.write(r, 2, toerns, def_ctr)
            ws_sk.write(r, 3, wochen, def_ctr)

        if season['past']:
            write_trip_sheet(f'Vergangen {label}', season['past'], sort_key='_end_iso', reverse=True)

    wb.close()
    buf.seek(0)
    return buf

# ── E-Mail Benachrichtigung ───────────────────────────────────────────────────
def iso_to_de(iso):
    return f"{iso[8:10]}.{iso[5:7]}.{iso[:4]}" if len(iso) == 10 else iso

def build_action_html(unassigned_trips, season_error):
    """Block 'Aktion erforderlich' – leer, wenn nichts zu tun ist."""
    if not unassigned_trips and not season_error:
        return ''
    td = "padding:6px 8px;border:1px solid #f0c9a8;font-family:Arial;font-size:12px"
    parts = []
    if season_error:
        parts.append(f'<p style="font-family:Arial;font-size:12px;margin:0 0 8px">{season_error}</p>')
    if unassigned_trips:
        rows = "".join(
            f"""<tr><td style="{td}">{t['trip_id']}</td><td style="{td}">{t['name']}</td>
                <td style="{td};white-space:nowrap">{iso_to_de(t['von'])} – {iso_to_de(t['bis'])}</td>
                <td style="{td};text-align:center">{t['termine']}</td></tr>"""
            for t in unassigned_trips)
        parts.append(f"""
          <p style="font-family:Arial;font-size:12px;margin:0 0 8px">
            {len(unassigned_trips)} Reise(n) ohne Saison-Zuordnung. Bitte in <b>{SEASON_FILE}</b> eintragen,
            z.B. <code>"{unassigned_trips[0]['trip_id']}": {{"name": "…", "saison": "Sommer"}}</code>
            (oder <code>"Winter"</code>).</p>
          <div style="overflow-x:auto">
          <table style="border-collapse:collapse;width:100%;font-family:Arial">
            <tr style="background:#f6dcc6">
              <th style="{td};text-align:left">Trip ID</th><th style="{td};text-align:left">Reise</th>
              <th style="{td};text-align:left">Zeitraum</th><th style="{td}">Termine</th>
            </tr>{rows}
          </table></div>""")
    return f"""
      <div style="background:#fff4ea;border:1px solid #f0a868;border-radius:8px;padding:12px;margin-bottom:16px">
        <div style="font-family:Arial;font-size:14px;font-weight:bold;color:#b45309;margin-bottom:8px">
          ⚠ Aktion erforderlich</div>
        {''.join(parts)}
      </div>"""

def send_notification(now_str, termine, yachten, changelog, sailing, action_html=''):
    if not RESEND_API_KEY:
        return

    recipients = [m.strip() for m in NOTIFY_EMAILS.split(',') if m.strip()] if NOTIFY_EMAILS else []
    if not recipients:
        return

    type_colors = {'Skipper geändert':'#FFF2CC','Neues Boot':'#E2EFDA',
                   'Storniert':'#FCE4D6','Neue Bewerbung(en)':'#D6E4F0'}
    th = "padding:8px;text-align:left;border:1px solid #ccc;font-family:Arial;font-size:12px"
    td = "padding:7px 8px;border:1px solid #ddd;font-family:Arial;font-size:12px"

    if changelog:
        cl_rows = "".join(f"""<tr style="background:{type_colors.get(ch.get('typ',''),'#fff')}">
            <td style="{td}">{ch.get('typ','')}</td>
            <td style="{td}">{ch.get('reisename','')}</td>
            <td style="{td};white-space:nowrap">{ch.get('startdatum','')}</td>
            <td style="{td}">{ch.get('yachtmodell','')}</td>
            <td style="{td}">{ch.get('skipper_name','') or '—'}</td>
            <td style="{td}">{ch.get('skipper_alt','')}</td>
            <td style="{td}">{ch.get('neue_bewerbungen','')}</td>
            </tr>""" for ch in changelog)
        changelog_html = f"""
        <h2 style="color:#1F4E79;font-family:Arial;font-size:14px;margin-top:20px">
            Changelog ({len(changelog)} Änderungen)</h2>
        <div style="overflow-x:auto;-webkit-overflow-scrolling:touch">
        <table style="border-collapse:collapse;width:100%;min-width:480px;font-family:Arial">
          <tr style="background:#1F4E79;color:#fff">
            <th style="{th}">Typ</th><th style="{th}">Reise</th><th style="{th}">Start</th>
            <th style="{th}">Yacht</th><th style="{th}">Skipper neu</th>
            <th style="{th}">Skipper alt</th><th style="{th}">Neue Bewerbungen</th>
          </tr>{cl_rows}
        </table></div>"""
    else:
        changelog_html = '<p style="font-family:Arial;color:#5a7399;font-size:13px">Keine Änderungen seit gestern.</p>'

    def stat(val, label):
        return f"""<div style="background:#fff;border:1px solid #d0dff0;border-radius:8px;
            padding:10px 6px;flex:1;text-align:center;min-width:55px">
            <div style="font-size:20px;font-weight:bold;color:#3b82f6;font-family:Arial">{val}</div>
            <div style="font-size:10px;color:#5a7399;font-family:Arial;text-transform:uppercase;margin-top:3px">{label}</div>
            </div>"""

    html = f"""<!DOCTYPE html><html><body style="margin:0;padding:12px;background:#e8edf5;font-family:Arial">
    <div style="max-width:600px;margin:0 auto;background:#f5f8ff;padding:16px;border-radius:12px">
      <div style="background:#1F4E79;border-radius:8px;padding:12px 16px;margin-bottom:14px">
        <span style="font-size:18px">⛵</span>
        <span style="font-size:15px;font-weight:bold;color:#fff;margin-left:8px">Skipperplan – Tagesupdate</span>
      </div>
      <p style="color:#5a7399;font-size:12px;margin-bottom:14px">
        Aktualisiert: <strong style="color:#1F4E79">{now_str}</strong></p>
      <div style="display:flex;gap:8px;margin-bottom:16px;flex-wrap:wrap">
        {stat(termine,'Termine')}{stat(yachten,'Yachten')}{stat(sailing,'Fahrend')}{stat(len(changelog),'Änderungen')}
      </div>
      {action_html}
      {changelog_html}
      <p style="font-size:11px;color:#aaa;margin-top:16px">Skipperplan · Flotilla Management</p>
    </div></body></html>"""

    try:
        requests.post(
            "https://api.resend.com/emails",
            headers={"Authorization": f"Bearer {RESEND_API_KEY}", "Content-Type": "application/json"},
            json={
                "from":    "Skipperplan <onboarding@resend.dev>",
                "to":      recipients,
                "subject": f"⛵ Skipperplan Update – {now_str}",
                "html":    html,
            },
            timeout=15,
        )
    except Exception as e:
        print(f"[mail error] {e}")


# ── Endpunkte ─────────────────────────────────────────────────────────────────
def password_ok():
    """Passwort kommt im Header 'X-App-Password' (nicht in der URL – die landet in den Logs).
    Ohne gesetztes APP_PASSWORD wird alles abgelehnt. Falsche Versuche werden 1 s verzögert."""
    given = request.headers.get("X-App-Password", "")
    if APP_PASSWORD and hmac.compare_digest(given.encode(), APP_PASSWORD.encode()):
        return True
    time.sleep(1)
    return False

def denied():
    return jsonify({"ok": False, "error": "Falsches Passwort"}), 401

def last_update_iso():
    """Zeitpunkt des letzten Commits von skipperplan.xlsx (UTC, ISO) oder None."""
    resp = requests.get(f"https://api.github.com/repos/{GITHUB_REPO}/commits",
                        params={"path": "skipperplan.xlsx", "per_page": 1},
                        headers=gh_headers(), timeout=15)
    if resp.ok and resp.json():
        return resp.json()[0]["commit"]["committer"]["date"]
    return None

@app.route("/check", methods=["POST"])
def check():
    """Login: prüft das Passwort und liefert den Stand der Excel."""
    if not password_ok():
        return denied()
    try:
        updated = last_update_iso()
    except Exception:
        updated = None
    return jsonify({"ok": True, "updated": updated})

@app.route("/download", methods=["GET"])
def download():
    """Liefert die aktuelle Excel aus dem privaten Repo."""
    if not password_ok():
        return denied()
    content, _ = gh_get_file("skipperplan.xlsx")
    if content is None:
        return jsonify({"ok": False, "error": "skipperplan.xlsx nicht gefunden"}), 404
    return Response(content,
                    mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    headers={"Content-Disposition": "attachment; filename=skipperplan.xlsx",
                             "Cache-Control": "no-store"})

def run_refresh(source):
    """Kompletter Ablauf, gemeinsam für /refresh und /refresh-async.
    source='daily': aktualisiert die Vergleichsbasis, skipperplan_daily.xlsx und schickt die Mail."""
    now_str  = datetime.datetime.now(datetime.UTC).strftime('%d.%m.%Y %H:%M UTC')
    today    = today_iso()
    api_key  = login()
    new_hits = fetch_algolia(api_key)

    # Laden – bricht ab, falls eine vorhandene Datei unlesbar ist
    history, hist_raw, hist_sha = load_json(HISTORY_FILE, HISTORY_BACKUP,
                                            {"daily_hits": [], "currently_sailing": []})
    past_trips, past_raw, past_sha = load_json(PAST_FILE, PAST_BACKUP, [])

    # Einmalige Migration: past_trips aus history.json nach past_trips.json
    old_past = history.pop('past_trips', None) or []
    if old_past:
        keys = {r['_yacht_key'] for r in past_trips}
        past_trips.extend(r for r in old_past if r['_yacht_key'] not in keys)

    prev_compact = history.get("daily_hits", [])
    changelog    = detect_changes(new_hits, prev_compact, today) if prev_compact else []
    history, past_trips = update_sailing_status(history, past_trips, new_hits, prev_compact, today)
    if source == "daily" or not prev_compact:
        history["daily_hits"] = compact_hits(new_hits)

    rows = extract_rows(new_hits)
    season_map, season_error = load_season_map()
    seasons, past_unassigned, unassigned_trips = build_season_data(
        rows, history['currently_sailing'], past_trips, season_map)
    excel_bytes = build_excel(rows, changelog, history['currently_sailing'],
                              seasons, past_unassigned).read()

    # Speichern: erst vergangene Törns, dann History. Fällt der zweite Schritt aus,
    # werden beim nächsten Lauf keine Törns doppelt angelegt (Abgleich über _yacht_key).
    save_json(PAST_FILE, PAST_BACKUP, past_trips, past_raw, past_sha,
              f"Vergangene Törns: {now_str}")
    save_json(HISTORY_FILE, HISTORY_BACKUP, history, hist_raw, hist_sha, f"History: {now_str}")
    gh_put_file("skipperplan.xlsx", excel_bytes, gh_get_sha("skipperplan.xlsx"),
                f"Skipperplan: {now_str}")
    if source == "daily":
        gh_put_file("skipperplan_daily.xlsx", excel_bytes, gh_get_sha("skipperplan_daily.xlsx"),
                    f"Täglich: {now_str}")
        send_notification(now_str, len(new_hits), len(rows), changelog,
                          len(history['currently_sailing']),
                          build_action_html(unassigned_trips, season_error))

    return {"ok": True, "updated": now_str,
            "termine": len(new_hits), "yachten": len(rows),
            "changes": len(changelog), "sailing": len(history['currently_sailing']),
            "past": len(past_trips),
            "saisons": sorted(seasons, key=lambda s: -seasons[s]['key']),
            "ohne_saison": [t['trip_id'] for t in unassigned_trips],
            "saison_fehler": season_error}


@app.route("/refresh", methods=["GET"])
def refresh():
    """Synchroner Endpunkt – Webseite (manual) und GitHub-Workflow (source=daily)."""
    if not password_ok():
        return denied()
    try:
        return jsonify(run_refresh(request.args.get("source", "manual")))
    except Exception as e:
        import traceback
        print(f"[refresh error] {e}\n{traceback.format_exc()}")
        return jsonify({"error": str(e), "trace": traceback.format_exc()[-800:]}), 500


@app.route("/refresh-async", methods=["GET"])
def refresh_async():
    """Asynchroner Endpunkt – antwortet sofort, arbeitet im Hintergrund (Standard: daily)."""
    if not password_ok():
        return denied()
    import threading
    source = request.args.get("source", "daily")

    def do_refresh():
        try:
            run_refresh(source)
        except Exception as e:
            import traceback
            print(f"[refresh-async error] {e}\n{traceback.format_exc()}")

    threading.Thread(target=do_refresh, daemon=True).start()
    return jsonify({"ok": True, "message": "Aktualisierung gestartet"}), 200

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
