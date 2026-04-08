"""
app.py  –  Skipperplan Backend
Läuft auf Render.com. Loggt sich ein, lädt alle Algolia-Daten
und gibt eine fertige Excel-Datei mit korrekter Farbgebung zurück.
"""
from flask import Flask, jsonify, send_file
from flask_cors import CORS
import requests, os, io, json, re, datetime
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

app = Flask(__name__)
CORS(app, origins=["https://wolkeyachting.github.io"])

JTC_BASE = "https://api-aws.join-the-crew.com"
JTC_HEADERS = {
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "de,en-US;q=0.9,en;q=0.8",
    "Origin":          "https://join-the-crew.com",
    "Referer":         "https://join-the-crew.com/",
    "X-Vendor":        "jtc",
}
ALGOLIA_HOST   = "https://m6n4q601zw-dsn.algolia.net"
ALGOLIA_APP_ID = "M6N4Q601ZW"
ALGOLIA_INDEX  = "prod_trip_dates_skipperplan_de"
HITS_PER_PAGE  = 100

JTC_USER     = os.environ.get("JTC_USER",     "")
JTC_PASSWORD = os.environ.get("JTC_PASSWORD", "")

# ── Stammskipper ──────────────────────────────────────────────────────────────
STAMMSKIPPER = {
    6429:'Julian S', 13707:'Miriam M', 2430:'Gregor K', 13035:'Nicole M',
    951:'Martin S',  14111:'Leon H',   17277:'David H',  28325:'Adam B',
    7022:'Mareike W',20147:'Yanik K',  6896:'Malte W',   9724:'Luke W',
    6758:'Marc L',   347:'Mario L',    9375:'Clemens P',
}

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

COL_HEADER      = '1F4E79'
COL_ROW_A       = 'D6E4F0'
COL_ROW_B       = 'FFFFFF'
COL_NO_SKIPPER  = 'B4F7B4'
COL_CANCEL      = 'FFCCCC'
COL_UNCONFIRMED = 'FCE4D6'
COL_STAMM       = 'C6EFCE'
COL_ADVANCED    = 'FFEB9C'

SKIPPER_STATUS_LABELS = {
    'confirmed_by_admin_and_skipper': 'Bestätigt',
    'confirmed_by_admin':             'Vom Admin bestätigt',
    'assigned':                       'Zugeteilt (offen)',
}

# ── Hilfsfunktionen ───────────────────────────────────────────────────────────
def ts_to_date(ts):
    if not ts: return ''
    try: return datetime.datetime.fromtimestamp(ts, datetime.UTC).strftime('%d.%m.%Y')
    except: return ''

def first(lst, default=''):
    return lst[0] if lst else default

def thin_border():
    s = Side(style='thin', color='BFBFBF')
    return Border(left=s, right=s, top=s, bottom=s)

def days_to_weeks(days):
    return round(days / 7)

def parse_days(s):
    m = re.search(r'(\d+)', str(s or ''))
    return int(m.group(1)) if m else 7

# ── Daten laden ───────────────────────────────────────────────────────────────
def login():
    resp = requests.post(f"{JTC_BASE}/de/api/users/login",
                         json={"user": JTC_USER, "password": JTC_PASSWORD},
                         headers=JTC_HEADERS, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    key  = (data.get("api_keys") or {}).get("algolia", {}).get("skipperplan")
    if not key:
        raise ValueError("Algolia-Key nicht gefunden")
    return key

def fetch_algolia(api_key):
    headers = {
        "x-algolia-application-id": ALGOLIA_APP_ID,
        "x-algolia-api-key":        api_key,
        "Content-Type":             "application/json",
    }
    all_hits, page, total = [], 0, None
    while True:
        body = {"requests": [{"indexName": ALGOLIA_INDEX,
                               "params": f"hitsPerPage={HITS_PER_PAGE}&page={page}&query="}]}
        resp = requests.post(f"{ALGOLIA_HOST}/1/indexes/*/queries",
                             headers=headers, json=body, timeout=30)
        resp.raise_for_status()
        result = resp.json()["results"][0]
        hits   = result.get("hits", [])
        all_hits.extend(hits)
        if total is None: total = result.get("nbHits", 0)
        if len(all_hits) >= total or len(hits) < HITS_PER_PAGE: break
        page += 1
    # Deduplizieren
    seen, unique = set(), []
    for h in all_hits:
        tid = h.get("trip_date_id") or h.get("objectID")
        if tid not in seen:
            seen.add(tid); unique.append(h)
    return unique

# ── Zeilen extrahieren ────────────────────────────────────────────────────────
def extract_rows(hits):
    rows = []
    for h in hits:
        dest      = h.get('trip_destination', {}) or {}
        proposals = [p['name'] for p in h.get('yacht_skipper_proposals', []) if p.get('name')]
        start_ts  = h.get('start_date_min') or first(h.get('start_date', []))
        end_ts    = first(h.get('end_date', []))
        trip_info = {
            'Startdatum':      ts_to_date(start_ts),
            'Enddatum':        ts_to_date(end_ts),
            'Reisename':       dest.get('name', ''),
            'Typ':             h.get('type', ''),
            'Tage':            first(h.get('trip_days', [])),
            'Altersgruppe':    first(h.get('age_range', [])),
            'Saison':          first(h.get('season', [])),
            'Preisspanne (€)': first(h.get('price_range', [])),
            'Land':            dest.get('country', ''),
            'Kontinent':       dest.get('continent', ''),
            'Anbieter':        h.get('vendor', ''),
            'Trip ID':         h.get('trip_id', ''),
            'Trip Date ID':    h.get('trip_date_id', ''),
        }
        for i, y in enumerate(h.get('yachts', [])):
            sk            = (y.get('skipper') or {})
            sk_status_raw = sk.get('status', '')
            sk_name       = sk.get('name', '')
            yacht_status  = y.get('status', '')
            if yacht_status == 'should_be_canceled': row_color = COL_CANCEL
            elif not sk_name:                         row_color = COL_NO_SKIPPER
            elif sk_status_raw == 'assigned':         row_color = COL_UNCONFIRMED
            else:                                     row_color = None
            row = dict(trip_info)
            row.update({
                'Skipper':         sk_name if sk_name else '⚠ Kein Skipper',
                'ID Skipper':      sk.get('id', ''),
                'Plätze gesamt':   y.get('places', ''),
                'Plätze belegt':   y.get('occupied_places', ''),
                'Yacht ID':        y.get('id', ''),
                'Yachtmodell':     y.get('accomodation_details_name', ''),
                'Yachtname':       y.get('yacht_name', ''),
                'Bootstyp':        y.get('accomodation_details_type', ''),
                'Baujahr':         y.get('yacht_year', ''),
                'Yacht Status':    'Zu stornieren' if yacht_status == 'should_be_canceled' else 'Bestätigt',
                'Skipper Status':  SKIPPER_STATUS_LABELS.get(sk_status_raw, sk_status_raw or '—'),
                'Flotillenführer': 'Ja' if sk.get('is_flotilla_leader') else 'Nein',
                'Bewerbungen':     ', '.join(proposals) if i == 0 else '',
                '_color':          row_color,
            })
            rows.append(row)
    return rows

def build_skipper_data(hits):
    from collections import defaultdict
    data = defaultdict(lambda: {'name':'','törns':0,'total_weeks':0,'summer_weeks':0})
    for h in hits:
        days   = parse_days(first(h.get('trip_days', [])))
        weeks  = days_to_weeks(days)
        season = (first(h.get('season', '')) or '').lower()
        for y in h.get('yachts', []):
            sk = y.get('skipper') or {}
            if not sk.get('id') or not sk.get('name'): continue
            d = data[sk['id']]
            d['name'] = sk['name']
            d['törns'] += 1
            d['total_weeks'] += weeks
            if season == 'summer': d['summer_weeks'] += weeks
    return sorted(data.items(), key=lambda x: -x[1]['törns'])

# ── Excel bauen ───────────────────────────────────────────────────────────────
def build_excel(rows, skipper_data):
    wb = Workbook()
    ws = wb.active
    ws.title = "Segelreisen"
    border = thin_border()
    fill_a = PatternFill('solid', start_color=COL_ROW_A)
    fill_b = PatternFill('solid', start_color=COL_ROW_B)

    # Kopfzeile
    ws.row_dimensions[1].height = 36
    for c, col in enumerate(COLUMN_ORDER, 1):
        cell = ws.cell(1, c, col)
        cell.font      = Font(name='Arial', bold=True, color='FFFFFF', size=11)
        cell.fill      = PatternFill('solid', start_color=COL_HEADER)
        cell.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
        cell.border    = border
        ws.column_dimensions[get_column_letter(c)].width = COL_WIDTHS.get(col, 14)

    # Datenzeilen
    for r, row in enumerate(rows, 2):
        override = row.get('_color')
        row_fill = PatternFill('solid', start_color=override) if override else (fill_a if r % 2 == 0 else fill_b)
        for c, col in enumerate(COLUMN_ORDER, 1):
            cell = ws.cell(r, c, row.get(col, ''))
            cell.font      = Font(name='Arial', size=10)
            cell.fill      = row_fill
            cell.alignment = Alignment(horizontal='left', vertical='center')
            cell.border    = border

    ws.freeze_panes = 'A2'
    ws.auto_filter.ref = f"A1:{get_column_letter(len(COLUMN_ORDER))}1"

    # Legende
    lc = len(COLUMN_ORDER) + 2
    ws.cell(1, lc, 'Legende').font = Font(name='Arial', bold=True, size=11)
    for i, (color, label) in enumerate([(COL_NO_SKIPPER,'⚠ Kein Skipper'),(COL_CANCEL,'🚫 Zu stornieren'),(COL_UNCONFIRMED,'⏳ Nicht bestätigt')], 2):
        cell = ws.cell(i, lc, label)
        cell.fill = PatternFill('solid', start_color=color)
        cell.font = Font(name='Arial', size=10)
        cell.border = border
    ws.column_dimensions[get_column_letter(lc)].width = 26

    # Skipper-Blatt
    ws2 = wb.create_sheet("Skipper")
    for c, (h, w) in enumerate(zip(['Skipper','ID','Törns','Wochen','Status'],[24,10,8,9,16]), 1):
        cell = ws2.cell(1, c, h)
        cell.font      = Font(name='Arial', bold=True, color='FFFFFF', size=11)
        cell.fill      = PatternFill('solid', start_color=COL_HEADER)
        cell.alignment = Alignment(horizontal='center', vertical='center')
        cell.border    = border
        ws2.column_dimensions[get_column_letter(c)].width = w

    status_colors = {'Stammskipper': COL_STAMM, 'Advanced': COL_ADVANCED, 'Hobby': COL_ROW_B}
    for r, (sid, d) in enumerate(skipper_data, 2):
        status = 'Stammskipper' if sid in STAMMSKIPPER else ('Advanced' if d['summer_weeks'] > 5 else 'Hobby')
        vals   = [d['name'], sid, d['törns'], d['total_weeks'], status]
        default_fill = PatternFill('solid', start_color=COL_ROW_A if r%2==0 else COL_ROW_B)
        for c, val in enumerate(vals, 1):
            cell = ws2.cell(r, c, val)
            cell.font   = Font(name='Arial', size=10)
            cell.fill   = PatternFill('solid', start_color=status_colors[status]) if c==5 else default_fill
            cell.alignment = Alignment(horizontal='center' if c in (2,3,4) else 'left', vertical='center')
            cell.border = border

    ws2.freeze_panes = 'A2'
    ws2.auto_filter.ref = 'A1:E1'

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf

# ── Endpunkte ─────────────────────────────────────────────────────────────────
@app.route("/download", methods=["GET"])
def download():
    """Alles in einem: Login → Algolia → Excel → Download"""
    try:
        api_key     = login()
        hits        = fetch_algolia(api_key)
        rows        = extract_rows(hits)
        skipper     = build_skipper_data(hits)
        buf         = build_excel(rows, skipper)
        date_str    = datetime.datetime.now().strftime('%Y-%m-%d')
        return send_file(buf,
                         mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                         as_attachment=True,
                         download_name=f'skipperplan_{date_str}.xlsx')
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
