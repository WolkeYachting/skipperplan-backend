"""
app.py  –  Skipperplan Backend (xlsxwriter)
"""
from flask import Flask, jsonify, request
from flask_cors import CORS
import requests, os, io, re, datetime, json, base64
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
NOTIFY_EMAIL   = os.environ.get("NOTIFY_EMAIL",   "")

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

def gh_get_file(path):
    resp = requests.get(f"https://api.github.com/repos/{GITHUB_REPO}/contents/{path}",
                        headers=gh_headers(), timeout=15)
    if resp.status_code == 404:
        return None, None
    resp.raise_for_status()
    data = resp.json()
    return base64.b64decode(data['content']), data['sha']

def gh_put_file(path, content_bytes, sha, message):
    payload = {"message": message, "content": base64.b64encode(content_bytes).decode()}
    if sha:
        payload["sha"] = sha
    resp = requests.put(f"https://api.github.com/repos/{GITHUB_REPO}/contents/{path}",
                        headers=gh_headers(), json=payload, timeout=30)
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"GitHub Fehler {resp.status_code}: {resp.text[:200]}")

def compact_hits(hits):
    """Reduziert Hits auf die für den Changelog relevanten Felder."""
    compact = []
    for h in hits:
        dest      = h.get('trip_destination', {}) or {}
        start_ts  = h.get('start_date_min') or first(h.get('start_date', []))
        end_ts    = first(h.get('end_date', []))
        proposals = [p['name'] for p in h.get('yacht_skipper_proposals', []) if p.get('name')]
        for y in h.get('yachts', []):
            sk = y.get('skipper') or {}
            compact.append({
                'k':  f"{h.get('trip_date_id')}_{y.get('id')}",  # yacht_key
                'tn': h.get('trip_date_id'),
                'yi': y.get('id'),
                'rn': dest.get('name', ''),
                'sd': ts_to_date(start_ts),
                'si': ts_to_iso(start_ts),
                'ed': ts_to_date(end_ts),
                'ei': ts_to_iso(end_ts),
                'ym': y.get('accomodation_details_name', ''),
                'yn': y.get('yacht_name', ''),
                'ys': y.get('status', ''),
                'sn': sk.get('name', ''),
                'si2': sk.get('id', ''),
                'pr': proposals,
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


    content, _ = gh_get_file("history.json")
    if content is None:
        return {"daily_hits": [], "currently_sailing": [], "past_trips": []}
    return json.loads(content.decode('utf-8'))

def save_history(history, message="History aktualisiert"):
    _, sha = gh_get_file("history.json")
    gh_put_file("history.json", json.dumps(history, ensure_ascii=False).encode('utf-8'), sha, message)

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

def build_skipper_data(hits):
    from collections import defaultdict
    data = defaultdict(lambda: {'name':'','törns':0,'total_weeks':0,'summer_weeks':0})
    for h in hits:
        weeks  = days_to_weeks(parse_days(first(h.get('trip_days', []))))
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
            if pd['skipper_name'] != nd['skipper_name']:
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

    return changes

# ── Fahrstatus ────────────────────────────────────────────────────────────────
def update_sailing_status(history, new_hits, prev_compact, today):
    new_idx = {f"{h.get('trip_date_id')}_{y.get('id')}": True
               for h in new_hits for y in h.get('yachts', [])}
    existing_keys = {r['_yacht_key'] for r in history.get('currently_sailing', [])}

    # Kompakte Einträge auf fahrende Boote prüfen
    for c in prev_compact:
        key = c['k']
        if key not in new_idx and key not in existing_keys:
            start_iso = c.get('si', '')
            end_iso   = c.get('ei', '')
            if start_iso and start_iso <= today and end_iso >= today:
                history['currently_sailing'].append({
                    '_yacht_key':    key,
                    '_start_iso':    start_iso,
                    '_end_iso':      end_iso,
                    'Startdatum':    c.get('sd', ''),
                    'Enddatum':      c.get('ed', ''),
                    'Reisename':     c.get('rn', ''),
                    'Yachtmodell':   c.get('ym', ''),
                    'Yachtname':     c.get('yn', ''),
                    'Skipper':       c.get('sn', '') or '—',
                    'ID Skipper':    str(c.get('si2', '')),
                    'Yacht ID':      str(c.get('yi', '')),
                    'Trip Date ID':  str(c.get('tn', '')),
                    'Yacht Status':  'Bestätigt',
                    'Tage': '', 'Altersgruppe': '', 'Saison': '',
                    'Preisspanne (€)': '', 'Land': '', 'Kontinent': '',
                    'Anbieter': '', 'Trip ID': '', 'Typ': '',
                    'Plätze gesamt': '', 'Plätze belegt': '',
                    'Bootstyp': '', 'Baujahr': '', 'Skipper Status': '',
                    'Flotillenführer': '', 'Bewerbungen': '',
                })

    cutoff = (datetime.datetime.now(datetime.UTC) - datetime.timedelta(weeks=4)).strftime('%Y-%m-%d')
    still, ended = [], []
    for row in history.get('currently_sailing', []):
        (ended if row.get('_end_iso','') < today else still).append(row)
    history['currently_sailing'] = still

    past_keys = {r['_yacht_key'] for r in history.get('past_trips', [])}
    for row in ended:
        if row['_yacht_key'] not in past_keys:
            history['past_trips'].append(row)
    history['past_trips'] = [r for r in history.get('past_trips', [])
                              if r.get('_end_iso','') >= cutoff]
    return history

# ── Excel bauen (xlsxwriter) ──────────────────────────────────────────────────
def build_excel(rows, skipper_data, changelog=None, currently_sailing=None, past_trips=None):
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
    stamm_f  = fmt('C6EFCE')
    adv_f    = fmt('FFEB9C')
    hobby_f  = fmt('FFFFFF')
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

    # Vergangene Törns
    write_trip_sheet('Vergangene Törns', past_trips or [], sort_key='_end_iso', reverse=True)

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

    # Skipper
    ws_sk = wb.add_worksheet('Skipper')
    sk_cols   = ['Skipper','ID','Törns','Wochen','Status']
    sk_widths = [24,10,8,9,16]
    ws_sk.freeze_panes(1, 0)
    ws_sk.autofilter(0, 0, 0, 4)
    for c, (col, w) in enumerate(zip(sk_cols, sk_widths)):
        ws_sk.set_column(c, c, w)
        ws_sk.write(0, c, col, hdr)
    ws_sk.set_row(0, 32)
    status_fmt = {'Stammskipper': stamm_f, 'Advanced': adv_f, 'Hobby': hobby_f}
    for r, (sid, d) in enumerate(skipper_data, 1):
        status  = 'Stammskipper' if sid in STAMMSKIPPER else ('Advanced' if d['summer_weeks'] > 5 else 'Hobby')
        def_fmt = row_a if r%2==1 else row_b
        def_ctr = center_a if r%2==1 else center_b
        sf      = status_fmt[status]
        ws_sk.write(r, 0, d['name'],        def_fmt)
        ws_sk.write(r, 1, sid,              def_ctr)
        ws_sk.write(r, 2, d['törns'],       def_ctr)
        ws_sk.write(r, 3, d['total_weeks'], def_ctr)
        ws_sk.write(r, 4, status,           sf)

    wb.close()
    buf.seek(0)
    return buf

# ── E-Mail Benachrichtigung ───────────────────────────────────────────────────
def send_notification(now_str, termine, yachten, changelog, sailing):
    if not RESEND_API_KEY or not NOTIFY_EMAIL:
        return
    th = "padding:8px 12px;text-align:left;border:1px solid #ccc;font-family:Arial"
    td = "padding:7px 12px;border:1px solid #ddd;font-family:Arial;font-size:13px"
    type_colors = {'Skipper geändert':'#FFF2CC','Neues Boot':'#E2EFDA',
                   'Storniert':'#FCE4D6','Neue Bewerbung(en)':'#D6E4F0'}
    if changelog:
        cl_rows = "".join(f"""<tr style="background:{type_colors.get(ch.get('typ',''),'#fff')}">
            <td style="{td}">{ch.get('typ','')}</td><td style="{td}">{ch.get('reisename','')}</td>
            <td style="{td}">{ch.get('startdatum','')}</td><td style="{td}">{ch.get('yachtmodell','')}</td>
            <td style="{td}">{ch.get('skipper_name','')}</td><td style="{td}">{ch.get('skipper_alt','')}</td>
            <td style="{td}">{ch.get('neue_bewerbungen','')}</td></tr>""" for ch in changelog)
        changelog_html = f"""<h2 style="color:#1F4E79;font-family:Arial;margin-top:24px">
            Changelog ({len(changelog)} Änderungen)</h2>
            <table style="border-collapse:collapse;width:100%;font-family:Arial;font-size:13px">
            <tr style="background:#1F4E79;color:#fff">
              <th style="{th}">Typ</th><th style="{th}">Reisename</th><th style="{th}">Startdatum</th>
              <th style="{th}">Yacht</th><th style="{th}">Skipper (neu)</th>
              <th style="{th}">Skipper (alt)</th><th style="{th}">Neue Bewerbungen</th>
            </tr>{cl_rows}</table>"""
    else:
        changelog_html = '<p style="font-family:Arial;color:#5a7399">Keine Änderungen seit gestern.</p>'

    stat = lambda val, label: f"""<div style="background:#fff;border:1px solid #d0dff0;border-radius:8px;
        padding:14px 20px;flex:1;text-align:center">
        <div style="font-size:28px;font-weight:bold;color:#3b82f6;font-family:Arial">{val}</div>
        <div style="font-size:12px;color:#5a7399;font-family:Arial;text-transform:uppercase">{label}</div></div>"""

    html = f"""<div style="max-width:700px;margin:0 auto;background:#f5f8ff;padding:24px;border-radius:12px">
      <div style="background:#1F4E79;border-radius:8px;padding:16px 20px;margin-bottom:20px">
        <span style="font-size:22px">⛵</span>
        <span style="font-family:Arial;font-size:18px;font-weight:bold;color:#fff;margin-left:10px">Skipperplan – Tagesupdate</span>
      </div>
      <p style="font-family:Arial;color:#5a7399;margin-bottom:16px">Aktualisiert: <strong style="color:#1F4E79">{now_str}</strong></p>
      <div style="display:flex;gap:12px;margin-bottom:20px">
        {stat(termine,'Termine')}{stat(yachten,'Yachten')}{stat(sailing,'Fahrend')}{stat(len(changelog),'Änderungen')}
      </div>
      {changelog_html}
      <p style="font-family:Arial;font-size:12px;color:#aaa;margin-top:24px">Skipperplan · Flotilla Management</p>
    </div>"""
    try:
        requests.post("https://api.resend.com/emails",
            headers={"Authorization": f"Bearer {RESEND_API_KEY}", "Content-Type": "application/json"},
            json={"from": "Skipperplan <onboarding@resend.dev>", "to": [NOTIFY_EMAIL],
                  "subject": f"⛵ Skipperplan Update – {now_str}", "html": html},
            timeout=15)
    except Exception:
        pass


# ── Endpunkte ─────────────────────────────────────────────────────────────────
def check_password():
    return not APP_PASSWORD or request.args.get("password","") == APP_PASSWORD

@app.route("/check", methods=["GET"])
def check():
    return (jsonify({"ok": True}) if check_password() else (jsonify({"ok": False}), 401))

@app.route("/refresh", methods=["GET"])
def refresh():
    source  = request.args.get("source", "manual")
    try:
        now_str  = datetime.datetime.now(datetime.UTC).strftime('%d.%m.%Y %H:%M UTC')
        today    = today_iso()
        api_key  = login()
        new_hits = fetch_algolia(api_key)
        history  = load_history()
        prev_compact = history.get("daily_hits", [])
        changelog = detect_changes(new_hits, prev_compact, today) if prev_compact else []
        history   = update_sailing_status(history, new_hits, prev_compact, today)
        if source == "daily" or not prev_compact:
            history["daily_hits"] = compact_hits(new_hits)
        rows         = extract_rows(new_hits)
        skipper_data = build_skipper_data(new_hits)
        buf          = build_excel(rows, skipper_data, changelog,
                                   history['currently_sailing'], history['past_trips'])
        excel_bytes  = buf.read()
        save_history(history, f"History: {now_str}")
        _, sha_main = gh_get_file("skipperplan.xlsx")
        gh_put_file("skipperplan.xlsx", excel_bytes, sha_main, f"Skipperplan: {now_str}")
        if source == "daily":
            _, sha_daily = gh_get_file("skipperplan_daily.xlsx")
            gh_put_file("skipperplan_daily.xlsx", excel_bytes, sha_daily, f"Täglich: {now_str}")
            send_notification(now_str, len(new_hits), len(rows), changelog,
                              len(history['currently_sailing']))
        return jsonify({"ok": True, "updated": now_str,
                        "termine": len(new_hits), "yachten": len(rows),
                        "changes": len(changelog), "sailing": len(history['currently_sailing'])})
    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "trace": traceback.format_exc()[-800:]}), 500

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
