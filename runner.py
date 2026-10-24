import os, sys, time, random, threading, sqlite3, json, logging
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs

DATA_DIR = os.environ.get("DATA_DIR", "/data")
os.makedirs(DATA_DIR, exist_ok=True)
link = os.path.join(os.path.dirname(os.path.abspath(__file__)), "jerufun.db")
target = os.path.join(DATA_DIR, "jerufun.db")
if not os.path.islink(link):
    if os.path.exists(link):
        os.remove(link)
    os.symlink(target, link)

import collector as c
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

STATE = {"started": time.time(), "ok": 0, "errors": 0, "last_error": None, "last_ok": None}
TOKEN = os.environ.get("VIEW_TOKEN", "")

_orig_get = c.SESSION.get
STATE["fail_log"] = []


def _get(url, **kw):
    kw["timeout"] = 30
    for attempt in (1, 2):
        t = time.time()
        try:
            return _orig_get(url, **kw)
        except Exception as e:
            msg = "%s try%d after %.1fs %s %s" % (time.strftime("%H:%M:%S"), attempt, time.time() - t, url.rsplit("/", 1)[-1][:12], type(e).__name__)
            STATE["fail_log"] = (STATE["fail_log"] + [msg])[-10:]
            c.log.warning("GET failed: %s", msg)
            if attempt == 2:
                raise
            time.sleep(3)


c.SESSION.get = _get



def migrate_delta(conn):
    """One-time migration: map_samples -> map_events (delta + daily anchor) + map_current. Transactional."""
    conn.execute("""create table if not exists map_events (
        ts TEXT NOT NULL, station_id TEXT NOT NULL, bikes_total INTEGER, novatech INTEGER, status TEXT, anchor INTEGER DEFAULT 0)""")
    conn.execute("create index if not exists idx_events_sid_ts on map_events(station_id, ts)")
    conn.execute("""create table if not exists map_current (
        station_id TEXT PRIMARY KEY, ts TEXT, bikes_total INTEGER, novatech INTEGER, status TEXT)""")
    conn.commit()
    has_samples = conn.execute("select count(*) from sqlite_master where type='table' and name='map_samples'").fetchone()[0]
    if not has_samples:
        return
    if conn.execute("select count(*) from map_events").fetchone()[0] > 0 and conn.execute("select count(*) from map_current").fetchone()[0] > 0:
        conn.execute("begin")
        conn.execute("drop table map_samples")
        conn.execute("drop index if exists idx_map_ts")
        conn.commit()
        conn.execute("vacuum")
        return
    c.log.info("migrating map_samples -> map_events ...")
    t0 = time.time()
    cur, anchor_day, anchor_ts, buf = {}, None, None, []
    for ts, sid, status, bikes, nova in conn.execute(
            "select ts, station_id, status, bikes_total, novatech from map_samples order by ts, station_id"):
        new = (bikes or 0, nova or 0, status or "")
        if ts[:10] != anchor_day:
            anchor_day, anchor_ts = ts[:10], ts
        if ts == anchor_ts or cur.get(sid) is None or cur[sid] != new:
            buf.append((ts, sid, new[0], new[1], new[2], 1 if ts == anchor_ts else 0))
            cur[sid] = new
    last_ts = conn.execute("select max(ts) from map_samples").fetchone()[0]
    conn.execute("begin")
    conn.executemany("insert into map_events values (?,?,?,?,?,?)", buf)
    conn.executemany("insert or replace into map_current values (?,?,?,?,?)",
        [(r[1], r[0], r[3] or 0, r[4] or 0, r[2] or "") for r in conn.execute(
            "select ts, station_id, status, bikes_total, novatech from map_samples where ts=?", (last_ts,))])
    conn.execute("drop table map_samples")
    conn.execute("drop index if exists idx_map_ts")
    conn.commit()
    conn.execute("vacuum")
    c.log.info("migration done: %d events in %.1fs", len(buf), time.time() - t0)


def loop():
    c.init_db()
    _m = sqlite3.connect(target)
    cols = [r[1] for r in _m.execute("pragma table_info(station_meta)")]
    if "is_shabbat_station" not in cols:
        _m.execute("alter table station_meta add column is_shabbat_station INTEGER DEFAULT 0")
        _m.commit()
    _m.execute("""create table if not exists stations (
        station_id TEXT PRIMARY KEY, name TEXT, lat REAL, lng REAL, status TEXT)""")
    _m.execute("create index if not exists idx_events_ts on map_events(ts)")
    _m.commit()
    migrate_delta(_m)
    _m.close()
    last_detail = 0.0
    while True:
        t0 = time.time()
        try:
            observed_at = datetime.now(c.IL_TZ)
            ts = observed_at.strftime("%Y-%m-%d %H:%M:%S")
            stations = c.fetch_map()
            conn = sqlite3.connect(target)
            cur = {r[0]: (r[1], r[2], r[3]) for r in conn.execute("select station_id, bikes_total, novatech, status from map_current")}
            anchor_needed = conn.execute("select count(*) from map_events where anchor=1 and substr(ts,1,10)=?", (ts[:10],)).fetchone()[0] == 0
            ev_rows, mc_rows = [], []
            for x in stations:
                new = (x.get("omniBikes", 0), x.get("novatechBikes", 0), x.get("status", ""))
                if anchor_needed or cur.get(x["id"]) != new:
                    ev_rows.append((ts, x["id"], new[0], new[1], new[2], 1 if anchor_needed else 0))
                mc_rows.append((x["id"], ts, new[0], new[1], new[2]))
            if ev_rows:
                for event in ev_rows:
                    inserted = conn.execute("insert into map_events values (?,?,?,?,?,?)", event)
                    conn.execute("insert into feed_event_times (event_rowid, epoch) values (?,?)",
                        (inserted.lastrowid, observed_at.timestamp()))
            conn.executemany("insert or replace into map_current values (?,?,?,?,?)", mc_rows)
            conn.executemany("""insert into stations (station_id, name, lat, lng, status) values (?,?,?,?,?)
                on conflict(station_id) do update set name=excluded.name, lat=excluded.lat, lng=excluded.lng, status=excluded.status""",
                [(x["id"], x.get("name", ""), (x.get("location") or {}).get("lat"), (x.get("location") or {}).get("lng"), x.get("status", "")) for x in stations])
            # Feed windows are at most 24h. Bound UTC metadata to 48h.
            conn.execute("delete from feed_event_times where epoch < ? or event_rowid not in (select rowid from map_events)",
                (observed_at.timestamp() - 48 * 3600,))
            conn.commit()
            conn.close()
            STATE["map_ok"] = STATE.get("map_ok", 0) + 1
            if t0 - last_detail >= int(os.environ.get("DETAIL_SECONDS", "600")) - 30:
                n = c.collect_once()
                last_detail = t0
                STATE["ok"] += 1
                STATE["stations"] = n
            STATE["last_ok"] = time.strftime("%Y-%m-%d %H:%M:%S")
        except Exception as e:
            STATE["errors"] += 1
            STATE["last_error"] = repr(e)[:300]
            c.log.error("collection failed: %s", e)
        iv = int(os.environ.get("INTERVAL_SECONDS", "600"))
        time.sleep(max(1, iv - (time.time() - t0)) + random.uniform(0, 5))


def compute_events(conn, names, ev_hours, now_dt):
    """Use actual UTC observation times and insertion-order station baselines."""
    now_epoch = now_dt.timestamp()
    start_epoch = now_epoch - ev_hours * 3600
    prev, events = {}, []
    rows = conn.execute("""select e.ts, e.station_id, e.bikes_total,
        e.novatech, t.epoch from map_events e
        left join feed_event_times t on t.event_rowid = e.rowid
        order by e.rowid""")
    for ts, sid, bikes, nova, epoch in rows:
        total = (bikes or 0) + (nova or 0)
        if epoch is None:
            # Do not guess the fold of an ambiguous historical wall time.
            naive = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
            first = naive.replace(tzinfo=ZoneInfo("Asia/Jerusalem"), fold=0)
            second = naive.replace(tzinfo=ZoneInfo("Asia/Jerusalem"), fold=1)
            if first.utcoffset() == second.utcoffset():
                epoch = first.timestamp()
        if epoch is not None and start_epoch <= epoch <= now_epoch and sid in prev:
            delta = total - prev[sid]
            if delta:
                events.append(dict(ts=ts, station=names.get(sid, sid), delta=delta, bikes=total))
        prev[sid] = total
    events.reverse()
    return events


DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="he" dir="rtl">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>JeruFun - מוניטור אופניים בירושלים</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
<style>
  body { font-family: -apple-system, "Segoe UI", Arial, sans-serif; margin: 0; background: #f5f6f8; color: #222; }
  header { background: #1a73e8; color: #fff; padding: 10px 16px; display: flex; align-items: baseline; gap: 12px; flex-wrap: wrap; }
  header h1 { font-size: 18px; margin: 0; }
  header .sub { font-size: 13px; opacity: .9; }
  #stats { display: flex; gap: 16px; padding: 10px 16px; background: #fff; border-bottom: 1px solid #e0e0e0; flex-wrap: wrap; font-size: 14px; }
  #stats b { font-size: 18px; display: block; }
  #map { height: 52vh; min-height: 320px; }
  #panel { background: #fff; padding: 12px 16px; }
  #panel h2 { font-size: 15px; margin: 0 0 8px; }
  #chartWrap { height: 260px; }
  .rangeBtns button { margin-left: 6px; padding: 3px 10px; border: 1px solid #ccc; background: #fff; border-radius: 4px; cursor: pointer; font-size: 13px; }
  .rangeBtns button.on { background: #1a73e8; color: #fff; border-color: #1a73e8; }
  footer { padding: 10px 16px; font-size: 12px; color: #666; }
  footer a { color: #1a73e8; }
  #feed { background: #fff; padding: 12px 16px; border-top: 1px solid #e0e0e0; }
  #feed h2 { font-size: 15px; margin: 0 0 4px; }
  #feed .note { font-size: 12px; color: #777; margin-bottom: 8px; }
  #feedRows { max-height: 300px; overflow-y: auto; }
  #feed .row { display: flex; gap: 10px; padding: 5px 0; border-bottom: 1px solid #f0f0f0; font-size: 14px; align-items: baseline; }
  #feed .row:last-child { border-bottom: none; }
  #feed .row .t { color: #666; min-width: 44px; font-variant-numeric: tabular-nums; }
  #feed .row .s { flex: 1; }
  #feed .row .d { font-weight: 600; min-width: 38px; text-align: left; direction: ltr; }
  #feed .d.plus { color: #2e7d32; }
  #feed .d.minus { color: #c62828; }
  #feed .empty { color: #666; font-size: 14px; padding: 8px 0; }
  #loading { padding: 20px; text-align: center; color: #666; }
</style>
</head>
<body>
<header>
  <h1>JeruFun ירושלים - מוניטור תחנות אופניים</h1>
  <span class="sub" id="updated"></span>
  <a id="reportLink" href="/report" style="color:#fff; font-size:13px">דוח עלות ויעילות</a>
</header>
<div id="stats">
  <div><b id="stTotal">-</b>אופניים זמינים</div>
  <div><b id="stElec">-</b>חשמליים</div>
  <div><b id="stEmpty">-</b>תחנות ריקות</div>
  <div><b id="stActive">-</b>תחנות פעילות</div>
</div>
<div id="map"></div>
<div id="feed">
  <h2>נסיעות בשעה האחרונה<span>*</span></h2>
  <div class="note">* משוערות לפי שינויי מלאי בתחנות, לא נסיעות מתועדות. ייתכן שמדובר בשינוי של המפעיל (פיזור מחדש של אופניים).</div>
  <div id="feedRows"></div>
</div>
<div id="panel">
  <h2 id="chartTitle">כלל הרשת - אופניים זמינים לאורך זמן</h2>
  <div class="rangeBtns">
    <button data-h="6">6 שעות</button>
    <button data-h="24" class="on">24 שעות</button>
    <button data-h="72">3 ימים</button>
    <button data-h="168">שבוע</button>
  </div>
  <div id="chartWrap"><canvas id="chart"></canvas></div>
</div>
<div id="loading" style="display:none">טוען...</div>
<footer>
  נתונים נאספים כל דקה מה-API הציבורי של JeruFun.
  מבוסס על הפרויקט הפתוח
  <a href="https://github.com/yuvalreisner/Jerufun-Monitor" target="_blank" rel="noopener">Jerufun-Monitor</a>
  מאת Yuval Reisner (יובל רייזנר). מפה: &copy; OpenStreetMap contributors.
</footer>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<script>
const TOKEN = new URLSearchParams(location.search).get('token') || '';
const qs = () => 'token=' + encodeURIComponent(TOKEN);
document.getElementById('reportLink').href = '/report?' + qs();
let map, markers = {}, selected = null, chart, hours = 24;

function color(b, status) {
  if (status === 'Disabled') return '#9e9e9e';
  if (b === 0) return '#e53935';
  if (b <= 2) return '#fdd835';
  return '#43a047';
}
const STATUS_HE = {Empty: 'תחנה ריקה', Full: 'תחנה מלאה', Disabled: 'תחנה מושבתת'};

async function loadStations() {
  const r = await fetch('/api/stations?' + qs());
  const data = await r.json();
  document.getElementById('updated').textContent = 'עודכן: ' + data.ts;
  document.getElementById('stTotal').textContent = data.totals.bikes;
  document.getElementById('stElec').textContent = data.totals.electric;
  document.getElementById('stEmpty').textContent = data.totals.empty;
  document.getElementById('stActive').textContent = data.totals.active;
  for (const s of data.stations) {
    if (s.lat == null) continue;
    const b = s.bikes;
    const html = '<b>' + s.name + '</b><br>אופניים: ' + b + (STATUS_HE[s.status] ? '<br>' + STATUS_HE[s.status] : '');
    if (markers[s.station_id]) {
      markers[s.station_id].setStyle({fillColor: color(b, s.status)});
      markers[s.station_id].setPopupContent(html);
    } else {
      const m = L.circleMarker([s.lat, s.lng], {radius: 7, color: '#fff', weight: 1, fillColor: color(b, s.status), fillOpacity: .95});
      m.bindPopup(html);
      m.on('click', () => { selected = s.station_id; document.getElementById('chartTitle').textContent = s.name + ' - אופניים זמינים לאורך זמן'; loadChart(); });
      m.addTo(map);
      markers[s.station_id] = m;
    }
  }
}

async function loadChart() {
  const sid = selected || 'all';
  const r = await fetch('/api/history?station=' + encodeURIComponent(sid) + '&hours=' + hours + '&' + qs());
  const data = await r.json();
  const labels = data.points.map(p => p.ts.slice(5, 16));
  const vals = data.points.map(p => p.bikes);
  if (chart) chart.destroy();
  chart = new Chart(document.getElementById('chart'), {
    type: 'line',
    data: { labels, datasets: [{ label: 'אופניים זמינים', data: vals, borderColor: '#1a73e8', backgroundColor: 'rgba(26,115,232,.15)', fill: true, pointRadius: 0, tension: .25 }] },
    options: { responsive: true, maintainAspectRatio: false, interaction: {mode: 'index', intersect: false},
      scales: { x: { ticks: { maxTicksLimit: 12 } }, y: { beginAtZero: true } },
      plugins: { legend: { display: false } } }
  });
}

async function loadFeed() {
  const r = await fetch('/api/events?hours=1&' + qs());
  const data = await r.json();
  const el = document.getElementById('feedRows');
  if (!data.events.length) {
    el.innerHTML = '<div class="empty">לא נצפו שינויים בשעה האחרונה</div>';
    return;
  }
  el.innerHTML = data.events.map(e => {
    const d = e.delta > 0 ? '+' + e.delta : '\u2212' + (-e.delta);
    const cls = e.delta > 0 ? 'plus' : 'minus';
    return '<div class="row"><span class="t">' + e.ts.slice(11, 16) + '</span><span class="s">' + e.station + '</span><span class="d ' + cls + '">' + d + '</span></div>';
  }).join('');
}

document.querySelectorAll('.rangeBtns button').forEach(btn => btn.onclick = () => {
  document.querySelectorAll('.rangeBtns button').forEach(b => b.classList.remove('on'));
  btn.classList.add('on');
  hours = +btn.dataset.h;
  loadChart();
});

map = L.map('map').setView([31.78, 35.21], 13);
L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {maxZoom: 19, attribution: '&copy; OpenStreetMap'}).addTo(map);
loadStations(); loadChart(); loadFeed();
setInterval(() => { loadStations(); loadFeed(); }, 120000);
</script>
</body>
</html>
"""


REPORT_HTML = """<!DOCTYPE html>
<html lang="he" dir="rtl">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>JeruFun - דוח עלות ויעילות</title>
<style>
  body { font-family: -apple-system, "Segoe UI", Arial, sans-serif; margin: 0; background: #f5f6f8; color: #222; }
  header { background: #1a73e8; color: #fff; padding: 10px 16px; display: flex; align-items: baseline; gap: 12px; flex-wrap: wrap; }
  header h1 { font-size: 18px; margin: 0; }
  header .sub { font-size: 13px; opacity: .9; }
  header a { color: #fff; font-size: 13px; }
  main { max-width: 640px; margin: 0 auto; }
  .card { background: #fff; margin: 12px; padding: 14px 16px; border-radius: 8px; box-shadow: 0 1px 2px rgba(0,0,0,.06); }
  .verdict { border-right: 4px solid #1a73e8; }
  .verdict .big { font-size: 17px; font-weight: 600; margin-bottom: 4px; }
  .verdict .why { font-size: 14px; color: #444; line-height: 1.5; }
  #stats { display: flex; flex-wrap: wrap; gap: 8px; margin: 12px; }
  #stats .stat { flex: 1 1 140px; background: #fff; border-radius: 8px; box-shadow: 0 1px 2px rgba(0,0,0,.06); padding: 10px 12px; font-size: 13px; color: #555; }
  #stats .stat b { display: block; font-size: 20px; color: #1a73e8; direction: ltr; text-align: right; }
  h2 { font-size: 15px; margin: 0 0 10px; }
  .rows .row { display: flex; gap: 10px; padding: 7px 0; border-bottom: 1px solid #f0f0f0; font-size: 14px; align-items: baseline; }
  .rows .row:last-child { border-bottom: none; }
  .rows .row .l { flex: 1; color: #444; }
  .rows .row .v { font-weight: 600; text-align: left; white-space: nowrap; }
  .bar { height: 10px; border-radius: 5px; background: #e8f0fe; overflow: hidden; margin: 6px 0 4px; }
  .bar .fill { height: 100%; background: #1a73e8; }
  .barNote { font-size: 12px; color: #777; margin-bottom: 10px; }
  ol { margin: 0; padding-right: 20px; font-size: 14px; line-height: 1.6; color: #333; }
  ol li { margin-bottom: 8px; }
  footer { padding: 10px 16px; font-size: 12px; color: #666; max-width: 640px; margin: 0 auto; }
  footer a { color: #1a73e8; }
</style>
</head>
<body>
<header>
  <h1>דוח עלות ויעילות - דגימה כל דקה</h1>
  <span class="sub">נמדד ב-24 באוקטובר 2026</span>
  <a id="mapLink" href="/dashboard">למפה החיה</a>
</header>
<main>
<div class="card verdict">
  <div class="big">שורה תחתונה: נשארים על דגימה כל דקה.</div>
  <div class="why">הדגימה עצמה כמעט לא עולה כסף - רוב העלות היא הזיכרון של השירות, לא כמות הקריאות. ודווקא הדגימה התכופה היא מה שנותן את דיוק הדקה שהפרויקט הוקם בשבילו.</div>
</div>
<div id="stats">
  <div class="stat"><b>~$1.10</b>עלות השירות לחודש</div>
  <div class="stat"><b>~$0.0004</b>עלות הדגימה לחודש</div>
  <div class="stat"><b>20%</b>מהאירועים היו הולכים לאיבוד בדגימה כל 5 דקות</div>
  <div class="stat"><b>0</b>שגיאות באיסוף, 123/123 תחנות</div>
</div>
<div class="card">
  <h2>כמה זה עולה</h2>
  <div class="bar"><div class="fill" style="width:90%"></div></div>
  <div class="barNote">כ-90% מהעלות היא זיכרון (השירות תופס כ-100MB כל הזמן), לא הדגימה.</div>
  <div class="rows">
    <div class="row"><span class="l">עלות השירות עד היום (6 ימים)</span><span class="v">$0.21</span></div>
    <div class="row"><span class="l">עלות ליום / לחודש</span><span class="v">~$0.037 / ~$1.10</span></div>
    <div class="row"><span class="l">ההפרש בין דגימה כל דקה לכל 5 דקות</span><span class="v">שבריר סנט בחודש</span></div>
    <div class="row"><span class="l">אחסון</span><span class="v">זניח (0.3MB)</span></div>
    <div class="row"><span class="l">חשבון Railway הכולל לתקופה (צפי)</span><span class="v">$9.75</span></div>
  </div>
  <div class="barNote">הצפי כולל $5 דמי מנוי; $3.22 מתוכם צבור שירות ישן שעצרנו ב-22.10.</div>
</div>
<div class="card">
  <h2>למה דווקא כל דקה</h2>
  <div class="rows">
    <div class="row"><span class="l">דגימה כל 5 דקות הייתה מפספסת</span><span class="v">20% מהאירועים</span></div>
    <div class="row"><span class="l">בשעות השיא (בוקר, צהריים, ערב מאוחר)</span><span class="v">36-40%</span></div>
    <div class="row"><span class="l">נסיעות קצרות של עד 5 דקות (השאלה+החזרה)</span><span class="v">כ-15% מהאירועים</span></div>
    <div class="row"><span class="l">יציבות האיסוף</span><span class="v">בלי פערים, בלי חסימות</span></div>
  </div>
  <div class="barNote">נסיעות קצרות לא נראות בכלל בדגימה גסה. נמדד על 1,375 אירועי שינוי ב-4 ימים.</div>
</div>
<div class="card">
  <h2>מה הלאה</h2>
  <ol>
    <li>מעבר ל-Oracle (חינם) - מחכה להשלמת הרישום, ומוריד את עלות הזיכרון שהיא כמעט כל החשבון.</li>
    <li>טבלת הפירוט גדלה כ-3MB ליום (בערך סנט לחודש) - אפשר לכווץ אותה באותה שיטת דלתא כשתרצה.</li>
    <li>הפיד של נסיעות השעה האחרונה רץ על אותם נתונים בלי עלות נוספת (אחסון תחום ~16KB), ומיפוי השעון ל-UTC אומת בפרודקשן לקראת מעבר שעון החורף.</li>
  </ol>
</div>
</main>
<footer>
  המספרים נמדדו ב-Railway ב-24 באוקטובר 2026, 17:30.
  מבוסס על הפרויקט הפתוח
  <a href="https://github.com/yuvalreisner/Jerufun-Monitor" target="_blank" rel="noopener">Jerufun-Monitor</a>
  מאת Yuval Reisner (יובל רייזנר).
</footer>
<script>
var t = new URLSearchParams(location.search).get('token') || '';
if (t) document.getElementById('mapLink').href = '/dashboard?token=' + encodeURIComponent(t);
</script>
</body>
</html>
"""


class H(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_GET(self):
        u = urlparse(self.path)
        path = u.path
        q = parse_qs(u.query)
        authed = (not TOKEN) or (q.get("token", [""])[0] == TOKEN)
        body, ctype = b"ok", "text/plain"
        if authed:
            if path == "/health":
                conn = sqlite3.connect(target)
                n, s, last = conn.execute("select count(*), count(distinct ts), max(ts) from snapshots").fetchone()
                ev = conn.execute("select count(*) from map_events").fetchone()[0]
                conn.close()
                body = json.dumps(dict(STATE, rows=n, samples=s, last_ts=last, events=ev, interval=os.environ.get("INTERVAL_SECONDS", "600")), ensure_ascii=False).encode()
                ctype = "application/json"
            elif path == "/db":
                body = open(target, "rb").read()
                ctype = "application/octet-stream"
            elif path == "/api/stations":
                conn = sqlite3.connect(target)
                last_ts = conn.execute("select max(ts) from map_current").fetchone()[0]
                rows = conn.execute("""select m.station_id, coalesce(s.name, ''), s.lat, s.lng, m.status, m.bikes_total, m.novatech
                    from map_current m left join stations s on s.station_id = m.station_id
                    where m.ts = ?""", (last_ts,)).fetchall()
                stations = [dict(station_id=r[0], name=r[1], lat=r[2], lng=r[3], status=r[4], bikes=(r[5] or 0) + (r[6] or 0)) for r in rows]
                live = [s for s in stations if s["status"] != "Disabled"]
                snap_ts = conn.execute("select max(ts) from snapshots").fetchone()[0]
                elec = conn.execute("select coalesce(sum(bikes_electric),0) from snapshots where ts = ?", (snap_ts,)).fetchone()[0]
                totals = dict(
                    bikes=sum(s["bikes"] for s in live),
                    electric=elec,
                    empty=sum(1 for s in live if s["bikes"] == 0),
                    active=len(live),
                )
                conn.close()
                body = json.dumps(dict(ts=last_ts, stations=stations, totals=totals), ensure_ascii=False).encode()
                ctype = "application/json"
            elif path == "/api/events":
                try:
                    ev_hours = max(1, min(24, int(q.get("hours", ["1"])[0])))
                except ValueError:
                    ev_hours = 1
                conn = sqlite3.connect(target)
                names = dict(conn.execute("select station_id, name from stations").fetchall())
                events = compute_events(conn, names, ev_hours, datetime.now(c.IL_TZ))
                conn.close()
                body = json.dumps(dict(events=events), ensure_ascii=False).encode()
                ctype = "application/json"
            elif path == "/api/history":
                sid = q.get("station", ["all"])[0]
                try:
                    hours = max(1, min(720, int(q.get("hours", ["24"])[0])))
                except ValueError:
                    hours = 24
                cutoff = (datetime.now(c.IL_TZ) - timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
                conn = sqlite3.connect(target)
                if sid == "all":
                    rows = conn.execute("""select ts, sum(bikes_regular + bikes_electric)
                        from snapshots where ts >= ?
                        group by ts order by ts""", (cutoff,)).fetchall()
                else:
                    rows = conn.execute("""select ts, bikes_regular + bikes_electric
                        from snapshots where station_id = ? and ts >= ?
                        order by ts""", (sid, cutoff)).fetchall()
                conn.close()
                body = json.dumps(dict(points=[dict(ts=r[0], bikes=r[1] or 0) for r in rows]), ensure_ascii=False).encode()
                ctype = "application/json"
            elif path in ("/", "/dashboard"):
                body = DASHBOARD_HTML.encode()
                ctype = "text/html; charset=utf-8"
            elif path == "/report":
                body = REPORT_HTML.encode()
                ctype = "text/html; charset=utf-8"
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)


# Feed-only metadata for new events. Existing history is not rewritten.
with sqlite3.connect(target) as feed_conn:
    feed_conn.execute("""create table if not exists feed_event_times (
        event_rowid INTEGER PRIMARY KEY, epoch REAL NOT NULL)""")

threading.Thread(target=loop, daemon=True).start()
HTTPServer(("0.0.0.0", int(os.environ.get("PORT", "8080"))), H).serve_forever()
