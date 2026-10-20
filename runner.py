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


def loop():
    c.init_db()
    _m = sqlite3.connect(target)
    cols = [r[1] for r in _m.execute("pragma table_info(station_meta)")]
    if "is_shabbat_station" not in cols:
        _m.execute("alter table station_meta add column is_shabbat_station INTEGER DEFAULT 0")
        _m.commit()
    _m.execute("create table if not exists map_samples (ts DATETIME NOT NULL, station_id TEXT NOT NULL, station_name TEXT, status TEXT, bikes_total INTEGER, novatech INTEGER)")
    _m.execute("create index if not exists idx_map_ts on map_samples(ts, station_id)")
    _m.execute("""create table if not exists stations (
        station_id TEXT PRIMARY KEY, name TEXT, lat REAL, lng REAL, status TEXT)""")
    _m.commit()
    _m.close()
    last_detail = 0.0
    while True:
        t0 = time.time()
        try:
            ts = datetime.now(c.IL_TZ).strftime("%Y-%m-%d %H:%M:%S")
            stations = c.fetch_map()
            conn = sqlite3.connect(target)
            conn.executemany("insert into map_samples values (?,?,?,?,?,?)", [(ts, x["id"], x.get("name", ""), x.get("status", ""), x.get("omniBikes", 0), x.get("novatechBikes", 0)) for x in stations])
            conn.executemany("""insert into stations (station_id, name, lat, lng, status) values (?,?,?,?,?)
                on conflict(station_id) do update set name=excluded.name, lat=excluded.lat, lng=excluded.lng, status=excluded.status""",
                [(x["id"], x.get("name", ""), (x.get("location") or {}).get("lat"), (x.get("location") or {}).get("lng"), x.get("status", "")) for x in stations])
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
  #loading { padding: 20px; text-align: center; color: #666; }
</style>
</head>
<body>
<header>
  <h1>JeruFun ירושלים - מוניטור תחנות אופניים</h1>
  <span class="sub" id="updated"></span>
</header>
<div id="stats">
  <div><b id="stTotal">-</b>אופניים זמינים</div>
  <div><b id="stElec">-</b>חשמליים</div>
  <div><b id="stEmpty">-</b>תחנות ריקות</div>
  <div><b id="stActive">-</b>תחנות פעילות</div>
</div>
<div id="map"></div>
<div id="panel">
  <h2 id="chartTitle">כלל הרשת - אופניים זמינים לאורך זמן</h2>
  <div class="rangeBtns">
    <button data-h="24" class="on">24 שעות</button>
    <button data-h="72">3 ימים</button>
    <button data-h="168">שבוע</button>
  </div>
  <div id="chartWrap"><canvas id="chart"></canvas></div>
</div>
<div id="loading" style="display:none">טוען...</div>
<footer>
  נתונים נאספים כל 5 דקות מה-API הציבורי של JeruFun.
  מבוסס על הפרויקט הפתוח
  <a href="https://github.com/yuvalreisner/Jerufun-Monitor" target="_blank" rel="noopener">Jerufun-Monitor</a>
  מאת Yuval Reisner (יובל רייזנר). מפה: &copy; OpenStreetMap contributors.
</footer>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<script>
const TOKEN = new URLSearchParams(location.search).get('token') || '';
const qs = () => 'token=' + encodeURIComponent(TOKEN);
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

document.querySelectorAll('.rangeBtns button').forEach(btn => btn.onclick = () => {
  document.querySelectorAll('.rangeBtns button').forEach(b => b.classList.remove('on'));
  btn.classList.add('on');
  hours = +btn.dataset.h;
  loadChart();
});

map = L.map('map').setView([31.78, 35.21], 13);
L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {maxZoom: 19, attribution: '&copy; OpenStreetMap'}).addTo(map);
loadStations(); loadChart();
setInterval(loadStations, 120000);
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
                conn.close()
                body = json.dumps(dict(STATE, rows=n, samples=s, last_ts=last, interval=os.environ.get("INTERVAL_SECONDS", "600")), ensure_ascii=False).encode()
                ctype = "application/json"
            elif path == "/db":
                body = open(target, "rb").read()
                ctype = "application/octet-stream"
            elif path == "/api/stations":
                conn = sqlite3.connect(target)
                last_ts = conn.execute("select max(ts) from map_samples").fetchone()[0]
                rows = conn.execute("""select m.station_id, coalesce(s.name, m.station_name), s.lat, s.lng, m.status, m.bikes_total, m.novatech
                    from map_samples m left join stations s on s.station_id = m.station_id
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
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)


threading.Thread(target=loop, daemon=True).start()
HTTPServer(("0.0.0.0", int(os.environ.get("PORT", "8080"))), H).serve_forever()
