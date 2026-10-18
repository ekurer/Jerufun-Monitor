import os, sys, time, random, threading, sqlite3, json, logging
from http.server import BaseHTTPRequestHandler, HTTPServer

DATA_DIR = os.environ.get("DATA_DIR", "/data")
os.makedirs(DATA_DIR, exist_ok=True)
link = os.path.join(os.path.dirname(os.path.abspath(__file__)), "jerufun.db")
target = os.path.join(DATA_DIR, "jerufun.db")
if not os.path.islink(link):
    if os.path.exists(link):
        os.remove(link)
    os.symlink(target, link)

import collector as c
from datetime import datetime

STATE = {"started": time.time(), "ok": 0, "errors": 0, "last_error": None, "last_ok": None}
TOKEN = os.environ.get("VIEW_TOKEN", "")


def loop():
    c.init_db()
    _m = sqlite3.connect(target)
    cols = [r[1] for r in _m.execute("pragma table_info(station_meta)")]
    if "is_shabbat_station" not in cols:
        _m.execute("alter table station_meta add column is_shabbat_station INTEGER DEFAULT 0")
        _m.commit()
    _m.close()
    _m = sqlite3.connect(target)
    _m.execute("create table if not exists map_samples (ts DATETIME NOT NULL, station_id TEXT NOT NULL, station_name TEXT, status TEXT, bikes_total INTEGER, novatech INTEGER)")
    _m.execute("create index if not exists idx_map_ts on map_samples(ts, station_id)")
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


class H(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_GET(self):
        path, _, q = self.path.partition("?")
        authed = (not TOKEN) or ("token=" + TOKEN in q)
        if path == "/health" and authed:
            conn = sqlite3.connect(target)
            n, s, last = conn.execute("select count(*), count(distinct ts), max(ts) from snapshots").fetchone()
            conn.close()
            body = json.dumps(dict(STATE, rows=n, samples=s, last_ts=last, interval=os.environ.get("INTERVAL_SECONDS", "600")), ensure_ascii=False).encode()
            ctype = "application/json"
        elif path == "/db" and authed:
            body = open(target, "rb").read()
            ctype = "application/octet-stream"
        else:
            body, ctype = b"ok", "text/plain"
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.end_headers()
        self.wfile.write(body)


threading.Thread(target=loop, daemon=True).start()
HTTPServer(("0.0.0.0", int(os.environ.get("PORT", "8080"))), H).serve_forever()
