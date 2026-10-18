import os, sys, time, threading, sqlite3, json, logging
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

STATE = {"started": time.time(), "ok": 0, "errors": 0, "last_error": None, "last_ok": None}
TOKEN = os.environ.get("VIEW_TOKEN", "")


def loop():
    c.init_db()
    while True:
        t0 = time.time()
        try:
            n = c.collect_once()
            STATE["ok"] += 1
            STATE["last_ok"] = time.strftime("%Y-%m-%d %H:%M:%S")
            STATE["stations"] = n
        except Exception as e:
            STATE["errors"] += 1
            STATE["last_error"] = repr(e)[:300]
            c.log.error("collection failed: %s", e)
        iv = int(os.environ.get("INTERVAL_SECONDS", "600"))
        time.sleep(max(1, iv - (time.time() - t0)))


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
