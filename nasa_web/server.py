"""Record real requests made by this local app to NASA's public home page."""

import os
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

LOG_DIR = Path("/logs")
NASA_URL = "https://www.nasa.gov/"
LOG_DIR.mkdir(exist_ok=True)
os.chmod(LOG_DIR, 0o777)


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/":
            self.respond(200, b'<h1>NASA live request generator</h1><p>Each click makes a real request to NASA and sends its result to the analytics pipeline.</p><a href="/fetch">Make a request to nasa.gov</a>')
        elif self.path == "/fetch":
            self.fetch_nasa()
        else:
            self.respond(404, b"Not found")

    def fetch_nasa(self):
        timestamp = datetime.now(timezone.utc).strftime("%d/%b/%Y:%H:%M:%S %z")
        try:
            with urlopen(Request(NASA_URL, headers={"User-Agent": "NASA-Analytics-Learning/1.0"}), timeout=15) as response:
                status = response.status
                size = len(response.read())
        except HTTPError as error:
            status, size = error.code, 0
        except (URLError, TimeoutError, OSError):
            status, size = 502, 0
        # Atomically rename only complete files so NiFi never reads partial records.
        record = f'{self.client_address[0]} - - [{timestamp}] "GET / HTTP/1.1" {status} {size}\n'
        name = uuid.uuid4().hex
        temporary = LOG_DIR / f".{name}.tmp"
        temporary.write_text(record, encoding="utf-8")
        temporary.rename(LOG_DIR / f"{name}.log")
        self.respond(200, f'<h1>NASA request complete</h1><p>NASA response status: {status}; bytes: {size}.</p><a href="/fetch">Request again</a> | <a href="http://localhost:8050">Open dashboard</a>'.encode())

    def respond(self, status, body):
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


ThreadingHTTPServer(("0.0.0.0", 8081), Handler).serve_forever()
