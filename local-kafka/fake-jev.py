#!/usr/bin/env python3
"""Deterministic local Jev substitute. It never logs request bodies."""

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != "/health":
            self.send_error(404)
            return
        self._json(200, {"status": "ok"})

    def do_POST(self):
        if self.path != "/v1/systemone":
            self.send_error(404)
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            request = json.loads(self.rfile.read(length))
            state = request["state"]
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            self._json(400, {"error": "invalid request"})
            return

        if "CONTROL_PERMANENT" in state:
            self._json(413, {"error": "permanent-secret-marker"})
        elif "CONTROL_TRANSIENT" in state:
            self._json(503, {"error": "transient-secret-marker"})
        else:
            self._json(
                200,
                {
                    "model": "jev-test-1",
                    "answers": {"department": "technical"},
                    "usage": {"input_tokens": 7, "output_tokens": 1},
                    "fake": {"deterministic": True},
                },
            )

    def _json(self, status, value):
        body = json.dumps(value, separators=(",", ":")).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, pattern, *args):
        # Method, path, and status are useful; Source Record-derived bodies are not.
        print("fake-jev " + pattern % args, flush=True)


ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
