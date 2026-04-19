import os
import time
from threading import Lock

from flask import Flask, request, jsonify

app = Flask(__name__)

DEFAULT_TTL_SEC = int(os.getenv("CONFIG_TTL_SEC", "120"))

_lock = Lock()
_registry = {}


def _now():
    return time.time()


def _purge_expired(service_name):
    ttl = DEFAULT_TTL_SEC
    cutoff = _now() - ttl
    svc = _registry.get(service_name)
    if not svc:
        return
    expired = [url for (url, last_seen) in svc.items() if last_seen < cutoff]
    for url in expired:
        svc.pop(url, None)
    if not svc:
        _registry.pop(service_name, None)


@app.post("/register")
def register():
    body = request.get_json(force=True)
    service = body.get("service")
    url = body.get("url")
    if not service or not url:
        return jsonify({"error": "required fields: service, url"}), 400

    with _lock:
        _registry.setdefault(service, {})[url] = _now()
        _purge_expired(service)

    return jsonify({"ok": True, "service": service, "url": url, "ttl_sec": DEFAULT_TTL_SEC})


@app.get("/services/<service>")
def get_service(service):
    with _lock:
        _purge_expired(service)
        urls = sorted((_registry.get(service) or {}).keys())
    return jsonify({"service": service, "urls": urls})


@app.get("/services")
def list_services():
    with _lock:
        for s in list(_registry.keys()):
            _purge_expired(s)
        out = {s: sorted(urls.keys()) for (s, urls) in _registry.items()}
    return jsonify({"services": out, "ttl_sec": DEFAULT_TTL_SEC})


@app.get("/health")
def health():
    return jsonify({"ok": True})


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8010"))
    app.run(host="0.0.0.0", port=port)
