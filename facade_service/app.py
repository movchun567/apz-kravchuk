import os
import time
import uuid
import random
import json
from threading import Lock
from concurrent.futures import ThreadPoolExecutor

import requests
from flask import Flask, request, jsonify
from confluent_kafka import Producer

app = Flask(__name__)

LOGGING_URL = os.getenv("LOGGING_URL", "http://logging-service:8001")
LOGGING_URLS = [
    u.strip()
    for u in os.getenv("LOGGING_URLS", "").split(",")
    if u.strip()
]
if not LOGGING_URLS:
    LOGGING_URLS = [LOGGING_URL]
COUNTER_URL = os.getenv("COUNTER_URL", "http://counter-service:8002")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "counter-transactions")
SERVICE_NAME = os.getenv("SERVICE_NAME", "facade-service")
CONFIG_SERVER_URL = os.getenv("CONFIG_SERVER_URL", "http://config-server:8010")
SELF_URL = os.getenv("SELF_URL", "")
CONFIG_REGISTER_INTERVAL_SEC = int(os.getenv("CONFIG_REGISTER_INTERVAL_SEC", "30"))

metrics = {
    "logging_calls": 0,
    "counter_calls": 0,
    "logging_time_sec": 0.0,
    "counter_time_sec": 0.0,
    "kafka_calls": 0,
    "kafka_time_sec": 0.0,
}
m_lock = Lock()

pool = ThreadPoolExecutor(max_workers=16)

_producer_lock = Lock()
_producer = None
_svc_cache_lock = Lock()
_svc_cache = {}
_SVC_CACHE_TTL_SEC = 2.0

def _get_service_urls(service_name, fallback_urls):
    tnow = time.time()
    with _svc_cache_lock:
        cached = _svc_cache.get(service_name)
        if cached and (tnow - cached["ts"] < _SVC_CACHE_TTL_SEC):
            return list(cached["urls"])

    try:
        r = requests.get(f"{CONFIG_SERVER_URL}/services/{service_name}", timeout=2)
        data = r.json() if r.ok else {}
        urls = data.get("urls") or []
        if urls:
            with _svc_cache_lock:
                _svc_cache[service_name] = {"ts": tnow, "urls": list(urls)}
            return list(urls)
    except Exception:
        pass

    return list(fallback_urls)

def _register_loop():
    if not SELF_URL:
        return
    payload = {"service": SERVICE_NAME, "url": SELF_URL}
    while True:
        try:
            requests.post(f"{CONFIG_SERVER_URL}/register", json=payload, timeout=3)
            time.sleep(CONFIG_REGISTER_INTERVAL_SEC)
        except Exception:
            time.sleep(1)

pool.submit(_register_loop)

def _get_producer():
    global _producer
    if _producer is not None:
        return _producer
    with _producer_lock:
        if _producer is not None:
            return _producer
        _producer = Producer(
            {
                "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
                "client.id": "facade-service",
                "message.timeout.ms": 5000,
            }
        )
        return _producer

def timed_request(method, url, **kwargs):
    t0 = time.perf_counter()
    r = requests.request(method, url, timeout=10, **kwargs)
    dt = time.perf_counter() - t0
    return r, dt

def timed_request_failover(method, base_urls, path, **kwargs):
    if not base_urls:
        raise ValueError("base_urls is empty")

    start = random.randrange(len(base_urls))
    ordered = base_urls[start:] + base_urls[:start]

    last_exc = None
    total_dt = 0.0
    for base in ordered:
        try:
            r, dt = timed_request(method, f"{base}{path}", **kwargs)
            total_dt += dt
            return r, total_dt
        except requests.RequestException as e:
            last_exc = e
            continue

    raise last_exc or RuntimeError("all logging-service instances unreachable")

def timed_request_service_failover(method, service_name, fallback_urls, path, **kwargs):
    urls = _get_service_urls(service_name, fallback_urls)
    return timed_request_failover(method, urls, path, **kwargs)

def _enqueue_to_kafka(payload):
    producer = _get_producer()
    t0 = time.perf_counter()
    producer.produce(
        KAFKA_TOPIC,
        key=str(payload.get("user_id", "")).encode("utf-8"),
        value=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
    )
    producer.poll(0)
    return time.perf_counter() - t0

@app.post("/transaction")
def post_transaction():
    body = request.get_json(force=True)
    if "user_id" not in body or "amount" not in body:
        return jsonify({"error": "required fields: user_id, amount"}), 400

    tx_id = body.get("transaction_id") or str(uuid.uuid4())
    ts = time.time()

    payload = {
        "transaction_id": tx_id,
        "timestamp": ts,
        "user_id": body["user_id"],
        "amount": int(body["amount"]),
    }
    if "message" in body:
        payload["message"] = body["message"]

    f_log = pool.submit(timed_request_service_failover, "POST", "logging-service", LOGGING_URLS, "/transactions", json=payload)
    f_kafka = pool.submit(_enqueue_to_kafka, payload)

    try:
        log_resp, log_dt = f_log.result()
    except Exception as e:
        return jsonify({"error": "logging-service unreachable", "details": str(e)}), 502

    try:
        kafka_dt = f_kafka.result()
    except Exception as e:
        return jsonify({"error": "kafka enqueue failed", "details": str(e)}), 502

    if log_resp.status_code >= 400:
        return jsonify({"error": "logging-service error", "details": log_resp.text}), 502

    with m_lock:
        metrics["logging_calls"] += 1
        metrics["logging_time_sec"] += log_dt
        metrics["kafka_calls"] += 1
        metrics["kafka_time_sec"] += kafka_dt

    return jsonify({"transaction_id": tx_id, "queued": True})

@app.get("/user/<user_id>")
def get_user(user_id):
    f_bal = pool.submit(timed_request_service_failover, "GET", "counter-service", [COUNTER_URL], f"/balance/user/{user_id}")
    f_txs = pool.submit(timed_request_service_failover, "GET", "logging-service", LOGGING_URLS, f"/transactions/user/{user_id}")

    try:
        bal_resp, bal_dt = f_bal.result()
    except Exception as e:
        return jsonify({"error": "counter-service unreachable", "details": str(e)}), 502

    try:
        txs_resp, txs_dt = f_txs.result()
    except Exception as e:
        return jsonify({"error": "logging-service unreachable", "details": str(e)}), 502

    if bal_resp.status_code >= 400:
        return jsonify({"error": "counter-service error", "details": bal_resp.text}), 502
    if txs_resp.status_code >= 400:
        return jsonify({"error": "logging-service error", "details": txs_resp.text}), 502

    with m_lock:
        metrics["counter_calls"] += 1
        metrics["logging_calls"] += 1
        metrics["counter_time_sec"] += bal_dt
        metrics["logging_time_sec"] += txs_dt

    return jsonify({
        "balance": bal_resp.json().get("balance", 0),
        "transactions": txs_resp.json().get("transactions", []),
    })

@app.get("/accounts")
def get_accounts():
    try:
        resp, dt = timed_request_service_failover("GET", "counter-service", [COUNTER_URL], "/balances")
    except Exception as e:
        return jsonify({"error": "counter-service unreachable", "details": str(e)}), 502
    if resp.status_code >= 400:
        return jsonify({"error": "counter-service error", "details": resp.text}), 502

    with m_lock:
        metrics["counter_calls"] += 1
        metrics["counter_time_sec"] += dt

    return jsonify(resp.json())

@app.get("/metrics")
def get_metrics():
    with m_lock:
        out = dict(metrics)
    out["logging_avg_ms"] = (out["logging_time_sec"] / out["logging_calls"] * 1000) if out["logging_calls"] else 0.0
    out["counter_avg_ms"] = (out["counter_time_sec"] / out["counter_calls"] * 1000) if out["counter_calls"] else 0.0
    out["kafka_avg_ms"] = (out["kafka_time_sec"] / out["kafka_calls"] * 1000) if out["kafka_calls"] else 0.0
    return jsonify(out)

@app.post("/metrics/reset")
def reset_metrics():
    with m_lock:
        metrics["logging_calls"] = 0
        metrics["counter_calls"] = 0
        metrics["logging_time_sec"] = 0.0
        metrics["counter_time_sec"] = 0.0
        metrics["kafka_calls"] = 0
        metrics["kafka_time_sec"] = 0.0
    return jsonify({"ok": True})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
