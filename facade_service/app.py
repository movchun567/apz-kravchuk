import os
import time
import uuid
from threading import Lock
from concurrent.futures import ThreadPoolExecutor

import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

LOGGING_URL = os.getenv("LOGGING_URL", "http://logging-service:8001")
COUNTER_URL = os.getenv("COUNTER_URL", "http://counter-service:8002")

# metrics
metrics = {
    "logging_calls": 0,
    "counter_calls": 0,
    "logging_time_sec": 0.0,
    "counter_time_sec": 0.0,
}
m_lock = Lock()

# parallel computing to make it faster
pool = ThreadPoolExecutor(max_workers=16)

def timed_request(method: str, url: str, **kwargs):
    t0 = time.perf_counter()
    r = requests.request(method, url, timeout=10, **kwargs)
    dt = time.perf_counter() - t0
    return r, dt

@app.post("/transaction")
def post_transaction():
    body = request.get_json(force=True)
    if "user_id" not in body or "amount" not in body:
        return jsonify({"error": "required fields: user_id, amount"}), 400

    tx_id = str(uuid.uuid4())
    ts = time.time()

    payload = {
        "transaction_id": tx_id,
        "timestamp": ts,
        "user_id": body["user_id"],
        "amount": int(body["amount"]),
    }

    f_log = pool.submit(timed_request, "POST", f"{LOGGING_URL}/transactions", json=payload)
    f_cnt = pool.submit(timed_request, "POST", f"{COUNTER_URL}/transactions", json=payload)

    log_resp, log_dt = f_log.result()
    cnt_resp, cnt_dt = f_cnt.result()

    if log_resp.status_code >= 400:
        return jsonify({"error": "logging-service error", "details": log_resp.text}), 502
    if cnt_resp.status_code >= 400:
        return jsonify({"error": "counter-service error", "details": cnt_resp.text}), 502

    with m_lock:
        metrics["logging_calls"] += 1
        metrics["counter_calls"] += 1
        metrics["logging_time_sec"] += log_dt
        metrics["counter_time_sec"] += cnt_dt

    balance = cnt_resp.json().get("balance")
    return jsonify({"transaction_id": tx_id, "balance": balance})

@app.get("/user/<user_id>")
def get_user(user_id: str):
    f_bal = pool.submit(timed_request, "GET", f"{COUNTER_URL}/balance/user/{user_id}")
    f_txs = pool.submit(timed_request, "GET", f"{LOGGING_URL}/transactions/user/{user_id}")

    bal_resp, bal_dt = f_bal.result()
    txs_resp, txs_dt = f_txs.result()

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
    resp, dt = timed_request("GET", f"{COUNTER_URL}/balances")
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
    return jsonify(out)

@app.post("/metrics/reset")
def reset_metrics():
    with m_lock:
        metrics["logging_calls"] = 0
        metrics["counter_calls"] = 0
        metrics["logging_time_sec"] = 0.0
        metrics["counter_time_sec"] = 0.0
    return jsonify({"ok": True})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)