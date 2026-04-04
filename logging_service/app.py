import atexit
import os
import time
from flask import Flask, request, jsonify
from threading import Lock, Thread, Event
import hazelcast


app = Flask(__name__)

INSTANCE_ID = os.getenv("LOGGING_INSTANCE_ID", "unknown")
HZ_MEMBERS = [
    m.strip()
    for m in os.getenv("HZ_MEMBERS", "hazelcast1:5701,hazelcast2:5701,hazelcast3:5701").split(",")
    if m.strip()
]
HZ_CLUSTER_NAME = os.getenv("HZ_CLUSTER_NAME", "dev")
LOG_ALL_TX = os.getenv("LOG_ALL_TX", "0") == "1"

_hz_lock = Lock()
_hz_client = None
_tx_map = None
_by_user_multimap = None
_hz_ready = Event()
_hz_error = None

def _init_hazelcast():
    global _hz_client, _tx_map, _by_user_multimap
    with _hz_lock:
        if _hz_client is not None and _hz_ready.is_set():
            return


        _hz_client = hazelcast.HazelcastClient(
            cluster_members=HZ_MEMBERS,
            cluster_name=HZ_CLUSTER_NAME,
        )
        _tx_map = _hz_client.get_map("transactions").blocking()
        _by_user_multimap = _hz_client.get_multi_map("transactions_by_user").blocking()
        _hz_ready.set()

def _hz_init_loop():
    global _hz_error
    while not _hz_ready.is_set():
        try:
            _init_hazelcast()
        except Exception as e:
            _hz_error = e
            time.sleep(1)

Thread(target=_hz_init_loop, daemon=True).start()

def _ensure_hazelcast_ready():
    if _hz_ready.is_set():
        return True, None
    err = _hz_error
    return False, str(err) if err else "initializing"

def _shutdown_hazelcast():
    global _hz_client
    try:
        if _hz_client is not None:
            _hz_client.shutdown()
    except Exception:
        pass

atexit.register(_shutdown_hazelcast)

@app.post("/transactions")
def add_transaction():
    tx = request.get_json(force=True)
    ok, err = _ensure_hazelcast_ready()
    if not ok:
        return jsonify({"error": "hazelcast not ready", "details": err}), 503

    required = {"transaction_id", "timestamp", "user_id", "amount"}
    if not required.issubset(tx):
        return jsonify({"error": f"missing fields, required: {sorted(required)}"}), 400

    tx_id = tx["transaction_id"]
    user_id = tx["user_id"]

    prev = _tx_map.put_if_absent(tx_id, tx)
    if prev is not None:
        return jsonify({"error": "transaction_id already exists"}), 409

    _by_user_multimap.put(user_id, tx_id)

    if LOG_ALL_TX or str(tx_id).startswith("msg"):
        msg = tx.get("message")
        extra = f" msg={msg}" if msg is not None else ""
        print(f"[logging-service:{INSTANCE_ID}] stored tx={tx_id} user={user_id} amount={tx['amount']}{extra}")
    return jsonify({"ok": True})

@app.get("/transactions/user/<user_id>")
def get_user_transactions(user_id):
    ok, err = _ensure_hazelcast_ready()
    if not ok:
        return jsonify({"error": "hazelcast not ready", "details": err}), 503
    ids = _by_user_multimap.get(user_id) or []
    txs = []
    for tx_id in ids:
        tx = _tx_map.get(tx_id)
        if tx is not None:
            txs.append(tx)
    return jsonify({"transactions": txs})

@app.get("/transactions")
def get_all_transactions():
    ok, err = _ensure_hazelcast_ready()
    if not ok:
        return jsonify({"error": "hazelcast not ready", "details": err}), 503
    txs = list(_tx_map.values())
    return jsonify({"transactions": txs})

@app.get("/health")
def health():
    ok, err = _ensure_hazelcast_ready()
    if ok:
        return jsonify({"ok": True, "instance_id": INSTANCE_ID})
    return jsonify({"ok": False, "instance_id": INSTANCE_ID, "error": err}), 503

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8001)
