import os
import time
import json
from flask import Flask, request, jsonify
from threading import Lock
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
import threading
from confluent_kafka import Consumer
import requests

app = Flask(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "counter-transactions")
SERVICE_NAME = os.getenv("SERVICE_NAME", "counter-service")
CONFIG_SERVER_URL = os.getenv("CONFIG_SERVER_URL", "http://config-server:8010")
SELF_URL = os.getenv("SELF_URL", "")
CONFIG_REGISTER_INTERVAL_SEC = int(os.getenv("CONFIG_REGISTER_INTERVAL_SEC", "30"))
KAFKA_COMMIT_EVERY_N = int(os.getenv("KAFKA_COMMIT_EVERY_N", "200"))
KAFKA_COMMIT_EVERY_SEC = float(os.getenv("KAFKA_COMMIT_EVERY_SEC", "2.0"))
_pool_lock = Lock()
_pool = None

def _get_pool():
    global _pool
    if _pool is not None:
        return _pool

    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")



    with _pool_lock:
        if _pool is not None:
            return _pool

        last_exc = None
        for _ in range(60):
            try:
                conn = psycopg2.connect(DATABASE_URL)
                conn.autocommit = True
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS balances (
                            user_id TEXT PRIMARY KEY,
                            balance BIGINT NOT NULL DEFAULT 0
                        )
                        """
                    )
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS applied_transactions (
                            transaction_id TEXT PRIMARY KEY,
                            user_id TEXT NOT NULL,
                            amount BIGINT NOT NULL,
                            timestamp DOUBLE PRECISION
                        )
                        """
                    )
                conn.close()
                _pool = ThreadedConnectionPool(1, 16, dsn=DATABASE_URL)
                return _pool
            except Exception as e:
                last_exc = e
                time.sleep(1)

        raise RuntimeError("failed to connect/init Postgres") from last_exc

def _with_conn(fn):
    pool = _get_pool()
    conn = pool.getconn()
    try:
        return fn(conn)
    finally:
        pool.putconn(conn)

def _apply_tx_to_db(tx):
    user_id = tx.get("user_id")
    tx_id = tx.get("transaction_id")
    amount = int(tx.get("amount", 0))
    ts = tx.get("timestamp")

    if not user_id or not tx_id:
        return False

    def run(conn):
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO applied_transactions (transaction_id, user_id, amount, timestamp)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (transaction_id) DO NOTHING
                    """,
                    (tx_id, user_id, amount, ts),
                )
                if cur.rowcount == 0:
                    return False
                cur.execute(
                    """
                    INSERT INTO balances (user_id, balance)
                    VALUES (%s, %s)
                    ON CONFLICT (user_id)
                    DO UPDATE SET balance = balances.balance + EXCLUDED.balance
                    """,
                    (user_id, amount),
                )
                return True

    return _with_conn(run)

def _kafka_consume_loop():
    _get_pool()

    c = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "counter-service",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    c.subscribe([KAFKA_TOPIC])
    print(
        f"[counter-service] consuming from kafka topic={KAFKA_TOPIC} bootstrap={KAFKA_BOOTSTRAP_SERVERS}",
        flush=True,
    )

    processed_since_commit = 0
    last_commit_t = time.monotonic()

    try:
        while True:
            msg = c.poll(1.0)
            if msg is None:
                # periodic commit even when idle (best-effort)
                if processed_since_commit and (time.monotonic() - last_commit_t) >= KAFKA_COMMIT_EVERY_SEC:
                    try:
                        c.commit(asynchronous=True)
                    except Exception:
                        pass
                    processed_since_commit = 0
                    last_commit_t = time.monotonic()
                continue
            if msg.error():
                continue

            try:
                tx = json.loads(msg.value().decode("utf-8"))
                applied = _apply_tx_to_db(tx)
                processed_since_commit += 1
                now = time.monotonic()
                if processed_since_commit >= KAFKA_COMMIT_EVERY_N or (now - last_commit_t) >= KAFKA_COMMIT_EVERY_SEC:
                    c.commit(asynchronous=True)
                    processed_since_commit = 0
                    last_commit_t = now
                if applied and str(tx.get("transaction_id", "")).startswith("msg"):
                    print(
                        f"[counter-service] applied tx={tx.get('transaction_id')} user={tx.get('user_id')} amount={tx.get('amount')}",
                        flush=True,
                    )
            except Exception as e:
                print(f"[counter-service] error processing msg: {e}", flush=True)
                time.sleep(0.2)
    finally:
        try:
            try:
                c.commit(asynchronous=False)
            except Exception:
                pass
            c.close()
        except Exception:
            pass

def _start_consumer_once():
    t = threading.Thread(target=_kafka_consume_loop, daemon=True)
    t.start()

_start_consumer_once()

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

threading.Thread(target=_register_loop, daemon=True).start()

@app.post("/transactions")
def apply_transaction():
    tx = request.get_json(force=True)

    required = {"transaction_id", "timestamp", "user_id", "amount"}
    if not required.issubset(tx):
        return jsonify({"error": f"missing fields, required: {sorted(required)}"}), 400

    user_id = tx["user_id"]
    amount = int(tx["amount"])

    def run(conn):
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO balances (user_id, balance)
                    VALUES (%s, %s)
                    ON CONFLICT (user_id)
                    DO UPDATE SET balance = balances.balance + EXCLUDED.balance
                    RETURNING balance
                    """,
                    (user_id, amount),
                )
                (new_balance,) = cur.fetchone()
                return int(new_balance)

    new = _with_conn(run)

    print(f"[counter-service] user={user_id} +{amount} -> {new}")
    return jsonify({"balance": new})

@app.get("/balance/user/<user_id>")
def get_balance(user_id):
    def run(conn):
        with conn.cursor() as cur:
            cur.execute("SELECT balance FROM balances WHERE user_id = %s", (user_id,))
            row = cur.fetchone()
            return int(row[0]) if row else 0

    bal = _with_conn(run)
    return jsonify({"user_id": user_id, "balance": bal})

@app.get("/balances")
def get_all_balances():
    def run(conn):
        with conn.cursor() as cur:
            cur.execute("SELECT user_id, balance FROM balances")
            return {user_id: int(balance) for (user_id, balance) in cur.fetchall()}

    out = _with_conn(run)
    return jsonify({"balances": out})

@app.get("/health")
def health():
    try:
        _get_pool()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 503

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8002)
