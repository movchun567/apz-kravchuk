import os
import time
from flask import Flask, request, jsonify
from threading import Lock
import psycopg2
from psycopg2.pool import ThreadedConnectionPool

app = Flask(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "")
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
