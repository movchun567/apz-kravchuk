from flask import Flask, request, jsonify
from threading import Lock

app = Flask(__name__)

store = {}
by_user = {}
lock = Lock()

@app.post("/transactions")
def add_transaction():
    tx = request.get_json(force=True)

    required = {"transaction_id", "timestamp", "user_id", "amount"}
    if not required.issubset(tx):
        return jsonify({"error": f"missing fields, required: {sorted(required)}"}), 400

    tx_id = tx["transaction_id"]
    user_id = tx["user_id"]

    with lock:
        if tx_id in store:
            return jsonify({"error": "transaction_id already exists"}), 409

        store[tx_id] = tx
        by_user.setdefault(user_id, []).append(tx_id)

    print(f"[logging-service] stored tx={tx_id} user={user_id} amount={tx['amount']}")
    return jsonify({"ok": True})

@app.get("/transactions/user/<user_id>")
def get_user_transactions(user_id: str):
    with lock:
        ids = by_user.get(user_id, [])
        txs = [store[i] for i in ids]
    return jsonify({"transactions": txs})

@app.get("/transactions")
def get_all_transactions():
    with lock:
        txs = list(store.values())
    return jsonify({"transactions": txs})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8001)