from flask import Flask, request, jsonify
from threading import Lock

app = Flask(__name__)

#  balances
balances = {}
lock = Lock()

@app.post("/transactions")
def apply_transaction():
    tx = request.get_json(force=True)

    required = {"transaction_id", "timestamp", "user_id", "amount"}
    if not required.issubset(tx):
        return jsonify({"error": f"missing fields, required: {sorted(required)}"}), 400

    user_id = tx["user_id"]
    amount = int(tx["amount"])

    with lock:
        prev = balances.get(user_id, 0)
        new = prev + amount
        balances[user_id] = new

    print(f"[counter-service] user={user_id} {prev} -> {new} (amount={amount})")
    return jsonify({"balance": new})

@app.get("/balance/user/<user_id>")
def get_balance(user_id: str):
    with lock:
        bal = balances.get(user_id, 0)
    return jsonify({"user_id": user_id, "balance": bal})

@app.get("/balances")
def get_all_balances():
    with lock:
        out = dict(balances)
    return jsonify({"balances": out})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8002)