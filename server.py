"""
GLM HTTP Server (Flask)
========================
Exposes the Global Lock Manager as a REST API so branch nodes
running as separate processes can communicate over HTTP/REST.

Endpoints:
  POST /transaction/begin          → { tx_id }
  POST /lock/request               → { granted: bool }
  POST /transaction/commit         → { ok }
  POST /transaction/abort          → { ok }
  GET  /status                     → snapshot JSON
  GET  /log                        → lock request log
  GET  /accounts                   → account balances
"""
import os
import sys
import json
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from flask import Flask, request, jsonify, send_from_directory
from glm.lock_manager import GlobalLockManager, LockType
from branches.branch_node import load_accounts

app = Flask(__name__)
glm = GlobalLockManager()

DATA_PATH = os.path.join(os.path.dirname(__file__), "data", "accounts.csv")
accounts  = load_accounts(DATA_PATH)
accounts_by_id = {a["AccountID"]: a for a in accounts}
_lock = threading.Lock()


# ── Endpoints ──────────────────────────────────────────────────────────────

@app.route("/transaction/begin", methods=["POST"])
def begin():
    body      = request.get_json(force=True)
    branch_id = body.get("branch_id", "UNKNOWN")
    tx_id     = glm.begin_transaction(branch_id)
    return jsonify({"tx_id": tx_id})


@app.route("/lock/request", methods=["POST"])
def lock_request():
    body      = request.get_json(force=True)
    tx_id     = body["tx_id"]
    item_id   = body["item_id"]
    lock_type = LockType(body["lock_type"].upper())
    timeout   = float(body.get("timeout_s", 5.0))
    granted   = glm.request_lock(tx_id, item_id, lock_type, timeout)
    return jsonify({"granted": granted})


@app.route("/transaction/commit", methods=["POST"])
def commit():
    tx_id = request.get_json(force=True)["tx_id"]
    glm.release_all_locks(tx_id, commit=True)
    return jsonify({"ok": True})


@app.route("/transaction/abort", methods=["POST"])
def abort():
    tx_id = request.get_json(force=True)["tx_id"]
    glm.release_all_locks(tx_id, commit=False)
    return jsonify({"ok": True})


@app.route("/status", methods=["GET"])
def status():
    return jsonify(glm.snapshot())


@app.route("/log", methods=["GET"])
def log():
    return jsonify(glm.get_lock_queue_log())


@app.route("/accounts", methods=["GET"])
def list_accounts():
    branch = request.args.get("branch")
    result = accounts if not branch else [a for a in accounts if a["BranchID"] == branch]
    return jsonify(result[:100])   # cap at 100 for display


@app.route("/transfer", methods=["POST"])
def transfer():
    """
    High-level transfer: the GLM server handles the full 2PL transaction.
    Body: { src_id, dst_id, amount, branch_id }
    """
    body      = request.get_json(force=True)
    src_id    = body["src_id"]
    dst_id    = body["dst_id"]
    amount    = float(body["amount"])
    branch_id = body.get("branch_id", "UNKNOWN")

    tx_id = glm.begin_transaction(branch_id)

    # 2PL growing phase — acquire in sorted order to avoid deadlock
    items = sorted([src_id, dst_id])
    granted = all(glm.request_lock(tx_id, item, LockType.WRITE) for item in items)

    if not granted:
        glm.release_all_locks(tx_id, commit=False)
        return jsonify({"ok": False, "reason": "lock_timeout", "tx_id": tx_id}), 409

    with _lock:
        src = accounts_by_id.get(src_id)
        dst = accounts_by_id.get(dst_id)
        if not src or not dst:
            glm.release_all_locks(tx_id, commit=False)
            return jsonify({"ok": False, "reason": "account_not_found"}), 404

        if float(src["Balance"]) < amount:
            glm.release_all_locks(tx_id, commit=False)
            return jsonify({"ok": False, "reason": "insufficient_funds", "tx_id": tx_id}), 422

        src["Balance"] = str(round(float(src["Balance"]) - amount, 2))
        dst["Balance"] = str(round(float(dst["Balance"]) + amount, 2))

    # 2PL shrinking phase — release all
    glm.release_all_locks(tx_id, commit=True)
    return jsonify({
        "ok":     True,
        "tx_id":  tx_id,
        "src_balance": src["Balance"],
        "dst_balance": dst["Balance"],
    })


@app.route("/", methods=["GET"])
def index():
    dashboard_dir = os.path.join(os.path.dirname(__file__), "dashboard")
    return send_from_directory(dashboard_dir, "index.html")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    print(f"\n  GLM Server running on http://localhost:{port}")
    print(f"  Dataset: {len(accounts)} accounts loaded\n")
    app.run(host="0.0.0.0", port=port, threaded=True)
