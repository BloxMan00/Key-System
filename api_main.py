import os

from flask import Flask, jsonify, request

from db import get_db_health, init_db, is_key_valid

app = Flask(__name__)


@app.route("/")
def home():
    return "Key API is running"


@app.route("/check", methods=["GET"])
def check_key():
    key_value = request.args.get("key")
    if not key_value:
        return jsonify({"valid": False, "reason": "No key"}), 400

    try:
        row = is_key_valid(key_value)
        if row:
            return jsonify({"valid": True}), 200
        return jsonify({"valid": False, "reason": "Invalid or expired"}), 401
    except Exception as e:
        print(f"/check error: {repr(e)}")
        return jsonify({"valid": False, "reason": "Server error"}), 500


@app.route("/health")
def health():
    try:
        health_data = get_db_health()
        return jsonify({
            "ok": True,
            "database": health_data["database"],
            "time": health_data["time"],
        }), 200
    except Exception as e:
        print(f"/health error: {repr(e)}")
        return jsonify({"ok": False, "error": repr(e)}), 500


def main():
    try:
        health_data = get_db_health()
        print(f"Startup DB connection OK: {health_data}")
    except Exception as e:
        print(f"Startup database check failed: {repr(e)}")

    try:
        init_db()
        print("Database initialized at startup")
    except Exception as e:
        print(f"Startup database init failed: {repr(e)}")

    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)


if __name__ == "__main__":
    main()
