"""
app.py  –  Skipperplan Backend
Läuft auf Render.com. Übernimmt den Login bei join-the-crew.com
und gibt den Algolia-Key an die GitHub-Page weiter.
"""
from flask import Flask, jsonify, request
from flask_cors import CORS
import requests
import os

app = Flask(__name__)
CORS(app)  # Erlaubt Anfragen von der GitHub-Page

JTC_BASE = "https://api-aws.join-the-crew.com"
JTC_HEADERS = {
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "de,en-US;q=0.9,en;q=0.8",
    "Origin":          "https://join-the-crew.com",
    "Referer":         "https://join-the-crew.com/",
    "X-Vendor":        "jtc",
}

# Zugangsdaten aus Umgebungsvariablen (werden in Render.com gesetzt)
JTC_USER     = os.environ.get("JTC_USER",     "")
JTC_PASSWORD = os.environ.get("JTC_PASSWORD", "")


@app.route("/algolia-key", methods=["GET"])
def get_algolia_key():
    """Loggt sich ein und gibt den Algolia-Key zurück."""
    try:
        resp = requests.post(
            f"{JTC_BASE}/de/api/users/login",
            json={"user": JTC_USER, "password": JTC_PASSWORD},
            headers=JTC_HEADERS,
            timeout=15,
        )
        if resp.status_code != 200:
            return jsonify({"error": f"Login fehlgeschlagen: HTTP {resp.status_code}"}), 502

        data = resp.json()
        key  = (data.get("api_keys") or {}).get("algolia", {}).get("skipperplan")

        if not key:
            return jsonify({"error": "Algolia-Key nicht in Login-Antwort gefunden"}), 502

        return jsonify({"key": key})

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
