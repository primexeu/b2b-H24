#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import subprocess
import sys
import threading
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from flask import (
    Flask,
    flash,
    redirect,
    render_template,
    request,
    session,
    url_for,
)
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import Flow
from google.oauth2.credentials import Credentials

load_dotenv()

# Allow local http callbacks unless explicitly disabled.
if not os.getenv("PUBLIC_URL") and os.getenv("ALLOW_INSECURE_OAUTH", "1") == "1":
    os.environ.setdefault("OAUTHLIB_INSECURE_TRANSPORT", "1")

app = Flask(__name__, static_folder="static", template_folder="templates")
app.secret_key = os.getenv("FLASK_SECRET_KEY", os.getenv("SESSION_SECRET", "dev-secret-key"))

SCOPES = [
    "https://www.googleapis.com/auth/gmail.modify",
    "https://www.googleapis.com/auth/gmail.send",
]

SECRET_PATH = Path(os.getenv("GMAIL_CLIENT_SECRET_PATH", "secrets/client_secret.json")).resolve()
TOKEN_PATH = Path(os.getenv("GMAIL_TOKEN_PATH", SECRET_PATH.parent / "token.json")).resolve()
BASE_DIR = Path(__file__).parent.resolve()
WATCHER_SCRIPT = BASE_DIR / "gmail_watch_and_process.py"

worker_lock = threading.Lock()
worker_process: subprocess.Popen | None = None


def watcher_running() -> bool:
    global worker_process
    if worker_process is not None and worker_process.poll() is None:
        return True
    if worker_process is not None and worker_process.poll() is not None:
        worker_process = None
    return False


def start_watcher() -> bool:
    """
    Launch gmail_watch_and_process.py if it exists and is not already running.
    Returns True when a new process was spawned.
    """
    if not WATCHER_SCRIPT.exists():
        return False

    if not TOKEN_PATH.exists():
        return False

    with worker_lock:
        if watcher_running():
            return False

        env = os.environ.copy()
        try:
            process = subprocess.Popen(
                [sys.executable, str(WATCHER_SCRIPT)],
                cwd=str(BASE_DIR),
                env=env,
            )
        except Exception:
            return False

        global worker_process
        worker_process = process
        return True


def _token_payload() -> dict | None:
    if not TOKEN_PATH.exists():
        return None

    try:
        payload = json.loads(TOKEN_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None

    info = {
        "path": str(TOKEN_PATH),
        "refresh_token": bool(payload.get("refresh_token")),
        "expiry": payload.get("expiry"),
        "updated_at": datetime.fromtimestamp(TOKEN_PATH.stat().st_mtime).isoformat(timespec="seconds"),
        "email": None,
    }

    try:
        creds = Credentials.from_authorized_user_info(payload, scopes=SCOPES)
        service = build("gmail", "v1", credentials=creds, cache_discovery=False)
        profile = service.users().getProfile(userId="me").execute()
        info["email"] = profile.get("emailAddress")
    except Exception:
        pass

    return info


def _build_redirect_uri() -> str:
    """
    Build the OAuth redirect based on environment settings.
    Defaults to whatever scheme the current request used so local
    testing keeps http:// callbacks without extra configuration.
    """
    public_base = os.getenv("PUBLIC_URL")
    if public_base:
        public_base = public_base.rstrip("/")
        return f"{public_base}{url_for('oauth_callback', _external=False)}"

    preferred_scheme = os.getenv("PREFERRED_URL_SCHEME")
    if not preferred_scheme:
        forwarded = request.headers.get("X-Forwarded-Proto")
        if forwarded:
            preferred_scheme = forwarded.split(",")[0].strip()
        else:
            preferred_scheme = request.scheme or "http"

    return url_for("oauth_callback", _external=True, _scheme=preferred_scheme)


@app.route("/")
def dashboard():
    token = _token_payload()
    watcher_state = "Running" if watcher_running() else ("Ready to auto-start" if token else "Pending auth")

    fun_cards = [
        {
            "label": "Watcher Status",
            "value": watcher_state,
            "hint": "Kicks off automatically after OAuth completes successfully.",
        },
        {
            "label": "Token freshness",
            "value": token["updated_at"] if token else "—",
            "hint": "Refresh tokens rarely expire, but you can re-run auth any time.",
        },
        {
            "label": "Scope granted",
            "value": ", ".join(["modify", "send"]),
            "hint": "Limited to sending mail and triaging inbox labels.",
        },
    ]
    return render_template("dashboard.html", token=token, cards=fun_cards, secret_path=str(SECRET_PATH))


@app.route("/auth")
def start_auth():
    if not SECRET_PATH.exists():
        flash(f"Client secret was not found at {SECRET_PATH}. Upload it before authenticating.", "error")
        return redirect(url_for("dashboard"))

    redirect_uri = _build_redirect_uri()
    flow = Flow.from_client_secrets_file(str(SECRET_PATH), scopes=SCOPES, redirect_uri=redirect_uri)
    authorization_url, state = flow.authorization_url(
        access_type="offline",
        include_granted_scopes="true",
        prompt="consent",
    )
    session["state"] = state
    session["redirect_uri"] = redirect_uri
    return redirect(authorization_url)


@app.route("/oauth2callback")
def oauth_callback():
    state = session.get("state")
    redirect_uri = session.get("redirect_uri")
    if not state or not redirect_uri:
        flash("Missing OAuth session state. Please start again.", "error")
        return redirect(url_for("dashboard"))

    flow = Flow.from_client_secrets_file(
        str(SECRET_PATH),
        scopes=SCOPES,
        state=state,
        redirect_uri=redirect_uri,
    )

    try:
        flow.fetch_token(authorization_response=request.url)
    except Exception as exc:
        flash(f"Auth failed: {exc}", "error")
        return redirect(url_for("dashboard"))

    creds = flow.credentials
    TOKEN_PATH.parent.mkdir(parents=True, exist_ok=True)
    TOKEN_PATH.write_text(creds.to_json(), encoding="utf-8")

    try:
        service = build("gmail", "v1", credentials=creds, cache_discovery=False)
        profile = service.users().getProfile(userId="me").execute()
        email = profile.get("emailAddress")
        flash(f"Connected to Gmail as {email}. Token saved.", "success")
    except Exception:
        flash("Token saved, but Gmail profile lookup failed. The watcher can still use this token.", "warning")

    started = start_watcher()
    if started:
        flash("Watcher started automatically with the fresh token.", "success")
    elif not WATCHER_SCRIPT.exists():
        flash("Token saved, but gmail_watch_and_process.py was not found so the watcher was not started.", "warning")
    elif watcher_running():
        flash("Watcher is already running; keeping the existing process alive.", "info")
    else:
        flash("Token ready, but watcher did not start. Check server logs for details.", "warning")

    session.pop("state", None)
    session.pop("redirect_uri", None)
    return redirect(url_for("dashboard"))


@app.route("/healthz")
def healthcheck():
    return {"status": "ok", "token_ready": TOKEN_PATH.exists()}


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))
