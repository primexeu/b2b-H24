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
SETTINGS_PATH = Path(os.getenv("APP_SETTINGS_PATH", SECRET_PATH.parent / "app_settings.json")).resolve()
WATCHER_STATE_PATH = Path(os.getenv("WATCHER_STATE_PATH", BASE_DIR / "watcher_state.json")).resolve()
DEFAULT_CLIENT_NOTIFICATION_TO = os.getenv("CLIENT_NOTIFICATION_TO", "primexpresentation2025@gmail.com")
SESSION_MAX_SECONDS = int(os.getenv("WATCHER_MAX_RUNTIME_SECONDS", str(3 * 60 * 60)))

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


def _load_app_settings() -> dict:
    data: dict[str, str] = {}
    if SETTINGS_PATH.exists():
        try:
            data = json.loads(SETTINGS_PATH.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            data = {}
    if not data.get("client_notification_to"):
        data["client_notification_to"] = DEFAULT_CLIENT_NOTIFICATION_TO
    return data


def _save_app_settings(payload: dict) -> None:
    SETTINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = SETTINGS_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(SETTINGS_PATH)


def _load_watcher_state() -> dict:
    if WATCHER_STATE_PATH.exists():
        try:
            return json.loads(WATCHER_STATE_PATH.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {}
    return {}


def _format_timestamp(ts: str | None) -> str | None:
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(ts)
    except ValueError:
        return ts
    return dt.strftime("%Y-%m-%d %H:%M:%S UTC")


def _humanize_duration(seconds: int | None) -> str | None:
    if seconds is None:
        return None
    seconds = max(int(seconds), 0)
    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    parts: list[str] = []
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    if not parts:
        parts.append(f"{secs}s")
    return " ".join(parts)


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
    settings = _load_app_settings()
    watcher_state = _load_watcher_state()
    watcher_process_active = watcher_running()

    state_status = (watcher_state.get("status") or "").capitalize()
    if watcher_process_active:
        watcher_status = "Running"
    elif state_status:
        watcher_status = state_status
    else:
        watcher_status = "Ready" if token else "Pending auth"

    time_remaining = None
    will_stop_at = watcher_state.get("will_stop_at")
    if will_stop_at:
        try:
            seconds_left = int((datetime.fromisoformat(will_stop_at) - datetime.utcnow()).total_seconds())
            if seconds_left > 0:
                time_remaining = _humanize_duration(seconds_left)
        except ValueError:
            time_remaining = None

    watcher_card_value = watcher_status
    if watcher_process_active and time_remaining:
        watcher_card_value = f"{watcher_status} ({time_remaining} left)"
    elif watcher_state.get("stopped_at") and not watcher_process_active:
        stopped_text = _format_timestamp(watcher_state.get("stopped_at"))
        if stopped_text:
            watcher_card_value = f"{watcher_status} @ {stopped_text}"

    last_event = watcher_state.get("last_event")
    if last_event:
        last_event = dict(last_event)
        last_event["timestamp_display"] = _format_timestamp(last_event.get("timestamp"))

    watcher_context = {
        "status": watcher_status,
        "process_active": watcher_process_active,
        "time_remaining": time_remaining,
        "started_at": _format_timestamp(watcher_state.get("started_at")),
        "will_stop_at": _format_timestamp(watcher_state.get("will_stop_at")),
        "stopped_at": _format_timestamp(watcher_state.get("stopped_at")),
        "last_heartbeat": _format_timestamp(watcher_state.get("last_heartbeat")),
        "last_event": last_event,
        "pending_messages": watcher_state.get("pending_messages"),
        "client_notification_to": watcher_state.get("client_notification_to") or settings["client_notification_to"],
        "max_runtime": _humanize_duration(SESSION_MAX_SECONDS),
    }

    fun_cards = [
        {
            "label": "Watcher Status",
            "value": watcher_card_value,
            "hint": "Sessions run up to three hours; restart whenever you need a fresh poll.",
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
        {
            "label": "Client notifications",
            "value": settings["client_notification_to"],
            "hint": "Client-facing replies are emailed to this destination.",
        },
    ]
    return render_template(
        "dashboard.html",
        token=token,
        cards=fun_cards,
        secret_path=str(SECRET_PATH),
        settings=settings,
        watcher=watcher_context,
        session_seconds=SESSION_MAX_SECONDS,
    )


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


@app.post("/settings/client-email")
def update_client_email():
    client_email = (request.form.get("client_notification_to") or "").strip()
    settings = _load_app_settings()
    if not client_email:
        flash("Please provide an email address for client notifications.", "error")
    elif "@" not in client_email:
        flash("That doesn't look like a valid email address.", "error")
    else:
        settings["client_notification_to"] = client_email
        _save_app_settings(settings)
        flash(f"Client notification email updated to {client_email}.", "success")
    return redirect(url_for("dashboard"))


@app.post("/watcher/start")
def trigger_watcher():
    if not TOKEN_PATH.exists():
        flash("Connect Gmail before starting the watcher.", "error")
        return redirect(url_for("dashboard"))
    if not WATCHER_SCRIPT.exists():
        flash("Watcher script is missing from the deployment image.", "error")
        return redirect(url_for("dashboard"))

    started = start_watcher()
    if started:
        flash("Watcher session started. It will run for up to three hours.", "success")
    else:
        if watcher_running():
            flash("Watcher is already running.", "info")
        else:
            flash("Watcher did not start. Check server logs for details.", "error")
    return redirect(url_for("dashboard"))


@app.route("/healthz")
def healthcheck():
    return {"status": "ok", "token_ready": TOKEN_PATH.exists()}


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))
