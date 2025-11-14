#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
One-time Gmail OAuth bootstrap that stores a refresh-tokened token.json
so you don't have to re-auth again.

What it does:
- Opens the browser for consent with access_type="offline" + prompt="consent"
- Saves token.json next to client_secret.json
- Verifies the token by calling Gmail API (prints your email)

Prereqs:
pip install google-auth google-auth-oauthlib google-api-python-client python-dotenv
"""

import json
from pathlib import Path
from dotenv import load_dotenv
import os

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials

# ---------- Config ----------
load_dotenv()

SCOPES = [
    "https://www.googleapis.com/auth/gmail.modify",
    "https://www.googleapis.com/auth/gmail.send",
]

# Where your OAuth client + token will live
SECRET_PATH = Path(os.getenv("GMAIL_CLIENT_SECRET_PATH", "secrets/client_secret.json"))
TOKEN_PATH = SECRET_PATH.parent / "token.json"

def main():
    if not SECRET_PATH.exists():
        raise FileNotFoundError(
            f"Client secret not found at {SECRET_PATH}. "
            "Download it from Google Cloud Console and set GMAIL_CLIENT_SECRET_PATH if needed."
        )

    # Run local OAuth flow and FORCE a refresh token to be issued
    flow = InstalledAppFlow.from_client_secrets_file(str(SECRET_PATH), SCOPES)
    creds = flow.run_local_server(
        port=0,
        access_type="offline",   # <-- ensures refresh_token
        prompt="consent"         # <-- forces showing consent to issue refresh_token
    )

    # Persist credentials
    TOKEN_PATH.parent.mkdir(parents=True, exist_ok=True)
    TOKEN_PATH.write_text(creds.to_json(), encoding="utf-8")

    # Quick verification call (optional but helpful)
    service = build("gmail", "v1", credentials=creds)
    profile = service.users().getProfile(userId="me").execute()
    print("✅ Auth success.")
    print(f"   Email: {profile.get('emailAddress')}")
    print(f"   Token saved to: {TOKEN_PATH}")
    print(f"   Has refresh_token? {'refresh_token' in json.loads(TOKEN_PATH.read_text())}")

if __name__ == "__main__":
    main()
