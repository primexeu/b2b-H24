#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import time
import json
import base64
from pathlib import Path
from typing import List, Tuple, Optional, Dict, Any
from io import BytesIO
from datetime import datetime, timedelta

from dotenv import load_dotenv
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import email.utils
from email.message import EmailMessage

# --- NEW: Imports for PDF & Excel Generation ---
import pandas as pd
import pytz
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.lib.colors import HexColor

# Local triage lib (your updated JSON-based version)
import claim_triage as CT

# ---------------------------
# Env / Config
# ---------------------------
load_dotenv()

SCOPES = [
    "https://www.googleapis.com/auth/gmail.modify",
    "https://www.googleapis.com/auth/gmail.send",
]

SECRET_PATH = Path(os.getenv("GMAIL_CLIENT_SECRET_PATH", "secrets/client_secret.json"))
TOKEN_PATH = SECRET_PATH.parent / "token.json"

POLL_SECONDS = int(os.getenv("GMAIL_POLL_SECONDS", "15"))
GMAIL_QUERY = os.getenv("GMAIL_QUERY", "in:inbox -in:spam -in:trash -from:me")
MAX_RESULTS = int(os.getenv("GMAIL_MAX_RESULTS", "200"))

OUTPUT_ROOT = Path(os.getenv("GMAIL_OUTPUT_ROOT", "inbox_downloads"))
OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

SEEN_STATE_FILE = Path(os.getenv("GMAIL_STATE_FILE", "gmail_seen_ids.json"))
BASELINE_ON_START = os.getenv("GMAIL_BASELINE_ON_START", "1") not in ("0", "false", "False")

# --- Claims Database File ---
CLAIMS_DB_FILE = Path("claims.json")

# GPT config
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5-chat-latest")

MARK_AS_READ = os.getenv("GMAIL_MARK_READ", "1") not in ("0", "false", "False")

AUTO_REPLY = os.getenv("GMAIL_AUTO_REPLY", "1") not in ("0", "false", "False")
REPLY_FROM_NAME = os.getenv("GMAIL_REPLY_FROM_NAME", "Claims Assistant")
REPLY_SIGNATURE = os.getenv("GMAIL_REPLY_SIGNATURE", "—\nPrimex Claims Assistant")

# --- NEW: fixed recipient for client-facing email
CLIENT_NOTIFICATION_TO = "primexpresentation@gmail.com"
# --- END NEW ---

# ---------------------------
# --- Generation & Database Helpers ---
# ---------------------------

def load_claims_from_json():
    """Loads claims from the JSON file database."""
    if not CLAIMS_DB_FILE.exists():
        return []
    try:
        with open(CLAIMS_DB_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (json.JSONDecodeError, FileNotFoundError):
        return []

def save_claims_to_json(claims_data):
    """Saves the updated list of claims to the JSON file."""
    with open(CLAIMS_DB_FILE, 'w', encoding='utf-8') as f:
        json.dump(claims_data, f, indent=4, ensure_ascii=False)

def generate_claims_excel(claims_data):
    """Generates an Excel file from the claims data in memory, structured like the ERP."""
    if not claims_data:
        claims_data = []
    
    df = pd.DataFrame(claims_data)
    
    expected_columns = [
        "Datum", "Bestellnummer", "Kunde", 
        "Pos.", "Artikel / Service", "Artikelbezeichnung", "Menge", 
        "BFQ - Ursache", "BFQ - Verursacher",
        "Wunsch-KW", "Bestätigungs-KW", 
        "Rechnungsadresse", "Lieferadresse", "Produkt", "Urteil"
    ]
    for col in expected_columns:
        if col not in df.columns:
            df[col] = None
    
    df = df[expected_columns]

    buffer = BytesIO()
    df.to_excel(buffer, index=False, engine='openpyxl')
    buffer.seek(0)
    return buffer

def generate_confirmation_pdf(approved_claims: list, assembly_data_used: dict, contact_info: dict, po_number: str):
    """
    Generates a dynamic order confirmation PDF with real data and calculated prices.
    For approved claims, prices are set to 0.00 to reflect a free-of-charge replacement.
    """
    buffer = BytesIO()
    p = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4
    
    # --- CONFIG ---
    VAT_RATE = 0.19
    GREEN_COLOR = HexColor("#9ACD32")
    GREY_COLOR = HexColor("#333333")
    
    # --- DYNAMIC DATA SETUP ---
    customer_name = contact_info.get("company") or contact_info.get("name") or "N/A"
    customer_address_lines = (contact_info.get("address") or "").split('\n')
    
    cet_tz = pytz.timezone('CET')
    now_cet = datetime.now(cet_tz)
    today_date_str = now_cet.strftime("%d.%m.%Y")
    iso_year, iso_week, _ = now_cet.isocalendar()
    current_kw_str = f"{iso_week}. KW {iso_year}"
    next_week_date = now_cet + timedelta(days=7)
    _, next_iso_week, _ = next_week_date.isocalendar()
    next_kw_str = f"{next_iso_week}. KW {iso_year}"

    def draw_text(x, y, text, font="Helvetica", size=10, color=GREY_COLOR, align="left"):
        p.setFont(font, size)
        p.setFillColor(color)
        if align == "right": p.drawRightString(x * mm, height - y * mm, text)
        else: p.drawString(x * mm, height - y * mm, text)

    # --- HEADER ---
    draw_text(20, 25, "X GmbH", font="Helvetica-Bold", size=24)
    p.setFillColor(GREEN_COLOR)
    p.rect(120 * mm, height - 30 * mm, 80 * mm, 10 * mm, stroke=0, fill=1)
    draw_text(122, 26, "Kundendienst Auftragsbestätigung", font="Helvetica-Bold", size=12, color=HexColor("#FFFFFF"))

    # --- CUSTOMER & ORDER DETAILS ---
    y_cust = 55
    draw_text(20, y_cust, customer_name, font="Helvetica-Bold", size=12)
    for i, line in enumerate(customer_address_lines):
        draw_text(20, y_cust + 5 + (i * 5), line, size=12)

    details = [
        ("Beleg Nr.", "N/A", "Datum", today_date_str),
        ("Kunden Nr.", "N/A", "Kunde", customer_name),
        ("Bestätigungs-KW", next_kw_str, "Bestellnummer", po_number),
        ("Referenz", "N/A", "Wunsch-KW", current_kw_str)
    ]
    x_start, y_start = 120, 35
    for i, row in enumerate(details):
        draw_text(x_start, y_start + i * 10, row[0], size=8)
        draw_text(x_start, y_start + 4 + i * 10, row[1], font="Helvetica-Bold")
        draw_text(x_start + 40, y_start + i * 10, row[2], size=8)
        draw_text(x_start + 40, y_start + 4 + i * 10, row[3], font="Helvetica-Bold")

    draw_text(120, 90, "Lieferadresse", size=8)
    draw_text(120, 95, customer_name, font="Helvetica-Bold")
    for i, line in enumerate(customer_address_lines):
        draw_text(120, 100 + (i * 5), line)

    # --- TABLE HEADER ---
    p.setFillColor(GREEN_COLOR)
    p.rect(15 * mm, height - 130 * mm, 185 * mm, 8 * mm, stroke=0, fill=1)
    headers = [("Artikelnr.", 20), ("Beschreibung", 40), ("Menge", 125), ("Preis", 155), ("Total EUR", 175)]
    for text, x_pos in headers: draw_text(x_pos, 127, text, font="Helvetica-Bold", color=HexColor("#FFFFFF"), size=9)
    
    # --- TABLE ROWS & PRICE CALCULATION ---
    y_pos = 138
    net_total = 0.0
    for claim in approved_claims:
        part_ref = _infer_part_ref(claim) # Use robust inference for consistency
        article_no = claim.get("article_no", "N/A")
        quantity = claim.get("quantity", 0)
        
        part_description = f"Teil Nr. {part_ref if part_ref is not None else 'N/A'}"
        
        # Free replacement, so price is 0
        part_price = 0.00
        part_total = quantity * part_price
        net_total += part_total

        draw_text(20, y_pos, article_no, size=8)
        draw_text(40, y_pos, part_description, font="Helvetica-Bold")
        draw_text(125, y_pos, f"{quantity} Stk.")
        draw_text(170, y_pos, f"{part_price:.2f}".replace('.', ','), align="right")
        draw_text(195, y_pos, f"{part_total:.2f}".replace('.', ','), align="right")
        y_pos += 15

    # --- TOTALS ---
    vat_amount = net_total * VAT_RATE
    grand_total = net_total + vat_amount

    p.line(130 * mm, height - 215 * mm, 200 * mm, height - 215 * mm)
    draw_text(135, 220, "Nettobetrag")
    draw_text(195, 220, f"{net_total:.2f}".replace('.', ','), align="right")
    draw_text(135, 225, f"Mehrwertsteuer {VAT_RATE*100:.0f}%")
    draw_text(195, 225, f"{vat_amount:.2f}".replace('.', ','), align="right")
    p.setLineWidth(1.5)
    p.line(130 * mm, height - 230 * mm, 200 * mm, height - 230 * mm)
    draw_text(135, 234, "Total EUR", font="Helvetica-Bold")
    draw_text(195, 234, f"{grand_total:.2f}".replace('.', ','), font="Helvetica-Bold", align="right")

    # --- FOOTER ---
    y_footer = 250
    draw_text(20, y_footer + 5, "30 Tage 5 %"); draw_text(20, y_footer + 15, "frei Haus")
    p.line(15 * mm, height - (y_footer + 20) * mm, 200 * mm, height - (y_footer + 20) * mm)
    draw_text(20, y_footer + 30, "X GmbH")
    draw_text(130, y_footer + 30, "IBAN: DE00 0000 0000 0000 0000 00")
    draw_text(130, y_footer + 35, "SWIFT: XXXXXXXX")
    
    p.showPage()
    p.save()
    buffer.seek(0)
    return buffer


# ---------------------------
# Gmail helpers
# ---------------------------
def get_service():
    if not TOKEN_PATH.exists():
        raise FileNotFoundError("Run your Gmail auth bootstrap to create token.json (scope includes gmail.send now).")
    creds = Credentials.from_authorized_user_file(str(TOKEN_PATH), SCOPES)
    return build("gmail", "v1", credentials=creds)

def _list_message_ids(service, query: str, max_total: int) -> List[str]:
    ids: List[str] = []
    page_token = None
    while True:
        resp = service.users().messages().list(userId="me", q=query, maxResults=min(500, max_total - len(ids)), pageToken=page_token).execute()
        ids.extend([m["id"] for m in resp.get("messages", []) or []])
        if len(ids) >= max_total: break
        page_token = resp.get("nextPageToken")
        if not page_token: break
    return ids

def _html_to_text(html: str) -> str:
    text = re.sub(r"(?is)<(script|style).*?>.*?</\1>", "", html or "")
    text = re.sub(r"(?is)<br\s*/?>", "\n", text)
    text = re.sub(r"(?is)</p>", "\n\n", text)
    text = re.sub(r"(?is)<.*?>", "", text)
    text = re.sub(r"\r\n?", "\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()

def _decode_b64url(data_b64: Optional[str]) -> bytes:
    if not data_b64: return b""
    return base64.urlsafe_b64decode(data_b64.encode("utf-8"))

def _safe_name(name: Optional[str], default: str = "file"):
    if not name: name = default
    return re.sub(r"[^\w\-.]+", "_", name)

def _save_bytes(path: Path, data: bytes) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return path

def _extract_body_from_payload(payload) -> str:
    def walk(p):
        mime = (p.get("mimeType") or "").lower()
        body = p.get("body", {}) or {}
        data = body.get("data")
        parts = p.get("parts", []) or []
        def _dec(d: bytes) -> str:
            try: return d.decode("utf-8", "ignore")
            except Exception: return d.decode(errors="ignore")
        if mime == "text/plain" and data: return _dec(_decode_b64url(data))
        if mime == "text/html" and data: return _html_to_text(_dec(_decode_b64url(data)))
        for sp in parts:
            txt = walk(sp)
            if txt: return txt
        return ""
    return walk(payload).strip()

def get_email_meta_and_body(service, msg_id: str):
    msg = service.users().messages().get(userId="me", id=msg_id, format="full").execute()
    payload = msg.get("payload", {}) or {}
    headers = payload.get("headers", []) or []
    hdr = {h["name"].lower(): h["value"] for h in headers if "name" in h and "value" in h}
    subject = hdr.get("subject", "")
    from_ = hdr.get("from", "")
    date_ = hdr.get("date", "")
    message_id = hdr.get("message-id", "")
    body_text = _extract_body_from_payload(payload)
    thread_id = msg.get("threadId")
    return subject, from_, date_, body_text, msg, hdr, message_id, thread_id

def download_attachments(service, msg) -> Tuple[List[Path], List[Path]]:
    msg_id = msg["id"]
    payload = msg.get("payload", {}) or {}
    parts = payload.get("parts", []) or []
    out_dir = OUTPUT_ROOT / msg_id
    pdfs, images = [], []
    def _fetch_and_save(att_id, filename):
        att = service.users().messages().attachments().get(userId="me", messageId=msg_id, id=att_id).execute()
        data = att.get("data")
        if not data: return None
        file_bytes = _decode_b64url(data)
        dest = out_dir / _safe_name(filename)
        _save_bytes(dest, file_bytes)
        return dest
    def walk(p):
        filename = p.get("filename") or ""
        body = p.get("body", {}) or {}
        att_id = body.get("attachmentId")
        mime = (p.get("mimeType") or "").lower()
        if att_id and (filename or mime.startswith(("image/", "application/pdf"))):
            if not filename:
                ext = ".pdf" if mime == "application/pdf" else f".{(mime.split('/')[-1] or 'bin')}"
                filename = f"part_{msg_id}{ext}"
            dest = _fetch_and_save(att_id, filename)
            if dest:
                if dest.suffix.lower() == ".pdf": pdfs.append(dest)
                elif mime.startswith("image/") or dest.suffix.lower() in {".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp"}: images.append(dest)
        for sp in (p.get("parts") or []): walk(sp)
    for prt in parts: walk(prt)
    return pdfs, images

def mark_as_read(service, msg_id: str):
    if not MARK_AS_READ: return
    service.users().messages().modify(userId="me", id=msg_id, body={"removeLabelIds": ["UNREAD"]}).execute()

# ---------------------------
# Auto-reply helpers
# ---------------------------

def _extract_email_address(raw_from: str) -> str:
    name, addr = email.utils.parseaddr(raw_from or "")
    return addr or (raw_from or "").strip()

def _esc(s: Optional[str | int]) -> str:
    if s is None: return ""
    return str(s).replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

# --- BUG FIX: Robust part reference inference for alphanumeric part numbers ---
def _infer_part_ref(claim: Dict[str, Any]) -> Optional[str]:
    """
    Resolve a displayable part reference from a claim. Handles numeric and alphanumeric parts.
    - prefer claim['part_reference'] if present and non-empty
    - fall back to common aliases (part_code/part_no/partnumber/part_id)
    - last resort: parse from assembly_check.comment like 'Teil H1' / 'Part 159138-A'
    Returns a string or None if nothing is found.
    """
    pr = claim.get("part_reference")
    if pr not in (None, "", 0):
        return str(pr)

    for k in ("part_code", "part_no", "partnumber", "part_id"):
        alt = claim.get(k)
        if alt not in (None, "", 0):
            return str(alt)

    asm_comment = (claim.get("assembly_check") or {}).get("comment", "") or ""
    # Regex now captures alphanumeric parts with hyphens (e.g., H1, S-25, 123-A)
    m = re.search(r"(?:teil|part)\s*(?:nr\.|number|no\.|#|:)?\s*[-]?\s*([\w-]+)", asm_comment, re.I)
    if m:
        return m.group(1)

    return None

def _summarize_unified(unified: Dict[str, Any]) -> Tuple[str, str]:
    if not isinstance(unified, dict): return ("Could not process analysis.", "Could not process analysis.")
    summary = unified.get("summary", "Ihre Anfrage wurde geprüft.")
    overall_verdict = unified.get("overall_verdict", "unknown")
    recommended_action = unified.get("recommended_action", "Bitte warten Sie auf die manuelle Bearbeitung.")
    per_claim_analysis = unified.get("per_claim_analysis", [])
    entities = unified.get("entities", {})
    ab_numbers = ", ".join(entities.get("ab_numbers", [])) or "N/A"
    po_numbers = ", ".join(entities.get("purchase_orders", [])) or "N/A"
    analysis_html = ""
    analysis_text = ""
    if per_claim_analysis:
        analysis_html = """<table style="width: 100%; border-collapse: collapse; margin-top: 15px; font-size: 12px; border: 1px solid #ddd;"><tr style="background-color: #f2f2f2; text-align: left;"><th style="padding: 8px; border: 1px solid #ddd;">Teil / Artikel</th><th style="padding: 8px; border: 1px solid #ddd;">Prüfung</th><th style="padding: 8px; border: 1px solid #ddd;">Schadensart</th><th style="padding: 8px; border: 1px solid #ddd;">Ergebnis</th></tr>"""
        analysis_text = "Details zur Prüfung:\n--------------------------------------------------\n"
        for claim in per_claim_analysis:
            part_ref_val = _infer_part_ref(claim)
            part_ref = _esc(part_ref_val if part_ref_val not in (None, "", 0) else "—")
            article = _esc(claim.get("article_no", 'N/A'))
            asm_comment = _esc((claim.get("assembly_check") or {}).get("comment", ''))
            img_obs = _esc((claim.get("image_check") or {}).get("observation", ''))
            damage_assess = claim.get("damage_assessment", {})
            damage_verdict_raw = damage_assess.get("verdict", 'unknown')
            damage_verdict = _esc(damage_verdict_raw.replace('_', ' ').title())
            damage_just = _esc(damage_assess.get("justification", ''))
            part_verdict_raw = claim.get("part_verdict", 'pending')
            part_verdict = _esc(part_verdict_raw.upper())
            verdict_color = {"approved": "#28a745", "rejected": "#dc3545", "pending_more_info": "#17a2b8"}.get(part_verdict_raw, "#6c757d")
            analysis_html += f"""<tr style="border-bottom: 1px solid #eee;"><td style="padding: 8px; border: 1px solid #ddd;"><b>Teil: {part_ref}</b><br><small>Artikel: {article}</small></td><td style="padding: 8px; border: 1px solid #ddd;">{asm_comment}<br><small><em>Foto: {img_obs}</em></small></td><td style="padding: 8px; border: 1px solid #ddd;"><b>{damage_verdict}</b><br><small>{damage_just}</small></td><td style="padding: 8px; border: 1px solid #ddd; color: {verdict_color}; font-weight: bold;">{part_verdict}</td></tr>"""
            analysis_text += f"Teil: {part_ref} (Artikel: {article})\n  - Prüfung: {asm_comment}\n  - Foto-Analyse: {img_obs}\n  - Schadensart: {damage_verdict} ({damage_just})\n  - Ergebnis: {part_verdict}\n\n"
        analysis_html += "</table>"
    text_lines = ["Guten Tag,", "", "vielen Dank für Ihre Nachricht. Wir haben Ihre Anfrage automatisiert geprüft.", "", f"Zusammenfassung: {summary}", "", f"Gesamtergebnis: {overall_verdict.replace('_', ' ').title()}", "", analysis_text if per_claim_analysis else "Keine spezifischen Teile zur Prüfung gefunden.", f"Empfohlene nächste Schritte: {recommended_action}", "", "Referenznummern:", f" - AB-Nummer(n): {ab_numbers}", f" - PO-Nummer(n): {po_numbers}", "", "Mit freundlichen Grüßen,", REPLY_SIGNATURE]
    text_body = "\n".join(text_lines)
    html_body = f"""<div style="font-family: Arial, sans-serif; font-size: 14px; color: #333;"><p>Guten Tag,</p><p>vielen Dank für Ihre Nachricht. Wir haben Ihre Anfrage automatisiert geprüft und können Ihnen folgendes Ergebnis mitteilen:</p><div style="background-color: #f8f9fa; border-left: 4px solid #007bff; padding: 10px 15px; margin: 15px 0;"><p style="margin: 0;"><b>Zusammenfassung:</b> {_esc(summary)}</p><p style="margin: 5px 0;"><b>Gesamtergebnis:</b> <strong>{_esc(overall_verdict.replace('_', ' ').title())}</strong></p></div>{analysis_html if per_claim_analysis else '<p><em>Keine spezifischen Teile zur Prüfung gefunden.</em></p>'}<div style="background-color: #f8f9fa; border-left: 4px solid #ffc107; padding: 10px 15px; margin: 15px 0;"><p style="margin: 0;"><b>Empfohlene nächste Schritte:</b><br>{_esc(recommended_action)}</p></div><div style="font-size: 12px; color: #666; margin-top: 20px;"><p style="margin: 0;"><b>Referenznummern:</b></p><ul style="margin: 5px 0; padding-left: 20px;"><li>AB-Nummer(n): {_esc(ab_numbers)}</li><li>PO-Nummer(n): {_esc(po_numbers)}</li></ul></div><p>Mit freundlichen Grüßen,</p><p style="white-space: pre-wrap;">{_esc(REPLY_SIGNATURE)}</p></div>"""
    return text_body, html_body.strip()

def _encode_message(msg: EmailMessage) -> str:
    return base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")

def send_reply_email(service, *, to_addr: str, subject: str, thread_id: Optional[str],
                     in_reply_to_message_id: Optional[str],
                     text_body: str, html_body: str,
                     attachments: Optional[List[Dict[str, Any]]] = None):
    msg = EmailMessage()
    msg["To"] = to_addr
    msg["Subject"] = subject
    if in_reply_to_message_id:
        msg["In-Reply-To"] = in_reply_to_message_id
        msg["References"] = in_reply_to_message_id
    msg["X-Primex-AutoReply"] = "ClaimsBot-6.0-Strategic"
    msg.set_content(text_body)
    msg.add_alternative(html_body, subtype="html")
    if attachments:
        for attachment in attachments:
            msg.add_attachment(attachment['data'], maintype=attachment['maintype'], subtype=attachment['subtype'], filename=attachment['filename'])
    body = {"raw": _encode_message(msg)}
    if thread_id:
        body["threadId"] = thread_id
    service.users().messages().send(userId="me", body=body).execute()


# --- MAJOR OVERHAUL: GPT acts as a "Customer Service Strategist" ---
def build_client_email_via_gpt(unified: Dict[str, Any], email_struct: Optional[Dict[str, Any]], original_subject: str) -> str:
    """
    Returns a professional, action-oriented email body (German) for the client.
    Uses GPT with advanced instructions to translate technical findings into customer-centric actions.
    Falls back to a smarter deterministic template if GPT fails.
    """
    # Gather all available context
    if not isinstance(unified, dict): unified = {}
    if not isinstance(email_struct, dict): email_struct = {}

    entities = unified.get("entities", {})
    po_list = entities.get("purchase_orders", [])
    ab_list = entities.get("ab_numbers", [])
    customer = email_struct.get("contact", {})
    cust_name = customer.get("company") or customer.get("name") or "Kundin/Kunde"
    is_price_quote_request = "price_quote" in email_struct.get("request_types", [])
    
    overall_verdict = unified.get("overall_verdict")
    recommended_action = unified.get("recommended_action")

    # Prepare a detailed summary of each claim for the AI
    claims = unified.get("per_claim_analysis", [])
    claims_brief = []
    for c in claims:
        claims_brief.append({
            "part_reference": _infer_part_ref(c),
            "article_no": c.get("article_no"),
            "quantity": c.get("quantity"),
            "part_verdict": c.get("part_verdict"), # e.g., 'approved', 'rejected'
            "damage_verdict": (c.get("damage_assessment") or {}).get("verdict"), # e.g., 'customer_damage'
            "damage_justification": (c.get("damage_assessment") or {}).get("justification"),
            "assembly_note": (c.get("assembly_check") or {}).get("comment"), # e.g., 'Part not listed'
            "image_observation": (c.get("image_check") or {}).get("observation"),
        })

    # Try GPT with the new, advanced prompt
    client = None
    if OPENAI_API_KEY:
        try:
            from openai import OpenAI
            client = OpenAI(api_key=OPENAI_API_KEY, base_url=OPENAI_BASE_URL or None)
        except Exception: client = None

    if client:
        try:
            # This is the new "brain" for the AI. It's much more detailed.
            sys = (
                "You are a German Customer Service Strategist for a furniture company. Your goal is to convert a technical JSON analysis into a friendly, professional, and ACTION-ORIENTED email to the customer. "
                "NEVER just state a verdict like 'rejected'. ALWAYS explain the situation and offer the correct next step. Use 'Sie'-form. Write a natural-sounding, flowing text, not a list."
                "\n\n--- YOUR STRATEGIC RULES ---\n"
                "1.  **If a part is 'approved'**: Confirm that the replacement will be shipped free of charge. Mention the attached order confirmation IF AND ONLY IF all parts in the claim were approved.\n"
                "2.  **If `damage_verdict` is 'customer_damage'**: The `part_verdict` will be 'rejected'. DO NOT just say 'abgelehnt'. Explain that this type of damage is not covered by the warranty.\n"
                "    - **If the user explicitly asked for a price quote (`is_price_quote_request: true`)**: State the price for the replacement part is **15.30 EUR per piece** plus shipping, and this is a non-binding offer. Ask if they wish to proceed with an official quote.\n"
                "    - **If the user did NOT ask for a price quote (`is_price_quote_request: false`)**: Simply offer to provide a non-binding price quote for a replacement part if they are interested.\n"
                "3.  **If `part_verdict` is 'pending_more_info' because of `assembly_note` (e.g., 'Part not listed', 'nicht gefunden')**: Explain that the provided part number could not be verified in the assembly plans. Ask the customer to double-check the number and, if possible, send a photo of the assembly manual with the required part marked.\n"
                "4.  **If `part_verdict` is 'pending_more_info' for other reasons (e.g., blurry photos)**: Clearly state what kind of photos are needed (e.g., 'a close-up of the damage', 'an overall view of the furniture').\n"
                "5.  **Structure**: Start with a polite opening referencing their order/AB number. Then, discuss each part and its outcome according to the rules above. End with a polite closing.\n"
                "6.  **Attachments**: If an order confirmation is being generated (because all parts were approved), remember to mention that it is attached.\n"
            )
            
            user_payload = {
                "customer_name": cust_name,
                "purchase_orders": po_list,
                "ab_numbers": ab_list,
                "is_price_quote_request": is_price_quote_request,
                "overall_verdict": overall_verdict,
                "recommended_action_internal": recommended_action,
                "claims": claims_brief
            }
            resp = client.chat.completions.create(
                model=OPENAI_MODEL,
                temperature=0.2,
                messages=[
                    {"role": "system", "content": sys},
                    {"role": "user", "content": f"Here is the technical analysis JSON. Please write the customer email based on my rules.\n\n{json.dumps(user_payload, ensure_ascii=False)}"}
                ]
            )
            text = (resp.choices[0].message.content or "").strip()
            if text:
                return text
        except Exception as e:
            print(f"GPT generation failed: {e}. Falling back to template.")
            pass

    # --- Smarter Fallback Template (if GPT fails) ---
    def _needs_refine_from_assembly(note: Optional[str]) -> bool:
        if not note: return False
        low = note.lower()
        return any(k in low for k in ["not listed", "nicht", "kein treffer", "no match", "nicht gefunden"])

    po_str = ", ".join(po_list) if po_list else "Ihre Bestellung"
    ab_str = ", ".join(ab_list) if ab_list else None
    
    lines = ["Guten Tag,", ""]
    if ab_str: lines.append(f"wir beziehen uns auf Ihre Reklamation zur AB {ab_str} (Bestellung: {po_str}).")
    else: lines.append(f"wir beziehen uns auf Ihre Reklamation zu Ihrer Bestellung {po_str}.")
    lines.append("Nach unserer Prüfung teilen wir Ihnen Folgendes mit:")
    lines.append("")

    if not claims:
        lines.append("Wir konnten Ihrer Anfrage keine spezifischen Teile zuordnen. Bitte senden Sie uns eine detaillierte Beschreibung und aussagekräftige Bilder (Detailaufnahme des Schadens, Gesamtansicht).")
    else:
        for c in claims:
            pr = _infer_part_ref(c) or "—"
            art = c.get("article_no") or "—"
            verdict = c.get("part_verdict", "pending").lower()
            damage_verdict = (c.get("damage_assessment") or {}).get("verdict", "")
            asm_note = (c.get("assembly_check") or {}).get("comment", "")
            prefix = f"- Teil {pr} (Artikel {art}): "

            if verdict == "approved":
                lines.append(prefix + "freigegeben – wir veranlassen den kostenfreien Versand des Ersatzteils.")
            elif damage_verdict == "customer_damage":
                base_text = prefix + "diese Art von Beschädigung ist leider nicht von der Gewährleistung abgedeckt. "
                if is_price_quote_request:
                    lines.append(base_text + "Wie gewünscht, teilen wir Ihnen mit, dass der Preis für ein Ersatzteil 15,30 EUR zzgl. Versand beträgt. Bitte geben Sie uns kurz Bescheid, ob wir ein verbindliches Angebot für Sie erstellen sollen.")
                else:
                    lines.append(base_text + "Gerne können wir Ihnen jedoch ein unverbindliches Angebot für ein Ersatzteil erstellen. Bitte geben Sie uns kurz Bescheid, ob dies gewünscht ist.")
            elif verdict == "pending_more_info" and _needs_refine_from_assembly(asm_note):
                lines.append(prefix + "die angegebene Teilenummer konnte in unseren Montageanleitungen nicht gefunden werden. Um Ihnen das korrekte Teil zusenden zu können, bitten wir Sie, die Nummer zu überprüfen und uns idealerweise ein Foto aus der Anleitung mit markiertem Teil zu senden.")
            else: # Generic pending or other rejection
                lines.append(prefix + "für eine abschließende Prüfung benötigen wir noch aussagekräftigere Fotos (eine Nahaufnahme des Schadens und eine Gesamtansicht des Möbels).")
    
    all_claims_analysis = unified.get("per_claim_analysis", [])
    approved_claims = [claim for claim in all_claims_analysis if claim.get("part_verdict") == "approved"]
    all_parts_approved = len(all_claims_analysis) > 0 and len(all_claims_analysis) == len(approved_claims)
    
    if all_parts_approved:
        lines.extend(["", "Die Auftragsbestätigung für den Versand finden Sie im Anhang."])
    
    lines.extend(["", "Bei Rückfragen helfen wir Ihnen gerne weiter.", "", "Mit freundlichen Grüßen", "Primex Kundendienst"])
    return "\n".join(lines)


# ---------------------------
# Seen-state helpers
# ---------------------------
def _load_seen_ids() -> set:
    if SEEN_STATE_FILE.exists():
        try: return set(json.loads(SEEN_STATE_FILE.read_text(encoding="utf-8")))
        except Exception: return set()
    return set()

def _save_seen_ids(seen: set):
    tmp = SEEN_STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(sorted(list(seen)), ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(SEEN_STATE_FILE)

# ---------------------------
# Main loop
# ---------------------------
def main_loop():
    service = get_service()
    seen = _load_seen_ids()

    print(f"🔩 Assembly Data source: {CT.ASSEMBLY_DATA_JSON}")
    print(f"👂 Listening for *new* emails… every {POLL_SECONDS}s")

    first_cycle = True
    while True:
        try:
            ids = _list_message_ids(service, GMAIL_QUERY, MAX_RESULTS)
            if first_cycle and BASELINE_ON_START and ids:
                print(f"⏭️  Baseline on start: recording {len(ids)} existing IDs as seen (skip them).")
                seen.update(ids); _save_seen_ids(seen); first_cycle = False; time.sleep(POLL_SECONDS); continue
            first_cycle = False
            new_ids = [mid for mid in ids if mid not in seen]
            if new_ids: print(f"✨ {len(new_ids)} new email(s) detected")

            for mid in reversed(new_ids):
                try:
                    subject, from_, date_, body_text, msg, headers_map, message_id, thread_id = get_email_meta_and_body(service, mid)
                    if (headers_map.get("x-primex-autoreply") or "").lower().startswith("claimsbot-"):
                        seen.add(mid); _save_seen_ids(seen); continue
                    
                    pdfs, images = download_attachments(service, msg)
                    print(f"\n[Processing] {mid[:12]}… | imgs={len(images)} pdfs={len(pdfs)} | From: {from_} | Subj: {subject[:60].replace(os.linesep,' ')}")
                    
                    # --- Stage 1: Initial Data Gathering ---
                    email_struct = CT.analyze_email_freeform(subject=subject, body=body_text, image_paths=[str(p) for p in images], api_key=OPENAI_API_KEY, model=OPENAI_MODEL, base_url=OPENAI_BASE_URL) if OPENAI_API_KEY else {}
                    
                    pdf_results = []
                    for pdf in pdfs:
                        try:
                            extracted_data = CT.extract_from_pdf(str(pdf))
                            pdf_results.append({"pdf_path": str(pdf), "extracted_json": extracted_data})
                        except Exception as e:
                            pdf_results.append({"pdf_path": str(pdf), "error": str(e)})

                    # --- Stage 2: Pre-flight Checks for Critical Info ---
                    
                    has_product_images = email_struct.get("has_product_images", False)

                    if not pdfs and not has_product_images:
                        print(f"❗️ No PDF and no relevant product images found for {mid[:12]}. Replying to request images.")
                        if AUTO_REPLY:
                            to_addr = _extract_email_address(from_)
                            if to_addr:
                                reply_subject = f"Re: {subject}" if subject and not subject.lower().startswith("re:") else (subject or "Re: Your Inquiry")
                                text_body = ("Guten Tag,\n\n"
                                             "vielen Dank für Ihre E-Mail.\n\n"
                                             "Für die Bearbeitung Ihrer Reklamation benötigen wir aussagekräftige Fotos des Problems. Bitte senden Sie uns Bilder von:\n"
                                             "1. Dem betroffenen Teil/Schaden (Nahaufnahme)\n"
                                             "2. Dem gesamten Möbelstück (Gesamtansicht)\n"
                                             "3. Dem Etikett auf dem Karton (falls noch vorhanden)\n\n"
                                             "Ohne diese Informationen können wir Ihre Anfrage leider nicht weiter bearbeiten.\n\n"
                                             "Mit freundlichen Grüßen,\n"
                                             f"{REPLY_SIGNATURE.splitlines()[1]}")
                                html_body = (f'<div style="font-family: Arial, sans-serif; font-size: 14px; color: #333;">'
                                             f'<p>Guten Tag,</p><p>vielen Dank für Ihre E-Mail.</p>'
                                             f'<div style="background-color: #fffbe6; border-left: 4px solid #ffc107; padding: 10px 15px; margin: 15px 0;">'
                                             f'<p style="margin: 0;"><b>Wichtiger Hinweis:</b> Für die Bearbeitung Ihrer Reklamation benötigen wir aussagekräftige Fotos des Problems. Bitte senden Sie uns Bilder von:</p>'
                                             f'<ul style="margin-top: 5px;"><li>Dem betroffenen Teil/Schaden (Nahaufnahme)</li><li>Dem gesamten Möbelstück (Gesamtansicht)</li><li>Dem Etikett auf dem Karton (falls noch vorhanden)</li></ul>'
                                             f'<p style="margin: 0; margin-top: 10px;">Ohne diese Informationen können wir Ihre Anfrage leider nicht weiter bearbeiten.</p>'
                                             f'</div>'
                                             f'<p>Mit freundlichen Grüßen,</p><p style="white-space: pre-wrap;">{_esc(REPLY_SIGNATURE.splitlines()[1])}</p>'
                                             f'</div>')
                                send_reply_email(service, to_addr=to_addr, subject=reply_subject, thread_id=thread_id, in_reply_to_message_id=message_id, text_body=text_body, html_body=html_body)
                                print(f"📧 Sent request for missing images to {to_addr}.")
                        if MARK_AS_READ: mark_as_read(service, mid)
                        seen.add(mid); _save_seen_ids(seen); continue

                    all_ab_numbers = email_struct.get("ab_numbers", [])
                    all_po_numbers = email_struct.get("purchase_orders", [])
                    for res in pdf_results:
                        pdf_po = (res.get("extracted_json") or {}).get("original_po")
                        if pdf_po and pdf_po not in all_po_numbers: all_po_numbers.append(pdf_po)

                    if not all_ab_numbers and not all_po_numbers:
                        print(f"❗️ Missing critical info (AB/PO Number) for {mid[:12]}. Replying to request info.")
                        if AUTO_REPLY:
                            to_addr = _extract_email_address(from_)
                            if to_addr:
                                reply_subject = f"Re: {subject}" if subject and not subject.lower().startswith("re:") else (subject or "Re: Your Inquiry")
                                text_body = ( "Guten Tag,\n\nvielen Dank für Ihre E-Mail.\n\n"
                                              "Um Ihre Anfrage bearbeiten zu können, benötigen wir zwingend eine AB-Nummer (Auftragsbestätigung) oder eine PO-Nummer (Ihre Bestellnummer).\n\n"
                                              "Bitte antworten Sie auf diese E-Mail und ergänzen Sie die fehlenden Informationen.\n\n"
                                              f"Mit freundlichen Grüßen,\n{REPLY_SIGNATURE.splitlines()[1]}" )
                                html_body = ( f'<div style="font-family: Arial, sans-serif; font-size: 14px; color: #333;">'
                                              f'<p>Guten Tag,</p><p>vielen Dank für Ihre E-Mail.</p><div style="background-color: #fffbe6; border-left: 4px solid #ffc107; padding: 10px 15px; margin: 15px 0;">'
                                              f'<p style="margin: 0;"><b>Wichtiger Hinweis:</b> Um Ihre Anfrage bearbeiten zu können, benötigen wir zwingend eine <b>AB-Nummer</b> (Auftragsbestätigung) oder eine <b>PO-Nummer</b> (Ihre Bestellnummer).</p></div>'
                                              f'<p>Bitte antworten Sie auf diese E-Mail und ergänzen Sie die fehlenden Informationen.</p>'
                                              f'<p>Mit freundlichen Grüßen,</p><p style="white-space: pre-wrap;">{_esc(REPLY_SIGNATURE.splitlines()[1])}</p></div>' )
                                send_reply_email( service, to_addr=to_addr, subject=reply_subject, thread_id=thread_id, in_reply_to_message_id=message_id, text_body=text_body, html_body=html_body )
                                print(f"📧 Sent request for missing AB/PO number to {to_addr}.")
                        if MARK_AS_READ: mark_as_read(service, mid)
                        seen.add(mid); _save_seen_ids(seen); continue

                    # --- Stage 3: Full Unified Analysis (only if checks passed) ---
                    unified = {}
                    if OPENAI_API_KEY:
                        print("🧠 Sending to Unified Case Arbiter...")
                        unified = CT.analyze_unified_case(subject=subject, body=body_text, email_struct=email_struct, pdf_results=pdf_results, image_paths=[str(p) for p in images], api_key=OPENAI_API_KEY, model=OPENAI_MODEL, base_url=OPENAI_BASE_URL)
                        if unified: unified = CT.enforce_image_confirmation_gate(unified, images_count=len(images))

                    envelope = { "gmail": { "id": mid, "subject": subject, "from": from_, "date": date_, "attachments": {"images": [str(p) for p in images], "pdfs": [str(p) for p in pdfs], }, }, "processed": { "email_freeform": email_struct, "pdfs": pdf_results, "unified_case": unified } }
                    out_file = OUTPUT_ROOT / f"email_{mid}.json"; out_file.write_text(json.dumps(envelope, ensure_ascii=False, indent=2), encoding="utf-8")
                    print(f"📄 Full analysis saved to: {out_file}")
                    
                    if AUTO_REPLY and unified and unified.get("per_claim_analysis"):
                        to_addr = _extract_email_address(from_)
                        if not to_addr:
                           print(f"❗️ Could not extract a valid reply-to address from '{from_}'. Skipping replies.")
                           if MARK_AS_READ: mark_as_read(service, mid)
                           seen.add(mid); _save_seen_ids(seen); continue

                        try:
                            # --- Define distinct subjects for internal and client emails ---
                            base_subject = subject or "Ihre Reklamation"
                            client_subject = f"Re: {base_subject}" if not base_subject.lower().startswith("re:") else base_subject
                            internal_subject = f"INTERN: {client_subject}"
                            
                            attachments_to_send = []
                            excel_entries = []
                            print("✨ Generating ERP-style data, attachments, and emails...")

                            all_claims_analysis = unified.get("per_claim_analysis", [])
                            approved_claims = [claim for claim in all_claims_analysis if claim.get("part_verdict") == "approved"]
                            all_parts_approved = len(all_claims_analysis) > 0 and len(all_claims_analysis) == len(approved_claims)
                            
                            first_po = (unified.get("entities", {}).get("purchase_orders", ["N/A"]))[0]
                            contact_info = email_struct.get("contact", {})
                            customer_name = contact_info.get("company") or contact_info.get("name") or "N/A"
                            customer_address = contact_info.get("address") or "N/A"
                            assembly_data_used = unified.get("_meta", {}).get("assembly_data_used", {})

                            verursacher_map = { "transport_damage": "30 - Spedition", "customer_damage": "20 - Kunde", "production_defect": "10 - Lieferant", "unknown": "Neu definieren", }
                            ursache_map = { "missing_part": "208 - Fehlteile", "damaged": "301 - Beschädigt", }
                            
                            cet_tz = pytz.timezone('CET')
                            now_cet = datetime.now(cet_tz)
                            iso_year, iso_week, _ = now_cet.isocalendar()
                            next_week_date = now_cet + timedelta(days=7)
                            _, next_iso_week, _ = next_week_date.isocalendar()
                            
                            for i, claim in enumerate(unified.get("per_claim_analysis", [])):
                                damage_verdict = claim.get("damage_assessment", {}).get("verdict", "unknown")
                                ursache_key = "damaged" if "damage" in damage_verdict else "missing_part"
                                part_ref_val = _infer_part_ref(claim)
                                part_ref_disp = part_ref_val if part_ref_val not in (None, "", 0) else "—"
                                article_no = claim.get('article_no')
                                
                                part_description = f"Teil Nr. {part_ref_disp}"
                                if article_no and article_no in assembly_data_used:
                                    for part_data in assembly_data_used[article_no].get("parts", []):
                                        if str(part_data.get("part_reference")) == str(part_ref_val):
                                            part_description = part_data.get("description", part_description)
                                            break
                                
                                entry = {
                                    "Datum": now_cet.strftime("%d.%m.%Y"),
                                    "Bestellnummer": first_po,
                                    "Kunde": customer_name,
                                    "Pos.": i + 1,
                                    "Artikel / Service": part_ref_disp,
                                    "Artikelbezeichnung": part_description,
                                    "Menge": claim.get("quantity"),
                                    "BFQ - Ursache": ursache_map.get(ursache_key, "301 - Beschädigt"),
                                    "BFQ - Verursacher": verursacher_map.get(damage_verdict, "Neu definieren"),
                                    "Wunsch-KW": f"{iso_week}. KW {iso_year}",
                                    "Bestätigungs-KW": f"{next_iso_week}. KW {iso_year}",
                                    "Rechnungsadresse": customer_address,
                                    "Lieferadresse": customer_address,
                                    "Produkt": article_no,
                                    "Urteil": damage_verdict,
                                }
                                excel_entries.append(entry)

                            # Generate Order Confirmation PDF ONLY if all parts were approved
                            pdf_buffer = None
                            if all_parts_approved:
                                print(f"✅ All {len(approved_claims)} claimed part(s) were approved. Generating Order Confirmation PDF.")
                                pdf_buffer = generate_confirmation_pdf(
                                    approved_claims=approved_claims,
                                    assembly_data_used=assembly_data_used,
                                    contact_info=contact_info,
                                    po_number=first_po
                                )
                                attachments_to_send.append({
                                    "data": pdf_buffer.getvalue(),
                                    "maintype": "application",
                                    "subtype": "pdf",
                                    "filename": "Auftragsbestaetigung.pdf"
                                })
                            else:
                                if len(all_claims_analysis) > 0:
                                    print(f"ℹ️ Not all parts approved ({len(approved_claims)}/{len(all_claims_analysis)}). Skipping PDF Order Confirmation generation.")
                                else:
                                    print("ℹ️ No claims analyzed. Skipping PDF Order Confirmation generation.")
                            
                            # --- THIS IS THE CORRECTED DATABASE LOGIC ---
                            all_claims_db = load_claims_from_json()
                            all_claims_db.extend(excel_entries)
                            save_claims_to_json(all_claims_db)
                            excel_buffer = generate_claims_excel(all_claims_db)
                            attachments_to_send.append({
                                "data": excel_buffer.getvalue(),
                                "maintype": "application",
                                "subtype": "vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                "filename": "Claims_Database_ERP.xlsx"
                            })

                            # --- Send internal email with full data ---
                            text_body, html_body = _summarize_unified(unified)
                            send_reply_email(
                                service, to_addr=to_addr, subject=internal_subject,
                                thread_id=thread_id, in_reply_to_message_id=message_id,
                                text_body=text_body, html_body=html_body,
                                attachments=attachments_to_send
                            )
                            print(f"📧 Replied to {to_addr} with internal analysis (Subject: {internal_subject}).")

                            # --- Send client email with strategic response ---
                            client_body = build_client_email_via_gpt(unified, email_struct, subject or "")
                            client_html = "<div style='font-family: Arial, sans-serif; white-space: pre-wrap; font-size:14px; color:#333;'>" + _esc(client_body) + "</div>"
                            
                            client_attachments = []
                            if pdf_buffer is not None:
                                client_attachments.append({
                                    "data": pdf_buffer.getvalue(),
                                    "maintype": "application",
                                    "subtype": "pdf",
                                    "filename": "Auftragsbestaetigung.pdf"
                                })

                            send_reply_email(
                                service,
                                to_addr=CLIENT_NOTIFICATION_TO,
                                subject=client_subject,
                                thread_id=None,
                                in_reply_to_message_id=None,
                                text_body=client_body,
                                html_body=client_html,
                                attachments=client_attachments
                            )
                            print(f"📧 Sent client-facing email to {CLIENT_NOTIFICATION_TO} (Subject: {client_subject}).")

                        except Exception as e:
                            print(f"❗️ Auto-reply or attachment generation error: {e}")

                    if MARK_AS_READ: mark_as_read(service, mid)
                finally:
                    seen.add(mid); _save_seen_ids(seen)
            time.sleep(POLL_SECONDS)
        except KeyboardInterrupt:
            print("\n👋 Exiting."); break
        except Exception as e:
            print(f"❗️ Watcher error: {e}"); time.sleep(POLL_SECONDS)

if __name__ == "__main__":
    main_loop()