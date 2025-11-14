#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PDF Claim Extractor & JSON-based Triage — with Unified Case Arbiter
====================================================================

What it does
------------
A) Single-PDF mode
   - extract_from_pdf()
   - optional analyze_with_gpt() (verifies parts against assemblies.json)
   - HTML report per PDF (single assembly preview, for simplicity)

B) Library helpers (importable)
   - analyze_email_freeform(): parse email subject/body/images → normalized JSON
   - analyze_unified_case(): unify email + images + PDFs → ONE final verdict
     • Automatically attaches MULTIPLE assembly part lists based on article numbers from assemblies.json
     • Outputs evidence.per_claim_analysis table with detailed findings per part
     • Includes damage_assessment verdict (transport, customer, production) per part
     • Adds _meta.assembly_data_used with the JSON data used for verification

Key details kept:
- Strict Original-PO extraction: digits only, 6–20 → else None
- "Original-\\nPO" line-break fixer
- Missing-part enforcement: any claimed part not in assembly data → missing
- Safer image gate: won't downrank if the model already says images are "consistent"

Env you might set:
- OPENAI_API_KEY, OPENAI_BASE_URL, OPENAI_MODEL (default: gpt-5-chat-latest)
- Assembly data: <script_dir>/assemblies.json (contains article_no -> parts mapping)
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import re
import pathlib
from pathlib import Path
import urllib.request
from dataclasses import dataclass, asdict, field
from typing import List, Optional, Dict, Any, Set

import pdfplumber
from dateutil.parser import parse as parse_dt

# Optional .env
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

# OpenAI (modern client)
try:
    from openai import OpenAI  # type: ignore
except Exception:
    OpenAI = None

# ===== Config / Paths =====
SCRIPT_DIR = pathlib.Path(__file__).resolve().parent

# Assembly data JSON file (NEW)
ASSEMBLY_DATA_JSON = SCRIPT_DIR / "assemblies.json"

# If you previously had a hardcoded key, keep this empty in source control.
HARDCODED_API_KEY = ""  # use env or CLI


# ===== NEW: Assembly Data Loading and Lookup =====
class AssemblyData:
    _instance = None

    def __init__(self):
        self.lookup_map: Dict[str, List[Dict[str, Any]]] = {} # CHANGED: List[int] -> List[Dict]
        self.raw_data: List[Dict] = []
        self._load_data()

    def _load_data(self):
        if not ASSEMBLY_DATA_JSON.exists():
            return
        try:
            with open(ASSEMBLY_DATA_JSON, "r", encoding="utf-8") as f:
                self.raw_data = json.load(f)
            for item in self.raw_data:
                article_no = item.get("article_no", "").strip().upper()
                parts = item.get("parts", [])
                if article_no and parts:
                    self.lookup_map[article_no] = parts
                    for alias in item.get("aliases", []):
                        self.lookup_map[alias.strip().upper()] = parts
        except Exception as e:
            print(f"Error loading assembly data from {ASSEMBLY_DATA_JSON}: {e}")

    def get_parts_for_article(self, article_no: Optional[str]) -> Optional[List[Dict[str, Any]]]: # CHANGED: Return type
        if not article_no:
            return None
        return self.lookup_map.get(article_no.strip().upper())
    
    def get_data_for_article(self, article_no: Optional[str]) -> Optional[Dict]:
        if not article_no: return None
        norm_article = article_no.strip().upper()
        for item in self.raw_data:
            if item.get("article_no", "").strip().upper() == norm_article:
                return item
            for alias in item.get("aliases", []):
                if alias.strip().upper() == norm_article:
                    return item
        return None

def get_assembly_db() -> AssemblyData:
    if AssemblyData._instance is None:
        AssemblyData._instance = AssemblyData()
    return AssemblyData._instance

# ===== Utils =====
def _norm(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    s = s.strip()
    return s or None


def _truncate(s: str, n: int = 180) -> str:
    s = s or ""
    return (s[:n] + "…") if len(s) > n else s


def _normalize_po_breaks(text: str) -> str:
    # Fix line-break splits like "Original-\nPO:"
    return re.sub(r"Original-\s*\n\s*PO", "Original-PO", text, flags=re.IGNORECASE)


def _validate_po(s: Optional[str]) -> Optional[str]:
    """
    Keep only digits; accept if length 6–20; else None.
    Example OK: 4500169211. Example WRONG: 'Original' or 'AB-123'.
    """
    if not s:
        return None
    digits = re.sub(r"\D", "", s)
    return digits if 6 <= len(digits) <= 20 else None


def _find_first(patterns: List[re.Pattern], text: str) -> Optional[str]:
    for pat in patterns:
        m = pat.search(text)
        if m:
            if "val" in m.groupdict():
                return _norm(m.group("val"))
            elif m.groups():
                return _norm(m.group(1))
            else:
                return _norm(m.group(0))
    return None


def _collect_all(patterns: List[re.Pattern], text: str) -> List[str]:
    vals: List[str] = []
    for pat in patterns:
        for m in pat.finditer(text):
            if "val" in m.groupdict():
                vals.append(_norm(m.group("val")) or "")
            elif m.groups():
                vals.append(_norm(m.group(1)) or "")
            else:
                vals.append(_norm(m.group(0)) or "")
    seen, out = set(), []
    for v in vals:
        if v and v not in seen:
            seen.add(v)
            out.append(v)
    return out


# ===== Image URL extraction from text =====
def extract_image_urls(full_text: str) -> List[str]:
    pattern = re.compile(r"(https?://.*?\.(?:jpg|jpeg|png|gif))", flags=re.IGNORECASE | re.DOTALL)
    cleaned = [re.sub(r"\s+", "", hit) for hit in pattern.findall(full_text)]
    seen, out = set(), []
    for u in cleaned:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


# ===== Claimed parts parser =====
def parse_claimed_parts(s: Optional[str]) -> List[Dict[str, int]]:
    if not s:
        return []
    # Match both "1 x 23" and "Teil 23" style claims.
    pairs = re.findall(r"(\d+)\s*x\s*([0-9]+)|(?:teil|part)\s*nr\.?\s*([0-9]+)", s.lower())
    merged: Dict[int, int] = {}
    for q, p, p_only in pairs:
        if p_only: # Matched "Teil Nr. 23"
            pr = int(p_only)
            merged[pr] = merged.get(pr, 0) + 1
        else: # Matched "1 x 23"
            pr = int(p)
            merged[pr] = merged.get(pr, 0) + int(q)
    return [{"part_reference": k, "quantity": v} for k, v in sorted(merged.items())]


# ===== Data model =====
@dataclass
class ClaimRecord:
    vendor: Optional[str] = None
    vendor_address: Optional[str] = None
    deliver_to: Optional[str] = None
    order_number: Optional[str] = None
    order_date: Optional[str] = None
    vat_id: Optional[str] = None
    supplier_id: Optional[str] = None
    email: Optional[str] = None

    original_article_no: Optional[str] = None
    spare_part_description: Optional[str] = None
    reason: Optional[str] = None
    image_urls: List[str] = field(default_factory=list)

    ean: Optional[str] = None
    dimensions: Optional[str] = None
    weight: Optional[str] = None
    qty: Optional[str] = None
    unit: Optional[str] = None
    original_po: Optional[str] = None

    claimed_parts: List[Dict[str, int]] = field(default_factory=list)
    raw_text_sample: Optional[str] = None


# ===== Extraction from PDF text =====
def extract_fields_from_text(text: str) -> ClaimRecord:
    raw_sample = text[:1500]
    text_for_po = _normalize_po_breaks(text)

    order_no_pats = [
        re.compile(r"\bBestellnummer\s*[:\s]*([A-Z0-9]{6,})\b", re.IGNORECASE),
        re.compile(r"\bBestellung\s*[:\s]*([A-Z0-9]{6,})\b", re.IGNORECASE),
        re.compile(r"\b([0-9]{8,})\b(?=.*Bestell)", re.IGNORECASE),
    ]
    date_pats = [
        re.compile(r"\bDatum\s*[:\s]*([0-3]?\d\.[01]?\d\.\d{4})", re.IGNORECASE),
        re.compile(r"\b([0-3]?\d\.[01]?\d\.\d{4})\b"),
    ]
    vat_pats = [
        re.compile(r"USt\.-?IdNr\.?\s*[:\s]*([A-Z]{2}\d+)", re.IGNORECASE),
        re.compile(r"Unsere USt\.-?IdNr\.\s*[:\s]*([A-Z]{2}\d+)", re.IGNORECASE),
    ]
    supplier_pats = [
        re.compile(r"Lieferanten\s*ID\s*[:\s]*([A-Z0-9]+)", re.IGNORECASE),
        re.compile(r"\bSupplier(?:en)?\s*ID\s*[:\s]*([A-Z0-9]+)", re.IGNORECASE),
    ]
    email_pats = [
        re.compile(r"\b[Ee]-?Mail\s*[:\s]*([^\s]+@[^\s]+)", re.IGNORECASE),
        re.compile(r"\b([\w\.-]+@[\w\.-]+\.\w+)\b"),
    ]
    article_pats = [
        re.compile(r"Original\s*Article-?No\.?\s*[:\s]*(?P<val>[A-Z0-9\-_/]+)", re.IGNORECASE),
        re.compile(r"\bArtikelnr\.\s*[:\s]*(?P<val>[A-Z0-9\-_/]+)", re.IGNORECASE),
    ]
    spare_desc_pats = [
        re.compile(r"Spare\s*Part\s*Description\s*:\s*(?P<val>.+?)(?:\n|$)", re.IGNORECASE),
        re.compile(r"Bezeichnung\s*[:\s]*(?P<val>.+?)(?:\n|$)", re.IGNORECASE),
    ]
    reason_pats = [
        re.compile(r"\bGrund\s*:\s*(?P<val>.+?)(?:\n|$)", re.IGNORECASE),
        re.compile(r"\bReason\s*[:\s]*(?P<val>.+?)(?:\n|$)", re.IGNORECASE),
    ]
    ean_pats = [
        re.compile(r"\bEAN\s*:\s*([0-9]+)", re.IGNORECASE),
        re.compile(r"\bEAN\b[^\d]*([0-9]{8,})", re.IGNORECASE),
    ]
    dims_pats = [
        re.compile(r"\b(\d{2,3}\s?x\s?\d{2,3}\s?x\s?\d{2,3}\s?CM)\b", re.IGNORECASE),
        re.compile(r"\bAbmessung[^\n]*?\b([0-9xX\s]+CM)\b", re.IGNORECASE),
        re.compile(r"\bMa(?:ße|sse)\b[^\n]*?\b(\d{2,3}\s?x\s?\d{2,3}\s?x\s?\d{2,3}\s?cm)\b", re.IGNORECASE),
    ]
    weight_pats = [re.compile(r"\b(\d+[.,]?\d*)\s*KG\b", re.IGNORECASE)]
    qty_pats = [
        re.compile(r"\bMenge\s*[:\s]*([0-9]+)\b", re.IGNORECASE),
        re.compile(r"\bGesamt\s*([0-9]+)\s*ST\b", re.IGNORECASE),
    ]
    unit_pats = [
        re.compile(r"\bEinh\.\s*([A-Z]+)\b", re.IGNORECASE),
        re.compile(r"\b(ST)\b", re.IGNORECASE),
        re.compile(r"\bColli\b", re.IGNORECASE),
    ]
    po_pats = [
        re.compile(r"\bOriginal[-\s]*PO\s*[:\-]?\s*(\d{6,20})\b", re.IGNORECASE | re.DOTALL),
        re.compile(r"\bPO\s*[:\-]?\s*(\d{6,20})\b", re.IGNORECASE),
    ]

    order_number = _find_first(order_no_pats, text)
    raw_date_str = _find_first(date_pats, text)
    order_date_iso = None
    if raw_date_str:
        try:
            order_date_iso = parse_dt(raw_date_str, dayfirst=True).date().isoformat()
        except Exception:
            order_date_iso = raw_date_str

    vat_id = _find_first(vat_pats, text)
    supplier_id = _find_first(supplier_pats, text)
    email = _find_first(email_pats, text)
    original_article_no = _find_first(article_pats, text)
    spare_part_description = _find_first(spare_desc_pats, text)
    reason = _find_first(reason_pats, text)
    ean = _find_first(ean_pats, text)
    dims = _find_first(dims_pats, text)

    all_weights = _collect_all(weight_pats, text)
    weight = all_weights[-1] if all_weights else None

    qty = _find_first(qty_pats, text)
    unit = _find_first(unit_pats, text)
    original_po = _find_first(po_pats, text_for_po)
    original_po = _validate_po(original_po)

    image_urls = extract_image_urls(text)
    claimed_parts = parse_claimed_parts(spare_part_description)

    return ClaimRecord(
        order_number=_norm(order_number),
        order_date=order_date_iso,
        vat_id=_norm(vat_id),
        supplier_id=_norm(supplier_id),
        email=_norm(email),
        original_article_no=_norm(original_article_no),
        spare_part_description=_norm(spare_part_description),
        reason=_norm(reason),
        image_urls=image_urls,
        ean=_norm(ean),
        dimensions=_norm(dims),
        weight=_norm(weight),
        qty=_norm(qty),
        unit=_norm(unit),
        original_po=_norm(original_po),
        claimed_parts=claimed_parts,
        raw_text_sample=raw_sample,
    )


def extract_from_pdf(pdf_path: str) -> Dict[str, Any]:
    if not os.path.exists(pdf_path):
        raise FileNotFoundError(f"PDF not found: {pdf_path}")
    pages_text: List[str] = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            txt = page.extract_text(x_tolerance=1.5, y_tolerance=1.5) or ""
            pages_text.append(txt)
    full_text = "\n".join(pages_text)

    record = extract_fields_from_text(full_text)

    vendor_block = deliver_to_block = None
    m_vendor = re.search(r"Firma\s*(.*?)\bLiefern Sie an\b", full_text, flags=re.DOTALL | re.IGNORECASE)
    if m_vendor:
        vendor_block = re.sub(r"\n{2,}", "\n", m_vendor.group(1)).strip()
    m_deliver = re.search(r"\bLiefern Sie an\b\s*(.*?)(?:\n{2,}|Pos\.|$)", full_text, flags=re.DOTALL | re.IGNORECASE)
    if m_deliver:
        deliver_to_block = re.sub(r"\n{2,}", "\n", m_deliver.group(1)).strip()

    record.vendor = _norm((vendor_block or "").split("\n")[0]) or record.vendor
    record.vendor_address = _norm("\n".join((vendor_block or "").split("\n")[1:])) or record.vendor_address
    record.deliver_to = _norm(deliver_to_block) or record.deliver_to

    data = asdict(record)
    data["original_po"] = _validate_po(data.get("original_po"))
    data["full_text"] = full_text
    return data


# ===== I/O helpers =====
def download_images(urls: List[str], out_dir: str) -> List[str]:
    saved: List[str] = []
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    for i, u in enumerate(urls, 1):
        try:
            ext = (u.split("?")[0].split(".")[-1] or "jpg").lower()
            if len(ext) > 4 or "/" in ext:
                ext = "jpg"
            dest = os.path.join(out_dir, f"claim_img_{i}.{ext}")
            urllib.request.urlretrieve(u, dest)
            saved.append(dest)
        except Exception:
            pass
    return saved


def _normalize_base(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    u = url.strip().rstrip("/")
    if not u.endswith("/v1"):
        u = u + "/v1"
    return u


def _encode_local_image_b64(path: str) -> str:
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode("ascii")


# ===== Build GPT messages (PDF triage) =====
def _build_multimodal_messages(extracted: Dict[str, Any],
                               assembly_data: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    system_prompt = (
        "You are an expert furniture claims triage assistant.\n"
        "1) Use the provided ASSEMBLY DATA JSON as the ground truth for valid part numbers and their descriptions.\n"
        "2) Verify the claimed parts & quantities against this data.\n"
        "3) Inspect claim photos for damage patterns.\n"
        "4) Compare with stated reason and output STRICT JSON with:\n"
        '{ "parts_verification": { "exists": bool, "claimed": [...], "found_in_diagram": [...], '
        '"missing_or_mismatch": [...], "comments": "" }, "image_findings": [...], '
        '"reason_consistency": "consistent|partially_consistent|inconsistent", '
        '"verdict": "transport_damage|customer_damage|production_defect|unknown", '
        '"confidence": 0.0-1.0, "rationale": "" }'
    )

    user_block = {
        "role": "user",
        "content": [
            {
                "type": "text",
                "text": "Extracted fields JSON:\n" + json.dumps(
                    {
                        "order_number": extracted.get("order_number"),
                        "order_date": extracted.get("order_date"),
                        "original_article_no": extracted.get("original_article_no"),
                        "spare_part_description": extracted.get("spare_part_description"),
                        "claimed_parts": extracted.get("claimed_parts", []),
                        "reason": extracted.get("reason"),
                        "image_urls": extracted.get("image_urls", []),
                        "original_po": extracted.get("original_po"),
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
            }
        ],
    }
    
    if assembly_data:
        user_block["content"][0]["text"] += "\n\nASSEMBLY DATA (ground truth for part numbers):\n" + json.dumps(assembly_data, ensure_ascii=False, indent=2)

    for url in extracted.get("image_urls", []):
        user_block["content"].append({"type": "image_url", "image_url": {"url": url}})

    return [{"role": "system", "content": system_prompt}, user_block]


# ===== Build GPT messages (Email freeform) =====
def _build_email_messages(subject: str,
                          body: str,
                          image_paths: List[str]) -> List[Dict[str, Any]]:
    system = (
        "You are a precise inbox triage parser for furniture B2B support.\n"
        "Goal: Normalize freeform EMAIL SUBJECT and BODY into STRICT JSON. "
        "German (DE) or English (EN) possible. Do not invent data.\n\n"
        "Output JSON schema:\n"
        "{\n"
        '  "language": "de|en|other",\n'
        '  "request_types": ["price_quote" | "missing_part" | "damage_claim" | "general_question" | "other", ...],\n'
        '  "has_product_images": boolean, \n' # --- NEW ---
        '  "ab_numbers": [string, ...],\n'
        '  "purchase_orders": [string, ...],\n'
        '  "articles": [{"article_no": string|null, "program": string|null, "variant": string|null}],\n'
        '  "claimed_parts": [{"part_reference": int|null, "quantity": int|null, "notes": string|null}],\n'
        '  "issue_summary": string,\n'
        '  "contact": {"name": string|null, "company": string|null, "phone": string|null, "email": string|null, "address": string|null}\n'
        "}\n\n"
        "- Extract AB-Nr. and any PO numbers (digits only, 6–20) separately.\n"
        "- Normalize integers when obvious (e.g., Teil 23 -> part_reference=23).\n"
        # --- NEW INSTRUCTION ---
        "- Critically evaluate attached images. Set `has_product_images` to `true` ONLY if they show furniture, parts, damage, or assembly instructions. Set to `false` for logos, icons, or signatures.\n"
        "- If an image suggests damage, you MAY add a brief note in claimed_parts[].notes, but never speculate.\n"
        "- Always fill request_types (can be multiple). Use nulls for unknowns."
    )

    user_content = [
        {"type": "text", "text": f"EMAIL SUBJECT:\n{subject or ''}\n\nEMAIL BODY:\n{body or ''}"}
    ]

    # Attach up to 8 images
    for p in (image_paths or [])[:8]:
        try:
            b64 = _encode_local_image_b64(p)
            user_content.append({"type": "image_url", "image_url": {"url": f"data:image/*;base64,{b64}"}})
        except Exception:
            pass

    return [{"role": "system", "content": system}, {"role": "user", "content": user_content}]


# ===== GPT client =====
def _openai_client(api_key: Optional[str], base_url: Optional[str]):
    key = api_key or HARDCODED_API_KEY or os.getenv("OPENAI_API_KEY")
    if not key:
        return None
    if OpenAI is None:
        raise RuntimeError("openai package not installed or too old. Run: pip install -U openai")
    client_kwargs = {"api_key": key}
    norm_base = _normalize_base(base_url or os.getenv("OPENAI_BASE_URL"))
    if norm_base:
        client_kwargs["base_url"] = norm_base
    return OpenAI(**client_kwargs)


# ===== Multi-assembly data resolver (by article numbers) =====
def _gather_article_numbers(email_struct: Optional[Dict[str, Any]],
                            pdf_results: Optional[List[Dict[str, Any]]]) -> List[str]:
    arts: Set[str] = set()
    if email_struct:
        for a in (email_struct.get("articles") or []):
            val = (a or {}).get("article_no")
            if val:
                arts.add(str(val).strip().upper())
    for pr in (pdf_results or []):
        ex = pr.get("extracted_json") or {}
        a1 = ex.get("original_article_no")
        if a1:
            arts.add(str(a1).strip().upper())
    return sorted(list(arts))

def _resolve_assembly_data_for_unified(email_struct: Optional[Dict[str, Any]],
                                        pdf_results: Optional[List[Dict[str, Any]]]) -> Dict[str, Any]:
    """
    Main resolver used by analyze_unified_case():
    1) Collect article numbers from email + PDFs.
    2) For each article, look it up in assemblies.json.
    3) Return a dictionary of the found assembly data.
    """
    found_data: Dict[str, Any] = {}
    db = get_assembly_db()
    for art_code in _gather_article_numbers(email_struct, pdf_results):
        data = db.get_data_for_article(art_code)
        if data:
            # Use the canonical article_no as the key
            canonical_no = data.get("article_no")
            if canonical_no and canonical_no not in found_data:
                found_data[canonical_no] = data
    return found_data


# ===== GPT calls =====
def analyze_email_freeform(subject: str,
                           body: str,
                           image_paths: List[str] | None = None,
                           api_key: Optional[str] = None,
                           model: str = None,
                           base_url: Optional[str] = None) -> Optional[dict]:
    client = _openai_client(api_key, base_url)
    if not client:
        return None
    messages = _build_email_messages(subject, body, image_paths or [])
    resp = client.chat.completions.create(
        model=model or os.getenv("OPENAI_MODEL", "gpt-5-chat-latest"),
        messages=messages,
        temperature=0.1,
        response_format={"type": "json_object"},
    )
    raw = resp.choices[0].message.content
    try:
        return json.loads(raw)
    except Exception:
        return {"raw": raw, "parse_error": True}


def analyze_with_gpt(extracted: Dict[str, Any],
                     api_key: Optional[str] = None,
                     model: str = None,
                     base_url: Optional[str] = None) -> Optional[str]:
    client = _openai_client(api_key, base_url)
    if not client:
        return None
    
    db = get_assembly_db()
    article_no = extracted.get("original_article_no")
    assembly_data = db.get_data_for_article(article_no)
    
    messages = _build_multimodal_messages(extracted, assembly_data)
    resp = client.chat.completions.create(
        model=model or os.getenv("OPENAI_MODEL", "gpt-5-chat-latest"),
        messages=messages,
        temperature=0.1,
        response_format={"type": "json_object"},
    )
    raw = resp.choices[0].message.content
    return enforce_missing_policy(extracted, raw, assembly_data is not None)


# ===== Safer image confirmation gate =====
def enforce_image_confirmation_gate(unified: Dict[str, Any], images_count: int) -> Dict[str, Any]:
    """
    If images do not clearly confirm the claim, force the final verdict to a "cannot proceed" state.
    This gate acts on the 'overall_image_consistency' field from the unified analysis.
    """
    if not unified or not isinstance(unified, dict):
        return unified

    # Trust the model's detailed per-part analysis if it exists.
    # The gate is a fallback for when overall consistency is low.
    if unified.get("per_claim_analysis"):
         # Check if any part is still pending due to images
         is_pending_image = any(
             (p.get("image_check") or {}).get("status") == "insufficient_evidence"
             for p in unified.get("per_claim_analysis", [])
         )
         if not is_pending_image:
             return unified # Detailed analysis is complete, no need to override

    img_cons = (unified.get("overall_image_consistency") or "").strip().lower()

    if img_cons == "consistent":
        return unified  # Trust the upstream analysis if it's confident

    downgrade_for_consistency = img_cons in {"inconsistent", "insufficient_evidence", "partially_consistent"}
    downgrade_for_no_attachments = (images_count == 0) and (img_cons in {"insufficient_evidence", "not_applicable", ""})

    if downgrade_for_consistency or downgrade_for_no_attachments:
        out = dict(unified)
        out["overall_verdict"] = "pending_insufficient_evidence"
        try:
            conf = float(out.get("confidence", 0.5))
        except Exception:
            conf = 0.5
        out["confidence"] = min(conf, 0.35)
        out["recommended_action"] = (
            "Bitte senden Sie passende Fotos zum betroffenen Produkt: "
            "1) Gesamtansicht mit erkennbarer Typ/Programm-Zuordnung, "
            "2) Montage-/Explosionszeichnung mit markiertem Teil, "
            "3) Nahaufnahme des Schadens, "
            "4) Etikett/AB-Nr. "
            "Ohne passende Bilder können wir nicht fortfahren."
        )
        # Clear detailed analysis if we are overriding with a generic request
        if "per_claim_analysis" in out:
            out["per_claim_analysis"] = []
    return unified


# ===== Enforce missing policy on per-PDF triage result =====
def enforce_missing_policy(extracted: Dict[str, Any], analysis_json: Optional[str], has_assembly_data: bool) -> str:
    """
    Rule: If a claimed part is NOT found in the assembly sheet, mark it as missing.
    Accepts GPT outputs where found_in_diagram/missing_or_mismatch items can be dicts or bare ints/strings.
    If GPT JSON is invalid, synthesize a minimal JSON and mark all claimed parts as missing.
    """
    claimed = extracted.get("claimed_parts") or []
    claimed_refs: set[int] = set()
    for x in claimed:
        pr = x.get("part_reference") if isinstance(x, dict) else x
        if isinstance(pr, (int, str)) and str(pr).isdigit():
            claimed_refs.add(int(pr))

    try:
        a = json.loads(analysis_json or "")
    except Exception:
        a = None

    NO_ASSUMPTION_COMMENT = "Missing in provided assembly data (no assumption)."

    if not a:
        missing = []
        if has_assembly_data:
            for ref in sorted(claimed_refs):
                missing.append({
                    "part_reference": ref,
                    "expected_info": "Part not present in assembly data",
                    "comment": NO_ASSUMPTION_COMMENT
                })
        synthesized = {
            "parts_verification": {
                "exists": False if missing else True,
                "claimed": claimed,
                "found_in_diagram": [],
                "missing_or_mismatch": missing,
                "comments": "Synthesized due to invalid GPT JSON; enforced missing policy applied."
            },
            "image_findings": [],
            "reason_consistency": "unknown",
            "verdict": "unknown",
            "confidence": 0.3 if missing else 0.2,
            "rationale": "Fallback: could not parse GPT output. Claimed parts marked missing because they were not confirmed in the assembly data."
        }
        return json.dumps(synthesized, ensure_ascii=False)

    pv = a.setdefault("parts_verification", {})

    found_refs: set[int] = set()
    for item in pv.get("found_in_diagram") or []:
        pr = item.get("part_reference") if isinstance(item, dict) else item
        if isinstance(pr, (int, str)) and str(pr).isdigit():
            found_refs.add(int(pr))

    aut_missing: List[Dict[str, Any]] = []
    if has_assembly_data:
        for ref in claimed_refs:
            if ref not in found_refs:
                aut_missing.append({
                    "part_reference": ref,
                    "expected_info": "Part not present in assembly data",
                    "comment": NO_ASSUMPTION_COMMENT
                })

    existing_list = pv.get("missing_or_mismatch") or []
    normalized_existing: List[Dict[str, Any]] = []
    for m in existing_list:
        if isinstance(m, dict):
            pr = m.get("part_reference")
            if isinstance(pr, (int, str)) and str(pr).isdigit():
                pr = int(pr)
            normalized_existing.append({
                "part_reference": pr,
                "expected_info": m.get("expected_info") or "—",
                "comment": m.get("comment") or ""
            })
        elif isinstance(m, (int, str)) and str(m).isdigit():
            normalized_existing.append({
                "part_reference": int(m),
                "expected_info": "Part not present in assembly data",
                "comment": NO_ASSUMPTION_COMMENT
            })

    seen = {(e.get("part_reference"), e.get("expected_info"), e.get("comment")) for e in normalized_existing}
    for m in aut_missing:
        key = (m["part_reference"], m["expected_info"], m["comment"])
        if key not in seen:
            normalized_existing.append(m)
            seen.add(key)

    pv["missing_or_mismatch"] = normalized_existing
    pv["exists"] = False if (has_assembly_data and normalized_existing) else pv.get("exists", True)
    if "claimed" not in pv or not pv["claimed"]:
        pv["claimed"] = claimed
    return json.dumps(a, ensure_ascii=False)


# ===== Unified case analyzer (MULTI-assembly) =====
def _build_unified_case_messages(subject: str,
                                 body: str,
                                 email_struct: Dict[str, Any] | None,
                                 pdf_results: List[Dict[str, Any]] | None,
                                 image_paths: List[str] | None,
                                 assembly_data: Dict[str, Any]) -> List[Dict[str, Any]]:

    system = (
        "Sie sind ein AKRIBISCHER und EXPERTISierter Senior-Sachbearbeiter für Reklamationen in einem Möbelunternehmen. Ihre Aufgabe ist es, alle bereitgestellten Informationen (E-Mail, PDFs, Bilder, Montagedaten) in einer einzigen, strukturierten und endgültigen JSON-Analyse zu konsolidieren. **Die GESAMTE JSON-Ausgabe, einschließlich aller Freitextfelder wie `summary`, `comment`, `justification` und `recommended_action`, MUSS AUF DEUTSCH sein.**\n\n"
        "**VORGEHENSWEISE:**\n"
        "1.  **Daten konsolidieren:** Sammeln Sie alle reklamierten Teilenummern, Mengen und zugehörigen Artikelnummern aus allen Quellen (E-Mail-Text, geparstes E-Mail-JSON, PDF-Extrakte). Erstellen Sie eine endgültige Liste einzigartiger Reklamationen zur Analyse.\n"
        "2.  **Analyse pro Reklamation:** Für JEDES reklamierte Teil müssen Sie eine strenge dreistufige Überprüfung durchführen:\n"
        "    a. **Schritt 1: Montage-Prüfung.** Identifizieren Sie die Artikelnummer für das reklamierte Teil. Suchen Sie die Daten im `ASSEMBLY DATA` JSON. Überprüfen Sie, ob die reklamierte `part_reference` in der `parts`-Liste für diesen Artikel existiert. **ENTSCHEIDEND: Nutzen Sie die `description` und den `type` ('structural' oder 'fitting') des Teils, um die freitextliche Problembeschreibung des Kunden zu validieren.** Vermerken Sie diese Übereinstimmung in Ihrem Kommentar.\n"
        "    b. **Schritt 2: Bild-Prüfung.** Untersuchen Sie die vom Benutzer bereitgestellten Reklamationsfotos. Zeigen sie das betreffende Teil? Stimmen die sichtbaren Beweise mit dem angegebenen Reklamationsgrund überein? Geben Sie eine prägnante, aber spezifische Beobachtung ab.\n"
        "    c. **Schritt 3: Schadensbewertung & Urteil.** DIES IST KRITISCH. Analysieren Sie für JEDES Teil den vom Kunden angegebenen Grund (aus E-Mail-Text oder PDF-Feld 'reason') und die Fotobeweise. Bestimmen Sie auf dieser Grundlage die wahrscheinliche Schadensursache. Schlüsselwörter sind wichtig: 'beim Aufbauen', 'beim Auspacken', 'runtergefallen' deuten auf **Kundenschaden (customer_damage)** hin. 'Karton beschädigt', 'Paket aufgerissen' deuten auf **Transportschaden (transport_damage)** hin. 'falsche Bohrung', 'Farbe fehlt', 'passt nicht' deuten auf **Produktionsfehler (production_defect)** hin. Wenn der Grund unklar ist oder keine Beweise vorliegen, verwenden Sie 'unknown'.\n"
        "3.  **Synthese und Entscheidung:** Weisen Sie auf der Grundlage aller drei Überprüfungsschritte ein endgültiges, spezifisches Urteil FÜR DIESES TEIL zu.\n"
        "4.  **Finale Zusammenfassung:** Geben Sie nach der Analyse aller Teile eine Gesamtzusammenfassung, ein Gesamturteil und eine klare, umsetzbare Handlungsempfehlung für den Sachbearbeiter ab.\n\n"
        "**NUR STRIKTES JSON AUSGEBEN:** Halten Sie sich strikt an das folgende Schema. Fügen Sie keine zusätzlichen Kommentare außerhalb des JSON hinzu.\n"
        "{\n"
        '  "summary": "Eine 1-2 Sätze umfassende, übergeordnete Zusammenfassung des gesamten Falls auf Deutsch.",\n'
        '  "entities": {\n'
        '    "ab_numbers": ["string", ...],\n'
        '    "purchase_orders": ["string", ...],\n'
        '    "articles": [{"article_no": "string|null", "source": "email|pdf"}]\n'
        "  },\n"
        '  "per_claim_analysis": [\n'
        "    {\n"
        '      "part_reference": "int",\n'
        '      "quantity": "int",\n'
        '      "article_no": "string|null",\n'
        '      "assembly_check": {\n'
        '        "status": "found|not_found",\n'
        '        "comment": "string" // z.B., "✅ Teil 39 (Türpaneel, mit Glas, strukturell) gefunden, stimmt mit der Nennung von \\"Tür\\" durch den Kunden überein." oder "✅ Teil 1018 (Holzdübel, Beschlag) gefunden, stimmt mit der Reklamation \\"fehlende Dübel\\" überein."\n'
        "      },\n"
        '      "image_check": {\n'
        '        "status": "consistent|inconsistent|insufficient_evidence|not_applicable",\n'
        '        "observation": "string"\n'
        "      },\n"
        '      "damage_assessment": { \n'
        '        "verdict": "transport_damage|customer_damage|production_defect|unknown",\n'
        '        "justification": "string" // z.B., "Kunde gibt an, dass der Karton bei Ankunft beschädigt war." oder "Foto zeigt einen sauberen Bruch, der auf einen Aufprall hindeutet, aber der Grund fehlt."\n'
        "      },\n"
        '      "part_verdict": "approved|rejected|pending_more_info"\n'
        "    }\n"
        "  ],\n"
        '  "overall_image_consistency": "consistent|partially_consistent|inconsistent|insufficient_evidence|not_applicable",\n'
        '  "overall_verdict": "approved_in_full|partially_approved|rejected_in_full|needs_clarification",\n'
        '  "confidence": "float",\n'
        '  "recommended_action": "string" // Eine klare, umsetzbare nächste Aktion auf Deutsch.\n'
        "}\n"
    )

    compact_pdfs = []
    for pr in (pdf_results or []):
        compact_pdfs.append({
            "pdf_path": pr.get("pdf_path"),
            "extracted_json": pr.get("extracted_json") or {},
            "gpt_analysis": pr.get("gpt_analysis") or {}
        })

    content = [
        {"type": "text", "text": "EMAIL SUBJECT:\n" + (subject or "")},
        {"type": "text", "text": "EMAIL BODY:\n" + (body or "")},
        {"type": "text", "text": "EMAIL PARSED JSON:\n" + json.dumps(email_struct or {}, ensure_ascii=False, indent=2)},
        {"type": "text", "text": "PDFS (extracted + analyses):\n" + json.dumps(compact_pdfs, ensure_ascii=False, indent=2)},
        {"type": "text", "text": "ASSEMBLY DATA (ground truth for part numbers):\n" + json.dumps(assembly_data or {}, ensure_ascii=False, indent=2)}
    ]

    # Attach user-provided claim photos
    for p in (image_paths or [])[:8]:
        try:
            b64 = _encode_local_image_b64(p)
            content.append({"type": "image_url", "image_url": {"url": f"data:image/*;base64,{b64}"}})
        except Exception:
            pass

    return [{"role": "system", "content": system}, {"role": "user", "content": content}]


def analyze_unified_case(subject: str,
                         body: str,
                         email_struct: Dict[str, Any] | None,
                         pdf_results: List[Dict[str, Any]] | None,
                         image_paths: List[str] | None,
                         api_key: Optional[str] = None,
                         model: str = None,
                         base_url: Optional[str] = None) -> Optional[dict]:
    """
    Unified flow: automatically resolves MULTIPLE assembly part lists using article numbers from
    the parsed email + each PDF via assemblies.json.
    """
    client = _openai_client(api_key, base_url)
    if not client:
        return None

    assembly_data = _resolve_assembly_data_for_unified(email_struct, pdf_results)

    messages = _build_unified_case_messages(
        subject=subject,
        body=body,
        email_struct=email_struct,
        pdf_results=pdf_results or [],
        image_paths=image_paths or [],
        assembly_data=assembly_data
    )
    resp = client.chat.completions.create(
        model=model or os.getenv("OPENAI_MODEL", "gpt-5-chat-latest"),
        messages=messages,
        temperature=0.05, # Lower temperature for more deterministic, structured output
        response_format={"type": "json_object"},
    )
    raw = resp.choices[0].message.content
    try:
        out = json.loads(raw)
    except Exception:
        out = {"raw": raw, "parse_error": True}

    # Always include which assembly data we actually used
    out.setdefault("_meta", {})["assembly_data_used"] = assembly_data
    return out


# ===== HTML report (single-PDF mode) =====
def _esc(s: Optional[str]) -> str:
    if s is None:
        return ""
    return (s.replace("&", "&amp;")
             .replace("<", "&lt;")
             .replace(">", "&gt;")
             .replace('"', "&quot;")
             .replace("'", "&#39;"))


def render_html_report(extracted: Dict[str, Any], analysis_json: Optional[str], out_path: str) -> None:
    verdict = "—"
    confidence = ""
    rationale = ""
    reason = _esc(extracted.get("reason"))
    reason_consistency = "—"

    # Resolve assembly data based on extracted article number
    db = get_assembly_db()
    article_no = extracted.get("original_article_no")
    assembly_data = db.get_data_for_article(article_no)

    parts_rows = ""
    mismatch_rows = "" # This will now be the main detail table
    photo_rows = ""

    extracted_claimed = extracted.get("claimed_parts") or []
    if extracted_claimed:
        for item in extracted_claimed:
            pr = item.get("part_reference") if isinstance(item, dict) else item
            qty = item.get("quantity") if isinstance(item, dict) else ""
            parts_rows += f"<tr><td>{_esc(str(pr))}</td><td>{_esc(str(qty))}</td></tr>"

    a = None
    if analysis_json:
        try:
            a = json.loads(analysis_json)
        except Exception:
            a = None
            rationale = "(Could not parse GPT JSON response.)"

    if a:
        verdict = a.get("overall_verdict", "unknown")
        confidence = f'{a.get("confidence", "")}'
        rationale = a.get("recommended_action", "") or rationale
        
        per_claim = a.get("per_claim_analysis") or []
        for item in per_claim:
            pr = _esc(str(item.get("part_reference")))
            qty = _esc(str(item.get("quantity")))
            article = _esc(item.get("article_no", ''))
            
            asm_check = item.get("assembly_check", {})
            asm_status_raw = asm_check.get("status")
            asm_status = "✅ Found" if asm_status_raw == "found" else "❌ Not Found"
            asm_comment = _esc(asm_check.get("comment", ''))

            img_check = item.get("image_check", {})
            img_obs = _esc(img_check.get("observation", ''))

            damage_assess = item.get("damage_assessment", {})
            damage_verdict = _esc(damage_assess.get("verdict", ''))
            damage_just = _esc(damage_assess.get("justification", ''))

            part_verdict = _esc(item.get("part_verdict", ''))
            
            mismatch_rows += f"""<tr>
                <td><b>{pr}</b> (x{qty})<br/><small>{article}</small></td>
                <td>{asm_status}<br/><small>{asm_comment}</small></td>
                <td>{img_obs}</td>
                <td><b>{damage_verdict.replace('_', ' ').title()}</b><br/><small>{damage_just}</small></td>
                <td>{part_verdict.upper()}</td>
            </tr>"""
            
        findings = a.get("image_findings") or [] # This part might be obsolete now but we keep it
        if isinstance(findings, list):
            # ... (photo findings logic remains the same)
            urls = extracted.get("image_urls") or []
            for idx, item in enumerate(findings, 1):
                if isinstance(item, dict):
                    pidx = item.get("photo_index", idx)
                    obs_full = item.get("observations") or ""
                else:
                    pidx = idx
                    obs_full = str(item)
                zero_based = (pidx - 1) if isinstance(pidx, int) and pidx > 0 else 0
                url = urls[zero_based] if 0 <= zero_based < len(urls) else None
                obs_short = _truncate(obs_full, 180)
                thumb = (f'<img class="findings-preview" src="{_esc(url)}" alt="photo {pidx}" />' if url else "")
                photo_rows += (
                    f'<tr><td style="width:56px">{_esc(str(pidx))}</td>'
                    f'<td style="width:120px">{thumb}</td>'
                    f'<td class="obs" title="{_esc(obs_full)}">{_esc(obs_short)}</td></tr>'
                )

    style = """
    <style>
    :root { --bg:#0b0f17; --panel:#121826; --muted:#8aa0b5; --text:#e9eff6; --green:#1db954; --amber:#f5a524; --red:#ef4444; --blue:#3b82f6;}
    body { margin:0; font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial; background:var(--bg); color:var(--text);}
    .wrap { max-width: 1200px; margin: 32px auto; padding: 0 16px;}
    .card { background:var(--panel); border-radius:16px; padding:20px; box-shadow: 0 6px 18px rgba(0,0,0,.25); margin-bottom:18px;}
    h1,h2,h3 { margin: 0 0 10px 0; }
    table { width:100%; border-collapse: collapse; table-layout: fixed; }
    th, td { text-align:left; padding:10px 8px; border-bottom:1px solid #223048; vertical-align: top; word-wrap: break-word;}
    th { color: var(--muted); font-size: 0.9em; text-transform: uppercase; }
    td small { color: var(--muted); }
    .grid { display:grid; grid-template-columns: 1fr 1fr; gap:16px; }
    .badge { display:inline-block; padding:6px 10px; border-radius:999px; font-weight:600; }
    .b-approved { background:rgba(29,185,84,.15); color:var(--green); }
    .b-rejected { background:rgba(239,68,68,.15); color:var(--red); }
    .b-pending { background:rgba(245,165,36,.15); color:var(--amber); }
    .thumbs img { height:96px; border-radius:10px; margin-right:10px; border:1px solid #223048; }
    pre.json { background:#0b1323; border:1px solid #223048; padding:12px; border-radius:10px; overflow:auto; }
    .findings-preview { height:72px; width:auto; border-radius:8px; border:1px solid #223048; }
    </style>
    """
    
    verdict_class_map = {
        "fully_approved": "b-approved",
        "partially_approved": "b-approved",
        "rejected": "b-rejected",
    }
    verdict_class = verdict_class_map.get(verdict, "b-pending")

    assembly_html = ""
    if assembly_data:
        parts_list = "\n".join([f"- Part {p.get('part_reference')} ({p.get('type', '')}): {p.get('description')}" for p in assembly_data.get("parts", [])])
        assembly_html = f"""
        <h4>Article: {_esc(assembly_data.get('article_no'))}</h4>
        <p class="muted">Valid Parts & Descriptions from JSON:</p>
        <pre class="json" style="white-space: pre-wrap; word-break: break-all;">{_esc(parts_list)}</pre>
        """
    else:
        assembly_html = '<p class="muted">No assembly data found for this article number.</p>'


    claim_photos_html = ''.join(
        f'<a href="{_esc(u)}" target="_blank"><img src="{_esc(u)}" alt="photo {i}" /></a>'
        for i, u in enumerate(extracted.get("image_urls", []), 1)
    ) or '<p class="muted">No photos.</p>'
    
    html = f"""<!DOCTYPE html>
<html lang="en">
<meta charset="utf-8" /><title>Claim Triage Report</title>{style}
<body>
<div class="wrap">
  <div class="card">
    <h1>Claim Triage Report</h1>
    <div class="grid">
      <div><table>
        <tr><th>Order #</th><td>{_esc(extracted.get("order_number"))}</td></tr>
        <tr><th>Article #</th><td>{_esc(extracted.get("original_article_no"))}</td></tr>
        <tr><th>Original PO</th><td>{_esc(extracted.get("original_po") or "—")}</td></tr>
      </table></div>
      <div><table>
        <tr><th>Vendor</th><td>{_esc(extracted.get("vendor"))}</td></tr>
        <tr><th>Date</th><td>{_esc(extracted.get("order_date"))}</td></tr>
        <tr><th>EAN</th><td>{_esc(extracted.get("ean"))}</td></tr>
      </table></div>
    </div>
  </div>
  <div class="card">
    <h2>Verdict</h2>
    <p>
      <span class="badge {verdict_class}">{_esc(verdict.replace("_", " ").title())}</span>
      &nbsp;&nbsp;<span class="muted">confidence:</span> {_esc(confidence)}
    </p>
    <p><span class="muted">Recommended Action:</span> {_esc(rationale)}</p>
  </div>
  <div class="card">
    <h2>Detailed Claim Analysis</h2>
    <table>
      <tr>
        <th style="width:15%">Part / Article</th>
        <th style="width:20%">Assembly Check</th>
        <th style="width:30%">Image Observation</th>
        <th style="width:20%">Damage Assessment</th>
        <th style="width:15%">Part Verdict</th>
      </tr>
      {mismatch_rows or '<tr><td colspan="5" class="muted">No detailed analysis available.</td></tr>'}
    </table>
  </div>
  <div class="card">
    <h2>Assembly Data</h2>
    {assembly_html}
  </div>
  <div class="card">
    <h2>Claim Photos</h2>
    <div class="thumbs">{claim_photos_html}</div>
  </div>
  <div class="card">
    <details><summary>Raw Extracted JSON</summary><pre class="json">{_esc(json.dumps(extracted, ensure_ascii=False, indent=2))}</pre></details>
    <details><summary>Raw GPT Analysis JSON</summary><pre class="json">{_esc(json.dumps(a, ensure_ascii=False, indent=2) if a else 'N/A')}</pre></details>
  </div>
</div></body></html>"""

    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)


# ===== CLI =====
def main():
    ap = argparse.ArgumentParser(description="Extract claim/order data, analyze with GPT, and optionally render HTML.")
    ap.add_argument("--pdf", help="Path to input PDF")
    ap.add_argument("--json", default=None, help="Path to write extracted JSON (single-PDF mode)")
    ap.add_argument("--gpt", action="store_true", help="Send extracted info to GPT for verdict / email parsing")
    ap.add_argument("--api-key", default=None, help="OpenAI API key (or set env OPENAI_API_KEY)")
    ap.add_argument("--base-url", default=os.getenv("OPENAI_BASE_URL"), help="Optional custom base URL")
    ap.add_argument("--model", default=os.getenv("OPENAI_MODEL", "gpt-5-chat-latest"), help="Model name (vision-capable)")
    ap.add_argument("--download-images", default=None, help="Folder to save extracted claim images (single-PDF mode)")
    ap.add_argument("--html", default=None, help="Write an HTML report to this path (single-PDF mode)")
    # The --assembly argument is no longer needed but kept for backward compatibility to avoid errors. It is ignored.
    ap.add_argument("--assembly", help=argparse.SUPPRESS)
    args = ap.parse_args()

    if not args.pdf:
        # Allow running as a library without CLI args
        if __name__ == "__main__":
             ap.print_help()
        return

    extracted = extract_from_pdf(args.pdf)

    if args.download_images:
        saved = download_images(extracted.get("image_urls", []), args.download_images)
        extracted["downloaded_images"] = saved

    if args.json:
        with open(args.json, "w", encoding="utf-8") as f:
            json.dump(extracted, f, ensure_ascii=False, indent=2)

    print("# Extracted JSON")
    print(json.dumps(extracted, ensure_ascii=False, indent=2))

    analysis = None
    if args.gpt:
        analysis = analyze_with_gpt(
            extracted,
            api_key=args.api_key,
            model=args.model,
            base_url=args.base_url,
        )
        if analysis:
            print("\n# GPT Analysis")
            # Pretty print the JSON string
            try:
                parsed_analysis = json.loads(analysis)
                print(json.dumps(parsed_analysis, ensure_ascii=False, indent=2))
            except json.JSONDecodeError:
                print(analysis)


    if args.html:
        render_html_report(extracted, analysis, args.html)
        print(f"\n# HTML report written to: {args.html}")


if __name__ == "__main__":
    main()