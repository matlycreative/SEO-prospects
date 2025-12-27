#!/usr/bin/env python3
"""
Gmail → Trello reply sync

- Polls Gmail IMAP for UNSEEN messages (INBOX).
- For each email, finds Trello card(s) whose description includes a line like "Email: someone@example.com".
- Moves matching card(s) to a target list and APPENDS the email (Subject/Body) to the description (at the bottom).
- Optionally writes a Cloudflare R2 delete-marker (so you can auto-delete samples later) if all R2 env vars are present.

Env (required):
  IMAP_USER, IMAP_PASS                     # Gmail address + App Password (IMAP enabled)
  IMAP_HOST (default: imap.gmail.com)
  IMAP_PORT (default: 993)

  TRELLO_KEY, TRELLO_TOKEN
  TRELLO_BOARD_ID                          # Board to scan (all lists)
  TRELLO_DEST_LIST_ID                      # List to move the card into (when a match is found)

Optional:
  MAX_EMAILS_PER_RUN (default: 20)
  BODY_MAX_CHARS (default: 4000)

Optional (R2 delete marker — if present, a marker is created once per sender upon first reply):
  R2_ACCOUNT_ID
  R2_ACCESS_KEY_ID
  R2_SECRET_ACCESS_KEY
  R2_BUCKET_NAME or R2_BUCKET
  R2_MARKER_PREFIX (default: delete_markers)
  R2_DELETE_AFTER_DAYS (default: 30)
"""

import os, re, time, json, html, email, imaplib
from email.header import decode_header, make_header
from email.message import Message
from datetime import datetime, timedelta
import requests

def log(*a): print(*a, flush=True)

# ---------- Env ----------
def _get_env(*names, default=""):
    for n in names:
        v = os.getenv(n)
        if v is not None and str(v).strip():
            return str(v).strip()
    return default

IMAP_USER = _get_env("IMAP_USER")
IMAP_PASS = _get_env("IMAP_PASS")
IMAP_HOST = _get_env("IMAP_HOST", default="imap.gmail.com")
IMAP_PORT = int(_get_env("IMAP_PORT", default="993"))

TRELLO_KEY        = _get_env("TRELLO_KEY")
TRELLO_TOKEN      = _get_env("TRELLO_TOKEN")
TRELLO_BOARD_ID   = _get_env("TRELLO_BOARD_ID")
DEST_LIST_ID      = _get_env("TRELLO_DEST_LIST_ID")

MAX_EMAILS_PER_RUN = int(_get_env("MAX_EMAILS_PER_RUN", default="20"))
BODY_MAX_CHARS     = int(_get_env("BODY_MAX_CHARS", default="4000"))

# Optional R2 (for delete markers)
R2_ACCOUNT_ID        = _get_env("R2_ACCOUNT_ID", "CF_R2_ACCOUNT_ID")
R2_ACCESS_KEY_ID     = _get_env("R2_ACCESS_KEY_ID", "CF_R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = _get_env("R2_SECRET_ACCESS_KEY", "CF_R2_SECRET_ACCESS_KEY")
R2_BUCKET_NAME       = _get_env("R2_BUCKET_NAME", "R2_BUCKET")
R2_MARKER_PREFIX     = _get_env("R2_MARKER_PREFIX", default="delete_markers")
R2_DELETE_AFTER_DAYS = int(_get_env("R2_DELETE_AFTER_DAYS", default="30"))
R2_ENABLED           = all([R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET_NAME])

# ---------- Trello helpers ----------
SESS = requests.Session()

def trello_call(method, path, **params):
    params.update({"key": TRELLO_KEY, "token": TRELLO_TOKEN})
    url = f"https://api.trello.com/1/{path.lstrip('/')}"
    for attempt in range(3):
        try:
            if method == "GET":
                r = SESS.get(url, params=params, timeout=30)
            elif method == "POST":
                r = SESS.post(url, params=params, timeout=30)
            elif method == "PUT":
                r = SESS.put(url, params=params, timeout=30)
            else:
                raise ValueError("method must be GET/POST/PUT")
            if r.status_code in (429, 500, 502, 503, 504):
                raise RuntimeError(f"Trello {r.status_code}")
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt == 2: raise
            time.sleep(1.2 * (attempt + 1))

def trello_get(path, **params):  return trello_call("GET", path, **params)
def trello_put(path, **params):  return trello_call("PUT", path, **params)
def trello_post(path, **params): return trello_call("POST", path, **params)

# Header parser (your standard card header)
TARGET_LABELS = ["Company","First","Email","Hook","Variant","Website"]
LABEL_RE = {lab: re.compile(rf'(?mi)^\s*{re.escape(lab)}\s*[:\-]\s*(.*)$') for lab in TARGET_LABELS}
EMAIL_RE = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", re.I)

def parse_header(desc: str) -> dict:
    out = {k: "" for k in TARGET_LABELS}
    d = (desc or "").replace("\r\n","\n").replace("\r","\n")
    lines = d.splitlines()
    i = 0
    while i < len(lines):
        line = lines[i]
        for lab in TARGET_LABELS:
            m = LABEL_RE[lab].match(line)
            if m:
                val = (m.group(1) or "").strip()
                if not val and (i+1) < len(lines):
                    nxt = lines[i+1]
                    if nxt.strip() and not any(LABEL_RE[L].match(nxt) for L in TARGET_LABELS):
                        val = nxt.strip(); i += 1
                out[lab] = val
                break
        i += 1
    return out

def clean_email(raw: str) -> str:
    if not raw: return ""
    m = EMAIL_RE.search(raw)
    return m.group(0).strip().lower() if m else ""

def _safe_id_from_email(em: str) -> str:
    return (em or "").strip().lower().replace("@","_").replace(".","_")

# ---------- Gmail helpers ----------
def decode_mime_words(s: str | None) -> str:
    if not s: return ""
    try:
        return str(make_header(decode_header(s)))
    except Exception:
        return s or ""

def extract_plain_text(msg: Message) -> str:
    """Prefer text/plain; fallback to stripped HTML; then remove quoted history + signatures."""
    parts = []
    if msg.is_multipart():
        for part in msg.walk():
            ctype = (part.get_content_type() or "").lower()
            disp  = (part.get("Content-Disposition") or "").lower()
            if "attachment" in disp:
                continue
            try:
                payload = part.get_payload(decode=True) or b""
                text = payload.decode(part.get_content_charset() or "utf-8", errors="replace")
            except Exception:
                continue
            if ctype.startswith("text/plain"):
                parts.append(text)
            elif not parts and ctype.startswith("text/html"):
                stripped = re.sub(r"(?is)<(script|style).*?>.*?</\1>", "", text)
                stripped = re.sub(r"(?is)<br\s*/?>", "\n", stripped)
                stripped = re.sub(r"(?is)</p\s*>", "\n\n", stripped)
                stripped = re.sub(r"(?is)<.*?>", "", stripped)
                parts.append(html.unescape(stripped))
    else:
        payload = msg.get_payload(decode=True) or b""
        parts.append(payload.decode(msg.get_content_charset() or "utf-8", errors="replace"))

    body = "\n".join(p.strip() for p in parts if p is not None).strip()
    body = strip_quoted_reply(body)

    if len(body) > BODY_MAX_CHARS:
        body = body[:BODY_MAX_CHARS].rstrip() + "\n…"
    return body

# Aggressive reply/quote/signature stripper
RE_REPLY_HEADER = re.compile(
    r"""(?imx)
    ^\s*
    (?:On|Le)
    [^\n\r]*
    (?:<[^>\n\r]+@[^>\n\r]+>|""" + EMAIL_RE.pattern + r""")?
    [^\n\r]*
    (?:wrote:|a\ écrit\s*:)?\s*$
    """
)
def strip_quoted_reply(text: str) -> str:
    if not text:
        return ""
    patterns = [
        RE_REPLY_HEADER,
        r"(?im)^\s*From:\s.*$",
        r"(?im)^\s*De\s*:\s.*$",
        r"(?im)^-+\s*Original Message\s*-+$",
        r"(?im)^Sent from my .*",
        r"(?m)^--\s*$",      # signature delimiter
        r"(?m)^__+\s*$",
        r"(?im)^>.*$",       # quoted lines
    ]
    cutoff = len(text)
    for pat in patterns:
        m = re.search(pat, text) if isinstance(pat, str) else pat.search(text)
        if m:
            cutoff = min(cutoff, m.start())
    text = text[:cutoff].rstrip()
    lines = []
    for ln in text.splitlines():
        s = ln.strip()
        if s.startswith(">"): continue
        if re.search(r"(?i)\bwrote:\s*$", s) or re.search(r"(?i)a écrit\s*:\s*$", s): break
        if (s.startswith("On ") or s.startswith("Le ")) and EMAIL_RE.search(s): break
        lines.append(ln)
    return "\n".join(lines).strip()

# ---------- Description helper ----------
def append_block(current_desc: str, block: str) -> str:
    """Append the new block at the BOTTOM of the description with a single separator."""
    cur = (current_desc or "").rstrip()
    if not cur:
        return block
    # check last non-empty line for an existing horizontal rule
    non_empty = [ln for ln in cur.splitlines() if ln.strip()]
    last = non_empty[-1].strip() if non_empty else ""
    if re.match(r"^(?:-{3,}|\*{3,}|_{3,})$", last):
        sep = "\n\n"
    else:
        sep = "\n\n---\n\n"
    return f"{cur}{sep}{block}"

# ---------- Optional R2 marker ----------
def write_r2_delete_marker(safe_id: str, due_iso: str):
    if not R2_ENABLED:
        return
    try:
        import boto3
        endpoint = f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com"
        s3 = boto3.client(
            "s3",
            aws_access_key_id=R2_ACCESS_KEY_ID,
            aws_secret_access_key=R2_SECRET_ACCESS_KEY,
            region_name="auto",
            endpoint_url=endpoint,
        )
        key = f"{R2_MARKER_PREFIX}/{safe_id}__{due_iso[:10].replace('-', '')}.json"
        # if exists, don't overwrite
        try:
            s3.head_object(Bucket=R2_BUCKET_NAME, Key=key)
            log(f"[r2] marker already exists: {key}")
            return
        except Exception:
            pass
        body = json.dumps({"id": safe_id, "due": due_iso}).encode("utf-8")
        s3.put_object(Bucket=R2_BUCKET_NAME, Key=key, Body=body, ContentType="application/json")
        log(f"[r2] queued delete marker: {key} (due {due_iso})")
    except Exception as e:
        log(f"[r2] marker write failed: {e}")

# ---------- Core ----------
def main():
    # sanity
    missing = []
    if not IMAP_USER:       missing.append("IMAP_USER")
    if not IMAP_PASS:       missing.append("IMAP_PASS")
    if not TRELLO_KEY:      missing.append("TRELLO_KEY")
    if not TRELLO_TOKEN:    missing.append("TRELLO_TOKEN")
    if not TRELLO_BOARD_ID: missing.append("TRELLO_BOARD_ID")
    if not DEST_LIST_ID:    missing.append("TRELLO_DEST_LIST_ID")
    if missing:
        raise SystemExit("Missing env: " + ", ".join(missing))

    # 1) Load cards (id, name, desc) and build email → cards map
    log("[trello] fetching board cards…")
    cards = trello_get(f"boards/{TRELLO_BOARD_ID}/cards", fields="id,name,desc,idList", limit=1000)
    email_to_cards = {}
    for c in cards or []:
        desc = c.get("desc") or ""
        fields = parse_header(desc)
        em = clean_email(fields.get("Email", ""))
        if em:
            email_to_cards.setdefault(em, []).append(c)

    # 2) Connect IMAP and search UNSEEN
    log("[imap] connecting…")
    M = imaplib.IMAP4_SSL(IMAP_HOST, IMAP_PORT)
    M.login(IMAP_USER, IMAP_PASS)
    M.select("INBOX")
    typ, data = M.search(None, 'UNSEEN')
    if typ != "OK":
        log("[imap] search failed")
        M.logout()
        return
    ids = (data[0] or b"").split()
    ids = ids[-MAX_EMAILS_PER_RUN:] if MAX_EMAILS_PER_RUN else ids
    log(f"[imap] unseen emails: {len(ids)}")

    processed = 0
    for eid in ids:
        typ, msgdata = M.fetch(eid, '(RFC822)')
        if typ != "OK" or not msgdata:
            continue
        raw = msgdata[0][1]
        msg = email.message_from_bytes(raw)

        from_hdr = decode_mime_words(msg.get("From", ""))
        subj_hdr = decode_mime_words(msg.get("Subject", ""))
        m = EMAIL_RE.search(from_hdr)
        sender = m.group(0).lower() if m else ""
        body = extract_plain_text(msg)
        when = decode_mime_words(msg.get("Date", "")) or datetime.utcnow().isoformat(timespec="seconds")+"Z"

        log(f"[imap] from={sender} subject={subj_hdr!r}")

        if not sender or sender not in email_to_cards:
            # mark as seen anyway so we don't loop forever on unmatched mail
            M.store(eid, '+FLAGS', '\\Seen')
            continue

        # Optional: queue R2 delete marker (one per sender)
        if R2_ENABLED:
            safe_id = _safe_id_from_email(sender)
            due_iso = (datetime.utcnow() + timedelta(days=R2_DELETE_AFTER_DAYS)).isoformat(timespec="seconds") + "Z"
            write_r2_delete_marker(safe_id, due_iso)

        # Build the appended block (goes at bottom)
        block = f"**Subject :**\n\n{subj_hdr}\n\n**Body :**\n\n{body}\n"

        for c in email_to_cards[sender]:
            cid    = c["id"]
            old    = c.get("desc") or ""
            title  = c.get("name") or "(no title)"
            new_desc = append_block(old, block)

            try:
                trello_put(f"cards/{cid}", idList=DEST_LIST_ID, desc=new_desc)
                trello_post(f"cards/{cid}/actions/comments", text=f"Synced reply from {sender} — {when}")
                log(f"[trello] updated + moved: {title}")
            except Exception as e:
                log(f"[trello] update failed for {title}: {e}")

        # mark email as seen so we don't process it again
        M.store(eid, '+FLAGS', '\\Seen')
        processed += 1
        time.sleep(0.6)

    M.close(); M.logout()
    log(f"Done. Emails processed: {processed}")

if __name__ == "__main__":
    main()
