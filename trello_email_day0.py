#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Day-0 — Poll Trello and send one email per card.

RULES:
- No text links at all (no portfolio link, no appended link).
- Only clickable logos (header + signature) point to your website.
- Uses Subject/Body A vs B depending on whether "First" exists.
- Marks card by posting a Trello comment marker to prevent resends.
- No Trello card moving (Trello automation handles it).
"""

import os, re, time, json, html, unicodedata
from datetime import datetime
import requests

def log(*a): print(*a, flush=True)

# ----------------- tiny utils -----------------
def _get_env(*names, default=""):
    for n in names:
        v = os.getenv(n)
        if v is not None and v.strip():
            return v.strip()
    return default

def _env_bool(name: str, default: str = "0") -> bool:
    val = os.getenv(name, default)
    return (val or "").strip().lower() in ("1","true","yes","on")

def _safe_id_from_email(email: str) -> str:
    return (email or "").strip().lower().replace("@", "_").replace(".", "_")

def _slugify_company(name: str) -> str:
    s = (name or "").strip()
    if not s: return ""
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = s.lower()
    s = re.sub(r"[^\w\s-]+", "", s)
    s = re.sub(r"[\s-]+", "_", s).strip("_")
    return s or ""

def choose_id(company: str, email: str) -> str:
    sid = _slugify_company(company)
    return sid if sid else _safe_id_from_email(email)

def _norm_base(u: str) -> str:
    u = (u or "").strip()
    if not u: return ""
    if not re.match(r"^https?://", u, flags=re.I):
        u = "https://" + u
    return u.rstrip("/")

# ----------------- env -----------------
TRELLO_KEY   = _get_env("TRELLO_KEY")
TRELLO_TOKEN = _get_env("TRELLO_TOKEN")
LIST_ID      = _get_env("TRELLO_LIST_ID_DAY0", "TRELLO_LIST_ID")

FROM_NAME  = _get_env("FROM_NAME",  default="Matthieu from Matly Ascend")
FROM_EMAIL = _get_env("FROM_EMAIL", default="matthieu@matlyascend.com")

SMTP_HOST    = _get_env("SMTP_HOST", "smtp_host", default="smtp.gmail.com")
SMTP_PORT    = int(_get_env("SMTP_PORT", "smtp_port", default="587"))
SMTP_USE_TLS = _get_env("SMTP_USE_TLS", "smtp_use_tls", default="1").lower() in ("1","true","yes","on")
SMTP_PASS    = _get_env("SMTP_PASS", "SMTP_PASSWORD", "smtp_pass", "smtp_password")
SMTP_USER    = _get_env("SMTP_USER", "SMTP_USERNAME", "smtp_user", "smtp_username", "FROM_EMAIL")
SMTP_DEBUG   = _env_bool("SMTP_DEBUG", "0")
BCC_TO       = _get_env("BCC_TO", default="").strip()

# Where logos click to (only links you want)
BRAND_URL = _norm_base(_get_env("BRAND_URL", default="https://matlyascend.com"))

# Send control
SENT_MARKER_TEXT = _get_env("SENT_MARKER_TEXT", default="Sent: Day0")
SENT_CACHE_FILE  = _get_env("SENT_CACHE_FILE", default=".data/sent_day0.json")
MAX_SEND_PER_RUN = int(_get_env("MAX_SEND_PER_RUN", default="0"))

log(f"[env] BRAND_URL={BRAND_URL}")

# ----------------- HTTP -----------------
UA = f"TrelloEmailer-Day0/clean (+{FROM_EMAIL or 'no-email'})"
SESS = requests.Session()
SESS.headers.update({"User-Agent": UA})

# ----------------- templates -----------------
USE_ENV_TEMPLATES = os.getenv("USE_ENV_TEMPLATES", "1").strip().lower() in ("1","true","yes","on")
if USE_ENV_TEMPLATES:
    SUBJECT_A = _get_env("SUBJECT_A", default="Quick question about {Company}’s listings")
    SUBJECT_B = _get_env("SUBJECT_B", default="Quick question about {Company}’s listings")

    # IMPORTANT: use {Company} not {{Company}}
    BODY_A = _get_env("BODY_A", default=
"""Hi there,

Quick question — do you handle the website / getting found on Google for {Company}?

I noticed a couple simple opportunities that could help {Company} attract more relevant visitors from Google.

If it’s you, should I send 3 quick bullets? (You can just reply “yes”.)
If not, who’s the best person to reach?

Best,
Matthieu from Matly Ascend""")

    BODY_B = _get_env("BODY_B", default=
"""Hi {First},

Quick question — do you handle the website / getting found on Google for {Company}?

I noticed a couple simple opportunities that could help {Company} attract more relevant visitors from Google.

If it’s you, should I send 3 quick bullets? (You can just reply “yes”.)
If not, who’s the best person to reach?

Best,
Matthieu from Matly Ascend""")
else:
    SUBJECT_A = "Quick question about {Company}’s listings"
    SUBJECT_B = "Quick question about {Company}’s listings"

    BODY_A = """Hi there,

Quick question — do you handle the website / getting found on Google for {Company}?

I noticed a couple simple opportunities that could help {Company} attract more relevant visitors from Google.

If it’s you, should I send 3 quick bullets? (You can just reply “yes”.)
If not, who’s the best person to reach?

Best,
Matthieu from Matly Ascend"""

    BODY_B = """Hi {First},

Quick question — do you handle the website / getting found on Google for {Company}?

I noticed a couple simple opportunities that could help {Company} attract more relevant visitors from Google.

If it’s you, should I send 3 quick bullets? (You can just reply “yes”.)
If not, who’s the best person to reach?

Best,
Matthieu from Matly Ascend"""

# ----------------- parsing -----------------
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
    txt = html.unescape(raw)
    m = EMAIL_RE.search(txt)
    return m.group(0).strip() if m else ""

# ----------------- Trello I/O -----------------
def _trello_call(method, url_path, **params):
    for attempt in range(3):
        try:
            params.update({"key": TRELLO_KEY, "token": TRELLO_TOKEN})
            url = f"https://api.trello.com/1/{url_path.lstrip('/')}"
            r = (SESS.get if method == "GET" else SESS.post)(url, params=params, timeout=30)
            if r.status_code in (429, 500, 502, 503, 504):
                raise RuntimeError(f"Trello {r.status_code}")
            r.raise_for_status()
            return r.json()
        except Exception:
            if attempt == 2: raise
            time.sleep(1.2 * (attempt + 1))
    raise RuntimeError("Unreachable")

def trello_get(url_path, **params):  return _trello_call("GET", url_path, **params)
def trello_post(url_path, **params): return _trello_call("POST", url_path, **params)

def already_marked(card_id: str, marker: str) -> bool:
    try:
        acts = trello_get(f"cards/{card_id}/actions", filter="commentCard", limit=50)
    except Exception:
        return False
    marker_l = (marker or "").lower().strip()
    for a in acts:
        txt = (a.get("data", {}).get("text") or a.get("text") or "").strip()
        if txt.lower().startswith(marker_l):
            return True
    return False

def mark_sent(card_id: str, marker: str, extra: str = ""):
    ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    text = f"{marker} — {ts}"
    if extra: text += f"\n{extra}"
    try:
        trello_post(f"cards/{card_id}/actions/comments", text=text)
    except Exception:
        pass

# ----------------- templating -----------------
def fill_template(tpl: str, *, company: str, first: str, from_name: str) -> str:
    def repl(m):
        key = m.group(1).strip().lower()
        if key == "company":   return company or ""
        if key == "first":     return first or ""
        if key == "from_name": return from_name or ""
        return m.group(0)
    return re.sub(r"{\s*(company|first|from_name)\s*}", repl, tpl, flags=re.I)

def sanitize_subject(s: str) -> str:
    return re.sub(r"[\r\n]+", " ", (s or "")).strip()[:250]

def text_to_html(text: str) -> str:
    esc = html.escape(text or "").replace("\r\n", "\n").replace("\r", "\n")
    esc = esc.replace("\n\n", "</p><p>").replace("\n", "<br>")

    p_style = (
        "margin:0 0 16px 0;"
        "color:#f5f5f7 !important;"
        "font-size:16px !important;"
        "line-height:1.8;"
        "font-weight:400;"
    )
    return f'<p style="{p_style}">{esc}</p>'

def wrap_html(inner: str) -> str:
    inner = inner or ""
    wrapper_style = (
        'font-family:"Roboto",-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;'
        "color:#f5f5f7 !important;"
        "font-size:16px;"
        "line-height:1.8;"
        "font-weight:400;"
        "-webkit-text-size-adjust:100%;-ms-text-size-adjust:100%;"
    )

    bar_color_top = "#292929"
    bar_color_bottom = "#292929"

    header_logo_url = (
        "https://matlyascend.com/wp-content/uploads/2025/12/cropped-logo-with-ascend-white-e.png"
    )

    brand_href = html.escape(BRAND_URL or "https://matlyascend.com", quote=True)

    return f"""
<table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="background:#FCFCFC;padding:16px 12px;">
  <tr>
    <td align="center">
      <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="max-width:720px;border-radius:18px;overflow:hidden;background:#1e1e1e;border:2.8px solid #000000;box-shadow:0 18px 45px rgba(0,0,0,.35);">
        <tr>
          <td style="padding:30px 12px;background:{bar_color_top};text-align:center;">
            <a href="{brand_href}" target="_blank" style="text-decoration:none;">
              <img src="{html.escape(header_logo_url)}"
                   alt="Matly Ascend"
                   style="max-height:48px;display:inline-block;border:0;">
            </a>
          </td>
        </tr>
        <tr>
          <td style="padding:24px 16px 24px 16px;">
            <div style="{wrapper_style}">
              {inner}
            </div>
          </td>
        </tr>
        <tr>
          <td style="padding:0;background:{bar_color_bottom};height:24px;line-height:0;font-size:0;">&nbsp;</td>
        </tr>
      </table>
    </td>
  </tr>
</table>
""".strip()

SIGNATURE_LOGO_URL = "https://matlyascend.com/wp-content/uploads/2025/12/cropped-logo-with-ascend-white-e.png"

def signature_html() -> str:
    brand_href = html.escape(BRAND_URL or "https://matlyascend.com", quote=True)
    return (
        """
<table role="presentation" width="100%%" cellpadding="0" cellspacing="0" border="0" style="margin-top:0px;">
  <tr>
    <td align="left" style="padding:0;">
      <a href="%s" target="_blank" style="text-decoration:none;">
        <img src="%s"
             alt="Matly Ascend"
             style="max-width:90px;height:auto;border:0;display:block;vertical-align:middle;">
      </a>
    </td>
  </tr>
</table>
""" % (brand_href, html.escape(SIGNATURE_LOGO_URL))
    )

# ----------------- sender -----------------
def send_email(to_email: str, subject: str, body_text: str):
    from email.message import EmailMessage
    import smtplib

    # Plain text
    body_pt = body_text.strip()

    # HTML (NO appended links)
    html_core_inner = text_to_html(body_text)
    html_full = wrap_html(html_core_inner + signature_html())

    msg = EmailMessage()
    msg["From"] = f"{FROM_NAME} <{FROM_EMAIL}>"
    msg["To"] = to_email
    msg["Subject"] = sanitize_subject(subject)
    msg.set_content(body_pt)
    msg.add_alternative(html_full, subtype="html")
    if BCC_TO:
        msg["Bcc"] = BCC_TO

    for attempt in range(3):
        try:
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as s:
                if SMTP_DEBUG:
                    s.set_debuglevel(1)
                if SMTP_USE_TLS:
                    s.starttls()
                s.login(SMTP_USER or FROM_EMAIL, SMTP_PASS)
                s.send_message(msg)
            return
        except Exception as e:
            log(f"[WARN] SMTP attempt {attempt+1}/3 failed: {e}")
            if attempt == 2:
                raise
            time.sleep(1.0 * (attempt + 1))

# ----------------- cache -----------------
def load_sent_cache():
    try:
        with open(SENT_CACHE_FILE, "r", encoding="utf-8") as f:
            return set(json.load(f))
    except Exception:
        return set()

def save_sent_cache(ids):
    d = os.path.dirname(SENT_CACHE_FILE)
    if d: os.makedirs(d, exist_ok=True)
    try:
        with open(SENT_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(sorted(ids), f)
    except Exception:
        pass

# ----------------- main -----------------
def main():
    missing = []
    for k in ("TRELLO_KEY","TRELLO_TOKEN","FROM_EMAIL","SMTP_PASS"):
        if not globals()[k]:
            missing.append(k)
    if not LIST_ID: missing.append("TRELLO_LIST_ID_DAY0")
    if missing:
        raise SystemExit("Missing env: " + ", ".join(missing))

    sent_cache = load_sent_cache()
    cards = trello_get(f"lists/{LIST_ID}/cards", fields="id,name,desc", limit=200)
    if not isinstance(cards, list):
        log("No cards found or Trello error.")
        return

    processed = 0
    for c in cards:
        if MAX_SEND_PER_RUN and processed >= MAX_SEND_PER_RUN:
            break

        card_id = c.get("id")
        title = c.get("name","(no title)")
        if not card_id or card_id in sent_cache:
            continue

        desc = c.get("desc") or ""
        fields  = parse_header(desc)
        company = (fields.get("Company") or "").strip()
        first   = (fields.get("First")   or "").strip()
        email_v = clean_email(fields.get("Email") or "") or clean_email(desc)

        if not email_v:
            log(f"Skip: no valid Email on '{title}'.")
            continue

        if already_marked(card_id, SENT_MARKER_TEXT):
            log(f"Skip: already marked '{SENT_MARKER_TEXT}' — {title}")
            sent_cache.add(card_id)
            continue

        _ = choose_id(company, email_v)  # kept for future consistency

        use_b    = bool(first)
        subj_tpl = SUBJECT_B if use_b else SUBJECT_A
        body_tpl = BODY_B    if use_b else BODY_A

        subject = fill_template(subj_tpl, company=company, first=first, from_name=FROM_NAME)
        body    = fill_template(body_tpl, company=company, first=first, from_name=FROM_NAME)

        try:
            send_email(email_v, subject, body)
            processed += 1
            log(f"Sent to {email_v} — '{title}'")
        except Exception as e:
            log(f"Send failed for '{title}' to {email_v}: {e}")
            continue

        mark_sent(card_id, SENT_MARKER_TEXT, extra=f"Subject: {subject}")
        sent_cache.add(card_id)
        save_sent_cache(sent_cache)
        time.sleep(0.8)

    log(f"Done. Emails sent: {processed}")

if __name__ == "__main__":
    main()
