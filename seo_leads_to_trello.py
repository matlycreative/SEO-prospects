# seo_leads_to_trello.py
#
# Local-business lead finder for SEO outreach + Trello push
# + optional enrichment from website pages + optional LinkedIn (via your Scrapy spider).
#
# IMPORTANT:
# - This script is written to RUN (no indentation landmines).
# - LinkedIn enrichment is OPTIONAL and will gracefully skip if Scrapy/spider/output is missing.
#
# NOTE (per your request):
# - Email extraction is DISABLED. The script will NOT scrape emails from websites
#   and will ALWAYS write Email: (blank) into Trello.
#
# FIXES INCLUDED (per your request):
# 1) SEEN FILE IS APPEND-ONLY: we NEVER rewrite/overwrite the seen domains file.
#    This prevents Git rebase conflicts from "full-file rewrites".
# 2) ENGLISH-ONLY FILTER: only keep websites that appear to be in English.

import os
import re
import json
import time
import math
import csv
import random
import pathlib
import subprocess
from datetime import date, datetime
from urllib.parse import urljoin, urlparse
from typing import Optional, List, Tuple
from functools import lru_cache

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import tldextract
import urllib.robotparser as robotparser


# ---------- optional local .env ----------
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass


# ---------- env helpers ----------
def env_int(name, default):
    v = (os.getenv(name) or "").strip()
    try:
        return int(v)
    except Exception:
        return int(default)

def env_float(name, default):
    v = (os.getenv(name) or "").strip()
    try:
        return float(v)
    except Exception:
        return float(default)

def env_on(name, default=False):
    v = (os.getenv(name) or "").strip().lower()
    if v in ("1","true","yes","on"): return True
    if v in ("0","false","no","off"): return False
    return bool(default)


# ---------- config ----------
DAILY_LIMIT      = env_int("DAILY_LIMIT", 20)
PUSH_INTERVAL_S  = env_int("PUSH_INTERVAL_S", 20)
REQUEST_DELAY_S  = env_float("REQUEST_DELAY_S", 0.2)

# Seen file: append-only. Never rewrite.
SEEN_FILE        = os.getenv("SEEN_FILE", "seen_domains.txt")

# English filter (DEFAULT ON)
ENGLISH_ONLY                 = env_on("ENGLISH_ONLY", True)
ENGLISH_MIN_WORDS            = env_int("ENGLISH_MIN_WORDS", 80)
ENGLISH_MIN_STOPWORD_RATIO   = env_float("ENGLISH_MIN_STOPWORD_RATIO", 0.012)
ENGLISH_MAX_NONASCII_RATIO   = env_float("ENGLISH_MAX_NONASCII_RATIO", 0.18)

# "Small site" filtering
SMALL_SITE_ONLY          = env_on("SMALL_SITE_ONLY", True)
MAX_HTML_KB_SMALL_SITE   = env_int("MAX_HTML_KB_SMALL_SITE", 600)
MAX_SCRIPTS_SMALL_SITE   = env_int("MAX_SCRIPTS_SMALL_SITE", 40)

# Batch rotation
BATCH_FILE = os.getenv("BATCH_FILE", "batch_state.txt")
BATCH_SLOTS = [
    "m monday 2",
    "a monday",
    "m tuesday",
    "a tuesday",
    "m wednesday",
    "a wednesday",
    "m thursday",
    "a thursday",
    "m friday",
    "a friday",
]

BUTLER_GRACE_S   = env_int("BUTLER_GRACE_S", 15)
DEBUG            = env_on("DEBUG", False)

# pre-clone toggle (disabled by default)
PRECLONE   = env_on("PRECLONE", False)

# OSM candidates source toggles
OVERPASS_ENABLED        = env_on("OVERPASS_ENABLED", 1)
NOMINATIM_POI_ENABLED   = env_on("NOMINATIM_POI_ENABLED", 1)
OVERPASS_NAME_LOOKUP_ENABLED = env_on("OVERPASS_NAME_LOOKUP_ENABLED", 0)

# Overpass tuning
OVERPASS_TIMEOUT_S      = env_int("OVERPASS_TIMEOUT_S", 45)
OVERPASS_QUERY_TIMEOUT  = env_int("OVERPASS_QUERY_TIMEOUT", 25)
OVERPASS_RETRIES        = env_int("OVERPASS_RETRIES", 2)
OVERPASS_MIN_INTERVAL_S = env_float("OVERPASS_MIN_INTERVAL_S", 2.0)

# NEW: chunk Overpass filters to avoid massive single queries
OVERPASS_FILTER_CHUNK   = env_int("OVERPASS_FILTER_CHUNK", 14)

# Nominatim POI fallback tuning
NOMINATIM_LIMIT                 = env_int("NOMINATIM_LIMIT", 60)
NOMINATIM_POI_QUERIES_PER_CITY  = env_int("NOMINATIM_POI_QUERIES_PER_CITY", 3)

# Nominatim + UA
NOMINATIM_EMAIL = os.getenv("NOMINATIM_EMAIL", "you@example.com")
UA              = os.getenv("USER_AGENT", f"SEOLeads/1.0 (+{NOMINATIM_EMAIL})")

# Trello
TRELLO_KEY      = os.getenv("TRELLO_KEY")
TRELLO_TOKEN    = os.getenv("TRELLO_TOKEN")
TRELLO_LIST_ID  = os.getenv("TRELLO_LIST_ID")
TRELLO_TEMPLATE_CARD_ID = os.getenv("TRELLO_TEMPLATE_CARD_ID")

# Discovery (optional)
FOURSQUARE_API_KEY = os.getenv("FOURSQUARE_API_KEY")

# LinkedIn Scrapy project folder (optional)
LINKEDIN_SCRAPY_DIR = os.getenv("LINKEDIN_SCRAPY_DIR", "linkedin")  # set "" if spider is at repo root

STATS = {
    "osm_candidates": 0,
    "cand_overpass": 0,
    "cand_nominatim_poi": 0,
    "skip_no_website": 0,
    "skip_dupe_domain": 0,
    "skip_robots": 0,
    "skip_fetch": 0,
    "skip_big_site": 0,
    "skip_non_english": 0,
    "website_direct": 0,
    "website_overpass_name": 0,
    "website_nominatim": 0,
    "website_fsq": 0,
    "website_wikidata": 0,
}

def dbg(msg):
    if DEBUG:
        print(msg, flush=True)


# ---------- throttling ----------
_LAST_CALL = {}
def throttle(key: str, min_interval_s: float):
    now = time.monotonic()
    last = _LAST_CALL.get(key, 0.0)
    wait = (min_interval_s - (now - last))
    if wait > 0:
        time.sleep(wait)
    _LAST_CALL[key] = time.monotonic()

def _sleep():
    time.sleep(REQUEST_DELAY_S)


# ---------- HTTP ----------
SESS = requests.Session()
SESS.headers.update({"User-Agent": UA, "Accept-Language": "en;q=0.9"})

try:
    _retries = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset({"GET", "POST", "PUT"}),
        respect_retry_after_header=True,
    )
except TypeError:
    _retries = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        method_whitelist=frozenset({"GET", "POST", "PUT"}),
    )

SESS.mount("https://", HTTPAdapter(max_retries=_retries))
SESS.mount("http://", HTTPAdapter(max_retries=_retries))


# ---------- geo / city ----------
COUNTRY_WHITELIST = [s.strip() for s in (os.getenv("COUNTRY_WHITELIST") or "").split(",") if s.strip()]
CITY_MODE     = os.getenv("CITY_MODE", "rotate")  # rotate | random | force
FORCE_COUNTRY = (os.getenv("FORCE_COUNTRY") or "").strip()
FORCE_CITY    = (os.getenv("FORCE_CITY") or "").strip()
CITY_HOPS     = env_int("CITY_HOPS", 8)
OSM_RADIUS_M  = env_int("OSM_RADIUS_M", 2500)

# BIG expansion: worldwide major cities
CITY_ROTATION = [
    # --- USA ---
    ("New York","United States"),
    ("Los Angeles","United States"),
    ("Chicago","United States"),
    ("Miami","United States"),
    ("San Francisco","United States"),
    ("Dallas","United States"),
    ("Houston","United States"),
    ("Atlanta","United States"),
    ("Boston","United States"),
    ("Seattle","United States"),
    ("Austin","United States"),
    ("Washington","United States"),
    ("Philadelphia","United States"),
    ("Phoenix","United States"),
    ("Denver","United States"),
    ("San Diego","United States"),
    ("Las Vegas","United States"),

    # --- Canada ---
    ("Toronto","Canada"),
    ("Vancouver","Canada"),
    ("Montreal","Canada"),
    ("Calgary","Canada"),
    ("Ottawa","Canada"),
    ("Edmonton","Canada"),
    ("Quebec City","Canada"),

    # --- UK ---
    ("London","United Kingdom"),
    ("Manchester","United Kingdom"),
    ("Birmingham","United Kingdom"),
    ("Edinburgh","United Kingdom"),
    ("Glasgow","United Kingdom"),
    ("Liverpool","United Kingdom"),
    ("Bristol","United Kingdom"),
    ("Leeds","United Kingdom"),

    # --- Ireland ---
    ("Dublin","Ireland"),
    ("Cork","Ireland"),
    ("Galway","Ireland"),

    # --- Australia ---
    ("Sydney","Australia"),
    ("Melbourne","Australia"),
    ("Brisbane","Australia"),
    ("Perth","Australia"),
    ("Adelaide","Australia"),
    ("Canberra","Australia"),
    ("Gold Coast","Australia"),

    # --- New Zealand ---
    ("Auckland","New Zealand"),
    ("Wellington","New Zealand"),
    ("Christchurch","New Zealand"),

    # --- Singapore / HK / Taiwan ---
    ("Singapore","Singapore"),
    ("Hong Kong","China"),
    ("Taipei","Taiwan"),

    # --- Japan ---
    ("Tokyo","Japan"),
    ("Osaka","Japan"),
    ("Yokohama","Japan"),
    ("Nagoya","Japan"),
    ("Fukuoka","Japan"),

    # --- South Korea ---
    ("Seoul","South Korea"),
    ("Busan","South Korea"),
    ("Incheon","South Korea"),

    # --- China (major metros) ---
    ("Shanghai","China"),
    ("Beijing","China"),
    ("Shenzhen","China"),
    ("Guangzhou","China"),

    # --- Southeast Asia ---
    ("Bangkok","Thailand"),
    ("Kuala Lumpur","Malaysia"),
    ("Jakarta","Indonesia"),
    ("Manila","Philippines"),
    ("Hanoi","Vietnam"),
    ("Ho Chi Minh City","Vietnam"),

    # --- India / Pakistan ---
    ("Mumbai","India"),
    ("Delhi","India"),
    ("Bengaluru","India"),
    ("Hyderabad","India"),
    ("Chennai","India"),
    ("Kolkata","India"),
    ("Karachi","Pakistan"),
    ("Lahore","Pakistan"),

    # --- UAE + Middle East ---
    ("Dubai","United Arab Emirates"),
    ("Abu Dhabi","United Arab Emirates"),
    ("Doha","Qatar"),
    ("Riyadh","Saudi Arabia"),
    ("Jeddah","Saudi Arabia"),
    ("Kuwait City","Kuwait"),
    ("Manama","Bahrain"),
    ("Muscat","Oman"),
    ("Tel Aviv","Israel"),

    # --- Europe: France ---
    ("Paris","France"),
    ("Lyon","France"),
    ("Marseille","France"),

    # --- Europe: Germany ---
    ("Berlin","Germany"),
    ("Munich","Germany"),
    ("Hamburg","Germany"),
    ("Frankfurt","Germany"),
    ("Cologne","Germany"),

    # --- Europe: Spain ---
    ("Madrid","Spain"),
    ("Barcelona","Spain"),
    ("Valencia","Spain"),

    # --- Europe: Italy ---
    ("Rome","Italy"),
    ("Milan","Italy"),
    ("Naples","Italy"),

    # --- Europe: Netherlands / Belgium / Switzerland / Austria ---
    ("Amsterdam","Netherlands"),
    ("Rotterdam","Netherlands"),
    ("Brussels","Belgium"),
    ("Antwerp","Belgium"),
    ("Zurich","Switzerland"),
    ("Geneva","Switzerland"),
    ("Vienna","Austria"),

    # --- Nordics ---
    ("Stockholm","Sweden"),
    ("Gothenburg","Sweden"),
    ("Copenhagen","Denmark"),
    ("Oslo","Norway"),
    ("Helsinki","Finland"),

    # --- Central/Eastern Europe ---
    ("Prague","Czechia"),
    ("Warsaw","Poland"),
    ("Budapest","Hungary"),
    ("Bucharest","Romania"),

    # --- Southern Europe / Turkey ---
    ("Lisbon","Portugal"),
    ("Porto","Portugal"),
    ("Athens","Greece"),
    ("Istanbul","Turkey"),

    # --- Africa ---
    ("Cairo","Egypt"),
    ("Johannesburg","South Africa"),
    ("Cape Town","South Africa"),
    ("Nairobi","Kenya"),
    ("Lagos","Nigeria"),
    ("Accra","Ghana"),
    ("Casablanca","Morocco"),
    ("Tunis","Tunisia"),

    # --- Latin America ---
    ("Mexico City","Mexico"),
    ("Guadalajara","Mexico"),
    ("Monterrey","Mexico"),
    ("São Paulo","Brazil"),
    ("Rio de Janeiro","Brazil"),
    ("Brasília","Brazil"),
    ("Buenos Aires","Argentina"),
    ("Santiago","Chile"),
    ("Lima","Peru"),
    ("Bogotá","Colombia"),
    ("Medellín","Colombia"),
    ("Panama City","Panama"),
]

def iter_cities():
    pool = CITY_ROTATION[:]
    if COUNTRY_WHITELIST:
        wl = {c.lower() for c in COUNTRY_WHITELIST}
        pool = [c for c in pool if c[1].lower() in wl]
    if FORCE_COUNTRY:
        pool = [c for c in pool if c[1].lower() == FORCE_COUNTRY.lower()]
    if FORCE_CITY:
        pool = [c for c in pool if c[0].lower() == FORCE_CITY.lower()]
    if not pool:
        pool = CITY_ROTATION

    if CITY_MODE.lower() == "random":
        random.shuffle(pool)
        for c in pool[:CITY_HOPS]:
            yield c
    else:
        start = random.randint(0, len(pool) - 1)
        hops = min(CITY_HOPS, len(pool))
        for i in range(hops):
            yield pool[(start + i) % len(pool)]


# ---------- utils ----------
def normalize_url(u):
    if not u:
        return None
    u = u.strip()
    if u.lower().startswith("mailto:"):
        return None
    if "@" in u and "://" not in u:
        if re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", u):
            return None
    p = urlparse(u)
    if not p.scheme:
        u = "https://" + u.strip("/")
    p2 = urlparse(u)
    if p2.scheme not in ("http", "https"):
        return None
    if not p2.netloc:
        return None
    if p2.username or p2.password or "@" in p2.netloc:
        return None
    return u

def etld1_from_url(u: str) -> str:
    try:
        ex = tldextract.extract(u or "")
        if ex.domain:
            return f"{ex.domain}.{ex.suffix}" if ex.suffix else ex.domain
    except Exception:
        pass
    return ""

def fetch(url):
    r = SESS.get(url, timeout=30)
    r.raise_for_status()
    return r


# ---------- robots ----------
@lru_cache(maxsize=2048)
def _robots_parser_for_base(base: str) -> robotparser.RobotFileParser:
    rp = robotparser.RobotFileParser()
    try:
        resp = SESS.get(urljoin(base, "/robots.txt"), timeout=10)
        if resp.status_code != 200:
            rp.parse([])
            return rp
        rp.parse(resp.text.splitlines())
        return rp
    except Exception:
        rp.parse([])
        return rp

def allowed_by_robots(base_url: str, path: str = "/") -> bool:
    try:
        p = urlparse(base_url)
        base = f"{p.scheme}://{p.netloc}"
        path0 = path or "/"
        if not path0.startswith("/"):
            path0 = "/" + path0
        rp = _robots_parser_for_base(base)
        return rp.can_fetch(UA, urljoin(base, path0))
    except Exception:
        return True


# ---------- English detection ----------
_EN_STOPWORDS = {
    # compact but effective list
    "the","and","for","with","from","that","this","you","your","are","our","we","us",
    "about","services","service","contact","home","pricing","company","business",
    "help","learn","more","book","call","get","start","support",
    "website","page","pages","privacy","terms",
    "location","locations","hours","open","email","phone"
}

def _parse_lang_header(content_language: str) -> List[str]:
    # "en-US,en;q=0.9" -> ["en-us","en"]
    parts = []
    for chunk in (content_language or "").split(","):
        c = chunk.strip().lower()
        if not c:
            continue
        c = c.split(";")[0].strip()
        if c:
            parts.append(c)
    return parts

def is_english_page(html_text: str, headers: dict) -> bool:
    """
    Returns True if page appears to be English.
    Priority:
    1) HTTP Content-Language header
    2) <html lang="..."> or meta content-language
    3) lightweight text heuristic (stopword ratio + non-ascii ratio)
    """
    # 1) Content-Language header
    try:
        cl = headers.get("Content-Language") or headers.get("content-language") or ""
        langs = _parse_lang_header(cl)
        if langs:
            if any(l.startswith("en") for l in langs):
                return True
            # if server explicitly says non-English and no en present -> reject
            return False
    except Exception:
        pass

    # 2) HTML lang attribute / meta
    try:
        soup = BeautifulSoup(html_text or "", "html.parser")
        html_tag = soup.find("html")
        lang = ""
        if html_tag:
            lang = (html_tag.get("lang") or html_tag.get("xml:lang") or "").strip().lower()
        if lang:
            if lang.startswith("en"):
                return True
            return False

        meta = soup.find("meta", attrs={"http-equiv": re.compile(r"content-language", re.I)})
        if meta:
            mcl = (meta.get("content") or "").strip().lower()
            if mcl:
                if mcl.startswith("en") or "en" in _parse_lang_header(mcl):
                    return True
                return False
    except Exception:
        soup = None

    # 3) Heuristic
    try:
        if soup is None:
            soup = BeautifulSoup(html_text or "", "html.parser")
        text = soup.get_text(" ", strip=True)
        # limit work
        text = (text or "")[:12000].lower()
        if not text:
            # can't detect -> don't block aggressively
            return False if ENGLISH_ONLY else True

        # compute non-ascii ratio
        non_ascii = sum(1 for ch in text if ord(ch) > 127)
        non_ascii_ratio = non_ascii / max(1, len(text))

        words = re.findall(r"[a-zA-Z]{2,}", text)
        if len(words) < ENGLISH_MIN_WORDS:
            # not enough signal -> reject if strict
            return False

        hits = sum(1 for w in words if w in _EN_STOPWORDS)
        ratio = hits / max(1, len(words))

        if non_ascii_ratio > ENGLISH_MAX_NONASCII_RATIO and ratio < ENGLISH_MIN_STOPWORD_RATIO:
            return False

        return ratio >= ENGLISH_MIN_STOPWORD_RATIO
    except Exception:
        return False


# ---------- "small site" heuristic ----------
def is_probably_small_site(html_text: str, url: str) -> bool:
    if not SMALL_SITE_ONLY:
        return True
    if not html_text:
        return True

    size_bytes = len(html_text.encode("utf-8", errors="ignore"))
    if size_bytes > MAX_HTML_KB_SMALL_SITE * 1024:
        dbg(f"[big-site] {url} skipped: HTML {size_bytes/1024:.1f} KB")
        return False

    script_count = html_text.lower().count("<script")
    if script_count > MAX_SCRIPTS_SMALL_SITE:
        dbg(f"[big-site] {url} skipped: {script_count} <script> tags")
        return False

    return True


# ---------- geo ----------
def geocode_city(city, country) -> Tuple[float,float,float,float]:
    throttle("nominatim", 1.1)
    r = SESS.get(
        "https://nominatim.openstreetmap.org/search",
        params={"q": f"{city}, {country}", "format":"json", "limit":1},
        headers={"Referer":"https://nominatim.org"},
        timeout=30
    )
    r.raise_for_status()
    data = r.json()
    if not data:
        raise RuntimeError(f"Nominatim couldn't find {city}, {country}")
    south, north, west, east = map(float, data[0]["boundingbox"])
    return south, west, north, east


# ---------- OSM candidates ----------
# Expanded niche coverage (roughly 2x+)
OSM_FILTERS = [
    # Real estate / property
    ("office", "estate_agent"),
    ("office", "real_estate"),
    ("office", "property_management"),
    ("shop", "estate_agent"),
    ("shop", "real_estate"),

    # Legal / finance
    ("office", "lawyer"),
    ("office", "solicitor"),
    ("office", "notary"),
    ("office", "legal"),
    ("office", "accountant"),
    ("office", "tax_advisor"),
    ("office", "financial_advisor"),
    ("office", "insurance"),
    ("office", "bank"),

    # Medical / healthcare
    ("amenity", "dentist"),
    ("amenity", "doctors"),
    ("amenity", "clinic"),
    ("amenity", "hospital"),
    ("amenity", "pharmacy"),
    ("amenity", "veterinary"),
    ("healthcare", "physiotherapist"),
    ("healthcare", "chiropractor"),
    ("shop", "optician"),

    # Marketing / design / IT services
    ("office", "advertising_agency"),
    ("office", "marketing"),
    ("office", "design"),
    ("office", "it"),
    ("office", "consulting"),
    ("office", "architect"),
    ("office", "coworking"),

    # Fitness / wellness
    ("leisure", "fitness_centre"),
    ("leisure", "sports_centre"),
    ("amenity", "gym"),
    ("leisure", "yoga"),
    ("leisure", "dance"),
    ("shop", "massage"),

    # Beauty / grooming
    ("shop", "hairdresser"),
    ("shop", "barber"),
    ("shop", "beauty"),
    ("shop", "cosmetics"),
    ("amenity", "spa"),
    ("amenity", "beauty_salon"),
    ("shop", "nail_salon"),

    # Auto
    ("shop", "car_repair"),
    ("shop", "car"),
    ("amenity", "car_wash"),
    ("amenity", "vehicle_inspection"),
    ("shop", "tyres"),
    ("shop", "motorcycle_repair"),

    # Trades / home services
    ("craft", "plumber"),
    ("craft", "electrician"),
    ("craft", "hvac"),
    ("craft", "roofer"),
    ("craft", "carpenter"),
    ("craft", "painter"),
    ("craft", "locksmith"),
    ("craft", "glaziery"),
    ("craft", "tiler"),
    ("craft", "gardener"),

    # Food / hospitality (very common SEO buyers)
    ("amenity", "restaurant"),
    ("amenity", "cafe"),
    ("amenity", "fast_food"),
    ("tourism", "hotel"),
    ("tourism", "guest_house"),
]

OVERPASS_ENDPOINTS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
]

def _overpass_post(query: str) -> Optional[dict]:
    body = (query or "").encode("utf-8")
    for base_url in OVERPASS_ENDPOINTS:
        for attempt in range(1, OVERPASS_RETRIES + 1):
            try:
                throttle("overpass", OVERPASS_MIN_INTERVAL_S)
                r = SESS.post(base_url, data=body, timeout=OVERPASS_TIMEOUT_S)
                if r.status_code == 200:
                    return r.json()
                dbg(f"[overpass] HTTP {r.status_code} via {base_url} attempt={attempt}")
            except Exception as e:
                dbg(f"[overpass] error via {base_url} attempt={attempt}: {e}")
                continue
    return None

def _chunked(lst, n):
    if n <= 0:
        yield lst
        return
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def _overpass_query_for_filters(filters: List[Tuple[str,str]], lat: float, lon: float, radius_m: int) -> str:
    parts = []
    for k, v in filters:
        for t in ("node", "way", "relation"):
            parts.append(f'{t}(around:{radius_m},{lat},{lon})["{k}"="{v}"];')
    return f"""[out:json][timeout:{OVERPASS_QUERY_TIMEOUT}];({ ' '.join(parts) });out tags center;"""

def overpass_local_businesses(lat: float, lon: float, radius_m: int) -> List[dict]:
    if not OVERPASS_ENABLED:
        return []

    elements_all = []

    # chunk big filter sets to reduce query-size/timeouts
    for filt_chunk in _chunked(OSM_FILTERS, OVERPASS_FILTER_CHUNK):
        q = _overpass_query_for_filters(filt_chunk, lat, lon, radius_m)
        js = _overpass_post(q)
        if js and js.get("elements"):
            elements_all.extend(js.get("elements", []))

    if not elements_all:
        return []

    rows = []
    for el in elements_all:
        tags = el.get("tags", {}) or {}
        name = (tags.get("name") or "").strip()
        if not name:
            continue
        website = tags.get("website") or tags.get("contact:website") or tags.get("url")
        wikidata = tags.get("wikidata")

        lat2 = el.get("lat")
        lon2 = el.get("lon")
        if (lat2 is None or lon2 is None) and isinstance(el.get("center"), dict):
            lat2 = el["center"].get("lat")
            lon2 = el["center"].get("lon")

        rows.append({
            "business_name": name,
           
