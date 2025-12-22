# seo_leads_to_trello.py
#
# Generic local-business lead finder for SEO outreach.
#
# What it does:
#   - Picks cities in mostly English-speaking / English-heavy markets:
#       US, UK, Canada, Australia, New Zealand, Ireland,
#       Singapore, UAE (Dubai/Abu Dhabi), Hong Kong.
#   - Uses Nominatim to geocode city
#   - Uses Overpass + Nominatim POI search to find local businesses in MANY niches:
#       * Real estate agencies / property management
#       * Law firms / lawyers
#       * Dentists / medical clinics
#       * Accountants / tax advisors / financial advisors
#       * Marketing / web / advertising agencies
#       * Gyms / fitness studios
#       * Beauty salons / hairdressers / spas
#       * Car repair / garages / vehicle services
#       * Home services where tagged (plumber, electrician, roofer, etc.)
#   - Resolves websites (direct tag, Wikidata, Nominatim, Foursquare)
#   - Deduplicates by domain, checks robots.txt, checks site is up
#   - Tries to keep ONLY "small-ish" sites, skipping very large/complex ones:
#       * If HTML bigger than MAX_HTML_KB_SMALL_SITE
#       * Or too many <script> tags
#   - Writes leads to CSV
#   - Fills Trello template cards with:
#       Company, Website
#     preserving existing:
#       First, Email, Hook, Variant
#
# No emails are scraped. No contact pages, nothing aggressive; just business name + website.

import os, re, json, time, random, csv, pathlib, math
from datetime import date, datetime
from urllib.parse import urljoin, urlparse
from typing import Optional, List, Dict, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import tldextract
import urllib.robotparser as robotparser
from functools import lru_cache

from bs4 import BeautifulSoup
import re

import subprocess
import json
import os
from pathlib import Path


def enrich_from_linkedin(profile_username):
    """
    Runs the linkedin_people_profile spider for the given username
    and returns a dict with name, job title, and LinkedIn URL.
    """
    output_file = f"data/linkedin_people_profile.jsonl"
    
    # Remove old output if it exists
    try:
        os.remove(output_file)
    except FileNotFoundError:
        pass

    # Run the spider
    subprocess.run([
        "scrapy", "crawl", "linkedin_people_profile",
        "-a", f"profile_list={profile_username}"
    ], check=True)

    # Read the output
    if Path(output_file).exists():
        with open(output_file, "r") as f:
            for line in f:
                try:
                    data = json.loads(line)
                    return {
                        "first_name": data.get("name", "").split(" ")[0],
                        "job_title": data.get("description", ""),
                        "linkedin_url": data.get("url", "")
                    }
                except Exception as e:
                    print("Error reading LinkedIn profile data:", e)
    
    return {
        "first_name": None,
        "job_title": None,
        "linkedin_url": None
    }

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
DAILY_LIMIT      = env_int("DAILY_LIMIT", 5)
PUSH_INTERVAL_S  = env_int("PUSH_INTERVAL_S", 20)
REQUEST_DELAY_S  = env_float("REQUEST_DELAY_S", 0.2)
SEEN_FILE        = os.getenv("SEEN_FILE", "seen_domains.txt")

# "Small site" filtering
SMALL_SITE_ONLY          = env_on("SMALL_SITE_ONLY", True)
MAX_HTML_KB_SMALL_SITE   = env_int("MAX_HTML_KB_SMALL_SITE", 600)   # ~600 KB HTML
MAX_SCRIPTS_SMALL_SITE   = env_int("MAX_SCRIPTS_SMALL_SITE", 40)    # max script tags before we consider it "big"

# Batch rotation for scheduling (e.g. "Monday morning", "Monday afternoon", etc.)
BATCH_FILE = os.getenv("BATCH_FILE", "batch_state.txt")
BATCH_SLOTS = [
    "m friday",
    "a friday",
    "m monday",
    "a monday",
    "m tuesday",
    "a tuesday",
    "m wednesday",
    "a wednesday",
    "m thursday",
    "a thursday",
]


def find_linkedin_profile(company_name):
    import requests
    from bs4 import BeautifulSoup

    query = f'site:linkedin.com/in "{company_name}" CEO OR Founder'
    headers = {"User-Agent": "Mozilla/5.0"}
    search_url = f"https://www.bing.com/search?q={requests.utils.quote(query)}"
    
    try:
        r = requests.get(search_url, headers=headers, timeout=10)
        soup = BeautifulSoup(r.text, 'html.parser')
        for a in soup.find_all('a', href=True):
            href = a['href']
            if 'linkedin.com/in/' in href:
                # Extract the LinkedIn username/slug
                parts = href.split('/')
                if 'in' in parts:
                    idx = parts.index('in')
                    profile_slug = parts[idx+1]
                    return profile_slug
    except Exception as e:
        print(f"Error finding LinkedIn profile: {e}")
    
    return None


def run_linkedin_spider(profile_slug):
    import subprocess
    import os
    import json

    try:
        # Remove previous data file if it exists
        data_file = f"data/linkedin_people_profile.jsonl"
        if os.path.exists(data_file):
            os.remove(data_file)

        # Run Scrapy spider via subprocess
        subprocess.run([
            "scrapy", "crawl", "linkedin_people_profile",
            "-a", f"profile_list={profile_slug}"
        ], cwd="linkedin", check=True)

        # Load the results
        with open(data_file, "r") as f:
            for line in f:
                data = json.loads(line)
                return data  # return the first (and only) profile scraped

    except Exception as e:
        print(f"[LinkedIn Spider] Error: {e}")

    return None


def enrich_website_contacts(base_url):
    enriched_data = {
        "first_name": None,
        "job_title": None,
        "email": None
    }

    potential_paths = ["/contact", "/team", "/about", "/about-us", "/contact-us"]
    visited_urls = set()

    for path in potential_paths:
        full_url = urljoin(base_url, path)
        if full_url in visited_urls:
            continue
        visited_urls.add(full_url)

        try:
            headers = {"User-Agent": "Mozilla/5.0"}
            response = requests.get(full_url, headers=headers, timeout=10)
            if response.status_code != 200:
                continue

            soup = BeautifulSoup(response.text, 'html.parser')
            text = soup.get_text()

            # Find emails
            emails = re.findall(r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+', text)
            if emails:
                enriched_data["email"] = emails[0]

            # Look for names + titles
            for tag in soup.find_all(['p', 'div', 'li', 'span']):
                t = tag.get_text(" ", strip=True)
                if len(t.split()) <= 10 and any(role in t.lower() for role in ['ceo', 'founder', 'owner', 'marketing', 'sales']):
                    enriched_data["first_name"] = t.split()[0]
                    enriched_data["job_title"] = ' '.join(t.split()[1:])
                    break

            if enriched_data["email"]:
                break

        except Exception as e:
            print(f"[enrich_website_contacts] error scraping {full_url}: {e}")
            continue

        time.sleep(1)

    return enriched_data

def load_batch_index() -> int:
    try:
        with open(BATCH_FILE, "r", encoding="utf-8") as f:
            val = f.read().strip()
            idx = int(val)
            if 0 <= idx < len(BATCH_SLOTS):
                return idx
    except Exception:
        pass
    return 0

def save_batch_index(idx: int) -> None:
    try:
        os.makedirs(os.path.dirname(BATCH_FILE) or ".", exist_ok=True)
        with open(BATCH_FILE, "w", encoding="utf-8") as f:
            f.write(str(idx))
    except Exception:
        pass

BUTLER_GRACE_S   = env_int("BUTLER_GRACE_S", 15)
DEBUG            = env_on("DEBUG", False)

# pre-clone toggle (disabled by default)
PRECLONE   = env_on("PRECLONE", False)

# OSM candidates source toggles
OVERPASS_ENABLED        = env_on("OVERPASS_ENABLED", 1)
NOMINATIM_POI_ENABLED   = env_on("NOMINATIM_POI_ENABLED", 1)
OVERPASS_NAME_LOOKUP_ENABLED = env_on("OVERPASS_NAME_LOOKUP_ENABLED", 0)

# Overpass tuning (GH Actions often times out)
OVERPASS_TIMEOUT_S      = env_int("OVERPASS_TIMEOUT_S", 45)
OVERPASS_QUERY_TIMEOUT  = env_int("OVERPASS_QUERY_TIMEOUT", 25)
OVERPASS_RETRIES        = env_int("OVERPASS_RETRIES", 2)
OVERPASS_MIN_INTERVAL_S = env_float("OVERPASS_MIN_INTERVAL_S", 2.0)

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
TRELLO_TEMPLATE_CARD_ID = os.getenv("TRELLO_TEMPLATE_CARD_ID")  # optional, if you want cloning

# Discovery (Foursquare v3 = single API Key) — optional
FOURSQUARE_API_KEY = os.getenv("FOURSQUARE_API_KEY")

STATS = {
    "osm_candidates": 0,
    "cand_overpass": 0,
    "cand_nominatim_poi": 0,
    "skip_no_website": 0,
    "skip_dupe_domain": 0,
    "skip_robots": 0,
    "skip_fetch": 0,
    "skip_big_site": 0,
    "website_direct": 0,
    "website_overpass_name": 0,
    "website_nominatim": 0,
    "website_fsq": 0,
    "website_wikidata": 0,
}

def dbg(msg):
    if DEBUG:
        print(msg, flush=True)

# ---------- threading-free throttling ----------
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
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
    )
except TypeError:
    # older urllib3
    _retries = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        method_whitelist=frozenset({"GET"}),
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

# NEW: Focus on English-heavy markets only
CITY_ROTATION = [
    # United States
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

    # United Kingdom
    ("London","United Kingdom"),
    ("Manchester","United Kingdom"),
    ("Birmingham","United Kingdom"),
    ("Edinburgh","United Kingdom"),
    ("Glasgow","United Kingdom"),

    # Canada
    ("Toronto","Canada"),
    ("Vancouver","Canada"),
    ("Montreal","Canada"),
    ("Calgary","Canada"),
    ("Ottawa","Canada"),

    # Australia
    ("Sydney","Australia"),
    ("Melbourne","Australia"),
    ("Brisbane","Australia"),
    ("Perth","Australia"),
    ("Adelaide","Australia"),

    # New Zealand
    ("Auckland","New Zealand"),
    ("Wellington","New Zealand"),
    ("Christchurch","New Zealand"),

    # Ireland
    ("Dublin","Ireland"),
    ("Cork","Ireland"),

    # Singapore
    ("Singapore","Singapore"),

    # UAE (English-heavy, especially Dubai)
    ("Dubai","United Arab Emirates"),
    ("Abu Dhabi","United Arab Emirates"),

    # Hong Kong
    ("Hong Kong","China"),
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
        # plain email typed as "a@b.com"
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
            rp.parse([])  # allow all
            return rp
        rp.parse(resp.text.splitlines())
        return rp
    except Exception:
        rp.parse([])  # allow all
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

# ---------- "small site" heuristic ----------
def is_probably_small_site(html_text: str, url: str) -> bool:
    """
    SUPER SIMPLE heuristic to skip obviously "big" / complex sites:
      - if HTML > MAX_HTML_KB_SMALL_SITE
      - or too many <script> tags
    This is not perfect, but it heavily biases toward local/smaller sites.
    """
    if not SMALL_SITE_ONLY:
        return True

    if not html_text:
        return True

    size_bytes = len(html_text.encode("utf-8", errors="ignore"))
    if size_bytes > MAX_HTML_KB_SMALL_SITE * 1024:
        dbg(f"[big-site] {url} skipped: HTML {size_bytes/1024:.1f} KB")
        return False

    lower = html_text.lower()
    script_count = lower.count("<script")
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

# ---------- OSM candidates (Overpass + Nominatim POI fallback) ----------

# MULTI-NICHE FILTERS:
OSM_FILTERS = [
    # Real estate
    ("office", "estate_agent"),
    ("office", "real_estate"),
    ("office", "property_management"),
    ("shop", "estate_agent"),
    ("shop", "real_estate"),

    # Legal / professional / financial
    ("office", "lawyer"),
    ("office", "solicitor"),
    ("office", "legal"),
    ("office", "accountant"),
    ("office", "tax_advisor"),
    ("office", "financial_advisor"),
    ("office", "insurance"),

    # Medical / dental
    ("amenity", "dentist"),
    ("amenity", "doctors"),
    ("amenity", "clinic"),
    ("amenity", "hospital"),

    # Marketing / web / design
    ("office", "advertising_agency"),
    ("office", "marketing"),
    ("office", "design"),

    # Fitness
    ("leisure", "fitness_centre"),
    ("leisure", "sports_centre"),
    ("amenity", "gym"),

    # Beauty / wellness
    ("shop", "hairdresser"),
    ("shop", "beauty"),
    ("shop", "cosmetics"),
    ("amenity", "spa"),
    ("amenity", "beauty_salon"),

    # Auto / vehicle services
    ("shop", "car_repair"),
    ("shop", "car"),
    ("amenity", "car_wash"),
    ("amenity", "vehicle_inspection"),

    # Home services (where explicitly tagged as crafts)
    ("craft", "plumber"),
    ("craft", "electrician"),
    ("craft", "hvac"),
    ("craft", "roofer"),
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

def overpass_local_businesses(lat: float, lon: float, radius_m: int) -> List[dict]:
    if not OVERPASS_ENABLED:
        return []
    parts = []
    for k, v in OSM_FILTERS:
        for t in ("node", "way", "relation"):
            parts.append(f'{t}(around:{radius_m},{lat},{lon})["{k}"="{v}"];')
    q = f"""[out:json][timeout:{OVERPASS_QUERY_TIMEOUT}];({ ' '.join(parts) });out tags center;"""

    js = _overpass_post(q)
    if not js:
        return []

    rows = []
    for el in js.get("elements", []):
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
            "website": normalize_url(website) if website else None,
            "wikidata": wikidata,
            "lat": lat2,
            "lon": lon2,
        })

    dedup = {}
    for r0 in rows:
        key = (r0["business_name"].lower(), etld1_from_url(r0["website"] or ""))
        if key not in dedup:
            dedup[key] = r0

    out = list(dedup.values())
    random.shuffle(out)
    STATS["cand_overpass"] += len(out)
    return out

def _viewbox_param(south: float, west: float, north: float, east: float) -> str:
    # Nominatim expects: left,top,right,bottom
    return f"{west},{north},{east},{south}"

def _guess_name_from_nominatim(item: dict) -> str:
    nd = item.get("namedetails") or {}
    nm = (nd.get("name") or item.get("name") or "").strip()
    if nm:
        return nm
    dn = (item.get("display_name") or "").strip()
    if dn and "," in dn:
        return dn.split(",", 1)[0].strip()
    return dn

def _nominatim_poi_queries_for(country: str) -> List[str]:
    """
    Multi-niche keyword list for Nominatim POI search.
    Adjusted per country when possible, but we mostly use English terms
    because we're focusing on English-heavy markets anyway.
    """
    c = (country or "").lower().strip()

    base = [
        "real estate agency",
        "property management",
        "law firm",
        "attorney",
        "dental clinic",
        "dentist",
        "medical clinic",
        "doctor",
        "accounting firm",
        "tax advisor",
        "bookkeeping",
        "marketing agency",
        "digital marketing",
        "web design agency",
        "gym",
        "fitness studio",
        "fitness centre",
        "beauty salon",
        "hairdresser",
        "spa",
        "car repair",
        "auto repair",
        "garage",
        "plumber",
        "electrician",
    ]

    # You *could* add local-language variants here, but since we're focusing on
    # English-heavy markets, the English queries are often enough.
    return base

def nominatim_poi_candidates(city: str, country: str, south: float, west: float, north: float, east: float) -> List[dict]:
    if not NOMINATIM_POI_ENABLED:
        return []
    vb = _viewbox_param(south, west, north, east)
    queries = _nominatim_poi_queries_for(country)
    random.shuffle(queries)
    queries = queries[:max(1, NOMINATIM_POI_QUERIES_PER_CITY)]

    out: List[dict] = []
    seen_key = set()

    for qstr in queries:
        try:
            throttle("nominatim_poi", 1.1)
            r = SESS.get(
                "https://nominatim.openstreetmap.org/search",
                params={
                    "q": f"{qstr} {city} {country}",
                    "format": "jsonv2",
                    "limit": NOMINATIM_LIMIT,
                    "viewbox": vb,
                    "bounded": 1,
                    "dedupe": 1,
                    "extratags": 1,
                    "namedetails": 1,
                },
                headers={"Referer":"https://nominatim.org"},
                timeout=30,
            )
            if r.status_code != 200:
                continue
            items = r.json() or []
        except Exception as e:
            dbg(f"[nominatim_poi] error: {e}")
            continue

        for it in items:
            nm = _guess_name_from_nominatim(it).strip()
            if not nm:
                continue

            klass = (it.get("class") or "").lower()
            typ   = (it.get("type") or "").lower()

            if klass and klass not in ("office", "shop", "amenity", "tourism", "leisure", "craft"):
                continue
            if typ in ("house", "residential", "road", "yes", "city", "town", "village", "suburb", "neighbourhood"):
                continue

            xt = it.get("extratags") or {}
            website = xt.get("website") or xt.get("contact:website") or xt.get("url")

            try:
                lat2 = float(it.get("lat")) if it.get("lat") is not None else None
                lon2 = float(it.get("lon")) if it.get("lon") is not None else None
            except Exception:
                lat2, lon2 = None, None

            website = normalize_url(website) if website else None
            key = (nm.lower(), etld1_from_url(website or "") or f"{lat2},{lon2}")
            if key in seen_key:
                continue
            seen_key.add(key)

            out.append({
                "business_name": nm,
                "website": website,
                "wikidata": xt.get("wikidata"),
                "lat": lat2,
                "lon": lon2,
            })

    random.shuffle(out)
    STATS["cand_nominatim_poi"] += len(out)
    return out

def get_osm_candidates(city: str, country: str, lat: float, lon: float,
                       south: float, west: float, north: float, east: float) -> List[dict]:
    cands: List[dict] = []
    if OVERPASS_ENABLED:
        for rad in (OSM_RADIUS_M, max(800, OSM_RADIUS_M // 2), max(500, OSM_RADIUS_M // 3)):
            cands = overpass_local_businesses(lat, lon, rad)
            if cands:
                break
    if (not cands) and NOMINATIM_POI_ENABLED:
        cands = nominatim_poi_candidates(city, country, south, west, north, east)
    return cands

# ---------- website resolution helpers ----------

LEGAL_SUFFIXES = [
    "ag","gmbh","sa","sarl","sàrl","llc","ltd","limited","inc","corp","s.p.a","spa","bv","nv",
    "kg","ohg","ug","gbr","kft","sro","s.r.o","oy","ab","as","aps"
]

def _norm_name(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[\u2019'`\".,:;()\\-_/\\\\]+", " ", s)
    parts = [p for p in s.split() if p and p not in LEGAL_SUFFIXES]
    return " ".join(parts)

def _escape_overpass_regex(s: str) -> str:
    return re.sub(r'([.^$*+?{}$begin:math:display$$end:math:display$\\|()])', r'\\\1', s)

def _haversine_km(lat1, lon1, lat2, lon2) -> float:
    if None in (lat1, lon1, lat2, lon2):
        return 999999.0
    R = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2-lat1)
    dl   = math.radians(lon2-lon1)
    a = math.sin(dphi/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
    return 2*R*math.asin(math.sqrt(a))

def overpass_lookup_website_by_name(name: str, lat: float, lon: float, radius_m: int = 20000) -> Optional[str]:
    if not (OVERPASS_ENABLED and OVERPASS_NAME_LOOKUP_ENABLED):
        return None
    if not name or lat is None or lon is None:
        return None
    n = _norm_name(name)
    if not n:
        return None

    tokens = [t for t in n.split() if len(t) >= 3] or n.split()
    tokens = tokens[:5]
    pattern = ".*".join(_escape_overpass_regex(t) for t in tokens)

    q = f"""
[out:json][timeout:{OVERPASS_QUERY_TIMEOUT}];
(
  node(around:{radius_m},{lat},{lon})["name"~"{pattern}",i];
  way(around:{radius_m},{lat},{lon})["name"~"{pattern}",i];
  relation(around:{radius_m},{lat},{lon})["name"~"{pattern}",i];
);
out tags center;
"""
    js = _overpass_post(q)
    if not js:
        return None

    best = None
    best_score = -1e9

    for el in js.get("elements", []):
        tags = el.get("tags", {}) or {}
        nm = (tags.get("name") or "").strip()
        w  = tags.get("website") or tags.get("contact:website") or tags.get("url")
        if not w:
            continue

        lat2 = el.get("lat")
        lon2 = el.get("lon")
        if (lat2 is None or lon2 is None) and isinstance(el.get("center"), dict):
            lat2 = el["center"].get("lat")
            lon2 = el["center"].get("lon")

        dist = _haversine_km(lat, lon, lat2, lon2)
        nm_norm = _norm_name(nm)

        score = 0.0
        if nm_norm == n:
            score += 50
        elif n and n in nm_norm:
            score += 30
        else:
            overlap = len(set(n.split()) & set(nm_norm.split()))
            score += overlap * 6

        score += max(0.0, 20.0 - dist)

        w0 = normalize_url(w)
        dom = etld1_from_url(w0 or "")
        if dom:
            score += 5

        if score > best_score:
            best_score = score
            best = w

    return normalize_url(best) if best else None

def nominatim_lookup_website(name: str, city: str, country: str, limit: int = 8) -> Optional[str]:
    if not name:
        return None
    try:
        throttle("nominatim_lookup", 1.1)
        q = f"{name}, {city}, {country}".strip(", ")
        r = SESS.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": q, "format":"jsonv2", "limit": limit, "extratags": 1, "namedetails": 1},
            headers={"Referer":"https://nominatim.org"},
            timeout=30
        )
        if r.status_code != 200:
            return None
        items = r.json() or []
        for it in items:
            xt = it.get("extratags") or {}
            w = xt.get("website") or xt.get("contact:website") or xt.get("url")
            w = normalize_url(w)
            if w:
                return w
    except Exception:
        return None
    return None

@lru_cache(maxsize=4096)
def wikidata_website_from_qid(qid: str) -> Optional[str]:
    if not qid or not qid.startswith("Q"):
        return None
    try:
        throttle("wikidata", 0.6)
        r = SESS.get(f"https://www.wikidata.org/wiki/Special:EntityData/{qid}.json", timeout=20)
        if r.status_code != 200:
            return None
        js = r.json()
        ent = (js.get("entities") or {}).get(qid) or {}
        claims = ent.get("claims") or {}
        for cl in claims.get("P856", []):
            dv = (((cl.get("mainsnak") or {}).get("datavalue") or {}).get("value") or "")
            w = normalize_url(dv)
            if w:
                return w
    except Exception:
        return None
    return None

def fsq_find_website(name, lat, lon):
    if not FOURSQUARE_API_KEY:
        return None
    headers = {"Authorization": FOURSQUARE_API_KEY, "Accept":"application/json"}
    try:
        throttle("foursquare", 0.6)
        params = {"query": name, "ll": f"{lat},{lon}", "limit": 1, "radius": 50000}
        r = SESS.get("https://api.foursquare.com/v3/places/search",
                     headers=headers, params=params, timeout=20)
        if r.status_code == 200:
            results = (r.json().get("results") or [])
            if results:
                first = results[0]
                website = first.get("website")
                if website:
                    return normalize_url(website)
                fsq_id = first.get("fsq_id")
                if fsq_id:
                    throttle("foursquare", 0.6)
                    d = SESS.get(f"https://api.foursquare.com/v3/places/{fsq_id}",
                                 headers=headers, params={"fields":"website"}, timeout=20)
                    if d.status_code == 200:
                        w = d.json().get("website")
                        if w:
                            return normalize_url(w)
    except Exception:
        return None
    return None

def resolve_website(biz_name: str, city: str, country: str, lat: float, lon: float,
                    direct: Optional[str], wikidata_qid: Optional[str] = None) -> Optional[str]:
    w = normalize_url(direct)
    if w:
        STATS["website_direct"] += 1
        return w

    w = wikidata_website_from_qid(wikidata_qid or "")
    if w:
        STATS["website_wikidata"] += 1
        return w

    w = nominatim_lookup_website(biz_name, city, country, limit=8)
    if w:
        STATS["website_nominatim"] += 1
        return w

    w = overpass_lookup_website_by_name(biz_name, lat, lon, radius_m=20000)
    if w:
        STATS["website_overpass_name"] += 1
        return w

    w = normalize_url(fsq_find_website(biz_name, lat, lon))
    if w:
        STATS["website_fsq"] += 1
        return w

    return None

# ---------- Trello helpers ----------
TARGET_LABELS = ["Company","First","Email","Hook","Variant","Website"]
LABEL_RE = {lab: re.compile(rf'(?mi)^\s*{re.escape(lab)}\s*:\s*(.*)$') for lab in TARGET_LABELS}

def trello_get_card(card_id):
    r = SESS.get(
        f"https://api.trello.com/1/cards/{card_id}",
        params={"key": TRELLO_KEY, "token": TRELLO_TOKEN, "fields": "name,desc"},
        timeout=30,
    )
    r.raise_for_status()
    js = r.json()
    desc = (js.get("desc") or "").replace("\r\n", "\n").replace("\r", "\n")
    name = js.get("name") or ""
    return {"name": name, "desc": desc}

def extract_label_value(desc: str, label: str) -> str:
    d = (desc or "").replace("\r\n", "\n").replace("\r", "\n")
    lines = d.splitlines()
    i = 0
    while i < len(lines):
        line = lines[i]
        m = LABEL_RE[label].match(line)
        if m:
            val = (m.group(1) or "").strip()
            if not val and (i + 1) < len(lines):
                nxt = lines[i + 1]
                if nxt.strip() and not any(LABEL_RE[L].match(nxt) for L in TARGET_LABELS):
                    val = nxt.strip()
                    i += 1
            return val
        i += 1
    return ""

def _split_header_rest(desc: str):
    d = (desc or "").replace("\r\n", "\n").replace("\r", "\n")
    lines = d.splitlines()
    i = 0
    while i < len(lines) and lines[i].strip() == "":
        i += 1
    if i >= len(lines) or not any(LABEL_RE[lab].match(lines[i]) for lab in TARGET_LABELS):
        return [], lines

    header = []
    seen_labels = set()
    started = False

    while i < len(lines):
        line = lines[i]
        m_lab = None
        for lab in TARGET_LABELS:
            m = LABEL_RE[lab].match(line)
            if m:
                m_lab = lab
                break
        if m_lab:
            started = True
            header.append(line)
            seen_labels.add(m_lab)
            val = (LABEL_RE[m_lab].match(line).group(1) or "").strip()
            if not val and (i + 1) < len(lines):
                nxt = lines[i + 1]
                if nxt.strip() and not any(LABEL_RE[L].match(nxt) for L in TARGET_LABELS):
                    header.append(nxt)
                    i += 1
            i += 1
            continue

        if line.strip() == "":
            header.append(line)
            i += 1
            if "Website" in seen_labels:
                break
            continue

        if started:
            break

        i += 1

    rest = lines[i:]
    return header, rest

def normalize_header_block(desc: str, company: str, website: str, batch: Optional[str] = None) -> str:
    desc = (desc or "").replace("\r\n", "\n").replace("\r", "\n")
    header_lines, rest_lines = _split_header_rest(desc)

    # preserve existing values (including Email)
    preserved = {"First": "", "Email": "", "Hook": "", "Variant": ""}

    i = 0
    while i < len(header_lines):
        line = header_lines[i]
        for lab in TARGET_LABELS:
            m = LABEL_RE[lab].match(line)
            if not m:
                continue
            val = (m.group(1) or "").strip()
            if not val and (i + 1) < len(header_lines):
                nxt = header_lines[i + 1]
                if nxt.strip() and not any(LABEL_RE[L].match(nxt) for L in TARGET_LABELS):
                    val = nxt.strip()
                    i += 1
            if lab in preserved and preserved[lab] == "":
                preserved[lab] = val
            break
        i += 1

    def hard(line: str) -> str:
        return (line or "").rstrip() + "  "

    new_header = [
        hard(f"Company: {company or ''}"),
        hard(f"First: {preserved['First']}"),
        hard(f"Email: {preserved['Email']}"),
        hard(f"Hook: {preserved['Hook']}"),
        hard(f"Variant: {preserved['Variant']}"),
        hard(f"Website: {website or ''}"),
        "",
    ]

    if batch:
        has_batch = any((line or "").strip() == batch for line in rest_lines)
        if not has_batch:
            if rest_lines and rest_lines[-1].strip() != "":
                rest_lines.append("")
            rest_lines.append(batch)

    out = ("\n".join(new_header + rest_lines)).rstrip("\n") + "\n\n@lead\n"
    return out

def update_card_header(card_id: str, company: str, website: str,
                       new_name: Optional[str] = None,
                       batch: Optional[str] = None) -> bool:
    cur = trello_get_card(card_id)
    desc_old = cur["desc"]
    name_old = cur["name"]

    desc_new = normalize_header_block(desc_old, company, website, batch=batch)

    payload = {}
    if desc_new != desc_old:
        payload["desc"] = desc_new

    desired_name = (new_name or "").strip()
    if desired_name and desired_name != name_old.strip():
        payload["name"] = desired_name

    if not payload:
        return False

    r = SESS.put(
        f"https://api.trello.com/1/cards/{card_id}",
        params={"key": TRELLO_KEY, "token": TRELLO_TOKEN},
        data=payload,
        timeout=30,
    )
    r.raise_for_status()
    return True

def is_template_blank(desc: str) -> bool:
    d = (desc or "").replace("\r\n", "\n").replace("\r", "\n")
    company = extract_label_value(d, "Company").strip()
    website = extract_label_value(d, "Website").strip()
    if company == "" and website == "":
        return True
    if re.search(r"(?mi)^\s*Company\s*:\s*$", d) and re.search(r"(?mi)^\s*Website\s*:\s*$", d):
        return True
    return False

def find_empty_template_cards(list_id: str, max_needed: int = 1) -> List[str]:
    r = SESS.get(
        f"https://api.trello.com/1/lists/{list_id}/cards",
        params={"key": TRELLO_KEY, "token": TRELLO_TOKEN, "fields": "id,name,desc"},
        timeout=30
    )
    r.raise_for_status()
    empties = []
    for c in r.json():
        if is_template_blank(c.get("desc") or ""):
            empties.append(c["id"])
        if len(empties) >= max_needed:
            break
    return empties

def clone_template_into_list(template_card_id: str, list_id: str, name: str="Lead (auto)"):
    if not template_card_id:
        return None
    r = SESS.post(
        "https://api.trello.com/1/cards",
        params={
            "key":TRELLO_KEY,
            "token":TRELLO_TOKEN,
            "idList":list_id,
            "idCardSource":template_card_id,
            "name":name
        },
        timeout=30
    )
    r.raise_for_status()
    return r.json()["id"]

def ensure_min_blank_templates(list_id: str, template_id: str, need: int):
    if need <= 0 or not template_id:
        return
    empties = find_empty_template_cards(list_id, max_needed=need)
    missing = max(0, need - len(empties))
    for i in range(missing):
        clone_template_into_list(template_id, list_id, name=f"Lead (auto) {int(time.time())%100000}-{i+1}")
        time.sleep(1.0)

# ---------- seen + CSV ----------
def load_seen():
    try:
        with open(SEEN_FILE, "r", encoding="utf-8") as f:
            return set(l.strip().lower() for l in f if l.strip())
    except Exception:
        return set()

def seen_domain_write(domain: str):
    if not domain:
        return
    d = domain.strip().lower()
    try:
        os.makedirs(os.path.dirname(SEEN_FILE) or ".", exist_ok=True)
        with open(SEEN_FILE, "a", encoding="utf-8") as f:
            f.write(d + "\n")
    except Exception:
        pass

def save_seen(seen: set):
    try:
        os.makedirs(os.path.dirname(SEEN_FILE) or ".", exist_ok=True)
        with open(SEEN_FILE, "w", encoding="utf-8") as f:
            for d in sorted(seen):
                f.write(d + "\n")
    except Exception:
        pass

def append_csv(leads, city, country):
    if not leads:
        return
    fname = os.getenv("LEADS_CSV", f"seo_leads_{date.today().isoformat()}.csv")
    file_exists = pathlib.Path(fname).exists()
    with open(fname, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if not file_exists:
            w.writerow(["timestamp","city","country","company","website"])
        ts = datetime.utcnow().isoformat(timespec="seconds")+"Z"
        for L in leads:
            w.writerow([ts, city, country, L["Company"], L["Website"]])

# ---------- main ----------
def main():
    missing = [n for n in ["TRELLO_KEY","TRELLO_TOKEN","TRELLO_LIST_ID"] if not os.getenv(n)]
    if missing:
        raise SystemExit(f"Missing env: {', '.join(missing)}")

    if not OVERPASS_ENABLED and not NOMINATIM_POI_ENABLED:
        raise SystemExit("Both OVERPASS_ENABLED and NOMINATIM_POI_ENABLED are disabled; no way to fetch candidates.")

    leads: List[dict] = []
    seen = load_seen()
    last_city = ""
    last_country = ""

    for (city, country) in iter_cities():
        print(f"\n=== CITY START: {city}, {country} ===", flush=True)
        t_city = time.time()
        last_city, last_country = city, country

        # --- geocode ---
        try:
            t_geo = time.time()
            south, west, north, east = geocode_city(city, country)
            lat = (south + north) / 2.0
            lon = (west + east) / 2.0
            print(f"[{city}, {country}] geocode OK -> {lat:.5f},{lon:.5f} (took {time.time()-t_geo:.1f}s)", flush=True)
        except Exception as e:
            print(f"[{city}, {country}] geocode FAILED: {e}", flush=True)
            continue

        # --- OSM / POI sources ---
        if len(leads) < DAILY_LIMIT:
            t_osm = time.time()
            print(f"[{city}] OSM search starting...", flush=True)

            cands = get_osm_candidates(city, country, lat, lon, south, west, north, east)
            STATS["osm_candidates"] += len(cands)
            via = "overpass+poi" if cands else "none"
            print(f"[{city}] OSM candidates: {len(cands)} (took {time.time()-t_osm:.1f}s) via {via}", flush=True)

            leads_before_osm = len(leads)

            for biz in cands:
                if len(leads) >= DAILY_LIMIT:
                    break

                lat0 = biz.get("lat") or lat
                lon0 = biz.get("lon") or lon

                website = resolve_website(
                    biz_name=biz["business_name"],
                    city=city,
                    country=country,
                    lat=lat0,
                    lon=lon0,
                    direct=biz.get("website"),
                    wikidata_qid=biz.get("wikidata"),
                )
                if not website:
                    STATS["skip_no_website"] += 1
                    continue

                site_dom = etld1_from_url(website)
                if site_dom and site_dom in seen:
                    STATS["skip_dupe_domain"] += 1
                    continue

                p = urlparse(website)
                base = f"{p.scheme}://{p.netloc}/"
                if not allowed_by_robots(base, "/"):
                    STATS["skip_robots"] += 1
                    continue

                try:
                    resp = fetch(website)
                except Exception:
                    STATS["skip_fetch"] += 1
                    continue

                html_text = ""
                try:
                    resp.encoding = resp.encoding or "utf-8"
                    html_text = resp.text
                except Exception:
                    html_text = resp.content.decode("utf-8", errors="ignore")

                if not is_probably_small_site(html_text, website):
                    STATS["skip_big_site"] += 1
                    continue

                leads.append({"Company": biz["business_name"], "Website": website})

                if site_dom:
                    seen_domain_write(site_dom)
                    seen.add(site_dom)

                _sleep()

            print(f"[{city}] OSM done: +{len(leads)-leads_before_osm} leads", flush=True)

        print(f"=== CITY END: {city} in {time.time()-t_city:.1f}s | total leads={len(leads)}/{DAILY_LIMIT} ===", flush=True)

        if len(leads) >= DAILY_LIMIT:
            break

    # Trim to limit
    if leads:
        leads = leads[:DAILY_LIMIT]

    # Ensure blank templates exist (optional)
    need = min(DAILY_LIMIT, len(leads))
    if PRECLONE and need > 0 and TRELLO_TEMPLATE_CARD_ID:
        ensure_min_blank_templates(TRELLO_LIST_ID, TRELLO_TEMPLATE_CARD_ID, need)

    # Save CSV
    if leads and last_city and last_country:
        append_csv(leads, last_city, last_country)

    # Push to Trello
    def push_one_lead(lead: dict, seen: set, batch_label: Optional[str] = None) -> bool:
    empties = find_empty_template_cards(TRELLO_LIST_ID, max_needed=1)
    if not empties:
        print("No empty template card available; skipping push.", flush=True)
        return False

    card_id = empties[0]

    # 1) Website enrichment
    enriched = enrich_website_contacts(lead["Website"])
    first = enriched.get("first_name") or ""
    email = enriched.get("email") or ""
    hook = enriched.get("job_title") or ""

    # 2) LinkedIn enrichment
    profile_slug = find_linkedin_profile(lead["Company"])
    if profile_slug:
        linkedin_data = run_linkedin_spider(profile_slug)
        if linkedin_data:
            first = linkedin_data.get("name", first).split(" ")[0]
            hook = linkedin_data.get("description", hook)

    # 3) Build Trello description
    desc_old = trello_get_card(card_id)["desc"]
    desc_new = normalize_header_block(
        desc_old,
        company=lead["Company"],
        website=lead["Website"],
        batch=batch_label
    )

    desc_new = desc_new.replace("First:  ", f"First: {first}  ")
    desc_new = desc_new.replace("Email:  ", f"Email: {email}  ")
    desc_new = desc_new.replace("Hook:  ", f"Hook: {hook}  ")

    # 4) Push to Trello
    r = SESS.put(
        f"https://api.trello.com/1/cards/{card_id}",
        params={"key": TRELLO_KEY, "token": TRELLO_TOKEN},
        data={"desc": desc_new, "name": lead["Company"]},
        timeout=30,
    )
    r.raise_for_status()

    # 5) Mark domain as seen
    dom = etld1_from_url(lead.get("Website") or "")
    if dom:
        seen_domain_write(dom)
        seen.add(dom)

    print(f"PUSHED ✅ — {lead['Company']} — {lead['Website']}", flush=True)
    return True

    batch_idx = load_batch_index()
    batch_label = BATCH_SLOTS[batch_idx]
    next_batch_idx = (batch_idx + 1) % len(BATCH_SLOTS)

    pushed = 0
    for lead in leads:
        if pushed >= DAILY_LIMIT:
            break
        ok = push_one_lead(lead, seen, batch_label=batch_label)
        if ok:
            pushed += 1
            time.sleep(max(0, PUSH_INTERVAL_S) + max(0, BUTLER_GRACE_S))

    if pushed > 0:
        save_batch_index(next_batch_idx)

    if DEBUG:
        print("Stats:", json.dumps(STATS, indent=2), flush=True)

    save_seen(seen)

    print(f"SEEN_FILE path: {os.path.abspath(SEEN_FILE)} — total domains in set: {len(seen)}", flush=True)
    print(f"Done. Leads pushed: {pushed}/{len(leads)}", flush=True)

if __name__ == "__main__":
    main()
