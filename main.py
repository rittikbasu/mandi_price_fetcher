import os
import sys
import time
import re
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Optional, Dict, Any, List

import requests
from dotenv import load_dotenv
from supabase import create_client
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_API_KEY")

SUPABASE_TABLE = "mandi_prices_duplicate"
LIMIT = int(os.getenv("LIMIT", "200"))
SLEEP = float(os.getenv("SLEEP", "0"))
RESUME_FROM_DB = os.getenv("RESUME_FROM_DB", "1").strip() not in (
    "0",
    "false",
    "False",
    "no",
    "NO",
)

DATA_API_KEY = "579b464db66ec23bdd000001cdc3b564546246a772a26393094f5645"
RESOURCE_ID = "9ef84268-d588-465a-a308-a864a43d0070"

TIMEOUT_CONNECT = 10
TIMEOUT_READ = 120

LOG_DIR = os.path.join(BASE_DIR, "logs")
LOG_FILE = os.path.join(LOG_DIR, "cron.log")

# Required vars check
for k in ("SUPABASE_URL", "SUPABASE_KEY"):
    if not globals().get(k):
        print(f"ERROR: {k} must be set in .env")
        sys.exit(1)

DATA_BASE = f"https://api.data.gov.in/resource/{RESOURCE_ID}"

IST = ZoneInfo("Asia/Kolkata")
TARGET_DATE = datetime.now(IST).date().isoformat()


def to_api_date(iso_date: str) -> str:
    """
    data.gov.in mandi dataset uses DD/MM/YYYY for arrival_date.
    Convert YYYY-MM-DD -> DD/MM/YYYY.
    """
    return datetime.strptime(iso_date, "%Y-%m-%d").strftime("%d/%m/%Y")


def normalize_arrival_date(value: Optional[str]) -> Optional[str]:
    """
    Normalize arrival_date from API into ISO (YYYY-MM-DD) for DB storage.
    Accepts either DD/MM/YYYY or already-ISO strings.
    """
    if not value or not isinstance(value, str):
        return value

    v = value.strip()
    # API often returns DD/MM/YYYY (e.g. 05/01/2026)
    try:
        return datetime.strptime(v, "%d/%m/%Y").date().isoformat()
    except ValueError:
        pass

    # Allow already-ISO
    try:
        return datetime.strptime(v, "%Y-%m-%d").date().isoformat()
    except ValueError:
        return v


# ------------------------------------------------------------
# logging (rotating, redacted)
# ------------------------------------------------------------

os.makedirs(LOG_DIR, exist_ok=True)

URL_RE = re.compile(r"https?://\S+")
TOKEN_RE = re.compile(r"(api[_-]?key|authorization|token)\s*[:=]\s*['\"]?\S+", re.I)


def redact(text: str) -> str:
    if not isinstance(text, str):
        return text
    text = URL_RE.sub("<REDACTED_URL>", text)
    text = TOKEN_RE.sub("<REDACTED_TOKEN>", text)
    return text


logger = logging.getLogger("mandi_fetcher")
logger.setLevel(logging.INFO)

fmt = logging.Formatter(
    "%(asctime)s,%(msecs)03d:%(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)

fh = RotatingFileHandler(LOG_FILE, maxBytes=5_000_000, backupCount=5, encoding="utf-8")
fh.setFormatter(fmt)
logger.addHandler(fh)

ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(fmt)
logger.addHandler(ch)


def log_info(msg):
    logger.info(redact(msg))


def log_warn(msg):
    logger.warning(redact(msg))


def log_error(offset, err, preview=None):
    err_msg = str(err)
    if "for url:" in err_msg:
        err_msg = err_msg.split("for url:", 1)[0].rstrip()
    logger.error(redact(f"⚠️ Error at offset={offset} — {err_msg}"))
    if preview:
        p = redact(str(preview))
        logger.error("   server response preview: %s", p[:800])


def build_session() -> requests.Session:
    """
    Shared HTTP session with sane retries for transient failures.
    """
    s = requests.Session()
    s.headers.update({"User-Agent": "mandi-fetcher/1.0", "Accept": "application/json"})

    retry = Retry(
        total=6,
        connect=6,
        read=6,
        status=6,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


def fetch_page(
    session: requests.Session, *, offset: int, limit: int, api_date: str
) -> List[Dict[str, Any]]:
    r = session.get(
        DATA_BASE,
        params={
            "api-key": DATA_API_KEY,
            "offset": offset,
            "limit": limit,
            "format": "json",
            # Ensure offset is within a stable, date-filtered result set.
            "filters[arrival_date]": api_date,
        },
        timeout=(TIMEOUT_CONNECT, TIMEOUT_READ),
    )
    # With Retry(raise_on_status=False) we need to raise manually.
    r.raise_for_status()
    payload = r.json()
    return payload.get("records", []) or []


def normalize_records(
    records: List[Dict[str, Any]], *, target_date: str
) -> List[Dict[str, Any]]:
    cleaned: List[Dict[str, Any]] = []
    for rec in records:
        out: Dict[str, Any] = {}
        for k, v in rec.items():
            if isinstance(v, str):
                v = v.strip()
            if k == "arrival_date":
                v = normalize_arrival_date(v)
            out[k] = v
        cleaned.append(out)

    # ---- HARD DATE CHECK (post-normalization) ----
    dates = {rec.get("arrival_date") for rec in cleaned}
    if dates != {target_date}:
        raise RuntimeError(f"Unexpected arrival_date(s) received: {dates}")

    return cleaned


def insert_batch(rows: List[Dict[str, Any]]) -> None:
    # insert (no ON CONFLICT / no DB-level dedupe)
    supabase.table(SUPABASE_TABLE).insert(rows).execute()


supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


def get_db_offset_for_date(date_str: str) -> int:
    """
    Use Supabase as the source of truth.
    """
    resp = (
        supabase.table(SUPABASE_TABLE)
        .select("id", count="exact")
        .eq("arrival_date", date_str)
        .execute()
    )
    return resp.count or 0


def main():
    log_info(f"Starting mandi fetch for date={TARGET_DATE}")

    offset = get_db_offset_for_date(TARGET_DATE) if RESUME_FROM_DB else 0
    log_info(f"Starting from offset={offset} (resume_from_db={RESUME_FROM_DB})")

    session = build_session()

    consecutive_errors = 0
    max_backoff = 300
    api_date = to_api_date(TARGET_DATE)

    while True:
        try:
            records = fetch_page(session, offset=offset, limit=LIMIT, api_date=api_date)

            if not records:
                log_info("No more records. Run complete.")
                break

            cleaned = normalize_records(records, target_date=TARGET_DATE)
            insert_batch(cleaned)

            offset += len(cleaned)
            log_info(f"Inserted {len(cleaned)} records | offset now {offset}")

            consecutive_errors = 0
            if SLEEP:
                time.sleep(SLEEP)

        except Exception as e:
            consecutive_errors += 1
            backoff = min(2**consecutive_errors, max_backoff)
            preview = getattr(e, "response", None)
            log_error(offset, e, preview)
            log_info(f"Retrying in {backoff}s")
            time.sleep(backoff)

    log_info("✅ Done.")


if __name__ == "__main__":
    main()
