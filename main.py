import os
import sys
import time
import random
import re
import logging
import json
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Optional, Dict, Any, List

import requests
from dotenv import load_dotenv
from supabase import create_client

# ----------------------------
# Config
# ----------------------------

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_API_KEY")

SUPABASE_TABLE = os.getenv("SUPABASE_TABLE")

DATA_API_KEY = "579b464db66ec23bdd000001cdc3b564546246a772a26393094f5645"
RESOURCE_ID = "9ef84268-d588-465a-a308-a864a43d0070"
DATA_BASE = f"https://api.data.gov.in/resource/{RESOURCE_ID}"

LIMIT = 200
SUCCESS_SLEEP_SECONDS = 0

RESUME_FROM_DB = True

MAX_RUNTIME_SECONDS = 4 * 60 * 60
MAX_CONSECUTIVE_ERRORS = 9999

INITIAL_BACKOFF_SECONDS = 5
MAX_BACKOFF_SECONDS = 120
JITTER_RATIO = 0.2  # +-20%
MAX_EMPTY_PAGE_RETRIES = 5
MAX_TOTAL_ROWS = 300_000
MAX_STATE_DATES = 3

TIMEOUT_CONNECT = 10
TIMEOUT_READ = 120

ROLLOVER_HOUR_IST = 9

LOG_DIR = os.path.join(BASE_DIR, "logs")
LOG_FILE = os.path.join(LOG_DIR, "cron.log")
STATE_FILE = os.path.join(LOG_DIR, "state.json")

for k in ("SUPABASE_URL", "SUPABASE_KEY"):
    if not globals().get(k):
        print(f"ERROR: {k} must be set in .env")
        sys.exit(1)

# ----------------------------
# Date logic (IST)
# ----------------------------

IST = ZoneInfo("Asia/Kolkata")


def target_date_for_run(now: Optional[datetime] = None) -> str:
    now = now or datetime.now(IST)
    today = now.date()
    if now.hour < ROLLOVER_HOUR_IST:
        return (today - timedelta(days=1)).isoformat()
    return today.isoformat()


TARGET_DATE = target_date_for_run()  # YYYY-MM-DD (DB)


def to_api_date(iso_date: str) -> str:
    """Convert YYYY-MM-DD -> DD/MM/YYYY (API format)."""
    return datetime.strptime(iso_date, "%Y-%m-%d").strftime("%d/%m/%Y")


def normalize_arrival_date(value: Optional[str]) -> Optional[str]:
    """Normalize API arrival_date (DD/MM/YYYY) into ISO (YYYY-MM-DD)."""
    if not value or not isinstance(value, str):
        return value
    v = value.strip()
    try:
        return datetime.strptime(v, "%d/%m/%Y").date().isoformat()
    except ValueError:
        pass
    try:
        return datetime.strptime(v, "%Y-%m-%d").date().isoformat()
    except ValueError:
        return v


# ----------------------------
# Logging (rotating, redacted)
# ----------------------------

os.makedirs(LOG_DIR, exist_ok=True)

URL_RE = re.compile(r"https?://\S+")
TOKEN_RE = re.compile(r"(api[_-]?key|authorization|token)\s*[:=]\s*['\"]?\S+", re.I)


def redact(text: str) -> str:
    if not isinstance(text, str):
        return text
    # URLs are also stripped from most requests errors ("for url: ..."), but keep this as a safety net.
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


def log_info(msg: str) -> None:
    logger.info(redact(msg))


def log_warn(msg: str) -> None:
    logger.warning(redact(msg))


def log_error(offset: int, err: Exception, preview: Optional[str] = None) -> None:
    # requests often includes "for url: ..." in the exception string; omit it from logs.
    err_msg = str(err)
    if "for url:" in err_msg:
        err_msg = err_msg.split("for url:", 1)[0].rstrip()
    logger.error(redact(f"⚠️ Error at offset={offset} — {err_msg}"))
    if preview:
        # skip noisy html error bodies
        if isinstance(preview, str) and "<html" in preview.lower():
            return
        logger.error("   preview: %s", redact(preview)[:800])


# ----------------------------
# Supabase
# ----------------------------

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


def get_db_offset_for_date(date_str: str) -> int:
    resp = (
        supabase.table(SUPABASE_TABLE)
        .select("id", count="exact")
        .eq("arrival_date", date_str)
        .execute()
    )
    return resp.count or 0


# ----------------------------
# Core pipeline
# ----------------------------

TRANSIENT_STATUS = {429, 500, 502, 503, 504}


class TransientFetchError(RuntimeError):
    def __init__(self, msg: str, *, preview: str = ""):
        super().__init__(msg)
        self.preview = preview


def build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": "mandi-fetcher/1.0", "Accept": "application/json"})
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
            "filters[arrival_date]": api_date,
        },
        timeout=(TIMEOUT_CONNECT, TIMEOUT_READ),
    )

    if r.status_code in TRANSIENT_STATUS:
        raise TransientFetchError(
            f"HTTP {r.status_code} from data.gov.in", preview=(r.text or "")[:2000]
        )

    r.raise_for_status()
    try:
        payload = r.json()
    except ValueError:
        raise TransientFetchError(
            "Invalid JSON from data.gov.in", preview=(r.text or "")[:2000]
        )

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

    dates = {rec.get("arrival_date") for rec in cleaned}
    if dates != {target_date}:
        raise RuntimeError(f"Unexpected arrival_date(s) received: {dates}")

    return cleaned


def insert_batch(rows: List[Dict[str, Any]], *, offset: int) -> int:
    # remove duplicates within the batch to avoid ON CONFLICT issues in a single statement
    deduped: Dict[tuple, Dict[str, Any]] = {}
    for row in rows:
        key = (
            row.get("state"),
            row.get("district"),
            row.get("market"),
            row.get("commodity"),
            row.get("variety"),
            row.get("grade"),
            row.get("arrival_date"),
        )
        deduped[key] = row

    if not deduped:
        return 0

    try:
        resp = (
            supabase.table(SUPABASE_TABLE)
            .upsert(
                list(deduped.values()),
                on_conflict="state,district,market,commodity,variety,grade,arrival_date",
            )
            .execute()
        )
    except Exception as exc:
        raise RuntimeError(f"Supabase insert failed: {exc}") from exc

    status = getattr(resp, "status_code", None)
    error = getattr(resp, "error", None)
    if (status is not None and status >= 400) or error:
        err_text = str(error) if error else f"HTTP {status}"
        raise RuntimeError(f"Supabase insert error: {err_text} (offset={offset})")
    return len(deduped)


def prune_if_needed() -> None:
    total_resp = supabase.table(SUPABASE_TABLE).select("id", count="exact").execute()
    total = total_resp.count or 0
    if total < MAX_TOTAL_ROWS:
        return

    earliest_resp = (
        supabase.table(SUPABASE_TABLE)
        .select("arrival_date")
        .order("arrival_date", desc=False)
        .limit(1)
        .execute()
    )
    rows = getattr(earliest_resp, "data", None) or []
    if not rows:
        return
    earliest_date = rows[0].get("arrival_date")
    if not earliest_date:
        return

    day_count_resp = (
        supabase.table(SUPABASE_TABLE)
        .select("id", count="exact")
        .eq("arrival_date", earliest_date)
        .execute()
    )
    day_count = day_count_resp.count or 0

    supabase.table(SUPABASE_TABLE).delete().eq("arrival_date", earliest_date).execute()
    log_warn(
        f"Pruned {day_count} rows for earliest date {earliest_date} (total >= {MAX_TOTAL_ROWS})"
    )


def load_state() -> Dict[str, int]:
    try:
        with open(STATE_FILE, "r") as fh:
            data = json.load(fh)
            if isinstance(data, dict):
                return {k: int(v) for k, v in data.items()}
    except Exception:
        pass
    return {}


def save_state(state: Dict[str, int]) -> None:
    # keep only the most recent dates to avoid unbounded growth
    trimmed = dict(
        sorted(state.items(), key=lambda kv: kv[0], reverse=True)[:MAX_STATE_DATES]
    )
    tmp_path = f"{STATE_FILE}.tmp"
    try:
        with open(tmp_path, "w") as fh:
            json.dump(trimmed, fh)
        os.replace(tmp_path, STATE_FILE)
    except Exception:
        pass


def jitter(seconds: float) -> float:
    if seconds <= 0:
        return 0.0
    delta = seconds * JITTER_RATIO
    return max(0.0, seconds + random.uniform(-delta, delta))


def main() -> int:
    start = time.monotonic()
    deadline = start + MAX_RUNTIME_SECONDS
    prune_if_needed()
    state = load_state()

    api_date = to_api_date(TARGET_DATE)
    db_offset = get_db_offset_for_date(TARGET_DATE) if RESUME_FROM_DB else 0
    offset = max(db_offset, state.get(TARGET_DATE, 0))
    empty_page_retries = 0

    log_info(
        "Starting mandi fetch "
        f"date={TARGET_DATE} api_date={api_date} table={SUPABASE_TABLE} "
        f"offset={offset} limit={LIMIT} "
        f"max_runtime_s={MAX_RUNTIME_SECONDS} resume_from_db={RESUME_FROM_DB}"
    )

    session = build_session()

    consecutive_errors = 0
    backoff = INITIAL_BACKOFF_SECONDS

    while True:
        now = time.monotonic()
        if now >= deadline:
            log_warn(
                f"Stopping due to max runtime. elapsed_s={int(now - start)} offset={offset}"
            )
            return 0

        try:
            records = fetch_page(session, offset=offset, limit=LIMIT, api_date=api_date)
            if not records:
                empty_page_retries += 1
                if empty_page_retries >= MAX_EMPTY_PAGE_RETRIES:
                    log_info(
                        f"No more records after {MAX_EMPTY_PAGE_RETRIES} retries. Run complete."
                    )
                    return 0
                sleep_s = jitter(
                    min(
                        INITIAL_BACKOFF_SECONDS * empty_page_retries,
                        MAX_BACKOFF_SECONDS,
                    )
                )
                remaining = max(0.0, deadline - time.monotonic())
                if remaining > 0:
                    time.sleep(min(sleep_s, remaining))
                log_warn(
                    f"Empty page received; retrying ({empty_page_retries}/{MAX_EMPTY_PAGE_RETRIES})."
                )
                continue

            cleaned = normalize_records(records, target_date=TARGET_DATE)
            inserted_count = insert_batch(cleaned, offset=offset)
            empty_page_retries = 0

            offset += len(cleaned)
            log_info(
                f"Inserted {inserted_count} records (post-dedupe) | offset now {offset}"
            )
            state[TARGET_DATE] = offset
            save_state(state)

            # Success resets backoff immediately.
            consecutive_errors = 0
            backoff = INITIAL_BACKOFF_SECONDS

            if SUCCESS_SLEEP_SECONDS:
                time.sleep(SUCCESS_SLEEP_SECONDS)

        except TransientFetchError as e:
            consecutive_errors += 1
            log_error(offset, e, e.preview)

        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            consecutive_errors += 1
            log_error(offset, e)

        except requests.exceptions.HTTPError as e:
            consecutive_errors += 1
            code = getattr(getattr(e, "response", None), "status_code", None)
            if code in TRANSIENT_STATUS:
                body = ""
                try:
                    body = (
                        (e.response.text or "")[:2000] if e.response is not None else ""
                    )
                except Exception:
                    body = ""
                log_error(offset, e, body)
            else:
                log_error(offset, e)
                log_error(offset, RuntimeError("Non-retryable HTTP error; aborting."))
                return 2

        except Exception as e:
            consecutive_errors += 1
            log_error(offset, e)

        if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
            log_error(
                offset,
                RuntimeError(
                    f"Exceeded MAX_CONSECUTIVE_ERRORS={MAX_CONSECUTIVE_ERRORS}; aborting."
                ),
            )
            return 2

        sleep_s = jitter(min(backoff, MAX_BACKOFF_SECONDS))
        remaining = max(0.0, deadline - time.monotonic())
        if remaining <= 0:
            continue
        time.sleep(min(sleep_s, remaining))
        backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)


if __name__ == "__main__":
    raise SystemExit(main())
