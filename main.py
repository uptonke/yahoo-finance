import csv
import html
import json
import logging
import os
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# =========================================================
# 0. 基本設定
# =========================================================

SCHEMA_VERSION = "A.8"

OUTPUT_DIR = Path("data")
MARKET_OUTPUT_FILE = OUTPUT_DIR / "market_data.json"
CENTRAL_BANK_OUTPUT_FILE = OUTPUT_DIR / "central_bank_data.json"

TWELVE_DATA_API_KEY = os.getenv("TWELVE_DATA_API_KEY", "").strip()
FRED_API_KEY = os.getenv("FRED_API_KEY", "").strip()

REQUEST_TIMEOUT = 20
MIN_REQUIRED_OK_COUNT = 4

DEFAULT_MAX_STALENESS_DAYS = 5
FOREIGN_FLOW_MAX_STALENESS_DAYS = 5

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

# =========================================================
# 1. 可調整的 symbol / source config
# =========================================================

MARKET_CONFIG: Dict[str, Dict[str, Any]] = {
    "sp500": {
        "display_name": "S&P 500 Proxy (SPY)",
        "provider": "twelve_data",
        "symbol": os.getenv("TD_SP500_SYMBOL", "SPY"),
        "asset_class": "equity_index_proxy",
        "report_category": "equities",
        "market": "US",
        "currency": "USD",
        "priority": 10,
        "decimals": 2,
        "required": True,
        "note": "Using SPY ETF as S&P 500 proxy.",
    },
    "taiex": {
        "display_name": "TAIEX Proxy (0050)",
        "provider": "twelve_data",
        "symbol": os.getenv("TD_TAIEX_SYMBOL", "0050"),
        "exchange": os.getenv("TD_TAIEX_EXCHANGE", "TWSE"),
        "asset_class": "equity_index_proxy",
        "report_category": "equities",
        "market": "TW",
        "currency": "TWD",
        "priority": 20,
        "decimals": 2,
        "required": False,
        "note": "Using 0050 ETF as Taiwan equity market proxy. Optional because Twelve Data plan coverage may vary.",
    },
    "wti": {
        "display_name": "WTI Crude Oil",
        "provider": "twelve_data",
        "symbol": os.getenv("TD_WTI_SYMBOL", "WTI"),
        "asset_class": "commodity",
        "report_category": "commodities",
        "market": "GLOBAL",
        "currency": "USD",
        "priority": 30,
        "decimals": 2,
        "required": True,
    },
    "usd_twd": {
        "display_name": "USD/TWD",
        "provider": "fred",
        "series_id": os.getenv("FRED_USDTWD_SERIES", "DEXTAUS"),
        "asset_class": "fx",
        "report_category": "fx",
        "market": "FX",
        "currency": "TWD_PER_USD",
        "priority": 40,
        "decimals": 4,
        "required": True,
        "note": "Using FRED DEXTAUS: Taiwan dollars to one U.S. dollar.",
    },
    "dxy_proxy": {
        "display_name": "US Dollar Broad Index Proxy",
        "provider": "fred",
        "series_id": os.getenv("FRED_DXY_PROXY_SERIES", "DTWEXBGS"),
        "asset_class": "fx_index",
        "report_category": "fx",
        "market": "US",
        "currency": "INDEX_POINTS",
        "priority": 50,
        "decimals": 2,
        "required": True,
        "note": "FRED broad dollar index proxy; not identical to ICE DXY.",
    },
    "us10y": {
        "display_name": "US 10Y Treasury Yield",
        "provider": "fred",
        "series_id": os.getenv("FRED_US10Y_SERIES", "DGS10"),
        "asset_class": "government_bond_yield",
        "report_category": "rates",
        "market": "US",
        "currency": "PERCENT",
        "priority": 60,
        "decimals": 3,
        "unit": "%",
        "required": True,
    },
}

CENTRAL_BANK_SOURCE_CONFIG = {
    "fed": {
        "display_name": "Fed",
        "provider": "fed_structured",
        "lower_series_id": os.getenv("FED_LOWER_SERIES_ID", "DFEDTARL").strip(),
        "upper_series_id": os.getenv("FED_UPPER_SERIES_ID", "DFEDTARU").strip(),
        "schedule_url": os.getenv(
            "FED_SCHEDULE_URL",
            "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm",
        ).strip(),
        "market_pricing_url": os.getenv("FED_MARKET_PRICING_URL", "").strip(),
    },
    "ecb": {
        "display_name": "ECB",
        "provider": "ecb_structured",
        "mrr_series_key": os.getenv("ECB_MRR_SERIES_KEY", "D.U2.EUR.4F.KR.MRR_RT.LEV").strip(),
        "dfr_series_key": os.getenv("ECB_DFR_SERIES_KEY", "D.U2.EUR.4F.KR.DFR.LEV").strip(),
        "mlfr_series_key": os.getenv("ECB_MLFR_SERIES_KEY", "D.U2.EUR.4F.KR.MLFR.LEV").strip(),
        "schedule_url": os.getenv(
            "ECB_SCHEDULE_URL",
            "https://www.ecb.europa.eu/press/calendars/mgcgc/html/index.en.html",
        ).strip(),
        "market_pricing_url": os.getenv("ECB_MARKET_PRICING_URL", "").strip(),
    },
    "boj": {
        "display_name": "BOJ",
        "provider": "boj_hybrid_official",
        "current_rate_url": os.getenv("BOJ_RATE_URL", "").strip(),
        "schedule_url": os.getenv(
            "BOJ_SCHEDULE_URL",
            "https://www.boj.or.jp/en/mopo/mpmsche_minu/index.htm",
        ).strip(),
        "market_pricing_url": os.getenv("BOJ_MARKET_PRICING_URL", "").strip(),
    },
    "pboc": {
        "display_name": "PBOC",
        "provider": "pboc_hybrid_official",
        "current_rate_url": os.getenv("PBOC_RATE_URL", "").strip(),
        "schedule_url": os.getenv(
            "PBOC_SCHEDULE_URL",
            "https://www.pbc.gov.cn/en/3688229/3688311/3688329/index.html",
        ).strip(),
        "market_pricing_url": os.getenv("PBOC_MARKET_PRICING_URL", "").strip(),
    },
}

TWSE_FOREIGN_FLOW_API_URL = os.getenv(
    "TWSE_FOREIGN_FLOW_API_URL",
    "https://www.twse.com.tw/rwd/zh/fund/T86",
).strip()
TWSE_FOREIGN_FLOW_SELECT_TYPE = os.getenv(
    "TWSE_FOREIGN_FLOW_SELECT_TYPE",
    "ALLBUT0999",
).strip()

TPEX_FOREIGN_FLOW_API_URL = os.getenv(
    "TPEX_FOREIGN_FLOW_API_URL",
    "https://www.tpex.org.tw/www/zh-tw/insti/dailyTrade",
).strip()
TPEX_FOREIGN_FLOW_SECT = os.getenv("TPEX_FOREIGN_FLOW_SECT", "AL").strip()
TPEX_FOREIGN_FLOW_TYPE = os.getenv("TPEX_FOREIGN_FLOW_TYPE", "Daily").strip()

# =========================================================
# 2. Session / HTTP helpers
# =========================================================


def build_session() -> requests.Session:
    session = requests.Session()

    retry = Retry(
        total=3,
        read=3,
        connect=3,
        backoff_factor=1.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "POST"]),
        raise_on_status=False,
    )

    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    session.headers.update(
        {
            "User-Agent": "macro-brief-bot/1.0",
            "Accept": "application/json, text/plain, */*",
        }
    )
    return session


SESSION = build_session()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def taipei_now() -> datetime:
    return datetime.now(timezone.utc) + timedelta(hours=8)


def safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        if isinstance(value, str) and not value.strip():
            return None
        return float(value)
    except Exception:
        return None


def safe_round(value: Optional[float], digits: int) -> Optional[float]:
    if value is None:
        return None
    try:
        return round(float(value), digits)
    except Exception:
        return None


def format_number(value: Optional[float], decimals: int = 2, use_sign: bool = False) -> str:
    if value is None:
        return "N/A"
    fmt = f"{{:{'+' if use_sign else ''},.{decimals}f}}"
    return fmt.format(value)


def fetch_json(url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    resp = SESSION.get(url, params=params, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def post_json(url: str, data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    resp = SESSION.post(url, data=data, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def fetch_text(url: str) -> str:
    resp = SESSION.get(url, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()

    content = resp.content
    encodings_to_try = [
        "utf-8",
        resp.apparent_encoding,
        resp.encoding,
        "big5",
        "cp950",
    ]

    for enc in encodings_to_try:
        if not enc:
            continue
        try:
            return content.decode(enc)
        except Exception:
            continue

    return content.decode("utf-8", errors="replace")


def atomic_write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, path)


def normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def html_to_visible_text(raw_html: str) -> str:
    text = re.sub(r"(?is)<script.*?>.*?</script>", " ", raw_html)
    text = re.sub(r"(?is)<style.*?>.*?</style>", " ", text)
    text = re.sub(r"(?i)<br\s*/?>", "\n", text)
    text = re.sub(r"(?i)</(p|div|li|tr|td|th|h1|h2|h3|h4|h5|h6|section|article)>", "\n", text)
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    text = html.unescape(text)
    return text


def html_to_lines(raw_html: str) -> list[str]:
    text = html_to_visible_text(raw_html)
    lines = [normalize_whitespace(line) for line in text.splitlines()]
    return [line for line in lines if line]


# =========================================================
# 3. 通用 market helpers
# =========================================================


def infer_direction(change: Optional[float]) -> str:
    if change is None:
        return "unknown"
    if change > 0:
        return "up"
    if change < 0:
        return "down"
    return "flat"


def market_session_label_from_date(date_str: Optional[str]) -> str:
    if not date_str:
        return "unknown"
    return "latest"


def build_as_of_label(date_str: Optional[str], market_session_label: str) -> str:
    if not date_str:
        return "N/A"
    if market_session_label == "latest":
        return f"latest close ({date_str})"
    if market_session_label == "recent_trading_day":
        return f"recent trading day close ({date_str})"
    return f"available close ({date_str})"


def format_close(value: Optional[float], cfg: Dict[str, Any]) -> str:
    if value is None:
        return "N/A"

    decimals = cfg.get("decimals", 2)
    currency = cfg.get("currency")
    unit = cfg.get("unit")

    if unit == "%" or currency == "PERCENT":
        return f"{format_number(value, decimals)}%"
    return format_number(value, decimals)


def format_change(change: Optional[float], change_pct: Optional[float], cfg: Dict[str, Any]) -> str:
    if change is None:
        return "N/A"

    decimals = cfg.get("decimals", 2)
    currency = cfg.get("currency")
    unit = cfg.get("unit")

    if unit == "%" or currency == "PERCENT":
        base = f"{format_number(change, decimals, use_sign=True)}pp"
    else:
        base = format_number(change, decimals, use_sign=True)

    if change_pct is None:
        return base
    return f"{base} ({format_number(change_pct, 2, use_sign=True)}%)"


def build_error_result(cfg: Dict[str, Any], message: str) -> Dict[str, Any]:
    return {
        "status": "error",
        "display_name": cfg["display_name"],
        "provider": cfg["provider"],
        "asset_class": cfg["asset_class"],
        "report_category": cfg["report_category"],
        "market": cfg["market"],
        "currency": cfg["currency"],
        "priority": cfg["priority"],
        "required": cfg.get("required", True),
        "error": message,
    }


def build_ok_result(
    cfg: Dict[str, Any],
    date_str: Optional[str],
    close: Optional[float],
    prev_close: Optional[float],
    raw_payload: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    decimals = cfg.get("decimals", 2)

    change = None
    change_pct = None
    if close is not None and prev_close is not None:
        change = safe_round(close - prev_close, decimals)
        if prev_close != 0:
            change_pct = round((change / prev_close) * 100, 2)

    session_label = market_session_label_from_date(date_str)

    result = {
        "status": "ok",
        "display_name": cfg["display_name"],
        "provider": cfg["provider"],
        "asset_class": cfg["asset_class"],
        "report_category": cfg["report_category"],
        "market": cfg["market"],
        "currency": cfg["currency"],
        "priority": cfg["priority"],
        "required": cfg.get("required", True),
        "date": date_str,
        "close": safe_round(close, decimals),
        "prev_close": safe_round(prev_close, decimals),
        "change": change,
        "change_pct": change_pct,
        "direction": infer_direction(change),
        "market_session_label": session_label,
        "display_close": format_close(close, cfg),
        "display_change": format_change(change, change_pct, cfg),
        "as_of_label": build_as_of_label(date_str, session_label),
        "raw_payload": raw_payload,
    }

    if "note" in cfg:
        result["note"] = cfg["note"]
    if "unit" in cfg:
        result["unit"] = cfg["unit"]

    return result


# =========================================================
# 4. Twelve Data
# =========================================================


def fetch_twelve_data_quote(cfg: Dict[str, Any]) -> Dict[str, Any]:
    if not TWELVE_DATA_API_KEY:
        raise RuntimeError("Missing TWELVE_DATA_API_KEY")

    url = "https://api.twelvedata.com/quote"
    params = {
        "symbol": cfg["symbol"],
        "apikey": TWELVE_DATA_API_KEY,
    }

    if cfg.get("exchange"):
        params["exchange"] = cfg["exchange"]

    return fetch_json(url, params=params)


def parse_twelve_data_quote(cfg: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
    possible_close_keys = ["close", "price", "previous_close"]
    possible_prev_close_keys = ["previous_close", "prev_close"]
    possible_date_keys = ["datetime", "timestamp", "date"]

    close = None
    for key in possible_close_keys:
        close = safe_float(payload.get(key))
        if close is not None:
            break

    prev_close = None
    for key in possible_prev_close_keys:
        prev_close = safe_float(payload.get(key))
        if prev_close is not None:
            break

    date_str = None
    for key in possible_date_keys:
        raw = payload.get(key)
        if raw:
            date_str = str(raw)[:10]
            break

    if close is None:
        raise RuntimeError(f"Twelve Data quote missing close/price field: {payload}")

    return build_ok_result(
        cfg=cfg,
        date_str=date_str,
        close=close,
        prev_close=prev_close,
        raw_payload=payload,
    )


def fetch_market_from_twelve_data(key: str, cfg: Dict[str, Any]) -> Dict[str, Any]:
    try:
        payload = fetch_twelve_data_quote(cfg)
        if "status" in payload and str(payload.get("status")).lower() == "error":
            return build_error_result(cfg, payload.get("message", "Twelve Data error"))
        return parse_twelve_data_quote(cfg, payload)
    except Exception as e:
        return build_error_result(cfg, f"Twelve Data fetch failed: {e}")


# =========================================================
# 5. FRED
# =========================================================


def fetch_fred_series_observations(series_id: str) -> Dict[str, Any]:
    if not FRED_API_KEY:
        raise RuntimeError("Missing FRED_API_KEY")

    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "sort_order": "asc",
    }
    return fetch_json(url, params=params)


def parse_fred_series(cfg: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
    observations = payload.get("observations", [])
    valid = []

    for obs in observations:
        value = safe_float(obs.get("value"))
        if value is None:
            continue
        valid.append(
            {
                "date": obs.get("date"),
                "value": value,
            }
        )

    if not valid:
        raise RuntimeError("FRED observations empty or invalid")

    latest = valid[-1]
    prev = valid[-2] if len(valid) >= 2 else None

    return build_ok_result(
        cfg=cfg,
        date_str=latest.get("date"),
        close=latest.get("value"),
        prev_close=prev.get("value") if prev else None,
        raw_payload=payload,
    )


def fetch_market_from_fred(key: str, cfg: Dict[str, Any]) -> Dict[str, Any]:
    try:
        payload = fetch_fred_series_observations(cfg["series_id"])
        return parse_fred_series(cfg, payload)
    except Exception as e:
        return build_error_result(cfg, f"FRED fetch failed: {e}")


# =========================================================
# 6. 台灣外資動向
# =========================================================


def clean_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    s = str(value).strip().replace(",", "")
    if s in {"", "-", "--", "N/A", "None"}:
        return None
    try:
        return int(s)
    except Exception:
        return None


def format_signed_int(value: Optional[int]) -> str:
    if value is None:
        return "N/A"
    return f"{value:,}"


def int_or_none(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return None


def normalize_to_iso_date(date_str: Optional[str]) -> Optional[str]:
    if not date_str:
        return None

    s = str(date_str).strip()

    m = re.match(r"^(\d{2,3})/(\d{1,2})/(\d{1,2})$", s)
    if m:
        year = int(m.group(1)) + 1911
        month = int(m.group(2))
        day = int(m.group(3))
        return f"{year:04d}-{month:02d}-{day:02d}"

    m = re.match(r"^(\d{4})/(\d{1,2})/(\d{1,2})$", s)
    if m:
        year = int(m.group(1))
        month = int(m.group(2))
        day = int(m.group(3))
        return f"{year:04d}-{month:02d}-{day:02d}"

    m = re.match(r"^(\d{4})(\d{2})(\d{2})$", s)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

    m = re.match(r"^(\d{4})-(\d{1,2})-(\d{1,2})$", s)
    if m:
        year = int(m.group(1))
        month = int(m.group(2))
        day = int(m.group(3))
        return f"{year:04d}-{month:02d}-{day:02d}"

    return s


def parse_iso_date(date_str: Optional[str]) -> Optional[date]:
    normalized = normalize_to_iso_date(date_str)
    if not normalized:
        return None
    try:
        return datetime.strptime(normalized, "%Y-%m-%d").date()
    except Exception:
        return None


def build_foreign_flow_structural_read(single_day_raw: Optional[int]) -> str:
    if single_day_raw is None:
        return "資料不足，無法判定外資當日方向"
    if single_day_raw > 0:
        return "外資當日偏多，上市櫃合計呈現淨買超"
    if single_day_raw < 0:
        return "外資當日偏空，上市櫃合計呈現淨賣超"
    return "外資當日中性，上市櫃合計接近平衡"


def taipei_today_yyyymmdd() -> str:
    return taipei_now().strftime("%Y%m%d")


def roc_or_gregorian_to_yyyymmdd(date_str: Optional[str]) -> str:
    if not date_str:
        return taipei_today_yyyymmdd()

    s = str(date_str).strip()

    m = re.match(r"^(\d{2,3})/(\d{1,2})/(\d{1,2})$", s)
    if m:
        roc_year = int(m.group(1))
        year = roc_year + 1911
        month = int(m.group(2))
        day = int(m.group(3))
        return f"{year:04d}{month:02d}{day:02d}"

    m = re.match(r"^(\d{4})/(\d{1,2})/(\d{1,2})$", s)
    if m:
        year = int(m.group(1))
        month = int(m.group(2))
        day = int(m.group(3))
        return f"{year:04d}{month:02d}{day:02d}"

    m = re.match(r"^(\d{4})-(\d{1,2})-(\d{1,2})$", s)
    if m:
        year = int(m.group(1))
        month = int(m.group(2))
        day = int(m.group(3))
        return f"{year:04d}{month:02d}{day:02d}"

    return taipei_today_yyyymmdd()


def fetch_tpex_foreign_flow_api() -> Dict[str, Any]:
    if not TPEX_FOREIGN_FLOW_API_URL:
        return {
            "status": "unconfigured",
            "single_day": "N/A",
            "single_day_raw": None,
            "date": None,
            "note": "TPEX_FOREIGN_FLOW_API_URL not configured",
        }

    payload = {
        "type": TPEX_FOREIGN_FLOW_TYPE or "Daily",
        "sect": TPEX_FOREIGN_FLOW_SECT or "AL",
    }

    try:
        raw = post_json(TPEX_FOREIGN_FLOW_API_URL, data=payload)

        tables = raw.get("tables") or []
        if not tables:
            return {
                "status": "error",
                "single_day": "N/A",
                "single_day_raw": None,
                "date": None,
                "error": "TPEX API returned no tables",
                "url": TPEX_FOREIGN_FLOW_API_URL,
                "request_payload": payload,
                "raw_payload": raw,
            }

        table = tables[0]
        rows = table.get("data") or []
        report_date_raw = table.get("date")
        report_date = normalize_to_iso_date(report_date_raw)
        title = table.get("title")

        foreign_ex_dealer_net = 0
        foreign_total_net = 0
        three_insti_total_net = 0
        valid_rows = 0

        for row in rows:
            if not isinstance(row, list) or len(row) < 24:
                continue

            v_ex_dealer = clean_int(row[4])
            v_foreign_total = clean_int(row[10])
            v_three_insti = clean_int(row[23])

            if v_ex_dealer is not None:
                foreign_ex_dealer_net += v_ex_dealer
            if v_foreign_total is not None:
                foreign_total_net += v_foreign_total
            if v_three_insti is not None:
                three_insti_total_net += v_three_insti

            valid_rows += 1

        if valid_rows == 0:
            return {
                "status": "error",
                "single_day": "N/A",
                "single_day_raw": None,
                "date": report_date,
                "raw_date": report_date_raw,
                "error": "TPEX API returned zero valid rows",
                "url": TPEX_FOREIGN_FLOW_API_URL,
                "request_payload": payload,
                "raw_payload": raw,
            }

        return {
            "status": "ok",
            "single_day": format_signed_int(foreign_total_net),
            "single_day_raw": foreign_total_net,
            "date": report_date,
            "raw_date": report_date_raw,
            "source": "official_api_json",
            "title": title,
            "row_count": valid_rows,
            "foreign_ex_dealer_single_day": format_signed_int(foreign_ex_dealer_net),
            "foreign_ex_dealer_single_day_raw": foreign_ex_dealer_net,
            "foreign_total_single_day": format_signed_int(foreign_total_net),
            "foreign_total_single_day_raw": foreign_total_net,
            "three_insti_total_single_day": format_signed_int(three_insti_total_net),
            "three_insti_total_single_day_raw": three_insti_total_net,
            "url": TPEX_FOREIGN_FLOW_API_URL,
            "request_payload": payload,
            "note": "Parsed from official TPEX POST API /www/zh-tw/insti/dailyTrade",
        }
    except Exception as e:
        return {
            "status": "error",
            "single_day": "N/A",
            "single_day_raw": None,
            "date": None,
            "error": str(e),
            "url": TPEX_FOREIGN_FLOW_API_URL,
            "request_payload": payload,
        }


def fetch_twse_foreign_flow_api(reference_date: Optional[str] = None) -> Dict[str, Any]:
    if not TWSE_FOREIGN_FLOW_API_URL:
        return {
            "status": "unconfigured",
            "single_day": "N/A",
            "single_day_raw": None,
            "date": None,
            "note": "TWSE_FOREIGN_FLOW_API_URL not configured",
        }

    target_date = roc_or_gregorian_to_yyyymmdd(reference_date)
    params = {
        "response": "json",
        "date": target_date,
        "selectType": TWSE_FOREIGN_FLOW_SELECT_TYPE or "ALLBUT0999",
    }

    try:
        raw = fetch_json(TWSE_FOREIGN_FLOW_API_URL, params=params)

        rows = raw.get("data") or []
        fields = raw.get("fields") or []

        if not rows or not fields:
            return {
                "status": "error",
                "single_day": "N/A",
                "single_day_raw": None,
                "date": normalize_to_iso_date(target_date),
                "raw_date": target_date,
                "error": "TWSE API returned no data/fields",
                "url": TWSE_FOREIGN_FLOW_API_URL,
                "request_params": params,
                "raw_payload": raw,
            }

        field_idx = {name: idx for idx, name in enumerate(fields)}

        idx_foreign_ex = field_idx.get("外陸資買賣超股數(不含外資自營商)")
        idx_foreign_dealer = field_idx.get("外資自營商買賣超股數")
        idx_three_insti = field_idx.get("三大法人買賣超股數")

        if idx_foreign_ex is None:
            return {
                "status": "error",
                "single_day": "N/A",
                "single_day_raw": None,
                "date": normalize_to_iso_date(target_date),
                "raw_date": target_date,
                "error": "TWSE API missing field: 外陸資買賣超股數(不含外資自營商)",
                "url": TWSE_FOREIGN_FLOW_API_URL,
                "request_params": params,
                "fields": fields,
            }

        foreign_ex_dealer_net = 0
        foreign_dealer_net = 0
        three_insti_total_net = 0
        valid_rows = 0

        for row in rows:
            if not isinstance(row, list):
                continue
            if idx_foreign_ex >= len(row):
                continue

            v_ex = clean_int(row[idx_foreign_ex])
            v_dealer = (
                clean_int(row[idx_foreign_dealer])
                if idx_foreign_dealer is not None and idx_foreign_dealer < len(row)
                else 0
            )
            v_three = (
                clean_int(row[idx_three_insti])
                if idx_three_insti is not None and idx_three_insti < len(row)
                else None
            )

            if v_ex is not None:
                foreign_ex_dealer_net += v_ex
                valid_rows += 1

            if v_dealer is not None:
                foreign_dealer_net += v_dealer

            if v_three is not None:
                three_insti_total_net += v_three

        foreign_total_net = foreign_ex_dealer_net + foreign_dealer_net
        report_date = normalize_to_iso_date(target_date)

        if valid_rows == 0:
            return {
                "status": "error",
                "single_day": "N/A",
                "single_day_raw": None,
                "date": report_date,
                "raw_date": target_date,
                "error": "TWSE API returned zero valid rows",
                "url": TWSE_FOREIGN_FLOW_API_URL,
                "request_params": params,
                "raw_payload": raw,
            }

        return {
            "status": "ok",
            "single_day": format_signed_int(foreign_total_net),
            "single_day_raw": foreign_total_net,
            "date": report_date,
            "raw_date": target_date,
            "source": "official_api_json",
            "title": raw.get("title"),
            "stat": raw.get("stat"),
            "row_count": valid_rows,
            "foreign_ex_dealer_single_day": format_signed_int(foreign_ex_dealer_net),
            "foreign_ex_dealer_single_day_raw": foreign_ex_dealer_net,
            "foreign_dealer_single_day": format_signed_int(foreign_dealer_net),
            "foreign_dealer_single_day_raw": foreign_dealer_net,
            "foreign_total_single_day": format_signed_int(foreign_total_net),
            "foreign_total_single_day_raw": foreign_total_net,
            "three_insti_total_single_day": format_signed_int(three_insti_total_net),
            "three_insti_total_single_day_raw": three_insti_total_net,
            "url": TWSE_FOREIGN_FLOW_API_URL,
            "request_params": params,
            "note": "Parsed from official TWSE T86 JSON route.",
        }
    except Exception as e:
        return {
            "status": "error",
            "single_day": "N/A",
            "single_day_raw": None,
            "date": normalize_to_iso_date(target_date),
            "raw_date": target_date,
            "error": str(e),
            "url": TWSE_FOREIGN_FLOW_API_URL,
            "request_params": params,
        }


def fetch_tw_foreign_flow() -> Dict[str, Any]:
    tpex_payload = fetch_tpex_foreign_flow_api()
    twse_payload = fetch_twse_foreign_flow_api(reference_date=tpex_payload.get("date"))
    return {
        "twse": twse_payload,
        "tpex": tpex_payload,
    }


def build_taiwan_view(market_data: Dict[str, Dict[str, Any]], foreign_flow_payload: Dict[str, Any]) -> Dict[str, Any]:
    usd_twd_item = market_data.get("usd_twd", {})
    usd_twd_spot = "N/A"
    if usd_twd_item.get("status") == "ok":
        usd_twd_spot = usd_twd_item.get("display_close", "N/A") or "N/A"

    twse_raw = int_or_none(
        foreign_flow_payload.get("twse", {}).get("foreign_total_single_day_raw")
    )
    if twse_raw is None:
        twse_raw = clean_int(
            foreign_flow_payload.get("twse", {}).get("foreign_total_single_day")
            or foreign_flow_payload.get("twse", {}).get("single_day")
        )

    tpex_raw = int_or_none(
        foreign_flow_payload.get("tpex", {}).get("foreign_total_single_day_raw")
    )
    if tpex_raw is None:
        tpex_raw = clean_int(
            foreign_flow_payload.get("tpex", {}).get("foreign_total_single_day")
            or foreign_flow_payload.get("tpex", {}).get("single_day")
        )

    combined_val = None
    source_used = None
    if twse_raw is not None and tpex_raw is not None:
        combined_val = twse_raw + tpex_raw
        source_used = "twse+tpex"
    elif twse_raw is not None:
        combined_val = twse_raw
        source_used = "twse"
    elif tpex_raw is not None:
        combined_val = tpex_raw
        source_used = "tpex"

    twse_date = foreign_flow_payload.get("twse", {}).get("date")
    tpex_date = foreign_flow_payload.get("tpex", {}).get("date")
    flow_date = twse_date or tpex_date

    return {
        "foreign_flow": {
            "single_day": format_signed_int(combined_val),
            "single_day_raw": combined_val,
            "date": flow_date,
            "source_used": source_used,
            "market_scope": "listed+otc" if source_used == "twse+tpex" else source_used,
            "structural_read": build_foreign_flow_structural_read(combined_val),
            "source_payload": foreign_flow_payload,
        },
        "semiconductor_supply_chain": {
            "core_logic": [
                "AI需求強（持續）",
                "能源與原物料成本上升（壓縮部分毛利）",
            ],
            "judgment": "需求 > 成本壓力（短期仍偏多），但波動放大",
        },
        "fx": {
            "usd_twd_spot": usd_twd_spot,
            "assessment": [
                "若油價上行 → USD 轉強",
                "若風險偏好回升 → TWD 偏強",
            ],
            "base_case": "區間震盪機率 60%",
        },
    }


# =========================================================
# 7. 央行動態（升級版：Fed/ECB 結構化；BOJ/PBOC 官方混合）
# =========================================================


MONTH_MAP = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "SEPT": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}


def fetch_fred_latest_observation(series_id: str) -> Dict[str, Any]:
    payload = fetch_fred_series_observations(series_id)
    observations = payload.get("observations", [])
    valid = []

    for obs in observations:
        value = safe_float(obs.get("value"))
        if value is None:
            continue
        valid.append(
            {
                "date": obs.get("date"),
                "value": value,
            }
        )

    if not valid:
        raise RuntimeError(f"FRED series {series_id} has no valid observations")

    latest = valid[-1]
    return {
        "date": latest["date"],
        "value": latest["value"],
        "series_id": series_id,
    }


def fetch_ecb_latest_observation(series_key: str) -> Dict[str, Any]:
    url = f"https://data-api.ecb.europa.eu/service/data/FM/{series_key}"
    headers = {"Accept": "text/csv"}
    resp = SESSION.get(
        url,
        params={"lastNObservations": 1},
        headers=headers,
        timeout=REQUEST_TIMEOUT,
    )
    resp.raise_for_status()

    lines = [line for line in resp.text.splitlines() if line.strip()]
    if len(lines) < 2:
        raise RuntimeError(f"ECB series {series_key} returned empty CSV")

    reader = csv.DictReader(lines)
    rows = list(reader)
    if not rows:
        raise RuntimeError(f"ECB series {series_key} has no rows")

    row = rows[-1]
    value = safe_float(row.get("OBS_VALUE"))
    date_str = row.get("TIME_PERIOD")

    if value is None or not date_str:
        raise RuntimeError(f"ECB series {series_key} missing OBS_VALUE/TIME_PERIOD")

    return {
        "date": normalize_to_iso_date(date_str),
        "value": value,
        "series_key": series_key,
    }


def extract_percent_from_text(text: str) -> Optional[str]:
    flat = normalize_whitespace(html_to_visible_text(text))

    patterns = [
        r"around\s+(\d+(?:\.\d+)?)\s*percent",
        r"(\d+(?:\.\d+)?)\s*percent",
        r"(\d+(?:\.\d+)?)\s*%",
    ]
    for pattern in patterns:
        m = re.search(pattern, flat, re.I)
        if m:
            return f"{m.group(1)}%"
    return None


def extract_next_fed_meeting(text: str) -> Optional[str]:
    lines = html_to_lines(text)
    today = taipei_now().date()
    year = today.year

    start_idx = None
    end_idx = None

    for i, line in enumerate(lines):
        if line == f"{year} FOMC Meetings":
            start_idx = i
            continue
        if start_idx is not None and line in {
            f"{year - 1} FOMC Meetings",
            f"{year + 1} FOMC Meetings",
        }:
            end_idx = i
            break

    if start_idx is not None:
        block = lines[start_idx:end_idx] if end_idx is not None else lines[start_idx:]

        i = 0
        while i < len(block) - 1:
            month_line = block[i].strip()
            date_line = block[i + 1].strip()

            month_num = MONTH_MAP.get(month_line.upper()[:3])
            if month_num:
                m = re.fullmatch(r"(\d{1,2})-(\d{1,2})\*?", date_line)
                if m:
                    end_day = int(m.group(2))
                    try:
                        meeting_date = datetime(year, month_num, end_day).date()
                        if meeting_date >= today:
                            return meeting_date.isoformat()
                    except Exception:
                        pass
            i += 1

    flat = normalize_whitespace(html_to_visible_text(text))
    m = re.search(
        rf"{year}\s+FOMC Meetings(.*?)(?:{year - 1}\s+FOMC Meetings|{year + 1}\s+FOMC Meetings|$)",
        flat,
        re.I,
    )
    if not m:
        return None

    block = m.group(1)
    matches = re.findall(
        r"([A-Za-z]+(?:/[A-Za-z]+)?)\s+(\d{1,2})-(\d{1,2})\*?",
        block,
        re.I,
    )

    for month_name, _day1, day2 in matches:
        month_num = MONTH_MAP.get(month_name.split("/")[0].upper()[:3])
        if not month_num:
            continue
        try:
            meeting_date = datetime(year, month_num, int(day2)).date()
            if meeting_date >= today:
                return meeting_date.isoformat()
        except Exception:
            continue

    return None


def extract_next_ecb_meeting(text: str) -> Optional[str]:
    lines = html_to_lines(text)
    today = taipei_now().date()

    for i in range(len(lines) - 1):
        date_line = lines[i]
        desc_line = lines[i + 1]

        m = re.fullmatch(r"(\d{2})/(\d{2})/(\d{4})", date_line)
        if not m:
            continue

        if "monetary policy meeting" in desc_line.lower() and "day 2" in desc_line.lower():
            day, month, year = map(int, m.groups())
            try:
                meeting_date = datetime(year, month, day).date()
                if meeting_date >= today:
                    return meeting_date.isoformat()
            except Exception:
                continue

    flat = normalize_whitespace(html_to_visible_text(text))
    matches = re.findall(
        r"(\d{2})/(\d{2})/(\d{4}).{0,200}?monetary policy meeting.{0,80}?day 2",
        flat,
        re.I,
    )

    for day, month, year in matches:
        try:
            meeting_date = datetime(int(year), int(month), int(day)).date()
            if meeting_date >= today:
                return meeting_date.isoformat()
        except Exception:
            continue

    return None


def extract_next_boj_meeting(text: str) -> Optional[str]:
    flat = normalize_whitespace(html_to_visible_text(text))
    today = taipei_now().date()
    year = today.year

    block_match = re.search(
        rf"{year}\s*Date of MPM Release Schedule(.*?)(?:{year + 1}\s*Date of MPM Release Schedule|$)",
        flat,
        re.I,
    )
    block = block_match.group(1) if block_match else flat

    matches = re.findall(
        r"([A-Za-z]{3,9})\.?\s+(\d{1,2})\s*\([A-Za-z]{3,}\.?\)\s*,\s*([A-Za-z]{3,9})\.?\s+(\d{1,2})\s*\([A-Za-z]{3,}\.?\)",
        block,
        re.I,
    )

    for _month1_name, _day1, month2_name, day2 in matches:
        month_num = MONTH_MAP.get(month2_name.upper())
        if not month_num:
            month_num = MONTH_MAP.get(month2_name.upper()[:3])
        if not month_num:
            continue

        try:
            meeting_date = datetime(year, month_num, int(day2)).date()
            if meeting_date >= today:
                return meeting_date.isoformat()
        except Exception:
            continue

    matches_same_month = re.findall(
        r"([A-Za-z]{3,9})\.?\s+(\d{1,2})\s*\([A-Za-z]{3,}\.?\)\s*,\s*(\d{1,2})\s*\([A-Za-z]{3,}\.?\)",
        block,
        re.I,
    )

    for month_name, _day1, day2 in matches_same_month:
        month_num = MONTH_MAP.get(month_name.upper())
        if not month_num:
            month_num = MONTH_MAP.get(month_name.upper()[:3])
        if not month_num:
            continue

        try:
            meeting_date = datetime(year, month_num, int(day2)).date()
            if meeting_date >= today:
                return meeting_date.isoformat()
        except Exception:
            continue

    return None


def extract_latest_pboc_meeting(text: str) -> Optional[str]:
    flat = normalize_whitespace(html_to_visible_text(text))
    m = re.search(
        r"(\d{4}-\d{2}-\d{2})\s+PBOC Monetary Policy Committee Holds",
        flat,
        re.I,
    )
    if not m:
        return None
    return normalize_to_iso_date(m.group(1))


def fetch_fed_structured_block(cfg: Dict[str, Any]) -> Dict[str, Any]:
    lower = fetch_fred_latest_observation(cfg["lower_series_id"])
    upper = fetch_fred_latest_observation(cfg["upper_series_id"])

    next_meeting = "N/A"
    notes = []
    sources = {
        "lower_series_id": cfg["lower_series_id"],
        "upper_series_id": cfg["upper_series_id"],
    }

    try:
        if cfg.get("schedule_url"):
            text = fetch_text(cfg["schedule_url"])
            parsed = extract_next_fed_meeting(text)
            next_meeting = parsed or "N/A"
            sources["schedule_url"] = cfg["schedule_url"]
    except Exception as e:
        notes.append(f"schedule fetch failed: {e}")

    market_pricing_path = "N/A"
    if cfg.get("market_pricing_url"):
        sources["market_pricing_url"] = cfg["market_pricing_url"]

    return {
        "status": "ok",
        "display_name": cfg["display_name"],
        "current_rate": f"{lower['value']:.2f}% - {upper['value']:.2f}%",
        "current_rate_date": lower["date"],
        "next_meeting": next_meeting,
        "market_pricing_path": market_pricing_path,
        "sources": sources,
        "notes": notes,
        "detail": {
            "lower_bound": f"{lower['value']:.2f}%",
            "upper_bound": f"{upper['value']:.2f}%",
        },
    }


def fetch_ecb_structured_block(cfg: Dict[str, Any]) -> Dict[str, Any]:
    mrr = fetch_ecb_latest_observation(cfg["mrr_series_key"])
    dfr = fetch_ecb_latest_observation(cfg["dfr_series_key"])
    mlfr = fetch_ecb_latest_observation(cfg["mlfr_series_key"])

    next_meeting = "N/A"
    notes = []
    sources = {
        "mrr_series_key": cfg["mrr_series_key"],
        "dfr_series_key": cfg["dfr_series_key"],
        "mlfr_series_key": cfg["mlfr_series_key"],
    }

    try:
        if cfg.get("schedule_url"):
            text = fetch_text(cfg["schedule_url"])
            parsed = extract_next_ecb_meeting(text)
            next_meeting = parsed or "N/A"
            sources["schedule_url"] = cfg["schedule_url"]
    except Exception as e:
        notes.append(f"schedule fetch failed: {e}")

    market_pricing_path = "N/A"
    if cfg.get("market_pricing_url"):
        sources["market_pricing_url"] = cfg["market_pricing_url"]

    return {
        "status": "ok",
        "display_name": cfg["display_name"],
        "current_rate": f"MRR {mrr['value']:.2f}% / DFR {dfr['value']:.2f}% / MLFR {mlfr['value']:.2f}%",
        "current_rate_date": mrr["date"],
        "next_meeting": next_meeting,
        "market_pricing_path": market_pricing_path,
        "sources": sources,
        "notes": notes,
        "detail": {
            "mrr": f"{mrr['value']:.2f}%",
            "dfr": f"{dfr['value']:.2f}%",
            "mlfr": f"{mlfr['value']:.2f}%",
        },
    }


def fetch_boj_hybrid_block(cfg: Dict[str, Any]) -> Dict[str, Any]:
    result = {
        "status": "ok",
        "display_name": cfg["display_name"],
        "current_rate": "N/A",
        "current_rate_date": "N/A",
        "next_meeting": "N/A",
        "market_pricing_path": "N/A",
        "sources": {},
        "notes": [],
    }

    try:
        if cfg.get("current_rate_url"):
            text = fetch_text(cfg["current_rate_url"])
            parsed_rate = extract_percent_from_text(text)
            if parsed_rate:
                result["current_rate"] = parsed_rate
            result["sources"]["current_rate_url"] = cfg["current_rate_url"]
        else:
            result["notes"].append(
                "current_rate_url not configured; BOJ policy target remains statement-driven"
            )
    except Exception as e:
        result["status"] = "partial_error"
        result["notes"].append(f"current_rate fetch failed: {e}")

    try:
        if cfg.get("schedule_url"):
            text = fetch_text(cfg["schedule_url"])
            parsed_meeting = extract_next_boj_meeting(text)
            if parsed_meeting:
                result["next_meeting"] = parsed_meeting
            else:
                result["notes"].append("next BOJ meeting not parsed from schedule page")
            result["sources"]["schedule_url"] = cfg["schedule_url"]
        else:
            result["notes"].append("schedule_url not configured")
    except Exception as e:
        result["status"] = "partial_error"
        result["notes"].append(f"schedule fetch failed: {e}")

    if cfg.get("market_pricing_url"):
        result["sources"]["market_pricing_url"] = cfg["market_pricing_url"]

    return result


def fetch_pboc_hybrid_block(cfg: Dict[str, Any]) -> Dict[str, Any]:
    result = {
        "status": "ok",
        "display_name": cfg["display_name"],
        "current_rate": "N/A",
        "current_rate_date": "N/A",
        "next_meeting": "N/A",
        "market_pricing_path": "N/A",
        "sources": {},
        "notes": [],
    }

    if cfg.get("current_rate_url"):
        result["notes"].append(
            "current_rate_url configured but skipped; no verified stable structured English endpoint for current PBOC policy rate"
        )

    try:
        if cfg.get("schedule_url"):
            text = fetch_text(cfg["schedule_url"])
            latest_meeting = extract_latest_pboc_meeting(text)
            if latest_meeting:
                result["notes"].append(f"latest MPC meeting observed: {latest_meeting}")
                result["detail"] = {"latest_mpc_meeting_date": latest_meeting}
            result["sources"]["schedule_url"] = cfg["schedule_url"]
        else:
            result["notes"].append("schedule_url not configured")
    except Exception as e:
        result["status"] = "partial_error"
        result["notes"].append(f"schedule fetch failed: {e}")

    if cfg.get("market_pricing_url"):
        result["sources"]["market_pricing_url"] = cfg["market_pricing_url"]

    return result


def fetch_central_bank_block(name: str, cfg: Dict[str, Any]) -> Dict[str, Any]:
    provider = cfg.get("provider")

    try:
        if provider == "fed_structured":
            return fetch_fed_structured_block(cfg)
        if provider == "ecb_structured":
            return fetch_ecb_structured_block(cfg)
        if provider == "boj_hybrid_official":
            return fetch_boj_hybrid_block(cfg)
        if provider == "pboc_hybrid_official":
            return fetch_pboc_hybrid_block(cfg)

        return {
            "status": "error",
            "display_name": cfg["display_name"],
            "current_rate": "N/A",
            "current_rate_date": "N/A",
            "next_meeting": "N/A",
            "market_pricing_path": "N/A",
            "sources": {},
            "notes": [f"unsupported provider: {provider}"],
        }
    except Exception as e:
        return {
            "status": "error",
            "display_name": cfg["display_name"],
            "current_rate": "N/A",
            "current_rate_date": "N/A",
            "next_meeting": "N/A",
            "market_pricing_path": "N/A",
            "sources": {},
            "notes": [str(e)],
        }


def fetch_all_central_banks() -> Dict[str, Any]:
    out = {}
    for key, cfg in CENTRAL_BANK_SOURCE_CONFIG.items():
        out[key] = fetch_central_bank_block(key, cfg)
    return out


# =========================================================
# 8. Hardening: smoke test + stale protection
# =========================================================


def days_old_from_taipei(date_str: Optional[str]) -> Optional[int]:
    d = parse_iso_date(date_str)
    if d is None:
        return None
    return (taipei_now().date() - d).days


def max_staleness_days_for_market(item: Dict[str, Any]) -> int:
    market = str(item.get("market", "")).upper()
    if market in {"US", "GLOBAL", "FX", "TW"}:
        return DEFAULT_MAX_STALENESS_DAYS
    return DEFAULT_MAX_STALENESS_DAYS


def collect_stale_market_items(market_data: Dict[str, Dict[str, Any]]) -> list[Dict[str, Any]]:
    stale_items: list[Dict[str, Any]] = []

    for key, item in market_data.items():
        if item.get("status") != "ok":
            continue
        if not item.get("required", True):
            continue

        days_old = days_old_from_taipei(item.get("date"))
        max_days = max_staleness_days_for_market(item)

        if days_old is None or days_old > max_days:
            stale_items.append(
                {
                    "key": key,
                    "display_name": item.get("display_name"),
                    "date": item.get("date"),
                    "days_old": days_old,
                    "max_allowed_days": max_days,
                }
            )

    return stale_items


def collect_stale_foreign_flow_items(foreign_flow_payload: Dict[str, Any]) -> list[Dict[str, Any]]:
    stale_items: list[Dict[str, Any]] = []

    for source_key in ["twse", "tpex"]:
        item = foreign_flow_payload.get(source_key, {})
        if item.get("status") != "ok":
            continue

        days_old = days_old_from_taipei(item.get("date"))
        if days_old is None or days_old > FOREIGN_FLOW_MAX_STALENESS_DAYS:
            stale_items.append(
                {
                    "source": source_key,
                    "date": item.get("date"),
                    "days_old": days_old,
                    "max_allowed_days": FOREIGN_FLOW_MAX_STALENESS_DAYS,
                }
            )

    return stale_items


def build_quality_checks(market_data: Dict[str, Dict[str, Any]], taiwan_view: Dict[str, Any]) -> Dict[str, Any]:
    foreign_flow_payload = taiwan_view.get("foreign_flow", {}).get("source_payload", {})
    stale_market_items = collect_stale_market_items(market_data)
    stale_foreign_flow_items = collect_stale_foreign_flow_items(foreign_flow_payload)
    single_day_raw = taiwan_view.get("foreign_flow", {}).get("single_day_raw")

    return {
        "smoke_test_passed": False,
        "stale_guard_passed": len(stale_market_items) == 0 and len(stale_foreign_flow_items) == 0,
        "stale_market_items": stale_market_items,
        "stale_foreign_flow_items": stale_foreign_flow_items,
        "foreign_flow_single_day_raw_type": type(single_day_raw).__name__ if single_day_raw is not None else "NoneType",
    }


def run_smoke_tests(market_output: Dict[str, Any], central_bank_output: Dict[str, Any]) -> None:
    if market_output.get("schema_version") != SCHEMA_VERSION:
        raise RuntimeError(
            f"market_output schema_version mismatch: {market_output.get('schema_version')} != {SCHEMA_VERSION}"
        )

    if central_bank_output.get("schema_version") != SCHEMA_VERSION:
        raise RuntimeError(
            f"central_bank_output schema_version mismatch: {central_bank_output.get('schema_version')} != {SCHEMA_VERSION}"
        )

    required_ok_count = market_output.get("summary_stats", {}).get("required_ok_count")
    if not isinstance(required_ok_count, int):
        raise RuntimeError("required_ok_count is missing or not int")

    foreign_flow_raw = market_output.get("taiwan_view", {}).get("foreign_flow", {}).get("single_day_raw")
    if foreign_flow_raw is not None and not isinstance(foreign_flow_raw, int):
        raise RuntimeError(
            f"foreign_flow.single_day_raw must be int or None, got {type(foreign_flow_raw).__name__}"
        )


# =========================================================
# 9. 組裝輸出
# =========================================================


def build_summary_stats(market_data: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    ok_items = [v for v in market_data.values() if v.get("status") == "ok"]
    required_items = [v for v in market_data.values() if v.get("required", True)]
    required_ok_items = [v for v in required_items if v.get("status") == "ok"]
    optional_items = [v for v in market_data.values() if not v.get("required", True)]
    optional_error_items = [v for v in optional_items if v.get("status") != "ok"]

    by_category: Dict[str, int] = {}
    for item in ok_items:
        cat = item.get("report_category", "uncategorized")
        by_category[cat] = by_category.get(cat, 0) + 1

    return {
        "total_instruments": len(market_data),
        "ok_count": len(ok_items),
        "error_count": len(market_data) - len(ok_items),
        "required_total": len(required_items),
        "required_ok_count": len(required_ok_items),
        "required_error_count": len(required_items) - len(required_ok_items),
        "optional_total": len(optional_items),
        "optional_error_count": len(optional_error_items),
        "category_counts": by_category,
    }


def build_report_ready_view(market_data: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    ok_items = [v for v in market_data.values() if v.get("status") == "ok"]
    ok_items = sorted(ok_items, key=lambda x: x.get("priority", 9999))

    by_category: Dict[str, list] = {}
    for item in ok_items:
        cat = item.get("report_category", "uncategorized")
        by_category.setdefault(cat, [])
        by_category[cat].append(
            {
                "display_name": item.get("display_name"),
                "display_close": item.get("display_close"),
                "display_change": item.get("display_change"),
                "as_of_label": item.get("as_of_label"),
                "direction": item.get("direction"),
                "required": item.get("required", True),
            }
        )

    return {
        "ordered_keys": [
            k
            for k, _ in sorted(
                ((k, v) for k, v in market_data.items() if v.get("status") == "ok"),
                key=lambda kv: kv[1].get("priority", 9999),
            )
        ],
        "by_category": by_category,
    }


def fetch_market_data() -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}

    for key, cfg in sorted(MARKET_CONFIG.items(), key=lambda x: x[1]["priority"]):
        logger.info("Fetching market item: %s (%s)", cfg["display_name"], cfg["provider"])

        provider = cfg["provider"]
        if provider == "twelve_data":
            out[key] = fetch_market_from_twelve_data(key, cfg)
        elif provider == "fred":
            out[key] = fetch_market_from_fred(key, cfg)
        else:
            out[key] = build_error_result(cfg, f"unsupported provider: {provider}")

    return out


def build_market_output(market_data: Dict[str, Dict[str, Any]], taiwan_view: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "generated_at": now_iso(),
        "schema_version": SCHEMA_VERSION,
        "source_stack": [
            "Twelve Data",
            "FRED",
            "TWSE/TPEX official APIs",
        ],
        "summary_stats": build_summary_stats(market_data),
        "report_ready_view": build_report_ready_view(market_data),
        "taiwan_view": taiwan_view,
        "market_data": market_data,
    }


def build_central_bank_output(central_bank_data: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "generated_at": now_iso(),
        "schema_version": SCHEMA_VERSION,
        "source_stack": [
            "Fed official structured sources",
            "ECB official structured sources",
            "BOJ official sources",
            "PBOC official sources",
        ],
        "central_banks": central_bank_data,
    }


# =========================================================
# 10. main
# =========================================================


def main() -> None:
    logger.info("Starting hardened scheme pipeline...")

    market_data = fetch_market_data()
    foreign_flow_payload = fetch_tw_foreign_flow()
    taiwan_view = build_taiwan_view(market_data, foreign_flow_payload)
    central_bank_data = fetch_all_central_banks()

    market_output = build_market_output(market_data, taiwan_view)
    central_bank_output = build_central_bank_output(central_bank_data)

    quality_checks = build_quality_checks(market_data, taiwan_view)
    market_output["quality_checks"] = quality_checks
    central_bank_output["quality_checks"] = {
        "smoke_test_passed": False,
        "stale_guard_passed": quality_checks["stale_guard_passed"],
        "stale_market_items": quality_checks["stale_market_items"],
        "stale_foreign_flow_items": quality_checks["stale_foreign_flow_items"],
    }

    failed_items = {
        k: v for k, v in market_data.items()
        if v.get("status") != "ok"
    }
    if failed_items:
        logger.error(
            "Failed market items: %s",
            json.dumps(failed_items, ensure_ascii=False, indent=2),
        )

    if quality_checks["stale_market_items"]:
        logger.error(
            "Stale required market items: %s",
            json.dumps(quality_checks["stale_market_items"], ensure_ascii=False, indent=2),
        )

    if quality_checks["stale_foreign_flow_items"]:
        logger.error(
            "Stale foreign flow items: %s",
            json.dumps(quality_checks["stale_foreign_flow_items"], ensure_ascii=False, indent=2),
        )

    run_smoke_tests(market_output, central_bank_output)

    required_ok_count = market_output["summary_stats"]["required_ok_count"]
    if required_ok_count < MIN_REQUIRED_OK_COUNT:
        raise RuntimeError(
            f"required_ok_count ({required_ok_count}) < MIN_REQUIRED_OK_COUNT ({MIN_REQUIRED_OK_COUNT}); refuse to write incomplete snapshot"
        )

    if not quality_checks["stale_guard_passed"]:
        raise RuntimeError("stale_guard failed; refuse to overwrite previous snapshot")

    market_output["quality_checks"]["smoke_test_passed"] = True
    central_bank_output["quality_checks"]["smoke_test_passed"] = True

    atomic_write_json(MARKET_OUTPUT_FILE, market_output)
    atomic_write_json(CENTRAL_BANK_OUTPUT_FILE, central_bank_output)

    logger.info("Done. Wrote %s", MARKET_OUTPUT_FILE)
    logger.info("Done. Wrote %s", CENTRAL_BANK_OUTPUT_FILE)


if __name__ == "__main__":
    main()
