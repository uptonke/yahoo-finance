import json
import logging
import os
import re
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# =========================================================
# 0. 基本設定
# =========================================================

OUTPUT_DIR = Path("data")
MARKET_OUTPUT_FILE = OUTPUT_DIR / "market_data.json"
CENTRAL_BANK_OUTPUT_FILE = OUTPUT_DIR / "central_bank_data.json"

TWELVE_DATA_API_KEY = os.getenv("TWELVE_DATA_API_KEY", "").strip()
FRED_API_KEY = os.getenv("FRED_API_KEY", "").strip()

REQUEST_TIMEOUT = 20
MIN_REQUIRED_OK_COUNT = 4

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
        "current_rate_url": os.getenv("FED_RATE_URL", "").strip(),
        "schedule_url": os.getenv("FED_SCHEDULE_URL", "").strip(),
        "market_pricing_url": os.getenv("FED_MARKET_PRICING_URL", "").strip(),
    },
    "ecb": {
        "display_name": "ECB",
        "current_rate_url": os.getenv("ECB_RATE_URL", "").strip(),
        "schedule_url": os.getenv("ECB_SCHEDULE_URL", "").strip(),
        "market_pricing_url": os.getenv("ECB_MARKET_PRICING_URL", "").strip(),
    },
    "boj": {
        "display_name": "BOJ",
        "current_rate_url": os.getenv("BOJ_RATE_URL", "").strip(),
        "schedule_url": os.getenv("BOJ_SCHEDULE_URL", "").strip(),
        "market_pricing_url": os.getenv("BOJ_MARKET_PRICING_URL", "").strip(),
    },
    "pboc": {
        "display_name": "PBOC",
        "current_rate_url": os.getenv("PBOC_RATE_URL", "").strip(),
        "schedule_url": os.getenv("PBOC_SCHEDULE_URL", "").strip(),
        "market_pricing_url": os.getenv("PBOC_MARKET_PRICING_URL", "").strip(),
    },
}

TW_TWSE_URL = os.getenv("TWSE_FOREIGN_FLOW_URL", "").strip()
TW_TPEX_URL = os.getenv("TPEX_FOREIGN_FLOW_URL", "").strip()

TWSE_FOREIGN_FLOW_API_URL = os.getenv(
    "TWSE_FOREIGN_FLOW_API_URL",
    "https://www.twse.com.tw/rwd/zh/fund/T86",
).strip()
TWSE_FOREIGN_FLOW_SELECT_TYPE = os.getenv("TWSE_FOREIGN_FLOW_SELECT_TYPE", "ALLBUT0999").strip()

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
        allowed_methods=frozenset(["GET"]),
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

    if unit == "%":
        return f"{format_number(value, decimals)}%"
    if currency == "PERCENT":
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


def normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def extract_first_match(text: str, patterns: list[str]) -> Optional[str]:
    for pattern in patterns:
        match = re.search(pattern, text, re.S | re.I)
        if match:
            return normalize_whitespace(match.group(1))
    return None


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


def taipei_today_yyyymmdd() -> str:
    dt = datetime.now(timezone.utc) + timedelta(hours=8)
    return dt.strftime("%Y%m%d")


def roc_or_gregorian_to_yyyymmdd(date_str: Optional[str]) -> str:
    if not date_str:
        return taipei_today_yyyymmdd()

    s = str(date_str).strip()

    # 115/04/22 -> 20260422
    m = re.match(r"^(\d{2,3})/(\d{1,2})/(\d{1,2})$", s)
    if m:
        roc_year = int(m.group(1))
        year = roc_year + 1911
        month = int(m.group(2))
        day = int(m.group(3))
        return f"{year:04d}{month:02d}{day:02d}"

    # 2026/04/22 -> 20260422
    m = re.match(r"^(\d{4})/(\d{1,2})/(\d{1,2})$", s)
    if m:
        year = int(m.group(1))
        month = int(m.group(2))
        day = int(m.group(3))
        return f"{year:04d}{month:02d}{day:02d}"

    # 2026-04-22 -> 20260422
    m = re.match(r"^(\d{4})-(\d{1,2})-(\d{1,2})$", s)
    if m:
        year = int(m.group(1))
        month = int(m.group(2))
        day = int(m.group(3))
        return f"{year:04d}{month:02d}{day:02d}"

    # fallback
    return taipei_today_yyyymmdd()


def fetch_tpex_foreign_flow_api() -> Dict[str, Any]:
    if not TPEX_FOREIGN_FLOW_API_URL:
        return {
            "status": "unconfigured",
            "single_day": "N/A",
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
                "date": None,
                "error": "TPEX API returned no tables",
                "url": TPEX_FOREIGN_FLOW_API_URL,
                "request_payload": payload,
                "raw_payload": raw,
            }

        table = tables[0]
        rows = table.get("data") or []
        report_date = table.get("date")
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
                "date": report_date,
                "error": "TPEX API returned zero valid rows",
                "url": TPEX_FOREIGN_FLOW_API_URL,
                "request_payload": payload,
                "raw_payload": raw,
            }

        return {
            "status": "ok",
            "single_day": format_signed_int(foreign_total_net),
            "date": report_date,
            "source": "official_api_json",
            "title": title,
            "row_count": valid_rows,
            "foreign_ex_dealer_single_day": format_signed_int(foreign_ex_dealer_net),
            "foreign_total_single_day": format_signed_int(foreign_total_net),
            "three_insti_total_single_day": format_signed_int(three_insti_total_net),
            "url": TPEX_FOREIGN_FLOW_API_URL,
            "request_payload": payload,
            "note": "Parsed from official TPEX POST API /www/zh-tw/insti/dailyTrade",
        }
    except Exception as e:
        return {
            "status": "error",
            "single_day": "N/A",
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
                "date": target_date,
                "error": "TWSE API returned no data/fields",
                "url": TWSE_FOREIGN_FLOW_API_URL,
                "request_params": params,
                "raw_payload": raw,
            }

        field_idx = {name: idx for idx, name in enumerate(fields)}

        # TWSE 官方 T86 常見欄位
        idx_foreign_ex = field_idx.get("外陸資買賣超股數(不含外資自營商)")
        idx_foreign_dealer = field_idx.get("外資自營商買賣超股數")
        idx_three_insti = field_idx.get("三大法人買賣超股數")

        if idx_foreign_ex is None:
            return {
                "status": "error",
                "single_day": "N/A",
                "date": target_date,
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
            v_dealer = clean_int(row[idx_foreign_dealer]) if idx_foreign_dealer is not None and idx_foreign_dealer < len(row) else 0
            v_three = clean_int(row[idx_three_insti]) if idx_three_insti is not None and idx_three_insti < len(row) else None

            if v_ex is not None:
                foreign_ex_dealer_net += v_ex
                valid_rows += 1

            if v_dealer is not None:
                foreign_dealer_net += v_dealer

            if v_three is not None:
                three_insti_total_net += v_three

        foreign_total_net = foreign_ex_dealer_net + foreign_dealer_net

        if valid_rows == 0:
            return {
                "status": "error",
                "single_day": "N/A",
                "date": target_date,
                "error": "TWSE API returned zero valid rows",
                "url": TWSE_FOREIGN_FLOW_API_URL,
                "request_params": params,
                "raw_payload": raw,
            }

        return {
            "status": "ok",
            "single_day": format_signed_int(foreign_total_net),
            "date": target_date,
            "source": "official_api_json",
            "title": raw.get("title"),
            "stat": raw.get("stat"),
            "row_count": valid_rows,
            "foreign_ex_dealer_single_day": format_signed_int(foreign_ex_dealer_net),
            "foreign_dealer_single_day": format_signed_int(foreign_dealer_net),
            "foreign_total_single_day": format_signed_int(foreign_total_net),
            "three_insti_total_single_day": format_signed_int(three_insti_total_net),
            "url": TWSE_FOREIGN_FLOW_API_URL,
            "request_params": params,
            "note": "Parsed from official TWSE T86 JSON route.",
        }
    except Exception as e:
        return {
            "status": "error",
            "single_day": "N/A",
            "date": target_date,
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

    twse_val = clean_int(
        foreign_flow_payload.get("twse", {}).get("foreign_total_single_day")
        or foreign_flow_payload.get("twse", {}).get("single_day")
    )
    tpex_val = clean_int(
        foreign_flow_payload.get("tpex", {}).get("foreign_total_single_day")
        or foreign_flow_payload.get("tpex", {}).get("single_day")
    )

    combined_val = None
    if twse_val is not None and tpex_val is not None:
        combined_val = twse_val + tpex_val
        source_used = "twse+tpex"
    elif twse_val is not None:
        combined_val = twse_val
        source_used = "twse"
    elif tpex_val is not None:
        combined_val = tpex_val
        source_used = "tpex"
    else:
        source_used = None

    return {
        "foreign_flow": {
            "single_day": format_signed_int(combined_val),
            "source_used": source_used,
            "market_scope": "listed+otc" if source_used == "twse+tpex" else source_used,
            "structural_read": "全球資金仍偏向 AI 與科技股（未見系統性撤出）",
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
# 7. 央行動態（骨架）
# =========================================================


def extract_basic_rate_and_date_from_text(text: str) -> Dict[str, Any]:
    rate_match = re.search(r"(\d+(?:\.\d+)?)\s?%", text)
    date_match = re.search(r"(\d{4}-\d{2}-\d{2})", text)

    return {
        "current_rate": rate_match.group(1) + "%" if rate_match else "N/A",
        "next_meeting": date_match.group(1) if date_match else "N/A",
    }


def fetch_central_bank_block(name: str, cfg: Dict[str, Any]) -> Dict[str, Any]:
    result = {
        "status": "ok",
        "display_name": cfg["display_name"],
        "current_rate": "N/A",
        "next_meeting": "N/A",
        "market_pricing_path": "N/A",
        "sources": {},
        "notes": [],
    }

    try:
        if cfg.get("current_rate_url"):
            text = fetch_text(cfg["current_rate_url"])
            parsed = extract_basic_rate_and_date_from_text(text)
            result["current_rate"] = parsed.get("current_rate", "N/A")
            result["sources"]["current_rate_url"] = cfg["current_rate_url"]
        else:
            result["notes"].append("current_rate_url not configured")
    except Exception as e:
        result["status"] = "partial_error"
        result["notes"].append(f"current_rate fetch failed: {e}")

    try:
        if cfg.get("schedule_url"):
            text = fetch_text(cfg["schedule_url"])
            parsed = extract_basic_rate_and_date_from_text(text)
            if parsed.get("next_meeting") != "N/A":
                result["next_meeting"] = parsed["next_meeting"]
            result["sources"]["schedule_url"] = cfg["schedule_url"]
        else:
            result["notes"].append("schedule_url not configured")
    except Exception as e:
        result["status"] = "partial_error"
        result["notes"].append(f"schedule fetch failed: {e}")

    try:
        if cfg.get("market_pricing_url"):
            text = fetch_text(cfg["market_pricing_url"])
            result["market_pricing_path"] = text[:300].strip() if text else "N/A"
            result["sources"]["market_pricing_url"] = cfg["market_pricing_url"]
        else:
            result["notes"].append("market_pricing_url not configured")
    except Exception as e:
        result["status"] = "partial_error"
        result["notes"].append(f"market pricing fetch failed: {e}")

    return result


def fetch_all_central_banks() -> Dict[str, Any]:
    out = {}
    for key, cfg in CENTRAL_BANK_SOURCE_CONFIG.items():
        out[key] = fetch_central_bank_block(key, cfg)
    return out


# =========================================================
# 8. 組裝輸出
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
        "schema_version": "A.6",
        "source_stack": [
            "Twelve Data",
            "FRED",
            "TWSE/TPEX official pages",
        ],
        "summary_stats": build_summary_stats(market_data),
        "report_ready_view": build_report_ready_view(market_data),
        "taiwan_view": taiwan_view,
        "market_data": market_data,
    }


def build_central_bank_output(central_bank_data: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "generated_at": now_iso(),
        "schema_version": "A.6",
        "source_stack": [
            "Fed official sources",
            "ECB official sources",
            "BOJ official sources",
            "PBOC/CFETS official sources",
        ],
        "central_banks": central_bank_data,
    }


# =========================================================
# 9. main
# =========================================================


def main() -> None:
    logger.info("Starting scheme A pipeline...")

    market_data = fetch_market_data()
    foreign_flow_payload = fetch_tw_foreign_flow()
    taiwan_view = build_taiwan_view(market_data, foreign_flow_payload)
    central_bank_data = fetch_all_central_banks()

    market_output = build_market_output(market_data, taiwan_view)
    central_bank_output = build_central_bank_output(central_bank_data)

    failed_items = {
        k: v for k, v in market_data.items()
        if v.get("status") != "ok"
    }
    if failed_items:
        logger.error(
            "Failed market items: %s",
            json.dumps(failed_items, ensure_ascii=False, indent=2),
        )

    required_ok_count = market_output["summary_stats"]["required_ok_count"]
    if required_ok_count < MIN_REQUIRED_OK_COUNT:
        raise RuntimeError(
            f"required_ok_count ({required_ok_count}) < MIN_REQUIRED_OK_COUNT ({MIN_REQUIRED_OK_COUNT}); refuse to write incomplete snapshot"
        )

    atomic_write_json(MARKET_OUTPUT_FILE, market_output)
    atomic_write_json(CENTRAL_BANK_OUTPUT_FILE, central_bank_output)

    logger.info("Done. Wrote %s", MARKET_OUTPUT_FILE)
    logger.info("Done. Wrote %s", CENTRAL_BANK_OUTPUT_FILE)


if __name__ == "__main__":
    main()
