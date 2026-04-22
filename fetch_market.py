import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import pandas as pd
import yfinance as yf

OUTPUT_FILE = "data/market_data.json"
DEFAULT_HISTORY_PERIOD = "1mo"

# 批次抓取重試
BATCH_RETRIES = 3
BATCH_BACKOFF_SECONDS = [20, 60, 120]

INSTRUMENTS: Dict[str, Dict[str, Any]] = {
    "sp500": {
        "display_name": "S&P 500",
        "symbol": "^GSPC",
        "asset_class": "equity_index",
        "report_category": "equities",
        "market": "US",
        "currency": "USD",
        "priority": 10,
        "decimals": 2,
    },
    "nasdaq": {
        "display_name": "NASDAQ Composite",
        "symbol": "^IXIC",
        "asset_class": "equity_index",
        "report_category": "equities",
        "market": "US",
        "currency": "USD",
        "priority": 20,
        "decimals": 2,
    },
    "dow_jones": {
        "display_name": "Dow Jones Industrial Average",
        "symbol": "^DJI",
        "asset_class": "equity_index",
        "report_category": "equities",
        "market": "US",
        "currency": "USD",
        "priority": 30,
        "decimals": 2,
    },
    "vix": {
        "display_name": "CBOE Volatility Index",
        "symbol": "^VIX",
        "asset_class": "volatility_index",
        "report_category": "risk",
        "market": "US",
        "currency": "INDEX_POINTS",
        "priority": 40,
        "decimals": 2,
    },
    "taiex": {
        "display_name": "TAIEX",
        "symbol": "^TWII",
        "asset_class": "equity_index",
        "report_category": "equities",
        "market": "TW",
        "currency": "TWD",
        "priority": 50,
        "decimals": 2,
    },
    "us10y": {
        "display_name": "US 10Y Treasury Yield",
        "symbol": "^TNX",
        "asset_class": "government_bond_yield",
        "report_category": "rates",
        "market": "US",
        "currency": "PERCENT",
        "priority": 60,
        "decimals": 3,
        "transform": {
            "divide_by": 10,
            "unit": "%"
        }
    },
    "wti": {
        "display_name": "WTI Crude Oil",
        "symbol": "CL=F",
        "asset_class": "commodity",
        "report_category": "commodities",
        "market": "GLOBAL",
        "currency": "USD",
        "priority": 70,
        "decimals": 2,
    },
    "gold": {
        "display_name": "Gold",
        "symbol": "GC=F",
        "asset_class": "commodity",
        "report_category": "commodities",
        "market": "GLOBAL",
        "currency": "USD",
        "priority": 80,
        "decimals": 2,
    },
    "dxy": {
        "display_name": "US Dollar Index",
        "symbol": "DX-Y.NYB",
        "asset_class": "fx_index",
        "report_category": "fx",
        "market": "US",
        "currency": "INDEX_POINTS",
        "priority": 90,
        "decimals": 2,
    },
    "usd_twd": {
        "display_name": "USD/TWD",
        "symbol": "TWD=X",
        "asset_class": "fx",
        "report_category": "fx",
        "market": "FX",
        "currency": "TWD_PER_USD",
        "priority": 100,
        "decimals": 4,
    },
}


def safe_round(value: Any, digits: int = 4) -> Optional[float]:
    try:
        if value is None or pd.isna(value):
            return None
        return round(float(value), digits)
    except Exception:
        return None


def format_number(value: Optional[float], decimals: int = 2, use_sign: bool = False) -> str:
    if value is None:
        return "N/A"
    fmt = f"{{:{'+' if use_sign else ''},.{decimals}f}}"
    return fmt.format(value)


def get_now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_freshness_days(date_str: str) -> Optional[int]:
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d").date()
        now_utc = datetime.now(timezone.utc).date()
        return (now_utc - d).days
    except Exception:
        return None


def infer_is_latest_trading_day(date_str: str) -> Optional[bool]:
    freshness_days = get_freshness_days(date_str)
    if freshness_days is None:
        return None
    return freshness_days <= 1


def infer_market_session_label(is_latest_trading_day: Optional[bool]) -> str:
    if is_latest_trading_day is True:
        return "latest"
    if is_latest_trading_day is False:
        return "recent_trading_day"
    return "unknown"


def infer_direction(change: Optional[float]) -> str:
    if change is None:
        return "unknown"
    if change > 0:
        return "up"
    if change < 0:
        return "down"
    return "flat"


def transform_price(raw_value: float, instrument: Dict[str, Any]) -> Tuple[float, Optional[str]]:
    transform_cfg = instrument.get("transform")
    if not transform_cfg:
        return raw_value, None

    divide_by = transform_cfg.get("divide_by", 1)
    unit = transform_cfg.get("unit")
    transformed = raw_value / divide_by if divide_by else raw_value
    return transformed, unit


def calculate_change_metrics(
    close: Optional[float],
    prev_close: Optional[float],
    price_digits: int = 4,
    pct_digits: int = 2
) -> Tuple[Optional[float], Optional[float]]:
    if close is None or prev_close is None:
        return None, None

    change = round(close - prev_close, price_digits)
    if prev_close == 0:
        return change, None

    change_pct = round((change / prev_close) * 100, pct_digits)
    return change, change_pct


def format_close(close: Optional[float], instrument: Dict[str, Any], unit: Optional[str]) -> str:
    if close is None:
        return "N/A"

    decimals = instrument.get("decimals", 2)
    currency = instrument.get("currency")

    if unit == "%":
        return f"{format_number(close, decimals)}%"

    if currency == "PERCENT":
        return f"{format_number(close, decimals)}%"
    return format_number(close, decimals)


def format_change(
    change: Optional[float],
    change_pct: Optional[float],
    instrument: Dict[str, Any],
    unit: Optional[str]
) -> str:
    if change is None:
        return "N/A"

    decimals = instrument.get("decimals", 2)
    currency = instrument.get("currency")

    if unit == "%" or currency == "PERCENT":
        base = f"{format_number(change, decimals, use_sign=True)}pp"
    else:
        base = format_number(change, decimals, use_sign=True)

    if change_pct is None:
        return base

    return f"{base} ({format_number(change_pct, 2, use_sign=True)}%)"


def build_as_of_label(date_str: Optional[str], market_session_label: str) -> str:
    if not date_str:
        return "N/A"
    if market_session_label == "latest":
        return f"latest close ({date_str})"
    if market_session_label == "recent_trading_day":
        return f"recent trading day close ({date_str})"
    return f"available close ({date_str})"


def infer_macro_signal_tag(key: str, direction: str) -> Optional[str]:
    mapping = {
        "sp500": {"up": "risk_on", "down": "risk_off"},
        "nasdaq": {"up": "risk_on", "down": "risk_off"},
        "dow_jones": {"up": "risk_on", "down": "risk_off"},
        "taiex": {"up": "risk_on", "down": "risk_off"},
        "vix": {"up": "risk_off", "down": "risk_on"},
        "us10y": {"up": "rates_up", "down": "rates_down"},
        "wti": {"up": "oil_up", "down": "oil_down"},
        "gold": {"up": "gold_up", "down": "gold_down"},
        "dxy": {"up": "usd_stronger", "down": "usd_weaker"},
        "usd_twd": {"up": "usd_stronger_vs_twd", "down": "twd_stronger_vs_usd"},
    }
    return mapping.get(key, {}).get(direction)


def infer_surprise_flag(asset_class: str, change_pct: Optional[float]) -> Optional[bool]:
    if change_pct is None:
        return None

    thresholds = {
        "equity_index": 1.5,
        "volatility_index": 5.0,
        "government_bond_yield": 1.0,
        "commodity": 2.0,
        "fx_index": 0.75,
        "fx": 0.75,
    }
    threshold = thresholds.get(asset_class, 2.0)
    return abs(change_pct) >= threshold


def build_llm_hint(item: Dict[str, Any]) -> Optional[str]:
    name = item.get("display_name")
    direction = item.get("direction")
    display_change = item.get("display_change")

    if not name or not direction or display_change in {None, "N/A"}:
        return None

    if direction == "up":
        return f"{name} moved higher: {display_change}."
    if direction == "down":
        return f"{name} moved lower: {display_change}."
    if direction == "flat":
        return f"{name} was broadly unchanged: {display_change}."
    return None


def build_error_result(instrument: Dict[str, Any], message: str) -> Dict[str, Any]:
    return {
        "status": "error",
        "display_name": instrument["display_name"],
        "symbol": instrument["symbol"],
        "asset_class": instrument["asset_class"],
        "report_category": instrument["report_category"],
        "market": instrument["market"],
        "currency": instrument["currency"],
        "priority": instrument["priority"],
        "error": message,
    }


def download_all_symbols(symbols: list[str]) -> pd.DataFrame:
    last_error = None

    for i in range(BATCH_RETRIES):
        try:
            df = yf.download(
                tickers=symbols,
                period=DEFAULT_HISTORY_PERIOD,
                interval="1d",
                auto_adjust=False,
                actions=False,
                progress=False,
                threads=False,
                group_by="ticker",
                multi_level_index=True,
                timeout=20,
            )
            if df is not None and not df.empty:
                return df
            last_error = "empty dataframe"
        except Exception as e:
            last_error = str(e)

        if i < BATCH_RETRIES - 1:
            wait_s = BATCH_BACKOFF_SECONDS[i]
            print(f"Batch download failed ({last_error}). Sleep {wait_s}s then retry...")
            time.sleep(wait_s)

    raise RuntimeError(f"batch download failed after {BATCH_RETRIES} retries: {last_error}")


def extract_symbol_history(batch_df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    if batch_df.empty:
        return pd.DataFrame()

    if isinstance(batch_df.columns, pd.MultiIndex):
        level0 = batch_df.columns.get_level_values(0)
        if symbol not in level0:
            return pd.DataFrame()
        df = batch_df[symbol].copy()
    else:
        # 理論上多 ticker 不太會走到這裡，但保底
        df = batch_df.copy()

    if "Close" not in df.columns:
        return pd.DataFrame()

    df = df.dropna(subset=["Close"])
    return df


def build_instrument_result(key: str, instrument: Dict[str, Any], hist: pd.DataFrame) -> Dict[str, Any]:
    if hist.empty:
        return build_error_result(instrument, "no data returned for symbol")

    latest = hist.iloc[-1]
    latest_raw = float(latest["Close"])
    latest_value, transformed_unit = transform_price(latest_raw, instrument)

    latest_date = hist.index[-1].strftime("%Y-%m-%d")
    decimals = instrument.get("decimals", 2)

    close = safe_round(latest_value, decimals)
    freshness_days = get_freshness_days(latest_date)
    is_latest_trading_day = infer_is_latest_trading_day(latest_date)
    market_session_label = infer_market_session_label(is_latest_trading_day)

    result: Dict[str, Any] = {
        "status": "ok",
        "display_name": instrument["display_name"],
        "symbol": instrument["symbol"],
        "asset_class": instrument["asset_class"],
        "report_category": instrument["report_category"],
        "market": instrument["market"],
        "currency": instrument["currency"],
        "priority": instrument["priority"],
        "unit": transformed_unit,
        "date": latest_date,
        "close": close,
        "raw_close": safe_round(latest_raw, 6),
        "freshness_days": freshness_days,
        "is_latest_trading_day": is_latest_trading_day,
        "market_session_label": market_session_label,
        "display_close": format_close(close, instrument, transformed_unit),
        "as_of_label": build_as_of_label(latest_date, market_session_label),
    }

    if len(hist) >= 2:
        prev = hist.iloc[-2]
        prev_raw = float(prev["Close"])
        prev_value, _ = transform_price(prev_raw, instrument)

        prev_date = hist.index[-2].strftime("%Y-%m-%d")
        prev_close = safe_round(prev_value, decimals)

        change, change_pct = calculate_change_metrics(
            close=close,
            prev_close=prev_close,
            price_digits=decimals,
            pct_digits=2,
        )
        direction = infer_direction(change)

        result.update({
            "prev_date": prev_date,
            "prev_close": prev_close,
            "raw_prev_close": safe_round(prev_raw, 6),
            "change": change,
            "change_pct": change_pct,
            "direction": direction,
            "display_change": format_change(change, change_pct, instrument, transformed_unit),
            "macro_signal_tag": infer_macro_signal_tag(key, direction),
            "surprise_flag": infer_surprise_flag(instrument["asset_class"], change_pct),
        })
    else:
        result.update({
            "prev_date": None,
            "prev_close": None,
            "raw_prev_close": None,
            "change": None,
            "change_pct": None,
            "direction": "unknown",
            "display_change": "N/A",
            "macro_signal_tag": None,
            "surprise_flag": None,
            "warning": "only one valid trading day found; change metrics unavailable",
        })

    result["llm_hint"] = build_llm_hint(result)
    return result


def build_summary_stats(market_data: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    ok_items = [v for v in market_data.values() if v.get("status") == "ok"]

    by_category: Dict[str, int] = {}
    surprise_count = 0

    for item in ok_items:
        cat = item.get("report_category", "uncategorized")
        by_category[cat] = by_category.get(cat, 0) + 1
        if item.get("surprise_flag") is True:
            surprise_count += 1

    return {
        "total_instruments": len(market_data),
        "ok_count": len(ok_items),
        "error_count": len(market_data) - len(ok_items),
        "surprise_count": surprise_count,
        "category_counts": by_category,
    }


def build_report_ready_view(market_data: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    ok_items = [v for v in market_data.values() if v.get("status") == "ok"]
    ok_items = sorted(ok_items, key=lambda x: x.get("priority", 9999))

    by_category: Dict[str, list] = {}
    for item in ok_items:
        cat = item.get("report_category", "uncategorized")
        by_category.setdefault(cat, [])
        by_category[cat].append({
            "display_name": item.get("display_name"),
            "display_close": item.get("display_close"),
            "display_change": item.get("display_change"),
            "as_of_label": item.get("as_of_label"),
            "direction": item.get("direction"),
            "macro_signal_tag": item.get("macro_signal_tag"),
            "surprise_flag": item.get("surprise_flag"),
            "llm_hint": item.get("llm_hint"),
        })

    return {
        "ordered_keys": [
            key for key, _ in sorted(
                ((k, v) for k, v in market_data.items() if v.get("status") == "ok"),
                key=lambda kv: kv[1].get("priority", 9999)
            )
        ],
        "by_category": by_category,
    }


def build_taiwan_view(market_data: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    usd_twd_item = market_data.get("usd_twd", {})
    usd_twd_spot = "N/A"

    if usd_twd_item.get("status") == "ok":
        usd_twd_spot = usd_twd_item.get("display_close", "N/A") or "N/A"

    return {
        "foreign_flow": {
            "single_day": "N/A",
            "structural_read": "全球資金仍偏向 AI 與科技股（未見系統性撤出）"
        },
        "semiconductor_supply_chain": {
            "core_logic": [
                "AI需求強（持續）",
                "能源與原物料成本上升（壓縮部分毛利）"
            ],
            "judgment": "需求 > 成本壓力（短期仍偏多），但波動放大"
        },
        "fx": {
            "usd_twd_spot": usd_twd_spot,
            "assessment": [
                "若油價上行 → USD 轉強",
                "若風險偏好回升 → TWD 偏強"
            ],
            "base_case": "區間震盪機率 60%"
        }
    }


def main() -> None:
    market_data: Dict[str, Dict[str, Any]] = {}
    symbols = [cfg["symbol"] for _, cfg in sorted(INSTRUMENTS.items(), key=lambda x: x[1]["priority"])]

    try:
        batch_df = download_all_symbols(symbols)
    except Exception as e:
        print(f"FATAL: {e}")
        # 全批失敗時，直接讓 workflow fail，避免把舊資料覆蓋成全 error JSON
        sys.exit(1)

    for key, instrument in sorted(INSTRUMENTS.items(), key=lambda x: x[1]["priority"]):
        symbol = instrument["symbol"]
        print(f"Processing {instrument['display_name']} ({symbol})...")
        hist = extract_symbol_history(batch_df, symbol)
        result = build_instrument_result(key, instrument, hist)
        market_data[key] = result
        print(f"  -> {result}")

    output = {
        "generated_at": get_now_utc_iso(),
        "schema_version": "Z.2",
        "source": "Yahoo Finance via yfinance",
        "note": (
            "Each instrument contains the latest available close and previous valid close. "
            "Dates follow each instrument's own trading calendar. "
            "display_close/display_change/as_of_label are preformatted for downstream report generation. "
            "^TNX is transformed from Yahoo's quoted value into actual 10Y Treasury yield percent."
        ),
        "summary_stats": build_summary_stats(market_data),
        "report_ready_view": build_report_ready_view(market_data),
        "taiwan_view": build_taiwan_view(market_data),
        "market_data": market_data,
    }

    ok_count = output["summary_stats"]["ok_count"]
    if ok_count == 0:
        print("FATAL: ok_count == 0, refuse to overwrite previous snapshot.")
        sys.exit(1)

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n=== Done. Saved to {OUTPUT_FILE} ===")
    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()