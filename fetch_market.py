import json
import os
import time
from datetime import datetime, timezone

import pandas as pd
import yfinance as yf

OUTPUT_FILE = "data/market_data.json"
DEFAULT_HISTORY_PERIOD = "1mo"
MAX_RETRIES = 3
RETRY_SLEEP_SECONDS = 1.5


INSTRUMENTS = {
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


def safe_round(value, digits=4):
    try:
        if value is None or pd.isna(value):
            return None
        return round(float(value), digits)
    except Exception:
        return None


def calculate_change_metrics(close, prev_close, price_digits=4, pct_digits=2):
    if close is None or prev_close is None:
        return None, None

    change = round(close - prev_close, price_digits)

    if prev_close == 0:
        return change, None

    change_pct = round((change / prev_close) * 100, pct_digits)
    return change, change_pct


def transform_price(raw_value, instrument):
    transform_cfg = instrument.get("transform")
    if not transform_cfg:
        return raw_value, None

    divide_by = transform_cfg.get("divide_by", 1)
    unit = transform_cfg.get("unit")
    transformed = raw_value / divide_by if divide_by else raw_value
    return transformed, unit


def get_freshness_days(latest_date_str):
    try:
        latest_date = datetime.strptime(latest_date_str, "%Y-%m-%d").date()
        now_utc = datetime.now(timezone.utc).date()
        return (now_utc - latest_date).days
    except Exception:
        return None


def infer_is_latest_trading_day(latest_date_str):
    freshness_days = get_freshness_days(latest_date_str)
    if freshness_days is None:
        return None
    return freshness_days <= 1


def infer_market_session_label(is_latest_trading_day):
    if is_latest_trading_day is True:
        return "latest"
    if is_latest_trading_day is False:
        return "recent_trading_day"
    return "unknown"


def infer_direction(change):
    if change is None:
        return "unknown"
    if change > 0:
        return "up"
    if change < 0:
        return "down"
    return "flat"


def format_number(value, decimals=2, use_sign=False):
    if value is None:
        return "N/A"
    fmt = f"{{:{'+' if use_sign else ''},.{decimals}f}}"
    return fmt.format(value)


def format_close(close, instrument, unit=None):
    if close is None:
        return "N/A"

    decimals = instrument.get("decimals", 2)
    currency = instrument.get("currency")

    if unit == "%":
        return f"{format_number(close, decimals)}%"

    if currency == "USD":
        return format_number(close, decimals)
    if currency == "TWD":
        return format_number(close, decimals)
    if currency == "PERCENT":
        return f"{format_number(close, decimals)}%"
    if currency in {"INDEX_POINTS", "TWD_PER_USD"}:
        return format_number(close, decimals)

    return format_number(close, decimals)


def format_change(change, change_pct, instrument, unit=None):
    if change is None:
        return "N/A"

    decimals = instrument.get("decimals", 2)

    if unit == "%":
        base = f"{format_number(change, decimals, use_sign=True)}pp"
    elif instrument.get("currency") == "PERCENT":
        base = f"{format_number(change, decimals, use_sign=True)}pp"
    else:
        base = format_number(change, decimals, use_sign=True)

    if change_pct is None:
        return base

    return f"{base} ({format_number(change_pct, 2, use_sign=True)}%)"


def build_as_of_label(date_str, market_session_label):
    if not date_str:
        return "N/A"
    if market_session_label == "latest":
        return f"latest close ({date_str})"
    if market_session_label == "recent_trading_day":
        return f"recent trading day close ({date_str})"
    return f"available close ({date_str})"


def build_error_result(instrument, message):
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


def fetch_instrument(instrument):
    symbol = instrument["symbol"]
    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(
                period=DEFAULT_HISTORY_PERIOD,
                auto_adjust=False,
                actions=False
            )

            if hist.empty:
                return build_error_result(instrument, "no data")

            hist = hist.dropna(subset=["Close"])

            if hist.empty:
                return build_error_result(instrument, "no close data")

            latest = hist.iloc[-1]
            latest_raw = float(latest["Close"])
            latest_value, transformed_unit = transform_price(latest_raw, instrument)

            latest_date = hist.index[-1].strftime("%Y-%m-%d")
            close = safe_round(latest_value, instrument.get("decimals", 2))

            freshness_days = get_freshness_days(latest_date)
            is_latest_trading_day = infer_is_latest_trading_day(latest_date)
            market_session_label = infer_market_session_label(is_latest_trading_day)

            result = {
                "status": "ok",
                "display_name": instrument["display_name"],
                "symbol": symbol,
                "asset_class": instrument["asset_class"],
                "report_category": instrument["report_category"],
                "market": instrument["market"],
                "currency": instrument["currency"],
                "priority": instrument["priority"],
                "unit": transformed_unit,
                "date": latest_date,
                "close": close,
                "freshness_days": freshness_days,
                "is_latest_trading_day": is_latest_trading_day,
                "market_session_label": market_session_label,
            }

            if len(hist) >= 2:
                prev = hist.iloc[-2]
                prev_raw = float(prev["Close"])
                prev_value, _ = transform_price(prev_raw, instrument)

                prev_date = hist.index[-2].strftime("%Y-%m-%d")
                prev_close = safe_round(prev_value, instrument.get("decimals", 2))
                change, change_pct = calculate_change_metrics(
                    close,
                    prev_close,
                    price_digits=instrument.get("decimals", 2),
                    pct_digits=2,
                )

                direction = infer_direction(change)

                result.update({
                    "prev_date": prev_date,
                    "prev_close": prev_close,
                    "change": change,
                    "change_pct": change_pct,
                    "direction": direction,
                    "display_close": format_close(close, instrument, transformed_unit),
                    "display_change": format_change(change, change_pct, instrument, transformed_unit),
                    "as_of_label": build_as_of_label(latest_date, market_session_label),
                })
            else:
                result.update({
                    "direction": "unknown",
                    "display_close": format_close(close, instrument, transformed_unit),
                    "display_change": "N/A",
                    "as_of_label": build_as_of_label(latest_date, market_session_label),
                    "warning": "only one valid trading day found; change metrics unavailable",
                })

            return result

        except Exception as e:
            last_error = str(e)
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_SLEEP_SECONDS)

    return build_error_result(
        instrument,
        f"fetch failed after {MAX_RETRIES} retries: {last_error}"
    )


def build_summary_stats(market_data):
    ok_items = [v for v in market_data.values() if v.get("status") == "ok"]

    by_category = {}
    for item in ok_items:
        cat = item.get("report_category", "uncategorized")
        by_category.setdefault(cat, 0)
        by_category[cat] += 1

    return {
        "total_instruments": len(market_data),
        "ok_count": len(ok_items),
        "error_count": len(market_data) - len(ok_items),
        "category_counts": by_category,
    }


def main():
    market_data = {}

    for key, instrument in sorted(INSTRUMENTS.items(), key=lambda x: x[1]["priority"]):
        print(f"Fetching {instrument['display_name']} ({instrument['symbol']})...")
        result = fetch_instrument(instrument)
        market_data[key] = result
        print(f"  -> {result}")

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "schema_version": "3.0",
        "source": "Yahoo Finance via yfinance",
        "note": (
            "Each instrument contains the latest available close and previous valid close. "
            "Dates follow each instrument's own trading calendar. "
            "display_close and display_change are preformatted for downstream report generation."
        ),
        "summary_stats": build_summary_stats(market_data),
        "market_data": market_data,
    }

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n=== Done. Saved to {OUTPUT_FILE} ===")
    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
