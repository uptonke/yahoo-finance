#!/usr/bin/env python3
from __future__ import annotations

import math
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests
from supabase import create_client

TAIPEI_TZ = timezone(timedelta(hours=8))
TODAY_TPE = datetime.now(TAIPEI_TZ).date()

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "") or os.getenv("SUPABASE_SECRET_KEY", "")
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "portfolio_db")
PORTFOLIO_ROW_ID = int(os.getenv("PORTFOLIO_ROW_ID", "1"))
FRED_API_KEY = os.getenv("FRED_API_KEY", "")

YAHOO_SYMBOLS = [
    "SPY", "QQQ", "RSP", "IWM", "HYG", "JNK", "LQD", "TLT", "IEF", "SHY",
    "^VIX", "^VXN", "^MOVE", "DX-Y.NYB", "^TNX", "GC=F", "CL=F", "BTC-USD",
]
CORE_YAHOO_SYMBOLS = ["SPY", "QQQ", "RSP", "IWM", "HYG", "JNK", "LQD", "TLT", "^VIX", "DX-Y.NYB", "BTC-USD"]

FRED_SERIES = {
    "hy_oas": "BAMLH0A0HYM2",
    "ig_oas": "BAMLC0A0CM",
    "financial_stress": "STLFSI4",
    "nfci": "NFCI",
    "dgs10": "DGS10",
    "dgs2": "DGS2",
    "real_10y": "DFII10",
    "breakeven_10y": "T10YIE",
    "fed_balance_sheet": "WALCL",
    "reverse_repo": "RRPONTSYD",
}
CORE_FRED_KEYS = ["hy_oas", "ig_oas", "financial_stress", "nfci", "real_10y", "breakeven_10y"]


def finite(x: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        v = float(x)
        return v if math.isfinite(v) else default
    except Exception:
        return default


def clamp(x: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, float(x)))


def pct_change(series: Optional[pd.Series], periods: int) -> Optional[float]:
    if series is None:
        return None
    s = pd.to_numeric(series, errors="coerce").dropna()
    if len(s) <= periods:
        return None
    base = finite(s.iloc[-periods - 1])
    last = finite(s.iloc[-1])
    if base is None or base == 0 or last is None:
        return None
    return (last / base - 1.0) * 100.0


def last_value(series: Optional[pd.Series]) -> Optional[float]:
    if series is None:
        return None
    s = pd.to_numeric(series, errors="coerce").dropna()
    return finite(s.iloc[-1]) if len(s) else None


def hv20(series: Optional[pd.Series]) -> Optional[float]:
    if series is None:
        return None
    s = pd.to_numeric(series, errors="coerce").dropna()
    if len(s) < 25:
        return None
    ret = s.pct_change().dropna().tail(20)
    if ret.empty:
        return None
    return float(ret.std() * math.sqrt(252) * 100.0)


def moving_average(series: Optional[pd.Series], window: int) -> Optional[float]:
    if series is None:
        return None
    s = pd.to_numeric(series, errors="coerce").dropna()
    if len(s) < window:
        return None
    return finite(s.tail(window).mean())


def http_get_with_retry(
    url: str,
    params: Dict[str, Any],
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 20,
    retries: int = 4,
) -> requests.Response:
    last_exc: Optional[BaseException] = None
    for attempt in range(retries):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=timeout)
            if resp.status_code in {429, 500, 502, 503, 504} and attempt < retries - 1:
                time.sleep(1.5 * (2 ** attempt))
                continue
            resp.raise_for_status()
            return resp
        except Exception as exc:
            last_exc = exc
            if attempt < retries - 1:
                time.sleep(1.5 * (2 ** attempt))
                continue
    raise RuntimeError(str(last_exc) if last_exc else "request failed")


def fetch_yahoo_chart(symbol: str, days: int = 430) -> pd.Series:
    now = int(datetime.now(timezone.utc).timestamp())
    start = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp())
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    params = {"period1": start, "period2": now, "interval": "1d", "events": "history"}
    resp = http_get_with_retry(
        url,
        params=params,
        headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
        timeout=20,
        retries=3,
    )
    result = (((resp.json().get("chart") or {}).get("result") or [])[:1])
    if not result:
        raise RuntimeError(f"YahooChart empty for {symbol}")
    r = result[0]
    ts = r.get("timestamp") or []
    quote = ((((r.get("indicators") or {}).get("quote") or [])[:1]) or [{}])[0]
    close = quote.get("close") or []
    if not ts or not close:
        raise RuntimeError(f"YahooChart malformed for {symbol}")
    s = pd.Series(pd.to_numeric(close, errors="coerce"), index=pd.to_datetime(ts, unit="s"), name=symbol)
    return s.dropna().sort_index()


def fetch_fred_series(series_id: str, days: int = 760) -> pd.Series:
    if not FRED_API_KEY:
        raise RuntimeError("FRED_API_KEY missing")
    start = TODAY_TPE - timedelta(days=days)
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "observation_start": str(start),
    }
    resp = http_get_with_retry(url, params=params, timeout=20, retries=4)
    obs = resp.json().get("observations") or []
    rows = []
    for o in obs:
        val = o.get("value")
        if val in {None, "", "."}:
            continue
        v = finite(val)
        if v is None:
            continue
        rows.append((pd.to_datetime(o.get("date")), v))
    if not rows:
        raise RuntimeError(f"FRED empty for {series_id}")
    return pd.Series([v for _, v in rows], index=[d for d, _ in rows], name=series_id).sort_index()


def z_score(series: Optional[pd.Series], window: int = 252) -> Optional[float]:
    if series is None:
        return None
    s = pd.to_numeric(series, errors="coerce").dropna().tail(window)
    if len(s) < 30:
        return None
    std = float(s.std())
    if std <= 0:
        return 0.0
    return float((s.iloc[-1] - s.mean()) / std)


def label_trend(score: float) -> str:
    if score >= 80:
        return "強勢延伸"
    if score >= 60:
        return "趨勢偏多"
    if score >= 45:
        return "趨勢中性"
    if score >= 30:
        return "趨勢轉弱"
    return "下行趨勢"


def label_breadth(score: float) -> str:
    if score >= 65:
        return "廣度改善"
    if score >= 50:
        return "廣度中性"
    if score >= 40:
        return "廣度偏弱"
    return "廣度不足"


def label_credit(score: float, quality: str = "FULL") -> str:
    if quality == "PROXY_ONLY":
        return "Proxy-only"
    if quality == "INVALID":
        return "資料無效"
    if score >= 65:
        return "信用穩定"
    if score >= 50:
        return "信用中性"
    if score >= 35:
        return "信用偏弱"
    return "信用壓力"


def label_volatility(score: float) -> str:
    if score >= 80:
        return "低波穩定"
    if score >= 55:
        return "正常波動"
    if score >= 30:
        return "波動升溫"
    return "高壓警戒"


def label_liquidity(score: float) -> str:
    if score >= 65:
        return "流動性寬鬆"
    if score >= 50:
        return "流動性中性"
    if score >= 35:
        return "流動性偏緊"
    return "流動性壓力"


def label_cross_asset(score: float) -> str:
    if score >= 65:
        return "跨資產確認"
    if score >= 50:
        return "跨資產中性"
    if score >= 40:
        return "跨資產偏弱"
    return "跨資產未確認"


def label_adjusted(score: float) -> str:
    if score < 20:
        return "系統性危機"
    if score < 40:
        return "偏空防守"
    if score < 55:
        return "觀望"
    if score < 70:
        return "脆弱偏多"
    if score < 85:
        return "Risk-on"
    return "強勢過熱"


def score_trend(prices: Dict[str, pd.Series]) -> Dict[str, Any]:
    parts = []
    notes = []
    details: Dict[str, Any] = {}
    for sym in ["SPY", "QQQ"]:
        s = prices.get(sym)
        last = last_value(s)
        ma50 = moving_average(s, 50)
        ma200 = moving_average(s, 200)
        m20 = pct_change(s, 20)
        m63 = pct_change(s, 63)
        score = 50.0
        if last is not None and ma50 is not None:
            score += 12 if last > ma50 else -12
        if last is not None and ma200 is not None:
            score += 18 if last > ma200 else -18
        if m20 is not None:
            score += max(-10, min(10, m20 * 1.5))
        if m63 is not None:
            score += max(-10, min(10, m63 * 0.8))
        score = clamp(score)
        parts.append(score)
        details[sym] = {"last": last, "ma50": ma50, "ma200": ma200, "mom20d": m20, "mom63d": m63, "score": round(score, 2)}
        notes.append(f"{sym} 20D {m20:+.1f}%" if m20 is not None else f"{sym} 20D N/A")
    final = float(np.mean(parts)) if parts else 50.0
    return {"score": round(final, 2), "label": label_trend(final), "details": details, "note": "；".join(notes)}


def score_breadth(prices: Dict[str, pd.Series]) -> Dict[str, Any]:
    details: Dict[str, Any] = {}
    comps = [("RSP", "SPY"), ("IWM", "SPY")]
    scores = []
    notes = []
    for a, b in comps:
        if a not in prices or b not in prices:
            continue
        rel = (prices[a] / prices[b]).dropna()
        m20 = pct_change(rel, 20)
        m63 = pct_change(rel, 63)
        score = 50.0
        if m20 is not None:
            score += max(-25, min(25, m20 * 8.0))
        if m63 is not None:
            score += max(-15, min(15, m63 * 4.0))
        score = clamp(score)
        scores.append(score)
        details[f"{a}/{b}"] = {"rel_20d": m20, "rel_63d": m63, "score": round(score, 2)}
        notes.append(f"{a}/{b} 20D {m20:+.2f}%" if m20 is not None else f"{a}/{b} N/A")
    final = float(np.mean(scores)) if scores else 50.0
    return {"score": round(final, 2), "label": label_breadth(final), "details": details, "note": "；".join(notes)}


def oas_score(value: Optional[float], z: Optional[float], kind: str) -> Optional[float]:
    if value is None:
        return None
    if kind == "HY":
        score = 70.0
        if value > 7.0:
            score -= 45
        elif value > 6.0:
            score -= 35
        elif value > 4.5:
            score -= 20
        elif value > 3.5:
            score -= 10
        elif value < 3.0:
            score += 10
    else:
        score = 70.0
        if value > 2.5:
            score -= 35
        elif value > 2.0:
            score -= 25
        elif value > 1.5:
            score -= 12
        elif value < 1.0:
            score += 8
    if z is not None:
        score -= max(-15, min(22, z * 8.0))
    return clamp(score)


def score_credit(prices: Dict[str, pd.Series], fred: Dict[str, pd.Series]) -> Dict[str, Any]:
    details: Dict[str, Any] = {}
    components: List[Tuple[str, float, float]] = []
    notes = []

    for name, a, b, weight in [("HYG/TLT", "HYG", "TLT", 0.30), ("JNK/LQD", "JNK", "LQD", 0.20)]:
        if a in prices and b in prices:
            rel = (prices[a] / prices[b]).dropna()
            m20 = pct_change(rel, 20)
            score = 50.0 + (max(-25, min(25, m20 * 10.0)) if m20 is not None else 0.0)
            score = clamp(score)
            components.append((name, score, weight))
            details[name] = {"rel_20d": m20, "score": round(score, 2), "weight": weight}
            notes.append(f"{name} 20D {m20:+.2f}%" if m20 is not None else f"{name} N/A")

    hy = fred.get("hy_oas")
    hy_present = hy is not None and not hy.empty
    if hy_present:
        val = last_value(hy)
        z = z_score(hy)
        score = oas_score(val, z, "HY")
        if score is not None:
            components.append(("HY_OAS", score, 0.30))
        details["HY_OAS"] = {"value": val, "z": z, "score": round(score, 2) if score is not None else None, "weight": 0.30, "series": FRED_SERIES["hy_oas"]}
        notes.append(f"HY OAS {val:.2f}%" if val is not None else "HY OAS N/A")

    ig = fred.get("ig_oas")
    ig_present = ig is not None and not ig.empty
    if ig_present:
        val = last_value(ig)
        z = z_score(ig)
        score = oas_score(val, z, "IG")
        if score is not None:
            components.append(("IG_OAS", score, 0.20))
        details["IG_OAS"] = {"value": val, "z": z, "score": round(score, 2) if score is not None else None, "weight": 0.20, "series": FRED_SERIES["ig_oas"]}
        notes.append(f"IG OAS {val:.2f}%" if val is not None else "IG OAS N/A")

    if not components:
        return {"score": 50.0, "label": "資料無效", "confidence": "INVALID", "details": details, "note": "Credit data N/A"}

    weight_sum = sum(w for _, _, w in components)
    final = sum(score * w for _, score, w in components) / weight_sum if weight_sum > 0 else 50.0

    if hy_present and ig_present:
        confidence = "FULL"
    elif hy_present or ig_present:
        confidence = "PARTIAL"
        final = min(final, 60.0)
    else:
        confidence = "PROXY_ONLY"
        final = min(final, 50.0)

    final = clamp(final)
    details["component_weights"] = {name: weight for name, _, weight in components}
    details["oas_available"] = {"hy_oas": hy_present, "ig_oas": ig_present}
    return {"score": round(final, 2), "label": label_credit(final, confidence), "confidence": confidence, "details": details, "note": "；".join(notes)}


def score_volatility(prices: Dict[str, pd.Series]) -> Dict[str, Any]:
    details: Dict[str, Any] = {}
    scores = []
    notes = []
    vix = last_value(prices.get("^VIX"))
    vxn = last_value(prices.get("^VXN"))
    move = last_value(prices.get("^MOVE"))
    spy_hv = hv20(prices.get("SPY"))
    qqq_hv = hv20(prices.get("QQQ"))

    def vol_to_score(v: Optional[float], low: float, high: float) -> Optional[float]:
        if v is None:
            return None
        return clamp(100.0 - (v - low) / (high - low) * 70.0)

    for name, val, low, high in [
        ("VIX", vix, 12, 35),
        ("VXN", vxn, 16, 45),
        ("MOVE", move, 80, 160),
        ("SPY_HV20", spy_hv, 10, 35),
        ("QQQ_HV20", qqq_hv, 15, 45),
    ]:
        sc = vol_to_score(val, low, high)
        if sc is not None:
            scores.append(sc)
        details[name] = {"value": val, "score": round(sc, 2) if sc is not None else None}
    if vix is not None:
        notes.append(f"VIX {vix:.1f}")
    if spy_hv is not None:
        notes.append(f"SPY HV20 {spy_hv:.1f}%")
    final = float(np.mean(scores)) if scores else 50.0
    return {"score": round(final, 2), "label": label_volatility(final), "details": details, "note": "；".join(notes)}


def score_liquidity(prices: Dict[str, pd.Series], fred: Dict[str, pd.Series]) -> Dict[str, Any]:
    details: Dict[str, Any] = {}
    scores = []
    notes = []
    dxy_m20 = pct_change(prices.get("DX-Y.NYB"), 20)
    tnx_m20 = pct_change(prices.get("^TNX"), 20)
    gold_m20 = pct_change(prices.get("GC=F"), 20)
    btc_m20 = pct_change(prices.get("BTC-USD"), 20)

    dxy_score = 50.0 - (max(-20, min(20, dxy_m20 * 6.0)) if dxy_m20 is not None else 0.0)
    tnx_score = 50.0 - (max(-15, min(15, tnx_m20 * 1.5)) if tnx_m20 is not None else 0.0)
    scores.extend([clamp(dxy_score), clamp(tnx_score)])
    details.update({
        "DXY_20D": {"value": dxy_m20, "score": round(clamp(dxy_score), 2)},
        "TNX_20D": {"value": tnx_m20, "score": round(clamp(tnx_score), 2)},
        "Gold_20D": {"value": gold_m20},
        "BTC_20D": {"value": btc_m20},
    })

    for key in ["financial_stress", "nfci", "real_10y"]:
        s = fred.get(key)
        if s is None or s.empty:
            continue
        val = last_value(s)
        z = z_score(s)
        score = 55.0
        if z is not None:
            score -= max(-20, min(25, z * 10.0))
        if key == "real_10y" and val is not None:
            score -= max(-10, min(10, (val - 1.5) * 5.0))
        score = clamp(score)
        scores.append(score)
        details[key] = {"value": val, "z": z, "score": round(score, 2), "series": FRED_SERIES[key]}

    if dxy_m20 is not None:
        notes.append(f"DXY 20D {dxy_m20:+.1f}%")
    if "financial_stress" in details and details["financial_stress"].get("value") is not None:
        notes.append(f"STLFSI {details['financial_stress']['value']:.2f}")
    final = float(np.mean(scores)) if scores else 50.0
    return {"score": round(final, 2), "label": label_liquidity(final), "details": details, "note": "；".join(notes)}


def score_cross_asset(prices: Dict[str, pd.Series], fred: Dict[str, pd.Series]) -> Dict[str, Any]:
    gold = pct_change(prices.get("GC=F"), 20)
    oil = pct_change(prices.get("CL=F"), 20)
    btc = pct_change(prices.get("BTC-USD"), 20)
    breakeven = fred.get("breakeven_10y")
    breakeven_val = last_value(breakeven) if breakeven is not None else None
    breakeven_m = pct_change(breakeven, 20) if breakeven is not None else None
    score = 50.0
    if btc is not None:
        score += max(-15, min(15, btc * 0.8))
    if gold is not None and btc is not None and gold > 3 and btc < 0:
        score -= 8
    if oil is not None and oil > 8:
        score -= 6
    if breakeven_m is not None and breakeven_m > 3:
        score -= 5
    score = clamp(score)
    details = {"Gold_20D": gold, "Oil_20D": oil, "BTC_20D": btc, "Breakeven10Y": breakeven_val, "Breakeven10Y_20D": breakeven_m}
    note = "；".join([x for x in [f"BTC 20D {btc:+.1f}%" if btc is not None else None, f"Gold 20D {gold:+.1f}%" if gold is not None else None] if x])
    return {"score": round(score, 2), "label": label_cross_asset(score), "details": details, "note": note}


def raw_score_from_modules(modules: Dict[str, Dict[str, Any]], weights: Dict[str, float]) -> float:
    return round(clamp(sum((finite(modules[k].get("score"), 50) or 50) * w for k, w in weights.items())), 2)


def data_quality(prices: Dict[str, pd.Series], fred: Dict[str, pd.Series], fred_errors: List[str], yahoo_errors: List[str]) -> Dict[str, Any]:
    missing_core_yahoo = [s for s in CORE_YAHOO_SYMBOLS if s not in prices]
    missing_core_fred = [k for k in CORE_FRED_KEYS if k not in fred]

    if not FRED_API_KEY:
        fred_status = "OFF"
    elif not missing_core_fred:
        fred_status = "ON"
    else:
        fred_status = "PARTIAL"

    if not missing_core_yahoo:
        yahoo_status = "OK"
    elif len(missing_core_yahoo) < len(CORE_YAHOO_SYMBOLS):
        yahoo_status = "PARTIAL"
    else:
        yahoo_status = "FAILED"

    hy_ok = "hy_oas" in fred
    ig_ok = "ig_oas" in fred
    if hy_ok and ig_ok:
        credit_quality = "FULL"
    elif hy_ok or ig_ok:
        credit_quality = "PARTIAL"
    else:
        credit_quality = "PROXY_ONLY"

    if yahoo_status == "OK" and fred_status == "ON":
        overall = "OK"
    elif yahoo_status == "FAILED":
        overall = "LOW"
    else:
        overall = "PARTIAL"

    return {
        "overall": overall,
        "fred_status": fred_status,
        "credit_quality": credit_quality,
        "yahoo_status": yahoo_status,
        "missing_core_fred": missing_core_fred,
        "missing_core_yahoo": missing_core_yahoo,
        "fred_errors": fred_errors[:8],
        "yahoo_errors": yahoo_errors[:8],
    }


def build_penalties(modules: Dict[str, Dict[str, Any]], dq: Dict[str, Any]) -> Tuple[float, List[Dict[str, Any]]]:
    penalties: List[Dict[str, Any]] = []
    trend = finite(modules.get("trend", {}).get("score"), 50) or 50
    breadth = finite(modules.get("breadth", {}).get("score"), 50) or 50
    cross = finite(modules.get("cross_asset", {}).get("score"), 50) or 50
    liquidity = finite(modules.get("liquidity", {}).get("score"), 50) or 50
    dxy = (modules.get("liquidity", {}).get("details") or {}).get("DXY_20D", {}).get("value")
    btc = (modules.get("cross_asset", {}).get("details") or {}).get("BTC_20D")

    if trend >= 80 and breadth < 45:
        penalties.append({"code": "TREND_BREADTH_DIVERGENCE", "points": -10, "reason": "趨勢強勢延伸，但市場廣度不足；大型權值股可能掩蓋內部脆弱。"})
    if trend >= 80 and breadth < 45 and cross < 45:
        penalties.append({"code": "NO_CROSS_ASSET_CONFIRMATION", "points": -5, "reason": "強趨勢未獲跨資產確認；風險資產動能與股指表現背離。"})
    if dq.get("credit_quality") == "PROXY_ONLY":
        penalties.append({"code": "CREDIT_OAS_MISSING", "points": -5, "reason": "HY OAS 與 IG OAS 缺失；Credit 分數僅能視為 ETF proxy。"})
    elif dq.get("credit_quality") == "PARTIAL":
        penalties.append({"code": "CREDIT_OAS_PARTIAL", "points": -3, "reason": "信用利差資料部分缺失；Credit 分數可信度下修。"})
    if finite(dxy) is not None and finite(btc) is not None and float(dxy) > 0 and float(btc) < 0:
        penalties.append({"code": "DXY_UP_BTC_DOWN", "points": -3, "reason": "美元走強且 BTC 走弱；流動性與高 beta 風險偏好未確認。"})
    if liquidity < 35:
        penalties.append({"code": "LIQUIDITY_STRESS", "points": -4, "reason": "流動性分數偏低；資金環境對風險資產不友善。"})

    total = sum(float(p["points"]) for p in penalties)
    return total, penalties


def regime(adjusted_score: float, modules: Dict[str, Dict[str, Any]], dq: Dict[str, Any]) -> str:
    vol = finite(modules.get("volatility", {}).get("score"), 50) or 50
    credit = finite(modules.get("credit", {}).get("score"), 50) or 50
    breadth = finite(modules.get("breadth", {}).get("score"), 50) or 50
    trend = finite(modules.get("trend", {}).get("score"), 50) or 50
    cross = finite(modules.get("cross_asset", {}).get("score"), 50) or 50
    liquidity = finite(modules.get("liquidity", {}).get("score"), 50) or 50

    if credit < 30 or vol < 25 or liquidity < 25:
        return "系統性警戒"
    if trend >= 80 and breadth < 45 and cross < 45:
        return "脆弱 Risk-On / 窄基礎上漲"
    if trend >= 80 and breadth < 45:
        return "強勢但窄基礎"
    if adjusted_score < 40:
        return "偏空防守"
    if adjusted_score < 55:
        return "觀望"
    if adjusted_score < 70:
        return "觀望偏多"
    if adjusted_score < 85:
        return "Risk-on"
    return "強勢過熱"


def reasons(modules: Dict[str, Dict[str, Any]], dq: Dict[str, Any], penalties: List[Dict[str, Any]]) -> List[str]:
    out: List[str] = []
    trend = finite(modules.get("trend", {}).get("score"), 50) or 50
    breadth = finite(modules.get("breadth", {}).get("score"), 50) or 50
    credit = finite(modules.get("credit", {}).get("score"), 50) or 50
    vol = finite(modules.get("volatility", {}).get("score"), 50) or 50
    liquidity = finite(modules.get("liquidity", {}).get("score"), 50) or 50
    cross = finite(modules.get("cross_asset", {}).get("score"), 50) or 50

    if trend >= 80:
        out.append(f"趨勢強勢延伸：{modules.get('trend', {}).get('note', '')}")
    elif trend <= 40:
        out.append(f"趨勢轉弱：{modules.get('trend', {}).get('note', '')}")

    if breadth < 45:
        out.append(f"市場廣度不足：{modules.get('breadth', {}).get('note', '')}")
    elif breadth >= 65:
        out.append(f"市場廣度改善：{modules.get('breadth', {}).get('note', '')}")

    if dq.get("credit_quality") in {"PROXY_ONLY", "PARTIAL"}:
        missing = ", ".join(dq.get("missing_core_fred") or [])
        out.append(f"信用資料不完整：{missing or 'FRED OAS partial'}；Credit={dq.get('credit_quality')}")
    elif credit < 45:
        out.append(f"信用偏弱：{modules.get('credit', {}).get('note', '')}")
    elif credit >= 65:
        out.append(f"信用穩定：{modules.get('credit', {}).get('note', '')}")

    if vol >= 80:
        out.append(f"波動風險低：{modules.get('volatility', {}).get('note', '')}")
    elif vol < 45:
        out.append(f"波動升溫：{modules.get('volatility', {}).get('note', '')}")

    if liquidity < 50:
        out.append(f"流動性偏緊：{modules.get('liquidity', {}).get('note', '')}")

    if cross < 45:
        out.append(f"跨資產未確認：{modules.get('cross_asset', {}).get('note', '')}")

    for p in penalties:
        if p["code"] in {"TREND_BREADTH_DIVERGENCE", "NO_CROSS_ASSET_CONFIRMATION"}:
            out.append(f"Penalty：{p['reason']}")

    if not out:
        out.append("多數指標落在中性區，暫無單邊訊號。")
    return out[:7]


def find_risk_concentration(chaos_meta: Dict[str, Any]) -> List[str]:
    xray = (chaos_meta or {}).get("xray_meta") or {}
    rows = xray.get("mrc_table") if isinstance(xray.get("mrc_table"), list) else []
    out: List[str] = []
    for row in rows[:5]:
        ticker = str(row.get("ticker") or row.get("yf_ticker") or "N/A")
        risk = finite(row.get("risk_pct"))
        weight = finite(row.get("weight_pct"))
        model_weight = finite(row.get("model_weight_pct"))
        if risk is None:
            continue
        if weight is not None:
            gap = risk - weight
            out.append(f"{ticker}: 風險貢獻 {risk:.1f}% / 實際資金 {weight:.1f}% / 差額 {gap:+.1f}pp")
        elif model_weight is not None:
            out.append(f"{ticker}: 風險貢獻 {risk:.1f}% / 模型權重 {model_weight:.1f}%")
        else:
            out.append(f"{ticker}: 風險貢獻 {risk:.1f}%")
    return out[:5]


def find_quant_watch_items(stock_meta: Dict[str, Any]) -> List[str]:
    items: List[Tuple[float, str]] = []
    if not isinstance(stock_meta, dict):
        return []
    for ticker, meta in stock_meta.items():
        if not isinstance(meta, dict):
            continue
        risk = finite(meta.get("risk_score"), 0) or 0
        health = finite(meta.get("quant_health_score"), 0) or 0
        pressure = abs(finite(meta.get("pivot_resistance_distance_pct"), 999) or 999)
        support = abs(finite(meta.get("pivot_support_distance_pct"), 999) or 999)
        status = str(meta.get("pivot_status") or "")
        score = 0.0
        reasons_local = []
        if risk >= 7:
            score += 3
            reasons_local.append(f"風險 {risk:.1f}")
        if health < 4:
            score += 2
            reasons_local.append(f"健康度 {health:.1f}")
        if pressure <= 1.5:
            score += 2
            reasons_local.append(f"接近月壓力 {pressure:.1f}%")
        if support <= 1.5:
            score += 1
            reasons_local.append(f"接近月支撐 {support:.1f}%")
        if "跌破" in status or "壓力" in status:
            score += 1
            reasons_local.append(status)
        if score > 0:
            items.append((score, f"{ticker}: {'；'.join(reasons_local)}"))
    items.sort(key=lambda x: x[0], reverse=True)
    return [x[1] for x in items[:6]]


def build_ai_data_deep_analysis(
    compass: Dict[str, Any],
    stock_meta: Dict[str, Any],
    chaos_meta: Dict[str, Any],
) -> Dict[str, Any]:
    modules = compass.get("modules") or {}
    dq = compass.get("data_quality") or {}
    penalties = compass.get("penalties") or []
    raw = compass.get("raw_score")
    adjusted = compass.get("adjusted_score")
    regime_label = compass.get("regime") or compass.get("label") or "N/A"

    market_structure = [
        f"Raw Score {raw} 與 Adjusted Score {adjusted} 的差距，代表原始加權分數被結構性背離或資料品質懲罰下修。",
        f"目前 Market Compass 標示為「{regime_label}」；此欄位只描述資料結構，不構成操作結論。",
    ]

    score_decomposition = []
    for key, zh in [("trend", "趨勢"), ("breadth", "廣度"), ("credit", "信用"), ("volatility", "波動"), ("liquidity", "流動性"), ("cross_asset", "跨資產")]:
        mod = modules.get(key) or {}
        score_decomposition.append(f"{zh}: {mod.get('score', 'N/A')} / {mod.get('label', 'N/A')}；{mod.get('note', 'N/A')}")

    divergences = []
    trend = finite((modules.get("trend") or {}).get("score"), 50) or 50
    breadth = finite((modules.get("breadth") or {}).get("score"), 50) or 50
    cross = finite((modules.get("cross_asset") or {}).get("score"), 50) or 50
    vol = finite((modules.get("volatility") or {}).get("score"), 50) or 50
    if trend >= 80 and breadth < 45:
        divergences.append("趨勢強但廣度弱：指數層級動能與市場內部擴散不一致，需避免把大型權值股強勢誤讀為全面 risk-on。")
    if trend >= 80 and cross < 45:
        divergences.append("趨勢強但跨資產偏弱：股指動能未被 BTC、黃金、油價或通膨預期等跨資產訊號完整確認。")
    if vol >= 80 and breadth < 45:
        divergences.append("低波穩定但廣度不足：低波不是壞訊號，但在窄基礎上漲環境下可能隱含擁擠交易。")
    if not divergences:
        divergences.append("目前主要模組之間沒有被規則明確標記的重大背離。")

    data_quality_notes = [
        f"FRED={dq.get('fred_status', 'N/A')}；Credit={dq.get('credit_quality', 'N/A')}；Yahoo={dq.get('yahoo_status', 'N/A')}；Overall={dq.get('overall', 'N/A')}。",
    ]
    missing = dq.get("missing_core_fred") or []
    if missing:
        data_quality_notes.append("缺失核心 FRED 欄位: " + ", ".join(missing))
    if penalties:
        data_quality_notes.append("Penalty Reasons: " + " | ".join(str(p.get("reason")) for p in penalties))

    portfolio_exposure = find_risk_concentration(chaos_meta)
    if not portfolio_exposure:
        portfolio_exposure = ["Risk X-Ray mrc_table 尚未提供可整理的風險貢獻資料。"]

    watch_points = find_quant_watch_items(stock_meta)
    if not watch_points:
        watch_points = ["量化因子總覽未偵測到明確的高風險/低健康度/月樞軸異常組合。"]

    return {
        "title": "AI 數據深度分析",
        "subtitle": "只整理數據、背離、資料品質與暴露來源；不輸出買賣、加減碼或目標價。",
        "updated_at": datetime.now(TAIPEI_TZ).isoformat(timespec="seconds"),
        "market_structure_summary": market_structure,
        "score_decomposition": score_decomposition,
        "major_divergences": divergences,
        "data_quality_and_blindspots": data_quality_notes,
        "portfolio_exposure_sources": portfolio_exposure,
        "manual_checkpoints": watch_points,
    }


def build_market_compass(stock_meta: Dict[str, Any], chaos_meta: Dict[str, Any]) -> Dict[str, Any]:
    prices: Dict[str, pd.Series] = {}
    yahoo_errors: List[str] = []
    for sym in YAHOO_SYMBOLS:
        try:
            prices[sym] = fetch_yahoo_chart(sym)
            print(f"OK Yahoo {sym}: {last_value(prices[sym])}")
            time.sleep(0.1)
        except Exception as exc:
            msg = f"{sym}:{exc}"
            yahoo_errors.append(msg)
            print(f"WARN Yahoo {msg}")

    fred: Dict[str, pd.Series] = {}
    fred_errors: List[str] = []
    for key, series_id in FRED_SERIES.items():
        try:
            fred[key] = fetch_fred_series(series_id)
            print(f"OK FRED {key}/{series_id}: {last_value(fred[key])}")
            time.sleep(0.1)
        except Exception as exc:
            msg = f"{key}:{series_id}:{exc}"
            fred_errors.append(msg)
            print(f"WARN FRED {msg}")

    modules = {
        "trend": score_trend(prices),
        "breadth": score_breadth(prices),
        "credit": score_credit(prices, fred),
        "volatility": score_volatility(prices),
        "liquidity": score_liquidity(prices, fred),
        "cross_asset": score_cross_asset(prices, fred),
    }

    weights = {"trend": 0.25, "breadth": 0.20, "credit": 0.20, "volatility": 0.15, "liquidity": 0.15, "cross_asset": 0.05}
    raw_score = raw_score_from_modules(modules, weights)
    dq = data_quality(prices, fred, fred_errors, yahoo_errors)
    penalty_points, penalties = build_penalties(modules, dq)
    adjusted_score = round(clamp(raw_score + penalty_points), 2)

    compass: Dict[str, Any] = {
        "version": "phase3_adjusted_fred_yahoo_v3",
        "updated_at": datetime.now(TAIPEI_TZ).isoformat(timespec="seconds"),
        "date": str(TODAY_TPE),
        "raw_score": raw_score,
        "adjusted_score": adjusted_score,
        "score": adjusted_score,
        "label": label_adjusted(adjusted_score),
        "regime": regime(adjusted_score, modules, dq),
        "modules": modules,
        "weights": weights,
        "penalty_points": penalty_points,
        "penalties": penalties,
        "penalty_reasons": [p["reason"] for p in penalties],
        "reasons": reasons(modules, dq, penalties),
        "data_quality": dq,
        "fred_enabled": bool(FRED_API_KEY),
        "fred_status": dq.get("fred_status"),
        "fred_errors": fred_errors[:8],
        "yahoo_errors": yahoo_errors[:8],
        "data_sources": {
            "market_prices": "Yahoo Chart unofficial endpoint",
            "macro_credit": "FRED API" if FRED_API_KEY else "FRED API not configured",
        },
    }
    compass["ai_data_deep_analysis"] = build_ai_data_deep_analysis(compass, stock_meta, chaos_meta)
    return compass


def main() -> None:
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        raise SystemExit("ERROR: SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY missing")
    client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
    row = client.table(SUPABASE_TABLE).select("macro_meta,stock_meta,chaos_meta").eq("id", PORTFOLIO_ROW_ID).single().execute().data
    if not row:
        raise SystemExit("ERROR: portfolio row not found")
    macro_meta = row.get("macro_meta") if isinstance(row.get("macro_meta"), dict) else {}
    stock_meta = row.get("stock_meta") if isinstance(row.get("stock_meta"), dict) else {}
    chaos_meta = row.get("chaos_meta") if isinstance(row.get("chaos_meta"), dict) else {}

    compass = build_market_compass(stock_meta, chaos_meta)
    macro_meta["market_compass"] = compass
    macro_meta["ai_data_deep_analysis"] = compass.get("ai_data_deep_analysis")
    client.table(SUPABASE_TABLE).update({"macro_meta": macro_meta}).eq("id", PORTFOLIO_ROW_ID).execute()
    print(
        "OK market compass "
        f"raw={compass['raw_score']} adjusted={compass['adjusted_score']} "
        f"regime={compass['regime']} fred_status={compass['fred_status']} "
        f"credit_quality={compass['data_quality']['credit_quality']}"
    )


if __name__ == "__main__":
    main()
