#!/usr/bin/env python3
"""
Fed Labor Pivot Monitor data fetcher.

Purpose:
- Fetch official labor-market time series from FRED.
- Compute deterministic numeric indicators for a ChatGPT scheduled report.
- Write clean JSON files under data/ without touching the existing main.py workflow.

Design choices:
- No hard failure when FRED_API_KEY is missing or an API call fails.
  The script writes DATA_INVALID / DATA_PARTIAL JSON instead, so GitHub Actions can still commit diagnostics.
- Prior two-month NFP revisions are computed from the previous committed snapshot in
  data/fed_labor_pivot_monitor.json. On the first run, revision data will be N/A.
"""

from __future__ import annotations

import json
import os
import statistics
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlencode

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore

try:
    import requests
except Exception as exc:  # pragma: no cover
    print(f"ERROR: requests import failed: {exc}", file=sys.stderr)
    sys.exit(1)

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
OUTPUT_PATH = DATA_DIR / "fed_labor_pivot_monitor.json"
BRIEF_PATH = DATA_DIR / "fed_labor_pivot_monitor_brief.json"
HISTORY_PATH = DATA_DIR / "fed_labor_pivot_monitor_history.jsonl"

FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"
FRED_API_KEY = os.getenv("FRED_API_KEY", "").strip()

REQUEST_TIMEOUT_SECONDS = 25
MAX_HISTORY_LINES = 260

SERIES = {
    "PAYEMS": {
        "name": "Total Nonfarm Payrolls",
        "unit": "thousand persons",
        "frequency": "monthly",
        "required": True,
    },
    "UNRATE": {
        "name": "Unemployment Rate",
        "unit": "percent",
        "frequency": "monthly",
        "required": True,
    },
    "U6RATE": {
        "name": "U-6 Unemployment Rate",
        "unit": "percent",
        "frequency": "monthly",
        "required": False,
    },
    "SAHMREALTIME": {
        "name": "Real-time Sahm Rule Recession Indicator",
        "unit": "percentage points",
        "frequency": "monthly",
        "required": True,
    },
    "ICSA": {
        "name": "Initial Claims",
        "unit": "persons",
        "frequency": "weekly",
        "required": True,
    },
    "CC4WSA": {
        "name": "Continuing Claims 4-week Moving Average",
        "unit": "persons",
        "frequency": "weekly",
        "required": False,
    },
    "CCSA": {
        "name": "Continued Claims",
        "unit": "persons",
        "frequency": "weekly",
        "required": False,
    },
    "JTSJOL": {
        "name": "JOLTS Job Openings: Total Nonfarm",
        "unit": "thousand persons",
        "frequency": "monthly",
        "required": True,
    },
    "JTSQUR": {
        "name": "JOLTS Quits Rate: Total Nonfarm",
        "unit": "percent",
        "frequency": "monthly",
        "required": False,
    },
    "JTSLDL": {
        "name": "JOLTS Layoffs and Discharges: Total Nonfarm",
        "unit": "thousand persons",
        "frequency": "monthly",
        "required": False,
    },
    "DGS2": {
        "name": "Market Yield on U.S. Treasury Securities at 2-Year Constant Maturity",
        "unit": "percent",
        "frequency": "daily",
        "required": False,
    },
    "TEMPHELPS": {
        "name": "All Employees, Temporary Help Services",
        "unit": "thousand persons",
        "frequency": "monthly",
        "required": False,
    },
    "AWHAETP": {
        "name": "Average Weekly Hours of All Employees, Total Private",
        "unit": "hours",
        "frequency": "monthly",
        "required": False,
    },
    "LNS13023621": {
        "name": "Unemployed Job Losers",
        "unit": "thousand persons",
        "frequency": "monthly",
        "required": False,
    },
    "PCEPI": {
        "name": "Personal Consumption Expenditures Price Index",
        "unit": "index 2017=100",
        "frequency": "monthly",
        "required": False,
    },
    "PCEPILFE": {
        "name": "Core PCE Price Index Less Food and Energy",
        "unit": "index 2017=100",
        "frequency": "monthly",
        "required": False,
    },
    "CPIAUCSL": {
        "name": "Consumer Price Index for All Urban Consumers: All Items",
        "unit": "index 1982-1984=100",
        "frequency": "monthly",
        "required": False,
    },
    "CPILFESL": {
        "name": "Core CPI Less Food and Energy",
        "unit": "index 1982-1984=100",
        "frequency": "monthly",
        "required": False,
    },
}


@dataclass
class FredObservation:
    date: str
    value: float


def now_iso_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def today_taipei() -> str:
    if ZoneInfo is None:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return datetime.now(ZoneInfo("Asia/Taipei")).strftime("%Y-%m-%d")


def ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def safe_float(raw: Any) -> Optional[float]:
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return float(raw)
    text = str(raw).strip()
    if text in {"", ".", "NaN", "nan", "None"}:
        return None
    try:
        return float(text.replace(",", ""))
    except ValueError:
        return None


def round_or_none(value: Optional[float], digits: int = 3) -> Optional[float]:
    if value is None:
        return None
    return round(float(value), digits)


def status_rank(status: str) -> int:
    order = {
        "green": 0,
        "yellow": 1,
        "orange": 2,
        "red": 3,
        "data_unavailable": -1,
        "not_applicable": -1,
    }
    return order.get(status, -1)


def fred_url(series_id: str, limit: int) -> str:
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "sort_order": "desc",
        "limit": str(limit),
    }
    return f"{FRED_BASE_URL}?{urlencode(params)}"


def fetch_fred_series(series_id: str, limit: int = 120) -> Tuple[List[FredObservation], Dict[str, Any]]:
    meta = SERIES.get(series_id, {})
    health: Dict[str, Any] = {
        "series_id": series_id,
        "name": meta.get("name"),
        "required": bool(meta.get("required")),
        "ok": False,
        "observation_count": 0,
        "latest_date": None,
        "error": None,
        "source": "FRED API",
        "url": f"https://fred.stlouisfed.org/series/{series_id}",
    }

    if not FRED_API_KEY:
        health["error"] = "Missing FRED_API_KEY. Add it as a GitHub Actions secret."
        return [], health

    try:
        response = requests.get(fred_url(series_id, limit), timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
        payload = response.json()
        observations_raw = payload.get("observations", [])
        observations: List[FredObservation] = []
        for item in observations_raw:
            value = safe_float(item.get("value"))
            date = str(item.get("date", "")).strip()
            if value is None or not date:
                continue
            observations.append(FredObservation(date=date, value=value))

        # FRED returned desc because sort_order=desc. Preserve latest-first order.
        health["ok"] = bool(observations)
        health["observation_count"] = len(observations)
        health["latest_date"] = observations[0].date if observations else None
        if not observations:
            health["error"] = "No valid numeric observations returned."
        return observations, health
    except Exception as exc:
        health["error"] = f"{type(exc).__name__}: {exc}"
        return [], health


def read_previous_payload() -> Dict[str, Any]:
    if not OUTPUT_PATH.exists():
        return {}
    try:
        return json.loads(OUTPUT_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def latest(obs: List[FredObservation]) -> Optional[FredObservation]:
    return obs[0] if obs else None


def changes_from_level_series(obs: List[FredObservation], count: int) -> List[Dict[str, Any]]:
    """Return latest-first month-to-month level changes for a level series."""
    if len(obs) < count + 1:
        return []
    changes: List[Dict[str, Any]] = []
    for i in range(count):
        current = obs[i]
        previous = obs[i + 1]
        changes.append(
            {
                "period": current.date,
                "current_level": current.value,
                "previous_level": previous.value,
                "change": current.value - previous.value,
            }
        )
    return changes


def classify_nfp(value: Optional[float]) -> str:
    if value is None:
        return "data_unavailable"
    if value < 50:
        return "red"
    if value < 75:
        return "yellow"
    return "green"


def classify_nfp_revision(downward_revision_abs: Optional[float]) -> str:
    if downward_revision_abs is None:
        return "data_unavailable"
    if downward_revision_abs > 100:
        return "red"
    if downward_revision_abs > 75:
        return "yellow"
    return "green"


def classify_unrate_mom(change_pp: Optional[float]) -> str:
    if change_pp is None:
        return "data_unavailable"
    return "yellow" if change_pp >= 0.2 else "green"


def classify_sahm(value: Optional[float]) -> str:
    if value is None:
        return "data_unavailable"
    if value >= 0.50:
        return "red"
    if value >= 0.40:
        return "orange"
    if value >= 0.30:
        return "yellow"
    return "green"


def classify_initial_claims(value: Optional[float]) -> str:
    if value is None:
        return "data_unavailable"
    if value > 300_000:
        return "red"
    if value > 275_000:
        return "orange"
    if value > 250_000:
        return "yellow"
    return "green"


def classify_bool_signal(triggered: Optional[bool]) -> str:
    if triggered is None:
        return "data_unavailable"
    return "yellow" if triggered else "green"


def moving_average(values: Iterable[float]) -> Optional[float]:
    clean = [float(v) for v in values if v is not None]
    if not clean:
        return None
    return statistics.fmean(clean)


def latest_n_values(obs: List[FredObservation], n: int) -> List[float]:
    return [item.value for item in obs[:n]]


def consecutive_decreases(obs: List[FredObservation], periods: int = 3) -> Optional[bool]:
    """True if the latest N monthly moves are all declines. Needs periods + 1 observations."""
    if len(obs) < periods + 1:
        return None
    for i in range(periods):
        if not (obs[i].value < obs[i + 1].value):
            return False
    return True


def consecutive_increases(obs: List[FredObservation], periods: int = 3) -> Optional[bool]:
    if len(obs) < periods + 1:
        return None
    for i in range(periods):
        if not (obs[i].value > obs[i + 1].value):
            return False
    return True


def cycle_high(obs: List[FredObservation], lookback: int = 52) -> Optional[bool]:
    if len(obs) < 2:
        return None
    sample = obs[: min(lookback, len(obs))]
    return obs[0].value >= max(item.value for item in sample)


def level_change(obs: List[FredObservation], periods: int) -> Optional[float]:
    """Latest minus observation N periods ago, where obs is latest-first."""
    if len(obs) <= periods:
        return None
    return obs[0].value - obs[periods].value


def pct_change_over_period(obs: List[FredObservation], periods: int) -> Optional[float]:
    if len(obs) <= periods or obs[periods].value == 0:
        return None
    return (obs[0].value / obs[periods].value - 1.0) * 100.0


def annualized_index_change_pct(obs: List[FredObservation], periods: int) -> Optional[float]:
    """Annualized percent change for monthly price-index series, latest-first."""
    if len(obs) <= periods or obs[periods].value <= 0 or obs[0].value <= 0:
        return None
    return ((obs[0].value / obs[periods].value) ** (12.0 / periods) - 1.0) * 100.0


def classify_secondary_bool(triggered: Optional[bool]) -> str:
    if triggered is None:
        return "data_unavailable"
    return "yellow" if triggered else "green"


def classify_u6(value_mom_pp: Optional[float], value_3m_pp: Optional[float]) -> str:
    if value_mom_pp is None and value_3m_pp is None:
        return "data_unavailable"
    if value_mom_pp is not None and value_mom_pp >= 0.2:
        return "yellow"
    if value_3m_pp is not None and value_3m_pp >= 0.3:
        return "yellow"
    return "green"


def classify_avg_weekly_hours(change_3m_hours: Optional[float]) -> str:
    if change_3m_hours is None:
        return "data_unavailable"
    return "yellow" if change_3m_hours <= -0.1 else "green"


def classify_temp_help(consecutive_declines: Optional[bool], pct_3m: Optional[float]) -> str:
    if consecutive_declines is None and pct_3m is None:
        return "data_unavailable"
    if consecutive_declines is True:
        return "yellow"
    if pct_3m is not None and pct_3m <= -1.0:
        return "yellow"
    return "green"


def classify_job_losers(consecutive_incs: Optional[bool], pct_3m: Optional[float]) -> str:
    if consecutive_incs is None and pct_3m is None:
        return "data_unavailable"
    if consecutive_incs is True:
        return "yellow"
    if pct_3m is not None and pct_3m >= 5.0:
        return "yellow"
    return "green"


def classify_inflation_pressure(yoy_pct: Optional[float], three_m_ann_pct: Optional[float], is_core: bool = False) -> str:
    """
    Classify inflation as a constraint on a labor-driven Fed pivot.
    These are heuristic gates, not independent trading signals.
    """
    if yoy_pct is None and three_m_ann_pct is None:
        return "data_unavailable"
    high_level = 3.0 if is_core else 3.5
    medium_level = 2.5 if is_core else 3.0
    values = [v for v in [yoy_pct, three_m_ann_pct] if v is not None]
    if any(v >= high_level for v in values):
        return "high"
    if any(v >= medium_level for v in values):
        return "medium"
    return "low"


def build_inflation_metric(obs: List[FredObservation], series_id: str, is_core: bool) -> Dict[str, Any]:
    latest_obs = latest(obs)
    yoy = pct_change_over_period(obs, 12)
    three_m_ann = annualized_index_change_pct(obs, 3)
    one_m_ann = annualized_index_change_pct(obs, 1)
    return {
        "latest_period": latest_obs.date if latest_obs else None,
        "index_value": round_or_none(latest_obs.value if latest_obs else None, 3),
        "yoy_percent": round_or_none(yoy, 2),
        "three_month_annualized_percent": round_or_none(three_m_ann, 2),
        "one_month_annualized_percent": round_or_none(one_m_ann, 2),
        "status": classify_inflation_pressure(yoy, three_m_ann, is_core=is_core),
        "threshold": "core: medium >=2.5%, high >=3.0%; headline: medium >=3.0%, high >=3.5%",
        "source_series": series_id,
        "role": "inflation constraint on Fed labor pivot, not a standalone inflation monitor",
    }


def combine_inflation_gate(core_pce_status: str, core_cpi_status: str) -> Dict[str, Any]:
    statuses = [s for s in [core_pce_status, core_cpi_status] if s != "data_unavailable"]
    if not statuses:
        return {
            "status": "data_unavailable",
            "label_zh": "不判斷",
            "interpretation_zh": "Core PCE 與 Core CPI 資料不足，無法判斷通膨是否壓住 Fed 轉向空間。",
        }
    if "high" in statuses:
        return {
            "status": "high",
            "label_zh": "高度限制",
            "interpretation_zh": "核心通膨仍偏熱或重新加速；即使勞動轉弱，Fed 轉向空間也被壓住。",
        }
    if "medium" in statuses:
        return {
            "status": "medium",
            "label_zh": "中度限制",
            "interpretation_zh": "核心通膨沒有完全放行 Fed pivot；勞動惡化較可能先被解讀為風險平衡調整，而非立即轉向。",
        }
    return {
        "status": "low",
        "label_zh": "限制較低",
        "interpretation_zh": "核心通膨未明顯重新加速；若勞動數據惡化，Fed 較有空間承認就業下行風險。",
    }


def compute_payems_revision(payems_obs: List[FredObservation], previous_payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Compute prior two-month NFP revisions from prior committed PAYEMS snapshot.

    FRED current observations are revised levels. To infer revisions, compare the current
    PAYEMS levels for the two months before the latest month against the levels captured
    in the previous run's snapshot.
    """
    current_snapshot = {item.date: item.value for item in payems_obs[:8]}
    previous_snapshot = (
        previous_payload.get("raw_snapshots", {})
        .get("PAYEMS", {})
        .get("latest_observations", {})
    )

    result: Dict[str, Any] = {
        "method": "Compare current PAYEMS levels against previous committed snapshot for the two months before latest release.",
        "status": "data_unavailable",
        "latest_payems_period": payems_obs[0].date if payems_obs else None,
        "target_periods": [],
        "net_revision_thousand": None,
        "downward_revision_abs_thousand": None,
        "details": [],
        "note": "First run or missing previous PAYEMS snapshot; revision metric will become available after the next successful run.",
    }

    if len(payems_obs) < 3 or not previous_snapshot:
        return result

    target_obs = payems_obs[1:3]
    details: List[Dict[str, Any]] = []
    revisions: List[float] = []
    for item in target_obs:
        prev_value = safe_float(previous_snapshot.get(item.date))
        if prev_value is None:
            continue
        revision = item.value - prev_value
        revisions.append(revision)
        details.append(
            {
                "period": item.date,
                "previous_snapshot_level_thousand": prev_value,
                "current_level_thousand": item.value,
                "revision_thousand": revision,
            }
        )

    if len(revisions) != 2:
        result["details"] = details
        result["target_periods"] = [item.date for item in target_obs]
        return result

    net_revision = sum(revisions)
    downward_abs = abs(net_revision) if net_revision < 0 else 0.0
    result.update(
        {
            "status": classify_nfp_revision(downward_abs),
            "target_periods": [item.date for item in target_obs],
            "net_revision_thousand": round_or_none(net_revision, 1),
            "downward_revision_abs_thousand": round_or_none(downward_abs, 1),
            "details": details,
            "note": None,
        }
    )
    return result


def derive_numeric_payload(series_data: Dict[str, List[FredObservation]], series_health: Dict[str, Any]) -> Dict[str, Any]:
    previous_payload = read_previous_payload()

    payems = series_data.get("PAYEMS", [])
    unrate = series_data.get("UNRATE", [])
    sahm = series_data.get("SAHMREALTIME", [])
    icsa = series_data.get("ICSA", [])
    cc4wsa = series_data.get("CC4WSA", [])
    ccsa = series_data.get("CCSA", [])
    continuing = cc4wsa if cc4wsa else ccsa
    continuing_series_id = "CC4WSA" if cc4wsa else ("CCSA" if ccsa else None)
    jolts_openings = series_data.get("JTSJOL", [])
    jolts_quits_rate = series_data.get("JTSQUR", [])
    jolts_layoffs = series_data.get("JTSLDL", [])
    dgs2 = series_data.get("DGS2", [])
    u6rate = series_data.get("U6RATE", [])
    temp_help = series_data.get("TEMPHELPS", [])
    avg_weekly_hours = series_data.get("AWHAETP", [])
    job_losers = series_data.get("LNS13023621", [])
    headline_pce = series_data.get("PCEPI", [])
    core_pce = series_data.get("PCEPILFE", [])
    headline_cpi = series_data.get("CPIAUCSL", [])
    core_cpi = series_data.get("CPILFESL", [])

    nfp_changes = changes_from_level_series(payems, 3)
    nfp_headline = nfp_changes[0]["change"] if nfp_changes else None
    nfp_3m_avg = moving_average([item["change"] for item in nfp_changes[:3]]) if len(nfp_changes) >= 3 else None
    nfp_revision = compute_payems_revision(payems, previous_payload)

    unrate_latest = latest(unrate)
    unrate_mom = None
    if len(unrate) >= 2:
        unrate_mom = unrate[0].value - unrate[1].value

    sahm_latest = latest(sahm)

    initial_claims_4w_avg = None
    if len(icsa) >= 4:
        initial_claims_4w_avg = moving_average(latest_n_values(icsa, 4))

    continuing_latest = latest(continuing)
    continuing_consecutive_increases = consecutive_increases(continuing, 3) if continuing else None
    continuing_cycle_high = cycle_high(continuing, 52) if continuing else None
    if continuing_latest is None:
        continuing_status = "data_unavailable"
    elif continuing_cycle_high:
        continuing_status = "orange"
    elif continuing_consecutive_increases:
        continuing_status = "yellow"
    else:
        continuing_status = "green"

    jolts_declines_3m = consecutive_decreases(jolts_openings, 3)
    quits_declines_3m = consecutive_decreases(jolts_quits_rate, 3)
    layoffs_increases_3m = consecutive_increases(jolts_layoffs, 3)

    dgs2_latest = latest(dgs2)
    dgs2_change_4w = None
    if len(dgs2) >= 20:
        dgs2_change_4w = dgs2[0].value - dgs2[19].value

    u6_latest = latest(u6rate)
    u6_mom = level_change(u6rate, 1)
    u6_3m_change = level_change(u6rate, 3)

    temp_help_latest = latest(temp_help)
    temp_help_declines_3m = consecutive_decreases(temp_help, 3)
    temp_help_3m_pct = pct_change_over_period(temp_help, 3)

    avg_hours_latest = latest(avg_weekly_hours)
    avg_hours_3m_change = level_change(avg_weekly_hours, 3)

    job_losers_latest = latest(job_losers)
    job_losers_increases_3m = consecutive_increases(job_losers, 3)
    job_losers_3m_pct = pct_change_over_period(job_losers, 3)

    headline_pce_metric = build_inflation_metric(headline_pce, "PCEPI", is_core=False)
    core_pce_metric = build_inflation_metric(core_pce, "PCEPILFE", is_core=True)
    headline_cpi_metric = build_inflation_metric(headline_cpi, "CPIAUCSL", is_core=False)
    core_cpi_metric = build_inflation_metric(core_cpi, "CPILFESL", is_core=True)
    inflation_gate = combine_inflation_gate(core_pce_metric["status"], core_cpi_metric["status"])

    metrics: Dict[str, Any] = {
        "nfp_headline": {
            "latest_period": payems[0].date if payems else None,
            "value_thousand": round_or_none(nfp_headline, 1),
            "status": classify_nfp(nfp_headline),
            "threshold": "yellow <75k; red <50k",
            "source_series": "PAYEMS",
            "calculation": "latest PAYEMS level minus previous monthly PAYEMS level",
        },
        "nfp_prior_two_month_revision": nfp_revision,
        "nfp_3m_avg": {
            "latest_period": payems[0].date if payems else None,
            "value_thousand": round_or_none(nfp_3m_avg, 1),
            "status": classify_nfp(nfp_3m_avg),
            "threshold": "yellow <75k; red <50k",
            "source_series": "PAYEMS",
            "components": nfp_changes[:3],
        },
        "unemployment_rate_u3": {
            "latest_period": unrate_latest.date if unrate_latest else None,
            "value_percent": round_or_none(unrate_latest.value if unrate_latest else None, 2),
            "mom_change_pp": round_or_none(unrate_mom, 2),
            "status": classify_unrate_mom(unrate_mom),
            "threshold": "month-over-month increase >=0.2 percentage points",
            "source_series": "UNRATE",
        },
        "sahm_rule_realtime": {
            "latest_period": sahm_latest.date if sahm_latest else None,
            "value_pp": round_or_none(sahm_latest.value if sahm_latest else None, 3),
            "status": classify_sahm(sahm_latest.value if sahm_latest else None),
            "threshold": "yellow >=0.30; orange >=0.40; red >=0.50",
            "source_series": "SAHMREALTIME",
        },
        "initial_claims_4w_avg": {
            "latest_period": icsa[0].date if icsa else None,
            "value_persons": round_or_none(initial_claims_4w_avg, 0),
            "status": classify_initial_claims(initial_claims_4w_avg),
            "threshold": "yellow >250k; orange >275k; red >300k",
            "source_series": "ICSA",
            "components": [{"date": item.date, "value_persons": item.value} for item in icsa[:4]],
        },
        "continuing_claims": {
            "latest_period": continuing_latest.date if continuing_latest else None,
            "value_persons": round_or_none(continuing_latest.value if continuing_latest else None, 0),
            "status": continuing_status,
            "threshold": "consecutive increases or 52-week cycle high",
            "source_series": continuing_series_id,
            "consecutive_3w_increases": continuing_consecutive_increases,
            "cycle_high_52w": continuing_cycle_high,
        },
        "jolts_openings": {
            "latest_period": jolts_openings[0].date if jolts_openings else None,
            "value_thousand": round_or_none(jolts_openings[0].value if jolts_openings else None, 1),
            "status": classify_bool_signal(jolts_declines_3m),
            "threshold": "3 consecutive monthly declines",
            "source_series": "JTSJOL",
            "consecutive_3m_declines": jolts_declines_3m,
            "latest_4_observations": [{"date": item.date, "value_thousand": item.value} for item in jolts_openings[:4]],
        },
        "jolts_quits_rate": {
            "latest_period": jolts_quits_rate[0].date if jolts_quits_rate else None,
            "value_percent": round_or_none(jolts_quits_rate[0].value if jolts_quits_rate else None, 2),
            "status": classify_bool_signal(quits_declines_3m),
            "threshold": "sustained decline proxy: 3 consecutive monthly declines",
            "source_series": "JTSQUR",
            "consecutive_3m_declines": quits_declines_3m,
        },
        "jolts_layoffs": {
            "latest_period": jolts_layoffs[0].date if jolts_layoffs else None,
            "value_thousand": round_or_none(jolts_layoffs[0].value if jolts_layoffs else None, 1),
            "status": classify_bool_signal(layoffs_increases_3m),
            "threshold": "sustained increase proxy: 3 consecutive monthly increases",
            "source_series": "JTSLDL",
            "consecutive_3m_increases": layoffs_increases_3m,
        },
        "u6_unemployment_rate": {
            "latest_period": u6_latest.date if u6_latest else None,
            "value_percent": round_or_none(u6_latest.value if u6_latest else None, 2),
            "mom_change_pp": round_or_none(u6_mom, 2),
            "three_month_change_pp": round_or_none(u6_3m_change, 2),
            "status": classify_u6(u6_mom, u6_3m_change),
            "threshold": "secondary confirmation: MoM >=0.2pp or 3M change >=0.3pp",
            "source_series": "U6RATE",
            "role": "secondary labor-quality confirmation; cannot independently trigger RED or PIVOT CONFIRMED",
        },
        "average_weekly_hours_total_private": {
            "latest_period": avg_hours_latest.date if avg_hours_latest else None,
            "value_hours": round_or_none(avg_hours_latest.value if avg_hours_latest else None, 2),
            "three_month_change_hours": round_or_none(avg_hours_3m_change, 2),
            "status": classify_avg_weekly_hours(avg_hours_3m_change),
            "threshold": "secondary confirmation: 3M change <= -0.1 hours",
            "source_series": "AWHAETP",
            "role": "secondary labor-quality confirmation; cannot independently trigger RED or PIVOT CONFIRMED",
        },
        "temporary_help_services": {
            "latest_period": temp_help_latest.date if temp_help_latest else None,
            "value_thousand": round_or_none(temp_help_latest.value if temp_help_latest else None, 1),
            "three_month_percent_change": round_or_none(temp_help_3m_pct, 2),
            "consecutive_3m_declines": temp_help_declines_3m,
            "status": classify_temp_help(temp_help_declines_3m, temp_help_3m_pct),
            "threshold": "secondary confirmation: 3 consecutive monthly declines or 3M change <= -1%",
            "source_series": "TEMPHELPS",
            "role": "secondary labor-quality confirmation; cannot independently trigger RED or PIVOT CONFIRMED",
        },
        "job_losers": {
            "latest_period": job_losers_latest.date if job_losers_latest else None,
            "value_thousand": round_or_none(job_losers_latest.value if job_losers_latest else None, 1),
            "three_month_percent_change": round_or_none(job_losers_3m_pct, 2),
            "consecutive_3m_increases": job_losers_increases_3m,
            "status": classify_job_losers(job_losers_increases_3m, job_losers_3m_pct),
            "threshold": "secondary confirmation: 3 consecutive monthly increases or 3M change >=5%",
            "source_series": "LNS13023621",
            "role": "secondary labor-quality confirmation; cannot independently trigger RED or PIVOT CONFIRMED",
        },
        "headline_pce": headline_pce_metric,
        "core_pce": core_pce_metric,
        "headline_cpi": headline_cpi_metric,
        "core_cpi": core_cpi_metric,
        "inflation_constraint_on_fed_pivot": inflation_gate,
        "two_year_treasury_yield": {
            "latest_period": dgs2_latest.date if dgs2_latest else None,
            "value_percent": round_or_none(dgs2_latest.value if dgs2_latest else None, 3),
            "change_approx_4w_pp": round_or_none(dgs2_change_4w, 3),
            "status": "not_applicable",
            "threshold": "market confirmation only; not used for labor-regime classification",
            "source_series": "DGS2",
        },
    }

    secondary_statuses = [
        metrics["u6_unemployment_rate"]["status"],
        metrics["average_weekly_hours_total_private"]["status"],
        metrics["temporary_help_services"]["status"],
        metrics["job_losers"]["status"],
    ]
    secondary_adverse_count = sum(1 for s in secondary_statuses if status_rank(s) >= 1)
    if secondary_adverse_count >= 3:
        secondary_strength = "high"
    elif secondary_adverse_count >= 2:
        secondary_strength = "medium"
    elif secondary_adverse_count >= 1:
        secondary_strength = "low"
    else:
        secondary_strength = "none"
    metrics["labor_quality_secondary_confirmation"] = {
        "status": secondary_strength,
        "adverse_indicator_count": secondary_adverse_count,
        "total_available_secondary_indicators": sum(1 for s in secondary_statuses if s != "data_unavailable"),
        "indicators": {
            "u6_unemployment_rate": metrics["u6_unemployment_rate"]["status"],
            "average_weekly_hours_total_private": metrics["average_weekly_hours_total_private"]["status"],
            "temporary_help_services": metrics["temporary_help_services"]["status"],
            "job_losers": metrics["job_losers"]["status"],
        },
        "rule": "Secondary indicators may raise confidence within YELLOW/ORANGE/RED, but must not independently trigger RED or PIVOT CONFIRMED.",
    }

    decisive_statuses = [
        metrics["nfp_headline"]["status"],
        metrics["nfp_prior_two_month_revision"]["status"],
        metrics["nfp_3m_avg"]["status"],
        metrics["unemployment_rate_u3"]["status"],
        metrics["sahm_rule_realtime"]["status"],
        metrics["initial_claims_4w_avg"]["status"],
        metrics["continuing_claims"]["status"],
        metrics["jolts_openings"]["status"],
    ]
    max_rank = max(status_rank(s) for s in decisive_statuses)
    if max_rank >= 3:
        numeric_regime = "RED"
    elif max_rank == 2:
        numeric_regime = "ORANGE"
    elif max_rank == 1:
        numeric_regime = "YELLOW"
    else:
        numeric_regime = "GREEN"

    required_series = [sid for sid, meta in SERIES.items() if meta.get("required")]
    missing_required = [sid for sid in required_series if not series_health.get(sid, {}).get("ok")]
    if missing_required:
        data_status = "DATA_INVALID" if len(missing_required) >= len(required_series) else "DATA_PARTIAL"
    else:
        data_status = "DATA_VALID"

    if data_status != "DATA_VALID":
        numeric_regime = data_status

    latest_observation_dates = {
        sid: series_health.get(sid, {}).get("latest_date") for sid in SERIES.keys()
    }

    raw_snapshots = {
        sid: {
            "latest_observations": {item.date: item.value for item in obs[:12]},
            "latest_first": [{"date": item.date, "value": item.value} for item in obs[:12]],
        }
        for sid, obs in series_data.items()
        if obs
    }

    warnings: List[str] = []
    if not FRED_API_KEY:
        warnings.append("FRED_API_KEY is missing; add it as a GitHub Actions secret for valid data.")
    if missing_required:
        warnings.append(f"Missing required FRED series: {', '.join(missing_required)}")
    if metrics["nfp_prior_two_month_revision"].get("status") == "data_unavailable":
        warnings.append("NFP prior two-month revision is unavailable until at least one prior PAYEMS snapshot exists.")

    return {
        "schema_version": "1.1.0",
        "generated_at_utc": now_iso_utc(),
        "generated_date_taipei": today_taipei(),
        "data_status": data_status,
        "numeric_regime_preliminary": numeric_regime,
        "important_note": "This JSON provides deterministic numeric inputs. Core labor indicators drive the preliminary regime; secondary labor-quality and inflation-gate indicators provide confirmation/constraints. Final Fed pivot classification still requires official Federal Reserve communication review.",
        "series_health": series_health,
        "latest_observation_dates": latest_observation_dates,
        "missing_required_series": missing_required,
        "warnings": warnings,
        "metrics": metrics,
        "raw_snapshots": raw_snapshots,
    }


def build_brief(payload: Dict[str, Any]) -> Dict[str, Any]:
    metrics = payload.get("metrics", {})
    return {
        "schema_version": payload.get("schema_version"),
        "generated_at_utc": payload.get("generated_at_utc"),
        "generated_date_taipei": payload.get("generated_date_taipei"),
        "data_status": payload.get("data_status"),
        "numeric_regime_preliminary": payload.get("numeric_regime_preliminary"),
        "latest_observation_dates": payload.get("latest_observation_dates"),
        "warnings": payload.get("warnings", []),
        "metrics": {
            "nfp_headline": metrics.get("nfp_headline"),
            "nfp_prior_two_month_revision": metrics.get("nfp_prior_two_month_revision"),
            "nfp_3m_avg": metrics.get("nfp_3m_avg"),
            "unemployment_rate_u3": metrics.get("unemployment_rate_u3"),
            "sahm_rule_realtime": metrics.get("sahm_rule_realtime"),
            "initial_claims_4w_avg": metrics.get("initial_claims_4w_avg"),
            "continuing_claims": metrics.get("continuing_claims"),
            "jolts_openings": metrics.get("jolts_openings"),
            "jolts_quits_rate": metrics.get("jolts_quits_rate"),
            "jolts_layoffs": metrics.get("jolts_layoffs"),
            "u6_unemployment_rate": metrics.get("u6_unemployment_rate"),
            "average_weekly_hours_total_private": metrics.get("average_weekly_hours_total_private"),
            "temporary_help_services": metrics.get("temporary_help_services"),
            "job_losers": metrics.get("job_losers"),
            "labor_quality_secondary_confirmation": metrics.get("labor_quality_secondary_confirmation"),
            "headline_pce": metrics.get("headline_pce"),
            "core_pce": metrics.get("core_pce"),
            "headline_cpi": metrics.get("headline_cpi"),
            "core_cpi": metrics.get("core_cpi"),
            "inflation_constraint_on_fed_pivot": metrics.get("inflation_constraint_on_fed_pivot"),
            "two_year_treasury_yield": metrics.get("two_year_treasury_yield"),
        },
    }


def atomic_write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temp_path.replace(path)


def append_history(payload: Dict[str, Any]) -> None:
    HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
    entry = {
        "generated_at_utc": payload.get("generated_at_utc"),
        "generated_date_taipei": payload.get("generated_date_taipei"),
        "data_status": payload.get("data_status"),
        "numeric_regime_preliminary": payload.get("numeric_regime_preliminary"),
        "latest_observation_dates": payload.get("latest_observation_dates"),
        "metrics": build_brief(payload).get("metrics"),
    }

    lines: List[str] = []
    if HISTORY_PATH.exists():
        lines = HISTORY_PATH.read_text(encoding="utf-8").splitlines()
    lines.append(json.dumps(entry, ensure_ascii=False, sort_keys=True))
    if len(lines) > MAX_HISTORY_LINES:
        lines = lines[-MAX_HISTORY_LINES:]
    HISTORY_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    ensure_data_dir()
    series_data: Dict[str, List[FredObservation]] = {}
    series_health: Dict[str, Any] = {}

    for series_id in SERIES.keys():
        limit = 260 if SERIES[series_id]["frequency"] in {"daily", "weekly"} else 120
        observations, health = fetch_fred_series(series_id, limit=limit)
        series_data[series_id] = observations
        series_health[series_id] = health
        status = "OK" if health.get("ok") else "WARN"
        print(f"[{status}] {series_id}: latest={health.get('latest_date')} count={health.get('observation_count')} error={health.get('error')}")

    payload = derive_numeric_payload(series_data, series_health)
    brief = build_brief(payload)

    atomic_write_json(OUTPUT_PATH, payload)
    atomic_write_json(BRIEF_PATH, brief)
    append_history(payload)

    print(f"Wrote {OUTPUT_PATH.relative_to(ROOT)}")
    print(f"Wrote {BRIEF_PATH.relative_to(ROOT)}")
    print(f"Updated {HISTORY_PATH.relative_to(ROOT)}")
    print(f"Data status: {payload.get('data_status')}")
    print(f"Preliminary numeric regime: {payload.get('numeric_regime_preliminary')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
