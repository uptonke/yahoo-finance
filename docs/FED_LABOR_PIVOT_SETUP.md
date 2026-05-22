# Fed Labor Pivot Data Fetch Setup v2

This patch adds or updates an independent Federal Reserve labor-pivot data pipeline inside the existing `uptonke/yahoo-finance` repository.

It does not modify `main.py`.

## Files added / updated

```text
fed_labor_pivot.py
requirements-fed-labor.txt
.github/workflows/fed-labor-pivot.yml
docs/FED_LABOR_PIVOT_SETUP.md
docs/FED_LABOR_PIVOT_PROMPT_V2.md
```

The script writes:

```text
data/fed_labor_pivot_monitor.json
data/fed_labor_pivot_monitor_brief.json
data/fed_labor_pivot_monitor_history.jsonl
```

## Required GitHub Secret

Add or keep this repository secret:

```text
FRED_API_KEY
```

The script will not crash if the key is missing, but the output will be marked `DATA_INVALID`.

## Series fetched

Core labor indicators:

```text
PAYEMS
UNRATE
SAHMREALTIME
ICSA
CC4WSA
CCSA
JTSJOL
JTSQUR
JTSLDL
DGS2
```

Secondary labor-quality confirmation indicators:

```text
U6RATE
TEMPHELPS
AWHAETP
LNS13023621
```

Inflation-gate indicators:

```text
PCEPI
PCEPILFE
CPIAUCSL
CPILFESL
```

## Logic notes

- `numeric_regime_preliminary` is driven by core labor indicators only.
- U-6, temporary help, average weekly hours, and job losers are secondary confirmation indicators.
- Secondary indicators may raise confidence inside a YELLOW / ORANGE / RED assessment, but they must not independently trigger RED or PIVOT CONFIRMED.
- PCE / CPI indicators are an inflation constraint on Fed pivot space, not a separate inflation monitor.
- PIVOT CONFIRMED still requires official Fed communication review by the ChatGPT scheduled report.

## cron-job.org trigger

Use cron-job.org to trigger the GitHub workflow via `workflow_dispatch`.

Method:

```text
POST
```

URL:

```text
https://api.github.com/repos/uptonke/yahoo-finance/actions/workflows/fed-labor-pivot.yml/dispatches
```

Headers:

```text
Accept: application/vnd.github+json
Authorization: Bearer YOUR_GITHUB_TOKEN
X-GitHub-Api-Version: 2022-11-28
Content-Type: application/json
```

Body:

```json
{
  "ref": "main"
}
```

Recommended timing:

```text
Every Saturday 09:00 Asia/Taipei
```

This gives the 10:00 ChatGPT scheduled report a one-hour buffer.

## ChatGPT scheduled-report JSON URL

Use this as the primary numeric input:

```text
https://raw.githubusercontent.com/uptonke/yahoo-finance/main/data/fed_labor_pivot_monitor.json
```

Or use the shorter version:

```text
https://raw.githubusercontent.com/uptonke/yahoo-finance/main/data/fed_labor_pivot_monitor_brief.json
```

## NFP revisions

Prior two-month NFP revisions are computed by comparing the latest PAYEMS levels against the previous committed JSON snapshot. The first successful run may mark this metric as unavailable; later runs can compute it once a prior snapshot exists.
