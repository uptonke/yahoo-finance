# Fed Labor Pivot Data Fetch Setup

This patch adds an independent Federal Reserve labor-pivot data pipeline to the existing `uptonke/yahoo-finance` repository.

## Files added

```text
fed_labor_pivot.py
requirements-fed-labor.txt
.github/workflows/fed-labor-pivot.yml
docs/FED_LABOR_PIVOT_SETUP.md
```

The script writes:

```text
data/fed_labor_pivot_monitor.json
data/fed_labor_pivot_monitor_brief.json
data/fed_labor_pivot_monitor_history.jsonl
```

## Required GitHub Secret

Add this repository secret:

```text
FRED_API_KEY
```

The script will not crash if the key is missing, but the output will be marked `DATA_INVALID`.

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

Suggested timing:

```text
Every Saturday 09:40 Asia/Taipei
Optional backup trigger: Every Saturday 09:50 Asia/Taipei
```

## ChatGPT scheduled-report JSON URL

Use this as the primary numeric input:

```text
https://raw.githubusercontent.com/uptonke/yahoo-finance/main/data/fed_labor_pivot_monitor.json
```

Or use the shorter version:

```text
https://raw.githubusercontent.com/uptonke/yahoo-finance/main/data/fed_labor_pivot_monitor_brief.json
```

## Design notes

- Numeric data comes from FRED API.
- Final Fed pivot classification still requires LLM review of official Federal Reserve communication.
- Prior two-month NFP revisions require a previous committed PAYEMS snapshot. The first run will mark this metric as unavailable; later runs can compute it by comparing the current PAYEMS levels with the previous JSON snapshot.
- This patch does not modify the existing `main.py`.
