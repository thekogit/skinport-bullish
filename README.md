# Skinport Bullish

**Skinport Bullish** is a Python tool that finds CS2 skins with strong recent sales. It outputs an **HTML** table (default) or **CSV** file.

---

## Highlights

- Queries Skinport API once for items and once for sales history.
- Filters for currency, price, minimum weekly sales, and item type (`knife`, `gloves`, `ak-47`, etc.).
- Calculates **Bullish Score**: combines short-term (24h vs 7d) and medium-term (7d vs 30d) trends, weighted by volume.
- Output: `skinport_bullish.html` or `skinport_bullish.csv`.

---

## Features

- Token-based filters and weapon aliases
- Excludes non-weapon items (cases, charms, stickers)
- Uses 30-day averages if available
- Caching via `requests_cache` or local file (`~/.skinport_skin_cache`)
- Merge mode updates a master CSV

---

## Requirements

- Python 3.9+
- Internet access
- Terminal (PowerShell, cmd, bash, etc.)

---

## Setup & Run

1. Clone repo:

```bash
git clone git@github.com:<user>/<repo>.git
cd <repo>
```

2. Create & activate venv:

```bash
python -m venv venv
# Windows PowerShell
venv\Scripts\Activate.ps1
# Windows cmd
venv\Scripts\activate
# macOS/Linux
source venv/bin/activate
```

3. Install dependencies:

```
requests>=2.28
requests-cache>=0.9
```

```bash
python -m pip install -r requirements.txt
```

4. Run:

```bash
python main.py
```

---

### Prompts

- **Currency** — `usd`, `eur`, `pln`, `gbp` (default `usd`)
- **Min/Max price** — numbers (blank = no limit)
- **Min weekly sales** — number
- **Filters** — comma-separated tokens (blank = all)
- **Output format** — `html` (default) or `csv`
- **Write mode** — `overwrite` (default) or `merge`

Outputs saved as `skinport_bullish.html` or `.csv`.

---

## Output Columns

- `Name`, `Skinport_URL`, `Steam_URL`
- `Price`, `Currency`
- `SalesThisWeek`, `7d_avg`, `24h_avg`, `30d_avg`
- `7d_vs_30d`, `GrowthRatio`, `BullishScore`, `LastUpdated`

---

## BullishScore

1. Short-term: `24h_avg / 7d_avg`
2. Medium-term: `7d_avg / 30d_avg`
3. Weighted: `0.6*short + 0.4*medium`
4. Adjust for volume: `1 + log10(1 + vol_7d)`
5. Final score = weighted growth × volume factor

---

## Filtering

- Handles hyphens (`m4a1-s`, `m4a4`)
- Excludes cases, charms, stickers, souvenirs
- Knives: specific tokens or star `★` + knife token

---

## Caching & Rate-Limits

- One request each to `/v1/items` and `/v1/sales/history`
- Uses `requests_cache` if available
- Local cache: `~/.skinport_skin_cache` (TTL 300s)
- Avoid repeated runs to prevent rate-limits

---

## Troubleshooting

- `406` → check `Accept-Encoding: br` header
- `400/429` → respect cache or retry later
- No results → widen filters
- Wrong matches → use specific tokens
- Datetime warnings → UTC used
- Automation → convert prompts to CLI args

---

## Optional Files

**requirements.txt**

```
requests>=2.28
requests-cache>=0.9
```

**.gitignore**

```
__pycache__/
*.py[cod]
venv/
.env/
.vscode/
.DS_Store
Thumbs.db
*.log
skinport_bullish.*
.skinport_skin_cache/
```

---

