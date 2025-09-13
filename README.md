# Skinport Bullish

> **Skinport Bullish** is a small terminal utility that queries the Skinport public API to find the most *bullish* CS2 skins — items showing strong recent sales activity relative to their weekly and monthly baseline — and writes the results as a human-friendly **HTML** (default) or **CSV** file.

---

## Quick highlights

- Uses the Skinport endpoints `/v1/items` and `/v1/sales/history` (one call each).
- Sends the `Accept-Encoding: br` header as required by Skinport.
- Interactive filters for currency, price window, minimum weekly sales, and category/weapon tokens (e.g. `knife`, `gloves`, `ak-47`, `m4a4`, `m4a1-s`, `butterfly knife`, `karambit`).
- Computes a **Bullish Score** combining short-term (24h vs 7d) and medium-term (7d vs 30d) momentum weighted by trading volume.
- Default output is an **HTML** table (`skinport_bullish.html`) for quick visual inspection; CSV (`skinport_bullish.csv`) option is available.

---

## Features

- Token-based filtering with expanded keyword lists (many knife, glove, rifle, SMG, pistol aliases).
- Correct handling for hyphenated weapon tokens (e.g. `m4a4` vs `m4a1-s`) so filters match specific weapon variants.
- Smart exclusion rules to avoid matching `case`, `charm`, `sticker`, `souvenir`, `package`, `key` and other non-weapon items.
- 30-day average detection: script checks for common keys in the sales history JSON (e.g. `last_30_days`, `last_30`, `last_30d`) to compute a medium-term trend.
- Caching: uses `requests_cache` when available; otherwise falls back to a local per-endpoint file cache (`~/.skinport_skin_cache`) with TTL (default 300s).
- Merge mode: maintain a canonical master CSV in your cache dir to merge results across runs, or overwrite mode to replace outputs.

---

## Prerequisites

- Python **3.9+** (tested on 3.13)  
- Internet access  
- Terminal (PowerShell, cmd, bash, etc.)

---

## Install & run

1. Clone the repo (if not already):

```bash
git clone git@github.com:<your-user>/<your-repo>.git
cd <your-repo>
```

2. Create and activate a virtual environment:

```bash
python -m venv venv

# Windows (PowerShell)
venv\Scripts\Activate.ps1

# Windows (cmd.exe)
venv\Scripts\activate

# macOS / Linux
source venv/bin/activate
```

3. Create a `requirements.txt` (suggested contents):

```
requests>=2.28
requests-cache>=0.9
```

Install dependencies:

```bash
python -m pip install -r requirements.txt
```

> `requests-cache` is optional but strongly recommended. If it is not installed the script will use a local file cache directory (`~/.skinport_skin_cache`) with a default TTL of 300 seconds.

4. Run the tool:

```bash
python main.py
```

### Interactive prompts

When you run `main.py` it will prompt for:

- **Currency** — `usd`, `eur`, `pln`, or `gbp` (case-insensitive). Default: `usd`.  
- **Minimum price** — number (blank → 0).  
- **Maximum price** — number (blank → no upper limit).  
- **Minimum sales in the last week** — integer (blank → ignored).  
- **Filters** — comma-separated categories or weapon tokens (blank → all). Examples:
  - `knife`
  - `ak-47`
  - `knife, m4a1-s`
  - `gloves`  
- **Output format** — `html` (default) or `csv`.  
- **Write mode** — `overwrite` (default) or `merge`. Merge mode updates a master CSV in `~/.skinport_skin_cache/skinport_bullish_master.csv`.

After fetching and computing scores the tool writes:
- `skinport_bullish.html` (default) or  
- `skinport_bullish.csv`  
to the current working directory.

---

## Output columns (CSV / HTML)

The output contains at least the following columns:

- `Name` — market name of the item  
- `Skinport_URL`  
- `Steam_URL`  
- `Price` — representative price (rounded)  
- `Currency`  
- `SalesThisWeek` — 7-day volume  
- `7d_avg` — 7-day average price  
- `24h_avg` — 24-hour average price  
- `30d_avg` — 30-day average price (if available)  
- `7d_vs_30d` — ratio: `7d_avg / 30d_avg` (if available)  
- `GrowthRatio` — legacy short-term ratio `24h_avg / 7d_avg`  
- `BullishScore` — combined metric used for ranking  
- `LastUpdated` — UTC timestamp of computation

---

## How BullishScore is computed

The goal is to surface items with meaningful, short-term upward activity while reducing noise from tiny-volume trades.

1. **Short-term growth**  
   `growth_short = avg_24h / avg_7d` (fallback used if `avg_7d` missing)

2. **Medium-term growth**  
   `growth_med = avg_7d / avg_30d` (neutral fallback used if `avg_30d` missing)

3. **Combined growth**  
   `combined = 0.6 * growth_short + 0.4 * growth_med` (weights configurable in code)

4. **Volume weighting**  
   `volume_factor = 1 + log10(1 + vol_7d)` — increases score for higher weekly volume but with diminishing returns

5. **Final score**  
   `BullishScore = combined * volume_factor`

Rationale: prefer items that show a real recent uptick relative to their weekly baseline, are supported by trading volume, and — where available — reflect the medium-term trend.

---

## Matching & filtering details

- Filters accept tokens and expand common aliases (e.g., `m4a1-s` vs `m4a4`).  
- Matching uses token-based regex checks so hyphenated names match correctly.  
- Exclusion lists are applied per-category to avoid matches for cases, charms, stickers, souvenir items, etc.  
- **Knife heuristics**: an item is considered a knife match if it contains a knife-specific token (e.g. `karambit`, `flip`, `kukri`, `bayonet`) or both contains a star `★` and a knife token. This avoids gloves/case matches that also include `★`.

---

## Caching & rate-limits

- The script makes **one** request to `/v1/items` and **one** request to `/v1/sales/history`, then merges locally.  
- `requests_cache` (if installed) will cache HTTP responses and prevent repeated API hits during development.  
- Otherwise the script creates a simple file cache at `~/.skinport_skin_cache` with TTL of `DEFAULT_CACHE_TTL` seconds (default 300s).  
- If you see rate-limiting responses (e.g. `429`), increase TTL or avoid repeated runs in short intervals.

---

## Troubleshooting

- **`406 Not Acceptable`** — Skinport requires the `Accept-Encoding: br` header. Ensure that header is present and not stripped by proxies or middleboxes.  
- **`400 / 429` or other HTTP errors** — check the response body for hints; respect caching and retry intervals.  
- **No results** — widen the price range, lower `Minimum sales in the last week`, or clear filter tokens.  
- **Wrong matches (cases/charms appear)** — use more specific tokens (e.g., `karambit`) or paste example names so the keyword/exclusion lists can be improved.  
- **Datetime warnings** — the tool uses timezone-aware UTC timestamps.  
- **Automated runs** — consider converting interactive prompts to CLI args (can be added).

---

## Files you might want to add

**Suggested `requirements.txt`:**

```
requests>=2.28
requests-cache>=0.9
```

**Suggested `.gitignore` (Python minimal):**

```
# Byte-compiled / optimized / DLL files
__pycache__/
*.py[cod]
*$py.class

# Virtual environments
venv/
ENV/
env/

# Editor / OS files
.vscode/
.DS_Store
Thumbs.db

# Cache / output
*.log
skinport_bullish.csv
skinport_bullish.html
.skinport_skin_cache/
```

---

## Contribution

Contributions, improvements and keyword additions are welcome. If you want to add tokens (e.g., a rare knife name or community alias), please:

1. Fork the repository  
2. Create a branch (eg. `feature/add-keywords`)  
3. Make the changes (update the keyword maps in `main.py`)  
4. Submit a pull request with examples (sample item names that should match)

If you'd like, open an issue with a few sample `market_hash_name` JSON snippets from `/v1/items` and the matching logic can be adapted to rely on structured API fields rather than heuristics.

---

## License

MIT License

```
MIT License

Copyright (c) 2025 Eliasz Dobrzański

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:


The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.


THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

