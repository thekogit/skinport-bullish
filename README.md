# Skinport Bullish — README

> **Skinport Bullish** is a small terminal utility that queries the Skinport public API to find the most *bullish* CS2 skins — i.e. items showing strong recent sales activity vs their weekly baseline — then displays them in a clear, human-friendly table.

---

## Quick highlights

- Uses the Skinport endpoints `/v1/items` and `/v1/sales/history` (single call each).
- Sends the `Accept-Encoding: br` header as required by Skinport.
- Filters by currency (USD, EUR, PLN, GBP), min/max price and minimum weekly sales.
- Computes a **Bullish Score** combining short-term growth and weekly volume to surface meaningful moves.

---

## Prerequisites

- Python 3.9+ (you used 3.13 — that works)
- Internet access
- A terminal (PowerShell, bash, etc.)


## Install & run

1. Create a virtual environment:

```bash
python -m venv venv
# windows
venv\Scripts\activate
# linux / mac
source venv/bin/activate
```

2. Install dependencies:

```bash
python -m pip install -r requirements.txt
```

3. Run the tool:

```bash
python main.py
```

The program will prompt for:
- `Currency` — one of: `usd`, `eur`, `pln`, `gbp` (case-insensitive)
- `Minimum price` — a number
- `Maximum price` — a number
- `Minimum sales in the last week` — integer

It will then fetch the data, compute the bullish scores, and print a nicely formatted table.

## Example run (illustrative)

```
=== Skinport Bullish Skins Analyzer ===
Currency (usd, pln, eur, gbp): pln
Minimum price: 5
Maximum price: 200
Minimum sales in the last week: 10

Most bullish skins (sorted):

+-----------------------------------------------------------+
         | MinPrice | Currency | 7d_vol | 7d_avg | 24h_avg | SalesGrowthRatio | BullishScore |
+-----------------------------------------------------------+
| AWP Dragon...|    45.00 |    PLN   |   112  |  40.50 |   65.70 |  1.623           |     2.3456   |
+-----------------------------------------------------------+
```

## How it works (technical detail)

1. **Endpoints used**
   - `/v1/items` — returns current item listings and `min_price` fields used as the representative price.
   - `/v1/sales/history` — returns aggregated sales stats that include `last_24_hours` and `last_7_days` objects containing `avg` and `volume` values.

2. **Required header**
   - The Skinport endpoints used require `Accept-Encoding: br` and a sensible `User-Agent` header. The program sets those in its requests.

3. **Rate limits & caching**
   - The endpoints are cached and rate-limited (docs indicate a small number of requests per time window). The script makes one call per endpoint and joins results locally to avoid hitting limits. If you run repeatedly while developing, add a 5-minute local cache file to limit calls.

4. **Bullish Score**
   - Intuition: we want items that show a significant short-term uptick *and* have meaningful volume.
   - Formula used in `main.py` (in words):
     - Compute short-term growth ratio: `growth_ratio = avg_24h / avg_7d` (or a fallback value if `avg_7d` missing).
     - Scale by `1 + log10(1 + vol_7d)` to prefer items with higher trading volume but with diminishing returns for huge volumes.
     - `BullishScore = growth_ratio * (1 + log10(1 + vol_7d))`.
   - This surfaces items where 24h activity meaningfully exceeds the weekly baseline while ensuring the move isn't from 1 or 2 trades (volume matters).

## Troubleshooting

- **400 Bad Request** — Ensure you include `Accept-Encoding: br` header exactly. If the script prints the response body, inspect it for hints.
- **Rate limiting / 429** — Wait a few minutes or cache the results locally; avoid per-item requests in quick loops.
- **No results** — widen your price window or lower `min_sales_week`.
- **SSL / network errors** — ensure your machine can reach `api.skinport.com` and that no corporate proxy blocks Brotli.

## Contribution & license

- This repository uses the **MIT License**.

```
MIT License

Copyright (c) 2025 Eliasz Dobrzański

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the \"Software\"), to deal
in the Software without restriction...
```