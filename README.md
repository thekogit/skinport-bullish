# Skinport Bullish CS2 Tool

This is a Python program for finding CS2 skins that had a lot of sales recently. It checks prices and sales from Skinport and Steam, does some math, and tries to show which items look "bullish" (going up in sales and price).

## Requirements

- Python 3.9 or newer
- Internet connection


## Install

Clone this repo:

```bash
git clone https://github.com/thekogit/skinport-bullish.git
cd skinport-bullish
```

Set up a virtual environment:

```bash
python -m venv venv
# Then activate it:
# Windows PowerShell:
venv\Scripts\Activate.ps1
# Windows cmd:
venv\Scripts\activate.bat
# macOS / Linux:
source venv/bin/activate
```

Install the needed stuff:

```bash
python -m pip install -r requirements.txt
```


## How To Run

Just run:

```bash
python main.py
```

It will ask questions and run with whatever answers are given, or use defaults. All the real stuff happens in `main.py` and it asks for the filters in the terminal.

## What It Does

- Gets CS2 skin info from Skinport (and Steam)
- Filters items by price, weekly sales, and item type like knife, glove, AK, etc.
- Tries to avoid cases, stickers, charms, souvenirs, etc.
- Scores skins with some custom formula, mixing short-term and long-term changes, and volume
- Supports currencies: usd, eur, pln, gbp


## Output

- Results are saved as `skinportbullish.html` (table) or `skinportbullish.csv` (spreadsheet)
- Can merge results with older CSV instead of overwriting


## Command-line Options

You can set these filters when running (or just use the prompts):

- Currency (`usd`, `eur`, etc., default: usd)
- Min/Max price (type a number, blank = all)
- Min weekly sales (number)
- Item type (`knife`, `ak-47`, etc)
- Output file format (`html` by default, or `csv`)
- Write mode: overwrite or merge with old results


## Features

- Tries not to repeat API calls too much (caches results)
- Deals with API errors and retries for Steam
- Conservative with timing (avoids bans/limits)
- Highlights top candidates with profit and risk info
- Handles Steam fees, Skinport fees, and does "arbitrage" checks


## Notes

- Always obey rate limits (sometimes you have to wait if using a lot)
- Fails gracefully and saves errors for later retry
- For faster results, install `aiohttp` (optional for concurrency)