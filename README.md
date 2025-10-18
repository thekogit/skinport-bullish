# Skinport Bullish Skins Analyzer

A simple tool to find price differences (arbitrage) between Skinport and Steam markets.

## Features

- Fetches item prices and sales data from Skinport and Steam.
- Calculates profitable arbitrage opportunities after fees.
- Highlights fee-aware profit percentages with intuitive color coding.
- Provides clickable links to Skinport and Steam listings.
- Supports multiple games: CS2, Dota 2, TF2, Rust.
- Handles multiple currencies accurately.

## Usage

Clone this repo and run:



```bash
python main.py
```

Follow the prompt to select the game(s), currency, and filters. The tool will fetch data and generate an HTML report highlighting arbitrage opportunities.

## Configuration

Edit `config.py` if you need to adjust:

- Supported games and their app IDs.
- Fee rates for Skinport and Steam.
- Currency mappings.
- Arbitrage thresholds.

## Notes

- Designed for speed using async requests with adaptive rate limiting.
- Cache support included to avoid excessive API calls.
- Requires Python 3.7+ with `aiohttp` and `tqdm` for best experience.
- Currently used for market analysis and browsing profitable trades, not automated trading.