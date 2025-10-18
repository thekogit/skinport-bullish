#!/usr/bin/env python3


import csv
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime, timezone

from config import CSV_FIELDS

def escape_html(s: Any) -> str:
    if s is None:
        return ""
    s = str(s)
    return (s.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;"))

def write_csv(path: Path, rows: List[Dict[str, Any]]):
    with path.open("w", newline="", encoding="utf8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for r in rows:
            out = {k: (r.get(k, "") if r.get(k, "") is not None else "") for k in CSV_FIELDS}
            writer.writerow(out)

def read_csv_to_map(path: Path) -> Dict[str, Dict[str, Any]]:
    if not path.exists():
        return {}
    out = {}
    with path.open("r", encoding="utf8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row.get("Name")
            if name:
                out[name] = row
    return out

def generate_html_with_candidates(candidates: List[Dict[str, Any]], rows: List[Dict[str, Any]], 
                                out_path: Path, title: str = "Fee-Aware Skinport Analysis"):
    def format_pump_risk_cell(pump_risk_str: str) -> str:
        try:
            pump_risk = float(pump_risk_str) if pump_risk_str else 0.0
            if pump_risk >= 60:
                return f'<td class="num" style="color:#ff1a1a; font-weight:700; background-color:rgba(255,107,107,.12);">{pump_risk_str}</td>'
            elif pump_risk >= 40:
                return f'<td class="num" style="color:#cc0000; font-weight:700;">{pump_risk_str}</td>'
            elif pump_risk >= 25:
                return f'<td class="num" style="color:#ff6600; font-weight:600;">{pump_risk_str}</td>'
            elif pump_risk >= 15:
                return f'<td class="num" style="color:#ff9900;">{pump_risk_str}</td>'
            else:
                return f'<td class="num" style="color:#34d399;">{pump_risk_str}</td>'
        except:
            return f'<td class="num">{pump_risk_str}</td>'

    def format_profit_cell(profit_str: str) -> str:
        try:
            profit = float(profit_str) if profit_str else 0.0
            if profit >= 20.0:
                return f'<td class="num"><span class="badge green">+{profit_str}%</span></td>'
            elif profit >= 10.0:
                return f'<td class="num"><span class="badge green">+{profit_str}%</span></td>'
            elif profit >= 5.0:
                return f'<td class="num"><span class="badge yellow">+{profit_str}%</span></td>'
            elif profit >= 0:
                return f'<td class="num"><span class="badge gray">+{profit_str}%</span></td>'
            else:
                return f'<td class="num"><span class="badge red">{profit_str}%</span></td>'
        except:
            return f'<td class="num">{profit_str}</td>'

    def arb_badge(v: str) -> str:
        v = (v or "").strip()
        if v == "EXCELLENT_BUY":
            return '<span class="badge green">EXCELLENT BUY</span>'
        elif v == "GOOD_BUY":
            return '<span class="badge green">GOOD BUY</span>'
        elif v == "GOOD_BUY_LOW_VOL":
            return '<span class="badge yellow">GOOD BUY</span>'
        elif v == "MARGINAL_PROFIT":
            return '<span class="badge yellow">MARGINAL</span>'
        elif v == "BREAKEVEN":
            return '<span class="badge gray">BREAKEVEN</span>'
        elif v == "SMALL_LOSS":
            return '<span class="badge red">SMALL LOSS</span>'
        elif v == "OVERPRICED":
            return '<span class="badge red">OVERPRICED</span>'
        elif v == "NO_STEAM_DATA":
            return '<span class="badge gray">NO DATA</span>'
        else:
            return f'<span class="badge gray">{v.replace("_", " ")}</span>'

    table_headers = [
        "Name", "Skinport", "Steam", "SP Price", "Steam Price", "Fee-Aware Profit", 
        "Net Steam", "Currency", "SP Sales7d", "Steam Sales7d", "SP 7d avg", "SP 24h avg", 
        "SP 30d avg", "Steam Explosiveness", "SP 7d vs 30d", "SP Growth", 
        "SP Bullish", "SP Expl", "PumpRisk", "Arbitrage", "Source"
    ]

    header_html = "<tr>" + "".join([f"<th>{h}<span class=\"sort-arrow\"> ▼</span></th>" for h in table_headers]) + "</tr>"

    def row_html(r, candidate=False):
        skin_anchor = f'<a href="{escape_html(r.get("Skinport_URL"))}" target="_blank" rel="noopener">Skinport</a>'
        steam_anchor = f'<a href="{escape_html(r.get("Steam_URL"))}" target="_blank" rel="noopener">Steam</a>'
        profit_cell = format_profit_cell(r.get("Fee_Aware_Profit", "0").rstrip("%"))
        pump_cell = format_pump_risk_cell(r.get("PumpRisk", "0"))
        arb_cell = arb_badge(r.get("Arbitrage_Opportunity", ""))

        tr_style = " style='background:linear-gradient(180deg, rgba(34,197,94,.12), transparent)'" if candidate else ""
        return (
            f"<tr{tr_style}>"
            f"<td>{escape_html(r.get('Name'))}</td>"
            f"<td>{skin_anchor}</td>"
            f"<td>{steam_anchor}</td>"
            f"<td class='num'>{escape_html(r.get('Skinport_Price'))}</td>"
            f"<td class='num'>{escape_html(r.get('Steam_Price'))}</td>"
            f"{profit_cell}"
            f"<td class='num'>{escape_html(r.get('Net_Steam_Proceeds', ''))}</td>"
            f"<td>{escape_html(r.get('Currency'))}</td>"
            f"<td class='num'>{escape_html(r.get('Skinport_Sales7d'))}</td>"
            f"<td class='num'>{escape_html(r.get('Steam_Sales7d'))}</td>"
            f"<td class='num'>{escape_html(r.get('Skinport_7d_avg'))}</td>"
            f"<td class='num'>{escape_html(r.get('Skinport_24h_avg'))}</td>"
            f"<td class='num'>{escape_html(r.get('Skinport_30d_avg'))}</td>"
            f"<td class='num'>{escape_html(r.get('Steam_Explosiveness'))}</td>"
            f"<td class='num'>{escape_html(r.get('Skinport_7d_vs_30d'))}</td>"
            f"<td class='num'>{escape_html(r.get('Skinport_GrowthRatio'))}</td>"
            f"<td class='num'>{escape_html(r.get('Skinport_BullishScore'))}</td>"
            f"<td class='num'>{escape_html(r.get('Skinport_Explosiveness'))}</td>"
            f"{pump_cell}"
            f"<td style='text-align:center'>{arb_cell}</td>"
            f"<td style='text-align:center; font-size:10px;'><span class='pill'>{escape_html(r.get('Steam_Source',''))}</span></td>"
            "</tr>"
        )

    cand_rows = [row_html(r, candidate=True) for r in candidates]
    main_rows = [row_html(r, candidate=False) for r in rows]

    # Generate complete HTML with modern styling and JavaScript
    html = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="color-scheme" content="light dark">
<title>{escape_html(title)}</title>
<style>
:root {{
    color-scheme: light dark;
    --bg: #0b0f14;
    --panel: #121823;
    --panel-2: #0f1520;
    --text: #e6edf3;
    --muted: #8b98a5;
    --border: #1f2a3a;
    --accent: #4aa3ff;
    --accent-2: #7cd992;
    --bad-red: #ff6b6b;
    --bad-amber: #f7c948;
    --good-green: #22c55e;
    --row-even: rgba(255,255,255,0.02);
    --row-hover: rgba(74,163,255,0.10);
}}
@media (prefers-color-scheme: light) {{
    :root {{
    --bg: #f7f9fc;
    --panel: #ffffff;
    --panel-2: #f0f4fa;
    --text: #0b1a2a;
    --muted: #5b6b7b;
    --border: #dee5ef;
    --accent: #1f73ff;
    --accent-2: #19a974;
    --bad-red: #d7263d;
    --bad-amber: #e5a100;
    --good-green: #0f9d58;
    --row-even: #fafcff;
    --row-hover: rgba(31,115,255,0.08);
    }}
}}
/* Base styles */
* {{ box-sizing: border-box; }}
html, body {{ height: 100%; }}
body {{ background: var(--bg); color: var(--text); font-family: Inter, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; padding: 20px; line-height: 1.5; }}
h1 {{ margin: 6px 0 14px; font-weight: 650; letter-spacing: .2px; }}
.toolbar {{ display: flex; gap: 12px; align-items: center; flex-wrap: wrap; margin-bottom: 12px; }}
.chip {{ background: var(--panel-2); border: 1px solid var(--border); color: var(--muted); padding: 6px 10px; border-radius: 999px; font-size: 12px; }}
.search {{ flex: 1 1 320px; display: flex; align-items: center; gap: 8px; background: var(--panel); border: 1px solid var(--border); border-radius: 10px; padding: 8px 10px; }}
.search input {{ flex: 1; background: transparent; border: 0; outline: 0; color: var(--text); font-size: 14px; }}
.search input::placeholder {{ color: var(--muted); }}
.card {{ background: var(--panel); border: 1px solid var(--border); border-radius: 12px; box-shadow: 0 6px 20px rgba(0,0,0,.15); overflow: hidden; margin-bottom: 20px; }}
.table-wrap {{ overflow: auto; max-width: 100%; border-radius: 12px; }}
table {{ border-collapse: separate; border-spacing: 0; width: 100%; font-size: 12.5px; }}
thead th {{ position: sticky; top: 0; z-index: 2; background: linear-gradient(180deg, var(--panel-2), var(--panel)); color: var(--muted); text-transform: uppercase; letter-spacing: .4px; font-weight: 600; padding: 10px 8px; border-bottom: 1px solid var(--border); backdrop-filter: saturate(180%) blur(6px); cursor: pointer; user-select: none; }}
thead th:hover {{ background: linear-gradient(180deg, var(--panel), var(--panel-2)); }}
tbody td, tbody th {{ padding: 9px 8px; border-bottom: 1px solid var(--border); }}
tbody tr:nth-child(even) {{ background: var(--row-even); }}
tbody tr:hover {{ background: var(--row-hover); }}
th, td {{ text-align: left; }}
td.num, th.num {{ text-align: right; font-variant-numeric: tabular-nums; }}
td a {{ text-decoration: none; color: var(--accent); background: transparent; padding: 0; }}
td a:hover {{ text-decoration: underline; }}
.sort-arrow {{ color: var(--muted); margin-left: 6px; font-size: 10px; }}
.legend {{ background: var(--panel); border: 1px solid var(--border); border-radius: 10px; padding: 12px; margin: 12px 0; color: var(--muted); font-size: 13px; }}
.cand-note {{ background: linear-gradient(180deg, rgba(34,197,94,.15), transparent); border-left: 4px solid var(--good-green); padding: 10px 14px; border-radius: 8px; margin: 14px 0; }}
.badge {{ display: inline-block; padding: 3px 8px; border-radius: 999px; font-weight: 600; font-size: 11px; border: 1px solid transparent; }}
.badge.green {{ background: rgba(34,197,94,.15); color: var(--good-green); border-color: rgba(34,197,94,.25); }}
.badge.yellow {{ background: rgba(234,179,8,.15); color: #eab308; border-color: rgba(234,179,8,.25); }}
.badge.red {{ background: rgba(255,107,107,.12); color: var(--bad-red); border-color: rgba(255,107,107,.2); }}
.badge.gray {{ background: rgba(148,163,184,.12); color: var(--muted); border-color: rgba(148,163,184,.2); }}
.pill {{ background: var(--panel-2); color: var(--muted); padding: 2px 6px; border-radius: 6px; font-size: 10px; }}
.footer {{ color: var(--muted); font-size: 12px; margin-top: 10px; text-align: center; }}
</style>
<script>
// JavaScript for table sorting and filtering
const tableSortStates = new Map();
function sortTable(table, column) {{
const tableId = table.getAttribute('data-table-id') || Math.random().toString();
table.setAttribute('data-table-id', tableId);
const stateKey = tableId + '-' + column;
let currentState = tableSortStates.get(stateKey) || 'none';
let newState, ascending;
if (currentState === 'none' || currentState === 'desc') {{ newState = 'asc'; ascending = true; }}
else {{ newState = 'desc'; ascending = false; }}
tableSortStates.set(stateKey, newState);
const tbody = table.querySelector('tbody');
const rows = Array.from(tbody.querySelectorAll('tr'));
rows.sort((a, b) => {{
    let aVal = a.children[column].textContent.trim();
    let bVal = b.children[column].textContent.trim();
    if (column >= 3 && column <= 17) {{
    aVal = parseFloat(aVal.replace(/[^\d.-]/g, '')) || 0;
    bVal = parseFloat(bVal.replace(/[^\d.-]/g, '')) || 0;
    return ascending ? aVal - bVal : bVal - aVal;
    }} else {{ return ascending ? aVal.localeCompare(bVal) : bVal.localeCompare(aVal); }}
}});
tbody.innerHTML = '';
rows.forEach(row => tbody.appendChild(row));
const headers = table.querySelectorAll('th');
headers.forEach((header, index) => {{
    const arrow = header.querySelector('.sort-arrow');
    if (arrow) {{
    if (index === column) {{
        arrow.textContent = ascending ? ' ▲' : ' ▼';
    }} else {{
        arrow.textContent = ' ▼';
        const otherStateKey = tableId + '-' + index;
        tableSortStates.set(otherStateKey, 'none');
    }}
    }}
}});
}}
function initSortableTable(table) {{
const headers = table.querySelectorAll('th');
headers.forEach((header, index) => {{
    if (index < 2) return;
    header.addEventListener('click', () => {{ sortTable(table, index); }});
}});
}}
function filterTables(){{
const q = (document.getElementById('filterInput')?.value || '').toLowerCase();
document.querySelectorAll('table.sortable-table tbody tr').forEach(tr=>{{
    const text = tr.innerText.toLowerCase();
    tr.style.display = text.includes(q) ? '' : 'none';
}});
}}
document.addEventListener('DOMContentLoaded', () => {{
const tables = document.querySelectorAll('.sortable-table');
tables.forEach(initSortableTable);
}});
</script>
</head>
<body>
<h1>{escape_html(title)}</h1>
<div class="toolbar">
<div class="chip">⚡ Concurrent Analysis</div>
<div class="search">
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
    <path d="M21 21l-3.8-3.8M10.8 18.6a7.8 7.8 0 1 1 0-15.6 7.8 7.8 0 0 1 0 15.6z" stroke="currentColor" stroke-width="1.6" stroke-linecap="round"/>
    </svg>
    <input id="filterInput" placeholder="Filter by name, type, or source" oninput="filterTables()" />
</div>
</div>

<div class="legend">
<strong>🚀 High-Speed Concurrent Analysis:</strong> Async fetching with adaptive rate limiting<br>
<strong>💰 Fee-Aware Arbitrage:</strong> Buy Skinport (0% fees) → Sell Steam (15% fees)<br>
<strong>Profit Colors:</strong> 
<span class="badge green">Green ≥10%</span> 
<span class="badge yellow">Yellow 5-10%</span> 
<span class="badge gray">Gray 0-5%</span>
<span class="badge red">Red <0%</span><br>
<strong>Performance:</strong> Cache hit rates and concurrent processing for maximum speed
</div>"""

    # Candidate table
    if candidates:
        html += f"<div class='cand-note'><strong>🎯 Fee-Aware Top Candidates:</strong> {len(candidates)} item(s) with profitable arbitrage after platform fees</div>"
        html += f"<div class='card table-wrap'><table class='sortable-table'><thead>{header_html}</thead><tbody>"
        html += "".join(cand_rows)
        html += "</tbody></table></div>"

    # Main table
    html += f"<div class='card table-wrap'><table class='sortable-table'><thead>{header_html}</thead><tbody>"
    html += "".join(main_rows)
    html += "</tbody></table></div>"

    html += f"<div class='footer'>High-speed concurrent analysis • Platform fees: Skinport 0% + Steam 15% • {len(rows)} items processed • Generated: {datetime.now(timezone.utc).isoformat()}</div>"
    html += "</body></html>"

    out_path.write_text(html, encoding="utf8")
    print(f"📄 Wrote concurrent analysis HTML to {out_path}")
