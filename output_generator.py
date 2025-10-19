#!/usr/bin/env python3
import csv
import json
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
                return f'<td class="num"><span class="badge red">🔴 {pump_risk:.1f}</span></td>'
            elif pump_risk >= 40:
                return f'<td class="num"><span class="badge orange">🟠 {pump_risk:.1f}</span></td>'
            elif pump_risk >= 25:
                return f'<td class="num"><span class="badge yellow">🟡 {pump_risk:.1f}</span></td>'
            elif pump_risk >= 15:
                return f'<td class="num"><span class="badge green">🟢 {pump_risk:.1f}</span></td>'
            else:
                return f'<td class="num"><span class="badge green">✅ {pump_risk:.1f}</span></td>'
        except:
            return f'<td class="num">{escape_html(pump_risk_str)}</td>'

    def format_profit_cell(profit_str: str) -> str:
        try:
            profit_val = float(profit_str) if profit_str else 0.0
            if profit_val >= 10:
                return f'<td class="num"><span class="badge green">+{profit_str}%</span></td>'
            elif profit_val >= 5:
                return f'<td class="num"><span class="badge yellow">+{profit_str}%</span></td>'
            elif profit_val > 0:
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
        "Name", "Chart", "Skinport", "Steam", "SP Price", "Steam Price", "Fee-Aware Profit", 
        "Net Steam", "Currency", "SP Sales7d", "Steam Sales7d", "SP 7d avg", "SP 24h avg", 
        "SP 30d avg", "Steam Explosiveness", "SP 7d vs 30d", "SP Growth", 
        "SP Bullish", "SP Expl", "PumpRisk", "Arbitrage", "Source"
    ]

    header_html = "<tr>" + "".join([f'<th>{h}<span class="sort-arrow"> ▼</span></th>' for h in table_headers]) + "</tr>"

    def row_html(r, candidate=False):
        skin_name = escape_html(r.get('Name'))
        skin_anchor = f'<a href="{escape_html(r.get("Skinport_URL"))}" target="_blank" rel="noopener">Skinport</a>'
        steam_anchor = f'<a href="{escape_html(r.get("Steam_URL"))}" target="_blank" rel="noopener">Steam</a>'
        profit_cell = format_profit_cell(r.get("Fee_Aware_Profit", "0").rstrip("%"))
        pump_cell = format_pump_risk_cell(r.get("PumpRisk", "0"))
        arb_cell = arb_badge(r.get("Arbitrage_Opportunity", ""))

        # Generate chart data ID for this skin
        chart_id = f"chart_{abs(hash(r.get('Name', '')))}".replace("-", "")

        # Create name cell with hover chart
        name_cell = f'<td class="skin-name-cell" data-chart-id="{chart_id}">{skin_name}</td>'

        # Create chart link cell
        chart_link = f'<td><a href="#{chart_id}" class="chart-link" onclick="showChart(\'{chart_id}\'); return false;">📊 View</a></td>'

        tr_style = " style='background:linear-gradient(180deg, rgba(34,197,94,.12), transparent)'" if candidate else ""
        return (
            f"<tr{tr_style}>"
            f"{name_cell}"
            f"{chart_link}"
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
            f"<td>{arb_cell}</td>"
            f"<td>{escape_html(r.get('Steam_Source', ''))}</td>"
            "</tr>"
        )

    cand_rows = [row_html(c, True) for c in candidates]
    main_rows = [row_html(r, False) for r in rows]

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    # Collect chart data for all items
    chart_data_map = {}
    for r in rows:
        chart_id = f"chart_{abs(hash(r.get('Name', '')))}".replace("-", "")
        price_history = r.get('PriceHistory', [])
        chart_data_map[chart_id] = {
            'name': r.get('Name', ''),
            'history': price_history
        }

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{escape_html(title)}</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
    <style>
        * {{ margin:0; padding:0; box-sizing:border-box; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; 
               background: #0f172a; color: #e2e8f0; padding:20px; }}
        .container {{ max-width:100%; margin:0 auto; }}
        h1 {{ color:#fff; margin-bottom:10px; }}
        .meta {{ color:#94a3b8; margin-bottom:20px; font-size:14px; }}
        .card {{ background:#1e293b; border-radius:8px; padding:20px; margin-bottom:20px; 
                box-shadow:0 4px 6px rgba(0,0,0,0.3); }}
        .cand-note {{ background:#1e293b; color:#fff; padding:12px; margin-bottom:15px; border-radius:6px;
                     border-left:4px solid #22c55e; }}
        .table-wrap {{ overflow-x:auto; }}
        table {{ width:100%; border-collapse:collapse; font-size:13px; }}
        th {{ background:#334155; color:#fff; padding:12px 8px; text-align:left; position:sticky; top:0;
             white-space:nowrap; cursor:pointer; user-select:none; }}
        th:hover {{ background:#475569; }}
        .sort-arrow {{ color:#94a3b8; font-size:10px; margin-left:4px; }}
        td {{ padding:10px 8px; border-bottom:1px solid #334155; }}
        tr:hover {{ background:#334155; }}
        .num {{ text-align:right; font-family:'Courier New', monospace; }}
        a {{ color:#3b82f6; text-decoration:none; }}
        a:hover {{ text-decoration:underline; }}
        .badge {{ padding:4px 8px; border-radius:4px; font-size:11px; font-weight:600; white-space:nowrap; }}
        .badge.green {{ background:#22c55e; color:#000; }}
        .badge.yellow {{ background:#eab308; color:#000; }}
        .badge.orange {{ background:#f97316; color:#fff; }}
        .badge.red {{ background:#ef4444; color:#fff; }}
        .badge.gray {{ background:#6b7280; color:#fff; }}
        .skin-name-cell {{ position: relative; cursor: help; }}
        .chart-popup {{ position: absolute; display: none; z-index: 1000; background: #1e293b; 
                       border: 2px solid #3b82f6; border-radius: 8px; padding: 15px; 
                       box-shadow: 0 10px 30px rgba(0,0,0,0.5); width: 400px; left: 0; top: 100%; margin-top: 5px; }}
        .chart-popup.active {{ display: block; }}
        .chart-link {{ color: #3b82f6; cursor: pointer; font-size: 16px; }}
        .chart-link:hover {{ color: #60a5fa; }}
        .modal {{ display: none; position: fixed; z-index: 2000; left: 0; top: 0; width: 100%; height: 100%; 
                 background-color: rgba(0,0,0,0.8); }}
        .modal-content {{ background-color: #1e293b; margin: 5% auto; padding: 20px; border: 1px solid #3b82f6; 
                         width: 80%; max-width: 800px; border-radius: 10px; }}
        .close {{ color: #aaa; float: right; font-size: 28px; font-weight: bold; cursor: pointer; }}
        .close:hover {{ color: #fff; }}
        #modalChartContainer {{ height: 400px; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>{escape_html(title)}</h1>
        <div class="meta">Generated: {timestamp}</div>
"""

    if candidates:
        html += f"<div class='cand-note'><strong>🎯 Fee-Aware Top Candidates:</strong> {len(candidates)} item(s) with profitable arbitrage after platform fees</div>"
        html += f"<div class='card table-wrap'><table class='sortable-table'><thead>{header_html}</thead><tbody>"
        html += "".join(cand_rows)
        html += "</tbody></table></div>"

    # Main table
    html += f"<div class='card table-wrap'><table class='sortable-table'><thead>{header_html}</thead><tbody>"
    html += "".join(main_rows)
    html += "</tbody></table></div>"

    # Chart modal
    html += """
    <div id="chartModal" class="modal">
        <div class="modal-content">
            <span class="close" onclick="closeModal()">&times;</span>
            <h2 id="modalChartTitle">Price Chart</h2>
            <div id="modalChartContainer">
                <canvas id="modalChart"></canvas>
            </div>
        </div>
    </div>
    """

    # JavaScript for chart data and interactions
    html += f"""
    <script>
    // Chart data embedded in page
    const chartDataMap = {json.dumps(chart_data_map, indent=2)};

    let currentChart = null;
    let modalChart = null;

    // Show chart in modal
    function showChart(chartId) {{
        const modal = document.getElementById('chartModal');
        const data = chartDataMap[chartId];

        if (!data || !data.history || data.history.length === 0) {{
            alert('No price history available for this item');
            return;
        }}

        document.getElementById('modalChartTitle').textContent = data.name + ' - Price History';
        modal.style.display = 'block';

        // Destroy existing chart if any
        if (modalChart) {{
            modalChart.destroy();
        }}

        const ctx = document.getElementById('modalChart').getContext('2d');
        modalChart = new Chart(ctx, {{
            type: 'line',
            data: {{
                labels: data.history.map((d, i) => d.date || `Day ${{i+1}}`),
                datasets: [{{
                    label: 'Price',
                    data: data.history.map(d => d.price),
                    borderColor: '#3b82f6',
                    backgroundColor: 'rgba(59, 130, 246, 0.1)',
                    borderWidth: 2,
                    fill: true,
                    tension: 0.4
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{
                    legend: {{
                        display: false
                    }},
                    tooltip: {{
                        mode: 'index',
                        intersect: false
                    }}
                }},
                scales: {{
                    y: {{
                        beginAtZero: false,
                        grid: {{
                            color: '#334155'
                        }},
                        ticks: {{
                            color: '#94a3b8'
                        }}
                    }},
                    x: {{
                        grid: {{
                            color: '#334155'
                        }},
                        ticks: {{
                            color: '#94a3b8',
                            maxRotation: 45,
                            minRotation: 45
                        }}
                    }}
                }}
            }}
        }});
    }}

    function closeModal() {{
        document.getElementById('chartModal').style.display = 'none';
        if (modalChart) {{
            modalChart.destroy();
            modalChart = null;
        }}
    }}

    // Close modal when clicking outside
    window.onclick = function(event) {{
        const modal = document.getElementById('chartModal');
        if (event.target == modal) {{
            closeModal();
        }}
    }}

    // Hover chart functionality
    document.addEventListener('DOMContentLoaded', function() {{
        const nameCells = document.querySelectorAll('.skin-name-cell');

        nameCells.forEach(cell => {{
            const chartId = cell.dataset.chartId;
            const data = chartDataMap[chartId];

            if (data && data.history && data.history.length > 0) {{
                // Create popup element
                const popup = document.createElement('div');
                popup.className = 'chart-popup';
                popup.innerHTML = '<canvas id="hover_' + chartId + '" width="350" height="200"></canvas>';
                cell.style.position = 'relative';
                cell.appendChild(popup);

                let hoverChart = null;

                cell.addEventListener('mouseenter', function() {{
                    popup.classList.add('active');

                    const ctx = document.getElementById('hover_' + chartId).getContext('2d');
                    hoverChart = new Chart(ctx, {{
                        type: 'line',
                        data: {{
                            labels: data.history.map((d, i) => d.date || `Day ${{i+1}}`),
                            datasets: [{{
                                label: 'Price',
                                data: data.history.map(d => d.price),
                                borderColor: '#3b82f6',
                                backgroundColor: 'rgba(59, 130, 246, 0.1)',
                                borderWidth: 2,
                                fill: true,
                                tension: 0.4,
                                pointRadius: 2
                            }}]
                        }},
                        options: {{
                            responsive: false,
                            plugins: {{
                                legend: {{ display: false }},
                                tooltip: {{ enabled: true }}
                            }},
                            scales: {{
                                y: {{
                                    beginAtZero: false,
                                    grid: {{ color: '#334155' }},
                                    ticks: {{ color: '#94a3b8', font: {{ size: 10 }} }}
                                }},
                                x: {{
                                    grid: {{ color: '#334155' }},
                                    ticks: {{ color: '#94a3b8', font: {{ size: 9 }}, maxRotation: 0 }}
                                }}
                            }}
                        }}
                    }});
                }});

                cell.addEventListener('mouseleave', function() {{
                    popup.classList.remove('active');
                    if (hoverChart) {{
                        hoverChart.destroy();
                        hoverChart = null;
                    }}
                }});
            }}
        }});
    }});

    // Table sorting functionality
    document.querySelectorAll('th').forEach((th, colIndex) => {{
        th.addEventListener('click', () => {{
            const table = th.closest('table');
            const tbody = table.querySelector('tbody');
            const rows = Array.from(tbody.querySelectorAll('tr'));

            const isNumeric = th.classList.contains('num') || th.textContent.includes('Price') || 
                            th.textContent.includes('Sales') || th.textContent.includes('Risk');

            rows.sort((a, b) => {{
                const aVal = a.cells[colIndex].textContent.trim();
                const bVal = b.cells[colIndex].textContent.trim();

                if (isNumeric) {{
                    const aNum = parseFloat(aVal.replace(/[^0-9.-]/g, '')) || 0;
                    const bNum = parseFloat(bVal.replace(/[^0-9.-]/g, '')) || 0;
                    return aNum - bNum;
                }} else {{
                    return aVal.localeCompare(bVal);
                }}
            }});

            rows.forEach(row => tbody.appendChild(row));
        }});
    }});
    </script>
</body>
</html>
"""

    with out_path.open("w", encoding="utf8") as f:
        f.write(html)

    print(f"✅ Generated HTML report: {out_path}")
    print(f"   {len(candidates)} candidates, {len(rows)} total items")
    print(f"   📊 Interactive price charts included (hover on names, click chart links)")
