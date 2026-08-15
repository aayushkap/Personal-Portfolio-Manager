Performance Page Ideas

1.  Realized Vs. Unrealized Gains
2.  Weighted Market Vs. Returns - Weighted Alpha. Time In Market Inclusive. Inclusive Of Sells. Filter: Include Dividends.
3.  Timing Skill Chart.

4. Centerpiece: Brinson-Fachler Attribution — the actual thing hedge funds report to LPs
This is the single most important addition, and it directly answers a question your current alpha number cannot answer: "Did I beat the market because I picked good stocks, or because I happened to be overweight in a sector/exchange that did well anyway?" Right now your alpha number conflates both. Brinson-Fachler splits it apart.

The three effects, using your actual data fields (weight_pct, twr_pct per position, grouped by sector or exchange, benchmark_return_pct per group):

Allocation effect  = (portfolio_weight_i − benchmark_weight_i) × (benchmark_return_i − total_benchmark_return)
Selection effect   = benchmark_weight_i × (portfolio_return_i − benchmark_return_i)
Interaction effect = (portfolio_weight_i − benchmark_weight_i) × (portfolio_return_i − benchmark_return_i)
Summed across all sectors/exchanges i, and verified: Allocation + Selection + Interaction = Total Active Return (your alpha). This identity is your QA check — if it doesn't reconcile to your existing alpha_pct, something's wrong in the calc.

5. Concentration risk (Herfindahl-Hirschman Index) — Σ(weight_i²) across positions. A single number that quantifies "how concentrated am I really," independent of raw position count. Trivial to compute, genuinely used by real risk desks, and you already have every weight needed.

6. Contribution to risk vs. contribution to return — for each position, compute its % contribution to total portfolio variance (using the correlation matrix + weights) versus its % contribution to total return. Plot both as a bar pair per position. This surfaces positions that are "carrying more risk than they're paying you for" — a genuinely institutional-grade insight retail dashboards almost never show. Can also account for volatility.

7. Per Position Analytics ( Sold & Active )
