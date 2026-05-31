from . import HQL
from app.data.cache import Cache
from app.data.db import DB

from datetime import date

hql = HQL(cache=Cache(), db=DB())

# print(hql.ticker("DFM:EMAAR").raw())
# print(hql.ticker("DFM:EMAAR").info())
# print(hql.ticker("DFM:EMAAR").overview())
# print(hql.ticker("DFM:EMAAR").statistics())
# print(hql.ticker("DFM:EMAAR").ohlcv(days=90).head())
# print(hql.ticker("DFM:EMAAR").dividends().head())
# print(hql.ticker("DFM:EMAAR").financials().head())
# print(hql.ticker("DFM:EMAAR").ratios().head())
# print(hql.ticker("DFM:EMAAR").prices(days=64).head())

# print(hql.tickers(["DFM:EMAAR", "NYSE:O"]).prices(days=365))
# print(
#     hql.tickers(["DFM:EMAAR", "DFM:DUBAIRESI"])
#     .compare("pe", "div_yield", "roe", "beta")
#     .head()
# )

print(f"Transactions: {hql.portfolio().transactions().shape[0]} rows")
print(hql.portfolio().transactions())

print(f"Holdings: {hql.portfolio().holdings().shape[0]} rows")
print(hql.portfolio().holdings())

print(f"Dividends: {hql.portfolio().dividends().shape[0]} rows")
print(hql.portfolio().dividends())

print("Value")
print(
    hql.portfolio()
    .value(start_date=date(2026, 1, 1), end_date=date(2026, 5, 31))
    .tail(10)
)

print("Allocation")
print(hql.portfolio().allocation())
