from .facade import HQL
from app.data.cache import Cache
from app.data.db import DB

hql = HQL(cache=Cache(), db=DB())

print(hql.ticker("DFM:EMAAR").raw())
print(hql.ticker("DFM:EMAAR").info())
print(hql.ticker("DFM:EMAAR").overview())
print(hql.ticker("DFM:EMAAR").statistics())
print(hql.ticker("DFM:EMAAR").ohlcv(days=90).head())
print(hql.ticker("DFM:EMAAR").dividends().head())
print(hql.ticker("DFM:EMAAR").financials().head())
print(hql.ticker("DFM:EMAAR").ratios().head())
print(hql.ticker("DFM:EMAAR").prices(days=64).head())

print(hql.tickers(["DFM:EMAAR", "NYSE:O"]).prices(days=365))
print(
    hql.tickers(["DFM:EMAAR", "DFM:DUBAIRESI"])
    .compare("pe", "div_yield", "roe", "beta")
    .head()
)
