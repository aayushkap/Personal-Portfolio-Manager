from curl_cffi import requests
import re

LOGO_RE = re.compile(
    r'https://s3-symbol-logo\.tradingview\.com/([^"\'\s\\)]+?)--big\.svg'
)


def get_logo(exchange, ticker):
    url = f"https://www.tradingview.com/symbols/{exchange}-{ticker}/"
    r = requests.get(url, impersonate="chrome", timeout=15)
    r.raise_for_status()
    found = list(dict.fromkeys(LOGO_RE.findall(r.text)))
    if found:
        return f"https://s3-symbol-logo.tradingview.com/{found[0]}--big.svg"
    fallback = re.findall(
        r"https://s3-symbol-logo\.tradingview\.com/source/%s\.svg" % exchange, r.text
    )
    return fallback[0] if fallback else None


tests = [
    # US stocks
    ("NASDAQ", "NVDA"),
    ("NYSE", "JPM"),
    ("NYSE", "BRK.B"),
    ("NASDAQ", "TSLA"),
    ("NYSE", "XOM"),
    # US ETFs
    ("NASDAQ", "QQQ"),
    ("AMEX", "GLD"),
    ("NYSE", "VTI"),
    ("AMEX", "IWM"),
    ("CBOE", "ARKK"),
    # UAE / Gulf
    ("DFM", "EMAAR"),
    ("DFM", "DIB"),
    ("ADX", "FAB"),
    ("ADX", "ADNOCDIST"),
    ("TADAWUL", "2222"),
    # Europe
    ("LSE", "AZN"),
    ("LSE", "SHEL"),
    ("XETR", "SAP"),
    ("EURONEXT", "ASML"),
    ("SIX", "NESN"),
    # Asia-Pacific
    ("TSE", "7203"),
    ("HKEX", "700"),
    ("KRX", "005930"),
    ("NSE", "RELIANCE"),
    ("ASX", "BHP"),
    # Other
    ("BMFBOVESPA", "VALE3"),
    ("TSX", "SHOP"),
    ("JSE", "NPN"),
    ("NASDAQ", "NVO"),
    ("NYSE", "TSM"),
]
for ex, t in tests:
    try:
        print(f"{ex}:{t:<10} {get_logo(ex, t)}")
    except Exception as e:
        print(f"{ex}:{t:<10} ERROR {e}")
