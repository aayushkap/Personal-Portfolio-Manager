from .ohlc import _set_ohlc


class Scraper:
    """
    Scrape & write to Cache / DB
    """

    def __init__(self):
        pass

    async def set_ohlc(self, exchange: str, symbol: str):
        return await _set_ohlc(exchange, symbol)

    async def get_fundamentals(self):
        pass


import asyncio


async def main():
    obj = Scraper()
    print(await obj.get_ohlc("ADX", "IHC"))


if __name__ == "__main__":
    asyncio.run(main())
