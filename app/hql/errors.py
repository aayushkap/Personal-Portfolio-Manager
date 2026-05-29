class HQLError(Exception):
    """Base HQL error."""


class HQLTickerNotFound(HQLError):
    def __init__(self, ticker: str):
        super().__init__(f"Ticker not found in cache: {ticker}")
        self.ticker = ticker


class HQLFieldError(HQLError):
    def __init__(self, field: str):
        super().__init__(f"Unsupported HQL field: {field}")
        self.field = field


class HQLDataError(HQLError):
    """Raised when source data is malformed or incomplete."""
