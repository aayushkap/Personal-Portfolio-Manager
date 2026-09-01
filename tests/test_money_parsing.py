import unittest

from app.utils.fin import parse_money
from app.utils.parsers import parse_money_string


class MoneyParsingTests(unittest.TestCase):
    def test_parses_ukw_pound_dividend(self):
        self.assertEqual(parse_money_string("£0.0268"), (0.0268, "GBP"))

    def test_parses_supported_symbol_currencies(self):
        self.assertEqual(parse_money_string("$1.25"), (1.25, "USD"))
        self.assertEqual(parse_money_string("€1.25"), (1.25, "EUR"))
        self.assertEqual(parse_money_string("CA$1.25"), (1.25, "CAD"))

    def test_legacy_money_parser_uses_the_same_currency_rules(self):
        self.assertEqual(parse_money("£0.0268"), (0.0268, "GBP"))
        self.assertEqual(parse_money("GBX 102.80"), (102.8, "GBX"))


if __name__ == "__main__":
    unittest.main()
