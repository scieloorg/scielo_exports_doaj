from datetime import datetime

from unittest import TestCase

from exporter import utils


class GetValidDatetimeTest(TestCase):
    def test_raises_exception_if_invalid_date(self):
        dates = ["01-01-01", "01-01", "01/01", "01/01/01", "2021-01-01"]
        for date in dates:
            with self.subTest(date=date):
                with self.assertRaises(ValueError) as exc_info:
                    utils.get_valid_datetime(date)
                self.assertEqual(
                    str(exc_info.exception),
                    "Data inválida. Formato esperado: DD-MM-YYYY",
                )

    def test_returns_datetime(self):
        date = utils.get_valid_datetime("01-01-2021")
        self.assertEqual(date, datetime(2021, 1, 1))
