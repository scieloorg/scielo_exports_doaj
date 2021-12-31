import json

from datetime import datetime
from exporter import utils
from unittest import TestCase
from xylose import scielodocument


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


class ISSNTest(TestCase):
    def setUp(self):
        with open("tests/fixtures/fake-article.json") as fp:
            article_json = json.load(fp)
        self.article = scielodocument.Article(article_json)
        self.managed_issns = utils.extract_issns_from_file("tests/fixtures/issns.txt")

    def test_is_valid_issn_returns_true(self):
        is_valid = utils.is_valid_issn('0123-4567')
        self.assertTrue(is_valid)

    def test_is_valid_issn_returns_false(self):
        is_valid = utils.is_valid_issn('01234567')
        self.assertFalse(is_valid)

    def test_is_managed_journal_document_returns_true(self):
        doc_issns = utils.extract_issns_from_document(self.article)
        is_managed = utils.is_managed_journal_document(doc_issns, self.managed_issns)
        self.assertTrue(is_managed)

    def test_is_managed_journal_document_returns_false(self):
        doc_issns = set(['0000-1112'])

        is_managed = utils.is_managed_journal_document(doc_issns, self.managed_issns)
        self.assertFalse(is_managed)

    def test_extract_issns_from_document_returns_set(self):
        doc_issns = utils.extract_issns_from_document(self.article)
        expected_issns = set(['0001-3765', '1678-2690'])
        self.assertSetEqual(doc_issns, expected_issns)

    def test_extract_issns_from_document_invalid_document_returns_empty_set(self):
        invalid_document = {}
        doc_issns = utils.extract_issns_from_document(invalid_document)
        self.assertSetEqual(doc_issns, set())

    def test_extract_issns_from_file_raises_file_error(self):
        invalid_path = "tests/fixtures/non_existent_file.txt"
        with self.assertRaises(utils.ISSNFileError) as exec_info:
            issns = utils.extract_issns_from_file(invalid_path)
