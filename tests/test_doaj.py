from unittest import TestCase, mock

import vcr
from xylose import scielodocument

from exporter import AMClient, doaj


class DOAJDocumentTest(TestCase):
    @vcr.use_cassette("tests/fixtures/vcr_cassettes/S0100-19651998000200002.yml")
    def setUp(self):
        client = AMClient()
        document = client.document(collection="scl", pid="S0100-19651998000200002")
        self.article = scielodocument.Article(document)
        self.doaj_document = doaj.DOAJDocument(article=self.article)

    def test_bibjson_author(self):
        for author in self.article.data.authors:
            with self.subTest(author=author):
                author_name = [author.get('given_names', ''), author.get('surname', '')]
                self.assertIn(
                    {"name": author_name},
                    self.doaj_document.bibjson_author,
                )


class DOAJDocumentErrorsTest(TestCase):
    @vcr.use_cassette("tests/fixtures/vcr_cassettes/S0100-19651998000200002.yml")
    def setUp(self):
        client = AMClient()
        document = client.document(collection="scl", pid="S0100-19651998000200002")
        self.article = scielodocument.Article(document)

    def test_raises_exception_if_no_author(self):
        del self.article.data.data["article"]["v10"]    # v10: authors
        with self.assertRaises(doaj.DOAJDocumentNoAuthorsException) as exc:
            doaj.DOAJDocument(article=self.article)
