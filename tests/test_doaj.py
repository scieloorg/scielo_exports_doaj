from unittest import TestCase, mock

import vcr
from xylose import scielodocument

from exporter import AMClient, doaj, config


class DOAJExporterXyloseArticleTest(TestCase):
    @vcr.use_cassette("tests/fixtures/vcr_cassettes/S0100-19651998000200002.yml")
    @mock.patch.dict("os.environ", {"DOAJ_API_KEY": "doaj-api-key-1234"})
    def setUp(self):
        client = AMClient()
        self.article = client.document(collection="scl", pid="S0100-19651998000200002")
        self.doaj_document = doaj.DOAJExporterXyloseArticle(article=self.article)

    def test_crud_article_url(self):
        self.assertEqual(
            config.get("DOAJ_API_URL") + "articles",
            self.doaj_document.crud_article_url,
        )

    def test_bibjson_author(self):
        for author in self.article.authors:
            with self.subTest(author=author):
                author_name = " ".join(
                    [author.get('given_names', ''), author.get('surname', '')]
                )
                self.assertIn(
                    {"name": author_name},
                    self.doaj_document.bibjson_author,
                )

    def test_bibjson_identifier(self):
        issn = self.article.journal.any_issn()
        if issn == self.article.journal.electronic_issn:
            issn_type = "eissn"
        else:
            issn_type = "pissn"

        self.assertIn(
            {"id": issn, "type": issn_type},
            self.doaj_document.bibjson_identifier,
        )
        self.assertIn(
            {"id": self.article.doi, "type": "doi"},
            self.doaj_document.bibjson_identifier,
        )

    def test_bibjson_title(self):
        title = self.article.original_title()
        if (
            not title and
            self.article.translated_titles() and
            len(self.article.translated_titles()) != 0
        ):
            item = [(k, v) for k, v in self.article.translated_titles().items()][0]
            title = item[1]

        self.assertEqual(
            title, self.doaj_document.bibjson_title
        )

    def test_post_request(self):
        expected = {
            "api_key": config.get("DOAJ_API_KEY"),
            "article_json": self.doaj_document._data
        }
        self.assertEqual(
            expected, self.doaj_document.post_request
        )


@mock.patch.dict("os.environ", {"DOAJ_API_KEY": "doaj-api-key-1234"})
class DOAJExporterXyloseArticleExceptionsTest(TestCase):
    @vcr.use_cassette("tests/fixtures/vcr_cassettes/S0100-19651998000200002.yml")
    def setUp(self):
        client = AMClient()
        self.article = client.document(collection="scl", pid="S0100-19651998000200002")

    @mock.patch.dict("os.environ", {"DOAJ_API_URL": ""})
    def test_raises_exception_if_no_post_url(self):
        with self.assertRaises(doaj.DOAJExporterXyloseArticleNoRequestData) as exc:
            doaj.DOAJExporterXyloseArticle(article=self.article).post_url
        self.assertEqual("No DOAJ_API_URL set", str(exc.exception))

    @mock.patch.dict("os.environ", {"DOAJ_API_KEY": ""})
    def test_raises_exception_if_no_api_key(self):
        with self.assertRaises(doaj.DOAJExporterXyloseArticleNoRequestData) as exc:
            doaj.DOAJExporterXyloseArticle(article=self.article)._api_key
        self.assertEqual("No DOAJ_API_KEY set", str(exc.exception))

    def test_raises_exception_if_no_author(self):
        del self.article.data["article"]["v10"]    # v10: authors
        with self.assertRaises(doaj.DOAJExporterXyloseArticleNoAuthorsException) as exc:
            doaj.DOAJExporterXyloseArticle(article=self.article)

    def test_raises_exception_if_no_eissn_nor_pissn(self):
        self.article.journal.electronic_issn = None
        self.article.journal.print_issn = None
        with self.assertRaises(doaj.DOAJExporterXyloseArticleNoISSNException) as exc:
            doaj.DOAJExporterXyloseArticle(article=self.article)

    def test_sets_as_untitled_document_if_no_article_title(self):
        del self.article.data["article"]["v12"]    # v12: titles
        doaj_document = doaj.DOAJExporterXyloseArticle(article=self.article)

        section_code = self.article.section_code
        original_lang = self.article.original_language()
        # Section title = "Artigos"
        self.assertEqual(
            self.article.issue.sections.get(section_code, {}).get(original_lang),
            doaj_document.bibjson_title,
        )
