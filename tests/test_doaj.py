from unittest import TestCase, mock

import vcr
import requests
from xylose import scielodocument

from exporter import AMClient, doaj, config


class DOAJExporterXyloseArticleTest(TestCase):
    @vcr.use_cassette("tests/fixtures/vcr_cassettes/doaj_exporter.yml")
    @mock.patch.dict("os.environ", {"DOAJ_API_KEY": "doaj-api-key-1234"})
    def setUp(self):
        client = AMClient()
        self.article = client.document(collection="scl", pid="S0100-19651998000200002")
        self.doaj_document = doaj.DOAJExporterXyloseArticle(
            article=self.article, now=self._fake_utcnow()
        )

    def _fake_utcnow(self):
        return "2021-01-01T00:00:00Z"

    def test_crud_article_url(self):
        self.assertEqual(
            config.get("DOAJ_API_URL") + "articles",
            self.doaj_document.crud_article_url,
        )

    def test_created_date(self):
        self.assertEqual(
            self._fake_utcnow(),
            self.doaj_document.created_date,
        )

    def test_last_updated(self):
        self.assertEqual(
            self._fake_utcnow(),
            self.doaj_document.last_updated,
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

    def test_bibjson_journal(self):
        expected = {}
        publisher_country = self.article.journal.publisher_country
        if publisher_country:
            country_code, __ = publisher_country
            expected["country"] = country_code
        languages = self.article.journal.languages
        if languages:
            expected["language"] = languages
        publisher_name = self.article.journal.publisher_name
        if publisher_name:
            expected["publisher"] = publisher_name
        title = self.article.journal.title
        if title:
            expected["title"] = title

        self.assertEqual(expected, self.doaj_document.bibjson_journal)

    def test_bibjson_keywords(self):
        keywords = self.article.keywords()
        expected = []
        for kw_lang in keywords.values():
            expected += kw_lang
        self.assertEqual(
            expected, self.doaj_document.bibjson_keywords
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
            "params": {"api_key": config.get("DOAJ_API_KEY")},
            "json": self.doaj_document._data,
        }
        self.assertEqual(
            expected, self.doaj_document.post_request
        )

    def test_post_response_201(self):
        fake_response = {
          "id": "doaj-1234",
          "location": "",
          "status": "OK",
        }
        expected = {
            "index_id": "doaj-1234",
            "status": "OK",
        }
        self.assertEqual(
            expected, self.doaj_document.post_response(fake_response)
        )

    def test_error_response(self):
        fake_response = {
          "id": "doaj-1234",
          "location": "",
          "status": "FAIL",
          "error": "Fake Field is missing.",
        }
        self.assertEqual(
            "Fake Field is missing.", self.doaj_document.error_response(fake_response)
        )


@mock.patch.dict("os.environ", {"DOAJ_API_KEY": "doaj-api-key-1234"})
class DOAJExporterXyloseArticleExceptionsTest(TestCase):
    @vcr.use_cassette("tests/fixtures/vcr_cassettes/doaj_exporter.yml")
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

    @mock.patch("exporter.doaj.requests.get")
    def test_send_request_get_with_eissn_and_pissn(self, mk_requests_get):
        # MockRequest = mock.Mock(spec=requests.Request, status_code=404)
        # MockRequest.json = mock.Mock(return_value={"results": [{"field": "value"}]})
        mk_requests_get.side_effect = [
            mock.MagicMock(status_code=404), mock.MagicMock(status_code=200),
        ]

        doaj_document = doaj.DOAJExporterXyloseArticle(article=self.article)
        mk_requests_get.assert_has_calls(
            [
                mock.call(
                    f"{doaj_document.search_journal_url}{self.article.journal.electronic_issn}"
                ),
                mock.call(
                    f"{doaj_document.search_journal_url}{self.article.journal.print_issn}"
                ),
            ]
        )

    @mock.patch("exporter.doaj.requests.get")
    def test_set_identifier_with_issn_returned_from_doaj_journals_search(
        self, mk_requests_get
    ):
        MockRequest = mock.Mock(spec=requests.Request, status_code=200)
        MockRequest.json = mock.Mock(
            return_value={
                "results": [
                    {
                        "bibjson": {
                            "eissn": "eissn-returned",
                            "pissn": "pissn-returned",
                        },
                    },
                ],
            }
        )
        mk_requests_get.return_value = MockRequest
        doaj_document = doaj.DOAJExporterXyloseArticle(article=self.article)

        self.assertIn(
            {"id": "eissn-returned", "type": "eissn"}, doaj_document.bibjson_identifier,
        )

    def test_raises_exception_if_no_journal_required_fields(self):
        del self.article.journal.data["v310"]    # v310: publisher_country
        del self.article.journal.data["v350"]    # v350: languages
        del self.article.journal.data["v480"]    # v480: publisher_name
        del self.article.journal.data["v100"]    # v100: title

        with self.assertRaises(
            doaj.DOAJExporterXyloseArticleNoJournalRequiredFields
        ) as exc:
            doaj.DOAJExporterXyloseArticle(article=self.article)

    def test_no_keywords_if_no_article_keywords(self):
        del self.article.data["article"]["v85"]    # v85: keywords
        doaj_document = doaj.DOAJExporterXyloseArticle(article=self.article)

        self.assertIsNone(doaj_document.bibjson_keywords)

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

    def test_error_response_return_empty_str_if_no_error(self):
        doaj_document = doaj.DOAJExporterXyloseArticle(article=self.article)
        fake_response = {
          "id": "doaj-1234",
          "location": "",
          "status": "FAIL",
        }
        self.assertEqual(
            "", doaj_document.error_response(fake_response)
        )
