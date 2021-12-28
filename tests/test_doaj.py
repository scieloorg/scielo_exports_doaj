import json
import re
from unittest import TestCase, mock
from datetime import datetime

import vcr
import requests
from xylose import scielodocument

from exporter import AMClient, doaj, config


class DOAJExporterXyloseArticleTest(TestCase):
    @mock.patch.dict("os.environ", {"DOAJ_API_KEY": "doaj-api-key-1234"})
    def setUp(self):
        with open("tests/fixtures/fake-article.json") as fp:
            article_json = json.load(fp)
        self.article = scielodocument.Article(article_json)
        self.doaj_document = doaj.DOAJExporterXyloseArticle(
            article=self.article, now=self._fake_utcnow()
        )

    def _fake_utcnow(self):
        return "2021-01-01T00:00:00Z"

    def _expected_created_date(self):
        return self._fake_utcnow()

    def _expected_last_updated(self):
        return self._fake_utcnow()

    def _expected_bibjson_abstract(self):
        return self.article.original_abstract()

    def _expected_bibjson_author(self):
        affiliation_institutions = {
            mixed_affiliation["index"]: mixed_affiliation["institution"]
            for mixed_affiliation in self.article.mixed_affiliations
        }

        bibjson_author = []
        for author in self.article.authors:
            _author = {
                "name": " ".join(
                    [author.get("given_names", ""), author.get("surname", "")]
                ),
            }
            affiliation_index = author.get("xref", [""])[0]
            if affiliation_index:
                _author["affiliation"] = affiliation_institutions.get(
                    affiliation_index, ""
                )
            if author.get("orcid", ""):
                _author["orcid_id"] = author["orcid"],
            bibjson_author.append(_author)
        return bibjson_author


    def _expected_bibjson_identifier(self):
        identifier = []
        issn = self.article.journal.any_issn()
        if issn == self.article.journal.electronic_issn:
            issn_type = "eissn"
        else:
            issn_type = "pissn"

        identifier.append({"id": issn, "type": issn_type})
        identifier.append({"id": self.article.doi, "type": "doi"})
        return identifier

    def _expected_issue_number(self):
        issue =  self.article.issue
        label_issue = issue.number.replace("ahead", "") if issue.number else ""

        if issue.supplement_number:
            label_issue += f" suppl {issue.supplement_number}"

        if issue.supplement_volume:
            label_issue += f" suppl {issue.supplement_volume}"

        label_issue = re.compile(r"^0 ").sub("", label_issue)
        label_issue = re.compile(r" 0$").sub("", label_issue)
        return label_issue.strip()

    def _expected_bibjson_journal(self):
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
        volume = self.article.issue.volume
        if volume:
            expected["volume"] = volume
        number = self._expected_issue_number()
        if number:
            expected["number"] = number
        start_page = self.article.start_page
        if start_page:
            expected["start_page"] = start_page
        end_page = self.article.end_page
        if end_page:
            expected["end_page"] = end_page

        return expected

    def _expected_bibjson_keywords(self):
        keywords = self.article.keywords()
        return keywords.get(self.article.original_language())

    def _expected_bibjson_link(self):
        MIME_TYPE = {
            "html": "text/html",
            "pdf": "application/pdf",
        }

        fulltexts = self.article.fulltexts()
        expected = []
        for content_type, links in fulltexts.items():
            for __, url in links.items():
                expected.append(
                    {
                        "content_type": MIME_TYPE[content_type],
                        "type": "fulltext",
                        "url": url,
                    }
                )
        return expected

    def _expected_bibjson_title(self):
        return self.article.original_title()

    def _expected_bibjson_month_and_year(self):
        if self.article.document_publication_date:
            pub_date = datetime.strptime(
                self.article.document_publication_date, "%Y-%m-%d"
            )
        else:
            pub_date = datetime.strptime(
                self.article.issue_publication_date, "%Y-%m"
            )
        return pub_date.month, pub_date.year


class PostDOAJExporterXyloseArticleTest(DOAJExporterXyloseArticleTest):
    def test_id(self):
        self.assertEqual(
            self.article.data["doaj_id"],
            self.doaj_document.id,
        )

    def test_crud_article_put_url(self):
        self.assertEqual(
            config.get("DOAJ_API_URL") + "articles",
            self.doaj_document.crud_article_put_url,
        )

    def test_search_journal_url(self):
        self.assertEqual(
            config.get("DOAJ_API_URL") + "search/journals/",
            self.doaj_document.search_journal_url,
        )

    def test_bulk_articles_url(self):
        self.assertEqual(
            config.get("DOAJ_API_URL") + "bulk/articles",
            self.doaj_document.bulk_articles_url,
        )

    def test_params_request(self):
        self.assertEqual(
            self.doaj_document.params_request,
            {"api_key": config.get("DOAJ_API_KEY")},
        )

    def test_post_request(self):
        expected = {
            "id": self.article.data["doaj_id"],
            "created_date": self._expected_created_date(),
            "last_updated": self._expected_last_updated(),
            "bibjson": {
                "abstract": self._expected_bibjson_abstract(),
                "author": self._expected_bibjson_author(),
                "identifier": self._expected_bibjson_identifier(),
                "journal": self._expected_bibjson_journal(),
                "keywords": self._expected_bibjson_keywords(),
                "link": self._expected_bibjson_link(),
                "title": self._expected_bibjson_title(),
            },
            "es_type": self.article.document_type,
        }
        expected["bibjson"]["month"], expected["bibjson"]["year"] = \
            self._expected_bibjson_month_and_year()
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


class PutDOAJExporterXyloseArticleTest(DOAJExporterXyloseArticleTest):
    def test_id(self):
        self.assertEqual(
            self.article.data["doaj_id"],
            self.doaj_document.id,
        )

    def test_crud_article_url(self):
        self.assertEqual(
            config.get("DOAJ_API_URL") + "articles/" + self.article.data["doaj_id"],
            self.doaj_document.crud_article_url,
        )

    def test_params_request(self):
        self.assertEqual(
            self.doaj_document.params_request,
            {"api_key": config.get("DOAJ_API_KEY")},
        )

    def test_put_request(self):
        fake_get_resp = {
            "id": self.article.data["doaj_id"],
            "created_date": "2020-01-01T00:00:00Z",
            "last_updated": "2020-01-01T00:00:00Z",
            "bibjson": {
                "abstract": "Old abstract",
                "author": [],
                "identifier": [],
                "journal": {
                    "country": "BR",
                    "language": ["pt"],
                    "publisher": "Journal Publisher",
                    "title": "Journal Title",
                },
                "keywords": [],
                "link": [],
                "title": "Article Title",
            },
        }
        expected = {
            "id": self.article.data["doaj_id"],
            "created_date": "2020-01-01T00:00:00Z",
            "last_updated": self._expected_last_updated(),
            "bibjson": {
                "abstract": self._expected_bibjson_abstract(),
                "author": self._expected_bibjson_author(),
                "identifier": self._expected_bibjson_identifier(),
                "journal": self._expected_bibjson_journal(),
                "keywords": self._expected_bibjson_keywords(),
                "link": self._expected_bibjson_link(),
                "title": self._expected_bibjson_title(),
            },
            "es_type": self.article.document_type,
        }
        expected["bibjson"]["month"], expected["bibjson"]["year"] = \
            self._expected_bibjson_month_and_year()
        self.assertEqual(
            expected, self.doaj_document.put_request(fake_get_resp)
        )


class DeleteDOAJExporterXyloseArticleTest(DOAJExporterXyloseArticleTest):
    def test_id(self):
        self.assertEqual(
            self.article.data["doaj_id"],
            self.doaj_document.id,
        )

    def test_crud_article_url(self):
        self.assertEqual(
            config.get("DOAJ_API_URL") + "articles/" + self.article.data["doaj_id"],
            self.doaj_document.crud_article_url,
        )

    def test_params_request(self):
        self.assertEqual(
            self.doaj_document.params_request,
            {"api_key": config.get("DOAJ_API_KEY")},
        )


@mock.patch.dict("os.environ", {"DOAJ_API_KEY": "doaj-api-key-1234"})
class DOAJExporterXyloseArticleExceptionsTestMixin:
    @mock.patch.dict("os.environ", {"DOAJ_API_URL": ""})
    def test_raises_exception_if_no_post_url(self):
        with self.assertRaises(doaj.DOAJExporterXyloseArticleNoRequestData) as exc:
            doaj.DOAJExporterXyloseArticle(article=self.article)
        self.assertEqual("No DOAJ_API_URL set", str(exc.exception))

    @mock.patch.dict("os.environ", {"DOAJ_API_KEY": ""})
    def test_raises_exception_if_no_api_key(self):
        with self.assertRaises(doaj.DOAJExporterXyloseArticleNoRequestData) as exc:
            doaj.DOAJExporterXyloseArticle(article=self.article)
        self.assertEqual("No DOAJ_API_KEY set", str(exc.exception))

    def test_raises_exception_if_no_doaj_id(self):
        self.article.data.pop("doaj_id", None)
        with self.assertRaises(doaj.DOAJExporterXyloseArticleNoRequestData) as exc:
            doaj.DOAJExporterXyloseArticle(article=self.article).crud_article_url
        self.assertEqual("No DOAJ ID for article", str(exc.exception))

    def test_http_request_has_no_abstract_if_no_article_abstract(self):
        del self.article.data["article"]["v83"]    # v83: abstract
        self.doaj_document = doaj.DOAJExporterXyloseArticle(article=self.article)
        req = self.http_request_function()
        self.assertIsNone(req["bibjson"].get("abstract"))

    def test_http_request_raises_exception_if_no_author(self):
        del self.article.data["article"]["v10"]    # v10: authors
        with self.assertRaises(doaj.DOAJExporterXyloseArticleNoAuthorsException) as exc:
            self.doaj_document = doaj.DOAJExporterXyloseArticle(article=self.article)
            self.http_request_function()

    def test_http_request_has_no_author_affiliation_if_no_affiliation(self):
        self.doaj_document = doaj.DOAJExporterXyloseArticle(article=self.article)
        req = self.http_request_function()
        for author in req["bibjson"]["author"]:
            self.assertIsNone(author.get("affiliation"))

    def test_http_request_has_no_author_orcid_id_if_no_orcid(self):
        self.doaj_document = doaj.DOAJExporterXyloseArticle(article=self.article)
        req = self.http_request_function()
        for author in req["bibjson"]["author"]:
            self.assertIsNone(author.get("orcid_id"))

    def test_http_request_raises_exception_if_no_eissn_nor_pissn(self):
        self.article.journal.electronic_issn = None
        self.article.journal.print_issn = None
        with self.assertRaises(doaj.DOAJExporterXyloseArticleNoISSNException) as exc:
            self.doaj_document = doaj.DOAJExporterXyloseArticle(article=self.article)
            self.http_request_function()

    @mock.patch("exporter.doaj.requests.get")
    def test_http_request_send_request_get_with_eissn_and_pissn(self, mk_requests_get):
        mk_requests_get.side_effect = [
            mock.MagicMock(status_code=404), mock.MagicMock(status_code=200),
        ]

        self.doaj_document = doaj.DOAJExporterXyloseArticle(article=self.article)
        self.http_request_function()
        mk_requests_get.assert_has_calls([
            mock.call(
                f"{self.doaj_document.search_journal_url}{self.article.journal.electronic_issn}"
            ),
            mock.call(
                f"{self.doaj_document.search_journal_url}{self.article.journal.print_issn}"
            ),
        ])

    @mock.patch("exporter.doaj.requests.get")
    def test_http_request_set_identifier_with_issn_returned_from_doaj_journals_search(
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
        self.doaj_document = doaj.DOAJExporterXyloseArticle(article=self.article)
        req = self.http_request_function()
        self.assertIn(
            {"id": "eissn-returned", "type": "eissn"},
            req["bibjson"]["identifier"],
        )

    def test_http_request_raises_exception_if_no_journal_required_fields(self):
        del self.article.journal.data["v310"]    # v310: publisher_country
        del self.article.journal.data["v350"]    # v350: languages
        del self.article.journal.data["v480"]    # v480: publisher_name
        del self.article.journal.data["v100"]    # v100: title

        with self.assertRaises(
            doaj.DOAJExporterXyloseArticleNoJournalRequiredFields
        ) as exc:
            self.doaj_document = doaj.DOAJExporterXyloseArticle(article=self.article)
            self.http_request_function()

    def test_http_request_has_no_volume_if_no_article_issue_volume(self):
        del self.article.issue.data["issue"]["v31"]    # v31: volume
        self.doaj_document = doaj.DOAJExporterXyloseArticle(article=self.article)
        req = self.http_request_function()
        self.assertIsNone(req["bibjson"]["journal"].get("volume"))

    def test_http_request_has_no_number_if_no_article_issue_number(self):
        del self.article.issue.data["issue"]["v32"]    # v32: number
        self.doaj_document = doaj.DOAJExporterXyloseArticle(article=self.article)
        req = self.http_request_function()
        self.assertIsNone(req["bibjson"]["journal"].get("number"))

    def test_http_request_has_suppl_volume_if_suppl_volume(self):
        self.article.issue.data["issue"].update({"v131": [{'_': "1"}]})
        self.doaj_document = doaj.DOAJExporterXyloseArticle(article=self.article)
        req = self.http_request_function()
        self.assertEqual(
            req["bibjson"]["journal"]["number"],
            f"{self.article.issue.number} suppl {self.article.issue.supplement_volume}"
        )

    def test_http_request_has_suppl_number_if_suppl_number(self):
        self.article.issue.data["issue"].update({"v132": [{'_': "1"}]})
        self.doaj_document = doaj.DOAJExporterXyloseArticle(article=self.article)
        req = self.http_request_function()
        self.assertEqual(
            req["bibjson"]["journal"]["number"],
            f"{self.article.issue.number} suppl {self.article.issue.supplement_number}"
        )

    def test_http_request_has_no_start_end_page_if_no_article_start_end_page(self):
        del self.article.data["article"]["v14"]    # v14: start and end page
        self.doaj_document = doaj.DOAJExporterXyloseArticle(article=self.article)
        req = self.http_request_function()
        self.assertIsNone(req["bibjson"]["journal"].get("start_page"))
        self.assertIsNone(req["bibjson"]["journal"].get("end_page"))

    def test_http_request_has_no_keywords_if_no_article_keywords(self):
        del self.article.data["article"]["v85"]    # v85: keywords
        self.doaj_document = doaj.DOAJExporterXyloseArticle(article=self.article)
        req = self.http_request_function()
        self.assertIsNone(req["bibjson"].get("keywords"))

    def test_http_request_raises_exception_if_no_doi_nor_fulltexts(self):
        del self.article.data["doi"]
        del self.article.data["article"]["v237"]    # v237: doi
        with mock.patch.object(self.article, "fulltexts") as mk_fulltexts:
            mk_fulltexts.return_value = {
                'html': {"pt": ""},
                'pdf': {"pt": ""},
            }
            with self.assertRaises(
                doaj.DOAJExporterXyloseArticleNoDOINorlink
            ) as exc:
                self.doaj_document = doaj.DOAJExporterXyloseArticle(article=self.article)
                self.http_request_function()
            self.assertEqual(
                str(exc.exception),
                "Documento não possui DOI ou links para texto completo",
            )

    def test_http_request_sets_as_untitled_document_if_no_article_title(self):
        del self.article.data["article"]["v12"]    # v12: titles
        self.doaj_document = doaj.DOAJExporterXyloseArticle(article=self.article)

        section_code = self.article.section_code
        original_lang = self.article.original_language()
        # Section title = "Artigos"
        req = self.http_request_function()
        self.assertEqual(
            self.article.issue.sections.get(section_code, {}).get(original_lang),
            req["bibjson"]["title"],
        )

    def test_http_request_no_month_if_no_article_month(self):
        if self.article.data["article"].get("v223"):    # v223: document publication date
            self.article.data["article"].pop("v223")
        self.doaj_document = doaj.DOAJExporterXyloseArticle(article=self.article)

        req = self.http_request_function()
        self.assertIsNone(req["bibjson"].get("month"))

    def test_http_request_undefined_type_if_no_document_type(self):
        del self.article.data["article"]["v71"]    # v71: document type
        self.doaj_document = doaj.DOAJExporterXyloseArticle(article=self.article)

        req = self.http_request_function()
        self.assertEqual(req["es_type"], "undefined")

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


@mock.patch.dict("os.environ", {"DOAJ_API_KEY": "doaj-api-key-1234"})
class PostDOAJExporterXyloseArticleExceptionsTest(
    DOAJExporterXyloseArticleExceptionsTestMixin, TestCase,
):
    @vcr.use_cassette("tests/fixtures/vcr_cassettes/doaj_exporter.yml")
    def setUp(self):
        client = AMClient()
        self.article = client.document(collection="scl", pid="S0100-19651998000200002")

    def http_request_function(self):
        return self.doaj_document.post_request


@mock.patch.dict("os.environ", {"DOAJ_API_KEY": "doaj-api-key-1234"})
class PutDOAJExporterXyloseArticleExceptionsTest(
    DOAJExporterXyloseArticleExceptionsTestMixin, TestCase,
):
    @vcr.use_cassette("tests/fixtures/vcr_cassettes/doaj_exporter.yml")
    def setUp(self):
        client = AMClient()
        self.article = client.document(collection="scl", pid="S0100-19651998000200002")
        self.article.data["doaj_id"] = "doaj-id-123456"
        self.fake_get_resp = {
            "id": self.article.data["doaj_id"],
            "created_date": "2020-01-01T00:00:00Z",
            "last_updated": "2020-01-01T00:00:00Z",
            "bibjson": {
                "author": [],
                "identifier": [],
                "journal": {
                    "country": "BR",
                    "language": ["pt"],
                    "publisher": "Journal Publisher",
                    "title": "Journal Title",
                },
                "link": [],
                "title": "Article Title",
            },
        }

    def http_request_function(self):
        return self.doaj_document.put_request(self.fake_get_resp)
