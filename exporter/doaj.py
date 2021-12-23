import typing

import requests
from xylose import scielodocument

from exporter import interfaces, config, utils


class DOAJExporterXyloseArticleNoRequestData(Exception):
    pass


class DOAJExporterXyloseArticleNoAuthorsException(Exception):
    pass


class DOAJExporterXyloseArticleNoJournalRequiredFields(Exception):
    pass


class DOAJExporterXyloseArticleNoISSNException(Exception):
    pass


class DOAJExporterXyloseArticleNoDOINorlink(Exception):
    pass


class DOAJExporterXyloseArticle(interfaces.IndexExporterInterface):
    def __init__(self, article: scielodocument.Article, now: callable = utils.utcnow()):
        self._set_api_config()
        self._article = article
        self._now = now
        self._data = {}
        if article.data.get("doaj_id"):
            self._data["id"] = article.data["doaj_id"]

    def _set_api_config(self):
        for attr, envvar in [("_api_url", "DOAJ_API_URL"), ("_api_key", "DOAJ_API_KEY")]:
            config_var = config.get(envvar)
            if not config_var:
                raise DOAJExporterXyloseArticleNoRequestData(f"No {envvar} set")
            setattr(self, attr, config_var)

        self.crud_article_put_url = f"{self._api_url}articles"
        self.search_journal_url = f"{self._api_url}search/journals/"
        self.bulk_articles_url = f"{self._api_url}bulk/articles"

    @property
    def crud_article_url(self):
        try:
            url = f'{self._api_url}articles/{self._data["id"]}'
        except KeyError:
            raise DOAJExporterXyloseArticleNoRequestData(
                "No DOAJ ID for article"
            ) from None
        else:
            return url

    @property
    def post_request(self) -> dict:
        self._data["created_date"] = self._data["last_updated"] = self._now
        self._data.setdefault("bibjson", {})
        self._set_bibjson_abstract()
        self._set_bibjson_author()
        self._set_bibjson_identifier()
        self._set_bibjson_journal()
        self._set_bibjson_keywords()
        self._set_bibjson_link()
        self._set_bibjson_title()
        return {
            "params": {"api_key": config.get("DOAJ_API_KEY")},
            "json": self._data
        }

    @property
    def get_request(self) -> dict:
        return {
            "params": {"api_key": config.get("DOAJ_API_KEY")},
        }

    @property
    def delete_request(self) -> dict:
        return {
            "params": {"api_key": config.get("DOAJ_API_KEY")},
        }

    def put_request(self, data: dict) -> dict:
        self._data = data
        self._data["last_updated"] = self._now
        self._data.setdefault("bibjson", {})
        self._set_bibjson_abstract()
        self._set_bibjson_author()
        self._set_bibjson_identifier()
        self._set_bibjson_journal()
        self._set_bibjson_keywords()
        self._set_bibjson_link()
        self._set_bibjson_title()
        return {
            "params": {"api_key": config.get("DOAJ_API_KEY")},
            "json": self._data
        }

    def post_response(self, response: dict) -> dict:
        return {
            "index_id": response.get("id"),
            "status": response.get("status"),
        }

    def error_response(self, response: dict) -> str:
        return response.get("error", "")

    def _set_bibjson_abstract(self):
        abstract = self._article.original_abstract()
        if abstract:
            self._data["bibjson"]["abstract"] = abstract

    def _set_bibjson_author(self):
        if not self._article.authors:
            raise DOAJExporterXyloseArticleNoAuthorsException()

        self._data["bibjson"].setdefault("author", [])
        for author in self._article.authors:
            author_name = " ".join(
                [author.get('given_names', ''), author.get('surname', '')]
            )
            self._data["bibjson"]["author"].append({"name": author_name})

    def _get_registered_journal_issn(self):
        for journal_attr in ["electronic_issn", "print_issn"]:
            issn = getattr(self._article.journal, journal_attr)
            if not issn:
                continue

            resp = requests.get(f"{self.search_journal_url}{issn}")
            if resp.status_code != 200 or not resp.json().get("results"):
                continue

            search_result = resp.json()["results"][0]
            bibjson = search_result.get("bibjson", {})
            bibjson_issn = bibjson.get("eissn")
            if bibjson_issn:
                return bibjson_issn, "eissn"
            else:
                return bibjson.get("pissn"), "pissn"
        else:
            raise DOAJExporterXyloseArticleNoISSNException()


    def _set_bibjson_identifier(self):
        issn, issn_type = self._get_registered_journal_issn()
        self._data["bibjson"]["identifier"] = [{"id": issn, "type": issn_type}]

        if self._article.doi:
            self._data["bibjson"]["identifier"].append(
                {"id": self._article.doi, "type": "doi"}
            )

    def _set_bibjson_journal(self):
        journal = {}

        def _set_journal_field(journal, article, field, field_to_set, required=False):
            journal_field = getattr(self._article.journal, field)
            if journal_field:
                journal[field_to_set] = journal_field
            elif not journal_field and required:
                raise DOAJExporterXyloseArticleNoJournalRequiredFields()


        publisher_country = self._article.journal.publisher_country
        if not publisher_country:
            raise DOAJExporterXyloseArticleNoJournalRequiredFields()
        else:
            country_code, __ = publisher_country
            journal["country"] = country_code

        _set_journal_field(journal, self._article, "languages", "language", required=True)
        _set_journal_field(
            journal, self._article, "publisher_name", "publisher", required=True
        )
        _set_journal_field(journal, self._article, "title", "title", required=True)

        self._data["bibjson"]["journal"] = journal

    def _set_bibjson_keywords(self):
        keywords = self._article.keywords()
        if keywords and keywords.get(self._article.original_language()):
            self._data["bibjson"]["keywords"] = keywords[self._article.original_language()]

    def _set_bibjson_link(self):
        MIME_TYPE = {
            "html": "text/html",
            "pdf": "application/pdf",
        }

        fulltexts = self._article.fulltexts()
        if fulltexts:
            self._data["bibjson"].setdefault("link", [])
            for content_type, links in fulltexts.items():
                for __, url in links.items():
                    if url:
                        self._data["bibjson"]["link"].append(
                            {
                                "content_type": MIME_TYPE[content_type],
                                "type": "fulltext",
                                "url": url,
                            }
                        )

        if not (self._data["bibjson"].get("link") or self._article.doi):
            raise DOAJExporterXyloseArticleNoDOINorlink(
                "Documento não possui DOI ou links para texto completo"
            )

    def _set_bibjson_title(self):
        title = self._article.original_title()

        if not title:
            section_code = self._article.section_code
            original_lang = self._article.original_language()
            title = self._article.issue.sections.get(section_code, {}).get(
                original_lang, "Documento sem título"
            )

        self._data["bibjson"]["title"] = title

    def command_function(self):
        pass
