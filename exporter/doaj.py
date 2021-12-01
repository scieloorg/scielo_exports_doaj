import typing

from xylose import scielodocument

from exporter import interfaces, config


class DOAJExporterXyloseArticleNoRequestData(Exception):
    pass


class DOAJExporterXyloseArticleNoAuthorsException(Exception):
    pass


class DOAJExporterXyloseArticleNoISSNException(Exception):
    pass


class DOAJExporterXyloseArticle(interfaces.IndexExporterInterface):
    def __init__(self, article: scielodocument.Article):
        self._set_api_config()
        self._data = {}
        self._data.setdefault("bibjson", {})
        self.add_bibjson_author(article)
        self.add_bibjson_identifier(article)
        self.add_bibjson_title(article)

    def _set_api_config(self):
        for attr, envvar in [("_api_url", "DOAJ_API_URL"), ("_api_key", "DOAJ_API_KEY")]:
            config_var = config.get(envvar)
            if not config_var:
                raise DOAJExporterXyloseArticleNoRequestData(f"No {envvar} set")
            setattr(self, attr, config_var)

        self.crud_article_url = f"{self._api_url}articles"

    @property
    def bibjson_author(self) -> typing.List[dict]:
        return self._data["bibjson"]["author"]

    @property
    def bibjson_identifier(self) -> typing.List[dict]:
        return self._data["bibjson"]["identifier"]

    @property
    def bibjson_title(self) -> str:
        return self._data["bibjson"]["title"]

    @property
    def bibjson_title(self) -> str:
        return self._data["bibjson"]["title"]

    @property
    def post_request(self) -> dict:
        return {
            "api_key": config.get("DOAJ_API_KEY"),
            "article_json": self._data
        }

    def post_response(self, response: dict) -> dict:
        return {
            "index_id": response.get("id"),
            "status": response.get("status"),
        }

    def add_bibjson_author(self, article: scielodocument.Article):
        if not article.authors:
            raise DOAJExporterXyloseArticleNoAuthorsException()

        self._data["bibjson"].setdefault("author", [])
        for author in article.authors:
            author_name = [author.get('given_names', ''), author.get('surname', '')]
            self._data["bibjson"]["author"].append({"name": author_name})

    def add_bibjson_identifier(self, article: scielodocument.Article):
        issn = article.journal.any_issn()
        if not issn:
            raise DOAJExporterXyloseArticleNoISSNException()

        if issn == article.journal.electronic_issn:
            issn_type = "eissn"
        else:
            issn_type = "pissn"

        self._data["bibjson"]["identifier"] = [{"id": issn, "type": issn_type}]

        if article.doi:
            self._data["bibjson"]["identifier"].append(
                {"id": article.doi, "type": "doi"}
            )

    def add_bibjson_title(self, article: scielodocument.Article):
        title = article.original_title()
        if (
            not title and
            article.translated_titles() and
            len(article.translated_titles()) != 0
        ):
            item = [(k, v) for k, v in article.translated_titles().items()][0]
            title = item[1]

        if not title:
            section_code = article.section_code
            original_lang = article.original_language()
            title = article.issue.sections.get(section_code, {}).get(
                original_lang, "Documento sem título"
            )

        self._data["bibjson"]["title"] = title

    def export(self):
        pass
