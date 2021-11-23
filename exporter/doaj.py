import typing

from xylose import scielodocument

from exporter import interfaces


class DOAJDocumentNoAuthorsException(Exception):
    pass


class DOAJDocument(interfaces.ExporterInterface):
    def __init__(self, article: scielodocument.Article):
        self.data = {}
        self.data.setdefault("bibjson", {})
        self.add_bibjson_author(article)

    @property
    def bibjson_author(self) -> typing.List[dict]:
        return self.data["bibjson"]["author"]

    def add_bibjson_author(self, article: scielodocument.Article):
        if not article.data.authors:
            raise DOAJDocumentNoAuthorsException()

        self.data["bibjson"].setdefault("author", [])
        for author in article.data.authors:
            author_name = [author.get('given_names', ''), author.get('surname', '')]
            self.data["bibjson"]["author"].append({"name": author_name})

    def get_request(self):
        return {}
