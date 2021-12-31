import tempfile
import pathlib
import json
import shutil
from unittest import TestCase, mock
from datetime import datetime, timedelta

import vcr
import articlemeta.client as articlemeta_client
import requests
from xylose import scielodocument

from exporter import (
    AMClient,
    process_extracted_documents,
    process_documents_in_bulk,
    doaj,
)
from exporter.main import (
    ArticleMetaDocumentNotFound,
    InvalidExporterInitData,
    IndexExporterHTTPError,
    OriginDataFilterError,
    XyloseArticleExporterAdapter,
    XyloseArticlesListExporterAdapter,
    process_document,
    articlemeta_parser,
    main_exporter,
)


class AMClientTest(TestCase):
    def make_client(self, connection:str=None, domain:str=None) -> AMClient:
        params = {}
        if connection:
            params["connection"] = connection
        if domain:
            params["domain"] = domain

        if params:
            return AMClient(**params)

        return AMClient()

    def test_with_no_specific_client(self):
        self.client = self.make_client()
        self.assertIsInstance(self.client._client, articlemeta_client.RestfulClient)

    def test_with_restful_client(self):
        self.client = self.make_client("restful")
        self.assertIsInstance(self.client._client, articlemeta_client.RestfulClient)

    def test_with_thrift_client(self):
        self.client = self.make_client("thrift")
        self.assertIsInstance(self.client._client, articlemeta_client.ThriftClient)

    @vcr.use_cassette("tests/fixtures/vcr_cassettes/S0100-19651998000200002.yml")
    def test_get_document(self):
        self.client = self.make_client()
        document = self.client.document(collection="scl", pid="S0100-19651998000200002")
        self.assertIsInstance(document, scielodocument.Article)
        self.assertEqual(document.collection_acronym, "scl")
        self.assertEqual(document.data["article"]["code"], "S0100-19651998000200002")

    @mock.patch("exporter.main.articlemeta_client.RestfulClient.documents_by_identifiers")
    def test_get_documents_identifiers_calls_documents_by_identifiers(
        self, mk_documents_by_identifiers
    ):
        self.client = self.make_client()
        documents = self.client.documents_identifiers(
            collection="scl", from_date=datetime(2021, 8, 2), until_date=datetime(2021, 8, 2)
        )
        mk_documents_by_identifiers.assert_called_once_with(
            collection="scl",
            from_date="2021-08-02",
            until_date="2021-08-02",
            only_identifiers=True,
        )

    @mock.patch("exporter.main.articlemeta_client.RestfulClient.documents_by_identifiers")
    def test_get_documents_identifiers_calls_documents_by_identifiers_with_collection(
        self, mk_documents_by_identifiers
    ):
        self.client = self.make_client()
        documents = self.client.documents_identifiers(collection="scl")
        mk_documents_by_identifiers.assert_called_once_with(
            collection="scl", only_identifiers=True,
        )

    @mock.patch("exporter.main.articlemeta_client.RestfulClient.documents_by_identifiers")
    def test_get_documents_identifiers_calls_documents_by_identifiers_with_from_date(
        self, mk_documents_by_identifiers
    ):
        self.client = self.make_client()
        documents = self.client.documents_identifiers(from_date=datetime(2021, 8, 2))
        mk_documents_by_identifiers.assert_called_once_with(
            from_date="2021-08-02", only_identifiers=True,
        )

    @mock.patch("exporter.main.articlemeta_client.RestfulClient.documents_by_identifiers")
    def test_get_documents_identifiers_calls_documents_by_identifiers_with_until_date(
        self, mk_documents_by_identifiers
    ):
        self.client = self.make_client()
        documents = self.client.documents_identifiers(until_date=datetime(2021, 8, 2))
        mk_documents_by_identifiers.assert_called_once_with(
            until_date="2021-08-02", only_identifiers=True,
        )

    @vcr.use_cassette(
        "tests/fixtures/vcr_cassettes/documents-identifiers.yml",
        record_mode="new_episodes",
    )
    def test_get_documents_identifiers_from_collection_from_and_until_dates(self):
        self.client = self.make_client()
        documents = self.client.documents_identifiers(
            collection="scl", from_date=datetime(2021, 8, 2), until_date=datetime(2021, 8, 2)
        )
        self.assertIsNotNone(documents)
        docs = [document["collection"] for document in documents]
        self.assertEqual(docs[0], "scl")


class XyloseArticleExporterAdapterTestMixin:
    def test_raises_exception_if_invalid_index(self):
        with self.assertRaises(InvalidExporterInitData) as exc:
            article_exporter = XyloseArticleExporterAdapter(
                index="abc", command=self.index_command, article=self.article
            )
        self.assertEqual(str(exc.exception), "Index informado inválido: abc")

    @mock.patch.dict("os.environ", {"DOAJ_API_KEY": "doaj-api-key-1234"})
    def test_raises_exception_if_invalid_command(self):
        with self.assertRaises(InvalidExporterInitData) as exc:
            article_exporter = XyloseArticleExporterAdapter(
                index=self.index, command="abc", article=self.article
            )
        self.assertEqual(str(exc.exception), "Comando informado inválido: abc")


class ExportXyloseArticleExporterAdapterTest(
    XyloseArticleExporterAdapterTestMixin, TestCase,
):
    index = "doaj"
    index_command = "export"

    @vcr.use_cassette("tests/fixtures/vcr_cassettes/S0100-19651998000200002.yml")
    def setUp(self):
        client = AMClient()
        self.article = client.document(collection="scl", pid="S0100-19651998000200002")

    @mock.patch.dict("os.environ", {"DOAJ_API_KEY": "doaj-api-key-1234"})
    @mock.patch("exporter.main.requests")
    def test_export_calls_requests_post_to_doaj_api_with_doaj_post_request(
        self, mk_requests
    ):
        with mock.patch(
            "exporter.doaj.DOAJExporterXyloseArticle.post_request",
            new_callable=mock.PropertyMock,
        ) as mk_post_request:
            mk_post_request.return_value = {"field": "value"}
            article_exporter = XyloseArticleExporterAdapter(
                index=self.index, command=self.index_command, article=self.article,
            )
            article_exporter.command_function()
            mk_requests.post.assert_called_once_with(
                url=article_exporter.index_exporter.crud_article_put_url,
                params=article_exporter.index_exporter.params_request,
                json={"field": "value"},
            )

    @mock.patch.dict("os.environ", {"DOAJ_API_KEY": "doaj-api-key-1234"})
    @mock.patch("exporter.main.requests")
    def test_export_raises_exception_with_json_error_if_post_raises_400_http_error(
        self, mk_requests
    ):
        mock_resp = mock.Mock(status_code=400)
        mock_resp.raise_for_status.side_effect = requests.exceptions.HTTPError(
            "HTTP Error"
        )
        mk_requests.post.return_value = mock_resp
        mk_requests.post.return_value.json.return_value = {
            "id": "doaj-id",
            "error": "wrong field.",
        }

        article_exporter: doaj.DOAJExporterXyloseArticle = XyloseArticleExporterAdapter(
            index=self.index, command=self.index_command, article=self.article
        )
        with self.assertRaises(IndexExporterHTTPError) as exc:
            article_exporter.command_function()
        self.assertEqual(
            "Erro na exportação ao doaj: HTTP Error. wrong field.", str(exc.exception)
        )

    @mock.patch.dict("os.environ", {"DOAJ_API_KEY": "doaj-api-key-1234"})
    @mock.patch("exporter.main.requests")
    def test_export_raises_exception_if_post_raises_http_error(self, mk_requests):
        mock_resp = mock.Mock()
        mock_resp.raise_for_status.side_effect = requests.exceptions.HTTPError(
            "HTTP Error"
        )
        mk_requests.post.return_value = mock_resp

        article_exporter: doaj.DOAJExporterXyloseArticle = XyloseArticleExporterAdapter(
            index=self.index, command=self.index_command, article=self.article
        )
        with self.assertRaises(IndexExporterHTTPError) as exc:
            article_exporter.command_function()
        self.assertEqual(
            "Erro na exportação ao doaj: HTTP Error.", str(exc.exception)
        )

    @mock.patch.dict("os.environ", {"DOAJ_API_KEY": "doaj-api-key-1234"})
    @mock.patch("exporter.main.requests")
    def test_export_returns_exporter_post_response(self, mk_requests):
        mk_requests.post.return_value.json.return_value = {
            "id": "doaj-id",
            "location": "br",
            "status": "OK",
        }
        article_exporter: doaj.DOAJExporterXyloseArticle = XyloseArticleExporterAdapter(
            index=self.index, command=self.index_command, article=self.article
        )
        ret = article_exporter.command_function()
        self.assertEqual(
            ret,
            {
                "pid": self.article.data["code"],
                "index_id": "doaj-id",
                "status": "OK",
            }
        )


class UpdateXyloseArticleExporterAdapterTest(
    XyloseArticleExporterAdapterTestMixin, TestCase,
):
    index = "doaj"
    index_command = "update"

    @vcr.use_cassette("tests/fixtures/vcr_cassettes/S0100-19651998000200002.yml")
    def setUp(self):
        client = AMClient()
        self.article = client.document(collection="scl", pid="S0100-19651998000200002")
        self.article.data["doaj_id"] = "doaj-id-123456"

    @mock.patch.dict("os.environ", {"DOAJ_API_KEY": "doaj-api-key-1234"})
    @mock.patch("exporter.main.requests")
    def test_update_calls_requests_get_to_doaj_api_with_doaj_get_request(
        self, mk_requests
    ):
        article_exporter = XyloseArticleExporterAdapter(
            index=self.index, command=self.index_command, article=self.article
        )
        crud_article_url = article_exporter.index_exporter.crud_article_url

        article_exporter.command_function()
        mk_requests.get.assert_called_once_with(
            url=crud_article_url,
            params=article_exporter.params_request,
        )

    @mock.patch.dict("os.environ", {"DOAJ_API_KEY": "doaj-api-key-1234"})
    @mock.patch("exporter.main.requests")
    def test_update_raises_exception_if_get_raises_http_error(self, mk_requests):
        mock_resp = mock.Mock()
        mock_resp.raise_for_status.side_effect = requests.exceptions.HTTPError(
            "HTTP Error"
        )
        mk_requests.get.return_value = mock_resp

        article_exporter = XyloseArticleExporterAdapter(
            index=self.index, command=self.index_command, article=self.article
        )
        with self.assertRaises(IndexExporterHTTPError) as exc:
            article_exporter.command_function()
        self.assertEqual(
            "Erro na consulta ao doaj: HTTP Error.", str(exc.exception)
        )

    @mock.patch.dict("os.environ", {"DOAJ_API_KEY": "doaj-api-key-1234"})
    @mock.patch("exporter.main.requests")
    @mock.patch("exporter.main.doaj.DOAJExporterXyloseArticle.put_request")
    def test_update_calls_doaj_put_request_with_doaj_get_response(
        self, mk_put_request, mk_requests,
    ):
        mock_resp = mock.Mock()
        mk_requests.get.return_value = mk_requests.put.return_value = mock_resp
        mk_requests.get.return_value.json.return_value = {
            "id": "doaj-id",
            "field": "value",
        }

        article_exporter = XyloseArticleExporterAdapter(
            index=self.index, command=self.index_command, article=self.article
        )
        article_exporter.command_function()
        mk_put_request.assert_called_once_with(
            {
                "id": "doaj-id",
                "field": "value",
            },
        )

    @mock.patch.dict("os.environ", {"DOAJ_API_KEY": "doaj-api-key-1234"})
    @mock.patch("exporter.main.requests")
    @mock.patch("exporter.main.doaj.DOAJExporterXyloseArticle.put_request")
    def test_update_calls_requests_put_to_doaj_api_with_doaj_put_request(
        self, mk_put_request, mk_requests,
    ):
        mock_resp = mock.Mock()
        mk_requests.get.return_value = mk_requests.put.return_value = mock_resp
        mk_put_request.return_value = {
            "id": "doaj-id",
            "field": "value",
        }

        article_exporter = XyloseArticleExporterAdapter(
            index=self.index, command=self.index_command, article=self.article
        )
        crud_article_url = article_exporter.index_exporter.crud_article_url

        article_exporter.command_function()
        mk_requests.put.assert_called_once_with(
            url=crud_article_url,
            params=article_exporter.params_request,
            json={
                "id": "doaj-id",
                "field": "value",
            },
        )

    @mock.patch.dict("os.environ", {"DOAJ_API_KEY": "doaj-api-key-1234"})
    @mock.patch("exporter.main.requests")
    @mock.patch("exporter.main.doaj.DOAJExporterXyloseArticle.put_request")
    def test_update_raises_exception_with_json_error_if_put_raises_400_http_error(
        self, mk_put_request, mk_requests,
    ):
        mock_put_resp = mock.Mock(status_code=400)
        mock_put_resp.raise_for_status.side_effect = requests.exceptions.HTTPError(
            "HTTP Error"
        )
        mk_requests.put.return_value = mock_put_resp
        mk_requests.put.return_value.json.return_value = {
            "id": "doaj-id",
            "error": "wrong field.",
        }

        article_exporter = XyloseArticleExporterAdapter(
            index=self.index, command=self.index_command, article=self.article
        )
        with self.assertRaises(IndexExporterHTTPError) as exc:
            article_exporter.command_function()
        self.assertEqual(
            "Erro ao atualizar o doaj: HTTP Error. wrong field.", str(exc.exception)
        )

    @mock.patch.dict("os.environ", {"DOAJ_API_KEY": "doaj-api-key-1234"})
    @mock.patch("exporter.main.requests")
    @mock.patch("exporter.main.doaj.DOAJExporterXyloseArticle.put_request")
    def test_update_raises_exception_if_put_raises_http_error(
        self, mk_put_request, mk_requests,
    ):
        mock_put_resp = mock.Mock()
        mock_put_resp.raise_for_status.side_effect = requests.exceptions.HTTPError(
            "HTTP Error"
        )
        mk_requests.put.return_value = mock_put_resp

        article_exporter = XyloseArticleExporterAdapter(
            index=self.index, command=self.index_command, article=self.article
        )
        with self.assertRaises(IndexExporterHTTPError) as exc:
            article_exporter.command_function()
        self.assertEqual(
            "Erro ao atualizar o doaj: HTTP Error.", str(exc.exception)
        )

    @mock.patch.dict("os.environ", {"DOAJ_API_KEY": "doaj-api-key-1234"})
    @mock.patch("exporter.main.requests")
    @mock.patch("exporter.main.doaj.DOAJExporterXyloseArticle.put_request")
    def test_update_returns_response(
        self, mk_put_request, mk_requests,
    ):
        mock_put_resp = mock.Mock()
        mk_requests.put.return_value = mock_put_resp

        article_exporter = XyloseArticleExporterAdapter(
            index=self.index, command=self.index_command, article=self.article
        )
        ret = article_exporter.command_function()
        self.assertEqual(
            ret,
            {
                "pid": self.article.data["code"],
                "status": "UPDATED",
            }
        )


class GetXyloseArticleExporterAdapterTest(
    XyloseArticleExporterAdapterTestMixin, TestCase,
):
    index = "doaj"
    index_command = "get"

    @vcr.use_cassette("tests/fixtures/vcr_cassettes/S0100-19651998000200002.yml")
    def setUp(self):
        client = AMClient()
        self.article = client.document(collection="scl", pid="S0100-19651998000200002")
        self.article.data["doaj_id"] = "doaj-id-123456"

    @mock.patch.dict("os.environ", {"DOAJ_API_KEY": "doaj-api-key-1234"})
    @mock.patch("exporter.main.requests")
    def test_get_calls_requests_get_to_doaj_api_with_doaj_get_request(
        self, mk_requests
    ):
        article_exporter = XyloseArticleExporterAdapter(
            index=self.index, command=self.index_command, article=self.article
        )
        crud_article_url = article_exporter.index_exporter.crud_article_url

        article_exporter.command_function()
        mk_requests.get.assert_called_once_with(
            url=crud_article_url,
            params=article_exporter.params_request,
        )

    @mock.patch.dict("os.environ", {"DOAJ_API_KEY": "doaj-api-key-1234"})
    @mock.patch("exporter.main.requests")
    def test_get_raises_exception_if_get_raises_http_error(self, mk_requests):
        mock_resp = mock.Mock()
        mock_resp.raise_for_status.side_effect = requests.exceptions.HTTPError(
            "HTTP Error"
        )
        mk_requests.get.return_value = mock_resp

        article_exporter = XyloseArticleExporterAdapter(
            index=self.index, command=self.index_command, article=self.article
        )
        with self.assertRaises(IndexExporterHTTPError) as exc:
            article_exporter.command_function()
        self.assertEqual(
            "Erro na consulta ao doaj: HTTP Error.", str(exc.exception)
        )

    @mock.patch.dict("os.environ", {"DOAJ_API_KEY": "doaj-api-key-1234"})
    @mock.patch("exporter.main.requests")
    def test_get_returns_response(
        self, mk_requests,
    ):
        mock_resp = mock.Mock()
        mk_requests.get.return_value = mock_resp
        mk_requests.get.return_value.json.return_value = {
            "id": "doaj-id",
            "field": "value",
        }

        article_exporter = XyloseArticleExporterAdapter(
            index=self.index, command=self.index_command, article=self.article
        )
        ret = article_exporter.command_function()
        self.assertEqual(
            ret,
            { "pid": self.article.data["code"], "id": "doaj-id", "field": "value" },
        )


class DeleteXyloseArticleExporterAdapterTest(
    XyloseArticleExporterAdapterTestMixin, TestCase,
):
    index = "doaj"
    index_command = "delete"

    @vcr.use_cassette("tests/fixtures/vcr_cassettes/S0100-19651998000200002.yml")
    def setUp(self):
        client = AMClient()
        self.article = client.document(collection="scl", pid="S0100-19651998000200002")
        self.article.data["doaj_id"] = "doaj-id-123456"

    @mock.patch.dict("os.environ", {"DOAJ_API_KEY": "doaj-api-key-1234"})
    @mock.patch("exporter.main.requests")
    def test_delete_calls_requests_delete_to_doaj_api_with_doaj_delete_request(
        self, mk_requests
    ):
        article_exporter = XyloseArticleExporterAdapter(
            index=self.index, command=self.index_command, article=self.article
        )
        crud_article_url = article_exporter.index_exporter.crud_article_url

        article_exporter.command_function()
        mk_requests.delete.assert_called_once_with(
            url=crud_article_url,
            params=article_exporter.params_request,
        )

    @mock.patch.dict("os.environ", {"DOAJ_API_KEY": "doaj-api-key-1234"})
    @mock.patch("exporter.main.requests")
    def test_delete_raises_exception_if_delete_raises_http_error(self, mk_requests):
        mock_resp = mock.Mock()
        mock_resp.raise_for_status.side_effect = requests.exceptions.HTTPError(
            "HTTP Error"
        )
        mk_requests.delete.return_value = mock_resp

        article_exporter = XyloseArticleExporterAdapter(
            index=self.index, command=self.index_command, article=self.article
        )
        with self.assertRaises(IndexExporterHTTPError) as exc:
            article_exporter.command_function()
        self.assertEqual(
            "Erro ao deletar no doaj: HTTP Error.", str(exc.exception)
        )

    @mock.patch.dict("os.environ", {"DOAJ_API_KEY": "doaj-api-key-1234"})
    @mock.patch("exporter.main.requests")
    def test_delete_returns_response(
        self, mk_requests,
    ):
        mock_delete_resp = mock.Mock()
        mk_requests.delete.return_value = mock_delete_resp

        article_exporter = XyloseArticleExporterAdapter(
            index=self.index, command=self.index_command, article=self.article
        )
        ret = article_exporter.command_function()
        self.assertEqual(
            ret,
            {
                "pid": self.article.data["code"],
                "status": "DELETED",
            }
        )


class XyloseArticlesListExporterAdapterTestMixin:
    def test_raises_exception_if_invalid_index(self):
        with self.assertRaises(InvalidExporterInitData) as exc:
            articles_exporter = XyloseArticlesListExporterAdapter(
                index="abc", command=self.index_command, articles=set(self.articles)
            )
        self.assertEqual(str(exc.exception), "Index informado inválido: abc")

    @mock.patch.dict("os.environ", {"DOAJ_API_KEY": "doaj-api-key-1234"})
    def test_raises_exception_if_invalid_command(self):
        for command in ["put", "get"]:
            with self.subTest(command=command):
                with self.assertRaises(InvalidExporterInitData) as exc:
                    articles_exporter = XyloseArticlesListExporterAdapter(
                        index=self.index, command=command, articles=set(self.articles)
                    )
                self.assertEqual(
                    str(exc.exception), f"Comando informado inválido: {command}"
                )


class PostXyloseArticlesListExporterAdapterTest(
    XyloseArticlesListExporterAdapterTestMixin, TestCase,
):
    index = "doaj"
    index_command = "export"

    def setUp(self):
        with open("tests/fixtures/full-articles.json") as fp:
            articles_json = json.load(fp)
        self.doaj_ids = [f"doaj-id-{num}" for num in range(1, 4)]
        self.articles = [
            scielodocument.Article(article_json)
            for article_json in articles_json
        ]

    @mock.patch.dict("os.environ", {"DOAJ_API_KEY": "doaj-api-key-1234"})
    @mock.patch("exporter.main.requests")
    def test_export_calls_requests_post_to_doaj_api_with_doaj_post_request(
        self, mk_requests
    ):
        with mock.patch(
            "exporter.doaj.DOAJExporterXyloseArticle.post_request",
            new_callable=mock.PropertyMock,
        ) as mk_post_request:
            mk_post_request.side_effect = [{"id": doaj_id} for doaj_id in self.doaj_ids]
            articles_exporter = XyloseArticlesListExporterAdapter(
                index=self.index, command=self.index_command, articles=set(self.articles),
            )
            articles_exporter.command_function()
            mk_requests.post.assert_called_once_with(
                url=articles_exporter.bulk_articles_url,
                params=articles_exporter.params_request,
                json=[{"id": doaj_id} for doaj_id in self.doaj_ids],
            )

    @mock.patch.dict("os.environ", {"DOAJ_API_KEY": "doaj-api-key-1234"})
    @mock.patch("exporter.main.requests")
    def test_export_raises_exception_with_json_error_if_post_raises_400_http_error(
        self, mk_requests
    ):
        mock_resp = mock.Mock(status_code=400)
        mock_resp.raise_for_status.side_effect = requests.exceptions.HTTPError(
            "HTTP Error"
        )
        mk_requests.post.return_value = mock_resp
        mk_requests.post.return_value.json.return_value = {
            "id": "doaj-id",
            "error": "wrong field.",
        }

        articles_exporter = XyloseArticlesListExporterAdapter(
            index=self.index, command=self.index_command, articles=set(self.articles)
        )
        with self.assertRaises(IndexExporterHTTPError) as exc:
            articles_exporter.command_function()
        self.assertEqual(
            "Erro na exportação ao doaj: HTTP Error. wrong field.", str(exc.exception)
        )

    @mock.patch.dict("os.environ", {"DOAJ_API_KEY": "doaj-api-key-1234"})
    @mock.patch("exporter.main.requests")
    def test_export_raises_exception_if_post_raises_http_error(self, mk_requests):
        mock_resp = mock.Mock()
        mock_resp.raise_for_status.side_effect = requests.exceptions.HTTPError(
            "HTTP Error"
        )
        mk_requests.post.return_value = mock_resp

        articles_exporter = XyloseArticlesListExporterAdapter(
            index=self.index, command=self.index_command, articles=set(self.articles)
        )
        with self.assertRaises(IndexExporterHTTPError) as exc:
            articles_exporter.command_function()
        self.assertEqual(
            "Erro na exportação ao doaj: HTTP Error.", str(exc.exception)
        )

    @mock.patch.dict("os.environ", {"DOAJ_API_KEY": "doaj-api-key-1234"})
    @mock.patch("exporter.main.requests")
    def test_export_returns_exporter_post_response(self, mk_requests):
        mk_requests.post.return_value.json.return_value = [
            {
                "id": doaj_id,
                "location": "br",
                "status": "OK",
            }
            for doaj_id in self.doaj_ids
        ]
        articles = set(self.articles)
        articles_exporter = XyloseArticlesListExporterAdapter(
            index=self.index, command=self.index_command, articles=articles
        )
        ret = articles_exporter.command_function()
        self.assertEqual(
            ret,
            [
                {
                    "pid": article.data["code"],
                    "index_id": doaj_id,
                    "status": "OK",
                }
                for doaj_id, article in zip(self.doaj_ids, articles)
            ]
        )


class DeleteXyloseArticlesListExporterAdapterTest(
    XyloseArticlesListExporterAdapterTestMixin, TestCase,
):
    index = "doaj"
    index_command = "delete"

    def setUp(self):
        with open("tests/fixtures/full-articles.json") as fp:
            articles_json = json.load(fp)
        self.doaj_ids = [f"doaj-id-{num}" for num in range(1, 4)]
        for doaj_id, article_json in zip(self.doaj_ids, articles_json):
            article_json.update({"doaj_id": doaj_id})
        self.articles = [
            scielodocument.Article(article_json)
            for article_json in articles_json
        ]

    @mock.patch.dict("os.environ", {"DOAJ_API_KEY": "doaj-api-key-1234"})
    @mock.patch("exporter.main.requests")
    def test_delete_request(
        self, mk_requests
    ):
        articles_exporter = XyloseArticlesListExporterAdapter(
            index=self.index, command=self.index_command, articles=set(self.articles),
        )
        self.assertEqual(
            sorted(articles_exporter.delete_request),
            [doaj_id for doaj_id in self.doaj_ids],
        )

    @mock.patch.dict("os.environ", {"DOAJ_API_KEY": "doaj-api-key-1234"})
    @mock.patch("exporter.main.requests")
    def test_delete_calls_requests_delete_to_doaj_api_with_doaj_delete_request(
        self, mk_requests
    ):
        articles_exporter = XyloseArticlesListExporterAdapter(
            index=self.index, command=self.index_command, articles=set(self.articles),
        )
        with mock.patch(
            "exporter.main.XyloseArticlesListExporterAdapter.delete_request",
            new_callable=mock.PropertyMock
        ) as mk_delete_request:
            mk_delete_request.return_value = [doaj_id for doaj_id in self.doaj_ids]
            articles_exporter.command_function()
            mk_requests.delete.assert_called_once_with(
                url=articles_exporter.bulk_articles_url,
                params=articles_exporter.params_request,
                json=[doaj_id for doaj_id in self.doaj_ids],
            )

    @mock.patch.dict("os.environ", {"DOAJ_API_KEY": "doaj-api-key-1234"})
    @mock.patch("exporter.main.requests")
    def test_delete_raises_exception_if_delete_raises_http_error(self, mk_requests):
        mock_resp = mock.Mock()
        mock_resp.raise_for_status.side_effect = requests.exceptions.HTTPError(
            "HTTP Error"
        )
        mk_requests.delete.return_value = mock_resp
        mk_requests.delete.return_value.json.return_value = {
            "id": "doaj-id",
            "error": "wrong field.",
        }

        articles_exporter = XyloseArticlesListExporterAdapter(
            index=self.index, command=self.index_command, articles=set(self.articles)
        )
        with self.assertRaises(IndexExporterHTTPError) as exc:
            articles_exporter.command_function()
        self.assertEqual(
            "Erro ao deletar no doaj: HTTP Error. wrong field.", str(exc.exception)
        )

    @mock.patch.dict("os.environ", {"DOAJ_API_KEY": "doaj-api-key-1234"})
    @mock.patch("exporter.main.requests")
    def test_delete_returns_exporter_delete_response(self, mk_requests):
        mock_delete_resp = mock.Mock()
        mk_requests.delete.return_value = mock_delete_resp
        articles_exporter = XyloseArticlesListExporterAdapter(
            index=self.index, command=self.index_command, articles=set(self.articles)
        )
        ret = articles_exporter.command_function()
        for article in self.articles:
            pid = article.data["code"]
            with self.subTest(pid=pid):
                self.assertIn({ "pid": pid, "status": "DELETED" }, ret)


class ProcessDocumentTestMixin:

    @mock.patch("exporter.main.XyloseArticleExporterAdapter")
    def test_amclient_document_called(self, MockXyloseArticleExporterAdapter):
        mk_document = mock.Mock()
        process_document(
            mk_document,
            index=self.index,
            index_command=self.index_command,
            collection="scl",
            pid="S0100-19651998000200002",
        )
        mk_document.assert_called_with(collection="scl", pid="S0100-19651998000200002")

    def test_raises_exception_if_get_document_raises_exception(self):
        mk_document = mock.Mock(side_effect=Exception("No document found"))
        with self.assertRaises(Exception) as exc_info:
            process_document(
                mk_document,
                index=self.index,
                index_command=self.index_command,
                collection="scl",
                pid="S0100-19651998000200002",
            )
        self.assertEqual(str(exc_info.exception), "No document found")

    def test_raises_exception_if_no_document_returned(self):
        mk_document = mock.Mock(return_value=None)
        with self.assertRaises(ArticleMetaDocumentNotFound) as exc_info:
            process_document(
                mk_document,
                index=self.index,
                index_command=self.index_command,
                collection="scl",
                pid="S0100-19651998000200002",
            )

    @mock.patch("exporter.main.XyloseArticleExporterAdapter")
    def test_XyloseArticleExporterAdapter_instance_created(
        self, MockXyloseArticleExporterAdapter
    ):
        document = mock.Mock(spec=scielodocument.Article, data={"id": "document-1234"})
        mk_document = mock.Mock(return_value=document)
        process_document(
            mk_document,
            index=self.index,
            index_command=self.index_command,
            collection="scl",
            pid="S0100-19651998000200002",
        )
        MockXyloseArticleExporterAdapter.assert_called_once_with(
            self.index, self.index_command, document,
        )

    @mock.patch("exporter.main.XyloseArticleExporterAdapter", autospec=True)
    def test_returns_XyloseArticleExporterAdapter_command_function(
        self, MockXyloseArticleExporterAdapter
    ):
        document = mock.create_autospec(
            spec=scielodocument.Article, data={"id": "document-1234"}
        )
        mk_document = mock.Mock(return_value=document)
        mk_command_function = mock.Mock(
            return_value={"id": "doaj-id-1234", "status": "OK"}
        )
        MockXyloseArticleExporterAdapter.return_value.command_function = \
            mk_command_function
        ret = process_document(
            mk_document,
            index=self.index,
            index_command=self.index_command,
            collection="scl",
            pid="S0100-19651998000200002",
        )
        self.assertEqual(ret, {"id": "doaj-id-1234", "status": "OK"})


class ExportDocumentTest(ProcessDocumentTestMixin, TestCase):
    index = "doaj"
    index_command = "export"


class UpdateDocumentTest(ProcessDocumentTestMixin, TestCase):
    index = "doaj"
    index_command = "update"


class GetDocumentTest(ProcessDocumentTestMixin, TestCase):
    index = "doaj"
    index_command = "get"


class DeleteDocumentTest(ProcessDocumentTestMixin, TestCase):
    index = "doaj"
    index_command = "delete"


@mock.patch("exporter.main.PoisonPill")
@mock.patch("exporter.main.process_document")
class ProcessExtractedDocumentsTestMixin:
    def test_process_document_called(
        self, mk_process_document, MockPoisonPill
    ):
        mk_process_document.return_value = {}
        process_extracted_documents(
            get_document=self.mk_get_document,
            index=self.index,
            index_command=self.index_command,
            output_path=self.output_path,
            pids_by_collection={"scl": ["S0100-19651998000200002"]},
        )
        mk_process_document.assert_called_with(
            get_document=self.mk_get_document,
            index=self.index,
            index_command=self.index_command,
            collection="scl",
            pid="S0100-19651998000200002",
            poison_pill=MockPoisonPill(),
        )

    def test_process_document_called_for_each_document(
        self, mk_process_document, MockPoisonPill
    ):
        mk_process_document.return_value = {}
        pids = [f"S0100-1965199800020000{num}" for num in range(1, 4)]
        process_extracted_documents(
            get_document=self.mk_get_document,
            index=self.index,
            index_command=self.index_command,
            output_path=self.output_path,
            pids_by_collection={"scl": pids},
        )
        for pid in pids:
            mk_process_document.assert_any_call(
                get_document=self.mk_get_document,
                index=self.index,
                index_command=self.index_command,
                collection="scl",
                pid=pid,
                poison_pill=MockPoisonPill(),
            )

    def test_logs_error_if_process_document_raises_exception(
        self, mk_process_document, MockPoisonPill
    ):
        exc = ArticleMetaDocumentNotFound()
        mk_process_document.side_effect = exc
        with mock.patch("exporter.main.logger.error") as mk_logger_error:
            process_extracted_documents(
                get_document=self.mk_get_document,
                index=self.index,
                index_command=self.index_command,
                output_path=self.output_path,
                pids_by_collection={"scl": ["S0100-19651998000200001"]},
            )
            mk_logger_error.assert_called_once_with(
                "Não foi possível processar documento '%s': '%s'.",
                "S0100-19651998000200001",
                exc
            )


class ExportExtractedDocumentsTest(ProcessExtractedDocumentsTestMixin, TestCase):
    index = "doaj"
    index_command = "export"
    output_path = pathlib.Path("output.log")

    @vcr.use_cassette("tests/fixtures/vcr_cassettes/S0100-19651998000200002.yml")
    def setUp(self):
        self.mk_get_document = mock.MagicMock()

    @mock.patch("exporter.main.PoisonPill")
    @mock.patch("exporter.main.process_document")
    def test_all_docs_successfully_exported_are_recorded_to_file(
        self, mk_process_document, MockPoisonPill
    ):
        fake_pids = [f"S0100-1965199800020000{count}" for count in range(1, 5)]
        fake_exported_docs = [
            {
                "index_id": f"doaj-{pid}",
                "status": "OK",
                "pid": pid,
            }
            for pid in fake_pids
        ]
        mk_process_document.side_effect = fake_exported_docs
        with tempfile.TemporaryDirectory() as tmpdirname:
            output_file = pathlib.Path(tmpdirname) / "output.log"
            process_extracted_documents(
                get_document=self.mk_get_document,
                index=self.index,
                index_command=self.index_command,
                output_path=output_file,
                pids_by_collection={"scl": fake_pids},
            )
            file_content = output_file.read_text()
            for pid in fake_pids:
                with self.subTest(pid=pid):
                    self.assertIn(pid, file_content)


class UpdateExtractedDocumentsTest(ProcessExtractedDocumentsTestMixin, TestCase):
    index = "doaj"
    index_command = "update"
    output_path = pathlib.Path("output.log")

    @vcr.use_cassette("tests/fixtures/vcr_cassettes/S0100-19651998000200002.yml")
    def setUp(self):
        self.mk_get_document = mock.MagicMock()

    @mock.patch("exporter.main.PoisonPill")
    @mock.patch("exporter.main.process_document")
    def test_all_docs_successfully_updated_are_recorded_to_file(
        self, mk_process_document, MockPoisonPill
    ):
        fake_pids = [f"S0100-1965199800020000{count}" for count in range(1, 5)]
        fake_exported_docs = [
            {
                "index_id": f"doaj-{pid}",
                "status": "UPDATED",
                "pid": pid,
            }
            for pid in fake_pids
        ]
        mk_process_document.side_effect = fake_exported_docs
        with tempfile.TemporaryDirectory() as tmpdirname:
            output_file = pathlib.Path(tmpdirname) / "output.log"
            process_extracted_documents(
                get_document=self.mk_get_document,
                index=self.index,
                index_command=self.index_command,
                output_path=output_file,
                pids_by_collection={"scl": fake_pids},
            )
            file_content = output_file.read_text()
            for pid in fake_pids:
                with self.subTest(pid=pid):
                    self.assertIn(pid, file_content)


class GetExtractedDocumentsTest(ProcessExtractedDocumentsTestMixin, TestCase):
    index = "doaj"
    index_command = "update"

    @vcr.use_cassette("tests/fixtures/vcr_cassettes/S0100-19651998000200002.yml")
    def setUp(self):
        self.mk_get_document = mock.MagicMock()
        self.output_path = pathlib.Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.output_path)

    @mock.patch("exporter.main.PoisonPill")
    @mock.patch("exporter.main.process_document")
    def test_all_docs_successfully_get_are_recorded_to_path(
        self, mk_process_document, MockPoisonPill
    ):
        fake_pids = [f"S0100-1965199800020000{count}" for count in range(1, 5)]
        fake_exported_docs = [
            {
                "index_id": f"doaj-{pid}",
                "status": "OK",
                "pid": pid,
            }
            for pid in fake_pids
        ]
        mk_process_document.side_effect = fake_exported_docs
        process_extracted_documents(
            get_document=self.mk_get_document,
            index=self.index,
            index_command=self.index_command,
            output_path=self.output_path,
            pids_by_collection={"scl": fake_pids},
        )
        for pid in fake_pids:
            with self.subTest(pid=pid):
                file_path = self.output_path / f"{pid}.json"
                self.assertTrue(file_path.exists())
                file_content = json.loads(file_path.read_text())
                self.assertEqual(
                    file_content,
                    {
                        "index_id": f"doaj-{pid}",
                        "status": "OK",
                        "pid": pid,
                    },
                )


class DeleteExtractedDocumentsTest(ProcessExtractedDocumentsTestMixin, TestCase):
    index = "doaj"
    index_command = "delete"
    output_path = pathlib.Path("output.log")

    @vcr.use_cassette("tests/fixtures/vcr_cassettes/S0100-19651998000200002.yml")
    def setUp(self):
        self.mk_get_document = mock.MagicMock()

    @mock.patch("exporter.main.PoisonPill")
    @mock.patch("exporter.main.process_document")
    def test_all_docs_successfully_deleted_are_recorded_to_file(
        self, mk_process_document, MockPoisonPill
    ):
        fake_pids = [f"S0100-1965199800020000{count}" for count in range(1, 5)]
        fake_exported_docs = [
            {
                "index_id": f"doaj-{pid}",
                "status": "DELETED",
                "pid": pid,
            }
            for pid in fake_pids
        ]
        mk_process_document.side_effect = fake_exported_docs
        with tempfile.TemporaryDirectory() as tmpdirname:
            output_file = pathlib.Path(tmpdirname) / "output.log"
            process_extracted_documents(
                get_document=self.mk_get_document,
                index=self.index,
                index_command=self.index_command,
                output_path=output_file,
                pids_by_collection={"scl": fake_pids},
            )
            file_content = output_file.read_text()
            for pid in fake_pids:
                with self.subTest(pid=pid):
                    self.assertIn(pid, file_content)


@mock.patch("exporter.main.PoisonPill")
@mock.patch("exporter.main.execute_get_document")
@mock.patch("exporter.main.XyloseArticlesListExporterAdapter")
class ProcessDocumentsInBulkTestMixin:
    def test_get_document_called_for_each_document(
        self,
        MockXyloseArticlesListExporterAdapter,
        mk_execute_get_document,
        MockPoisonPill,
    ):
        process_documents_in_bulk(
            get_document=self.mk_get_document,
            index=self.index,
            index_command=self.index_command,
            output_path=self.output_path,
            pids_by_collection={"scl": self.pids},
        )
        for pid in self.pids:
            mk_execute_get_document.assert_any_call(
                get_document=self.mk_get_document,
                collection="scl",
                pid=pid,
                poison_pill=MockPoisonPill(),
            )

    def test_logs_error_if_execute_get_document_raises_exception(
        self,
        MockXyloseArticlesListExporterAdapter,
        mk_execute_get_document,
        MockPoisonPill,
    ):
        exc = ArticleMetaDocumentNotFound()
        mk_execute_get_document.side_effect = [
            self.articles[0],
            exc,
            self.articles[2],
        ]
        with mock.patch("exporter.main.logger.error") as mk_logger_error:
            process_documents_in_bulk(
                get_document=self.mk_get_document,
                index=self.index,
                index_command=self.index_command,
                output_path=self.output_path,
                pids_by_collection={"scl": self.pids},
            )
            mk_logger_error.assert_called_once_with(
                "Não foi possível processar documento '%s': '%s'.",
                "S0100-19651998000200002",
                exc
            )

    def test_XyloseArticlesListExporterAdapter_created(
        self,
        MockXyloseArticlesListExporterAdapter,
        mk_execute_get_document,
        MockPoisonPill,
    ):
        exc = ArticleMetaDocumentNotFound()
        mk_execute_get_document.side_effect = [
            self.articles[0],
            exc,
            self.articles[2],
        ]
        process_documents_in_bulk(
            get_document=self.mk_get_document,
            index=self.index,
            index_command=self.index_command,
            output_path=self.output_path,
            pids_by_collection={"scl": self.pids},
        )
        MockXyloseArticlesListExporterAdapter.assert_called_once_with(
            self.index, self.index_command, {self.articles[0], self.articles[2]}
        )

    def test_XyloseArticlesListExporterAdapter_not_created_if_no_documents(
        self,
        MockXyloseArticlesListExporterAdapter,
        mk_execute_get_document,
        MockPoisonPill,
    ):
        exc = ArticleMetaDocumentNotFound()
        mk_execute_get_document.side_effect = [exc, exc, exc]
        process_documents_in_bulk(
            get_document=self.mk_get_document,
            index=self.index,
            index_command=self.index_command,
            output_path=self.output_path,
            pids_by_collection={"scl": self.pids},
        )
        MockXyloseArticlesListExporterAdapter.assert_not_called()

    def test_XyloseArticlesListExporterAdapter_command_function_called(
        self,
        MockXyloseArticlesListExporterAdapter,
        mk_execute_get_document,
        MockPoisonPill,
    ):
        mk_command_function = mock.Mock(return_value=[{}])
        MockXyloseArticlesListExporterAdapter.return_value.command_function = \
            mk_command_function
        mk_execute_get_document.side_effect = self.articles
        process_documents_in_bulk(
            get_document=self.mk_get_document,
            index=self.index,
            index_command=self.index_command,
            output_path=self.output_path,
            pids_by_collection={"scl": self.pids},
        )
        mk_command_function.assert_called_once_with()

    def test_writes_command_function_result(
        self,
        MockXyloseArticlesListExporterAdapter,
        mk_execute_get_document,
        MockPoisonPill,
    ):
        fake_export_response = [{ "pid": pid, "status": "OK" } for pid in self.pids]
        mk_command_function = mock.Mock(return_value=fake_export_response)
        MockXyloseArticlesListExporterAdapter.return_value.command_function = \
            mk_command_function
        mk_execute_get_document.side_effect = self.articles
        with tempfile.TemporaryDirectory() as tmpdirname:
            output_path = pathlib.Path(tmpdirname) / self.output_path
            process_documents_in_bulk(
                get_document=self.mk_get_document,
                index=self.index,
                index_command=self.index_command,
                output_path=output_path,
                pids_by_collection={"scl": self.pids},
            )
            with output_path.open(encoding="utf-8") as fp:
                self.assertEqual(
                    [json.loads(line) for line in fp],
                    fake_export_response,
                )


class ExportDocumentsInBulkTest(ProcessDocumentsInBulkTestMixin, TestCase):
    index = "doaj"
    index_command = "export"
    output_path = pathlib.Path("output.log")
    pids = [f"S0100-1965199800020000{num}" for num in range(1, 4)]

    @vcr.use_cassette("tests/fixtures/vcr_cassettes/S0100-19651998000200002.yml")
    def setUp(self):
        self.mk_get_document = mock.MagicMock()
        with open("tests/fixtures/full-articles.json") as fp:
            articles_json = json.load(fp)
        self.articles = [
            scielodocument.Article(article_json)
            for article_json in articles_json
        ]


class DeleteDocumentsInBulkTest(ProcessDocumentsInBulkTestMixin, TestCase):
    index = "doaj"
    index_command = "delete"
    output_path = pathlib.Path("output.log")
    pids = [f"S0100-1965199800020000{num}" for num in range(1, 4)]

    @vcr.use_cassette("tests/fixtures/vcr_cassettes/S0100-19651998000200002.yml")
    def setUp(self):
        self.mk_get_document = mock.MagicMock()
        with open("tests/fixtures/full-articles.json") as fp:
            articles_json = json.load(fp)
        self.articles = [
            scielodocument.Article(article_json)
            for article_json in articles_json
        ]


class ArticleMetaParserTest(TestCase):
    def test_from_date(self):
        sargs = [
            "--from-date",
            "01-01-2021",
        ]
        parser = articlemeta_parser(sargs)
        args = parser.parse_args(sargs)
        self.assertEqual(args.from_date, "01-01-2021")

    def test_from_date_and_collection(self):
        sargs = [
            "--from-date",
            "01-01-2021",
            "--collection",
            "spa"
        ]
        parser = articlemeta_parser(sargs)
        args = parser.parse_args(sargs)
        self.assertEqual(args.from_date, "01-01-2021")
        self.assertEqual(args.collection, "spa")

    def test_from_and_until_date(self):
        sargs = [
            "--from-date",
            "01-01-2021",
            "--until-date",
            "07-01-2021",
        ]
        parser = articlemeta_parser(sargs)
        args = parser.parse_args(sargs)
        self.assertEqual(args.from_date, "01-01-2021")
        self.assertEqual(args.until_date, "07-01-2021")

    def test_from_and_until_date_and_collection(self):
        sargs = [
            "--from-date",
            "01-01-2021",
            "--until-date",
            "07-01-2021",
            "--collection",
            "spa"
        ]
        parser = articlemeta_parser(sargs)
        args = parser.parse_args(sargs)
        self.assertEqual(args.from_date, "01-01-2021")
        self.assertEqual(args.until_date, "07-01-2021")
        self.assertEqual(args.collection, "spa")

    def test_invalid_until_date_raises_exception(self):
        sargs = [
            "--until-date",
            "123456",
        ]
        with self.assertRaises(ValueError) as exc:
            parser = articlemeta_parser(sargs)
            args = parser.parse_args(sargs)

    def test_future_from_date_is_changed_to_today(self):
        today = datetime.today()
        next_week = today + timedelta(days=7)
        sargs = [
            "--from-date",
            next_week.strftime("%d-%m-%Y"),
        ]
        parser = articlemeta_parser(sargs)
        args = parser.parse_args(sargs)
        self.assertEqual(args.from_date, today.strftime("%d-%m-%Y"))

    def test_future_until_date_is_changed_to_today(self):
        today = datetime.today()
        next_week = today + timedelta(days=7)
        sargs = [
            "--until-date",
            next_week.strftime("%d-%m-%Y"),
        ]
        parser = articlemeta_parser(sargs)
        args = parser.parse_args(sargs)
        self.assertEqual(args.until_date, today.strftime("%d-%m-%Y"))

    def test_collection_and_pid(self):
        sargs = [
            "--collection",
            "spa",
            "--pid",
            "S0100-19651998000200002",
        ]
        parser = articlemeta_parser(sargs)
        args = parser.parse_args(sargs)
        self.assertEqual(args.collection, "spa")
        self.assertEqual(args.pid, "S0100-19651998000200002")
        self.assertIsNone(args.from_date)
        self.assertIsNone(args.until_date)

    def test_collection_and_pids(self):
        sargs = [
            "--collection",
            "spa",
            "--pids",
            "pids.txt",
        ]
        parser = articlemeta_parser(sargs)
        args = parser.parse_args(sargs)
        self.assertEqual(args.collection, "spa")
        self.assertEqual(str(args.pids), "pids.txt")
        self.assertIsNone(args.from_date)
        self.assertIsNone(args.until_date)

    def test_connection(self):
        sargs = [
            "--collection",
            "spa",
            "--pids",
            "pids.txt",
            "--connection",
            "thrift",
        ]
        parser = articlemeta_parser(sargs)
        args = parser.parse_args(sargs)
        self.assertEqual(args.collection, "spa")
        self.assertEqual(str(args.pids), "pids.txt")
        self.assertEqual(str(args.connection), "thrift")

    def test_connection(self):
        sargs = [
            "--collection",
            "spa",
            "--pids",
            "pids.txt",
            "--domain",
            "http://anotheram.scielo.org",
        ]
        parser = articlemeta_parser(sargs)
        args = parser.parse_args(sargs)
        self.assertEqual(args.collection, "spa")
        self.assertEqual(str(args.pids), "pids.txt")
        self.assertEqual(str(args.domain), "http://anotheram.scielo.org")


class MainExporterTestMixin:
    def test_raises_exception_if_no_index_command(self):
        with self.assertRaises(SystemExit) as exc:
            main_exporter(
                [
                    "--output",
                    str(self.output_path),
                ]
            )

    def test_raises_exception_if_no_doaj_command(self):
        with self.assertRaises(SystemExit) as exc:
            main_exporter(
                [
                    "--output",
                    str(self.output_path),
                    self.index,
                ]
            )

    def test_raises_exception_if_no_dates_nor_pids(self):
        with self.assertRaises(OriginDataFilterError) as exc:
            main_exporter(
                [
                    "--output",
                    str(self.output_path),
                    self.index,
                    self.index_command,
                ]
            )
        self.assertEqual(
            str(exc.exception),
            "Informe ao menos uma das datas (from-date ou until-date), pid ou pids",
        )

    def test_raises_exception_if_pid_and_no_collection(self):
        with self.assertRaises(OriginDataFilterError) as exc:
            main_exporter(
                [
                    "--output",
                    str(self.output_path),
                    self.index,
                    self.index_command,
                    "--pid",
                    "S0100-19651998000200002",
                ] + self.extra_args
            )
        self.assertEqual(
            str(exc.exception),
            "Coleção é obrigatória para exportação de um PID",
        )

    def test_raises_exception_if_pids_and_no_collection(self):
        pids = [
            "S0100-19651998000200001",
            "S0100-19651998000200002",
            "S0100-19651998000200003",
        ]
        with tempfile.TemporaryDirectory() as tmpdirname:
            pids_file = pathlib.Path(tmpdirname) / "pids.txt"
            pids_file.write_text("\n".join(pids))
            with self.assertRaises(OriginDataFilterError) as exc:
                main_exporter(
                    [
                        "--output",
                        str(self.output_path),
                        self.index,
                        self.index_command,
                        "--pids",
                        str(pids_file),
                    ] + self.extra_args
                )
            self.assertEqual(
                str(exc.exception),
                "Coleção é obrigatória para exportação de lista de PIDs",
            )

    @mock.patch("exporter.main.AMClient")
    def test_instanciates_AMClient(self, MockAMClient):
        main_exporter(
            [
                "--output",
                str(self.output_path),
                self.index,
                self.index_command,
                "--connection",
                "thrift",
                "--collection",
                "spa",
                "--pid",
                "S0100-19651998000200002",
            ] + self.extra_args
        )
        MockAMClient.assert_called_with(connection="thrift")

    @mock.patch("exporter.main.AMClient")
    def test_instanciates_AMClient_with_another_domain(self, MockAMClient):
        main_exporter(
            [
                "--output",
                str(self.output_path),
                self.index,
                self.index_command,
                "--domain",
                "http://anotheram.scielo.org",
                "--collection",
                "spa",
                "--pid",
                "S0100-19651998000200002",
            ] + self.extra_args
        )
        MockAMClient.assert_called_with(domain="http://anotheram.scielo.org")

    @mock.patch.object(AMClient, "document")
    def test_process_extracted_documents_called_with_collection_and_pid(
        self, mk_document
    ):
        main_exporter(
            [
                "--output",
                str(self.output_path),
                self.index,
                self.index_command,
                "--collection",
                "spa",
                "--pid",
                "S0100-19651998000200002",
            ] + self.extra_args
        )
        self.mk_process_documents.assert_called_with(
            get_document=mk_document,
            index=self.index,
            index_command=self.index_command,
            output_path=self.output_path,
            pids_by_collection={"spa": ["S0100-19651998000200002"]},
        )

    @mock.patch.object(AMClient, "document")
    def test_process_extracted_documents_called_with_collection_and_pids_from_file(
        self, mk_document
    ):
        pids = [
            "S0100-19651998000200001",
            "S0100-19651998000200002",
            "S0100-19651998000200003",
        ]
        with tempfile.TemporaryDirectory() as tmpdirname:
            pids_file = pathlib.Path(tmpdirname) / "pids.txt"
            pids_file.write_text("\n".join(pids))
            main_exporter(
                [
                    "--output",
                    str(self.output_path),
                    self.index,
                    self.index_command,
                    "--collection",
                    "spa",
                    "--pids",
                    str(pids_file),
                ] + self.extra_args
            )
        self.mk_process_documents.assert_called_with(
            get_document=mk_document,
            index=self.index,
            index_command=self.index_command,
            output_path=self.output_path,
            pids_by_collection={"spa": pids},
        )

    @mock.patch("exporter.main.utils.get_valid_datetime")
    @mock.patch.object(AMClient, "documents_identifiers")
    def test_calls_get_valid_datetime_with_dates(
        self,
        mk_documents_identifiers,
        mk_get_valid_datetime,
    ):
        tests_args = [
            ["--from-date", "01-01-2021",],
            ["--until-date", "02-01-2021",],
            ["--from-date", "01-01-2021", "--until-date", "07-01-2021",],
        ]

        for args in tests_args:
            main_exporter(
                [
                    "--output",
                    str(self.output_path),
                    self.index,
                    self.index_command,
                ] +
                args + self.extra_args
            )

        mk_get_valid_datetime.assert_has_calls(
            [
                mock.call("01-01-2021"),
                mock.call("02-01-2021"),
                mock.call("01-01-2021"),
                mock.call("07-01-2021"),
            ]
        )

    @mock.patch.object(AMClient, "documents_identifiers")
    def test_calls_am_client_documents_identifiers_with_args(
        self, mk_documents_identifiers
    ):
        tests_args_and_calls = [
            (["--from-date", "01-01-2021",], {"from_date": datetime(2021, 1, 1, 0, 0)}),
            (
                ["--until-date", "02-01-2021",],
                {"until_date": datetime(2021, 1, 2, 0, 0)},
            ),
            (
                ["--from-date", "01-01-2021", "--until-date", "07-01-2021",],
                {"from_date": datetime(2021, 1, 1), "until_date": datetime(2021, 1, 7)},
            ),
            (
                ["--collection", "spa", "--from-date", "01-01-2021",],
                {"collection": "spa", "from_date": datetime(2021, 1, 1)},
            ),
            (
                ["--collection", "spa", "--until-date", "02-01-2021",],
                {"collection": "spa", "until_date": datetime(2021, 1, 2)},
            ),
        ]

        for args, call_params in tests_args_and_calls:
            with self.subTest(args=args, call_params=call_params):
                main_exporter(
                    [
                        "--output",
                        str(self.output_path),
                        self.index,
                        self.index_command,
                    ] +
                    args + self.extra_args
                )
                mk_documents_identifiers.assert_called_with(**call_params)

    @mock.patch.object(AMClient, "documents_identifiers")
    @mock.patch.object(AMClient, "document")
    def test_process_extracted_documents_called_with_identifiers_from_date_search(
        self, mk_document, mk_documents_identifiers
    ):
        mk_documents_identifiers.return_value = [
            {
                'doi': 'doi-123456',
                'collection': 'scl',
                'processing_date': '2021-12-06',
                'code': 'S0101-01019000090090097',
            },
            {
                'doi': 'doi-654321',
                'collection': 'arg',
                'processing_date': '2021-12-06',
                'code': 'S0202-01019000090090098',
            },
            {
                'doi': 'doi-162534',
                'collection': 'cub',
                'processing_date': '2021-12-06',
                'code': 'S0303-01019000090090099',
            },
        ]
        main_exporter(
            [
                "--output",
                str(self.output_path),
                self.index,
                self.index_command,
                "--from-date",
                "01-01-2021",
                "--until-date",
                "07-01-2021",
            ] + self.extra_args
        )
        self.mk_process_documents.assert_called_once_with(
            get_document=mk_document,
            index=self.index,
            index_command=self.index_command,
            output_path=self.output_path,
            pids_by_collection={
                "scl": ["S0101-01019000090090097"],
                "arg": ["S0202-01019000090090098"],
                "cub": ["S0303-01019000090090099"],
            },
        )


class DOAJExportMainExporterTest(MainExporterTestMixin, TestCase):
    index = "doaj"
    index_command = "export"
    output_path = pathlib.Path("output.log")
    extra_args = []

    def setUp(self):
        self.patcher = mock.patch("exporter.main.process_extracted_documents")
        self.mk_process_documents = self.patcher.start()

    def tearDown(self):
        self.mk_process_documents.stop()


class DOAJExportinBulkMainExporterTest(MainExporterTestMixin, TestCase):
    index = "doaj"
    index_command = "export"
    output_path = pathlib.Path("output.log")
    extra_args = ["--bulk"]

    def setUp(self):
        self.patcher = mock.patch("exporter.main.process_documents_in_bulk")
        self.mk_process_documents = self.patcher.start()

    def tearDown(self):
        self.mk_process_documents.stop()


class DOAJUpdateMainExporterTest(MainExporterTestMixin, TestCase):
    index = "doaj"
    index_command = "update"
    output_path = pathlib.Path("output.log")
    extra_args = []

    def setUp(self):
        self.patcher = mock.patch("exporter.main.process_extracted_documents")
        self.mk_process_documents = self.patcher.start()

    def tearDown(self):
        self.mk_process_documents.stop()


class DOAJGetMainExporterTest(MainExporterTestMixin, TestCase):
    index = "doaj"
    index_command = "get"
    extra_args = []

    def setUp(self):
        self.output_path = pathlib.Path(tempfile.mkdtemp())
        self.patcher = mock.patch("exporter.main.process_extracted_documents")
        self.mk_process_documents = self.patcher.start()

    def tearDown(self):
        shutil.rmtree(self.output_path)
        self.mk_process_documents.stop()


class DOAJDeleteMainExporterTest(MainExporterTestMixin, TestCase):
    index = "doaj"
    index_command = "delete"
    output_path = pathlib.Path("output.log")
    extra_args = []

    def setUp(self):
        self.patcher = mock.patch("exporter.main.process_extracted_documents")
        self.mk_process_documents = self.patcher.start()

    def tearDown(self):
        self.mk_process_documents.stop()


class DOAJDeleteinBulkMainExporterTest(MainExporterTestMixin, TestCase):
    index = "doaj"
    index_command = "delete"
    output_path = pathlib.Path("output.log")
    extra_args = ["--bulk"]

    def setUp(self):
        self.patcher = mock.patch("exporter.main.process_documents_in_bulk")
        self.mk_process_documents = self.patcher.start()

    def tearDown(self):
        self.mk_process_documents.stop()
