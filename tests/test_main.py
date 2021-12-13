import tempfile
import pathlib
import json
from unittest import TestCase, mock

import vcr
import articlemeta.client as articlemeta_client
import requests
from xylose import scielodocument

from exporter import AMClient, extract_and_export_documents, doaj
from exporter.main import (
    ArticleMetaDocumentNotFound,
    InvalidIndexExporter,
    IndexExporterHTTPError,
    XyloseArticleExporterAdapter,
    export_document,
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


class XyloseArticleExporterAdapterTest(TestCase):
    @vcr.use_cassette("tests/fixtures/vcr_cassettes/S0100-19651998000200002.yml")
    def setUp(self):
        client = AMClient()
        self.article = client.document(collection="scl", pid="S0100-19651998000200002")

    def test_raises_exception_if_invalid_index(self):
        with self.assertRaises(InvalidIndexExporter) as exc:
            article_exporter = XyloseArticleExporterAdapter(
                index="abc", article=self.article
            )

    @mock.patch.dict("os.environ", {"DOAJ_API_KEY": "doaj-api-key-1234"})
    @mock.patch("exporter.main.requests")
    def test_export_calls_requests_post_to_doaj_api_with_doaj_post_request(
        self, mk_requests
    ):
        with mock.patch(
            "exporter.doaj.DOAJExporterXyloseArticle.post_request",
            new_callable=mock.PropertyMock,
        ) as mk_post_request:
            mk_post_request.return_value = {
                "api_key": "doaj-api-key-1234",
                "json": {"field": "value"},
            }
            article_exporter: doaj.DOAJExporterXyloseArticle = XyloseArticleExporterAdapter(
                index="doaj", article=self.article
            )
            article_exporter.export()
            mk_requests.post.assert_called_once_with(
                url=article_exporter.index_exporter.crud_article_url,
                **{"api_key": "doaj-api-key-1234", "json": {"field": "value"}},
            )

    @mock.patch.dict("os.environ", {"DOAJ_API_KEY": "doaj-api-key-1234"})
    @mock.patch("exporter.main.requests")
    def test_export_raises_exception_if_post_raises_http_error(self, mk_requests):
        mock_resp = mock.Mock()
        mock_resp.raise_for_status.side_effect = requests.exceptions.HTTPError(
            "HTTP Error"
        )
        mk_requests.post.return_value = mock_resp
        mk_requests.post.return_value.json.return_value = {
            "id": "doaj-id",
            "error": "wrong field.",
        }

        article_exporter: doaj.DOAJExporterXyloseArticle = XyloseArticleExporterAdapter(
            index="doaj", article=self.article
        )
        with self.assertRaises(IndexExporterHTTPError) as exc:
            article_exporter.export()
        self.assertEqual(
            "Erro na exportação ao doaj: HTTP Error. wrong field.", str(exc.exception)
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
            index="doaj", article=self.article
        )
        ret = article_exporter.export()
        self.assertEqual(
            ret,
            {
                "pid": self.article.data["code"],
                "index_id": "doaj-id",
                "status": "OK",
            }
        )


class ExportDocumentTest(TestCase):

    @mock.patch("exporter.main.XyloseArticleExporterAdapter")
    def test_amclient_document_called(self, MockXyloseArticleExporterAdapter):
        mk_document = mock.Mock()
        export_document(
            mk_document, index="doaj", collection="scl", pid="S0100-19651998000200002"
        )
        mk_document.assert_called_with(collection="scl", pid="S0100-19651998000200002")

    def test_raises_exception_if_get_document_raises_exception(self):
        mk_document = mock.Mock(side_effect=Exception("No document found"))
        with self.assertRaises(Exception) as exc_info:
            export_document(
                mk_document,
                index="doaj",
                collection="scl",
                pid="S0100-19651998000200002",
            )
        self.assertEqual(str(exc_info.exception), "No document found")

    def test_raises_exception_if_no_document_returned(self):
        mk_document = mock.Mock(return_value=None)
        with self.assertRaises(ArticleMetaDocumentNotFound) as exc_info:
            export_document(
                mk_document,
                index="doaj",
                collection="scl",
                pid="S0100-19651998000200002",
            )

    @mock.patch("exporter.main.XyloseArticleExporterAdapter")
    def test_XyloseArticleExporterAdapter_instance_created(self, MockXyloseArticleExporterAdapter):
        document = mock.Mock(spec=scielodocument.Article, data={"id": "document-1234"})
        mk_document = mock.Mock(return_value=document)
        export_document(
            mk_document, index="doaj", collection="scl", pid="S0100-19651998000200002"
        )
        MockXyloseArticleExporterAdapter.assert_called_once_with("doaj", document)

    @mock.patch("exporter.main.XyloseArticleExporterAdapter", autospec=True)
    def test_calls_XyloseArticleExporterAdapter_export(self, MockXyloseArticleExporterAdapter):
        document = mock.create_autospec(
            spec=scielodocument.Article, data={"id": "document-1234"}
        )
        mk_document = mock.Mock(return_value=document)
        export_document(
            mk_document, index="doaj", collection="scl", pid="S0100-19651998000200002"
        )
        MockXyloseArticleExporterAdapter.return_value.export.assert_called_once()


@vcr.use_cassette("tests/fixtures/vcr_cassettes/S0100-19651998000200002.yml")
class ExtractAndExportDocumentsTest(TestCase):
    @mock.patch("exporter.main.AMClient")
    @mock.patch("exporter.main.export_document")
    def test_instanciates_AMClient(self, mk_export_document, MockAMClient):
        mk_export_document.return_value = {}
        extract_and_export_documents(
            index="doaj",
            collection="scl",
            output_path="output.log",
            pids=["S0100-19651998000200002"],
            connection="thrift",
        )
        MockAMClient.assert_called_with(connection="thrift")

    @mock.patch("exporter.main.AMClient")
    @mock.patch("exporter.main.export_document")
    def test_instanciates_AMClient_with_another_domain(
        self, mk_export_document, MockAMClient
    ):
        mk_export_document.return_value = {}
        extract_and_export_documents(
            index="doaj",
            collection="scl",
            output_path="output.log",
            pids=["S0100-19651998000200002"],
            domain="http://anotheram.scielo.org",
        )
        MockAMClient.assert_called_with(domain="http://anotheram.scielo.org")

    @mock.patch("exporter.main.PoisonPill")
    @mock.patch("exporter.main.export_document")
    @mock.patch.object(AMClient, "document")
    def test_export_document_called(
        self, mk_get_document, mk_export_document, MockPoisonPill
    ):
        mk_export_document.return_value = {}
        extract_and_export_documents(
            index="doaj",
            collection="scl",
            output_path="output.log",
            pids=["S0100-19651998000200002"],
            connection="thrift",
        )
        mk_export_document.assert_called_with(
            get_document=mk_get_document,
            index="doaj",
            collection="scl",
            pid="S0100-19651998000200002",
            poison_pill=MockPoisonPill(),
        )

    @mock.patch("exporter.main.PoisonPill")
    @mock.patch("exporter.main.export_document")
    @mock.patch.object(AMClient, "document")
    def test_export_document_called_for_each_document(
        self, mk_get_document, mk_export_document, MockPoisonPill
    ):
        mk_export_document.return_value = {}
        pids = [f"S0100-1965199800020000{num}" for num in range(1, 4)]
        extract_and_export_documents(
            index="doaj",
            collection="scl",
            output_path="output.log",
            pids=pids,
            connection="thrift",
        )
        for pid in pids:
            mk_export_document.assert_any_call(
                get_document=mk_get_document,
                index="doaj",
                collection="scl",
                pid=pid,
                poison_pill=MockPoisonPill(),
            )

    @mock.patch("exporter.main.logger.error")
    @mock.patch("exporter.main.PoisonPill")
    @mock.patch("exporter.main.export_document")
    @mock.patch.object(AMClient, "document")
    def test_logs_error_if_export_document_raises_exception(
        self, mk_get_document, mk_export_document, MockPoisonPill, mk_logger_error
    ):
        exc = ArticleMetaDocumentNotFound()
        mk_export_document.side_effect = exc
        extract_and_export_documents(
            index="doaj",
            collection="scl",
            output_path="output.log",
            pids=["S0100-19651998000200002"],
            connection="thrift",
        )
        mk_logger_error.assert_called_once_with(
            "Não foi possível exportar documento '%s': '%s'.",
            "S0100-19651998000200002",
            exc
        )

    @mock.patch("exporter.main.PoisonPill")
    @mock.patch("exporter.main.export_document")
    @mock.patch.object(AMClient, "document")
    def test_all_docs_successfully_posted_are_recorded_to_file(
        self, mk_get_document, mk_export_document, MockPoisonPill
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
        mk_export_document.side_effect = fake_exported_docs
        with tempfile.TemporaryDirectory() as tmpdirname:
            output_file = pathlib.Path(tmpdirname) / "output.log"
            extract_and_export_documents(
                index="doaj",
                collection="scl",
                output_path=output_file,
                pids=fake_pids,
                connection="thrift",
            )
            file_content = output_file.read_text()
            for pid in fake_pids:
                with self.subTest(pid=pid):
                    self.assertIn(pid, file_content)


class MainExporterTest(TestCase):
    @mock.patch("exporter.main.extract_and_export_documents")
    def test_extract_and_export_documents_called_with_collection_and_pid(
        self, mk_extract_and_export_documents
    ):
        main_exporter(
            [
                "--output",
                "output.log",
                "doaj",
                "--collection",
                "spa",
                "--pid",
                "S0100-19651998000200002",
            ]
        )
        mk_extract_and_export_documents.assert_called_with(
            index="doaj",
            collection="spa",
            output_path="output.log",
            pids=["S0100-19651998000200002"],
        )

    @mock.patch("exporter.main.extract_and_export_documents")
    def test_extract_and_export_documents_called_with_collection_and_pids_from_file(
        self, mk_extract_and_export_documents
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
                    "output.log",
                    "doaj",
                    "--collection",
                    "spa",
                    "--pids",
                    str(pids_file),
                ]
            )
        mk_extract_and_export_documents.assert_called_with(
            index="doaj", collection="spa", output_path="output.log", pids=pids
        )
