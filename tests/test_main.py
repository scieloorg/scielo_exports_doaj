import unittest
from unittest.mock import patch, Mock

import vcr
import articlemeta.client as articlemeta_client
from xylose import scielodocument

from exporter import AMClient, extract_and_export_documents
from exporter.main import export_document, ArticleMetaDocumentNotFound


class AMClientTest(unittest.TestCase):
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


class ExportDocumentTest(unittest.TestCase):
    def test_amclient_document_called(self):
        mk_document = Mock()
        export_document(
            mk_document, collection="scl", pid="S0100-19651998000200002"
        )
        mk_document.assert_called_with(collection="scl", pid="S0100-19651998000200002")

    def test_raises_exception_if_get_document_raises_exception(self):
        mk_document = Mock(side_effect=Exception("No document found"))
        with self.assertRaises(Exception) as exc_info:
            export_document(
                mk_document, collection="scl", pid="S0100-19651998000200002"
            )
        self.assertEqual(str(exc_info.exception), "No document found")

    def test_raises_exception_if_no_document_returned(self):
        mk_document = Mock(return_value=None)
        with self.assertRaises(ArticleMetaDocumentNotFound) as exc_info:
            export_document(
                mk_document, collection="scl", pid="S0100-19651998000200002"
            )


class ExtractAndExportDocumentsTest(unittest.TestCase):
    @patch("exporter.main.AMClient")
    def test_instanciates_AMClient(self, MockAMClient):
        extract_and_export_documents(
            collection="scl", pids=["S0100-19651998000200002"], connection="thrift"
        )
        MockAMClient.assert_called_with(connection="thrift")

    @patch("exporter.main.AMClient")
    def test_instanciates_AMClient_with_another_domain(self, MockAMClient):
        extract_and_export_documents(
            collection="scl",
            pids=["S0100-19651998000200002"],
            domain="http://anotheram.scielo.org",
        )
        MockAMClient.assert_called_with(domain="http://anotheram.scielo.org")

    @patch("exporter.main.PoisonPill")
    @patch("exporter.main.export_document")
    @patch.object(AMClient, "document")
    def test_export_document_called(
        self, mk_get_document, mk_export_document, MockPoisonPill
    ):
        extract_and_export_documents(
            collection="scl", pids=["S0100-19651998000200002"], connection="thrift"
        )
        mk_export_document.assert_called_with(
            get_document=mk_get_document,
            collection="scl",
            pid="S0100-19651998000200002",
            poison_pill=MockPoisonPill(),
        )

    @patch("exporter.main.PoisonPill")
    @patch("exporter.main.export_document")
    @patch.object(AMClient, "document")
    def test_export_document_called_for_each_document(
        self, mk_get_document, mk_export_document, MockPoisonPill
    ):
        pids = [f"S0100-1965199800020000{num}" for num in range(1, 4)]
        extract_and_export_documents(
            collection="scl", pids=pids, connection="thrift"
        )
        for pid in pids:
            mk_export_document.assert_any_call(
                get_document=mk_get_document,
                collection="scl",
                pid=pid,
                poison_pill=MockPoisonPill(),
            )

    @patch("exporter.main.logger.error")
    @patch("exporter.main.PoisonPill")
    @patch("exporter.main.export_document")
    @patch.object(AMClient, "document")
    def test_logs_error_if_export_document_raises_exception(
        self, mk_get_document, mk_export_document, MockPoisonPill, mk_logger_error
    ):
        exc = ArticleMetaDocumentNotFound()
        mk_export_document.side_effect = exc
        extract_and_export_documents(
            collection="scl", pids=["S0100-19651998000200002"], connection="thrift"
        )
        mk_logger_error.assert_called_once_with(
            "Não foi possível exportar documento '%s': '%s'.",
            "S0100-19651998000200002",
            exc
        )
