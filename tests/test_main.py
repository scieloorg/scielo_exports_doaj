import tempfile
import pathlib
import json
from unittest import TestCase, mock
from datetime import datetime, timedelta

import vcr
import articlemeta.client as articlemeta_client
import requests
from xylose import scielodocument

from exporter import AMClient, process_extracted_documents, doaj
from exporter.main import (
    ArticleMetaDocumentNotFound,
    InvalidExporterInitData,
    IndexExporterHTTPError,
    OriginDataFilterError,
    XyloseArticleExporterAdapter,
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
            mk_post_request.return_value = {
                "params": {"api_key": "doaj-api-key-1234"},
                "json": {"field": "value"},
            }
            article_exporter = XyloseArticleExporterAdapter(
                index=self.index, command=self.index_command, article=self.article,
            )
            article_exporter.command_function()
            mk_requests.post.assert_called_once_with(
                url=article_exporter.index_exporter.crud_article_put_url,
                **{
                    "params": {"api_key": "doaj-api-key-1234"},
                    "json": {"field": "value"},
                },
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
            index=self.index, command=self.index_command, article=self.article
        )
        with self.assertRaises(IndexExporterHTTPError) as exc:
            article_exporter.command_function()
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
    def test_calls_XyloseArticleExporterAdapter_command_function(
        self, MockXyloseArticleExporterAdapter
    ):
        document = mock.create_autospec(
            spec=scielodocument.Article, data={"id": "document-1234"}
        )
        mk_document = mock.Mock(return_value=document)
        mk_command_function = mock.Mock(return_value={})
        MockXyloseArticleExporterAdapter.return_value.command_function = \
            mk_command_function
        process_document(
            mk_document,
            index=self.index,
            index_command=self.index_command,
            collection="scl",
            pid="S0100-19651998000200002",
        )
        mk_command_function.assert_called_once()


class ExportDocumentTest(ProcessDocumentTestMixin, TestCase):
    index = "doaj"
    index_command = "export"


class UpdateDocumentTest(ProcessDocumentTestMixin, TestCase):
    index = "doaj"
    index_command = "update"


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
            output_path=pathlib.Path("output.log"),
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
            output_path=pathlib.Path("output.log"),
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
                output_path=pathlib.Path("output.log"),
                pids_by_collection={"scl": ["S0100-19651998000200001"]},
            )
            mk_logger_error.assert_called_once_with(
                "Não foi possível exportar documento '%s': '%s'.",
                "S0100-19651998000200001",
                exc
            )

    def test_all_docs_successfully_posted_are_recorded_to_file(
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


class ExportExtractedDocumentsTest(ProcessExtractedDocumentsTestMixin, TestCase):
    index = "doaj"
    index_command = "export"

    @vcr.use_cassette("tests/fixtures/vcr_cassettes/S0100-19651998000200002.yml")
    def setUp(self):
        self.mk_get_document = mock.MagicMock()


class UpdateExtractedDocumentsTest(ProcessExtractedDocumentsTestMixin, TestCase):
    index = "doaj"
    index_command = "update"

    @vcr.use_cassette("tests/fixtures/vcr_cassettes/S0100-19651998000200002.yml")
    def setUp(self):
        self.mk_get_document = mock.MagicMock()


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
    @mock.patch("exporter.main.process_extracted_documents")
    def test_raises_exception_if_no_index_command(
        self, mk_process_extracted_documents
    ):
        with self.assertRaises(SystemExit) as exc:
            main_exporter(
                [
                    "--output",
                    "output.log",
                ]
            )

    @mock.patch("exporter.main.process_extracted_documents")
    def test_raises_exception_if_no_doaj_command(
        self, mk_process_extracted_documents
    ):
        with self.assertRaises(SystemExit) as exc:
            main_exporter(
                [
                    "--output",
                    "output.log",
                    self.index,
                ]
            )

    @mock.patch("exporter.main.process_extracted_documents")
    def test_raises_exception_if_no_dates_nor_pids(
        self, mk_process_extracted_documents
    ):
        with self.assertRaises(OriginDataFilterError) as exc:
            main_exporter(
                [
                    "--output",
                    "output.log",
                    self.index,
                    self.index_command,
                ]
            )
        self.assertEqual(
            str(exc.exception),
            "Informe ao menos uma das datas (from-date ou until-date), pid ou pids",
        )

    @mock.patch("exporter.main.process_extracted_documents")
    def test_raises_exception_if_pid_and_no_collection(
        self, mk_process_extracted_documents
    ):
        with self.assertRaises(OriginDataFilterError) as exc:
            main_exporter(
                [
                    "--output",
                    "output.log",
                    self.index,
                    self.index_command,
                    "--pid",
                    "S0100-19651998000200002",
                ]
            )
        self.assertEqual(
            str(exc.exception),
            "Coleção é obrigatória para exportação de um PID",
        )

    @mock.patch("exporter.main.process_extracted_documents")
    def test_raises_exception_if_pids_and_no_collection(
        self, mk_process_extracted_documents
    ):
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
                        "output.log",
                        self.index,
                        self.index_command,
                        "--pids",
                        str(pids_file),
                    ]
                )
            self.assertEqual(
                str(exc.exception),
                "Coleção é obrigatória para exportação de lista de PIDs",
            )

    @mock.patch("exporter.main.AMClient")
    @mock.patch("exporter.main.process_extracted_documents")
    def test_instanciates_AMClient(self, mk_process_extracted_documents, MockAMClient):
        main_exporter(
            [
                "--output",
                "output.log",
                self.index,
                self.index_command,
                "--connection",
                "thrift",
                "--collection",
                "spa",
                "--pid",
                "S0100-19651998000200002",
            ]
        )
        MockAMClient.assert_called_with(connection="thrift")

    @mock.patch("exporter.main.AMClient")
    @mock.patch("exporter.main.process_extracted_documents")
    def test_instanciates_AMClient_with_another_domain(
        self, mk_process_extracted_documents, MockAMClient
    ):
        main_exporter(
            [
                "--output",
                "output.log",
                self.index,
                self.index_command,
                "--domain",
                "http://anotheram.scielo.org",
                "--collection",
                "spa",
                "--pid",
                "S0100-19651998000200002",
            ]
        )
        MockAMClient.assert_called_with(domain="http://anotheram.scielo.org")

    @mock.patch.object(AMClient, "document")
    @mock.patch("exporter.main.process_extracted_documents")
    def test_process_extracted_documents_called_with_collection_and_pid(
        self, mk_process_extracted_documents, mk_document
    ):
        main_exporter(
            [
                "--output",
                "output.log",
                self.index,
                self.index_command,
                "--collection",
                "spa",
                "--pid",
                "S0100-19651998000200002",
            ]
        )
        mk_process_extracted_documents.assert_called_with(
            get_document=mk_document,
            index=self.index,
            index_command=self.index_command,
            output_path=pathlib.Path("output.log"),
            pids_by_collection={"spa": ["S0100-19651998000200002"]},
        )

    @mock.patch.object(AMClient, "document")
    @mock.patch("exporter.main.process_extracted_documents")
    def test_process_extracted_documents_called_with_collection_and_pids_from_file(
        self, mk_process_extracted_documents, mk_document
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
                    self.index,
                    self.index_command,
                    "--collection",
                    "spa",
                    "--pids",
                    str(pids_file),
                ]
            )
        mk_process_extracted_documents.assert_called_with(
            get_document=mk_document,
            index=self.index,
            index_command=self.index_command,
            output_path=pathlib.Path("output.log"),
            pids_by_collection={"spa": pids},
        )

    @mock.patch("exporter.main.utils.get_valid_datetime")
    @mock.patch.object(AMClient, "documents_identifiers")
    @mock.patch("exporter.main.process_extracted_documents")
    def test_calls_get_valid_datetime_with_dates(
        self,
        mk_process_extracted_documents,
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
                    "output.log",
                    self.index,
                    self.index_command,
                ] +
                args
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
    @mock.patch("exporter.main.process_extracted_documents")
    def test_calls_am_client_documents_identifiers_with_args(
        self, mk_process_extracted_documents, mk_documents_identifiers
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
                        "output.log",
                        self.index,
                        self.index_command,
                    ] +
                    args
                )
                mk_documents_identifiers.assert_called_with(**call_params)

    @mock.patch.object(AMClient, "documents_identifiers")
    @mock.patch.object(AMClient, "document")
    @mock.patch("exporter.main.process_extracted_documents")
    def test_process_extracted_documents_called_with_identifiers_from_date_search(
        self, mk_process_extracted_documents, mk_document, mk_documents_identifiers
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
                "output.log",
                self.index,
                self.index_command,
                "--from-date",
                "01-01-2021",
                "--until-date",
                "07-01-2021",
            ],
        )
        mk_process_extracted_documents.assert_called_once_with(
            get_document=mk_document,
            index=self.index,
            index_command=self.index_command,
            output_path=pathlib.Path("output.log"),
            pids_by_collection={
                "scl": ["S0101-01019000090090097"],
                "arg": ["S0202-01019000090090098"],
                "cub": ["S0303-01019000090090099"],
            },
        )


class DOAJExportMainExporterTest(MainExporterTestMixin, TestCase):
    index = "doaj"
    index_command = "export"


class DOAJUpdateMainExporterTest(MainExporterTestMixin, TestCase):
    index = "doaj"
    index_command = "update"
