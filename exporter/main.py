import typing
import concurrent.futures
import logging
import argparse
import pathlib
import json
import datetime

import tenacity
import requests
from requests.exceptions import HTTPError
from tqdm import tqdm
import articlemeta.client as articlemeta_client
from xylose import scielodocument

from exporter import interfaces, doaj, config, utils


logger = logging.getLogger(__name__)


class ArticleMetaDocumentNotFound(Exception):
    pass


class InvalidExporterInitData(Exception):
    pass


class IndexExporterHTTPError(Exception):
    pass


class OriginDataFilterError(Exception):
    pass


class AMClient:
    def __init__(self, connection: str = None, domain: str = None):
        self._client = self._get_client(connection, domain)

    def _get_client(self, connection: str = None, domain: str = None):
        client_class = articlemeta_client.RestfulClient
        if connection and connection == "thrift":
            client_class = articlemeta_client.ThriftClient

        if domain:
            return client_class(domain)
        return client_class()

    def document(self, collection: str, pid: str) -> scielodocument.Article:
        return self._client.document(collection=collection, code=pid)

    def documents_identifiers(
        self,
        collection: str = None,
        from_date: datetime = None,
        until_date: datetime = None,
    ) -> typing.List[dict]:
        filter = {}
        if collection:
            filter["collection"] = collection
        if from_date:
            filter["from_date"] = from_date.strftime("%Y-%m-%d")
        if until_date:
            filter["until_date"] = until_date.strftime("%Y-%m-%d")

        return self._client.documents_by_identifiers(only_identifiers=True, **filter)


class XyloseArticleExporterAdapter(interfaces.IndexExporterInterface):
    index_exporter: interfaces.IndexExporterInterface

    def __init__(self, index: str, command: str, article: scielodocument.Article):
        if index == "doaj":
            self.index_exporter = doaj.DOAJExporterXyloseArticle(article)
        else:
            raise InvalidExporterInitData(f"Index informado inválido: {index}")

        if command == "export":
            self._command_function = self._export
        elif command == "update":
            self._command_function = self._update
        else:
            raise InvalidExporterInitData(f"Comando informado inválido: {command}")

        self.index = index
        self._pid = article.data.get("code", "")

    @property
    def post_request(self) -> dict:
        return self.index_exporter.post_request

    def post_response(self, response: dict) -> dict:
        return self.index_exporter.post_response(response)

    def error_response(self, response: dict) -> dict:
        return self.index_exporter.error_response(response)

    @tenacity.retry(
        wait=tenacity.wait_exponential(),
        stop=tenacity.stop_after_attempt(config.get("EXPORT_RUN_RETRIES")),
        retry=tenacity.retry_if_exception_type(
            (requests.ConnectionError, requests.Timeout),
        ),
    )
    def _send_http_request(self, request_method: callable, url: str, **request: json):
        return request_method(url=url, **request)

    def _export(self):
        resp = self._send_http_request(
            requests.post, self.index_exporter.crud_article_put_url, **self.post_request
        )
        try:
            resp.raise_for_status()
        except HTTPError as exc:
            error_response = self.error_response(resp.json())
            exc_msg = f"Erro na exportação ao {self.index}: {exc}. {error_response}"
            raise IndexExporterHTTPError(exc_msg)
        else:
            export_result = self.post_response(resp.json())
            export_result["pid"] = self._pid
            return export_result

    def _update(self):
        pass

    def command_function(self):
        return self._command_function()


class PoisonPill:
    def __init__(self):
        self.poisoned = False


class JobExecutor:
    def __init__(
        self,
        func: callable,
        max_workers: int = 1,
        success_callback: callable = (lambda *k: k),
        exception_callback: callable = (lambda *k: k),
        update_bar: callable = (lambda *k: k),
    ):
        self.poison_pill = PoisonPill()
        self.func = func
        self.executor = concurrent.futures.ThreadPoolExecutor
        self.max_workers = max_workers
        self.success_callback = success_callback
        self.exception_callback = exception_callback
        self.update_bar = update_bar

    def run(self, jobs: list = []):
        with self.executor(max_workers=self.max_workers) as _executor:
            futures = {
                _executor.submit(self.func, **job, poison_pill=self.poison_pill): job
                for job in jobs
            }

            try:
                for future in concurrent.futures.as_completed(futures):
                    job = futures[future]
                    try:
                        result = future.result()
                    except Exception as exc:
                        self.exception_callback(exc, job)
                    else:
                        self.success_callback(result)
                    finally:
                        self.update_bar()
            except KeyboardInterrupt:
                logging.info("Finalizando...")
                self.poisoned = True
                raise


def process_document(
    get_document: callable,
    index: str,
    index_command: str,
    collection: str,
    pid: str,
    poison_pill: PoisonPill = PoisonPill(),
):
    if poison_pill.poisoned:
        return

    document = get_document(collection=collection, pid=pid)
    if not document or not document.data:
        raise ArticleMetaDocumentNotFound()

    article_adapter = XyloseArticleExporterAdapter(index, index_command, document)
    return article_adapter.command_function()


def process_extracted_documents(
    get_document:callable,
    index:str,
    index_command:str,
    output_path:pathlib.Path,
    pids_by_collection:typing.Dict[str, list],
) -> None:

    jobs = [
        {
            "get_document": get_document,
            "index": index,
            "index_command": index_command,
            "collection": collection,
            "pid": pid,
        }
        for collection, pids in pids_by_collection.items()
        for pid in pids
    ]

    with tqdm(total=len(jobs)) as pbar:

        def update_bar(pbar=pbar):
            pbar.update(1)

        def write_result(result, path:pathlib.Path=output_path):
            with path.open("a", encoding="utf-8") as fp:
                fp.write(json.dumps(result) + "\n")

        def log_exception(exception, job, logger=logger):
            logger.error(
                "Não foi possível exportar documento '%s': '%s'.",
                job["pid"],
                exception,
            )

        executor = JobExecutor(
            process_document,
            max_workers=4,
            success_callback=write_result,
            exception_callback=log_exception,
            update_bar=update_bar,
        )
        executor.run(jobs)
    return


def articlemeta_parser(sargs):
    """Parser para capturar informações sobre conexão com o Article Meta"""

    class FutureDateAction(argparse.Action):
        def __call__(self, parser, namespace, values, option_string=None):
            date = datetime.datetime.strptime(values, "%d-%m-%Y")
            today = datetime.datetime.today()
            if date > today:
                setattr(namespace, self.dest, today.strftime("%d-%m-%Y"))
            else:
                setattr(namespace, self.dest, values)


    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument(
        "--from-date",
        type=str,
        action=FutureDateAction,
        dest="from_date",
        help="Data inicial de processamento",
    )

    parser.add_argument(
        "--until-date",
        type=str,
        dest="until_date",
        action=FutureDateAction,
        help="Data final de processamento",
    )

    parser.add_argument(
        "--collection",
        type=str,
        help="Coleção do(s) documento(s) publicados",
    )

    parser.add_argument(
        "--pid",
        type=str,
        help="PID do documento",
    )

    parser.add_argument(
        "--pids",
        type=pathlib.Path,
        help="Caminho para arquivo com lista de PIDs de documentos a exportar",
    )

    parser.add_argument(
        "--connection",
        type=str,
        help="Tipo de conexão com Article Meta: Restful ou Thrift",
    )

    parser.add_argument(
        "--domain",
        type=str,
        help="Endereço de conexão com Article Meta",
    )

    return parser


def main_exporter(sargs):
    parser = argparse.ArgumentParser(description="Exportador de documentos")
    parser.add_argument("--loglevel", default="INFO")
    parser.add_argument(
        "--output",
        type=pathlib.Path,
        required=True,
        help="Caminho para arquivo de resultado da exportação",
    )

    subparsers = parser.add_subparsers(title="Index", dest="index", required=True)

    doaj_parser = subparsers.add_parser("doaj", help="Base de indexação DOAJ")
    doaj_export_subparsers = doaj_parser.add_subparsers(
        title="DOAJ Command", dest="doaj_command", required=True,
    )

    doaj_export_subparsers.add_parser(
        "export", help="Exporta documentos", parents=[articlemeta_parser(sargs)],
    )

    doaj_export_subparsers.add_parser(
        "update", help="Atualiza documentos", parents=[articlemeta_parser(sargs)],
    )

    args = parser.parse_args(sargs)

    if not (args.from_date or args.until_date or args.pid or args.pids):
        raise OriginDataFilterError(
            "Informe ao menos uma das datas (from-date ou until-date), pid ou pids"
        )

    # Change Logger level
    level = getattr(logging, args.loglevel.upper())
    logger = logging.getLogger()
    logger.setLevel(level)

    params = {
        "index": args.index,
        "index_command": args.doaj_command,
        "output_path": args.output,
    }

    am_client_params = {}
    if args.connection:
        am_client_params["connection"] = args.connection
    if args.domain:
        am_client_params["domain"] = args.domain

    am_client = AMClient(**am_client_params) if am_client_params else AMClient()
    params["get_document"] = am_client.document

    if args.pid:
        if not args.collection:
            raise OriginDataFilterError(
                "Coleção é obrigatória para exportação de um PID"
            )

        params["pids_by_collection"] = {args.collection: [args.pid]}
    elif args.pids:
        if not args.collection:
            raise OriginDataFilterError(
                "Coleção é obrigatória para exportação de lista de PIDs"
            )

        params["pids_by_collection"] = {
            args.collection: [pid for pid in args.pids.read_text().split("\n") if pid]
        }
    else:
        filter = {}
        if args.collection:
            filter["collection"] = args.collection
        if args.from_date:
            filter["from_date"] = utils.get_valid_datetime(args.from_date)
        if args.until_date:
            filter["until_date"] = utils.get_valid_datetime(args.until_date)

        params["pids_by_collection"] = {}
        docs = am_client.documents_identifiers(**filter)
        for doc in docs or []:
            params["pids_by_collection"].setdefault(doc["collection"], [])
            params["pids_by_collection"][doc["collection"]].append(doc["code"])

    process_extracted_documents(**params)
