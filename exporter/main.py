import typing
import concurrent.futures
import logging
import argparse
import pathlib
import json

import tenacity
import requests
from requests.exceptions import HTTPError
from tqdm import tqdm
import articlemeta.client as articlemeta_client
from xylose import scielodocument

from exporter import interfaces, doaj, config


logger = logging.getLogger(__name__)


class ArticleMetaDocumentNotFound(Exception):
    pass


class InvalidIndexExporter(Exception):
    pass


class IndexExporterHTTPError(Exception):
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


class XyloseArticleExporterAdapter(interfaces.IndexExporterInterface):
    index_exporter: interfaces.IndexExporterInterface

    def __init__(self, index: str, article: scielodocument.Article):
        if index == "doaj":
            self.index_exporter = doaj.DOAJExporterXyloseArticle(article)
        else:
            raise InvalidIndexExporter()
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
    def _http_post_articles(self):
        return requests.post(
            self.index_exporter.crud_article_url, data=self.post_request
        )

    def export(self):
        resp = self._http_post_articles()
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


def export_document(
    get_document: callable,
    index: str,
    collection: str,
    pid: str,
    poison_pill: PoisonPill = PoisonPill(),
):
    if poison_pill.poisoned:
        return

    document = get_document(collection=collection, pid=pid)
    if not document or not document.data:
        raise ArticleMetaDocumentNotFound()

    article_adapter = XyloseArticleExporterAdapter(index, document)
    return article_adapter.export()


def extract_and_export_documents(
    index:str,
    collection:str,
    output_path:str,
    pids:typing.List[str],
    connection:str=None,
    domain:str=None,
) -> None:
    params = {}
    if connection:
        params["connection"] = connection
    if domain:
        params["domain"] = domain

    am_client = AMClient(**params) if params else AMClient()

    jobs = [
        {"get_document": am_client.document, "index": index, "collection": collection, "pid": pid}
        for pid in pids
    ]

    with tqdm(total=len(pids)) as pbar:

        def update_bar(pbar=pbar):
            pbar.update(1)

        def write_result(result, path=output_path):
            output_file = pathlib.Path(path)
            output_file.write_text(json.dumps(result) + "\n")

        def log_exception(exception, job, logger=logger):
            logger.error(
                "Não foi possível exportar documento '%s': '%s'.",
                job["pid"],
                exception,
            )

        executor = JobExecutor(
            export_document,
            max_workers=4,
            success_callback=write_result,
            exception_callback=log_exception,
            update_bar=update_bar,
        )
        executor.run(jobs)
    return


def main_exporter(sargs):
    parser = argparse.ArgumentParser(description="Exportador de documentos")
    parser.add_argument("--loglevel", default="INFO")
    parser.add_argument(
        "--output",
        required=True,
        help="Caminho para arquivo de resultado da exportação",
    )

    subparsers = parser.add_subparsers(title="Index", metavar="", dest="index")

    doaj_parser = subparsers.add_parser(
        "doaj", help="Base de indexação DOAJ"
    )

    doaj_parser.add_argument(
        "--collection",
        type=str,
        help="Coleção do(s) documento(s) publicados",
    )

    doaj_parser.add_argument(
        "--pid",
        type=str,
        help="PID do documento",
    )

    doaj_parser.add_argument(
        "--pids",
        help="Caminho para arquivo com lista de PIDs de documentos a exportar",
    )

    args = parser.parse_args(sargs)

    # Change Logger level
    level = getattr(logging, args.loglevel.upper())
    logger = logging.getLogger()
    logger.setLevel(level)

    params = {
        "index": args.index, "collection": args.collection, "output_path": args.output
    }
    if args.pid:
        params["pids"] = [args.pid]
    elif args.pids:
        pidsfile = pathlib.Path(args.pids)
        params["pids"] = [pid for pid in pidsfile.read_text().split("\n") if pid]

    extract_and_export_documents(**params)
