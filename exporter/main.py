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


class UnmanagedJournalDocument(Exception):
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


class ExporterAdapterMixin:

    @tenacity.retry(
        wait=tenacity.wait_exponential(),
        stop=tenacity.stop_after_attempt(config.get("EXPORT_RUN_RETRIES")),
        retry=tenacity.retry_if_exception_type(
            (requests.ConnectionError, requests.Timeout),
        ),
    )
    def _send_http_request(
        self, request_method: callable, url: str, params: json = None, json: json = None
    ):
        logger.debug("Enviando requisição HTTP %s", url)
        kwargs = {}
        if params:
            kwargs["params"] = params
        if json:
            kwargs["json"] = json
        return request_method(url=url, **kwargs)

    def command_function(self):
        return self._command_function()


class XyloseArticleExporterAdapter(
    ExporterAdapterMixin, interfaces.IndexExporterInterface
):
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
        elif command == "get":
            self._command_function = self._get
        elif command == "delete":
            self._command_function = self._delete
        else:
            raise InvalidExporterInitData(f"Comando informado inválido: {command}")

        self.index = index
        self._pid = article.data.get("code", "")

    @property
    def params_request(self) -> dict:
        return self.index_exporter.params_request

    @property
    def post_request(self) -> dict:
        return self.index_exporter.post_request

    def put_request(self, data: dict) -> dict:
        return self.index_exporter.put_request(data)

    def post_response(self, response: dict) -> dict:
        return self.index_exporter.post_response(response)

    def error_response(self, response: dict) -> dict:
        return self.index_exporter.error_response(response)

    def _export(self):
        resp = self._send_http_request(
            requests.post,
            self.index_exporter.crud_article_put_url,
            self.params_request,
            self.post_request,
        )
        try:
            resp.raise_for_status()
        except HTTPError as exc:
            error_response = ""
            if resp.status_code == 400:
                error_response = " " + self.error_response(resp.json())
            exc_msg = f"Erro na exportação ao {self.index}: {exc}.{error_response}"
            raise IndexExporterHTTPError(exc_msg)
        else:
            export_result = self.post_response(resp.json())
            export_result["pid"] = self._pid
            logger.debug("Resultado do export: %s", export_result)
            return export_result

    def _update(self):
        get_resp = self._send_http_request(
            requests.get,
            self.index_exporter.crud_article_url,
            self.params_request,
        )
        try:
            get_resp.raise_for_status()
        except HTTPError as exc:
            raise IndexExporterHTTPError(
                f"Erro na consulta ao {self.index}: {exc}."
            )
        else:
            put_req = self.put_request(get_resp.json())
            put_resp = self._send_http_request(
                requests.put,
                self.index_exporter.crud_article_url,
                self.params_request,
                put_req,
            )
            try:
                put_resp.raise_for_status()
            except HTTPError as exc:
                error_response = ""
                if put_resp.status_code == 400:
                    error_response = " " + self.error_response(put_resp.json())
                exc_msg = f"Erro ao atualizar o {self.index}: {exc}.{error_response}"
                raise IndexExporterHTTPError(exc_msg)
            else:
                update_result = { "pid": self._pid, "status": "UPDATED" }
                logger.debug("Resultado da atualização: %s", update_result)
                return update_result

    def _get(self):
        get_resp = self._send_http_request(
            requests.get,
            self.index_exporter.crud_article_url,
            self.params_request,
        )
        try:
            get_resp.raise_for_status()
        except HTTPError as exc:
            raise IndexExporterHTTPError(
                f"Erro na consulta ao {self.index}: {exc}."
            )
        else:
            get_result = get_resp.json()
            get_result["pid"] = self._pid
            return get_result

    def _delete(self):
        delete_resp = self._send_http_request(
            requests.delete,
            self.index_exporter.crud_article_url,
            self.params_request,
        )
        try:
            delete_resp.raise_for_status()
        except HTTPError as exc:
            raise IndexExporterHTTPError(
                f"Erro ao deletar no {self.index}: {exc}."
            )
        else:
            delete_result = { "pid": self._pid, "status": "DELETED" }
            logger.debug("Resultado da deleção: %s", delete_result)
            return delete_result


class XyloseArticlesListExporterAdapter(
    ExporterAdapterMixin, interfaces.IndexExporterInterface
):
    index_exporters: typing.List[interfaces.IndexExporterInterface]

    def __init__(
        self, index: str, command: str, articles: typing.Set[scielodocument.Article]
    ):
        if index == "doaj":
            self.index_exporters = [
                {
                    "pid": article.data["code"],
                    "index_exporter": doaj.DOAJExporterXyloseArticle(article)
                }
                for article in articles
            ]
            self.bulk_articles_url = self.index_exporters[0]["index_exporter"].\
                bulk_articles_url
        else:
            raise InvalidExporterInitData(f"Index informado inválido: {index}")

        if command == "export":
            self._command_function = self._export
        elif command == "delete":
            self._command_function = self._delete
        else:
            raise InvalidExporterInitData(f"Comando informado inválido: {command}")

        self.index = index

    @property
    def params_request(self) -> dict:
        return self.index_exporters[0]["index_exporter"].params_request

    @property
    def post_request(self) -> dict:
        return [
            item["index_exporter"].post_request
            for item in self.index_exporters
        ]

    @property
    def delete_request(self) -> dict:
        return [
            item["index_exporter"].id
            for item in self.index_exporters
        ]

    def put_request(self, data: dict) -> dict:
        pass

    def post_response(self, response: dict) -> dict:
        resp = []
        for item, resp_article in zip(self.index_exporters, response):
            new_resp_article = item["index_exporter"].post_response(resp_article)
            new_resp_article["pid"] = item["pid"]
            resp.append(new_resp_article)
        return resp

    def error_response(self, response: dict) -> dict:
        return self.index_exporters[0]["index_exporter"].error_response(response)

    def _export(self) -> dict:
        resp = self._send_http_request(
            requests.post,
            self.bulk_articles_url,
            self.params_request,
            self.post_request,
        )
        try:
            resp.raise_for_status()
        except HTTPError as exc:
            error_response = ""
            if resp.status_code == 400:
                error_response = " " + self.error_response(resp.json())
            exc_msg = f"Erro na exportação ao {self.index}: {exc}.{error_response}"
            raise IndexExporterHTTPError(exc_msg)
        else:
            export_result = self.post_response(resp.json())
            logger.debug("Resultado do export: %s", export_result)
            return export_result

    def _delete(self):
        resp = self._send_http_request(
            requests.delete,
            self.bulk_articles_url,
            self.params_request,
            self.delete_request,
        )
        try:
            resp.raise_for_status()
        except HTTPError as exc:
            error_response = self.error_response(resp.json())
            exc_msg = f"Erro ao deletar no {self.index}: {exc}. {error_response}"
            raise IndexExporterHTTPError(exc_msg)
        else:
            delete_result = [
                { "pid": item["pid"], "status": "DELETED" }
                for item in self.index_exporters
            ]
            logger.debug("Resultado da deleção: %s", delete_result)
            return delete_result


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
                        self.success_callback(result, job)
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

    logger.debug('Executando comando "%s" para PID "%s"', index_command, pid)
    document = get_document(collection=collection, pid=pid)
    if not document or not document.data:
        raise ArticleMetaDocumentNotFound()

    article_adapter = XyloseArticleExporterAdapter(index, index_command, document)
    return article_adapter.command_function()


def log_exception(exception, job, logger=logger):
    logger.error(
        "Não foi possível processar documento '%s': '%s'.",
        job["pid"],
        exception,
    )


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

        def write_result(result, job, path:pathlib.Path=output_path):
            if path.is_dir():
                file_path = path / f'{job["pid"]}.json'
                logger.debug('Gravando resultado em arquivo %s: "%s"', file_path)
                with file_path.open("w", encoding="utf-8") as fp:
                    json.dump(result, fp)
            else:
                logger.debug('Gravando resultado em arquivo %s: "%s"', path, result)
                with path.open("a", encoding="utf-8") as fp:
                    fp.write(json.dumps(result) + "\n")

        executor = JobExecutor(
            process_document,
            max_workers=4,
            success_callback=write_result,
            exception_callback=log_exception,
            update_bar=update_bar,
        )
        executor.run(jobs)
    return


def execute_get_document(
    get_document: callable,
    collection: str,
    pid: str,
    poison_pill: PoisonPill = PoisonPill(),
):
    if poison_pill.poisoned:
        return

    logger.debug('Executando get_document para PID "%s"', pid)
    document = get_document(collection=collection, pid=pid)
    if not document or not document.data:
        raise ArticleMetaDocumentNotFound()

    return document


def process_documents_in_bulk(
    get_document:callable,
    index:str,
    index_command:str,
    output_path:pathlib.Path,
    pids_by_collection:typing.Dict[str, list],
) -> None:
    jobs = [
        { "get_document": get_document, "collection": collection, "pid": pid }
        for collection, pids in pids_by_collection.items()
        for pid in pids
    ]

    documents = set()
    with tqdm(total=len(jobs)) as pbar:

        def update_bar(pbar=pbar):
            pbar.update(1)

        def write_result(result, job, path:pathlib.Path=output_path):
            documents.add(result)

        executor = JobExecutor(
            execute_get_document,
            max_workers=4,
            success_callback=write_result,
            exception_callback=log_exception,
            update_bar=update_bar,
        )
        executor.run(jobs)

    if documents:
        articles_adapter = XyloseArticlesListExporterAdapter(
            index, index_command, documents
        )
        ret = articles_adapter.command_function()

        logger.debug('Gravando resultado em arquivo %s', output_path)
        with output_path.open("w", encoding="utf-8") as fp:
            for line in ret:
                fp.write(json.dumps(line) + "\n")

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
        "--issns", 
        type=pathlib.Path,
        default=set(),
        help="Caminho para arquivo de ISSNs gerenciados",
    )
    parser.add_argument(
        "--output",
        type=pathlib.Path,
        required=True,
        help="Caminho para arquivo de resultado da exportação",
    )

    subparsers = parser.add_subparsers(title="Index", dest="index", required=True)

    doaj_parser = subparsers.add_parser("doaj", help="Base de indexação DOAJ")
    doaj_subparsers = doaj_parser.add_subparsers(
        title="DOAJ Command", dest="doaj_command", required=True,
    )

    doaj_export_parser = doaj_subparsers.add_parser(
        "export", help="Exporta documentos", parents=[articlemeta_parser(sargs)],
    )
    doaj_export_parser.add_argument(
        "--bulk", action="store_true", help="Exporta documentos em lote"
    )

    doaj_subparsers.add_parser(
        "update", help="Atualiza documentos", parents=[articlemeta_parser(sargs)],
    )

    doaj_subparsers.add_parser(
        "get", help="Obtém documentos", parents=[articlemeta_parser(sargs)],
    )

    doaj_delete_parser = doaj_subparsers.add_parser(
        "delete", help="Deleta documentos", parents=[articlemeta_parser(sargs)],
    )
    doaj_delete_parser.add_argument(
        "--bulk", action="store_true", help="Deleta documentos em lote"
    )

    args = parser.parse_args(sargs)

    if not (args.from_date or args.until_date or args.pid or args.pids):
        raise OriginDataFilterError(
            "Informe ao menos uma das datas (from-date ou until-date), pid ou pids"
        )

    # Change Logger level
    level = getattr(logging, args.loglevel.upper())
    logging.basicConfig(level=level, **config.INITIAL_LOG_CONFIG)
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

    if getattr(args, "bulk", None):
        process_documents_in_bulk(**params)
    else:
        process_extracted_documents(**params)
