import concurrent.futures
import logging

import articlemeta.client as articlemeta_client
from xylose import scielodocument


logger = logging.getLogger(__name__)


class ArticleMetaDocumentNotFound(Exception):
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
    collection: str,
    pid: str,
    poison_pill: PoisonPill = PoisonPill(),
):
    if poison_pill.poisoned:
        return

    document = get_document(collection=collection, pid=pid)
    if not document or not document.data:
        raise ArticleMetaDocumentNotFound()


def extract_and_export_documents(
    collection:str, pids:typing.List[str], connection:str=None, domain:str=None
) -> scielodocument.Article:
    params = {}
    if connection:
        params["connection"] = connection
    if domain:
        params["domain"] = domain

    am_client = AMClient(**params) if params else AMClient()

    return
