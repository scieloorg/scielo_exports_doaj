import articlemeta.client as articlemeta_client
from xylose import scielodocument


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
