from abc import ABC, abstractmethod


class IndexExporterInterface(ABC):
    @abstractmethod
    def post_request(self) -> dict:
        pass

    @abstractmethod
    def post_response(self, response: dict) -> dict:
        pass

    @abstractmethod
    def export(self):
        pass
