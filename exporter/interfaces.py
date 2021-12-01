from abc import ABC, abstractmethod


class IndexExporterInterface(ABC):
    @abstractmethod
    def post_request(self):
        pass

    @abstractmethod
    def export(self):
        pass
