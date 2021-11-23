from abc import ABC, abstractmethod


class ExporterInterface(ABC):
    @abstractmethod
    def get_request(self):
        pass


class IndexExporterInterface(ABC):
    @abstractmethod
    def export(self):
        pass
