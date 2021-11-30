from abc import ABC, abstractmethod


class IndexExporterInterface(ABC):
    @abstractmethod
    def export(self):
        pass
