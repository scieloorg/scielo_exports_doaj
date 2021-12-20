import os
import sys


INITIAL_LOG_CONFIG = {
    "format": "%(asctime)s %(levelname)-5.5s [%(name)s] %(message)s",
    "filename": "exporter.log",
}


_default = dict(
    DOAJ_API_URL="https://doaj.org/api/",
    EXPORT_RUN_RETRIES=3,
)


def get(var_name: str):
    return os.environ.get(var_name, _default.get(var_name, ""))
