import os
import sys

_default = dict(
    DOAJ_API_URL="https://doaj.org/api/",
)

def get(var_name: str):
    return os.environ.get(var_name, _default.get(var_name, ""))
