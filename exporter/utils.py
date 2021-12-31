from datetime import datetime
from pathlib import Path
from xylose import scielodocument


class ISSNFileError(Exception):
    pass


def utcnow():
    return str(datetime.utcnow().isoformat() + "Z")


def get_valid_datetime(strdate: str) -> datetime:
    try:
        date = datetime.strptime(strdate, "%d-%m-%Y")
    except ValueError as exc_info:
        raise ValueError("Data inválida. Formato esperado: DD-MM-YYYY") from None
    else:
        return date


def is_valid_issn(issn: str):
    if len(issn) == 9 and '-' in issn[4]:
        return True
    return False


def extract_issns_from_file(issns: Path):
    try:
        with open(issns) as fin:
            return set([i.strip() for i in fin if is_valid_issn(i.strip())])
    except:
        raise ISSNFileError()

