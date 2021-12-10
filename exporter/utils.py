from datetime import datetime


def utcnow():
    return str(datetime.utcnow().isoformat() + "Z")


def get_valid_datetime(strdate: str) -> datetime:
    try:
        date = datetime.strptime(strdate, "%d-%m-%Y")
    except ValueError as exc_info:
        raise ValueError("Data inválida. Formato esperado: DD-MM-YYYY") from None
    else:
        return date
