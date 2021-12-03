from datetime import datetime


def utcnow():
    return str(datetime.utcnow().isoformat() + "Z")
