import sys
import logging

from .main import (
    AMClient,
    process_extracted_documents,
    process_documents_in_bulk,
    main_exporter,
)

__all__ = [
    "AMClient",
    "process_extracted_documents",
    "process_documents_in_bulk",
]


logger = logging.getLogger(__name__)


def export_documents():
    try:
        sys.exit(main_exporter(sys.argv[1:]))
    except KeyboardInterrupt:
        # É convencionado no shell que o programa finalizado pelo signal de
        # código N deve retornar o código N + 128.
        sys.exit(130)
    except Exception as exc:
        logger.exception(
            "erro durante a execução da função 'main_exporter' com os args %s",
            sys.argv[1:],
        )
        sys.exit("Um erro inexperado ocorreu: %s" % exc)
