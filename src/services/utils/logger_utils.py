import os
import logging


LOGGER_FORMAT = os.environ.get("LOGGER_FORMAT", "[%(asctime)s][%(name)s][%(levelname)s] %(message)s")
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

general_logger = None
def get_general_logger() -> logging.Logger:
    """
    Initialize the general logger with a StreamHandler.
    """
    global general_logger
    if general_logger is None:
        general_logger = logging.getLogger("general")
        formatter = logging.Formatter("%(message)s")
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        general_logger.addHandler(handler)
        general_logger.setLevel(LOG_LEVEL)
    return general_logger


def getLogger(file: str) -> logging.Logger:
    """
    Get a logger with the specified name.
    :param file: Name of the logger (use __file__ ).
    :return: Logger instance

    example: getLogger(__file__)
    """
    general_logger = get_general_logger()
    general_logger.info(f"Initializing logger for {os.path.basename(file)}, level: {LOG_LEVEL} format: {LOGGER_FORMAT}")
    logger = logging.getLogger(os.path.basename(file).split(".")[0])

    formatter = logging.Formatter(LOGGER_FORMAT)
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    logger.setLevel(LOG_LEVEL)
    return logger


def hline(length=80) -> None:
    """
    Print a horizontal line to the console.
    """

    logger = get_general_logger()
    logger.info("-" * length)
