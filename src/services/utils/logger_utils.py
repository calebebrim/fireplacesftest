import os
import logging

from functools import lru_cache

LOGGER_FORMAT = os.environ.get(
    "LOGGER_FORMAT", "[%(asctime)s][%(name)s][%(levelname)s] %(message)s"
)
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
LOG_LEVEL_MAPPINGS = os.environ.get(
    "LOG_LEVEL_MAPPINGS", "general:INFO,logger_utils:INFO"
)


@lru_cache(maxsize=1)
def parse_log_level_mappings() -> dict[str, str]:
    return {
        map.split(":")[0]: map.split(":")[1] for map in LOG_LEVEL_MAPPINGS.split(",")
    }


def effective_log_level(file) -> str:
    map = parse_log_level_mappings()
    return map.get(file, LOG_LEVEL)


general_logger = None
def get_general_logger() -> logging.Logger:
    """
    Initialize the general logger with a StreamHandler.
    """
    global general_logger
    if general_logger is None:
        general_logger = logging.getLogger(os.path.basename(__file__).split(".")[0])

        formatter = logging.Formatter("%(message)s")
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        general_logger.addHandler(handler)
        general_logger.setLevel(effective_log_level(general_logger.name))
    return general_logger


def getLogger(file: str) -> logging.Logger:
    """
    Get a logger with the specified name.
    :param file: Name of the logger (use __file__ ).
    :return: Logger instance

    example: getLogger(__file__)
    """
    general_logger = get_general_logger()
    logger = logging.getLogger(os.path.basename(file).split(".")[0])

    formatter = logging.Formatter(LOGGER_FORMAT)
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    logger.setLevel(effective_log_level(logger.name))
    general_logger.debug(
        f"Initializing logger for {os.path.basename(file)}, level: {effective_log_level(logger.name)} format: {LOGGER_FORMAT}"
    )
    return logger


def hline(
    ln=80, char: str = "-", header="", as_debug=False, as_error=False, as_warning=False
) -> None:
    """
    Print a horizontal line to the console.
    """

    _h = f" {header} " if header else ""

    def half_line():
        return char * (int(ln / 2) - int(len(_h) / 2))

    logger = get_general_logger()
    content = half_line() + _h + half_line()

    if as_debug:
        logger.debug(content)
    elif as_error:
        logger.error(content)
    elif as_warning:
        logger.warning(content)
    else:
        logger.info(content)
