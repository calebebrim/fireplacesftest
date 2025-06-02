from src.services.utils.logger_utils import getLogger
from datetime import datetime

logger = getLogger(__file__)


def try_strptime(date_str: str | None, formats: list[str]) -> datetime:
    """
    Try to parse a date string with multiple formats.

    :param date_str: The date string to parse.
    :param formats: A list of date formats to try.
    :return: The parsed date if successful, None otherwise.
    """
    if not date_str:
        raise ValueError("Empty date string provided.")
    if not formats:
        raise ValueError("No date formats provided.")
    if not isinstance(formats, list):
        raise ValueError(f"Expected formats to be a list, got {type(formats)}")

    logger.debug(f"Trying to parse date: {date_str} with formats: {formats}")
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    raise ValueError(f"Failed to parse date: {date_str} with formats: {formats}")


def try_strftime(date: datetime | None, formats: list[str]) -> str:
    """
    Try to parse a date string with multiple formats.

    :param date_str: The date string to parse.
    :param formats: A list of date formats to try.
    :return: The parsed date if successful, None otherwise.
    """
    from datetime import datetime

    if not date:
        logger.error("Empty date string provided.")
        return ""
    if not formats:
        logger.error("No date formats provided.")
        return ""
    if not isinstance(formats, list):
        logger.error(f"Expected formats to be a list, got {type(formats)}")
        return ""

    logger.debug(f"Trying to parse date: {date} with formats: {formats}")
    for fmt in formats:
        try:
            return date.strftime(fmt)
        except ValueError:
            continue
    logger.error(f"Failed to parse date: {date} with formats: {formats}")
    return ""
