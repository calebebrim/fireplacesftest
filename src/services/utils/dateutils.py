from src.services.utils.logger_utils import getLogger
from datetime import datetime

logger = getLogger(__file__)

def try_dateformats(date_str: str | None, formats: list[str]) -> datetime | None: 
    """
    Try to parse a date string with multiple formats.

    :param date_str: The date string to parse.
    :param formats: A list of date formats to try.
    :return: The parsed date if successful, None otherwise.
    """
    from datetime import datetime
    if not date_str:
        logger.error("Empty date string provided.")
        return None
    if not formats:
        logger.error("No date formats provided.")
        return None
    if not isinstance(formats, list):
        logger.error(f"Expected formats to be a list, got {type(formats)}")
        return None

    
    logger.debug(f"Trying to parse date: {date_str} with formats: {formats}")
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    logger.error(f"Failed to parse date: {date_str} with formats: {formats}")
    return None