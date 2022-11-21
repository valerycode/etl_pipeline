import logging
from functools import wraps
from time import sleep

logger = logging.getLogger(__name__)

EXECUTION_ERROR = "Error execute function {name}: {error}"
MAX_ATTEMPTS = "Number of attempts transcend the limit."


def backoff(
    exceptions: tuple,
    start_sleep_time: float = 0.1,
    factor: int = 2,
    border_sleep_time: int = 10,
    max_attempts: int = 5,
    logger=logger,
):
    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            sleep_time = start_sleep_time
            attempts = 1
            while attempts <= max_attempts:
                try:
                    return func(*args, **kwargs)
                except exceptions as error:
                    logger.error(
                        EXECUTION_ERROR.format(name=func.__name__, error=error)
                    )
                    if sleep_time < border_sleep_time:
                        sleep_time = sleep_time * factor**attempts
                    else:
                        sleep_time = border_sleep_time
                    sleep(sleep_time)
                    attempts += 1
            logger.error(MAX_ATTEMPTS)
            exit()

        return inner

    return func_wrapper
