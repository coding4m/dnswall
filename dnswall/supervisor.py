import functools
import threading
import time

from dnswall import loggers

_logger = loggers.get_logger('d.s.supervise')


def supervise(min_seconds=None, max_seconds=None):
    """

    :param min_seconds:
    :param max_seconds:
    :return:
    """

    def decorator(function):
        @functools.wraps(function)
        def wrapped(*args, **kwargs):

            retry_seconds = min_seconds
            next_retry_seconds = retry_seconds

            current_thread = threading.current_thread()
            while current_thread.is_alive():
                try:
                    return function(*args, **kwargs)
                except KeyboardInterrupt:
                    _logger.w('thread interrupted, stop supervise and exit.')
                    return None
                except:
                    _logger.ex('function call occurs error.')
                    _logger.w('sleep %d seconds and retry again.', retry_seconds)

                    time.sleep(retry_seconds)
                    next_retry_seconds *= 2
                    if next_retry_seconds > max_seconds:
                        next_retry_seconds = min_seconds
                    retry_seconds = next_retry_seconds

        return wrapped

    return decorator
