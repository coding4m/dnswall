"""

"""

import logging

logging.basicConfig(
    format='%(asctime)-15s [%(threadName)s] %(levelname)s %(name)s - %(message)s')

_logger_methods = {
    'debug': 'd',
    'info': 'i',
    'warn': 'w',
    'error': 'e'
}


def get_logger(name, level=logging.WARN):
    """

    :param name:
    :param level:
    :return:
    """
    _logger = logging.getLogger(name)
    _logger.setLevel(level)
    for item in _logger_methods.items():
        try:
            m = getattr(_logger, item[0])
            if m and not hasattr(_logger, item[1]):
                setattr(_logger, item[1], m)
        except AttributeError:
            pass

    def _ex(msg, *args, **kwargs):
        kwargs['exc_info'] = True
        _logger.error(msg, *args, **kwargs)

    if not hasattr(_logger, 'ex'):
        setattr(_logger, 'ex', _ex)
    return _logger
