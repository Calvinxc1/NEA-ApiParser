import logging
from sys import stdout, stderr

def init_root_logger(level=logging.WARN):
    logger = logging.getLogger(__name__.split('.')[0])
    logger.setLevel(level)

    formatter = logging.Formatter(
        '%(asctime)s - %(task)s | %(name)s-%(funcName)s\n'
        '[%(levelname)s] %(message)s\n'
        '----------------'
    )

    info_handler = logging.StreamHandler(stdout)
    info_handler.addFilter(lambda x: x.levelno <= logging.INFO)
    info_handler.setFormatter(formatter)
    logger.addHandler(info_handler)

    error_handler = logging.StreamHandler(stderr)
    error_handler.addFilter(lambda x: x.levelno > logging.INFO)
    error_handler.setFormatter(formatter)
    logger.addHandler(error_handler)
    
    logger = logging.LoggerAdapter(logger, {'proc': 'root'})
    
    return logger
