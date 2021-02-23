import logging

LEVEL = logging.DEBUG

def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(LEVEL)
    ch = logging.StreamHandler()
    ch.setLevel(LEVEL)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    logger.propagate=False
    return logger
