import logging

LEVEL = logging.DEBUG

def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(LEVEL)
    if 0:
        ch = logging.StreamHandler()
        ch.setLevel(LEVEL)
        # create formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        # add formatter to ch
        ch.setFormatter(formatter)
        # add ch to logger
        logger.addHandler(ch)
    return logger
