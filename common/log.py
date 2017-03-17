#!/usr/bin/python

from logging import handlers
import logging
import config

CONF = config.get_config()
MAP_LOGLEVEL = {'debug': logging.DEBUG,
                'warning': logging.WARNING,
                'info': logging.INFO,
                'critical': logging.CRITICAL,
                'error': logging.ERROR}

def setup_log(name):
    logger = logging.getLogger(name)
    logger.setLevel(MAP_LOGLEVEL[CONF.get('logging', 'log_level')])
    file_handler = logging.FileHandler(CONF.get('logging', 'log_file'))
    formater = logging.Formatter('%(asctime)s -'
                                 '%(name)s - %(levelname)s - %(message)s')

    file_handler.setFormatter(formater)
    logger.addHandler(file_handler)
    return logger


if __name__ == "__main__":
    LOG = setup_log(__name__)
    LOG.info("Welcome to  Logging")
    LOG.debug("A debugging message")
    LOG.warning("A warning occurred")
    LOG.error("An error occurred")
    LOG.exception("An Exception occurred")
