#!/usr/bin/python
"""
Load Config File
"""
import ConfigParser


CONF_FILE = '/tmp/autoscale.conf'


def get_config():
    """
    Return config object
    """
    config = ConfigParser.RawConfigParser()
    return config.read(CONF_FILE)
