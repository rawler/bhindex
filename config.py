# -*- coding: utf-8 -*-

import os.path as path
from ConfigParser import SafeConfigParser as ConfigParser

HERE = path.dirname(__file__)
DEFAULT_CONFIG = path.join(HERE, 'my.config')

def read(configfile=DEFAULT_CONFIG):
    config = ConfigParser()
    config.set(None, 'here', path.dirname(configfile))
    config.read(configfile)
    return config
