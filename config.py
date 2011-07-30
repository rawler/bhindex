# -*- coding: utf-8 -*-

import os.path as path
from ConfigParser import SafeConfigParser as ConfigParser

HERE = path.dirname(__file__)
DEFAULT_CONFIG = path.join(HERE, 'my.config')

CONFIG_DEFAULTS = {
    "DB": {
        "file": "bhindex.db",
    },
    "BITHORDE": {
        "fusedir": "/tmp/bhfuse",
        "unixsocket": "/tmp/bithorde",
        "pressure": "10",
    },
}

def read(configfile=DEFAULT_CONFIG):
    config = ConfigParser()
    for section, options in CONFIG_DEFAULTS.iteritems():
        config.add_section(section)
        for option, value in options.iteritems():
            config.set(section, option, value)
    config.set(None, 'here', path.dirname(configfile))
    config.read(configfile)
    return config
