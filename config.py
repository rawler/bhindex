# -*- coding: utf-8 -*-

import os.path as path
from ConfigParser import SafeConfigParser as ConfigParser

HERE = path.dirname(__file__)
DEFAULT_CONFIG = path.join(HERE, 'my.config')

CONFIG_DEFAULTS = {
    "DB": {
        "file": path.join(HERE,"bhindex.sqlite"),
    },
    "BITHORDE": {
        "fusedir": "/tmp/bhfuse",
        "unixsocket": "/tmp/bithorde",
        "pressure": "10",
        "upload_link": "false",
        "asset_timeout": "1000",
    },
    "TXTSYNC": {
        "asset_import_timeout": "2500",
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
