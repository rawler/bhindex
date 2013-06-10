# -*- coding: utf-8 -*-

import os.path as path
from ConfigParser import SafeConfigParser as ConfigParser

HERE = path.dirname(__file__)
DEFAULT_CONFIG = path.join(HERE, 'my.config')

class VersioningConfigParser(ConfigParser):
    def __init__(self, deprecations):
        self.deprecations = deprecations
        ConfigParser.__init__(self)

    def get(self, section, option):
        res = ConfigParser.get(self, section, option)
        if not res and (section, option) in self.deprecations:
            old_section, old_option = self.deprecations[(section, option)]
            res = self.get(old_section, old_option)
            if res:
                print "WARNING, Deprecation: %s.%s in config renamed to %s.%s" % (old_section, old_option, section, option)
        return res

    def items(self, section, raw=None, vars=None):
        raise NotImplementedError


CONFIG_DEFAULTS = {
    "DB": {
        "file": path.join(HERE,"bhindex.sqlite"),
    },
    "BITHORDE": {
        "fusedir": "/tmp/bhfuse",
        "address": "/tmp/bithorde",
        "pressure": "10",
        "upload_link": "false",
        "asset_timeout": "1000",
    },
    "TXTSYNC": {
        "asset_import_timeout": "2500",
    },
}

def read(configfile=DEFAULT_CONFIG):
    config = VersioningConfigParser(deprecations={
        ('BITHORDE','address'): ('BITHORDE','unixsocket')
    })
    for section, options in CONFIG_DEFAULTS.iteritems():
        config.add_section(section)
        for option, value in options.iteritems():
            config.set(section, option, value)
    config.set(None, 'here', path.dirname(configfile))
    config.read(configfile)
    return config
