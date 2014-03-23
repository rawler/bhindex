# -*- coding: utf-8 -*-

import os.path as path
from ConfigParser import SafeConfigParser as ConfigParser
from socket import gethostname

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
        res = ConfigParser.items(self, section, raw, vars)
        for (new_section, new_key), (old_section, old_key) in self.deprecations.iteritems():
            if section == new_section:
                value = self.get(old_section, old_key)
                if value:
                    print "WARNING, Deprecation: %s.%s in config renamed to %s.%s" % (old_section, old_option, section, option)
                    if new_key not in res:
                        res[new_key] = value
        return res

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
    "LIVESYNC": {
        "name": gethostname(),
        "db_poll_interval": "1.0",
        "connect": "",
        "port": "4000",
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
