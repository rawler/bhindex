# -*- coding: utf-8 -*-

from os import environ, path
from warnings import warn
from ConfigParser import SafeConfigParser as ConfigParser, NoOptionError
from socket import gethostname

CODE_PATH = path.dirname(path.dirname(path.abspath(__file__)))
if path.exists(path.join(CODE_PATH, 'bhindex.py')):
    BHINDEX_DEFAULT_PATH = CODE_PATH
else:
    BHINDEX_DEFAULT_PATH = path.expanduser('~/.bhindex')

BHINDEX_PATH = environ.get('BHINDEX_PATH', BHINDEX_DEFAULT_PATH)

CONFIG_DEFAULTS = {
    "DB": {
        "file": path.join(BHINDEX_PATH, 'bhindex.sqlite'),
    },
    "BITHORDE": {
        "fusedir": "/tmp/bhfuse",
        "address": "/tmp/bithorde",
        "pressure": "10",
        "upload_link": "false",
        "asset_timeout": "1000",
    },
    "LINKSEXPORT": {
        "linksdir": "",
    },
    "LIVESYNC": {
        "name": gethostname(),
        "db_poll_interval": "1.0",
        "connect": "",
        "port": "4000",
    },
}

MY_CONFIG_PATH = path.join(BHINDEX_PATH, 'my.config')
BHINDEX_CONFIG_PATH = path.join(BHINDEX_PATH, 'bhindex.conf')
CONFIG_LOCATIONS = [
    BHINDEX_CONFIG_PATH,
    '/etc/bhindex.conf',
]


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
                warn("Deprecation: %s.%s in config renamed to %s.%s" % (old_section, old_option, section, option))
        return res

    def items(self, section, raw=None, vars=None):
        res = dict(ConfigParser.items(self, section, raw, vars))
        for (new_section, new_option), (old_section, old_option) in self.deprecations.iteritems():
            if section == new_section:
                if new_option in res:
                    continue
                try:
                    value = self.get(old_section, old_option)
                    warn("Deprecation: %s.%s in config renamed to %s.%s" % (old_section, old_option, section, new_option))
                    res[new_option] = value
                except NoOptionError:
                    continue
        return res


class ConfigNotFoundError(Exception):
    pass


def deprecated_config_location(what):
    warn("%s is deprecated. Please install in one of (%s)" % (what, '|'.join(CONFIG_LOCATIONS)))


def locate_config():
    env_path = environ.get('BHINDEX_CONF', None)
    if env_path:
        return env_path

    if path.exists(MY_CONFIG_PATH):
        deprecated_config_location("my.config in source-directory")
        return MY_CONFIG_PATH
    for config_path in CONFIG_LOCATIONS:
        if path.exists(config_path):
            return config_path
    raise ConfigNotFoundError("Could not locate config file neither in %s nor in $BHINDEX_CONF" % '|'.join(CONFIG_LOCATIONS))


def read(configfile=None):
    if configfile is None:
        configfile = locate_config()
    config = VersioningConfigParser(deprecations={
        ('BITHORDE', 'address'): ('BITHORDE', 'unixsocket')
    })
    for section, options in CONFIG_DEFAULTS.iteritems():
        config.add_section(section)
        for option, value in options.iteritems():
            config.set(section, option, value)
    config.set(None, 'here', path.dirname(configfile))
    config.read(configfile)
    return config
