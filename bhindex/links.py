from __future__ import absolute_import

import argparse
import os
import os.path as path
from time import time

from db import ANY, DB, ValueSet
from bithorde import Client, parseConfig

from .tree import Filesystem
from .util import cachedAssetLiveChecker

import config
config = config.read()

DEFAULT_LINKSDIR = path.normpath(config.get('LINKSEXPORT', 'linksdir'))
DEFAULT_BHFUSEDIR = config.get('BITHORDE', 'fusedir')


class LinksWriter(object):
    @staticmethod
    def _ensuredir(dir):
        if not path.exists(dir):
            old_umask = os.umask(0o022)  # u=rwx,g=rx,o=rx
            try:
                os.makedirs(dir)
            except OSError as e:
                print "Failed to create directory %s:" % dir
                print "  ", e
                return False
            finally:
                os.umask(old_umask)
        return True

    def __init__(self, linksdir, bhfusedir):
        self.linksdir = os.path.normpath(linksdir)
        self.bhfuse = os.path.normpath(bhfusedir)

    def __call__(self, p, t):
        linkpath = path.join(self.linksdir, p)
        tgt = path.join(self.bhfuse, t)

        if (not linkpath.startswith(self.linksdir)) or len(linkpath) <= len(self.linksdir):
            print "Warning! %s (%s) tries to break out of directory!" % (linkpath, tgt)
            return False
        print u"Linking %s -> %s".encode('utf-8') % (p, t)

        try:
            oldtgt = os.readlink(linkpath)
        except OSError:
            oldtgt = None

        if oldtgt == tgt:
            return True
        elif oldtgt:
            try:
                os.unlink(linkpath)
            except OSError as e:
                print "Failed to remove old link %s:" % oldtgt
                print "  ", e
                return False

        dstdir = path.dirname(linkpath)
        if not self._ensuredir(dstdir):
            return False

        try:
            os.symlink(tgt, linkpath)
        except OSError as e:
            print "Failed to create link at %s:" % linkpath
            print "  ", e
            return False
        return True


def path_in_prefixes(path, prefixes):
    for prefix in prefixes:
        if path.startswith(prefix) and len(path) > len(prefix):
            return True
    return False


class FilteredExporter(object):
    def __init__(self, output, prefixes):
        self.output = output
        self.prefixes = prefixes

    def __call__(self, p, tgt):
        if self.prefixes and not path_in_prefixes(p, self.prefixes):
            return True

        return self.output(p, tgt)


class DBExporter(object):
    def __init__(self, db, bithorde, tgt):
        self.db = db
        self.fs = Filesystem(db)
        self.bithorde = bithorde
        self.tgt = tgt

    def write_links(self, obj):
        xt_spec = "&".join("xt=urn:" + xt for xt in obj['xt'])
        tgt = "magnet:?" + xt_spec
        return all(self.tgt("/".join(p), tgt) for p in self.fs.paths_for(obj))

    def export(self, force_all):
        t = time()
        count = size = 0

        crit = {'directory': ANY, 'xt': ANY}
        if not force_all:
            crit['@linked'] = None
        objs = self.db.query(crit)
        for obj, status_ok in cachedAssetLiveChecker(self.bithorde, objs, db=self.db):
            if not status_ok:
                continue

            if self.write_links(obj):
                fsize = int(obj.any('filesize') or 0)
                # Reasonably sized file
                if fsize and (fsize < 1024 * 1024 * 1024 * 1024):
                    count += 1
                    size += int(fsize)

                obj[u'@linked'] = ValueSet((u'true',), t=t)
                with self.db.transaction():
                    self.db.update(obj)
        return count, size


def export(db, bithorde, output_dir=DEFAULT_LINKSDIR, bhfusedir=DEFAULT_BHFUSEDIR, prefix=None, force_all=False):
    tgt = LinksWriter(output_dir, bhfusedir)
    if prefix:
        tgt = FilteredExporter(tgt, prefix)

    count, size = DBExporter(db, bithorde, tgt).export(force_all)

    print("Exported %s assets totaling %s GB" %
          (count, size / (1024 * 1024 * 1024)))


def prepare_args(parser):
    parser.add_argument("-T", "--force-all", action="store_true",
                        help="Export ALL links, even links marked in DB as already exported")
    parser.add_argument("-o", "--output-dir", action="store",
                        default=DEFAULT_LINKSDIR,
                        help="Directory to write links in. Default is read from config")
    parser.add_argument("-t", "--target-dir", action="store",
                        default=DEFAULT_BHFUSEDIR,
                        help="Directory to point symlinks to. Defaults to look for bhfuse amount mounted filesystems")
    parser.add_argument("prefix", nargs='*',
                        help="List of prefixes to export links for")
    parser.set_defaults(main=main)


def main(args):
    if not args.output_dir:
        raise argparse.ArgumentError(
            None, "Needs link output-dir in either config or as argument\n")

    db = DB(config.get('DB', 'file'))
    bithorde = Client(parseConfig(config.items('BITHORDE')))

    return export(db, bithorde, args.output_dir, args.target_dir, args.prefix, args.force_all)
