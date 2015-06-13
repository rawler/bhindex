#!/usr/bin/env python

from __future__ import division, print_function, absolute_import

import atexit, os, sys, warnings

import fusell
import errno
import stat
import os.path as path
from time import time
from types import GeneratorType
import sqlite3
import logging
from collections import defaultdict

import itertools

from util import hasValidStatus, timed

from bithorde.eventlet import Client, parseConfig
from bithorde import parseHashIds, message

log = logging.getLogger()

# For Python 2 + 3 compatibility
if sys.version_info[0] == 2:
    def next(it):
        return it.next()
else:
    buffer = memoryview

current_uid = os.getuid()
current_gid = os.getgid()

ino_source = itertools.count(1)
fh_source = itertools.count(1)

class Pool(set):
    def __init__(self, create):
        self._create = create
        set.__init__(self)
    def get(self):
        try:
            return self.pop()
        except KeyError:
            return self._create()
    def put(self, x):
        self.add(x)

ino_pool = Pool(create=lambda: next(ino_source))
fh_pool = Pool(create=lambda: next(fh_source))

class INode(object):
    MODE_0755 = stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH
    MODE_0555 = stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH

    def __init__(self):
        super(INode, self).__init__()
        self.ino = ino_pool.get()

    def __del__(self):
        ino_pool.put(self.ino)

    def entry(self):
        entry = fusell.fuse_entry_param()
        entry.ino = self.ino
        entry.generation = 0
        entry.entry_timeout = 2
        entry.attr_timeout = 10

        entry.attr = self.attr()

        return entry

    def attr(self):
        attr = fusell.c_stat()

        attr.st_ino = self.ino
        attr.st_mode = self.MODE_0555
        attr.st_nlink = 1
        attr.st_uid = current_uid
        attr.st_gid = current_gid
        attr.st_rdev = 1
        attr.st_size = 0

        attr.st_blksize = 512
        attr.st_blocks = 1
        now = time()
        attr.st_atime = now
        attr.st_mtime = now
        attr.st_ctime = now

        return attr

class Timed:
    def __init__(self, tag):
        self.tag = tag

    def __enter__(self):
        self.start = time()
        return self

    def __exit__(self, type, value, traceback):
        delta = (time() - self.start) * 1000
        log.debug("<%s>: %.1fms" % (self.tag, delta))

def timed(method):
    def timed(*args, **kw):
        with Timed("%r (%r, %r)" % (method.__name__, args, kw)):
            res = method(*args, **kw)
            if isinstance(res, GeneratorType):
                return list(res)
            else:
                return res

        return result

    return timed

import db, config
config = config.read()
DB=db.open(config.get('DB', 'file'))

fields=set((u'directory', u'name', u'ext', u'xt', u'bh_status', u'bh_status_confirmed', u'bh_availability', u'filesize'))
def scan(directory_obj):
    dir_prefix = directory_obj.id+'/'
    for obj in DB.query({u'directory': db.Starts(dir_prefix)}, fields=fields):
        name_found = 0
        for directory in obj['directory']:
            if directory.startswith(dir_prefix):
                name = directory[len(dir_prefix):]
                if name:
                    name_found += 1
                    yield name, obj

        if not name_found and obj.any('name'):
            name = obj.any('name')
            ext = obj.any('ext')
            if ext:
                if ext.startswith('.'):
                    name += ext
                else:
                    name += ".%s" % obj.any('ext')
            yield name, obj

def map_objects(objs):
    if any(o.any('xt') for o in objs):
        if len(objs) > 1:
            warnings.warn("TODO: Merge NON-directories")
        else:
            return File(objs[0])
    else:
        return Directory(objs)


class File(INode):
    def __init__(self, obj):
        super(File, self).__init__()
        self.obj = obj

    def attr(self):
        attr = super(File, self).attr()
        attr.st_mode |= stat.S_IFREG
        attr.st_size = int(self.obj.any(u'filesize', 0))
        return attr

    def is_available(self):
        return hasValidStatus(self.obj)

    def ids(self):
        return parseHashIds(self.obj['xt'])

class Symlink(INode):
    def __init__(self, obj):
        super(Symlink, self).__init__()
        self.obj = obj

    def attr(self):
        attr = super(Symlink, self).attr()
        attr.st_mode |= stat.S_IFLNK
        return attr

    def readlink(self):
        return (u"/tmp/bhfuse/magnet:?xt=urn:" + self.obj.any('xt')).encode('utf8')

    def is_available(self):
        return hasValidStatus(self.obj)

class Directory(INode):
    def __init__(self, objs):
        super(Directory, self).__init__()
        self.objs = objs

    def attr(self):
        attr = super(Directory, self).attr()
        attr.st_mode |= stat.S_IFDIR
        attr.st_nlink = 2
        attr.st_size = 0
        return attr

    def is_available(self):
        return hasValidStatus(self.objs)

    def lookup(self, name):
        objs = list()
        for obj in self.objs:
            objs += DB.query({u'directory': u'%s/%s' % (obj.id, name)}, fields=fields)

        if not objs:
            raise(fusell.FUSEError(errno.ENOENT))

        return map_objects(objs)

    def readdir(self):
        children = dict()
        for obj in self.objs:
            for name, obj in scan(obj):
                if not hasValidStatus(obj):
                    continue
                children.setdefault(name, []).append(obj)

        for name, objs in children.iteritems():
            inode = map_objects(objs)
            if inode:
                yield name, inode

class Operations(fusell.FUSELL):
    def __init__(self, bithorde, mountpoint, options):
        self.root = Directory((DB['dir:'],))
        self.inode_open_count = defaultdict(int)

        self.inodes = {
            fusell.ROOT_INODE: self.root
        }
        self.files = {}

        self.bithorde = bithorde
        super(Operations, self).__init__(mountpoint, options)

    def _inode_resolve(self, ino, cls=INode):
        try:
            inode = self.inodes[ino]
            assert isinstance(inode, cls)
            return inode
        except KeyError:
            raise(fusell.FUSEError(errno.ENOENT))

    @timed
    def lookup(self, inode_p, name):
        inode_p = self._inode_resolve(inode_p, Directory)
        inode = inode_p.lookup(name.decode('utf-8'))
        self.inodes[inode.ino] = inode
        return inode.entry()

    def forget(self, ino, nlookup):
        # Assuming the kernel only notifies when nlookup really reaches 0
        try:
            del self.inodes[ino]
        except:
            warnings.warn('Tried to forget something already missing.')

    def getattr(self, inode):
        inode = self._inode_resolve(inode)
        return inode.attr()

    def opendir(self, inode):
        inode = self._inode_resolve(inode, Directory)
        return inode.ino

    @timed
    def readdir(self, inode, off):
        if off:
            return

        directory = self._inode_resolve(inode, Directory)

        i = 1
        for name, inode in directory.readdir():
            #self.inodes[inode.ino] = inode
            yield name.encode('utf8'), inode.attr(), i
            i += 1

    def releasedir(self, inode):
        pass

    @timed
    def open(self, inode, flags):
        inode = self._inode_resolve(inode, File)
        supported_flags = os.O_RDONLY | os.O_LARGEFILE
        if (flags & supported_flags) != flags:
            raise(fusell.FUSEError(errno.EINVAL))
        fh = fh_pool.get()
        asset = self.bithorde.open(inode.ids())
        status = asset.status()
        assert status and status.status == message.SUCCESS
        self.files[fh] = asset
        return fh

    @timed
    def read(self, fh, off, size):
        try:
            f = self.files[fh]
        except KeyError:
            raise(fusell.FUSEError(errno.EBADF))

        return f.read(off, size)

    def release(self, fh):
        try:
            del self.files[fh]
        except:
            warnings.warn("Trying to release unknown file handle: %s" % fh)

    def readlink(self, inode):
        return self._inode_resolve(inode, Symlink).readlink()

    def statfs(self):
        stat = fusell.c_statvfs
        stat.f_bsize = 64*1024
        stat.f_frsize = 64*1024
        stat.f_blocks = stat.f_bfree = stat.f_bavail = 0
        stat.f_files  = stat.f_ffree = stat.f_favail = 0
        return stat


def init_logging():
    formatter = logging.Formatter('%(message)s')
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    handler.setLevel(logging.DEBUG)
    log.setLevel(logging.DEBUG)
    log.addHandler(handler)

if __name__ == '__main__':
    init_logging()
    try:
        self, mountpoint = sys.argv
    except:
        raise SystemExit('Usage: %s <mountpoint>' % sys.argv[0])

    bithorde = Client(parseConfig(config.items('BITHORDE')), autoconnect=False)
    bithorde.connect()

    mount_point_created = None
    if not os.path.exists(mountpoint):
        os.mkdir(mountpoint)
        mount_point_created = mountpoint

    def cleanup(remove_mountpoint):
        if remove_mountpoint:
            os.rmdir(remove_mountpoint)

    atexit.register(cleanup, mount_point_created)

    try:
        print("Entering llfuse")
        fsopts = [ 'fsname=bhindex', 'nonempty', 'debug', 'allow_other', 'max_read=65536', 'ro' ]
        operations = Operations(bithorde=bithorde, mountpoint=mountpoint, options=fsopts)
    except Exception, e:
        log.exception("Error!", exc_info=True)
