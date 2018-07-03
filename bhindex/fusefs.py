#!/usr/bin/env python

from __future__ import division, print_function, absolute_import

import os
import sys
import warnings

import errno
import itertools
import logging
import stat

from time import time
from threading import Thread

from .scanner import Scanner
from .tree import Filesystem, FoundError, NotFoundError
from .util import set_new_availability
from bithorde import Client, parseConfig, message
from distdb import DB

import llfuse

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
base_fields = set((u'directory', u'name', u'ext', u'xt', u'bh_status', u'bh_status_confirmed', u'bh_availability', u'filesize'))


class IDResource(object):
    def __init__(self):
        self._dict = dict()
        iter = itertools.count(1)
        self._pool = Pool(create=lambda: next(iter))

    def insert(self, value):
        id = self._pool.get()
        self._dict[id] = value
        return id

    def __delitem__(self, id):
        del self._dict[id]
        self._pool.put(id)

    def __getitem__(self, id):
        return self._dict[id]


class INode(object):
    __slots__ = ('ino')
    MODE_0755 = stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH
    MODE_0555 = stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH

    @staticmethod
    def map(x):
        if hasattr(x, 'ls'):
            return Directory(x)
        else:
            return File(x)

    def __init__(self):
        super(INode, self).__init__()
        self.ino = ino_pool.get()

    def __del__(self):
        ino_pool.put(self.ino)

    def attr(self):
        attr = llfuse.EntryAttributes()

        attr.generation = 0
        attr.attr_timeout = 2
        attr.attr_timeout = 10

        attr.st_ino = self.ino
        attr.st_mode = self.MODE_0555
        attr.st_nlink = 1
        attr.st_uid = current_uid
        attr.st_gid = current_gid
        attr.st_rdev = 1
        attr.st_size = 0

        attr.st_blksize = 512
        attr.st_blocks = 1
        self._set_times(attr)

        return attr

    if hasattr(llfuse.EntryAttributes, 'st_atime_ns'):
        @staticmethod
        def _set_times(attr):
            now = time() * 1e9
            attr.st_atime_ns = now
            attr.st_mtime_ns = now
            attr.st_ctime_ns = now
    else:
        @staticmethod
        def _set_times(attr):
            now = time()
            attr.st_atime = now
            attr.st_mtime = now
            attr.st_ctime = now


class File(INode):
    __slots__ = ('f')

    def __init__(self, f):
        INode.__init__(self)
        self.f = f

    def attr(self):
        attr = INode.attr(self)
        attr.st_mode |= stat.S_IFREG
        attr.st_size = int(self.f.obj.any(u'filesize', 0))
        return attr

    def ids(self):
        return self.f.ids().proto_ids()


class Directory(INode):
    __slots__ = ('d', 'readers')

    def __init__(self, d):
        INode.__init__(self)
        self.d = d
        self.readers = IDResource()

    def attr(self):
        attr = INode.attr(self)
        attr.st_mode |= stat.S_IFDIR
        attr.st_nlink = 2
        attr.st_size = 0
        return attr

    def lookup(self, name):
        try:
            f = self.d[name]
        except NotFoundError:
            raise(llfuse.FUSEError(errno.ENOENT))
        else:
            return INode.map(f)

    def readdir(self, id):
        if id == 0:
            iter = self.d.ls()
            id = self.readers.insert(iter)
        else:
            iter = self.readers[id]

        for name, f in iter:
            yield name, INode.map(f), id

    def mkdir(self, name):
        return Directory(self.d.mkdir(name))


class Operations(llfuse.Operations):
    def __init__(self, bithorde, database):
        super(Operations, self).__init__()

        self.fs = Filesystem(database)

        self.inodes = {
            llfuse.ROOT_INODE: Directory(self.fs.root())
        }
        self.files = {}

        self.bithorde = bithorde

    def _inode_resolve(self, ino, cls=INode):
        try:
            inode = self.inodes[ino]
            assert isinstance(inode, cls)
            return inode
        except KeyError:
            raise(llfuse.FUSEError(errno.ENOENT))

    def lookup(self, inode_p, name, ctx=None):
        inode_p = self._inode_resolve(inode_p, Directory)
        inode = inode_p.lookup(name.decode('utf-8'))
        self.inodes[inode.ino] = inode
        return inode.attr()

    def forget(self, inodes, ctx=None):
        for ino, nlookup in inodes:
            # Assuming the kernel only notifies when nlookup really reaches 0
            try:
                del self.inodes[ino]
            except KeyError:
                warnings.warn('Tried to forget something already missing.')

    def getattr(self, inode, ctx=None):
        inode = self._inode_resolve(inode)
        return inode.attr()

    def opendir(self, inode, ctx=None):
        inode = self._inode_resolve(inode, Directory)
        return inode.ino

    def readdir(self, inode, id, ctx=None):
        directory = self._inode_resolve(inode, Directory)

        for name, inode, id in directory.readdir(id):
            yield name.encode('utf8'), inode.attr(), id

    def releasedir(self, inode, ctx=None):
        pass

    def open(self, inode, flags, ctx=None):
        inode = self._inode_resolve(inode, File)
        unsupported_flags = os.O_WRONLY | os.O_RDWR
        if (flags & unsupported_flags):
            raise(llfuse.FUSEError(errno.EINVAL))
        fh = fh_pool.get()
        asset = self.bithorde.open(inode.ids())
        status = asset.status()
        status_ok = status and status.status == message.SUCCESS

        if not status_ok:
            db = self.fs.db
            with db.transaction() as tr:
                set_new_availability(inode.f.obj, status_ok)
                tr.update(inode.f.obj)
            raise(llfuse.FUSEError(errno.ENOENT))

        self.files[fh] = asset
        return fh

    def read(self, fh, off, size, ctx=None):
        try:
            f = self.files[fh]
        except KeyError:
            raise(llfuse.FUSEError(errno.EBADF))

        res = f.read(off, size)
        if res:
            return res
        else:
            raise(llfuse.FUSEError(errno.EIO))

    def release(self, fh, ctx=None):
        try:
            del self.files[fh]
        except KeyError:
            warnings.warn("Trying to release unknown file handle: %s" % fh)

    def statfs(self, ctx=None):
        stat = llfuse.StatvfsData()
        stat.f_bsize = 64 * 1024
        stat.f_frsize = 64 * 1024
        stat.f_blocks = stat.f_bfree = stat.f_bavail = 0
        stat.f_files = stat.f_ffree = stat.f_favail = 0
        return stat

    def mkdir(self, parent, name, ctx=None):
        parent = self._inode_resolve(parent, Directory)
        try:
            return parent.mkdir(name).attr()
        except FoundError:
            raise(llfuse.FUSEError(errno.EEXIST))
        except NotFoundError:
            raise(llfuse.FUSEError(errno.ENOENT))

    def rename(self, parent, name, newparent, newname, ctx=None):
        parent = self._inode_resolve(parent, Directory)
        newparent = self._inode_resolve(newparent, Directory)

        try:
            self.fs.mv((parent.d, name), (newparent.d, newname))
        except FoundError:
            raise(llfuse.FUSEError(errno.EEXIST))
        except NotFoundError:
            raise(llfuse.FUSEError(errno.ENOENT))


def background_scan(args, config):
    while True:
        try:
            bithorde = Client(parseConfig(config.items('BITHORDE')), autoconnect=False)
            bithorde.connect()

            Scanner(DB(args.db), bithorde).run()
        except Exception:
            log.exception("Error in scanner")


def prepare_args(parser, config):
    parser.add_argument("--fs-debug", action="store_true", default=False,
                        help="Enable FS-debugging")
    parser.add_argument("--no-scan", dest="scan", action="store_false", default=True,
                        help="Don't run local scanner, rely on remote")
    parser.add_argument("mountpoint", help="Directory to mount the file under, I.E. 'dir/file'")
    parser.set_defaults(main=main, setup=setup)


def setup(args, config, db):
    from .util import noop_context_manager
    bithorde = Client(parseConfig(config.items('BITHORDE')), autoconnect=False)
    bithorde.connect()

    # fsopts = set(llfuse.default_options())
    fsopts = ['nonempty', 'allow_other', 'max_read=262144', 'fsname=bhindex']
    if args.fs_debug:
        fsopts.append('debug')
    ops = Operations(database=db, bithorde=bithorde)

    llfuse.init(ops, args.mountpoint, fsopts)

    if args.scan:
        scanner = Thread(target=background_scan, args=(args, config))
        scanner.setDaemon(True)
    else:
        scanner = None

    return noop_context_manager(), (scanner,)


def main(scanner):
    if scanner:
        scanner.start()

    try:
        llfuse.main()
    except Exception:
        log.exception("Error in FuseFS")
    finally:
        llfuse.close()
