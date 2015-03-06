# Copyright (c) 2010 Giorgos Verigakis <verigak@gmail.com>
#
# Permission to use, copy, modify, and distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

from __future__ import division

from ctypes import *
from ctypes.util import find_library as _find_library
from errno import *
from sys import exc_info
from math import modf
from functools import partial, wraps
from inspect import getmembers, ismethod
from platform import machine, system
from stat import S_IFDIR, S_IFREG
from os import path
import os

from concurrent import Pool, trampoline

_system = system()
_machine = machine()

##### FUSE Low C-binding declarations #####
c_void_p_p = POINTER(c_void_p)

# Tries to locate a shared library, with fallbacks over ctypes.find_library
def find_library(lib):
    l = _find_library(lib)
    if l: return l

    for libpath in ('/usr/lib/libfuse.so',):
        if path.exists(libpath):
            return libpath

    raise IOError("Shared library '%s' not found" % lib)

class LibFUSE(CDLL):
    def __init__(self):
        if _system == 'Darwin':
            self.libiconv = CDLL(find_library('iconv'), RTLD_GLOBAL)
        super(LibFUSE, self).__init__(find_library('fuse'))

        # 3d arg should be pointer of type fuse_opt
        self.fuse_opt_parse.argtypes = (POINTER(fuse_args), c_void_p, c_void_p, fuse_opt_proc_t)
        self.fuse_opt_parse.restype = c_int

        self.fuse_mount.argtypes = (c_char_p, POINTER(fuse_args))
        self.fuse_mount.restype = c_void_p
        self.fuse_lowlevel_new.argtypes = (POINTER(fuse_args), POINTER(fuse_lowlevel_ops),
                                            c_size_t, c_void_p)
        self.fuse_lowlevel_new.restype = c_void_p
        self.fuse_set_signal_handlers.argtypes = (c_void_p,)
        self.fuse_session_add_chan.argtypes = (c_void_p, c_void_p)
        self.fuse_session_loop.argtypes = (c_void_p,)
        self.fuse_remove_signal_handlers.argtypes = (c_void_p,)
        self.fuse_session_remove_chan.argtypes = (c_void_p,)
        self.fuse_session_destroy.argtypes = (c_void_p,)
        self.fuse_unmount.argtypes = (c_char_p, c_void_p)

        self.fuse_chan_fd.argtypes = (c_void_p,)
        self.fuse_chan_fd.restype = c_int
        self.fuse_chan_recv.argtypes = (c_void_p_p, c_char_p, c_size_t)
        self.fuse_chan_recv.restype = c_int
        self.fuse_session_process.argtypes = (c_void_p, c_char_p, c_size_t, c_void_p)

        self.fuse_req_ctx.restype = POINTER(fuse_ctx)
        self.fuse_req_ctx.argtypes = (fuse_req_t,)

        self.fuse_reply_err.argtypes = (fuse_req_t, c_int)
        self.fuse_reply_attr.argtypes = (fuse_req_t, c_void_p, c_double)
        self.fuse_reply_entry.argtypes = (fuse_req_t, c_void_p)
        self.fuse_reply_open.argtypes = (fuse_req_t, c_void_p)
        self.fuse_reply_buf.argtypes = (fuse_req_t, c_char_p, c_size_t)
        self.fuse_reply_readlink.argtypes = (fuse_req_t, c_char_p)
        self.fuse_reply_write.argtypes = (fuse_req_t, c_size_t)

        self.fuse_add_direntry.argtypes = (c_void_p, c_char_p, c_size_t, c_char_p,
                                            c_stat_p, c_off_t)

class fuse_args(Structure):
    _fields_ = [('argc', c_int), ('argv', POINTER(c_char_p)), ('allocated', c_int)]

class c_timespec(Structure):
    _fields_ = [('tv_sec', c_long), ('tv_nsec', c_long)]
    def __init__(self, value=None):
        super(c_timespec, self).__init__()
        if isinstance(value, float):
            seconds, decimals = modf(value)
            self.tv_sec = c_long(seconds)
            self.tv_nsec = c_long(decimals * 1000000000)

class c_stat(Structure):
    pass    # Platform dependent

if _system == 'Darwin':
    ENOTSUP = 45
    c_dev_t = c_int32
    c_fsblkcnt_t = c_ulong
    c_fsfilcnt_t = c_ulong
    c_gid_t = c_uint32
    c_mode_t = c_uint16
    c_off_t = c_int64
    c_pid_t = c_int32
    c_uid_t = c_uint32
    c_stat._fields_ = [
        ('st_dev', c_dev_t),
        ('st_ino', c_uint32),
        ('st_mode', c_mode_t),
        ('st_nlink', c_uint16),
        ('st_uid', c_uid_t),
        ('st_gid', c_gid_t),
        ('st_rdev', c_dev_t),
        ('st_atimespec', c_timespec),
        ('st_mtimespec', c_timespec),
        ('st_ctimespec', c_timespec),
        ('st_size', c_off_t),
        ('st_blocks', c_int64),
        ('st_blksize', c_int32)]
elif _system == 'Linux':
    ENOTSUP = 95
    c_dev_t = c_ulonglong
    c_fsblkcnt_t = c_ulonglong
    c_fsfilcnt_t = c_ulonglong
    c_gid_t = c_uint
    c_mode_t = c_uint
    c_off_t = c_longlong
    c_pid_t = c_int
    c_uid_t = c_uint

    if _machine == 'x86_64':
        c_stat._fields_ = [
            ('st_dev', c_dev_t),
            ('st_ino', c_ulong),
            ('st_nlink', c_ulong),
            ('st_mode', c_mode_t),
            ('st_uid', c_uid_t),
            ('st_gid', c_gid_t),
            ('__pad0', c_int),
            ('st_rdev', c_dev_t),
            ('st_size', c_off_t),
            ('st_blksize', c_long),
            ('st_blocks', c_long),
            ('st_atimespec', c_timespec),
            ('st_mtimespec', c_timespec),
            ('st_ctimespec', c_timespec)]
    elif _machine == 'ppc':
        c_stat._fields_ = [
            ('st_dev', c_dev_t),
            ('st_ino', c_ulonglong),
            ('st_mode', c_mode_t),
            ('st_nlink', c_uint),
            ('st_uid', c_uid_t),
            ('st_gid', c_gid_t),
            ('st_rdev', c_dev_t),
            ('__pad2', c_ushort),
            ('st_size', c_off_t),
            ('st_blksize', c_long),
            ('st_blocks', c_longlong),
            ('st_atimespec', c_timespec),
            ('st_mtimespec', c_timespec),
            ('st_ctimespec', c_timespec)]
    else:
        # i686, use as fallback for everything else
        c_stat._fields_ = [
            ('st_dev', c_dev_t),
            ('__pad1', c_ushort),
            ('__st_ino', c_ulong),
            ('st_mode', c_mode_t),
            ('st_nlink', c_uint),
            ('st_uid', c_uid_t),
            ('st_gid', c_gid_t),
            ('st_rdev', c_dev_t),
            ('__pad2', c_ushort),
            ('st_size', c_off_t),
            ('st_blksize', c_long),
            ('st_blocks', c_longlong),
            ('st_atimespec', c_timespec),
            ('st_mtimespec', c_timespec),
            ('st_ctimespec', c_timespec),
            ('st_ino', c_ulonglong)]
else:
    raise NotImplementedError('%s is not supported.' % _system)

class c_statvfs(Structure):
    _fields_ = [
        ('f_bsize', c_ulong),
        ('f_frsize', c_ulong),
        ('f_blocks', c_fsblkcnt_t),
        ('f_bfree', c_fsblkcnt_t),
        ('f_bavail', c_fsblkcnt_t),
        ('f_files', c_fsfilcnt_t),
        ('f_ffree', c_fsfilcnt_t),
        ('f_favail', c_fsfilcnt_t)]

class fuse_file_info(Structure):
    _fields_ = [
        ('flags', c_int),
        ('fh_old', c_ulong),
        ('writepage', c_int),
        ('direct_io', c_uint, 1),
        ('keep_cache', c_uint, 1),
        ('flush', c_uint, 1),
        ('padding', c_uint, 29),
        ('fh', c_uint64),
        ('lock_owner', c_uint64)]

class fuse_ctx(Structure):
    _fields_ = [('uid', c_uid_t), ('gid', c_gid_t), ('pid', c_pid_t)]

fuse_ino_t = c_ulong
fuse_req_t = c_void_p
c_stat_p = POINTER(c_stat)
fuse_file_info_p = POINTER(fuse_file_info)
fuse_opt_proc_t = CFUNCTYPE(c_int, c_void_p, c_char_p, c_int, POINTER(fuse_args))

FUSE_SET_ATTR = ('st_mode', 'st_uid', 'st_gid', 'st_size', 'st_atime', 'st_mtime')

ROOT_INODE = 1

class fuse_entry_param(Structure):
    _fields_ = [
        ('ino', fuse_ino_t),
        ('generation', c_ulong),
        ('attr', c_stat),
        ('attr_timeout', c_double),
        ('entry_timeout', c_double)]

class fuse_lowlevel_ops(Structure):
    _fields_ = [
        ('init', CFUNCTYPE(None, c_void_p, c_void_p)),
        ('destroy', CFUNCTYPE(None, c_void_p)),
        ('lookup', CFUNCTYPE(None, fuse_req_t, fuse_ino_t, c_char_p)),
        ('forget', CFUNCTYPE(None, fuse_req_t, fuse_ino_t, c_ulong)),
        ('getattr', CFUNCTYPE(None, fuse_req_t, fuse_ino_t, fuse_file_info_p)),
        ('setattr', CFUNCTYPE(None, fuse_req_t, fuse_ino_t, c_stat_p, c_int, fuse_file_info_p)),
        ('readlink', CFUNCTYPE(None, fuse_req_t, fuse_ino_t)),
        ('mknod', CFUNCTYPE(None, fuse_req_t, fuse_ino_t, c_char_p, c_mode_t, c_dev_t)),
        ('mkdir', CFUNCTYPE(None, fuse_req_t, fuse_ino_t, c_char_p, c_mode_t)),
        ('unlink', CFUNCTYPE(None, fuse_req_t, fuse_ino_t, c_char_p)),
        ('rmdir', CFUNCTYPE(None, fuse_req_t, fuse_ino_t, c_char_p)),
        ('symlink', CFUNCTYPE(None, fuse_req_t, c_char_p, fuse_ino_t, c_char_p)),
        ('rename', CFUNCTYPE(None, fuse_req_t, fuse_ino_t, c_char_p, fuse_ino_t, c_char_p)),
        ('link', CFUNCTYPE(None, fuse_req_t, fuse_ino_t, fuse_ino_t, c_char_p)),
        ('open', CFUNCTYPE(None, fuse_req_t, fuse_ino_t, fuse_file_info_p)),
        ('read', CFUNCTYPE(None, fuse_req_t, fuse_ino_t, c_size_t, c_off_t, fuse_file_info_p)),
        ('write', CFUNCTYPE(None, fuse_req_t, fuse_ino_t, c_char_p, c_size_t, c_off_t, fuse_file_info_p)),
        ('flush', CFUNCTYPE(None, fuse_req_t, fuse_ino_t, fuse_file_info_p)),
        ('release', CFUNCTYPE(None, fuse_req_t, fuse_ino_t, fuse_file_info_p)),
        ('fsync', CFUNCTYPE(None, fuse_req_t, fuse_ino_t, c_int, fuse_file_info_p)),
        ('opendir', CFUNCTYPE(None, fuse_req_t, fuse_ino_t, fuse_file_info_p)),
        ('readdir', CFUNCTYPE(None, fuse_req_t, fuse_ino_t, c_size_t, c_off_t, fuse_file_info_p)),
        ('releasedir', CFUNCTYPE(None, fuse_req_t, fuse_ino_t, fuse_file_info_p)),
        ('fsyncdir', CFUNCTYPE(None, fuse_req_t, fuse_ino_t, c_int, fuse_file_info_p))]

##### Intermediate Binding #####

def setattr_mask_to_list(mask):
    return [FUSE_SET_ATTR[i] for i in range(len(FUSE_SET_ATTR)) if mask & (1 << i)]

def decode_flags(flags):
    res = dict()
    for symbol in dir(os):
        if not symbol.startswith('O_'):
            continue
        value = getattr(os, symbol)
        if value & flags:
            flags = flags ^ value
            res[symbol] = value
    return res

class FUSEError(Exception):
    def __init__(self, errno):
        # Call the base class constructor with the parameters it needs
        super(FUSEError, self).__init__("FS error: %s" % errorcode.get(errno, errno))

        self.errno = errno

class guard:
    def __init__(self, f, *args, **kwargs):
        self.f = f
        self.args = args
        self.kwargs = kwargs
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_value, traceback):
        self.f(*self.args, **self.kwargs)

def copy_value(x):
    if hasattr(x, 'contents') and issubclass(x._type_, Structure):
        res = x._type_()
        pointer(res)[0] = x.contents
        return res
    else:
        return x

class FUSELL(object):
    def __init__(self, filesystem, mountpoint, fuse_options = [], parallel=8):
        self.filesystem = filesystem
        self.libfuse = LibFUSE()

        args = ['fuse']
        for opt in fuse_options:
            args += ['-o', opt]
        argv = fuse_args(len(args), (c_char_p * len(args))(*args), 0)

        # TODO: handle initialization errors

        chan = self.libfuse.fuse_mount(mountpoint, argv)
        assert chan

        self.pool = Pool(size=parallel)

        with guard(self.libfuse.fuse_unmount, mountpoint, chan):
            self._fuse_run_session(chan, argv)

    def _dispatcher(self, method):
        def _handler(req, *args):
            try:
                return method(req, *args)
            except FUSEError, e:
                return self.reply_err(req, e.errno)
        def _dispatch(req, *args):
            # Copy pointer-values in args. They will not be valid later
            args = [copy_value(x) for x in args]
            self.pool.spawn(_handler, req, *args)
        return _dispatch

    def _fuse_run_session(self, chan, argv):
        fuse_ops = fuse_lowlevel_ops()

        for name, prototype in fuse_lowlevel_ops._fields_:
            method = getattr(self, 'fuse_' + name, None) or getattr(self, name, None)
            if method:
                args = prototype._argtypes_
                if len(args) and args[0] is fuse_req_t:
                    setattr(fuse_ops, name, prototype(self._dispatcher(method)))
                else:
                    setattr(fuse_ops, name, prototype(method))

        session = self.libfuse.fuse_lowlevel_new(argv, byref(fuse_ops), sizeof(fuse_ops), None)
        assert session
        with guard(self.libfuse.fuse_session_destroy, session):
            self.libfuse.fuse_session_add_chan(session, chan)
            with guard(self.libfuse.fuse_session_remove_chan, chan):
                fd = self.libfuse.fuse_chan_fd(chan)
                while True:
                    trampoline(fd, read=True)
                    data = create_string_buffer(64*1024)
                    read = self.libfuse.fuse_chan_recv(c_void_p_p(c_void_p(chan)), data, len(data))
                    assert read > 0
                    self.libfuse.fuse_session_process(session, data[0:read], read, chan)

    def reply_err(self, req, err):
        return self.libfuse.fuse_reply_err(req, err)

    def reply_none(self, req):
        self.libfuse.fuse_reply_none(req)

    def reply_entry(self, req, entry):
        self.libfuse.fuse_reply_entry(req, byref(entry))

    def reply_create(self, req, *args):
        pass    # XXX

    def reply_attr(self, req, attr, attr_timeout):
        return self.libfuse.fuse_reply_attr(req, byref(attr), c_double(attr_timeout))

    def reply_readlink(self, req, link_contents):
        return self.libfuse.fuse_reply_readlink(req, create_string_buffer(link_contents))

    def reply_open(self, req, fi):
        return self.libfuse.fuse_reply_open(req, byref(fi))

    def reply_write(self, req, count):
        return self.libfuse.fuse_reply_write(req, count)

    def reply_buf(self, req, buf):
        return self.libfuse.fuse_reply_buf(req, buf, len(buf or ''))

    def reply_readdir(self, req, size, entries):
        off = 0
        buf = create_string_buffer(size)
        for name, attr, index in entries:
            bufptr = cast(addressof(buf) + off, c_char_p)
            bufsize = size-off
            entsize = self.libfuse.fuse_add_direntry(req, bufptr, bufsize, name, byref(attr), index)
            if entsize >= bufsize:
                break
            off += entsize

        if off > 0:
            return self.libfuse.fuse_reply_buf(req, buf, off)
        else:
            return self.libfuse.fuse_reply_buf(req, None, 0)


    # If you override the following methods you should reply directly
    # with the self.libfuse.fuse_reply_* methods.

    def fuse_lookup(self, req, parent, name):
        entry = self.filesystem.lookup(parent, name)
        return self.reply_entry(req, entry)

    def fuse_forget(self, req, ino, nlookup):
        self.filesystem.forget(ino, nlookup)
        return self.reply_none(req)

    def fuse_getattr(self, req, ino, fi):
        attr, timeout = self.filesystem.getattr(ino)
        return self.reply_attr(req, attr, timeout)

    def fuse_setattr(self, req, ino, attr, to_set, fi):
        to_set_list = setattr_mask_to_list(to_set)
        self.filesystem.setattr(req, ino, attr_dict, to_set_list, fi)

    def fuse_open(self, req, ino, fi):
        fi.fh = self.filesystem.open(ino, fi.flags)
        return self.reply_open(req, fi)

    def fuse_read(self, req, ino, size, off, fi):
        res = self.filesystem.read(fi.fh, off, size)
        if res is None:
            return self.reply_err(req, EIO)
        else:
            return self.reply_buf(req, res)

    def fuse_write(self, req, ino, buf, size, off, fi):
        buf_str = string_at(buf, size)
        written = self.filesystem.write(req, ino, buf_str, off, fi)
        return self.reply_write(req, written)

    def fuse_release(self, req, ino, fi):
        self.filesystem.release(fi.fh)
        return self.reply_err(req, 0)

    def fuse_fsync(self, req, ino, datasync, fi):
        self.filesystem.fsyncdir(req, ino, datasync, fi)

    def fuse_opendir(self, req, ino, fi):
        fi.fh = self.filesystem.opendir(ino)
        return self.reply_open(req, fi)

    def fuse_readdir(self, req, ino, size, off, fi):
        entries = self.filesystem.readdir(ino, off)
        return self.reply_readdir(req, size, entries)

    def fuse_releasedir(self, req, ino, fi):
        self.filesystem.releasedir(ino)
        return self.reply_err(req, 0)

    def fuse_readlink(self, req, ino):
        contents = self.filesystem.readlink(ino)
        return self.reply_readlink(req, contents)

    def fuse_fsyncdir(self, req, ino, datasync, fi):
        self.filesystem.fsyncdir(req, ino, datasync, fi)


class Filesystem:
    def init(self, userdata, conn):
        """Initialize filesystem

        There's no reply to this method
        """
        pass

    def destroy(self, userdata):
        """Clean up filesystem

        There's no reply to this method
        """
        pass

    def lookup(self, parent, name):
        """Look up a directory entry by name and get its attributes.

        Valid replies:
            fuse_entry_param()
            FUSEError
        """
        raise FUSEError(ENOENT)

    def forget(self, req, ino, nlookup):
        """Forget about an inode

        Valid replies:
            None
        """
        pass

    def getattr(self, req, ino, fi):
        """Get file attributes

        Valid replies:
            c_stat()
            FUSEError
        """
        if ino == 1:
            attr = fusell.c_stat(
                st_ino   = 1,
                st_mode  = S_IFDIR | 0755,
                st_nlink = 2,
            )
            return attr, 1.0
        else:
            raise FUSEError(ENOENT)

    def setattr(self, req, ino, attr, to_set, fi):
        """Set file attributes

        Valid replies:
            c_stat()
            FUSEError
        """
        raise FUSEError(EROFS)

    def readlink(self, req, ino):
        """Read symbolic link

        Valid replies:
            str()
            FUSEError
        """
        raise FUSEError(ENOENT)

    def mknod(self, req, parent, name, mode, rdev):
        """Create file node

        Valid replies:
            fuse_entry_param()
            FUSEError
        """
        raise FUSEError(EROFS)

    def mkdir(self, req, parent, name, mode):
        """Create a directory

        Valid replies:
            fuse_entry_param()
            FUSEError
        """
        raise FUSEError(EROFS)

    def unlink(self, req, parent, name):
        """Remove a file

        Valid replies:
            FUSEError
        """
        raise FUSEError(EROFS)

    def rmdir(self, req, parent, name):
        """Remove a directory

        Valid replies:
            FUSEError
        """
        raise FUSEError(EROFS)

    def symlink(self, req, link, parent, name):
        """Create a symbolic link

        Valid replies:
            fuse_entry_param()
            FUSEError
        """
        raise FUSEError(EROFS)

    def rename(self, req, parent, name, newparent, newname):
        """Rename a file

        Valid replies:
            FUSEError
        """
        raise FUSEError(EROFS)

    def link(self, req, ino, newparent, newname):
        """Create a hard link

        Valid replies:
            fuse_entry_param()
            FUSEError
        """
        raise FUSEError(EROFS)

    def open(self, req, ino, flags):
        """Open a file

        Valid replies:
            int(), file handle
            FUSEError
        """
        raise FUSEError(EIO)

    def read(self, req, ino, size, off, fi):
        """Read data

        Valid replies:
            str()
            FUSEError
        """
        raise FUSEError(EIO)

    def write(self, req, ino, buf, off, fi):
        """Write data

        Valid replies:
            int(), written
            FUSEError
        """
        raise FUSEError(EROFS)

    def flush(self, req, ino, fi):
        """Flush method

        Valid replies:
            FUSEError
        """
        raise FUSEError(0)

    def release(self, req, ino, fi):
        """Release an open file

        Valid replies:
            FUSEError
        """
        raise FUSEError(0)

    def fsync(self, req, ino, datasync, fi):
        """Synchronize file contents

        Valid replies:
            FUSEError
        """
        raise FUSEError(0)

    def opendir(self, req, ino, fi):
        """Open a directory

        Valid replies:
            int(), file handle
            FUSEError
        """
        self.reply_open(req, fi)

    def readdir(self, req, ino, size, off, fi):
        """Read directory

        Valid replies:
            generator of (str() as name, c_stat() as attr, int() as offset)
            FUSEError
        """
        if ino == 1:
            attr = {'st_ino': 1, 'st_mode': S_IFDIR}
            entries = [('.', attr), ('..', attr)]
            self.reply_readdir(req, size, off, entries)
        else:
            raise FUSEError(ENOENT)

    def releasedir(self, req, ino, fi):
        """Release an open directory

        Valid replies:
            FUSEError
        """
        raise FUSEError(0)

    def fsyncdir(self, req, ino, datasync, fi):
        """Synchronize directory contents

        Valid replies:
            FUSEError
        """
        raise FUSEError(0)
