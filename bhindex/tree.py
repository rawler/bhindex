from base64 import b64encode
from logging import getLogger
from time import time
from uuid import uuid4
from warnings import warn

from db import Starts, ValueSet

log = getLogger('tree')


def get_folder_id(db, parent_id, name, t):
    directory_attr = u'%s/%s' % (parent_id, name)
    folders = list(db.query_ids({u'directory': directory_attr}))
    if folders:
        if len(folders) > 1:
            log.warning(
                "Duplicate folders for %s: %r", directory_attr, folders)
        return folders[0]
    else:
        folder_id = u'dir:%s' % b64encode(uuid4().bytes).strip('=')
        folder = db[folder_id]
        folder[u'directory'] = ValueSet((directory_attr,), t=t)
        db.update(folder)
        return folder_id


class File(object):
    def __init__(self, ctx, obj):
        self.db = getattr(ctx, 'db', ctx)
        self.obj = obj

    def xt(self):
        return self.obj['xt']


def split_directory_entry(dirent):
    dir, name = dirent.rsplit('/', 1)
    if not dir:
        raise ValueError("Directory ID is missing")
    if not name:
        raise ValueError("File name is missing")
    return dir, name


class Directory(object):
    def __init__(self, ctx, objs):
        self.db = getattr(ctx, 'db', ctx)
        self.objs = objs

    def _map(self, objs):
        if any(o.any('xt') for o in objs):
            if len(objs) > 1:
                warn("TODO: Merge NON-directories: %s" % ([o.id for o in objs]))
            else:
                return File(self, objs[0])
        else:
            return Directory(self, objs)

    def ls(self):
        d = dict()
        for dirobj in self.objs:
            for child in self.db.query({u'directory': Starts("%s/" % dirobj.id)}):
                for dirent in child[u'directory']:
                    try:
                        dir, name = split_directory_entry(dirent)
                    except ValueError:
                        warn("Malformed directory for %s: %s" % (child.id, dirent))
                    if dir == dirobj.id:
                        d.setdefault(name, []).append(child)
        for objs in d.itervalues():
            yield self._map(objs)

    def __iter__(self):
        return self.ls()

    def __getitem__(self, key):
        objs = list()
        for obj in self.objs:
            objs += self.db.query({u'directory': "%s/%s" % (obj.id, key)})
        if objs > 0:
            return self._map(objs)
        else:
            raise KeyError("%s not found in %s" % key, self.id)


class Filesystem(object):
    def __init__(self, db):
        self.db = db

    def paths_for(self, obj):
        dirents = obj.get(u'directory', [])
        if not dirents:
            return set([()])
        res = set()
        for dirent in dirents:
            try:
                dir, name = split_directory_entry(dirent)
            except ValueError:
                warn("Malformed directory for %s: %s" % (obj.id, dirent))
            else:
                res |= set(path + (name,) for path in self.paths_for(self.db[dir]))
        return res

    def root(self):
        return Directory(self, [self.db['dir:']])

    def lookup(self, path):
        res = self.root()
        for p in path:
            res = res[p]
        return res

    def mkdir(self, directory, t=None):
        if t is None:
            t = time()
        dir_id = u"dir:"
        db = self.db
        for segment in directory:
            dir_id = get_folder_id(db, dir_id, segment, t)
        return dir_id


def make_directory(db, directory, t=None):
    return Filesystem(db).mkdir(directory, t)
