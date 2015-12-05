from logging import getLogger
from os.path import normpath as osnormpath
from time import time
from warnings import warn

from db import Object, Starts, ValueSet
from .bithorde import Identifiers, obj_from_ids
log = getLogger('tree')


class NotFoundError(LookupError):
    pass


class Node(object):
    def __init__(self, ctx, objs):
        self.db = getattr(ctx, 'db', ctx)
        self.objs = objs


class File(Node):
    def __init__(self, ctx, obj):
        super(File, self).__init__(ctx, [obj])
        self.obj = obj

    def ids(self):
        return Identifiers(self.obj['xt'])

    def __eq__(self, other):
        return self.obj == other.obj

    def __ne__(self, other):
        return not self.__eq__(other)


def split_directory_entry(dirent):
    dir, name = dirent.rsplit('/', 1)
    if not dir:
        raise ValueError("Directory ID is missing")
    if not name:
        raise ValueError("File name is missing")
    return dir, name


class Directory(Node):
    def _map(self, objs, name):
        if any(o.any('xt') for o in objs):
            if len(objs) > 1:
                return Split(self, objs, name)
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
        for name, objs in sorted(d.iteritems()):
            yield name, self._map(objs, name)

    def __iter__(self):
        return self.ls()

    def __getitem__(self, key):
        objs = list()
        for obj in self.objs:
            objs += self.db.query({u'directory': "%s/%s" % (obj.id, key)})
        if objs:
            return self._map(objs, key)
        else:
            raise NotFoundError("%s not found in %s" % (key, [x.id for x in self.objs]))

    def mkdir(self, name, t=None):
        try:
            return self[name]
        except:
            directory_attr = u'%s/%s' % (self.objs[0].id, name)
            new = Object.new('dir')
            new[u'directory'] = ValueSet((directory_attr,), t=t)
            self.db.update(new)
            return Directory(self, (new,))

    def link(self, name, node, t=None):
        directory_attr = u'%s/%s' % (self.objs[0].id, name)
        for obj in node.objs:
            try:
                dir = obj[u'directory']
            except KeyError:
                obj[u'directory'] = ValueSet((directory_attr,), t=t)
            else:
                dir.add(directory_attr, t=t)
                obj[u'directory'] = dir
            self.db.update(obj)

    def rm(self, name, t=None):
        try:
            n = self[name]
        except NotFoundError:
            return

        purge_list = set(u'%s/%s' % (obj.id, name) for obj in self.objs)

        for obj in getattr(n, 'objs', None) or [n.obj]:
            dirs = ValueSet(obj[u'directory'] - purge_list, t=t)
            obj[u'directory'] = dirs
            self.db.update(obj)

    def add_file(self, name, ids, t=None):
        ids = Identifiers(ids)
        f = File(self, obj_from_ids(self.db, ids))

        self.link(name, f, t=t)
        return f


class Split(Directory):
    def __init__(self, ctx, objs, conflictname):
        super(Split, self).__init__(ctx, objs)
        self._dir = ctx
        self._conflictname = conflictname
        name_ext = conflictname.rsplit('.', 1)
        if len(name_ext) > 1:
            self._ext = ".%s" % name_ext[1]
        else:
            self._ext = ""

    def _entry(self, id):
        return "%s%s" % (id.replace("/", "_"), self._ext)

    def ls(self):
        for obj in self.objs:
            name = self._entry(obj.id)
            yield name, self._map((obj,), name)

    def __getitem__(self, key):
        for obj in self.objs:
            name = self._entry(obj.id)
            if name == key:
                return self._map((obj,), key)

    def rm(self, name, t=None):
        purge_list = set("%s/%s" % (obj.id, self._conflictname) for obj in self._dir.objs)
        for obj in self.objs:
            ename = self._entry(obj.id)
            if ename == name:
                dir = ValueSet(obj['directory']-purge_list, t=t)
                obj['directory'] = dir
                self.db.update(obj)


class Path(tuple):
    def __new__(cls, p):
        if p and isinstance(p, basestring):
            p = unicode(p, 'utf8')
            p = osnormpath(p).strip(u'/ ').split(u'/')
        return super(Path, cls).__new__(cls, p)

    def __str__(self):
        return "/".join(self)

    def __unicode__(self):
        return u"/".join(self)


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

    def transaction(self):
        return self.db.transaction()

    def lookup(self, path):
        res = self.root()
        current_path = list()
        for p in path:
            try:
                res = res[p]
            except NotFoundError:
                raise NotFoundError("%s not found under '%s'" % (p, Path(current_path)))
            else:
                current_path.append(p)

        return res

    def mkdir(self, directory, t=None):
        if t is None:
            t = time()
        dir = self.root()
        for segment in directory:
            dir = dir.mkdir(segment, t)
        return dir

    def mv(self, src, dst, t=None):
        src_dir = self.lookup(src[:-1])
        target = src_dir[src[-1]]

        dst_dir = self.mkdir(dst[:-1], t=t)
        dst_dir.link(dst[-1], target, t=t)
        src_dir.rm(src[-1], t=t)


def prepare_ls_args(parser, config):
    parser.add_argument("path", nargs='*', help="Path to list files in.")
    parser.set_defaults(main=ls_main)


def ls_main(args, config, db):
    paths = args.path or ['']
    fs = Filesystem(db)
    for path in paths:
        dir = fs.lookup(Path(path))
        if not hasattr(dir, 'ls'):
            raise RuntimeError('%s is not ')
        for name, node in dir.ls():
            if isinstance(node, Directory):
                print "%s/" % name
            else:
                print "%s -> %s" % (name, node.ids())


def prepare_mv_args(parser, config):
    parser.add_argument("source", help="Source path to move. IE 'dir/file'")
    parser.add_argument("destination", help="Path and name to move the file to. IE 'dir/file'")
    parser.set_defaults(main=mv_main)


def mv_main(args, config, db):
    t = time()

    src = Path(args.source)
    dst = Path(args.destination)

    with db.transaction():
        tree = Filesystem(db)
        tree.mv(src, dst, t)
