from logging import getLogger
from os.path import normpath as osnormpath
from time import time
from warnings import warn

from distdb import Object, Starts, Sorting
from .bithorde import Identifiers
log = getLogger('tree')


class NotFoundError(LookupError):
    pass


class FoundError(LookupError):
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

    def size(self):
        return int(self.obj.any(u'filesize', 0))

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


def itermerged(gen, collection=set):
    gen = iter(gen)
    (key, values) = gen.next()
    values = [values]
    for k, v in gen:
        if k == key:
            values.append(v)
        else:
            yield key, collection(values)
            key, values = k, [v]
    yield key, collection(values)


class Directory(Node):
    def _map(self, objs, name):
        if any(o.any('xt') for o in objs):
            if len(objs) > 1:
                return Split(self, objs, name)
            else:
                return File(self, next(iter(objs)))
        else:
            return Directory(self, objs)

    def ls(self):
        for name, children in itermerged(self._ls()):
            yield name, self._map(children, name)

    def _ls(self):
        dirids = set("%s" % dirobj.id for dirobj in self.objs)
        children = self.db.query_keyed(
            {'directory': Starts("%s/" % d for d in dirids)}, key="+directory",
            sortmeth=Sorting.split('/'), fields=('directory', 'xt'),
        )
        for dirent, child in children:
            try:
                dir, name = split_directory_entry(dirent)
            except ValueError:
                warn("Malformed directory for %s: %s" % (child.id, dirent))
                continue
            if dir in dirids:
                yield name, child

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

    def mkdir(self, name, t=None, tr=None):
        try:
            return self[name]
        except:
            directory_attr = u'%s/%s' % (self.objs[0].id, name)
            new = Object.new('dir')
            new.set('directory', directory_attr, t=t)
            with tr or Filesystem.db_transaction(self.db) as tr:
                tr.update(new)
            return Directory(self, (new,))

    def link(self, name, node, t=None, tr=None):
        directory_attr = u'%s/%s' % (self.objs[0].id, name)
        for obj in node.objs:
            try:
                dir = obj[u'directory']
            except KeyError:
                obj.set('directory', directory_attr, t=t)
            else:
                obj.set('directory', dir | {directory_attr}, t=t)
            with tr or Filesystem.db_transaction(self.db) as tr:
                tr.update(obj)

    def rm(self, name, t=None, tr=None):
        try:
            n = self[name]
        except NotFoundError:
            return

        purge_list = set(u'%s/%s' % (obj.id, name) for obj in self.objs)

        with tr or Filesystem.db_transaction(self.db) as tr:
            for obj in getattr(n, 'objs', None) or [n.obj]:
                obj.set('directory', obj['directory'] - purge_list, t=t)
                tr.update(obj)

    def add_file(self, name, ids, size, t=None):
        ids = Identifiers(ids)
        f = File(self, ids.add_to(self.db, size))

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

    def ls(self, offset=0):
        for obj in self.objs[offset:]:
            name = self._entry(obj.id)
            yield name, self._map((obj,), name)

    def __getitem__(self, key):
        for obj in self.objs:
            name = self._entry(obj.id)
            if name == key:
                return self._map((obj,), key)

    def rm(self, name, t=None, tr=None):
        purge_list = set("%s/%s" % (obj.id, self._conflictname) for obj in self._dir.objs)
        with tr or Filesystem.db_transaction(self.db) as tr:
            for obj in self.objs:
                ename = self._entry(obj.id)
                if ename == name:
                    obj.set('directory', obj['directory'] - purge_list, t=t)
                    tr.update(obj)


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


# TODO: rewrite so changes can only be done in a transaction
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
        return self.db_transaction(self.db)

    @staticmethod
    def db_transaction(db):
        return db.in_transaction or db.transaction()

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

    def mkdir(self, directory, t=None, tr=None):
        if t is None:
            t = time()
        with tr or self.transaction() as tr:
            dir = self.root()
            for segment in directory:
                dir = dir.mkdir(segment, t=t, tr=tr)
        return dir

    def mv(self, src, dst, t=None, tr=None):
        try:
            self.lookup(dst)
            raise FoundError("%s already exist")
        except NotFoundError:
            pass
        src_dir = self.lookup(src[:-1])
        target = src_dir[src[-1]]

        with tr or self.transaction() as tr:
            dst_dir = self.mkdir(dst[:-1], t=t, tr=tr)
            dst_dir.link(dst[-1], target, t=t, tr=tr)
            src_dir.rm(src[-1], t=t, tr=tr)


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

    tree = Filesystem(db)
    tree.mv(src, dst, t)
