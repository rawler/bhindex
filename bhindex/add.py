from __future__ import absolute_import, print_function

from os import walk
from os.path import getmtime, join
from time import time
from warnings import warn

from .errors import CLIError
from .tree import Filesystem, NotFoundError, Path
from .links import export_from_config


class FileExistsError(BaseException):
    def __init__(self, path):
        super(FileExistsError, self).__init__(
            self, "%s already in database" % path)
        self.path = path


class AddController(object):
    def __init__(self, fs, bhupload):
        self.fs = fs
        self.bhupload = bhupload
        self.added = set()

    def _check_file_exists(self, path, mtime, force):
        try:
            old_file = self.fs.lookup(path)
        except NotFoundError:
            old_file = None
        else:
            if mtime is None:
                mtime = getmtime(str(path))
            if (not force) and old_file.obj['xt'].t >= mtime:
                raise FileExistsError(str(path))
        return old_file

    def __call__(self, path, mtime=None, force=False, t=None):
        if t is None:
            t = time()
        treepath = Path(path)

        old_file = self._check_file_exists(treepath, mtime, force)

        ids = self.bhupload(path)

        # Wrap in transaction, all or nothing, and flush on success
        with self.fs.transaction():
            directory = self.fs.mkdir(treepath[:-1])
            fname = treepath[-1]
            if old_file:
                directory.rm(fname, t=t)
            f = directory.add_file(fname, ids, t=t)

        self.added.add((path, f))
        return f


def prepare_args(parser, config):
    mode = parser.add_mutually_exclusive_group()
    link_default = config.getboolean('BITHORDE', 'upload_link')
    mode.add_argument("-l", "--link",
                      action="store_true", dest="link", default=link_default,
                      help="Use file linking to add the file to bithorde")
    mode.add_argument("-u", "--upload",
                      action="store_false", dest="link", default=link_default,
                      help="Upload files to bithorde")

    parser.add_argument("--no-export-links", action="store_false",
                        dest="export_links", default=True,
                        help="When done, don't immediately export links to links-directory")
    parser.add_argument("-L", "--no-links", action="store_false",
                        dest="export_links", default=True,
                        help="Deprecated aliases for --no-export-links")
    parser.add_argument("-f", "--force",
                        action="store_true", dest="force", default=False,
                        help="Force upload even of assets already found in sync in index")

    parser.add_argument("-R", "--recursive-exts",
                        dest="recurse_exts", default=set(),
                        type=lambda s: set(s.split(',')),
                        help="Recursively look for files of named extensions (comma-separated) in path")
    parser.add_argument("-r", "--recursive-add",
                        action="store_true", dest="recursive", default=False,
                        help="Recursively add files from a folder")
    parser.add_argument("-e", "--ext",
                        action="append", dest="exts",
                        help="Define file extensions to be added. Only valid with -r / --recursive-add")

    parser.add_argument("path", nargs='+',
                        help="A path to upload and add to index")
    parser.set_defaults(main=main)


def args_recurse(args):
    if args.recurse_exts:
        return args.recurse_exts

    if args.recursive:
        warn("Deprecated -r|--recursive-add. Use -R|--recursive-exts instead")
        if args.exts:
            return set(args.exts)
        else:
            raise CLIError("--recursive-add without --ext. Wild adding prohibited")

    return None


def find_files(args):
    recurse_exts = args_recurse(args)

    for path in args.path:
        if recurse_exts:
            for root, dirs, files in walk(path):
                for file in files:
                    for ext in recurse_exts:
                        if file.endswith(".%s" % ext):
                            yield join(root, file)
        else:
            yield path


def main(args, config, db):
    from bithorde import Client, parseConfig

    bithorde = Client(parseConfig(config.items('BITHORDE')))
    if args.link:
        bhupload = bithorde.link
    else:
        bhupload = bithorde.upload

    add = AddController(Filesystem(db), bhupload)
    for path in find_files(args):
        print("Adding %s" % path)
        try:
            f = add(path, force=args.force)
        except FileExistsError:
            print("  file exists, skipped...")
        except:
            import traceback
            print("  unexpected error:")
            traceback.print_exc()
        else:
            print("  -> %s" % f.ids())

    if add.added and args.export_links:
        export_from_config(db, bithorde, config)
