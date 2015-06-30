#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, os.path as path, sys
from ConfigParser import NoOptionError
from time import time
from concurrent import subprocess

HERE = path.dirname(__file__)
sys.path.append(HERE)

import db, config, export_txt, export_links, magnet, scraper
from util import make_directory

config = config.read()

try:
  bh_bindir = config.get('BITHORDE', 'bindir')
  bh_bindir = path.expanduser(bh_bindir)
  os.environ['PATH'] = "%s:%s" % (bh_bindir, os.environ['PATH'])
except NoOptionError:
  pass
bh_upload_bin = 'bhupload'

def sanitizedpath(file):
    '''Assumes path is normalized through path.normpath first,
    then santizies by removing leading path-fixtures such as [~./]'''
    return file.lstrip('./~') # Remove .. and similar placeholders

def bh_upload(file, link):
    '''Assumes file is normalized through path.normpath'''
    cmd = [bh_upload_bin, '-u', config.get('BITHORDE', 'address')]
    if link:
        cmd.append('--link')
    cmd.append(file)
    bhup = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    bhup_out, _ = bhup.communicate()

    for line in bhup_out.splitlines():
        try:
            proto, _ = line.split(':', 1)
        except ValueError:
            continue
        if proto == 'magnet':
            return magnet.objectFromMagnet(line.strip().decode('utf8'), fullPath=unicode(file))
    return None

if __name__ == '__main__':
    import cliopt
    usage = "usage: %prog [options] file1/dir1 [file2/dir2 ...]\n" \
            "  An argument of '-' will expand to filenames read line for line on standard input."
    parser = cliopt.OptionParser(usage=usage)
    parser.add_option("-l", "--upload_link", action="store_true", dest="upload_link",
                      default=config.getboolean('BITHORDE', 'upload_link'),
                      help="Upload as a linked asset, instead of adding it to cache-dir. NEEDS appropriate bithorded-config.")
    parser.add_option("-L", "--no-links", action="store_false",
                      dest="export_links", default=True,
                      help="When done, don't immediately export links to links-directory")
    parser.add_option("-T", "--no-txt", action="store_false",
                      dest="export_txt", default=True,
                      help="When done, don't immediately publish txt-format index")
    parser.add_option("-t", "--tag", action="append", dest="tags",
                      help="Define a tag for these uploads, such as '-tname:monkey'")
    parser.add_option("-s", "--strip-path", action="store_const", dest="sanitizer",
                      default=sanitizedpath, const=path.basename,
                      help="Strip name to just the name of the file, without path")
    parser.add_option("-S", "--scrapers", dest="scrapers",
                      default=','.join(scraper.SCRAPERS),
                      help="Scrapers enabled for pulling extra metadata")
    parser.add_option("-f", "--force",
                      action="store_true", dest="force", default=False,
                      help="Force upload even of assets already found in sync in index")
    parser.add_option("-r", "--recursive-add",
                      action="store_true", dest="recursive", default=False,
                      help="Recursively add files from a folder")
    parser.add_option("-e", "--ext",
                      action="append", dest="exts",
                      help="Define file extensions to be added. Only valid with -r / --recursive-add")

    (options, args) = parser.parse_args()
    if len(args) < 1:
        parser.error("At least one file or directory must be specified.")

    do_export = False

    # Parse into DB-tag-objects
    tags = cliopt.parse_attrs(options.tags)

    DB = db.open(config.get('DB', 'file'))
    SCRAPERS = set(options.scrapers.split(','))

    def add(file, tags, exts=None):
        '''Try to upload one file to bithorde and add to index'''
        file = unicode(path.normpath(file), sys.stdin.encoding or 'UTF-8')
        name = options.sanitizer(file)
        mtime = path.getmtime(file)
        ext = os.path.splitext(file)[1]
        tags = tags or {}
        exts = exts or set()

        if type(exts).__name__ == 'list':
           exts = set(exts)

        try:
            for e in config.get('ADD', 'extensions').split(','):
                exts.add(e.strip())
        except NoOptionError:
               pass

        if ext.strip('.') not in exts and options.recursive == True:
           print "* Skipping %s because of extension %s (Add extensions to be included with -e)." % (name, ext)
           return

        if not options.force:
            oldassets = DB.query({'path': name})
            found_up2date = False
            for a in oldassets:
                if a['name'].t >= mtime:
                    found_up2date = True
            if found_up2date:
                print "* File \"%s\" already in database, won't add." % file
                return

        asset = bh_upload(file, options.upload_link)
        if asset:
            asset.name = name

            t = time()
            path_list = name.split('/')
            asset[u'directory'] = db.ValueSet(u"%s/%s" % (make_directory(DB, path_list[:-1], t), path_list[-1]), t)
            for k,v in tags.iteritems():
                v.t = t
                asset[k] = v
            scraper.scrape_for(asset, SCRAPERS)
            for k,v in sorted(asset.iteritems()):
                print u"%s: %s" % (k, v.join())

            DB.update(asset)
            global do_export
            do_export = True

        else:
            print "Error adding %s" % file

    try:
        for arg in args:
            if arg == '-':
                for line in sys.stdin:
                    add(line.strip(), options.tags)
            else:
                if os.path.isdir(arg):
                   if options.recursive == True:
                      fileList = []
                      for root, subFolders, files in os.walk(arg):
                          subFolders[:] = [d for d in subFolders if not d.startswith('.')] # Remove dotted dirs.
                          for file in files:
                              fileList.append(os.path.join(root, file))

                      for file in fileList:
                          print "* Considering %s" % file
                          add(file, options.tags, options.exts)

                      print "* Processed %s files." % len(fileList)

                   else:
                       print "%s is a directory. Perhaps you'd like to add all files from it by specifying -r or --recursive-add?" % arg

                elif os.path.isfile(arg):
                         add(arg, options.tags, options.exts)

                else:
                    parser.error("At least one file or directory must be specified.")
    finally:
           DB.commit()

    if options.export_links and export_links.LINKDIR and do_export == True:
       export_links.main()
    if options.export_txt and export_txt.TXTPATH and do_export == True:
       export_txt.main()

