#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, os.path as path, sys
from ConfigParser import NoOptionError
from time import time
import subprocess

HERE = path.dirname(__file__)
sys.path.append(HERE)

import db, config, export_txt, export_links, magnet

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

def bh_upload(file):
    '''Assumes file is normalized through path.normpath'''
    bhup = subprocess.Popen([bh_upload_bin, file], stdout=subprocess.PIPE)
    bhup_out, _ = bhup.communicate()

    for line in bhup_out.splitlines():
        try:
            proto, _ = line.split(':', 1)
        except ValueError:
            continue
        if proto == 'magnet':
            return magnet.objectFromMagnet(line.strip().decode('utf8'))
    return None

if __name__ == '__main__':
    import cliopt
    usage = "usage: %prog [options] file1 [file2 ...]\n" \
            "  An argument of '-' will expand to filenames read line for line on standard input."
    parser = cliopt.OptionParser(usage=usage)
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
    parser.add_option("-f", "--force",
                      action="store_true", dest="force", default=False,
                      help="Force upload even of assets already found in sync in index")

    (options, args) = parser.parse_args()
    if len(args) < 1:
        parser.error("At least one file must be specified")

    # Parse into DB-tag-objects
    tags = cliopt.parse_attrs(options.tags)

    DB = db.open(config)

    def add(file, tags):
        '''Try to upload one file to bithorde and add to index'''
        file = path.normpath(file)
        name = options.sanitizer(file)
        mtime = path.getmtime(file)

        if not options.force:
            oldassets = DB.query({'name': name})
            found_up2date = False
            for a in oldassets:
                if a['name'].t >= mtime:
                    found_up2date = True
            if found_up2date:
                return

        asset = bh_upload(file)
        if asset:
            asset.name = name
            t = time()
            for k,v in tags.iteritems():
                v.t = t
                asset[k] = v
            scraper.scrape_for(asset)
            for k,v in sorted(asset.iteritems()):
                print u"%s: %s" % (k, v.join())
            DB.update(asset)
        else:
            print "Error adding %s" % file

    try:
        for arg in args:
            if arg == '-':
                for line in sys.stdin:
                    add(line.strip(), options.tags)
            else:
                add(arg, options.tags)
    finally:
        DB.commit()

    if options.export_links:
        export_links.main()
    if options.export_txt:
        export_txt.main()
