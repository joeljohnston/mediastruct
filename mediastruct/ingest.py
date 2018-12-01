import os
import sys
import time
import shutil
import logging
from glob import glob

class ingest(object):

    def __init__(self,_sourcedir,_destdir):
        log = logging.getLogger(__name__)
        ingest.mvrnm(self,_sourcedir,_destdir)

    def mvrnm(self,sourcedir,destdir):
        log.info("Dirctory root: {}".format(sourcedir))
        if os.path.isdir(sourcedir):
            os.chdir(sourcedir)
            for folder, subs, files in os.walk(sourcedir):
                #with open(os.path.join(folder, 'python-outfile.txt'), 'w') as dest:
                for filename in files:
                    log.info("filename: %s" % (filename))
                    ext = os.path.splitext(filename)[1][1:]
                    newfile = os.path.splitext(filename)[0]
                    log.info("ext: %s" % (ext))
                    log.info("file: %s" % (newfile))
                    newfilename = "%s.%s.%s" % (newfile, int((time.time() + 0.5) * 1000 ), ext)
                    log.info("newfilename: %s" % (newfilename))
                    filepath = "%s/%s" % (folder,filename)
                    log.info("filepath: %s" % (filepath))
                    ftime = time.gmtime(os.path.getmtime(filepath))
                    ctime_dir = "%s/%s" % (str(ftime.tm_year), str(ftime.tm_mon))
                    dest_dir="%s/%s" % (destdir, ctime_dir)
                    dest="%s/%s/%s" % (destdir, ctime_dir, filename)
                    log.info("Destination Path: %s" % (dest))
                    newdest= "%s/%s" % (dest_dir, newfilename)
                    log.info("newdest: %s" % (newdest))
                    if not os.path.exists(dest_dir):
                        os.makedirs(dest_dir)
                    if not os.path.exists(dest):
                        shutil.move(filepath, dest)
                    else:
                            shutil.move(filepath, newdest)
        else:
            log.error("Source Directory {} doesn't exist".format(sourcedir))
