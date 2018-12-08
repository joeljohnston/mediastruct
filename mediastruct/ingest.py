import os
import sys
import time
import shutil
import logging
from glob import glob

log = logging.getLogger(__name__)
log.info('Launching the Crawl Class')

class ingest(object):
    '''the ingest class manages contents entering the workflow by organizing files by their last modified date
    into the working directory / media directory'''
    #class init
    def __init__(self,_sourcedir,_destdir):
        #setup logging for this child class
        log = logging.getLogger(__name__)
        ingest.mvrnm(self,_sourcedir,_destdir)
    #Move and Rename as Necessary   
    def mvrnm(self,sourcedir,destdir):
        '''this function ensures that no data is lost via file collisions as files are moved into the working dir
        by renaming them with a .<unixdatetimestamp. addition to the existing filename'''
        log.info("Dirctory root: %s" % (sourcedir))
        #ensure the source directory exists
        if os.path.isdir(sourcedir):
            #change parser to the sourcedir
            #os.chdir(sourcedir)
            #loop through contents of the ingest directory
            for folder, subs, files in os.walk(sourcedir):
                for filename in files:
                    #split the filename up
                    ext = os.path.splitext(filename)[1][1:]
                    newfile = os.path.splitext(filename)[0]
                    #rename the file with a unique timestamp based name 
                    millis = int(round(time.time() * 1000))
                    newfilename = "%s.%s.%s" % (newfile, millis, ext)
                    log.info("newfilename: %s" % (newfilename))
                    #new file path
                    filepath = "%s/%s" % (folder,filename)
                    ftime = time.gmtime(os.path.getmtime(filepath))
                    #create date based year and month directories as needed
                    ctime_dir = "%s/%s" % (str(ftime.tm_year), str(ftime.tm_mon))
                    dest_dir="%s/%s" % (destdir, ctime_dir)
                    dest="%s/%s/%s" % (destdir, ctime_dir, filename)
                    newdest= "%s/%s" % (dest_dir, newfilename)
                    if not os.path.exists(dest_dir):
                        os.makedirs(dest_dir)
                    if not os.path.exists(dest):
                        log.info('Moving %s from %s to %s' % (ext,filename,dest))
                        shutil.move(filepath, dest)
                    else:
                        shutil.move(filepath, newdest)
        else:
            log.error("Source Directory {} doesn't exist".format(sourcedir))
