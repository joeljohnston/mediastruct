import os
import sys
import time
import shutil
import logging
from glob import glob

################################################################################################
###################################### Setup Logging ###########################################
################################################################################################
# create logger
logger = logging.getLogger('ingest')
logger.setLevel(logging.DEBUG)

# create file handler which logs even debug messages
fh = logging.FileHandler('logs/ingest.log')
fh.setLevel(logging.DEBUG)
# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)

# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(fh)
logger.addHandler(ch)

class ingest(object):

    def __init__(self,_sourcedir,_destdir):
        #logging.basicConfig(filename='logs/ingest.log', level=logging.ERROR)
        #formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        ingest.mvrnm(self,_sourcedir,_destdir)
        print("yeah")

    def mvrnm(self,sourcedir,destdir):
        logger.info("Dirctory root: {}".format(sourcedir))
        if os.path.isfile(sourcedir):
            os.chdir(sourcedir)
            for folder, subs, files in os.walk(sourcedir):
                with open(os.path.join(folder, 'python-outfile.txt'), 'w') as dest:
                    for filename in files:
                        print("filename: ", filename)
                        ext = os.path.splitext(filename)[1][1:]
                        newfile = os.path.splitext(filename)[0]
                    print("ext: ", ext)
                    print("file: ", newfile)
                    newfilename = "%s.%s.%s" % (newfile, int((time.time() + 0.5) * 1000 ), ext)
                    print("newfilename: ", newfilename)
                    filepath = "%s/%s" % (folder,filename)
                    print("filepath: ", filepath)
                    ftime = time.gmtime(os.path.getmtime(filepath))
                    ctime_dir = "%s/%s" % (str(ftime.tm_year), str(ftime.tm_mon))
                    dest_dir="%s/%s" % (destdir, ctime_dir)
                    dest="%s/%s/%s" % (destdir, ctime_dir, filename)
                    print("Destination Path: %s" % (dest))
                    newdest= "%s/%s" % (dest_dir, newfilename)
                    print("newdest: ", newdest)
                    if not os.path.exists(dest_dir):
                            os.makedirs(dest_dir)
                    if not os.path.exists(dest):
                        shutil.move(filepath, dest)
                    else:
                        shutil.move(filepath, newdest)
        else:
            logger.error("Source Directory {} doesn't exist".format(sourcedir))
