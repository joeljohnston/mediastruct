import os
import sys
import time
import shutil
from glob import glob

class ingest(object):

    def __init__(self):
        #mvrnm(): 
        print("yeah")

    def mvrnm(self):
        fromdir = sys.argv[1]
        print("From Dir:%s " % (fromdir))
        todir = sys.argv[2]
        print("To Dir:%s" % (todir))
        os.chdir(fromdir)

        for folder, subs, files in os.walk(fromdir):
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
                dest_dir="%s/%s" % (todir, ctime_dir)
                dest="%s/%s/%s" % (todir, ctime_dir, filename)
                print("Destination Path: %s" % (dest))
                newdest= "%s/%s" % (dest_dir, newfilename)
                print("newdest: ", newdest)
                if not os.path.exists(dest_dir):
                        os.makedirs(dest_dir)
                if not os.path.exists(dest):
                    shutil.move(filepath, dest)
                else:
                    shutil.move(filepath, newdest)
