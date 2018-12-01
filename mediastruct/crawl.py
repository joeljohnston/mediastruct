"""Find duplicate files inside a directory tree."""
import os
import re
from os import walk, remove, stat
from os.path import join as joinpath
import logging
import hashlib
import shutil
import json
import glob

log = logging.getLogger(__name__)
log.info('Launching the Crawl Class')

class crawl:
    """Iterate a dir tree and build a sum index"""
    def __init__(self,rootdir):
        dirname = re.split(r"\/", rootdir)
        print("project: ",dirname[2])
        print("rootdir: ",rootdir)
        log.info("Running Crawl init")
        if os.path.isdir(rootdir):
            if os.path.isfile('data/%s_index.json' % (dirname[2])):
                with open('data/%s_index.json' % (dirname[2]), 'r') as f:
                    array = json.load(f)
                    if array['du']:
                        currentdu = crawl.getFolderSize(self,rootdir)
                        if currentdu != array['du']:
                            index = crawl.index_sum(self, rootdir)
                        else:
                            print("The Index matches the Directory")
            else:
                index = crawl.index_sum(self, rootdir)

    def index_sum(self, rootdir):
        """Index md5 sum of all files in a directory tree and write to Json file"""
        dirname = re.split(r"\/", rootdir)
        sum_dict = {}
        for path, dirs, files in walk(rootdir):
            for filename in files:
                index_line = {}
                filepath = joinpath( path, filename)
                filesize = stat( filepath ).st_size
                filehash = hashlib.md5(open(filepath,'rb').read()).hexdigest()
                index_line.update([ ('path',filepath) , ('filesize',filesize) ])
                print(index_line)
                sum_dict[filehash] = index_line
        sum_dict['du'] = crawl.getFolderSize(self, rootdir)
        log.info(sum_dict)
        indexfilepath = ('data/%s_index.json' % (dirname[2]))
        indexfile = open(indexfilepath,"w")
        jsonoutput = json.dumps(sum_dict)
        indexfile.write(jsonoutput)
        indexfile.close()
        return sum_dict

    def getFolderSize(self,start_path = '.'):
        total_size = 0
        for dirpath, dirnames, filenames in os.walk(start_path):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                total_size += os.path.getsize(fp)
        return total_size
