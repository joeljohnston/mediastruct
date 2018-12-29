"""Find duplicate files inside a directory tree."""
import os
import re
import logging
import xxhash
import shutil
import json
import glob
import uuid
import time
from mediastruct.utils import *
from os import walk, remove, stat
from os.path import join as joinpath

#setup logging from the parent class
log = logging.getLogger(__name__)

class crawl:
    """Iterate a dir tree and build a sum index - We first load our json data file and inspect the value of du.  This is 
    compared to a fresh 'quick' check of the directory size using utils.getFolderSize.  If they are different we are 
    going to re-index this directory and re-write all of our file hashes to the json file. This saves time on directory structures 
    such as the archive, that rarely change"""
    def __init__(self,force,rootdir,datadir):
        dirname = re.split(r"\/",rootdir)
        dirname_len = len(dirname) -1
        print('dirname_len: ', dirname_len)
        log.info("Crawling %s" % (rootdir))
        if os.path.isdir(rootdir):
            if force == True:
                log.info('Force Attribute set to True - indexing %s' % (rootdir))
                index = crawl.index_sum(self,rootdir,datadir)
            else:
                #if our data file exists for this directory load it and compare
                if os.path.isfile('%s/%s_index.json' % (datadir,dirname[dirname_len])):
                    print('dirname: ',dirname[dirname_len])
                    with open('%s/%s_index.json' % (datadir,dirname[dirname_len]), 'r') as f:
                        array = json.load(f)
                        #here we are comparing
                        if array['du']:
                            currentdu = utils.getFolderSize(self,rootdir)
                            if currentdu != array['du'] or array['du'] == 0:
                                index = crawl.index_sum(self,rootdir,datadir)
                            else:
                                log.info("The Index matches the Directory")
                #otherwise start the index process
                else:
                    index = crawl.index_sum(self,rootdir,datadir)

    def index_sum(self,rootdir,datadir):
        """Index hash sum of all files in a directory tree and write to Json file"""
        #isolate the name of the directory from our argument
        dirname = re.split(r"\/",rootdir)
        dirname_len = len(dirname) -1
        sum_dict = {}
        #walk the structure of the target dir tree
        for path, dirs, files in walk(rootdir):
            for filename in files:
                index_line = {}
                fileid = str(uuid.uuid1())
                filepath = joinpath(path,filename)
                filesize = stat(filepath).st_size
                this_year = int(str(os.path.splitext(filepath)[0][1:]).split('/')[2])
                #this can be changed out with any hash library you prefer
                print("year: ", this_year)
                try:
                    filehash = xxhash.xxh64(open(filepath,'rb').read()).hexdigest()
                    if filehash != '':
                        index_line.update([('filehash',filehash),('path',filepath),('filesize',filesize),('year',this_year)])
                        sum_dict[fileid] = index_line
                except:
                    print("broken file: ", filepath)
                    log.info("broken file: %s" %  (filepath))
                    time.sleep(120)
                #we're creating a key-based dictionary here
        sum_dict['du'] = utils.getFolderSize(self,rootdir)
        indexfilepath = ('%s/%s_index.json' % (datadir, dirname[dirname_len]))
        indexfile = open(indexfilepath,"w")
        jsonoutput = json.dumps(sum_dict)
        indexfile.write(jsonoutput)
        indexfile.close()
        #return the key-based dictionary with updated hash values
        log.info("Completed crawl of %s)" % (rootdir))
        return sum_dict
