"""Find duplicate files inside a directory tree."""
import os
import re
from os import walk, remove, stat
from os.path import join as joinpath
import logging
import xxhash
import shutil
import json
import glob
import uuid
from mediastruct.utils import *

log = logging.getLogger(__name__)
log.info('Launching the Crawl Class')

class crawl:
    """Iterate a dir tree and build a sum index"""
    def __init__(self, rootdir, datadir):
        dirname = re.split(r"\/", rootdir)
        print("rootdir: ",rootdir)
        log.info("Running Crawl init")
        if os.path.isdir(rootdir):
            if os.path.isfile('%s/%s_index.json' % (datadir, dirname[2])):
                with open('%s/%s_index.json' % (datadir, dirname[2]), 'r') as f:
                    array = json.load(f)
                    print(array)
                    if array['du']:
                        currentdu = utils.getFolderSize(self, rootdir)
                        print("currentdu: ", currentdu)
                        if currentdu != array['du'] or array['du'] == 0:
                            index = crawl.index_sum(self, rootdir, datadir)
                        else:
                            log.info("The Index matches the Directory")
            else:
                index = crawl.index_sum(self, rootdir, datadir)

    def index_sum(self, rootdir, datadir):
        """Index md5 sum of all files in a directory tree and write to Json file"""
        dirname = re.split(r"\/", rootdir)
        sum_dict = {}
        for path, dirs, files in walk(rootdir):
            for filename in files:
                index_line = {}
                fileid = str(uuid.uuid1())
                filepath = joinpath( path, filename )
                filesize = stat( filepath ).st_size
                filehash = xxhash.xxh64(open(filepath,'rb').read()).hexdigest()
                index_line.update([ ('filehash',filehash) , ('path',filepath) , ('filesize',filesize) ])
                sum_dict[fileid] = index_line
        sum_dict['du'] = utils.getFolderSize(self, rootdir)
        log.info(sum_dict)
        indexfilepath = ('%s/%s_index.json' % (datadir, dirname[2]))
        indexfile = open(indexfilepath,"w")
        jsonoutput = json.dumps(sum_dict)
        indexfile.write(jsonoutput)
        indexfile.close()
        return sum_dict
