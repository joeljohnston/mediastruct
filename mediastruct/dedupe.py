import os
import logging
import hashlib
import json
import glob
import re
import itertools
import shutil
import itertools
from itertools import chain
from collections import OrderedDict
from collections import Counter
log = logging.getLogger(__name__)
log.info('Launching the Dedupe Class')

class dedupe:
    '''the dedupe process combines all of the hash indexes together and identifies files that match 
    by hash. It moves the duplicates out of the working directory and into the directory under two 
    conditions. 1) if the duplicate is a duplicate of files in the ingest, or working directory structs
    2) if the file is a duplicate of a file already in the archive directory set.  No files from the 
    archive directory set are ever moved to the duplicates target directory.'''

    #class init
    def __init__(self, data_files,duplicates_dir):
        log.info("Deduping")
        combined_dataset = dedupe.combine_array(self, data_files)
        dedupe.dups(self,combined_dataset,duplicates_dir)

    #put the various datasets together
    def combine_array(self, data_files):
        combined_array = {}
        for i in data_files:
            if os.path.isfile(i):
                with open( i , 'r') as f:
                    print("Loading :", i)
                    array = json.load(f)
                    combined_array = {**combined_array, **array}
        return combined_array

    #cycle through the dataset and find duplicate entries
    def dups(self, array, duplicates_dir):
        #init dictionaries
        dictlist = [] 
        dictordered = OrderedDict()
        duplicate = []
        duplicates = []
        to_delete = {}
        id_list = []
        dictlist_line = {}
        #loop thru combined dataset
        arraylen = len(array)

        for d in array:
            if d != 'du':
                #dictlist_line = ([ ('id',d) ])
                #print(d)
                dictlist_line = (d,array[d]['filehash'])
                dictlist.append(dictlist_line)
                #dictlist[array[d]['filehash']] = dictlist_line
                #results = [k for k,v in Counter(dictlist[d]['id']) if len(v)>1]
                #results = [k for k in Counter(dictlist[d]['id']) if len(k)>1]
        
        for i,j in dictlist:
            #print('%s,%s' % (i,j))
            dictordered.setdefault(j,[]).append(i)

        to_delete = list(chain.from_iterable([j for i, j in dictordered.items() if len(j)>1]))

        #loop through the "to be deleted" files and move them to the duplicates directory
        for key in to_delete:
            if os.path.isfile(array[key]['path']):
                log.info("Moving Duplicate %s to %s" % (array[key]['path'],duplicates_dir))
                #get year
                fullpath = re.split(r"\/",array[key][0]['path'])
                year = fullpath[3]
                dest_dir = archive_dir + '/' + str(files_to_archive[h][0]['volume']) + '/' + year + '/' + fullpath[4]
                dest_path = archive_dir + '/' + str(files_to_archive[h][0]['volume']) + '/' + year + '/' + fullpath[4] + '/' + fullpath[5]
                if not os.path.isdir(dest_dir):
                    utils.mkdir_p(self, dest_dir)
                from_path =  files_to_archive[h][0]['path']
                log.info("Archiving %s to %s" % (from_path, dest_path))
                if os.path.isfile(files_to_archive[h][0]['path']):
                    print("Moving :", files_to_archive[h][0]['path'])
                    shutil.move(files_to_archive[h][0]['path'], dest_path)
                
                shutil.move(array[key]['path'],duplicates_dir + '/')
            #del array[key]
