import os
import logging
import hashlib
import json
import glob
import re
import itertools
import shutil
import itertools
import time
from itertools import chain
from collections import OrderedDict
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
        to_keep = []
        dictordered = OrderedDict()
        to_delete = []
        seen = set()
        #loop thru combined dataset
        arraylen = len(array)

        log.info("Looping Through Combined Array and Creating list")
        for d in array:
            if d != 'du':
                dictlist_line = (d,array[d]['filehash'],array[d]['path'])
                dictlist.append(dictlist_line)

        log.info("Looping Through Combined Array and adding archived files to keep list")
        for a, b, c in dictlist:
            if 'archive' in c:
                log.info("File Already Archived: %s" % (c))
                if a in to_keep:
                    to_keep.append(a)

        log.info("Looping the dictionary and removing any archive references")
        for a in to_keep:
            dictlist.remove(a)

        log.info("Looping the dictionary and isolating 1st record")
        for a, b, c in dictlist:
            if not b in seen:
                seen.add(b)
                to_keep.append(a)

        to_delete = [(x,y,z) for x, y, z in dictlist if x not in to_keep]
        print("seen_len", len(seen))
        print("to_delete_len", len(to_delete))

        #loop through the "to be deleted" files and move them to the duplicates directory
        for k in range(len(to_delete)):
            key = to_delete[k][0]
            if os.path.isfile(array[key]['path']):
                from_path =  array[key]['path']
                log.info("To Delete : %s" %  (to_delete[k][0]))
                filename = os.path.basename(from_path)
                dest_path = str("%s/%s" % (duplicates_dir, filename))
                if os.path.isfile(from_path):
                    if os.path.isfile(dest_path):
                        log.info("Found a duplicate named file %s" % (dest_path))
                        ext = os.path.splitext(from_path)[1][1:]
                        newfile = os.path.splitext(filename)[0]
                        millis = int(round(time.time() * 1000))
                        newfilename = str("%s.%s.%s" % (filename, millis, ext))
                        dest_path = str("%s/%s" % (duplicates_dir, newfilename))

                    if 'archive' not in from_path:
                        log.info("Moving Duplicate %s to %s" % (array[key]['path'],duplicates_dir))
                        shutil.move(from_path, dest_path)
