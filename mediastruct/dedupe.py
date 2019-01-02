import os
import sys
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
log.info('Dedupe - Launching the Dedupe Class')

class dedupe:
    '''the dedupe process combines all of the hash indexes together and identifies files that match 
    by hash. It moves the duplicates out of the working directory and into the directory under two 
    conditions. 1) if the duplicate is a duplicate of files in the ingest, or working directory structs
    2) if the file is a duplicate of a file already in the archive directory set.  No files from the 
    archive directory set are ever asoved to the duplicates target directory.'''

    #class init
    def __init__(self,data_files,duplicates_dir,archive_dir):
        combined_dataset = dedupe.combine_array(self, data_files)
        dedupe.dups(self,combined_dataset,duplicates_dir,archive_dir)

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

    def countX(lst, x): 
        return lst.count(x) 

    #cycle through the dataset and find duplicate entries
    def dups(self, array, duplicates_dir, archive_dir):
        archivepath = re.split(r"\/",archive_dir)
        arc_len = len(archivepath)
        archive_dir = archivepath[arc_len-1]
        print("Archive Keyword: %s" % (archive_dir))
        #init dictionaries
        dictlist = []
        to_keep = []
        to_delete = []
        seen = set()
        #loop thru combined dataset
        arraylen = len(array)

        log.info("Dedupe - Looping Through Combined Array and Creating list")
        for d in array:
            if d != 'du':
                dictlist_line = (d,array[d]['filehash'],array[d]['path'])
                dictlist.append(dictlist_line)

        #excluding archive entries from equation
        for a, b, c in dictlist:
            if b not in seen and archive_dir in c:
                log.info("Dedupe - To_keep: %s - %s - %s" % (a,b,c)) 
                seen.add(b)
                to_keep.append(a)

        prunedict = [(x) for x in dictlist if not x in to_keep ]

        log.info("Dedupe - Looping Through Combined Array and adding archived files to keep list")
        for r in range(len(prunedict)):
            for a, b, c in prunedict:
                if archive_dir not in c:
                    if b not in seen:
                        seen.add(b)
                        if a not in to_keep:
                            log.info("Dedupe - To_keep: %s - %s - %s" % (a,b,c)) 
                            to_keep.append(a)
                            break

        to_delete = [(x,y,z) for x, y, z in dictlist if x not in to_keep and archive_dir not in z]

        print("seen_len", len(seen))
        print("to_delete_len", len(to_delete))
        #loop through the "to be deleted" files and move them to the duplicates directory
        for k in range(len(to_delete)):
            key = to_delete[k][0]
            from_path =  array[key]['path']
            if os.path.isfile(from_path):
                log.info("Dedupe - To Delete : %s" %  (from_path))
                filename = os.path.basename(from_path)
                dest_path = str("%s/%s" % (duplicates_dir, filename))
                if os.path.isfile(from_path):
                    if os.path.isfile(dest_path):
                        log.info("Dedupe - Found a duplicate named file %s" % (dest_path))
                        ext = os.path.splitext(from_path)[1][1:]
                        newfile = os.path.splitext(filename)[0]
                        millis = int(round(time.time() * 1000))
                        newfilename = str("%s.%s.%s" % (filename, millis, ext))
                        dest_path = str("%s/%s" % (duplicates_dir, newfilename))

                    if archive_dir not in from_path:
                        log.info("Dedupe - Moving Duplicate %s to %s" % (from_path,duplicates_dir))
                        shutil.move(from_path, dest_path)
