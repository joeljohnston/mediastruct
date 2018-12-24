import os
import logging
import hashlib
import json
import glob
import re
import itertools
import shutil
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
        dictlist = {}
        to_delete = {}
        id_list = []
        dictlist_line = {}
        #loop thru combined dataset
        arraylen = len(array)
        for d in array:
            dictlist_line = []
            if d != 'du':
                #dictlist_line = ([ ('id',d) ])
                #print(d)
                dictlist_line = ([ ('id',d) ])
                dictlist[array[d]['filehash']] = dictlist_line
            results = [k for k,v in Counter(dictlist[d]) if len(v)>1]
        print(results)
        for i in range(len(results)):
            print("%s %s" % (results[i]['id'],results[i]))
#            for deletes in to_delete:
#                if to_delete[deletes]['id'] not in id_list:
#                    id_list.append(to_delete[deletes]['id'])
#            arraylen = arraylen - 1
#            print("Remaining Records: ", arraylen)
#            #loop through each item in the combined dataset, searching for identical hashes
#            if d not in id_list:
#                for e in array:
#                    #init dictionary
#                    index_line = {}
#                    match = 0
#                    #omit the du record in the json
#                    if d != 'du' and e != 'du' and 'archive' not in array[e]['path']:
#                        #as we loop through match hashes that don't have identical path/name
#                        if array[d]['filehash'] == array[e]['filehash'] and array[d]['path'] != array[e]['path']:
#                            index_line.update([ ('id',e) , ('filehash',array[e]['filehash']) , ('path',array[e]['path']) , ('filesize',array[e]['filesize']) ])
#                            #for deletes in to_delete:
#                            #    if array[e]['filehash'] == to_delete[deletes]['filehash'] or e == deletes:
#                            #        match = 1
#                            #if match != 1 and 'archive' not in array[e]['path']: 
#                            #print(e)
#                            if e not in to_delete.values():
#                                log.info("Duplicate: %s - %s - %s -  %s" % ( array[e]['filehash'],array[d]['path'],array[e]['path'], array[e]['filesize']))
#                                to_delete[e] = index_line
#                                #print(to_delete)
#        #loop through the "to be deleted" files and move them to the duplicates directory
#        for key in to_delete:
#            if os.path.isfile(array[key]['path']):
#                log.info("Moving Duplicate %s to %s" % (array[key]['path'],duplicates_dir))
#                shutil.move(array[key]['path'],duplicates_dir + '/')
#            del array[key]

