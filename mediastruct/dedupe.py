import os
import logging
import hashlib
import json
import glob
import re
import itertools
import shutil

log = logging.getLogger(__name__)
log.info('Launching the Dedupe Class')

class dedupe:
    def __init__(self, data_files,duplicates_dir):
        combined_dataset = dedupe.combine_array(self, data_files)
        print(combined_dataset)
        dedupe.dups(self,combined_dataset,duplicates_dir)

    def combine_array(self, data_files):
        combined_array = {}
        for i in data_files:
            if os.path.isfile(i):
                with open( i , 'r') as f:
                    array = json.load(f)
                    combined_array = {**combined_array, **array}
        return combined_array

    def dups(self, array, duplicates_dir):
        key_values = {}
        to_delete = {}
        for d in array:
            for e in array:
                index_line = {}
                match = 0
                if d != 'du' and e != 'du':
                    if array[d]['filehash'] == array[e]['filehash'] and array[d]['path'] != array[e]['path']:
                        print(e)
                        print("e: %s - %s - %s -  %s" % ( array[e]['filehash'],array[d]['path'],array[e]['path'], array[e]['filesize']))
                        index_line.update([ ('filehash',array[e]['filehash']) , ('path',array[e]['path']) , ('filesize',array[e]['filesize']) ])
                        for deletes in to_delete:
                            if array[e]['filehash'] == to_delete[deletes]['filehash'] or e == deletes:
                                match = 1
                        if match != 1:
                            to_delete[e] = index_line

        for key in to_delete:
            if os.path.isfile(array[key]['path']):
                shutil.move(array[key]['path'],duplicates_dir + '/')
            del array[key]

