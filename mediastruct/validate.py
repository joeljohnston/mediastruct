import logging
import os
import sys
import time
import shutil
import xxhash
import shutil
from glob import glob
from os import walk, remove, stat
from mediastruct.utils import *

log = logging.getLogger(__name__)

class validate:
    '''This class goes file by file through the duplicates directry and ensures that there's a matching file in either the working
    or media directories. It then moves the file out of the duplicates directory into the validated directory so that the admin
    can do what he/she will with them as doubly verified duplicates'''

    def __init__(self,duplicates_dir,media_dir,archive_dir,validated_dir):
        #fire duplicate iteration function
        duplicates = validate.iter_duplicates(self,duplicates_dir)
        vali_media = validate.iter_media(self,media_dir)
        vali_archive = validate.iter_archive(self,archive_dir)

        compare = validate.find_matches(self,duplicates,vali_media,vali_archive,validated_dir)

    def iter_duplicates(self, duplicates_dir):
        tobevalidated = [] 
        if os.path.isdir(duplicates_dir):
            for path, dirs, files in walk(duplicates_dir):
                for filename in files:
                    filepath = joinpath( path, filename)
                    if os.path.isfile(filepath):
                        filesize = stat(filepath).st_size
                        try:
                            filehash = xxhash.xxh64(open(filepath, 'rb').read()).hexdigest()
                        except:
                            print("Didnt like this file: ",filepath)
                        if filehash != '':
                            tobevalidated.append([{'filehash':filehash,'path':filepath}])
        return tobevalidated 

    def iter_media(self,media_dir):
        mediahashes = [] 
        if os.path.isdir(media_dir):
            for path, dirs, files in walk(media_dir):
                for filename in files:
                    filepath = joinpath( path, filename)
                    if os.path.isfile(filepath):
                        filesize = stat(filepath).st_size
                        try:
                            filehash = xxhash.xxh64(open(filepath, 'rb').read()).hexdigest()
                        except:
                            print("Didnt like this file: ",filepath)
                        if filehash != '':
                            mediahashes.append([{'filehash':filehash,'path':filepath}])
        return mediahashes


    def iter_archive(self,archive_dir):
        archivehashes = [] 
        if os.path.isdir(archive_dir):
            for path, dirs, files in walk(archive_dir):
                for filename in files:
                    filepath = joinpath( path, filename)
                    if os.path.isfile(filepath):
                        filesize = stat(filepath).st_size
                        try:
                            filehash = xxhash.xxh64(open(filepath, 'rb').read()).hexdigest()
                        except:
                            print("Didnt like this file",filepath)
                        if filehash != '':
                            archivehashes.append([{'filehash':filehash,'path':filepath}])
        return archivehashes 

    def find_matches(self,duplicates,mediahashes,archivehashes,validated_dir):
        #iterate through dupliates dataset and compare hashes with every file  
        duplicates_len = len(duplicates)
        mediahashes_len = len(mediahashes)
        archivehashes_len = len(archivehashes)
        matched = 0
        for dup in range(duplicates_len):
            for med in range(mediahashes_len):
                if duplicates[dup][0]['filehash'] == mediahashes[med][0]['filehash']:
                    matched = 1
                    log.info("Validate - Duplicate %s found in Media %s" % ( duplicates[dup][0]['path'], mediahashes[med][0]['path']))
                    
            for arc in range(archivehashes_len):
                if duplicates[dup][0]['filehash'] == archivehashes[arc][0]['filehash']:
                    matched = 1
                    log.info("Validate - Duplicate %s found in Media %s" % ( duplicates[dup][0]['path'], archivehashes[arc][0]['path']))

            if matched == 1:
                shutil.move(duplicates[dup][0]['path'], validated_dir)
