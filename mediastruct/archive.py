import os
import sys
import time
import shutil
import json
from glob import glob
from os import walk, remove, stat
from mediastruct.utils import *

class archive:
    '''The archive function forms a volume-grouped collection of data based on the size you specifiy for your volumes.
    If using tape you could set the mediasize value in the config.ini to 1000 (GB) for instance and the archive function 
    will assemble a sequentially numbered series of volumes of that size or slightly less, moving up to 1TB into each i
    directory.  You can then write hat volume off to tape/optical as necessary.'''
    
    #class init
    def __init__(self,archive_dir,data_dir,media_dir,mediasize):
        totalmedia = 0

        next_volume = archive.dirstruct(self,archive_dir,media_dir,mediasize)
        files_to_archive = archive.assembleVolume(self,archive_dir,data_dir,media_dir,mediasize,next_volume)
        archive.archive_files(self,files_to_archive,next_volume,archive_dir)

    def dirstruct(self,archive_dir,media_dir,mediasize):
        '''builds the volume directory structure based on what is already there'''
        folders = 0
        vol_size = 0
        #create the next directory after the last non-empty directory
        folders += len([name for name in os.listdir(archive_dir)]) 
        next_volume = folders + 1
        #create next volume directory 
        utils.mkdir_p(self, (archive_dir + '/' + str(next_volume)))
        return next_volume

    def assembleVolume(self,archive_dir,data_dir,media_dir,mediasize,next_volume):
        '''loop thru the contents of the working directory and build a recordset for files to be moved'''
        dirname = re.split(r"\/", media_dir)
        mediasize = int(mediasize) * 1000 * 1000 * 1000
        print("Target Volume Size: ", mediasize)
        archivefiles = [] 
        array = {}
        mediatotal = 0
        #check for the data file passed in
        if os.path.isfile('%s/%s_index.json' % (data_dir, dirname[2])):
            #loop through 
            with open('%s/%s_index.json' % (data_dir, dirname[2]), 'r') as f:
                    array = json.load(f)
                    for g in array:
                        if g != 'du':
                            thisfilesize = array[g]['filesize']
                            mediatotal = thisfilesize + int(mediatotal)
                            print("Total: ", mediatotal)
                            if mediatotal <= mediasize:
                                log.info("Adding %s to Archive" % (array[g]['path']))
                                #adding file to the dictionary used to move files
                                archivefiles.append([{"volume": next_volume,"path": array[g]['path']}])
                            else:
                                next_volume = archive.dirstruct(self,archive_dir,media_dir,mediasize)
                                mediatotal = 1 
                                

        return archivefiles

    def archive_files(self, files_to_archive,next_volume,archive_dir):
        arraylen = len(files_to_archive)
        print("files_to_archive: ", files_to_archive)
        print("arraylen: ", arraylen)
        for h in range(arraylen):
            #get year
            fullpath = re.split(r"\/",files_to_archive[h][0]['path'])
            print(fullpath)
            year = fullpath[3]
            print(year)
            dest_dir = archive_dir + '/' + str(files_to_archive[h][0]['volume']) + '/' + year + '/' + fullpath[4]
            dest_path = archive_dir + '/' + str(files_to_archive[h][0]['volume']) + '/' + year + '/' + fullpath[4] + '/' + fullpath[5]
            if not os.path.isdir(dest_dir):
                utils.mkdir_p(self, dest_dir)
            print("destination path: ", dest_path)
            shutil.move(files_to_archive[h][0]['path'], dest_path)

