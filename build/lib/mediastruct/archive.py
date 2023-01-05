import os
import sys
import time
import shutil
import json
from glob import glob
from os import walk, remove, stat
from mediastruct.utils import *
from collections import OrderedDict

class archive:
    '''The archive function forms a volume-grouped collection of data based on the size you specifiy for your volumes.
    If using tape you could set the mediasize value in the config.ini to 1000 (GB) for instance and the archive function 
    will assemble a sequentially numbered series of volumes of that size or slightly less, moving up to 1TB into each i
    directory.  You can then write hat volume off to tape/optical as necessary.'''

    #class init
    def __init__(self,archive_dir,data_dir,media_dir,mediasize):
        totalmedia = 0
        next_volume = archive.dirstruct(self,archive_dir,media_dir)
        files_to_archive = archive.assembleVolume(self,archive_dir,data_dir,media_dir,mediasize,next_volume)
        archive.archive_files(self,files_to_archive,mediasize,media_dir,next_volume,archive_dir)

    def dirstruct(self,archive_dir,media_dir):
        '''builds the volume directory structure based on what is already there'''
        folders = 0
        vol_size = 0
        #create the next directory after the last non-empty directory
        folders += len([name for name in os.listdir(archive_dir)]) 
        print("Folders:", folders)
        next_volume = folders + 1
        print("Next Volume Number: ", next_volume)
        #response = input("Is this okay?")
        #if response != "yes":
        #    sys.exit("Exiting")

        #create next volume directory 
        utils.mkdir_p(self, (archive_dir + '/' + str(next_volume)))
        totalmedia = 0
        return next_volume

    def assembleVolume(self,archive_dir,data_dir,media_dir,mediasize,next_volume):
        '''loop thru the contents of the working directory and build a recordset for files to be moved'''
        dirname = re.split(r"\/", media_dir)
        dirname_len = len(dirname) -1
        log.info("Archive - Target Volume Size: %s" %  (mediasize))
        archivefiles = []
        array = {}
        mediatotal = 0
        print("archive_dir: ", archive_dir)
        print("data_dir: ", data_dir)
        print("media_dir: ", media_dir)
        print("dirname: ", dirname[dirname_len])
        #check for the data file passed in
        if os.path.isfile('%s/%s_index.json' % (data_dir, dirname[dirname_len])):
            #loop through
            with open('%s/%s_index.json' % (data_dir, dirname[dirname_len]), 'r') as f:
                    array = json.load(f)
                    #print(array)
                    #array = sorted(unordered_array.items())
                    for g in array:
                        if g != 'du':
                            this_year = str(array[g]['year'])
                            #print("dir ",dirname[dirname_len])
                            #log.info("Archive - Adding %s to Archive" % (array[g]['path']))
                            #adding file to the dictionary used to move files
                            archivefiles.append([{"year": this_year, "path": array[g]['path'], "filesize":array[g]['filesize']}])
            sortedarchive = sorted(archivefiles, key=lambda x: x[0]['year'])
        return sortedarchive

    def archive_files(self, files_to_archive,mediasize,media_dir,next_volume,archive_dir):
        mediasize = int(mediasize) * 1000 * 1000 * 1000
        log.info("Archive - Mediasize: %s" % (mediasize))
        mediatotal = 0
        arraylen = len(files_to_archive)
        for h in range(arraylen):
            log.info("===================%s==================" % (h))
            #get year
            fullpath = re.split(r"\/",files_to_archive[h][0]['path'])
            fpath_len = len(fullpath)
            year = files_to_archive[h][0]['year']
            dest_dir = archive_dir + '/' + str(next_volume) + '/' + year + '/' + fullpath[fpath_len-2]
            dest_path = archive_dir + '/' + str(next_volume) + '/' + year + '/' + fullpath[fpath_len-2] + '/' + fullpath[fpath_len-1]
            from_path =  files_to_archive[h][0]['path']
            #log.info("Archive - Archiving %s to %s" % (from_path, dest_path))
            thisfilesize = files_to_archive[h][0]['filesize']
            log.info("Archive - FileSize: %s" % (thisfilesize))
            mediatotal = thisfilesize + int(mediatotal)
            #log.info("Archive - Media Total: %s" % (mediatotal))
            log.info(("Archive: %s - Mediatotal: %s / MediaSize: %s") % ( next_volume, mediatotal, mediasize))
            if mediatotal <= mediasize:
                if not os.path.isdir(dest_dir):
                    utils.mkdir_p(self, dest_dir)
                if os.path.isfile(files_to_archive[h][0]['path']):
                    log.info("Archive - Moving : %s to %s" % (files_to_archive[h][0]['path'],dest_path))

                    #Verify the file has been moved and confirm file size
                    dest_file_size = 0

                    while dest_file_size != thisfilesize:
                        #Move the file from the origin path to the destination path
                        try:
                            shutil.move(files_to_archive[h][0]['path'], dest_path)
                        except:
                            sys.exit("Error Moving File")

                        dest_file_size = os.path.getsize(dest_path)
            else:
                log.info("==================================NEXT ARCHIVE ======================")
                next_volume = archive.dirstruct(self,archive_dir,media_dir)
                mediatotal=0
            log.info("Total Volume Size: %sGB" % (mediatotal))
