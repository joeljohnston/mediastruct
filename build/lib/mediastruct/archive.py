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
    '''The archive function forms a volume-grouped collection of data based on the size you specify for your volumes.'''

    def __init__(self, archive_dir, data_dir, media_dir, mediasize, monitor=None):
        self.monitor = monitor
        if self.monitor:
            self.monitor.update_progress("archive", status="Running", processed=0, total=0, current="Initializing")
        totalmedia = 0
        next_volume = self.dirstruct(archive_dir, media_dir)
        files_to_archive = self.assembleVolume(archive_dir, data_dir, media_dir, mediasize, next_volume)
        self.archive_files(files_to_archive, mediasize, media_dir, next_volume, archive_dir)

    def dirstruct(self, archive_dir, media_dir):
        '''Builds the volume directory structure based on what is already there'''
        folders = 0
        vol_size = 0
        folders += len([name for name in os.listdir(archive_dir)])
        print("Folders:", folders)
        next_volume = folders + 1
        print("Next Volume Number: ", next_volume)
        utils.mkdir_p(self, (archive_dir + '/' + str(next_volume)))
        totalmedia = 0
        return next_volume

    def assembleVolume(self, archive_dir, data_dir, media_dir, mediasize, next_volume):
        '''Loop through the contents of the working directory and build a recordset for files to be moved'''
        dirname = re.split(r"\/", media_dir)
        dirname_len = len(dirname) - 1
        log.info("Archive - Target Volume Size: %s" % (mediasize))
        archivefiles = []
        array = {}
        mediatotal = 0
        print("archive_dir: ", archive_dir)
        print("data_dir: ", data_dir)
        print("media_dir: ", media_dir)
        print("dirname: ", dirname[dirname_len])
        if os.path.isfile('%s/%s_index.json' % (data_dir, dirname[dirname_len])):
            with open('%s/%s_index.json' % (data_dir, dirname[dirname_len]), 'r') as f:
                array = json.load(f)
                total_files = sum(1 for g in array if g != 'du')
                if self.monitor:
                    self.monitor.update_progress("archive", total=total_files, current="Assembling volume")
                processed = 0
                for g in array:
                    if g != 'du':
                        this_year = str(array[g]['year'])
                        archivefiles.append([{"year": this_year, "path": array[g]['path'], "filesize": array[g]['filesize']}])
                        processed += 1
                        if self.monitor:
                            self.monitor.update_progress("archive", processed=processed, current=array[g]['path'])
            sortedarchive = sorted(archivefiles, key=lambda x: x[0]['year'])
        return sortedarchive

    def archive_files(self, files_to_archive, mediasize, media_dir, next_volume, archive_dir):
        mediasize = int(mediasize) * 1000 * 1000 * 1000
        log.info("Archive - Mediasize: %s" % (mediasize))
        mediatotal = 0
        arraylen = len(files_to_archive)
        if self.monitor:
            self.monitor.update_progress("archive", total=arraylen, current="Archiving files")
        for h in range(arraylen):
            log.info("===================%s==================" % (h))
            fullpath = re.split(r"\/", files_to_archive[h][0]['path'])
            fpath_len = len(fullpath)
            year = files_to_archive[h][0]['year']
            dest_dir = archive_dir + '/' + str(next_volume) + '/' + year + '/' + fullpath[fpath_len-2]
            dest_path = archive_dir + '/' + str(next_volume) + '/' + year + '/' + fullpath[fpath_len-2] + '/' + fullpath[fpath_len-1]
            from_path = files_to_archive[h][0]['path']
            thisfilesize = files_to_archive[h][0]['filesize']
            log.info("Archive - FileSize: %s" % (thisfilesize))
            mediatotal = thisfilesize + int(mediatotal)
            log.info(("Archive: %s - Mediatotal: %s / MediaSize: %s") % (next_volume, mediatotal, mediasize))
            if mediatotal <= mediasize:
                if not os.path.isdir(dest_dir):
                    utils.mkdir_p(self, dest_dir)
                if os.path.isfile(files_to_archive[h][0]['path']):
                    log.info("Archive - Moving: %s to %s" % (files_to_archive[h][0]['path'], dest_path))
                    dest_file_size = 0
                    while dest_file_size != thisfilesize:
                        try:
                            shutil.move(files_to_archive[h][0]['path'], dest_path)
                        except:
                            sys.exit("Error Moving File")
                        dest_file_size = os.path.getsize(dest_path)
            else:
                log.info("==================================NEXT ARCHIVE ======================")
                next_volume = self.dirstruct(archive_dir, media_dir)
                mediatotal = 0
            log.info("Total Volume Size: %sGB" % (mediatotal))
            if self.monitor:
                self.monitor.update_progress("archive", processed=h + 1, current=from_path)
        if self.monitor:
            self.monitor.update_progress("archive", status="Completed", processed=arraylen, total=arraylen, current="")
