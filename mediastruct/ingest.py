import os
import sys
import time
import shutil
import logging
from glob import glob

log = logging.getLogger(__name__)
log.info('Ingest - Launching the Ingest Class')

class ingest:
    '''The ingest class manages contents entering the workflow by organizing files by their last modified date
    into the working directory / media directory'''

    def __init__(self, _sourcedir, _destdir, monitor=None):
        self.monitor = monitor
        # Setup logging for this child class
        log = logging.getLogger(__name__)
        if self.monitor:
            self.monitor.update_progress("ingest", status="Starting", processed=0, total=0, current="Initializing ingest process")
        self.mvrnm(_sourcedir, _destdir)
        if self.monitor:
            self.monitor.update_progress("ingest", status="Completed", processed=100, total=100, current="Ingest finished")

    def mvrnm(self, sourcedir, destdir):
        '''This function ensures that no data is lost via file collisions as files are moved into the working dir
        by renaming them with a .<unixdatetimestamp> addition to the existing filename'''
        log.info("Ingest - Directory root: %s" % (sourcedir))
        # Ensure the source directory exists
        if not os.path.isdir(sourcedir):
            log.error("Ingest - Source Directory {} doesn't exist".format(sourcedir))
            if self.monitor:
                self.monitor.update_progress("ingest", status="Failed", processed=0, total=0, current="Source directory does not exist")
            return

        # Count total files for progress calculation
        total_files = sum(len(files) for folder, _, files in os.walk(sourcedir))
        processed_files = 0

        if self.monitor:
            self.monitor.update_progress("ingest", status="Running", processed=0, total=total_files, current="Starting file ingestion")

        # Loop through contents of the ingest directory
        for folder, subs, files in os.walk(sourcedir):
            for filename in files:
                # Split the filename up
                ext = os.path.splitext(filename)[1][1:]
                newfile = os.path.splitext(filename)[0]
                # Rename the file with a unique timestamp-based name
                millis = int(round(time.time() * 1000))
                newfilename = "%s.%s.%s" % (newfile, millis, ext)
                log.info("Ingest - oldfilename: %s" % (filename))
                log.info("Ingest - newfilename: %s" % (newfilename))
                # New file path
                filepath = "%s/%s" % (folder, filename)
                try:
                    ftime = time.gmtime(os.path.getmtime(filepath))
                except Exception as e:
                    log.error(f"Ingest - Failed to get mtime for {filepath}: {e}")
                    continue

                # Create date-based year and month directories as needed
                ctime_dir = "%s/%s" % (str(ftime.tm_year), str(ftime.tm_mon))
                dest_dir = "%s/%s" % (destdir, ctime_dir)
                dest = "%s/%s/%s" % (destdir, ctime_dir, filename)
                newdest = "%s/%s" % (dest_dir, newfilename)

                try:
                    if not os.path.exists(dest_dir):
                        os.makedirs(dest_dir)
                    if not os.path.exists(dest):
                        log.info('Ingest - Moving %s from %s to %s' % (ext, filename, dest))
                        shutil.move(filepath, dest)
                    else:
                        log.info("Ingest - Duplicate Name found - new path: %s" % (newdest))
                        shutil.move(filepath, newdest)
                except Exception as e:
                    log.error(f"Ingest - Failed to move {filepath} to {dest} or {newdest}: {e}")
                    continue

                processed_files += 1
                if self.monitor and total_files > 0:
                    self.monitor.update_progress("ingest", status="Running", processed=processed_files, total=total_files, current=f"Moving file: {filename}")
                # Add a small delay to simulate a slower operation (for testing)
                time.sleep(0.1)  # 100ms delay per file

        if self.monitor and total_files > 0:
            self.monitor.update_progress("ingest", status="Running", processed=processed_files, total=total_files, current="Finishing ingestion")
