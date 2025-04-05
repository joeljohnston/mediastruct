"""Find duplicate files inside a directory tree."""
import os
import re
import logging
import xxhash
import shutil
import json
import glob
import uuid
import time
import datetime
from datetime import timedelta
from pathlib import Path
from mediastruct.utils import *
from os import walk, remove, stat
from os.path import join as joinpath

# Setup logging from the parent class
log = logging.getLogger(__name__)

class crawl:
    """Iterate a dir tree and build a sum index - We first load our json data file and inspect the value of du.  This is 
    compared to a fresh 'quick' check of the directory size using utils.getFolderSize.  If they are different we are 
    going to re-index this directory and re-write all of our file hashes to the json file. This saves time on directory structures 
    such as the archive, that rarely change"""
    METADATA_FILE = ".mediastruct"
    MAX_AGE_DAYS = 120

    def __init__(self, force, rootdir, datadir, monitor=None):
        self.monitor = monitor
        dirname = re.split(r"\/", rootdir)
        dirname_len = len(dirname) - 1
        print('dirname_len: ', dirname_len)
        log.info("Crawl - Crawling %s" % (rootdir))
        if os.path.isdir(rootdir):
            if force:
                log.info('Crawl - Force Attribute set to True - indexing %s' % (rootdir))
                index = self.index_sum(rootdir, datadir)
            else:
                # If our data file exists for this directory, load it and compare
                if os.path.isfile('%s/%s_index.json' % (datadir, dirname[dirname_len])):
                    print('dirname: ', dirname[dirname_len])
                    with open('%s/%s_index.json' % (datadir, dirname[dirname_len]), 'r') as f:
                        array = json.load(f)
                        # Here we are comparing
                        if array['du']:
                            currentdu = utils.getFolderSize(self, rootdir)
                            if currentdu != array['du'] or array['du'] == 0:
                                index = self.index_sum(rootdir, datadir)
                            else:
                                log.info("Crawl - The Index matches the Directory")
                # Otherwise, start the index process
                else:
                    index = self.index_sum(rootdir, datadir)

    def _is_metadata_current(self, metadata_path: str) -> bool:
        """Check if the .mediastruct metadata file exists and is less than 120 days old."""
        metadata_path = Path(metadata_path)
        if not metadata_path.exists():
            log.debug(f"Crawl - Metadata file {metadata_path} does not exist")
            return False
        try:
            with metadata_path.open("r") as f:
                metadata = json.load(f)
            timestamp = datetime.datetime.fromisoformat(metadata["timestamp"])
            age = datetime.datetime.now() - timestamp
            is_current = age <= timedelta(days=self.MAX_AGE_DAYS)
            log.debug(f"Crawl - Metadata file {metadata_path} is {'current' if is_current else 'outdated'} (age: {age})")
            return is_current
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            log.error(f"Crawl - Failed to read metadata file {metadata_path}: {e}")
            return False

    def _create_or_update_metadata(self, directory: str) -> dict:
        """Create or update the .mediastruct metadata file in the given directory, including all subdirectories."""
        directory = Path(directory)
        metadata_path = directory / self.METADATA_FILE
        metadata = {"timestamp": datetime.datetime.now().isoformat(), "files": {}}

        # If metadata exists and is current, return it
        if self._is_metadata_current(metadata_path):
            log.debug(f"Crawl - Using existing metadata file: {metadata_path}")
            with metadata_path.open("r") as f:
                return json.load(f)

        # Walk the directory tree to build a tiered index
        total_files = sum(len(files) for _, _, files in walk(directory))
        processed_files = 0

        if self.monitor:
            self.monitor.update_progress("crawl", status="Running", processed=0, total=total_files, current=f"Hashing files in {directory}")
        log.debug(f"Crawl - Hashing {total_files} files in {directory} and subdirectories")

        for root, _, files in walk(directory):
            relative_root = Path(root).relative_to(directory)
            for filename in files:
                if filename == self.METADATA_FILE:
                    continue
                file_path = Path(root) / filename
                relative_path = str(relative_root / filename)
                try:
                    file_hash = xxhash.xxh64(file_path.open('rb').read()).hexdigest()
                    if file_hash:
                        metadata["files"][relative_path] = file_hash
                    processed_files += 1
                    if self.monitor and total_files > 0:
                        self.monitor.update_progress("crawl", status="Running", processed=processed_files, total=total_files, current=f"Hashing file: {relative_path}")
                    log.debug(f"Crawl - Hashed {processed_files}/{total_files} files in {directory} ({(processed_files/total_files)*100:.1f}%)")
                except Exception as e:
                    log.error(f"Crawl - Failed to hash file {file_path}: {e}")

        try:
            with metadata_path.open("w") as f:
                json.dump(metadata, f, indent=2)
            log.debug(f"Crawl - Wrote metadata file: {metadata_path}")
        except Exception as e:
            log.error(f"Crawl - Failed to write metadata file {metadata_path}: {e}")

        return metadata

    def index_sum(self, rootdir, datadir):
        """Index hash sum of all files in a directory tree and write to Json file"""
        # Isolate the name of the directory from our argument
        dirname = re.split(r"\/", rootdir)
        dirname_len = len(dirname) - 1
        sum_dict = {}

        # Walk the structure of the target dir tree
        total_files = sum(len(files) for path, dirs, files in walk(rootdir))
        processed_files = 0

        if self.monitor:
            self.monitor.update_progress("crawl", status="Running", processed=0, total=total_files, current=f"Indexing files in {rootdir}")
        log.debug(f"Crawl - Indexing {total_files} files in {rootdir}")

        for path, dirs, files in walk(rootdir):
            # Check if the current path matches /data/media/media/<year> or /data/archive/<archive_no>
            path_str = str(Path(path).resolve())
            if re.match(r".*/data/media/media/\d{4}$", path_str) or re.match(r".*/data/archive/\d+$", path_str):
                log.debug(f"Crawl - Found target directory: {path}")
                self._create_or_update_metadata(path)

            for filename in files:
                index_line = {}
                fileid = str(uuid.uuid1())
                filepath = joinpath(path, filename)
                filesize = stat(filepath).st_size
                this_year = str(datetime.datetime.fromtimestamp(os.path.getmtime(filepath))).split('-')[0]

                # This can be changed out with any hash library you prefer
                log.info("Crawl - Hashing File: %s" % (filepath))
                try:
                    filehash = xxhash.xxh64(open(filepath, 'rb').read()).hexdigest()
                    if filehash != '':
                        index_line.update([('filehash', filehash), ('path', filepath), ('filesize', filesize), ('year', this_year)])
                        sum_dict[fileid] = index_line
                except Exception as e:
                    print("broken file: ", filepath)
                    log.info("Crawl - broken file: %s" % (filepath))
                    time.sleep(120)
                # We're creating a key-based dictionary here
                processed_files += 1
                if self.monitor and total_files > 0:
                    self.monitor.update_progress("crawl", status="Running", processed=processed_files, total=total_files, current=f"Indexing file: {filename}")
                log.debug(f"Crawl - Indexed {processed_files}/{total_files} files ({(processed_files/total_files)*100:.1f}%)")

        sum_dict['du'] = utils.getFolderSize(self, rootdir)
        indexfilepath = ('%s/%s_index.json' % (datadir, dirname[dirname_len]))
        try:
            with open(indexfilepath, "w") as indexfile:
                jsonoutput = json.dumps(sum_dict)
                indexfile.write(jsonoutput)
            log.debug(f"Crawl - Wrote index file: {indexfilepath}")
        except Exception as e:
            log.error(f"Crawl - Failed to write index file {indexfilepath}: {e}")

        # Return the key-based dictionary with updated hash values
        log.info("Crawl - Completed crawl of %s)" % (rootdir))
        return sum_dict
