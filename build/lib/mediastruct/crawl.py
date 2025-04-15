"""Find duplicate files inside a directory tree."""
import os
import re
import logging
import xxhash
import shutil
import json
import yaml
import glob
import uuid
import time
import datetime
from datetime import timedelta
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
from mediastruct.utils import *
from os import walk, remove, stat
from os.path import join as joinpath

# Setup logging from the parent class
log = logging.getLogger(__name__)

# Function to hash a file (must be defined at the module level for ProcessPoolExecutor)
def hash_file(file_path: str) -> tuple[str, str]:
    """Hash a single file using xxhash."""
    try:
        file_path = Path(file_path)  # Convert to Path object
        file_hash = xxhash.xxh64(file_path.open('rb').read()).hexdigest()
        return str(file_path), file_hash
    except Exception as e:
        log.error(f"Crawl - Failed to hash file {file_path}: {e}")
        return str(file_path), None

class crawl:
    """Iterate a dir tree and build a sum index - We first load our json data file and inspect the value of du.  This is 
    compared to a fresh 'quick' check of the directory size using utils.getFolderSize.  If they are different we are 
    going to re-index this directory and re-write all of our file hashes to the json file. This saves time on directory structures 
    such as the archive, that rarely change"""
    METADATA_FILE = ".mediastruct"
    MAX_AGE_DAYS = 120
    MAX_PROCESSES = os.cpu_count() or 4  # Use number of CPU cores, default to 4 if unknown

    def __init__(self, force, rootdir, datadir, monitor=None):
        self.monitor = monitor
        dirname = re.split(r"\/", rootdir)
        dirname_len = len(dirname) - 1
        print('dirname_len: ', dirname_len)
        log.info("Crawl - Crawling %s" % (rootdir))
        if os.path.isdir(rootdir):
            # Always index the directory, relying on .mediastruct files to avoid rehashing
            log.info('Crawl - Indexing %s' % (rootdir))
            index = self.index_sum(rootdir, datadir, force)

    def _is_metadata_current(self, metadata_path: str) -> bool:
        """Check if the .mediastruct metadata file exists and is less than 120 days old."""
        metadata_path = Path(metadata_path)
        if not metadata_path.exists():
            log.debug(f"Crawl - Metadata file {metadata_path} does not exist")
            return False
        try:
            with metadata_path.open("r") as f:
                metadata = yaml.safe_load(f)
            timestamp = datetime.datetime.fromisoformat(metadata["timestamp"])
            age = datetime.datetime.now() - timestamp
            is_current = age <= timedelta(days=self.MAX_AGE_DAYS)
            log.debug(f"Crawl - Metadata file {metadata_path} is {'current' if is_current else 'outdated'} (age: {age})")
            return is_current
        except (yaml.YAMLError, KeyError, ValueError) as e:
            log.error(f"Crawl - Failed to read metadata file {metadata_path}: {e}")
            return False

    def _create_or_update_metadata(self, directory: str, force: bool) -> dict:
        """Create or update the .mediastruct metadata file in the given directory, including all subdirectories."""
        directory = Path(directory)
        metadata_path = directory / self.METADATA_FILE
        metadata = {"timestamp": datetime.datetime.now().isoformat(), "files": {}}

        # If metadata exists and is current, return it unless force is True
        if not force and self._is_metadata_current(metadata_path):
            log.debug(f"Crawl - Using existing metadata file: {metadata_path}")
            with metadata_path.open("r") as f:
                return yaml.safe_load(f)

        # Walk the directory tree to build a tiered index
        total_files = sum(len(files) for _, _, files in walk(directory))
        processed_files = 0

        if self.monitor:
            self.monitor.update_progress("crawl", status="Running", processed=0, total=total_files, current=f"Hashing files in {directory}")
        log.debug(f"Crawl - Hashing {total_files} files in {directory} and subdirectories with {self.MAX_PROCESSES} processes")

        file_paths = []
        for root, _, files in walk(directory):
            relative_root = Path(root).relative_to(directory)
            for filename in files:
                if filename == self.METADATA_FILE:
                    continue
                file_path = Path(root) / filename
                relative_path = str(relative_root / filename)
                file_paths.append((str(file_path), relative_path))

        # Use a process pool to hash files in parallel
        with ProcessPoolExecutor(max_workers=self.MAX_PROCESSES) as executor:
            futures = {executor.submit(hash_file, file_path): (file_path, relative_path) for file_path, relative_path in file_paths}
            for future in futures:
                file_path, relative_path = futures[future]
                try:
                    _, file_hash = future.result()
                    if file_hash:
                        metadata["files"][relative_path] = file_hash
                    processed_files += 1
                    # Update monitor progress every 100 files to reduce overhead
                    if processed_files % 100 == 0 and self.monitor and total_files > 0:
                        self.monitor.update_progress("crawl", status="Running", processed=processed_files, total=total_files, current=f"Hashing file: {relative_path}")
                    log.debug(f"Crawl - Hashed {processed_files}/{total_files} files in {directory} ({(processed_files/total_files)*100:.1f}%)")
                except Exception as e:
                    log.error(f"Crawl - Failed to process file {file_path}: {e}")

        try:
            with metadata_path.open("w") as f:
                yaml.dump(metadata, f, default_flow_style=False)
            log.debug(f"Crawl - Wrote metadata file: {metadata_path}")
        except Exception as e:
            log.error(f"Crawl - Failed to write metadata file {metadata_path}: {e}")

        log.info(f"Crawl - Indexed {len(metadata['files'])} files in {directory}")
        return metadata

    def index_sum(self, rootdir, datadir, force):
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
                metadata = self._create_or_update_metadata(path, force)
                # Add the hashes from the .mediastruct file to sum_dict
                for relative_path, file_hash in metadata["files"].items():
                    file_path = os.path.join(path, relative_path)
                    fileid = str(uuid.uuid1())
                    filesize = stat(file_path).st_size
                    this_year = str(datetime.datetime.fromtimestamp(os.path.getmtime(file_path))).split('-')[0]
                    index_line = {
                        'filehash': file_hash,
                        'path': file_path,
                        'filesize': filesize,
                        'year': this_year
                    }
                    sum_dict[fileid] = index_line
                    processed_files += 1
                    if self.monitor and total_files > 0:
                        self.monitor.update_progress("crawl", status="Running", processed=processed_files, total=total_files, current=f"Indexing file: {relative_path}")
                    log.debug(f"Crawl - Indexed {processed_files}/{total_files} files ({(processed_files/total_files)*100:.1f}%)")

        sum_dict['du'] = utils.getFolderSize(self, rootdir)
        indexfilepath = ('%s/%s_index.json' % (datadir, dirname[dirname_len]))
        try:
            with open(indexfilepath, "w") as indexfile:
                jsonoutput = json.dumps(sum_dict)
                indexfile.write(jsonoutput)
            log.debug(f"Crawl - Wrote index file: {indexfilepath}")
            log.info(f"Crawl - Indexed {len(sum_dict) - 1} files to {indexfilepath}")
        except Exception as e:
            log.error(f"Crawl - Failed to write index file {indexfilepath}: {e}")

        # Return the key-based dictionary with updated hash values
        log.info("Crawl - Completed crawl of %s)" % (rootdir))
        return sum_dict
