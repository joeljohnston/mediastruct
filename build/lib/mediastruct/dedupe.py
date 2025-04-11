import os
import sys
import logging
import json
import yaml
import glob
import re
import shutil
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from collections import OrderedDict

log = logging.getLogger(__name__)
log.info('Dedupe - Launching the Dedupe Class')

class dedupe:
    '''The dedupe process combines all of the hash indexes together and identifies files that match 
    by hash. It moves the duplicates out of the working directory and into the duplicates directory under two 
    conditions: 1) if the duplicate is a duplicate of files in the ingest or working directory structs,
    2) if the file is a duplicate of a file already in the archive directory set. No files from the 
    archive directory set are ever moved to the duplicates target directory.'''

    METADATA_FILE = ".mediastruct"
    MAX_THREADS = os.cpu_count() or 4  # Use number of CPU cores, default to 4 if unknown

    def __init__(self, data_files, duplicates_dir, archive_dir, monitor=None):
        self.monitor = monitor
        self.duplicates_dir = duplicates_dir
        if self.monitor:
            self.monitor.update_progress("dedupe", status="Starting", processed=0, total=0, current="Initializing deduplication")
        # Combine the datasets from JSON files
        combined_dataset = self.combine_array(data_files)
        # Run deduplication using precomputed hashes
        self.dups(combined_dataset, archive_dir)
        if self.monitor:
            self.monitor.update_progress("dedupe", status="Completed", processed=100, total=100, current="Deduplication finished")

    def combine_array(self, data_files):
        """Combine multiple JSON hash index files into a single dataset."""
        combined_array = {}
        total_files = len(data_files)
        processed_files = 0

        if self.monitor:
            self.monitor.update_progress("dedupe", status="Running", processed=0, total=total_files, current="Combining datasets")
        log.debug(f"Dedupe - Combining {total_files} data files")

        for i in data_files:
            if os.path.isfile(i):
                print("Loading:", i)
                log.debug(f"Dedupe - Loading file: {i}")
                with open(i, 'r') as f:
                    array = json.load(f)
                    combined_array = {**combined_array, **array}
                processed_files += 1
                if self.monitor and total_files > 0:
                    self.monitor.update_progress("dedupe", status="Running", processed=processed_files, total=total_files, current=f"Combining dataset: {i}")
                log.debug(f"Dedupe - Combined {processed_files}/{total_files} files ({(processed_files/total_files)*100:.1f}%)")
            else:
                log.warning(f"Dedupe - File not found: {i}")

        return combined_array

    def _get_archive_hashes(self, archive_dir: str) -> dict:
        """Build a hash-to-path mapping for all files in /data/archive/<number> directories using .mediastruct files."""
        archive_hashes = {}
        archive_dir = Path(archive_dir)

        if not archive_dir.exists():
            log.warning(f"Dedupe - Archive directory {archive_dir} does not exist")
            return archive_hashes

        # Count total directories for progress calculation
        total_dirs = sum(1 for _ in archive_dir.iterdir() if _.is_dir() and _.name.isdigit())
        processed_dirs = 0

        if self.monitor:
            self.monitor.update_progress("dedupe", status="Running", processed=0, total=total_dirs, current="Processing archive directories")
        log.debug(f"Dedupe - Processing {total_dirs} archive directories in {archive_dir}")

        # Iterate over each numbered directory in /data/archive
        for sub_dir in archive_dir.iterdir():
            if not sub_dir.is_dir() or not sub_dir.name.isdigit():
                log.debug(f"Dedupe - Skipping non-directory or non-numeric entry: {sub_dir}")
                continue

            # Read the .mediastruct file (now in YAML format)
            metadata_path = sub_dir / self.METADATA_FILE
            if not metadata_path.exists():
                log.warning(f"Dedupe - No .mediastruct file found in {sub_dir}")
                continue

            log.debug(f"Dedupe - Reading metadata file: {metadata_path}")
            try:
                with metadata_path.open("r") as f:
                    metadata = yaml.safe_load(f)
                for relative_path, file_hash in metadata["files"].items():
                    file_path = sub_dir / relative_path
                    if file_hash in archive_hashes:
                        log.info(f"Dedupe - Duplicate in archive: {file_path} matches {archive_hashes[file_hash]}")
                    else:
                        archive_hashes[file_hash] = str(file_path)
            except Exception as e:
                log.error(f"Dedupe - Failed to read metadata file {metadata_path}: {e}")
                continue

            processed_dirs += 1
            if self.monitor and total_dirs > 0:
                self.monitor.update_progress("dedupe", status="Running", processed=processed_dirs, total=total_dirs, current=f"Processed archive directory: {sub_dir.name}")
            log.debug(f"Dedupe - Processed {processed_dirs}/{total_dirs} archive directories ({(processed_dirs/total_dirs)*100:.1f}%)")

        return archive_hashes

    def _process_entry(self, entry, seen, to_keep, archive_dir_name):
        """Process a single entry to determine if it should be kept."""
        a, b, c = entry
        if b not in seen and archive_dir_name in c:
            log.info(f"Dedupe - To_keep: {a} - {b} - {c}")
            seen.add(b)
            to_keep.append(a)
        return seen, to_keep

    def _process_non_archive_entry(self, entry, seen, to_keep, archive_dir_name):
        """Process a single non-archive entry to determine if it should be kept."""
        a, b, c = entry
        if archive_dir_name not in c:
            if b not in seen:
                seen.add(b)
                if a not in to_keep:
                    log.info(f"Dedupe - To_keep: {a} - {b} - {c}")
                    to_keep.append(a)
        return seen, to_keep

    def _move_duplicate(self, key, array, archive_dir_name):
        """Move a single duplicate file to the duplicates directory."""
        from_path = array[key]['path']
        if os.path.isfile(from_path):
            log.info(f"Dedupe - To Delete: {from_path}")
            filename = os.path.basename(from_path)
            dest_path = os.path.join(self.duplicates_dir, filename)
            if os.path.isfile(dest_path):
                log.info(f"Dedupe - Found a duplicate named file {dest_path}")
                ext = os.path.splitext(from_path)[1][1:]
                newfile = os.path.splitext(filename)[0]
                millis = int(round(time.time() * 1000))
                newfilename = f"{newfile}.{millis}.{ext}"
                dest_path = os.path.join(self.duplicates_dir, newfilename)

            if archive_dir_name not in from_path:
                log.info(f"Dedupe - Moving Duplicate {from_path} to {dest_path}")
                shutil.move(from_path, dest_path)

    def dups(self, array, archive_dir):
        """Cycle through the dataset and find duplicate entries using precomputed hashes."""
        # Get hashes of all archived files from .mediastruct files
        archive_hashes = self._get_archive_hashes(archive_dir)

        # Initialize lists for deduplication
        dictlist = []
        to_keep = []
        to_delete = []
        seen = set()

        # Loop through combined dataset from JSON files (from /data/media/media)
        total_entries = len(array)
        processed_entries = 0

        log.info("Dedupe - Looping Through Combined Array and Creating list")
        if self.monitor:
            self.monitor.update_progress("dedupe", status="Running", processed=0, total=total_entries, current="Building deduplication list")

        for d in array:
            if d != 'du':
                dictlist_line = (d, array[d]['filehash'], array[d]['path'])
                dictlist.append(dictlist_line)
                processed_entries += 1
                if self.monitor and total_entries > 0:
                    self.monitor.update_progress("dedupe", status="Running", processed=processed_entries, total=total_entries, current=f"Processing entry: {d}")
                log.debug(f"Dedupe - Processed {processed_entries}/{total_entries} entries for deduplication list ({(processed_entries/total_entries)*100:.1f}%)")

        # Prioritize keeping files in the archive using multi-threading
        archive_dir_name = Path(archive_dir).name
        print("Archive Keyword:", archive_dir_name)
        total_dictlist = len(dictlist)
        processed_dictlist = 0

        log.info("Dedupe - Prioritizing archive files to keep")
        if self.monitor:
            self.monitor.update_progress("dedupe", status="Running", processed=0, total=total_dictlist, current="Prioritizing archive files")

        with ThreadPoolExecutor(max_workers=self.MAX_THREADS) as executor:
            futures = []
            for entry in dictlist:
                future = executor.submit(self._process_entry, entry, seen, to_keep, archive_dir_name)
                futures.append(future)

            for future in futures:
                seen, to_keep = future.result()
                processed_dictlist += 1
                if self.monitor and total_dictlist > 0:
                    self.monitor.update_progress("dedupe", status="Running", processed=processed_dictlist, total=total_dictlist, current=f"Checking archive entry: {processed_dictlist}")

        # Prune the list to exclude archive entries already marked to keep
        prunedict = [x for x in dictlist if x[0] not in to_keep]

        # Add non-archive files to keep list if they haven't been seen, using multi-threading
        total_prunedict = len(prunedict)
        processed_prunedict = 0

        log.info("Dedupe - Looping Through Combined Array and adding non-archived files to keep list")
        if self.monitor:
            self.monitor.update_progress("dedupe", status="Running", processed=0, total=total_prunedict, current="Processing non-archive files")

        with ThreadPoolExecutor(max_workers=self.MAX_THREADS) as executor:
            futures = []
            for entry in prunedict:
                future = executor.submit(self._process_non_archive_entry, entry, seen, to_keep, archive_dir_name)
                futures.append(future)

            for future in futures:
                seen, to_keep = future.result()
                processed_prunedict += 1
                if self.monitor and total_prunedict > 0:
                    self.monitor.update_progress("dedupe", status="Running", processed=processed_prunedict, total=total_prunedict, current=f"Checking non-archive entry: {processed_prunedict}")

        # Identify files to delete (duplicates not in archive)
        to_delete = [(x, y, z) for x, y, z in dictlist if x[0] not in to_keep and archive_dir_name not in z]

        print("seen_len", len(seen))
        print("to_delete_len", len(to_delete))

        # Check ingest directory for duplicates against archive
        ingest_dir = Path("/data/ingest")
        if ingest_dir.exists():
            total_ingest_files = sum(1 for _ in ingest_dir.iterdir() if _.is_file())
            processed_ingest_files = 0

            if self.monitor:
                self.monitor.update_progress("dedupe", status="Running", processed=0, total=total_ingest_files, current="Checking ingest directory")
            log.debug(f"Dedupe - Checking {total_ingest_files} files in ingest directory")

            for ingest_file in ingest_dir.iterdir():
                if not ingest_file.is_file():
                    continue
                # Since crawl should have indexed /data/ingest, we should have its hash in the combined dataset
                ingest_path = str(ingest_file)
                file_hash = None
                for key, value in array.items():
                    if value['path'] == ingest_path:
                        file_hash = value['filehash']
                        break
                if not file_hash:
                    log.warning(f"Dedupe - No hash found for ingest file {ingest_file} in combined dataset")
                    continue
                if file_hash in archive_hashes:
                    log.info(f"Dedupe - Ingest duplicate found: {ingest_file} matches {archive_hashes[file_hash]}")
                    filename = ingest_file.name
                    dest_path = os.path.join(self.duplicates_dir, filename)
                    if os.path.isfile(dest_path):
                        log.info(f"Dedupe - Found a duplicate named file {dest_path}")
                        ext = ingest_file.suffix[1:]
                        newfile = ingest_file.stem
                        millis = int(round(time.time() * 1000))
                        newfilename = f"{newfile}.{millis}.{ext}"
                        dest_path = os.path.join(self.duplicates_dir, newfilename)
                    log.info(f"Dedupe - Moving Ingest Duplicate {ingest_file} to {dest_path}")
                    shutil.move(str(ingest_file), dest_path)
                processed_ingest_files += 1
                if self.monitor and total_ingest_files > 0:
                    self.monitor.update_progress("dedupe", status="Running", processed=processed_ingest_files, total=total_ingest_files, current=f"Checking ingest file: {ingest_file.name}")
                log.debug(f"Dedupe - Checked {processed_ingest_files}/{total_ingest_files} ingest files ({(processed_ingest_files/total_ingest_files)*100:.1f}%)")

        # Loop through the "to be deleted" files and move them to the duplicates directory using multi-threading
        total_to_delete = len(to_delete)
        processed_to_delete = 0

        if self.monitor:
            self.monitor.update_progress("dedupe", status="Running", processed=0, total=total_to_delete, current="Moving duplicates")
        log.debug(f"Dedupe - Moving {total_to_delete} duplicate files")

        with ThreadPoolExecutor(max_workers=self.MAX_THREADS) as executor:
            futures = []
            for k in range(total_to_delete):
                key = to_delete[k][0]
                future = executor.submit(self._move_duplicate, key, array, archive_dir_name)
                futures.append(future)

            for future in futures:
                future.result()
                processed_to_delete += 1
                if self.monitor and total_to_delete > 0:
                    self.monitor.update_progress("dedupe", status="Running", processed=processed_to_delete, total=total_to_delete, current=f"Moved {processed_to_delete}/{total_to_delete} duplicates")
