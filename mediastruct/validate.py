import logging
import os
import sys
import time
import shutil
import json
import yaml
from glob import glob
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from mediastruct.utils import *

log = logging.getLogger(__name__)

class validate:
    '''This class goes file by file through the duplicates directory and ensures that there's a matching file in either the working
    or media directories. It then moves the file out of the duplicates directory into the validated directory so that the admin
    can do what he/she will with them as doubly verified duplicates'''

    METADATA_FILE = ".mediastruct"

    def __init__(self, duplicates_dir, media_dir, archive_dir, validated_dir, monitor=None):
        self.monitor = monitor
        self.duplicates_dir = duplicates_dir
        self.validated_dir = validated_dir

        if self.monitor:
            self.monitor.update_progress("validate", status="Starting", processed=0, total=0, current="Initializing validation process")

        # Load precomputed hashes
        duplicates = self.load_duplicates(duplicates_dir)
        mediahashes = self.load_media(media_dir)
        archivehashes = self.load_archive(archive_dir)

        # Build a hash table for fast lookups
        hash_table = self.build_hash_table(mediahashes, archivehashes)

        # Find matches using the hash table
        processed_duplicates = self.find_matches_with_hash_table(duplicates, hash_table)

        if self.monitor:
            self.monitor.update_progress("validate", status="Completed", processed=100, total=100, current="Validation finished")

    def load_duplicates(self, duplicates_dir):
        """Load hashes for duplicates from the JSON index file."""
        tobevalidated = []
        if not os.path.isdir(duplicates_dir):
            log.error(f"Validate - Duplicates directory {duplicates_dir} does not exist")
            return tobevalidated

        # Load the JSON index file for /data/ingest (which includes duplicates moved by dedupe)
        index_file = "/opt/mediastruct/data/ingest_index.json"
        if not os.path.isfile(index_file):
            log.error(f"Validate - Index file {index_file} does not exist")
            return tobevalidated

        with open(index_file, 'r') as f:
            array = json.load(f)

        total_files = sum(1 for key, value in array.items() if key != 'du' and duplicates_dir in value['path'])
        processed_files = 0

        if self.monitor:
            self.monitor.update_progress("validate", status="Running", processed=0, total=total_files, current="Loading duplicates")

        for key, value in array.items():
            if key != 'du' and duplicates_dir in value['path']:
                tobevalidated.append({'key': key, 'filehash': value['filehash'], 'path': value['path']})
                processed_files += 1
                if self.monitor and total_files > 0:
                    self.monitor.update_progress("validate", status="Running", processed=processed_files, total=total_files, current=f"Loading duplicate: {os.path.basename(value['path'])}")

        log.info(f"Validate - Loaded {len(tobevalidated)} duplicates from {index_file}")
        return tobevalidated

    def load_media(self, media_dir):
        """Load hashes for media files from the JSON index file."""
        mediahashes = []
        if not os.path.isdir(media_dir):
            log.error(f"Validate - Media directory {media_dir} does not exist")
            return mediahashes

        index_file = "/opt/mediastruct/data/media_index.json"
        if not os.path.isfile(index_file):
            log.error(f"Validate - Index file {index_file} does not exist")
            return mediahashes

        with open(index_file, 'r') as f:
            array = json.load(f)

        total_files = sum(1 for key, value in array.items() if key != 'du')
        processed_files = 0

        if self.monitor:
            self.monitor.update_progress("validate", status="Running", processed=0, total=total_files, current="Loading media files")

        for key, value in array.items():
            if key != 'du':
                mediahashes.append({'key': key, 'filehash': value['filehash'], 'path': value['path']})
                processed_files += 1
                if self.monitor and total_files > 0:
                    self.monitor.update_progress("validate", status="Running", processed=processed_files, total=total_files, current=f"Loading media file: {os.path.basename(value['path'])}")

        log.info(f"Validate - Loaded {len(mediahashes)} media files from {index_file}")
        return mediahashes

    def load_archive(self, archive_dir):
        """Load hashes for archive files from .mediastruct files."""
        archivehashes = []
        if not os.path.isdir(archive_dir):
            log.error(f"Validate - Archive directory {archive_dir} does not exist")
            return archivehashes

        archive_dir = Path(archive_dir)
        total_dirs = sum(1 for _ in archive_dir.iterdir() if _.is_dir() and _.name.isdigit())
        processed_dirs = 0

        if self.monitor:
            self.monitor.update_progress("validate", status="Running", processed=0, total=total_dirs, current="Loading archive directories")

        for sub_dir in archive_dir.iterdir():
            if not sub_dir.is_dir() or not sub_dir.name.isdigit():
                continue

            metadata_path = sub_dir / self.METADATA_FILE
            if not metadata_path.exists():
                log.warning(f"Validate - No .mediastruct file found in {sub_dir}")
                continue

            log.debug(f"Validate - Reading metadata file: {metadata_path}")
            try:
                with metadata_path.open("r") as f:
                    metadata = yaml.safe_load(f)
                for relative_path, file_hash in metadata["files"].items():
                    file_path = sub_dir / relative_path
                    archivehashes.append({'key': str(file_path), 'filehash': file_hash, 'path': str(file_path)})
            except Exception as e:
                log.error(f"Validate - Failed to read metadata file {metadata_path}: {e}")
                continue

            processed_dirs += 1
            if self.monitor and total_dirs > 0:
                self.monitor.update_progress("validate", status="Running", processed=processed_dirs, total=total_dirs, current=f"Loaded archive directory: {sub_dir.name}")

        log.info(f"Validate - Loaded {len(archivehashes)} archive files from {archive_dir}")
        return archivehashes

    def build_hash_table(self, mediahashes, archivehashes):
        """Build a hash table for fast lookups, mapping hashes to (source, filepath) pairs."""
        hash_table = {}
        total_entries = len(mediahashes) + len(archivehashes)
        processed_entries = 0

        if self.monitor:
            self.monitor.update_progress("validate", status="Running", processed=0, total=total_entries, current="Building hash table")

        for entry in mediahashes:
            filehash = entry['filehash']
            filepath = entry['path']
            if filehash not in hash_table:
                hash_table[filehash] = []
            hash_table[filehash].append(('media', filepath))
            processed_entries += 1
            if self.monitor and total_entries > 0:
                self.monitor.update_progress("validate", status="Running", processed=processed_entries, total=total_entries, current=f"Building hash table: media entry {processed_entries}")

        for entry in archivehashes:
            filehash = entry['filehash']
            filepath = entry['path']
            if filehash not in hash_table:
                hash_table[filehash] = []
            hash_table[filehash].append(('archive', filepath))
            processed_entries += 1
            if self.monitor and total_entries > 0:
                self.monitor.update_progress("validate", status="Running", processed=processed_entries, total=total_entries, current=f"Building hash table: archive entry {processed_entries}")

        log.info(f"Validate - Built hash table with {len(hash_table)} unique hashes")
        return hash_table

    def _get_unique_destination_path(self, src_path: str, dest_dir: str) -> str:
        """Generate a unique destination path by appending a timestamp if the file already exists."""
        filename = os.path.basename(src_path)
        dest_path = os.path.join(dest_dir, filename)
        
        if not os.path.exists(dest_path):
            return dest_path

        base, ext = os.path.splitext(filename)
        timestamp = int(time.time() * 1000)
        new_filename = f"{base}_{timestamp}{ext}"
        new_dest_path = os.path.join(dest_dir, new_filename)
        
        while os.path.exists(new_dest_path):
            timestamp += 1
            new_filename = f"{base}_{timestamp}{ext}"
            new_dest_path = os.path.join(dest_dir, new_filename)
        
        log.debug(f"Validate - Generated unique destination path: {new_dest_path}")
        return new_dest_path

    def validate_file(self, dup, hash_table):
        """Validate a single duplicate file using the hash table."""
        filename = os.path.basename(dup['path'])
        filehash = dup['filehash']
        src_path = dup['path']

        if filehash in hash_table:
            matches = hash_table[filehash]
            for source, filepath in matches:
                log.info(f"Validate - Duplicate {src_path} found in {source.capitalize()} {filepath}")
                break  # First match is sufficient

            # Move the duplicate file to validated directory
            dest_path = self._get_unique_destination_path(src_path, self.validated_dir)
            try:
                shutil.move(src_path, dest_path)
                log.info(f"Validate - Moved {src_path} to {dest_path}")
            except Exception as e:
                log.error(f"Validate - Failed to move {src_path} to {dest_path}: {e}")
        else:
            log.debug(f"Validate - No match found for duplicate {src_path}")

    def find_matches_with_hash_table(self, duplicates, hash_table):
        """Find matches using a hash table for O(n+m) complexity."""
        duplicates_len = len(duplicates)
        processed_duplicates = 0

        if self.monitor:
            self.monitor.update_progress("validate", status="Running", processed=0, total=duplicates_len, current="Finding matches")

        with ThreadPoolExecutor() as executor:
            futures = []
            for dup in duplicates:
                # Skip if the file has already been moved to validated
                if not os.path.exists(dup['path']):
                    log.debug(f"Validate - Skipping {dup['path']}: Already processed")
                    continue
                future = executor.submit(self.validate_file, dup, hash_table)
                futures.append(future)

            for future in futures:
                future.result()
                processed_duplicates += 1
                if self.monitor and duplicates_len > 0:
                    self.monitor.update_progress("validate", status="Running", processed=processed_duplicates, total=duplicates_len, current=f"Processed {processed_duplicates}/{duplicates_len} duplicates")

        log.info(f"Validate - Processed {processed_duplicates} duplicates")
        return processed_duplicates
