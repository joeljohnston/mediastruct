import logging
import os
import sys
import time
import shutil
import xxhash
from glob import glob
from os import walk, remove, stat
from pathlib import Path
from mediastruct.utils import *

log = logging.getLogger(__name__)

class validate:
    '''This class goes file by file through the duplicates directory and ensures that there's a matching file in either the working
    or media directories. It then moves the file out of the duplicates directory into the validated directory so that the admin
    can do what he/she will with them as doubly verified duplicates'''

    def __init__(self, duplicates_dir, media_dir, archive_dir, validated_dir, monitor=None):
        self.monitor = monitor
        if self.monitor:
            self.monitor.update_progress("validate", status="Starting", processed=0, total=0, current="Initializing validation process")

        # Fire duplicate iteration function
        duplicates = self.iter_duplicates(duplicates_dir)
        vali_media = self.iter_media(media_dir)
        vali_archive = self.iter_archive(archive_dir)

        compare = self.find_matches(duplicates, vali_media, vali_archive, validated_dir)

        if self.monitor:
            self.monitor.update_progress("validate", status="Completed", processed=100, total=100, current="Validation finished")

    def iter_duplicates(self, duplicates_dir):
        tobevalidated = []
        if not os.path.isdir(duplicates_dir):
            log.error(f"Validate - Duplicates directory {duplicates_dir} does not exist")
            return tobevalidated

        # Count total files for progress calculation
        total_files = sum(len(files) for _, _, files in walk(duplicates_dir))
        processed_files = 0

        if self.monitor:
            self.monitor.update_progress("validate", status="Running", processed=0, total=total_files, current="Iterating duplicates directory")

        for path, dirs, files in walk(duplicates_dir):
            for filename in files:
                filepath = os.path.join(path, filename)
                if os.path.isfile(filepath):
                    filesize = stat(filepath).st_size
                    try:
                        filehash = xxhash.xxh64(open(filepath, 'rb').read()).hexdigest()
                    except Exception as e:
                        print(f"Didnt like this file: {filepath}")
                        log.error(f"Validate - Failed to hash file {filepath}: {e}")
                        continue

                    if filehash != '':
                        tobevalidated.append([{'filehash': filehash, 'path': filepath}])

                    processed_files += 1
                    if self.monitor and total_files > 0:
                        self.monitor.update_progress("validate", status="Running", processed=processed_files, total=total_files, current=f"Processing duplicates: {filename}")

        return tobevalidated

    def iter_media(self, media_dir):
        mediahashes = []
        if not os.path.isdir(media_dir):
            log.error(f"Validate - Media directory {media_dir} does not exist")
            return mediahashes

        # Count total files for progress calculation
        total_files = sum(len(files) for _, _, files in walk(media_dir))
        processed_files = 0

        if self.monitor:
            self.monitor.update_progress("validate", status="Running", processed=0, total=total_files, current="Iterating media directory")

        for path, dirs, files in walk(media_dir):
            for filename in files:
                filepath = os.path.join(path, filename)
                if os.path.isfile(filepath):
                    filesize = stat(filepath).st_size
                    try:
                        filehash = xxhash.xxh64(open(filepath, 'rb').read()).hexdigest()
                    except Exception as e:
                        print(f"Didnt like this file: {filepath}")
                        log.error(f"Validate - Failed to hash file {filepath}: {e}")
                        continue

                    if filehash != '':
                        mediahashes.append([{'filehash': filehash, 'path': filepath}])

                    processed_files += 1
                    if self.monitor and total_files > 0:
                        self.monitor.update_progress("validate", status="Running", processed=processed_files, total=total_files, current=f"Processing media: {filename}")

        return mediahashes

    def iter_archive(self, archive_dir):
        archivehashes = []
        if not os.path.isdir(archive_dir):
            log.error(f"Validate - Archive directory {archive_dir} does not exist")
            return archivehashes

        # Count total files for progress calculation
        total_files = sum(len(files) for _, _, files in walk(archive_dir))
        processed_files = 0

        if self.monitor:
            self.monitor.update_progress("validate", status="Running", processed=0, total=total_files, current="Iterating archive directory")

        for path, dirs, files in walk(archive_dir):
            for filename in files:
                filepath = os.path.join(path, filename)
                if os.path.isfile(filepath):
                    filesize = stat(filepath).st_size
                    try:
                        filehash = xxhash.xxh64(open(filepath, 'rb').read()).hexdigest()
                    except Exception as e:
                        print(f"Didnt like this file: {filepath}")
                        log.error(f"Validate - Failed to hash file {filepath}: {e}")
                        continue

                    if filehash != '':
                        archivehashes.append([{'filehash': filehash, 'path': filepath}])

                    processed_files += 1
                    if self.monitor and total_files > 0:
                        self.monitor.update_progress("validate", status="Running", processed=processed_files, total=total_files, current=f"Processing archive: {filename}")

        return archivehashes

    def _get_unique_destination_path(self, src_path: str, dest_dir: str) -> str:
        """Generate a unique destination path by appending a timestamp if the file already exists."""
        filename = os.path.basename(src_path)
        dest_path = os.path.join(dest_dir, filename)
        
        if not os.path.exists(dest_path):
            return dest_path

        # File exists, generate a unique name by appending a timestamp
        base, ext = os.path.splitext(filename)
        timestamp = int(time.time() * 1000)  # Milliseconds
        new_filename = f"{base}_{timestamp}{ext}"
        new_dest_path = os.path.join(dest_dir, new_filename)
        
        # Ensure the new path is unique (increment timestamp if necessary)
        while os.path.exists(new_dest_path):
            timestamp += 1
            new_filename = f"{base}_{timestamp}{ext}"
            new_dest_path = os.path.join(dest_dir, new_filename)
        
        log.debug(f"Validate - Generated unique destination path: {new_dest_path}")
        return new_dest_path

    def find_matches(self, duplicates, mediahashes, archivehashes, validated_dir):
        """Iterate through duplicates dataset and compare hashes with every file."""
        duplicates_len = len(duplicates)
        mediahashes_len = len(mediahashes)
        archivehashes_len = len(archivehashes)
        processed_duplicates = 0

        if self.monitor:
            self.monitor.update_progress("validate", status="Running", processed=0, total=duplicates_len, current="Finding matches")

        for dup in range(duplicates_len):
            matched = 0
            filename = os.path.basename(duplicates[dup][0]['path'])
            for med in range(mediahashes_len):
                if duplicates[dup][0]['filehash'] == mediahashes[med][0]['filehash']:
                    matched = 1
                    log.info(f"Validate - Duplicate {duplicates[dup][0]['path']} found in Media {mediahashes[med][0]['path']}")
                    break  # No need to check further if a match is found

            if not matched:
                for arc in range(archivehashes_len):
                    if duplicates[dup][0]['filehash'] == archivehashes[arc][0]['filehash']:
                        matched = 1
                        log.info(f"Validate - Duplicate {duplicates[dup][0]['path']} found in Archive {archivehashes[arc][0]['path']}")
                        break

            if matched == 1:
                src_path = duplicates[dup][0]['path']
                # Generate a unique destination path to avoid conflicts
                dest_path = self._get_unique_destination_path(src_path, validated_dir)
                try:
                    shutil.move(src_path, dest_path)
                    log.info(f"Validate - Moved {src_path} to {dest_path}")
                except Exception as e:
                    log.error(f"Validate - Failed to move {src_path} to {dest_path}: {e}")

            processed_duplicates += 1
            if self.monitor and duplicates_len > 0:
                self.monitor.update_progress("validate", status="Running", processed=processed_duplicates, total=duplicates_len, current=f"Matching duplicates: {filename}")

        return processed_duplicates
