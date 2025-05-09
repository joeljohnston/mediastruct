import os
import xxhash
import shutil
import json
import yaml
import logging
import glob
import time
import threading
from os import walk  # Explicitly import walk
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

# Setup logging
log = logging.getLogger(__name__)

class validate:
    """Validate duplicates by comparing their hashes against media and archive files."""
    # Adjust MAX_THREADS for I/O-bound tasks (higher multiplier for Threadripper)
    MAX_THREADS = (os.cpu_count() or 4) * 2  # Double the CPU count for I/O-bound tasks

    def __init__(self, duplicates_dir, media_dir, archive_dir, validated_dir, datadir, monitor=None):
        self.monitor = monitor
        self.datadir = datadir
        self.duplicates_dir = duplicates_dir
        self.media_dir = media_dir
        self.archive_dir = archive_dir
        self.validated_dir = validated_dir
        # Thread lock for file moving to prevent race conditions
        self.move_lock = threading.Lock()

        if self.monitor:
            self.monitor.update_progress("validate", status="Starting", processed=0, total=0, current="Initializing validation")

        # Create the validated directory if it doesn't exist
        if not os.path.isdir(self.validated_dir):
            try:
                os.makedirs(self.validated_dir)
                log.info(f"Validate - Created validated directory: {self.validated_dir}")
            except Exception as e:
                log.error(f"Validate - Failed to create validated directory {self.validated_dir}: {e}")

        # Log the number of threads being used
        log.info(f"Validate - Using {self.MAX_THREADS} threads for multi-threading")

        # Load media and archive hashes
        media_hashes = self.load_media()
        archive_hashes = self.load_archive()
        # Combine hashes into a single lookup table
        hash_table = self.build_hash_table(media_hashes, archive_hashes)
        # Validate duplicates and move them to the validated directory
        self.validate_duplicates(hash_table)

        if self.monitor:
            self.monitor.update_progress("validate", status="Completed", processed=100, total=100, current="Validation finished")

    def compute_file_hash(self, file_path: str) -> tuple[str, str]:
        """Compute the xxhash of a single file."""
        try:
            thread_id = threading.current_thread().name
            log.debug(f"Validate - Thread {thread_id} hashing file {file_path}")
            file_path = Path(file_path)
            file_hash = xxhash.xxh64(file_path.open('rb').read()).hexdigest()
            return str(file_path), file_hash
        except Exception as e:
            log.error(f"Validate - Failed to hash file {file_path}: {e}")
            return str(file_path), None

    def load_media(self):
        """Load precomputed hashes from media_index.json."""
        media_hashes = {}
        index_file = os.path.join(self.datadir, "media_index.json")
        if not os.path.isfile(index_file):
            log.error(f"Validate - Index file {index_file} does not exist, falling back to rehashing")
            return media_hashes

        try:
            with open(index_file, 'r') as f:
                array = json.load(f)

            total_files = sum(1 for key in array if key != 'du')
            processed_files = 0

            if self.monitor:
                self.monitor.update_progress("validate", status="Running", processed=0, total=total_files, current="Loading media files")

            for key, value in array.items():
                if key != 'du':
                    media_hashes[value['filehash']] = value['path']
                    processed_files += 1
                    if self.monitor and total_files > 0:
                        self.monitor.update_progress("validate", status="Running", processed=processed_files, total=total_files, current=f"Loaded media file: {value['path']}")
            log.info(f"Validate - Loaded {len(media_hashes)} media files from {index_file}")
        except Exception as e:
            log.error(f"Validate - Failed to load media index file {index_file}: {e}")

        return media_hashes

    def load_archive(self):
        """Load precomputed hashes from .mediastruct files in the archive directory."""
        archive_hashes = {}
        total_files = sum(1 for _ in glob.glob(f"{self.archive_dir}/*"))
        processed_files = 0

        if self.monitor:
            self.monitor.update_progress("validate", status="Running", processed=0, total=total_files, current="Loading archive files")

        for archive_volume in glob.glob(f"{self.archive_dir}/*"):
            metadata_file = os.path.join(archive_volume, ".mediastruct")
            if not os.path.isfile(metadata_file):
                log.warning(f"Validate - No .mediastruct file found in {archive_volume}")
                continue

            log.debug(f"Validate - Reading metadata file: {metadata_file}")
            try:
                with open(metadata_file, 'r') as f:
                    metadata = yaml.safe_load(f)
                for relative_path, file_hash in metadata["files"].items():
                    file_path = os.path.join(archive_volume, relative_path)
                    archive_hashes[file_hash] = file_path
                processed_files += 1
                if self.monitor and total_files > 0:
                    self.monitor.update_progress("validate", status="Running", processed=processed_files, total=total_files, current=f"Loaded archive volume: {archive_volume}")
            except Exception as e:
                log.error(f"Validate - Failed to read metadata file {metadata_file}: {e}")

        log.info(f"Validate - Loaded {len(archive_hashes)} archive files from {self.archive_dir}")
        return archive_hashes

    def build_hash_table(self, media_hashes, archive_hashes):
        """Build a hash table from media and archive hashes for quick lookup."""
        hash_table = {**media_hashes, **archive_hashes}
        log.info(f"Validate - Built hash table with {len(hash_table)} unique hashes")
        return hash_table

    def process_duplicate(self, file_path: str, file_hash: str, hash_table: dict) -> tuple[str, bool]:
        """Compare a duplicate's hash against the hash table and move it if matched."""
        try:
            thread_id = threading.current_thread().name
            log.debug(f"Validate - Thread {thread_id} processing duplicate {file_path}")
            if file_hash in hash_table:
                original_path = hash_table[file_hash]
                source = "Media" if self.media_dir in original_path else "Archive"
                log.info(f"Validate - Duplicate {file_path} found in {source} {original_path}")
                # Move the duplicate to the validated directory
                filename = os.path.basename(file_path)
                with self.move_lock:  # Ensure thread-safe file moving
                    dest_path = os.path.join(self.validated_dir, filename)
                    if os.path.isfile(dest_path):
                        log.info(f"Validate - Destination path already exists, renaming: {dest_path}")
                        ext = os.path.splitext(filename)[1][1:]
                        newfile = os.path.splitext(filename)[0]
                        millis = int(round(time.time() * 1000))
                        newfilename = f"{newfile}.{millis}.{ext}"
                        dest_path = os.path.join(self.validated_dir, newfilename)
                    shutil.move(file_path, dest_path)
                    log.info(f"Validate - Moved {file_path} to {dest_path}")
                return file_path, True
            else:
                log.debug(f"Validate - No match found for duplicate {file_path} with hash {file_hash}")
                return file_path, False
        except Exception as e:
            log.error(f"Validate - Failed to process duplicate {file_path}: {e}")
            return file_path, False

    def validate_duplicates(self, hash_table):
        """Validate duplicates by computing their hashes and comparing against the hash table."""
        if not os.path.isdir(self.duplicates_dir):
            log.error(f"Validate - Duplicates directory {self.duplicates_dir} does not exist")
            return

        # List all files in the duplicates directory
        duplicate_files = []
        for root, _, files in walk(self.duplicates_dir):
            for filename in files:
                if filename.startswith("._"):  # Skip macOS metadata files
                    log.debug(f"Validate - Skipping file {filename} in {root}")
                    continue
                file_path = os.path.join(root, filename)
                duplicate_files.append(file_path)

        total_files = len(duplicate_files)
        processed_files = 0

        if self.monitor:
            self.monitor.update_progress("validate", status="Running", processed=0, total=total_files, current="Validating duplicates")

        # Step 1: Compute hashes for duplicate files using multi-threading
        duplicate_hashes = {}
        with ThreadPoolExecutor(max_workers=self.MAX_THREADS) as executor:
            futures = {executor.submit(self.compute_file_hash, file_path): file_path for file_path in duplicate_files}
            for future in futures:
                file_path = futures[future]
                try:
                    _, file_hash = future.result()
                    if file_hash:
                        duplicate_hashes[file_path] = file_hash
                        log.debug(f"Validate - Computed hash for {file_path}: {file_hash}")
                    else:
                        log.warning(f"Validate - No hash generated for file {file_path}")
                except Exception as e:
                    log.error(f"Validate - Failed to process file {file_path}: {e}")
                processed_files += 1
                if self.monitor and total_files > 0:
                    self.monitor.update_progress("validate", status="Running", processed=processed_files, total=total_files, current=f"Hashed duplicate: {file_path}")

        # Step 2: Compare hashes and move files using multi-threading
        moved_files = 0
        processed_files = 0
        total_files = len(duplicate_hashes)

        if self.monitor:
            self.monitor.update_progress("validate", status="Running", processed=0, total=total_files, current="Processing duplicates")

        with ThreadPoolExecutor(max_workers=self.MAX_THREADS) as executor:
            futures = {executor.submit(self.process_duplicate, file_path, file_hash, hash_table): file_path for file_path, file_hash in duplicate_hashes.items()}
            for future in futures:
                file_path = futures[future]
                try:
                    _, moved = future.result()
                    if moved:
                        moved_files += 1
                except Exception as e:
                    log.error(f"Validate - Failed to process duplicate {file_path}: {e}")
                processed_files += 1
                if self.monitor and total_files > 0:
                    self.monitor.update_progress("validate", status="Running", processed=processed_files, total=total_files, current=f"Processed duplicate: {file_path}")

        log.info(f"Validate - Processed {processed_files} duplicates, moved {moved_files} to {self.validated_dir}")
