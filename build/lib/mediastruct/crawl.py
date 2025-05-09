"""Find duplicate files inside a directory tree."""
import os
import re
import logging
import xxhash
import json
import yaml
import uuid
import time
import datetime
from datetime import timedelta
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
from mediastruct.utils import *
from os import walk, stat
from os.path import join as joinpath
import psutil

# Setup logging from the parent class
log = logging.getLogger(__name__)
log.info("Crawl - Loaded crawl.py module")

# Function to hash a file in chunks (must be defined at the module level for ProcessPoolExecutor)
def hash_file(file_path: str) -> tuple[str, str]:
    """Hash a single file using xxhash, reading in chunks to reduce memory usage."""
    try:
        file_path = Path(file_path)
        hasher = xxhash.xxh64()
        with file_path.open('rb') as f:
            # Read in 1 MB chunks to limit memory usage
            chunk_size = 1024 * 1024  # 1 MB
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                hasher.update(chunk)
        file_hash = hasher.hexdigest()
        return str(file_path), file_hash
    except Exception as e:
        log.error(f"Crawl - Failed to hash file {file_path}: {e}")
        return str(file_path), None

class crawl:
    """Iterate a dir tree and build a sum index with memory usage capping."""
    METADATA_FILE = ".mediastruct"
    MAX_AGE_DAYS = 120
    MEMORY_LIMIT_PERCENT = 0.8  # Use 80% of total system memory
    BASE_MAX_PROCESSES = os.cpu_count() or 4  # Base number of processes
    BATCH_SIZE = 1000  # Process files in batches of 1000 to limit memory usage

    def __init__(self, force, rootdir, datadir, monitor=None):
        self.monitor = monitor
        self.force = force
        self.datadir = datadir
        self.rootdir = rootdir
        # Get total system memory
        self.total_memory = psutil.virtual_memory().total
        self.memory_limit = self.total_memory * self.MEMORY_LIMIT_PERCENT
        log.info(f"Crawl - Total system memory: {self.total_memory / (1024**3):.2f} GB, memory limit (80%): {self.memory_limit / (1024**3):.2f} GB")
        dirname = re.split(r"\/", rootdir)
        dirname_len = len(dirname) - 1
        print('dirname_len: ', dirname_len)
        log.info("Crawl - Crawling %s" % (rootdir))
        if os.path.isdir(rootdir):
            log.info('Crawl - Indexing %s' % (rootdir))
            index = self.index_sum()

    def _is_metadata_current(self, metadata_path: str) -> bool:
        """Check if the .mediastruct metadata file exists and is less than 120 days old."""
        metadata_path = Path(metadata_path)
        if not metadata_path.exists():
            log.debug(f"Crawl - Metadata file {metadata_path} does not exist")
            return False
        try:
            with metadata_path.open("r") as f:
                metadata = yaml.safe_load(f)
            if not metadata or 'timestamp' not in metadata:
                log.error(f"Crawl - Metadata file {metadata_path} is missing 'timestamp' key")
                return False
            timestamp = datetime.datetime.fromisoformat(metadata["timestamp"])
            age = datetime.datetime.now() - timestamp
            is_current = age <= timedelta(days=self.MAX_AGE_DAYS)
            log.debug(f"Crawl - Metadata file {metadata_path} is {'current' if is_current else 'outdated'} (age: {age}, timestamp: {timestamp})")
            return is_current
        except (yaml.YAMLError, KeyError, ValueError) as e:
            log.error(f"Crawl - Failed to read metadata file {metadata_path}: {e}")
            return False

    def _is_target_subdirectory(self, path_str: str) -> bool:
        """Determine if a directory is a target subdirectory to process (one level down from parent)."""
        # Skip duplicates, validated, and ingest directories
        if any(path_str.startswith(prefix) for prefix in ["/data/media/duplicates", "/data/media/validated", "/data/media/ingest"]):
            log.debug(f"Crawl - Skipping directory (excluded): {path_str}")
            return False
        # Skip parent directories
        if path_str in ["/data/archive", "/data/media/media"]:
            log.debug(f"Crawl - Skipping parent directory: {path_str}")
            return False
        # For /data/archive, match /data/archive/\d+ (e.g., /data/archive/01)
        if path_str.startswith("/data/archive"):
            match = bool(re.match(r"^/data/archive/\d+$", path_str))
            log.debug(f"Crawl - Directory {path_str} {'is' if match else 'is not'} a target archive subdirectory")
            return match
        # For /data/media/media, match /data/media/media/\d{4} (e.g., /data/media/2024)
        if path_str.startswith("/data/media/media"):
            match = bool(re.match(r"^/data/media/media/\d{4}$", path_str))
            log.debug(f"Crawl - Directory {path_str} {'is' if match else 'is not'} a target media subdirectory")
            return match
        log.debug(f"Crawl - Skipping directory not under /data/archive or /data/media/media: {path_str}")
        return False

    def _should_force_rehash(self, path_str: str) -> bool:
        """Determine if rehashing should be forced for a directory."""
        # Always force rehash for /data/media/media subdirectories
        if path_str.startswith("/data/media/media"):
            log.debug(f"Crawl - Forcing rehash for media directory: {path_str}")
            return True
        # Respect force flag for other directories (e.g., /data/archive subdirectories)
        return self.force

    def _estimate_file_size(self, file_path: str) -> int:
        """Estimate the file size for memory usage calculations."""
        try:
            return os.path.getsize(file_path)
        except (OSError, FileNotFoundError):
            log.warning(f"Crawl - Could not get size for file {file_path}, assuming 1 MB")
            return 1024 * 1024  # Assume 1 MB if size cannot be determined

    def _calculate_max_workers(self, file_paths: list) -> int:
        """Calculate the maximum number of workers based on memory constraints."""
        # Estimate memory usage per file (average file size)
        total_size = 0
        sample_size = min(100, len(file_paths))  # Sample up to 100 files for estimation
        if sample_size > 0:
            sampled_paths = file_paths[:sample_size]
            total_size = sum(self._estimate_file_size(file_path) for file_path, _ in sampled_paths)
            avg_file_size = total_size / sample_size
        else:
            avg_file_size = 1024 * 1024  # Assume 1 MB if no files

        # Calculate how many files can be processed concurrently within memory limit
        # Add 2x buffer to account for Python overhead
        memory_per_file = avg_file_size * 2
        max_concurrent_files = int(self.memory_limit / memory_per_file)
        max_concurrent_files = max(1, min(max_concurrent_files, self.BASE_MAX_PROCESSES))
        log.debug(f"Crawl - Estimated avg file size: {avg_file_size / (1024**2):.2f} MB, max concurrent files: {max_concurrent_files}")
        return max_concurrent_files

    def _create_or_update_metadata(self, directory: str) -> dict:
        """Create or update the .mediastruct metadata file in the given directory."""
        directory = Path(directory)
        metadata_path = directory / self.METADATA_FILE
        metadata = {"timestamp": datetime.datetime.now().isoformat(), "files": {}}

        # Check if we should force rehashing
        force_rehash = self._should_force_rehash(str(directory.as_posix()))

        # If metadata exists and is current, return it unless force_rehash is True
        if not force_rehash and self._is_metadata_current(metadata_path):
            log.debug(f"Crawl - Using existing metadata file: {metadata_path}")
            with metadata_path.open("r") as f:
                loaded_metadata = yaml.safe_load(f)
                log.debug(f"Crawl - Loaded metadata with {len(loaded_metadata['files'])} file entries")
                return loaded_metadata

        # Walk the directory to build a tiered index
        total_files = sum(len(files) for _, _, files in walk(directory))
        processed_files = 0

        if self.monitor:
            self.monitor.update_progress("crawl", status="Running", processed=0, total=total_files, current=f"Hashing files in {directory}")
        log.debug(f"Crawl - Hashing {total_files} files in {directory}")

        file_paths = []
        for root, _, files in walk(directory):
            relative_root = Path(root).relative_to(directory)
            for filename in files:
                if filename == self.METADATA_FILE or filename.startswith("._"):
                    log.debug(f"Crawl - Skipping file {filename} in {root}")
                    continue
                file_path = Path(root) / filename
                relative_path = str(relative_root / filename)
                file_paths.append((str(file_path), relative_path))

        log.debug(f"Crawl - Found {len(file_paths)} files to hash in {directory}")

        # Calculate max workers based on memory constraints
        max_workers = self._calculate_max_workers(file_paths)
        log.debug(f"Crawl - Using {max_workers} workers for directory {directory}")

        # Process files in batches to limit memory usage
        for batch_start in range(0, len(file_paths), self.BATCH_SIZE):
            batch = file_paths[batch_start:batch_start + self.BATCH_SIZE]
            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(hash_file, file_path): (file_path, relative_path) for file_path, relative_path in batch}
                for future in futures:
                    file_path, relative_path = futures[future]
                    try:
                        _, file_hash = future.result()
                        if file_hash:
                            metadata["files"][relative_path] = file_hash
                            log.debug(f"Crawl - Hashed file {file_path} with hash {file_hash}")
                        else:
                            log.warning(f"Crawl - No hash generated for file {file_path}")
                        processed_files += 1
                        if processed_files % 100 == 0 and self.monitor and total_files > 0:
                            self.monitor.update_progress("crawl", status="Running", processed=processed_files, total=total_files, current=f"Hashing file: {relative_path}")
                        log.debug(f"Crawl - Hashed {processed_files}/{total_files} files in {directory} ({(processed_files/total_files)*100:.1f}%)")
                    except Exception as e:
                        log.error(f"Crawl - Failed to process file {file_path}: {e}")

        log.info(f"Crawl - Generated metadata with {len(metadata['files'])} file entries for {directory}")

        try:
            with metadata_path.open("w") as f:
                yaml.dump(metadata, f, default_flow_style=False)
            log.debug(f"Crawl - Wrote metadata file: {metadata_path}")
        except Exception as e:
            log.error(f"Crawl - Failed to write metadata file {metadata_path}: {e}")

        return metadata

    def _index_files(self, directory: str, sum_dict: dict, processed_file_paths: set) -> int:
        """Index files in a directory and add to sum_dict, returning the number of files processed."""
        metadata = self._create_or_update_metadata(directory)
        processed_files = 0
        for relative_path, file_hash in metadata["files"].items():
            file_path = os.path.join(directory, relative_path)
            if file_path in processed_file_paths:
                log.debug(f"Crawl - Skipping already processed file: {file_path}")
                continue
            processed_file_paths.add(file_path)
            fileid = str(uuid.uuid1())
            try:
                filesize = stat(file_path).st_size
                this_year = str(datetime.datetime.fromtimestamp(os.path.getmtime(file_path))).split('-')[0]
            except FileNotFoundError as e:
                log.error(f"Crawl - Skipping file {file_path}: {e}")
                continue
            index_line = {
                'filehash': file_hash,
                'path': file_path,
                'filesize': filesize,
                'year': this_year
            }
            sum_dict[fileid] = index_line
            processed_files += 1
        return processed_files

    def index_sum(self):
        """Index hash sum of all files in a directory tree and write to Json file"""
        log.info("Crawl - Executing index_sum method")
        rootdir = self.rootdir
        datadir = self.datadir
        # Isolate the name of the directory from our argument
        dirname = re.split(r"\/", rootdir)
        dirname_len = len(dirname) - 1
        sum_dict = {}
        processed_file_paths = set()  # Track processed files to avoid duplicates

        # Ensure the data directory exists
        datadir_path = Path(datadir)
        if not datadir_path.exists():
            try:
                datadir_path.mkdir(parents=True, exist_ok=True)
                log.info(f"Crawl - Created data directory: {datadir}")
            except Exception as e:
                log.error(f"Crawl - Failed to create data directory {datadir}: {e}")
                return sum_dict  # Return empty dict if directory creation fails

        # Walk the structure of the target dir tree
        total_files = sum(len(files) for path, dirs, files in walk(rootdir))
        processed_files = 0

        if self.monitor:
            self.monitor.update_progress("crawl", status="Running", processed=0, total=total_files, current=f"Indexing files in {rootdir}")
        log.debug(f"Crawl - Indexing {total_files} files in {rootdir}")

        # Process the root directory for /data/media/ingest
        if rootdir == "/data/media/ingest":
            log.debug(f"Crawl - Skipping ingest directory: {rootdir} (excluded)")
            # Ingest is not hashed, so we skip it but still need to create an index file
            sum_dict['du'] = utils.getFolderSize(self, rootdir)
        else:
            # Process subdirectories for /data/media/media and /data/archive
            for path, dirs, files in walk(rootdir):
                path_str = str(Path(path).as_posix())  # Normalize path
                log.debug(f"Crawl - Checking directory: {path_str}")
                # Process only target subdirectories (one level down)
                if not self._is_target_subdirectory(path_str):
                    continue
                log.debug(f"Crawl - Processing target subdirectory: {path_str}")
                processed_files += self._index_files(path_str, sum_dict, processed_file_paths)
                if self.monitor and total_files > 0:
                    self.monitor.update_progress("crawl", status="Running", processed=processed_files, total=total_files, current=f"Processed directory: {path_str}")
                log.debug(f"Crawl - Indexed {processed_files}/{total_files} files ({(processed_files/total_files)*100:.1f}%)")

        log.info(f"Crawl - Populated sum_dict with {len(sum_dict)} entries for {rootdir}")

        # Always write the index file, even if empty (e.g., for ingest)
        if 'du' not in sum_dict:
            sum_dict['du'] = utils.getFolderSize(self, rootdir)
        indexfilepath = f'{datadir}/{dirname[dirname_len]}_index.json'
        try:
            with open(indexfilepath, "w") as indexfile:
                jsonoutput = json.dumps(sum_dict)
                indexfile.write(jsonoutput)
            log.debug(f"Crawl - Wrote index file: {indexfilepath}")
        except Exception as e:
            log.error(f"Crawl - Failed to write index file {indexfilepath}: {e}")

        log.info("Crawl - Completed crawl of %s" % (rootdir))
        return sum_dict
