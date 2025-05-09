import os
import logging
import json
import xxhash
import shutil
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

log = logging.getLogger(__name__)

class validate:
    def __init__(self, data_files, duplicates_dir, archive_dir, ingest_dir, monitor=None):
        self.data_files = data_files
        self.duplicates_dir = duplicates_dir
        self.archive_dir = archive_dir
        self.ingest_dir = ingest_dir
        self.validated_dir = "/data/media/validated"  # Hardcoded for now, can be made configurable
        self.monitor = monitor
        self.max_threads = os.cpu_count() or 4
        self._log_progress(f"Initialized validate with data_files: {self.data_files}, duplicates_dir: {self.duplicates_dir}, archive_dir: {self.archive_dir}, ingest_dir: {self.ingest_dir}, validated_dir: {self.validated_dir}")
        self.validate_files()

    def _log_progress(self, message, level="info"):
        """Log progress messages and print to console."""
        print(f"Validate - {message}")
        getattr(log, level)(message)

    def _load_index_files(self):
        """Load hashes from index files for validation, creating a hash-to-file mapping."""
        hash_to_files = {}
        for index_file in self.data_files:
            if not os.path.isfile(index_file):
                self._log_progress(f"Index file {index_file} not found, skipping", "warning")
                continue
            try:
                with open(index_file, 'r') as f:
                    index_data = json.load(f)
                for file_id, data in index_data.items():
                    if file_id == 'du':
                        continue
                    file_path = data.get('path', '')
                    file_hash = data.get('filehash', '')
                    if file_path and file_hash:
                        hash_to_files.setdefault(file_hash, []).append(Path(file_path).as_posix())
                self._log_progress(f"Loaded {len(index_data)} entries from {index_file}")
            except Exception as e:
                self._log_progress(f"Failed to load index file {index_file}: {e}", "error")
        return hash_to_files

    def _hash_file(self, file_path):
        """Compute the xxhash of a file."""
        try:
            with open(file_path, 'rb') as f:
                hasher = xxhash.xxh64()
                chunk_size = 1024 * 1024  # 1 MB chunks
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    hasher.update(chunk)
                return hasher.hexdigest()
        except Exception as e:
            self._log_progress(f"Failed to hash file {file_path}: {e}", "error")
            return None

    def validate_files(self):
        """Validate files in the duplicates directory and move valid ones to validated_dir."""
        self._log_progress("Starting validation")

        # Ensure duplicates directory exists
        if not os.path.isdir(self.duplicates_dir):
            self._log_progress(f"Duplicates directory {self.duplicates_dir} does not exist, skipping validation", "warning")
            return

        # Ensure validated directory exists
        try:
            Path(self.validated_dir).mkdir(parents=True, exist_ok=True)
            self._log_progress(f"Ensured validated directory exists: {self.validated_dir}")
        except Exception as e:
            self._log_progress(f"Failed to create validated directory {self.validated_dir}: {e}", "error")
            return

        # Load hashes from index files
        hash_to_files = self._load_index_files()

        # Collect all files in the duplicates directory
        file_paths = []
        for root, _, files in os.walk(self.duplicates_dir):
            for filename in files:
                file_path = os.path.join(root, filename)
                if not os.path.isfile(file_path):
                    self._log_progress(f"Skipping non-file: {file_path}", "warning")
                    continue
                file_paths.append(file_path)

        total_files = len(file_paths)
        validated_files = 0
        failed_files = 0

        if total_files == 0:
            self._log_progress(f"No files found in {self.duplicates_dir}, skipping validation", "info")
            return

        if self.monitor:
            self.monitor.update_progress("validate", status="Running", processed=0, total=total_files, current=f"Validating files in {self.duplicates_dir}")

        # Validate files in parallel using ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            futures = {executor.submit(self._hash_file, file_path): file_path for file_path in file_paths}
            for future in as_completed(futures):
                file_path = futures[future]
                computed_hash = future.result()
                if computed_hash is None:
                    failed_files += 1
                    continue

                # Check if the computed hash exists in the index
                if computed_hash in hash_to_files:
                    # File is a valid duplicate if its hash matches an entry in the index
                    indexed_paths = hash_to_files[computed_hash]
                    self._log_progress(f"File {file_path} validated successfully (hash matches: {computed_hash}, original paths: {indexed_paths})")

                    # Move the validated file to validated_dir
                    filename = os.path.basename(file_path)
                    dest_path = os.path.join(self.validated_dir, filename)
                    if os.path.isfile(dest_path):
                        base, ext = os.path.splitext(filename)
                        millis = int(round(time.time() * 1000))
                        new_filename = f"{base}.{millis}{ext}"
                        dest_path = os.path.join(self.validated_dir, new_filename)
                        self._log_progress(f"Destination path {dest_path} already exists, renamed to: {new_filename}")

                    try:
                        shutil.move(file_path, dest_path)
                        self._log_progress(f"Moved validated file: {file_path} -> {dest_path}")
                    except Exception as e:
                        self._log_progress(f"Failed to move validated file {file_path} to {dest_path}: {e}", "error")
                        failed_files += 1
                        continue

                    validated_files += 1
                else:
                    self._log_progress(f"File {file_path} hash not found in index files (computed: {computed_hash})", "error")
                    failed_files += 1

                if self.monitor and total_files > 0:
                    self.monitor.update_progress("validate", status="Running", processed=validated_files + failed_files, total=total_files, current=f"Processed file: {os.path.basename(file_path)}")
                self._log_progress(f"Processed {validated_files + failed_files}/{total_files} files ({((validated_files + failed_files)/total_files)*100:.1f}%)")

        self._log_progress(f"Validation completed: {validated_files} files validated and moved to {self.validated_dir}, {failed_files} files failed and remain in {self.duplicates_dir}")
