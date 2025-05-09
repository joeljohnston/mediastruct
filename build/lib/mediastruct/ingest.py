import os
import logging
import xxhash
import shutil
import time
from datetime import datetime
from pathlib import Path

log = logging.getLogger(__name__)

class ingest:
    def __init__(self, source_dir, target_dir, monitor=None):
        self.source_dir = source_dir
        self.target_dir = target_dir
        self.monitor = monitor
        self._log_progress(f"Initialized ingest with source_dir: {self.source_dir}, target_dir: {self.target_dir}")

        # Ensure source directory exists
        try:
            Path(self.source_dir).mkdir(parents=True, exist_ok=True)
            self._log_progress(f"Created source directory: {self.source_dir}")
        except Exception as e:
            self._log_progress(f"Failed to create source directory {self.source_dir}: {e}", "error")
            raise

        # Ensure target directory exists
        try:
            Path(self.target_dir).mkdir(parents=True, exist_ok=True)
            self._log_progress(f"Created target directory: {self.target_dir}")
        except Exception as e:
            self._log_progress(f"Failed to create target directory {self.target_dir}: {e}", "error")
            raise

        self.process_files()

    def _log_progress(self, message, level="info"):
        """Log progress messages and print to console."""
        print(f"Ingest - {message}")
        getattr(log, level)(message)

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

    def process_files(self):
        """Process files in the source directory, renaming and organizing by date."""
        self._log_progress("Starting file processing")
        total_files = sum(len(files) for _, _, files in os.walk(self.source_dir))
        processed_files = 0

        if self.monitor:
            self.monitor.update_progress("ingest", status="Running", processed=0, total=total_files, current=f"Processing files in {self.source_dir}")

        for root, _, files in os.walk(self.source_dir):
            for filename in files:
                source_path = os.path.join(root, filename)
                if not os.path.isfile(source_path):
                    self._log_progress(f"Skipping non-file: {source_path}", "warning")
                    continue

                # Compute hash
                file_hash = self._hash_file(source_path)
                if not file_hash:
                    self._log_progress(f"Skipping file due to hash failure: {source_path}", "warning")
                    continue

                # Get file modification time
                try:
                    mtime = os.path.getmtime(source_path)
                    date = datetime.fromtimestamp(mtime)
                    year = date.strftime("%Y")
                    month = date.strftime("%m")
                except Exception as e:
                    self._log_progress(f"Failed to get modification time for {source_path}: {e}", "error")
                    continue

                # Construct target directory
                target_subdir = os.path.join(self.target_dir, year, month)
                try:
                    Path(target_subdir).mkdir(parents=True, exist_ok=True)
                except Exception as e:
                    self._log_progress(f"Failed to create target directory {target_subdir}: {e}", "error")
                    continue

                # Construct new filename with datetime hash
                ext = os.path.splitext(filename)[1]
                new_filename = f"{date.strftime('%Y%m%d_%H%M%S')}_{file_hash}{ext}"
                target_path = os.path.join(target_subdir, new_filename)

                # Move the file
                try:
                    shutil.move(source_path, target_path)
                    self._log_progress(f"Moved file: {source_path} -> {target_path}")
                except Exception as e:
                    self._log_progress(f"Failed to move file {source_path} to {target_path}: {e}", "error")
                    continue

                processed_files += 1
                if self.monitor and total_files > 0:
                    self.monitor.update_progress("ingest", status="Running", processed=processed_files, total=total_files, current=f"Processed file: {filename}")
                self._log_progress(f"Processed {processed_files}/{total_files} files ({(processed_files/total_files)*100:.1f}%)")

        self._log_progress("File processing completed")
