import os
import sys
import logging
import json
import shutil
import time
import timeout_decorator
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

log = logging.getLogger(__name__)
print("Dedupe - Initializing logging")
log.info('Dedupe - Launching the Dedupe Class')

class dedupe:
    def __init__(self, data_files, duplicates_dir, archive_dir, ingest_dir, monitor=None):
        self.monitor = monitor
        self.duplicates_dir = duplicates_dir
        self.archive_dir = archive_dir
        self.ingest_dir = ingest_dir
        self.max_threads = os.cpu_count() or 4
        self.max_processes = os.cpu_count() or 4
        self._log_progress(f"Initialized with data_files: {data_files}, duplicates_dir: {duplicates_dir}, archive_dir: {archive_dir}, ingest_dir: {ingest_dir}")
        
        if self.monitor:
            self.monitor.update_progress("dedupe", status="Starting", processed=0, total=0, current="Initializing deduplication")
        
        combined_dataset = self.combine_array(data_files)
        self.dups(combined_dataset)
        
        if self.monitor:
            self.monitor.update_progress("dedupe", status="Completed", processed=100, total=100, current="Deduplication finished")
        self._log_progress("Exiting __init__")

    @timeout_decorator.timeout(300, timeout_exception=TimeoutError)
    def _load_json_file(self, file_path):
        """Load a JSON file with a timeout."""
        self._log_progress(f"Attempting to load file: {file_path}")
        with open(file_path, 'r') as f:
            array = json.load(f)
        self._log_progress(f"Successfully loaded file: {file_path} with {len(array) - 1 if 'du' in array else len(array)} entries (excluding 'du')")
        return array

    def _log_progress(self, message, level="info"):
        """Log progress messages and print to console."""
        print(f"Dedupe - {message}")
        getattr(log, level)(message)

    def combine_array(self, data_files):
        """Combine multiple JSON hash index files into a single dataset."""
        self._log_progress("Entering combine_array")
        combined_array = {}
        total_files = len(data_files)

        self._log_progress(f"Total data files to process: {total_files}")
        if self.monitor:
            self.monitor.update_progress("dedupe", status="Running", processed=0, total=total_files, current="Combining datasets")

        for i, file_path in enumerate(data_files, 1):
            self._log_progress(f"Processing file: {file_path}")
            if not os.path.isfile(file_path):
                self._log_progress(f"File not found: {file_path}", "warning")
                continue
            try:
                start_time = time.time()
                array = self._load_json_file(file_path)
                combined_array.update(array)
                self._log_progress(f"Loaded file {file_path} in {time.time() - start_time:.2f} seconds")
            except TimeoutError:
                self._log_progress(f"Timeout while loading file {file_path}, skipping", "error")
            except Exception as e:
                self._log_progress(f"Failed to load file {file_path}: {e}", "error")
            if self.monitor:
                self.monitor.update_progress("dedupe", status="Running", processed=i, total=total_files, current=f"Combining dataset: {file_path}")

        self._log_progress(f"Combined array contains {len(combined_array) - 1 if 'du' in combined_array else len(combined_array)} entries (excluding 'du')")
        self._log_progress("Exiting combine_array")
        return combined_array

    def _identify_duplicates(self, chunk, archive_hashes, media_hashes, archive_dir_name, media_dir):
        """Identify duplicates in a chunk of files using precomputed hashes."""
        process_id = os.getpid()
        self._log_progress(f"Process {process_id} starting to identify duplicates for chunk of size {len(chunk)}")
        duplicates = []
        for file_id, file_hash, file_path in chunk:
            # Normalize paths for consistent comparison
            normalized_path = Path(file_path).as_posix().lower()
            self._log_progress(f"Process {process_id} processing file: {normalized_path}")
            if archive_dir_name.lower() in normalized_path:
                self._log_progress(f"Process {process_id} skipping archive file: {normalized_path}")
                continue
            if file_hash in archive_hashes:
                duplicates.append((file_id, file_hash, file_path))
                self._log_progress(f"Process {process_id} found duplicate against archive: {normalized_path}")
                continue
            if media_dir.lower() in normalized_path:
                media_hashes.setdefault(file_hash, []).append((file_id, file_hash, file_path))
                self._log_progress(f"Process {process_id} processed media file: {normalized_path}, using precomputed hash: {file_hash}")
            else:
                self._log_progress(f"Process {process_id} keeping non-media, non-archive file (e.g., ingest): {normalized_path}")
        self._log_progress(f"Process {process_id} identified {len(duplicates)} duplicates in chunk")
        return duplicates, media_hashes

    def _move_file(self, file_entry, array, archive_dir_name):
        """Move a file to the duplicates directory."""
        file_id, _, file_path = file_entry
        from_path = array[file_id]['path']
        if not os.path.isfile(from_path):
            self._log_progress(f"File not found, cannot move: {from_path}", "warning")
            return
        filename = os.path.basename(from_path)
        dest_path = os.path.join(self.duplicates_dir, filename)
        if os.path.isfile(dest_path):
            ext = os.path.splitext(from_path)[1][1:]
            newfile = os.path.splitext(filename)[0]
            millis = int(round(time.time() * 1000))
            newfilename = f"{newfile}.{millis}.{ext}"
            dest_path = os.path.join(self.duplicates_dir, newfilename)
            self._log_progress(f"Destination path already exists, renamed to: {dest_path}")

        if archive_dir_name.lower() not in Path(from_path).as_posix().lower():
            self._log_progress(f"Moving duplicate to duplicates directory: {from_path} -> {dest_path}")
            shutil.move(from_path, dest_path)
        else:
            self._log_progress(f"File {from_path} is in archive directory and will not be moved (safety check)", "warning")

    def dups(self, array):
        """Deduplicate files using precomputed hashes."""
        self._log_progress("Entering dups")
        
        # Step 1: Build list of all files and archive hashes
        self._log_progress("Building list of all files and archive hashes")
        all_files = []
        archive_hashes = set()
        archive_dir_name = Path(self.archive_dir).name
        media_dir = "/data/media"
        total_entries = len(array)

        if self.monitor:
            self.monitor.update_progress("dedupe", status="Running", processed=0, total=total_entries, current="Building file lists")

        for i, (d, data) in enumerate(array.items(), 1):
            if d == 'du':
                continue
            file_path = data['path']
            if not os.path.isfile(file_path):
                self._log_progress(f"File {file_path} not found on disk, skipping", "warning")
                continue
            file_entry = (d, data['filehash'], file_path)
            all_files.append(file_entry)
            if archive_dir_name.lower() in Path(file_path).as_posix().lower():
                archive_hashes.add(data['filehash'])
            if i % 10000 == 0:
                self._log_progress(f"Processed {i}/{total_entries} entries ({(i/total_entries)*100:.1f}%)")
            if self.monitor:
                self.monitor.update_progress("dedupe", status="Running", processed=i, total=total_entries, current=f"Processing entry: {d}")

        self._log_progress(f"Total files after filtering: {len(all_files)}, archive hashes: {len(archive_hashes)}")

        # Step 2: Identify duplicates using multi-processing
        self._log_progress("Identifying duplicates using multi-processing")
        chunk_size = len(all_files) // self.max_processes + 1
        chunks = [all_files[i:i + chunk_size] for i in range(0, len(all_files), chunk_size)]
        self._log_progress(f"Split all_files into {len(chunks)} chunks of size {chunk_size} for parallel processing")

        to_delete = []
        media_hashes_per_chunk = [{} for _ in range(len(chunks))]
        media_files_count = 0  # Track number of media files loaded
        with ProcessPoolExecutor(max_workers=self.max_processes) as executor:
            futures = [executor.submit(self._identify_duplicates, chunk, archive_hashes, media_hashes_per_chunk[i], archive_dir_name, media_dir) for i, chunk in enumerate(chunks)]
            for i, future in enumerate(futures):
                chunk_duplicates, chunk_media_hashes = future.result()
                to_delete.extend(chunk_duplicates)
                media_hashes_per_chunk[i] = chunk_media_hashes

        # Combine media hashes from all chunks
        media_hashes = {}
        for chunk_hashes in media_hashes_per_chunk:
            for file_hash, entries in chunk_hashes.items():
                media_hashes.setdefault(file_hash, []).extend(entries)
                media_files_count += len(entries)
        self._log_progress(f"Loaded {media_files_count} media files into media_hashes")
        self._log_progress(f"Identified {len(to_delete)} duplicates against archive (including ingest files)")

        # Step 3: Deduplicate within media directory
        self._log_progress("Deduplicating within media directory")
        media_to_keep = []
        for file_hash, entries in media_hashes.items():
            if len(entries) <= 1:
                media_to_keep.extend(entry[0] for entry in entries)
                self._log_progress(f"Keeping media file (unique hash {file_hash}): {entries[0][2]}")
                continue
            entries.sort(key=lambda x: x[2])  # Sort by filepath
            media_to_keep.append(entries[0][0])
            self._log_progress(f"Keeping media file (first instance of hash {file_hash}): {entries[0][2]}")
            to_delete.extend(entries[1:])
            for entry in entries[1:]:
                self._log_progress(f"Media duplicate will be moved: {entry[2]} (hash: {file_hash})")

        self._log_progress(f"Total duplicates after media deduplication: {len(to_delete)}")

        # Step 4: Move duplicates using multi-threading
        total_to_delete = len(to_delete)
        self._log_progress(f"Moving {total_to_delete} duplicate files")
        if self.monitor:
            self.monitor.update_progress("dedupe", status="Running", processed=0, total=total_to_delete, current="Moving duplicates")

        with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            futures = [executor.submit(self._move_file, entry, array, archive_dir_name) for entry in to_delete]
            for i, future in enumerate(futures, 1):
                future.result()
                if i % 100 == 0:
                    self._log_progress(f"Moved {i}/{total_to_delete} duplicates ({(i/total_to_delete)*100:.1f}%)")
                if self.monitor:
                    self.monitor.update_progress("dedupe", status="Running", processed=i, total=total_to_delete, current=f"Moved {i}/{total_to_delete} duplicates")

        # Log summary statistics
        total_files = len(all_files)
        archive_files = len(archive_hashes)
        non_archive_files = total_files - archive_files
        kept_non_archive = len(media_to_keep)
        moved_files = total_to_delete

        self._log_progress(f"Summary: Total files processed: {total_files}")
        self._log_progress(f"{archive_files} archive files were kept (not moved)")
        self._log_progress(f"{kept_non_archive} non-archive files were kept (first instance of hash)")
        self._log_progress(f"{moved_files} files were moved to duplicates directory")
        self._log_progress("Exiting dups")
