mediastruct
WARNING: This software is EXPERIMENTAL!!!!! Use at your own risk. Even though the application doesn't delete files, I'm not responsible for any data loss or data structure loss that you may incur while working with this software. Anything you do is on you.
I highly recommend using this project's sister project (Mediagen) to create a bogus dataset to work with, so that you can understand how this software works.
MediaStruct is a comprehensive suite of tools used for managing a media archive, designed to organize, deduplicate, and archive media files efficiently while ensuring production-ready deployment and resource management.
My Use Case
I have a data archive workflow that consists of an ingest filesystem for all new files to be deposited. The ingest/"drop" directory is scanned, and files are moved/renamed into the target date structure (e.g., /data/media/YYYY). As years go by, I take each year's directory structure and move it to an /archive filesystem, which is carved up into volumes that fit onto optical media (Blu-ray). Over time, backing up from various devices produces duplicates in the /data/media directory that I don't want. I need to compare what's in multiple directory structures to identify what still needs to be archived and what can be moved out of /data/media (if it has already been archived). This requires ensuring that duplicates are never moved out of /archive—it’s like the "black-flag motel."
How It Works
Ingest

Takes the ingest directory as an argument (configured in /etc/mediastruct/config.ini).
Renames files with a datetime hash, preserving the extension.
Organizes files by date into the target directory (e.g., /data/media/YYYY).

Crawl

Takes multiple directory structures as arguments (configured in /etc/mediastruct/config.ini).
Detects the machine's number of cores and dynamically adjusts the number of processes to cap memory usage at 80% of total system memory.
Crawls the directory trees one level deep (e.g., /data/archive/01, /data/media/2024), creating a master index file with three fields: path, hash, and size.
For /data/archive/NN directories, reindexes only if the .mediastruct file is over 120 days old (unless --force is specified).
For /data/media/YYYY directories, always reindexes.
Skips /data/media/duplicates, /data/media/validated, and /data/media/ingest directories.

DeDupe

Loads the master index files (ingest_index.json, media_index.json, archive_index.json) containing paths and precomputed file hashes.
Compares hashes to identify duplicates.
Creates a list of duplicates.
Moves duplicates to the duplicates directory (e.g., /data/media/duplicates), ensuring archive files are never moved.

Archive

Creates an archive target directory and calculates the target size of contents based on index file sources.
Moves files into the archive directory structure, keeping the date folder structure intact across multiple optical archive target directories.
Marks unburned directories accordingly.
Creates archive_index.json.


Installation and Setup
Prerequisites

Python 3.6 or higher
pip for installing dependencies
Root access (for installing config files to /etc/mediastruct)

Step 1: Clone the Repository
git clone <repository-url>
cd mediastruct

Step 2: Install the Package
The installation process will:

Install dependencies listed in requirements.txt.
Install the mediastruct binary.
Copy the default config.ini to /etc/mediastruct/config.ini.

Run the following command with root privileges:
sudo pip install .

Step 3: Verify Config File
After installation, verify that the configuration file is in place:
cat /etc/mediastruct/config.ini

It should contain:
[Paths]
logdir = /data/logs
datadir = /opt/mediastruct/data
ingest_dir = /data/media/ingest
media_dir = /data/media
archive_dir = /data/archive
duplicates_dir = /data/media/duplicates
validated_dir = /data/media/validated

You can edit /etc/mediastruct/config.ini to customize paths for your environment. Ensure the directories exist and are writable by the user running the script.
Step 4: Ensure Log Directory Exists
The script logs to /data/logs/mediastruct.log. Ensure the directory exists and is writable:
sudo mkdir -p /data/logs
sudo chown $(whoami) /data/logs

Execution
Crawl
To index your media and archive directories:
mediastruct crawl


Use the --force flag to reindex all directories, even if their .mediastruct files are current:mediastruct crawl --force


The script will:
Skip /data/media/ingest, /data/media/duplicates, and /data/media/validated.
Index /data/archive/NN directories (e.g., /data/archive/01), rehashing only if .mediastruct is over 120 days old.
Index /data/media/YYYY directories (e.g., /data/media/2024), always rehashing.
Generate index files in /opt/mediastruct/data (e.g., media_index.json, archive_index.json).



Dedupe
To deduplicate files:
mediastruct dedupe


The script will:
Load precomputed hashes from index files.
Identify duplicates by comparing hashes.
Move duplicate media files to /data/media/duplicates, ensuring archive files are never moved.



Log Review
Logs are written to /data/logs/mediastruct.log with a 500 MB rotation limit. To review logs:
cat /data/logs/mediastruct.log


Look for key messages:
Crawl: Crawl - Processing target subdirectory: /data/media/2024
Dedupe: Dedupe - Keeping media file (first instance of hash <hash>): /data/media/2024/wonderunit.png
Errors: Any ERROR messages indicating issues (e.g., file access errors).



Packaging for Distribution
To package the project for distribution:
python setup.py sdist


This creates a source distribution in the dist/ directory (e.g., mediastruct-0.1.0.tar.gz).
To install the package on another system:sudo pip install dist/mediastruct-0.1.0.tar.gz



Notes

The crawl phase caps memory usage at 80% of total system memory to prevent crashes on systems with limited resources.
The dedupe phase relies entirely on precomputed hashes from the crawl phase and does not perform any hashing.
Ensure all directories specified in /etc/mediastruct/config.ini exist and are accessible before running the script.


