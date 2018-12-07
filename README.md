# mediastruct
WARNING: This software is EXPERIMENTAL!!!!! Use at your own risk.  Even though the application doesn't delete files, I'm not responsible for any data loss or data structure loss that you may incur while working with this software. Anything you do is on you. 

I highly recommend using this project's sister project (Mediagen) to create a bogus dataset to work with, so that you can understand how this software works.  

MediaStruct is a comprehensive suite of tools used for managing a media archive. 

My Use Case:
I have a data archive workflow that consists of an ingest filesystem for all new files to be deposited. The ingest/"drop" directory is scanned and files are moved/renamed into the target date structure (e.g. /data/photos) As years go by I take each years directory structure and move it off to an /Archive filesystem. That filesystem is carved up into volumes that will fit onto optical media (blu-ray.) As years go by backing up from various devices produces duplicates of files in the /data/photos directory that I don't want. I need to compare what's in a number of directory structures to see what still needs to be archived and what can be moved out of /data/photos (if it has already been archived.) This requires making sure that duplicates are never moved out of /Archive (its like the black-flag motel.)

Here's how it works:

Ingest
1. Takes ingest directory as argument
2. Renames files with datetime hash preserving extension
3. Organizes by date into target directory

Crawl
1. Takes n number of directory structures as arguments
2. Detects the machines's number of cores
3. Splits the crawl tree into appropriate number of threads
4. Crawls the trees and creates a master index file of 3 fields, path, hash, size.
5. Detects age of index files (if existing) and decides whethere or not to reindex (useful for a huge static master archive directory structure that doesn't change)

DeDupe
1. loads the master index file (that includes paths and file hashes)
2. compares hashes and finds duplicates
3. creates a list of duplicates
4. Moves the duplicates into the duplicates directory

Archive
1. Creates Archive target directory and calculates the target size of contents based on index file sources
2. Moves files into Archive directory structure, keeping date folder structure intact across multiple optical archive target directories
3. Marks unburned directories accordingly
4. Creates Archive index.json

----

How to Package: python setup.py sdist
