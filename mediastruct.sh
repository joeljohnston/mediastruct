#!/bin/bash
rsync -av 10.99.0.11:/data/data/media/ingest/ /data/media/ingest/
python3 mediastruct ingest
python3 mediastruct crawl 
python3 mediastruct dedupe
python3 mediastruct validate 
rsync -av --delete --exclude validated /data/ 10.99.0.11:/data/data/
