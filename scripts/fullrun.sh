#!/bin/bash
cd ../
python mediastruct ingest
python mediastruct crawl
python mediastruct dedupe
