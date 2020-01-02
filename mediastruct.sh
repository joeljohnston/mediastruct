#!/bin/bash
python3 mediastruct ingest
python3 mediastruct crawl 
python3 mediastruct dedupe
python3 mediastruct validate 
