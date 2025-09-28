from setuptools import setup, find_packages
import sys
import os
from pathlib import Path

# Prepare data files for installation
config_path = "mediastruct/conf/config.ini"
data_files = []
if os.path.isfile(config_path):
    data_files.append(('/etc/mediastruct', [config_path]))
    print(f"Config file {config_path} will be installed to /etc/mediastruct/config.ini")
else:
    print(f"Warning: Config file {config_path} not found. You must create /etc/mediastruct/config.ini manually after installation with the following content:")
    print("""
[Paths]
logdir = /data/logs
datadir = /opt/mediastruct/data
ingest_dir = /data/media/ingest
media_dir = /data/media
archive_dir = /data/archive
duplicates_dir = /data/media/duplicates
validated_dir = /data/media/validated
    """)

# Define package metadata
setup(
    name="mediastruct",
    version="0.1.0",
    description="A tool for media deduplication and archiving",
    author="Your Name",
    author_email="your.email@example.com",
    packages=find_packages(),
    include_package_data=True,
    package_data={
        'mediastruct': ['conf/*.ini'],
    },
    data_files=data_files,
    entry_points={
        'console_scripts': [
            'mediastruct = mediastruct.__main__:main',
        ],
    },
    install_requires=[
        'xxhash>=3.5.0',
        'pyyaml>=6.0.2',
        'timeout-decorator>=0.5.0',
        'psutil>=6.0.0',
    ],
    python_requires='>=3.6',
)
