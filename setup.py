from setuptools import setup, find_packages
import subprocess
import sys
import os
from pathlib import Path

# Ensure dependencies are installed from requirements.txt
def install_requirements():
    """Install dependencies from requirements.txt."""
    requirements_file = "requirements.txt"
    if os.path.isfile(requirements_file):
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", requirements_file])
            print("Successfully installed dependencies from requirements.txt")
        except subprocess.CalledProcessError as e:
            print(f"Error installing dependencies: {e}")
            sys.exit(1)
    else:
        print(f"requirements.txt not found at {requirements_file}, skipping dependency installation")

# Install requirements before proceeding with setup
install_requirements()

# Prepare data files for installation
config_path = "mediastruct/conf/config.ini"
data_files = []
if os.path.isfile(config_path):
    data_files.append(('/etc/mediastruct', [config_path]))
else:
    print(f"Warning: {config_path} not found. Config file will not be installed to /etc/mediastruct. Please create /etc/mediastruct/config.ini manually after installation.")

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
        'mediastruct': ['conf/config.ini'],
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
