import os
import time
import sys
import logging
import argparse
import configparser
import logging.handlers
from pathlib import Path
from mediastruct import crawl, dedupe, ingest, validate

# Setup logging
log = logging.getLogger(__name__)

def setup_logging(logdir):
    """Setup logging with rotation to the specified log directory."""
    log_file_path = os.path.join(logdir, "mediastruct.log")
    log_dir = os.path.dirname(log_file_path)

    # Ensure the log directory exists
    try:
        os.makedirs(log_dir, exist_ok=True)
    except Exception as e:
        print(f"Error creating log directory {log_dir}: {e}")
        log.error(f"Error creating log directory {log_dir}: {e}")
        sys.exit(1)

    # Configure logging with rotation
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.handlers.RotatingFileHandler(
                log_file_path, maxBytes=500*1024*1024, backupCount=5
            ),
            logging.StreamHandler()
        ]
    )
    log.debug(f"Logging configured to write to {log_file_path}")

class mediastruct:
    def __init__(self):
        print("Setting up sys.path")
        log.debug("Setting up sys.path")
        self.this_path = os.path.dirname(os.path.abspath(__file__))
        self.app_path = os.path.abspath(os.path.join(self.this_path, '..'))
        sys.path.append(self.app_path)

        print("Importing local classes")
        log.debug("Importing local classes")

        # Setup configuration
        config_path = "/etc/mediastruct/config.ini"
        self.config = configparser.ConfigParser()

        # Default configuration values
        self.config['Paths'] = {
            'logdir': '/data/logs',
            'datadir': '/opt/mediastruct/data',
            'ingest_dir': '/data/media/ingest',
            'media_dir': '/data/media',
            'archive_dir': '/data/archive',
            'duplicates_dir': '/data/media/duplicates',
            'validated_dir': '/data/media/validated',
        }

        # Try to read the config file from /etc/mediastruct/config.ini
        if os.path.isfile(config_path):
            try:
                self.config.read(config_path)
                print(f"Read configuration from {config_path}")
                log.debug(f"Read configuration from {config_path}")
            except Exception as e:
                print(f"Error reading config file {config_path}: {e}")
                log.error(f"Error reading config file {config_path}: {e}")
                print("Using default configuration values")
                log.info("Using default configuration values")
        else:
            print(f"Config file {config_path} not found, using default configuration:")
            log.debug(f"Config file {config_path} not found, using default configuration:")
            print("\n".join(f"{key} = {value}" for key, value in self.config['Paths'].items()))
            log.debug("\n".join(f"{key} = {value}" for key, value in self.config['Paths'].items()))

        # Extract paths from config
        self.logdir = self.config['Paths']['logdir']
        self.datadir = self.config['Paths']['datadir']
        self.ingestdir = self.config['Paths']['ingest_dir']
        self.workingdir = self.config['Paths']['media_dir']
        self.archivedir = self.config['Paths']['archive_dir']
        self.duplicatedir = self.config['Paths']['duplicates_dir']
        self.validateddir = self.config['Paths']['validated_dir']

        print(f"Set up paths: this_path={self.this_path}, app_path={self.app_path}")
        log.debug(f"Set up paths: this_path={self.this_path}, app_path={self.app_path}")

        # Setup logging with the specified log directory
        setup_logging(self.logdir)

        # Setup argument parser
        self.parser = argparse.ArgumentParser(description='MediaStruct')
        self.parser.add_argument('command', choices=['ingest', 'crawl', 'dedupe', 'archive', 'validate'], help='Command to execute')
        self.parser.add_argument('-f', '--force', action='store_true', help='Force reprocessing')
        self.parser.add_argument('-m', '--monitor', action='store_true', help='Enable monitoring')
        self.args = self.parser.parse_args()

        print(f"Command: {self.args.command}")
        log.info(f"Command: {self.args.command}")

        # Set monitor flag
        self.monitor = self.args.monitor if hasattr(self.args, 'monitor') else None

        # Execute the command
        log.debug(f"Executing command: {self.args.command}")
        getattr(self, self.args.command)()

    def ingest(self):
        """Execute the ingest command."""
        log.debug("Ingest command starting")
        ingest.ingest(source_dir=self.ingestdir, target_dir=self.workingdir, monitor=self.monitor)
        log.debug("Ingest command completed")

    def crawl(self):
        """Execute the crawl command."""
        log.debug("Crawl command starting")
        crawl.crawl(force=self.args.force, rootdir=self.ingestdir, datadir=self.datadir, monitor=self.monitor)
        crawl.crawl(force=self.args.force, rootdir=self.workingdir, datadir=self.datadir, monitor=self.monitor)
        crawl.crawl(force=self.args.force, rootdir=self.archivedir, datadir=self.datadir, monitor=self.monitor)
        log.debug("Crawl command completed")

    def dedupe(self):
        """Execute the dedupe command."""
        log.debug("Starting dedupe command")
        data_files = [
            os.path.join(self.datadir, 'ingest_index.json'),
            os.path.join(self.datadir, 'media_index.json'),
            os.path.join(self.datadir, 'archive_index.json')
        ]
        dedupe.dedupe(data_files, self.duplicatedir, self.archivedir, self.ingestdir, monitor=self.monitor)
        log.debug("Dedupe command completed")

    def archive(self):
        """Execute the archive command."""
        log.debug("Archive command starting")
        # Placeholder for archive functionality
        log.debug("Archive command completed")

    def validate(self):
        """Execute the validate command."""
        log.debug("Validate command starting")
        data_files = [
            os.path.join(self.datadir, 'ingest_index.json'),
            os.path.join(self.datadir, 'media_index.json'),
            os.path.join(self.datadir, 'archive_index.json')
        ]
        validate.validate(data_files, self.duplicatedir, self.archivedir, self.ingestdir, monitor=self.monitor)
        log.debug("Validate command completed")

def main():
    log.debug("Entering main function")
    mediastruct_instance = mediastruct()
    log.debug("Exiting main function")

if __name__ == "__main__":
    main()
