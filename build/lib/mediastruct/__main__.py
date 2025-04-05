import os
import sys
import configparser
import glob
import logging
import logging.config
import argparse

# Set up a temporary logger to capture early debug messages
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')
console.setFormatter(formatter)
log.addHandler(console)
log.debug("Starting mediastruct application")

# Add the parent directory to sys.path to ensure the mediastruct package is found
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
log.debug("Added parent directory to sys.path")

# Import local classes
log.debug("Importing local classes")
from mediastruct import ingest
from mediastruct import crawl
from mediastruct import dedupe
from mediastruct import archive
from mediastruct import validate
from mediastruct.monitor import ProgressMonitor
log.debug("Finished importing local classes")

# Config files - create with defaults if they don't exist
configfile_name = "conf/config.ini"
log.debug(f"Checking for config file: {configfile_name}")
if not os.path.isfile(configfile_name):
    log.debug("Config file not found, creating with defaults")
    cfgfile = open(configfile_name, 'w')
    appConfig = configparser.ConfigParser()
    appConfig.add_section('ingestdirs')
    appConfig.set('ingestdirs', 'ingestdir', '/data/ingest')
    appConfig.add_section('workingdirs')
    appConfig.set('workingdirs', 'workingdir', '/data/media')
    appConfig.add_section('archivedir')
    appConfig.set('archivedir', 'archivedir', '/data/archive')
    appConfig.add_section('archivemedia')
    appConfig.set('archivemedia', 'mediasize', '24')
    appConfig.set('archivemedia', 'burnedtag', 'wr')
    appConfig.add_section('duplicates')
    appConfig.set('duplicates', 'duplicatedir', '/data/duplicates')
    appConfig.add_section('validated')
    appConfig.set('validated', 'validateddir', '/validated')
    appConfig.add_section('datadir')
    appConfig.set('datadir', 'jsondatadir', 'data')
    appConfig.set('datadir', 'logdir', '/opt/mediastruct/logs')
    appConfig.write(cfgfile)
    cfgfile.close()
    log.debug("Created config file with defaults")
else:
    log.debug("Config file found, reading configuration")
    config = configparser.ConfigParser()
    config.read('conf/config.ini')
    ingestdir = config['ingestdirs']['ingestdir']
    workingdir = config['workingdirs']['workingdir']
    archivedir = config['archivedir']['archivedir']
    mediasize = config['archivemedia']['mediasize']
    burnedtag = config['archivemedia']['burnedtag']
    duplicatedir = config['duplicates']['duplicatedir']
    validateddir = config['validated']['validateddir']
    jsondatadir = config['datadir']['jsondatadir']
    logdir = config['datadir']['logdir']
    log.debug(f"Read configuration: logdir={logdir}")

# Setup Paths
this_path = os.path.dirname(os.path.realpath(__file__))
one_up = os.path.dirname(os.path.realpath(__file__)) + '/../'
app_path = os.path.join(this_path, one_up)
config_path = app_path + 'conf/'
log.debug(f"Set up paths: this_path={this_path}, app_path={app_path}")

# Logging
log_path = '/opt/mediastruct/logs/mediastruct.log'  # Use absolute path
log.debug(f"Setting log file path: {log_path}")
# Ensure the log directory exists
log_dir = os.path.dirname(log_path)
try:
    os.makedirs(log_dir, exist_ok=True)
    log.debug(f"Created log directory: {log_dir}")
except Exception as e:
    log.error(f"Failed to create log directory {log_dir}: {e}")
    sys.exit(1)

# Configure logging with DEBUG level for both file and console
try:
    # Remove any existing handlers to avoid conflicts
    for handler in logging.getLogger('').handlers[:]:
        logging.getLogger('').removeHandler(handler)
    # Set up file handler
    file_handler = logging.FileHandler(log_path)
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s', datefmt='%Y-%m-%d %I:%M:%S %p %Z')
    file_handler.setFormatter(file_formatter)
    # Set up console handler
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    console_formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
    console.setFormatter(console_formatter)
    # Configure root logger
    logging.getLogger('').setLevel(logging.DEBUG)
    logging.getLogger('').addHandler(file_handler)
    logging.getLogger('').addHandler(console)
    log.debug("Logging configuration set up successfully")
except Exception as e:
    print(f"Failed to set up logging: {e}", file=sys.stderr)
    sys.exit(1)

class mediastruct_init:
    def __init__(self, enable_monitor=False):
        log.debug("Initializing mediastruct_init")
        self.monitor = None
        if enable_monitor:
            try:
                log.debug("Attempting to initialize ProgressMonitor")
                self.monitor = ProgressMonitor()
                self.monitor.start()
                log.debug("ProgressMonitor started successfully")
            except Exception as e:
                log.error(f"Failed to initialize ProgressMonitor: {e}")
                log.info("Falling back to logging-only mode")

        log.info("########################## Starting Medistruct ##########################")

        # argparse init
        log.debug("Setting up argument parser")
        parser = argparse.ArgumentParser(description='Manage media file structure for archiving.', usage='''mediastruct [-m] <command> [<args>]
        Commands:
        ingest -  Moves files from the ingest directory set in conf/config.ini to the working directory set in conf/config.ini in a date structure

        crawl -   Iterates through all configured directories (except duplicates) and creates a hash index json file in data/

        dedupe -  Combines all configured directory's json datasets and moves duplicates in the working directory or ingest into the duplicates directory

        archive - Uses the mediasize variable set in conf/config.ini to create sized volumes in the archive directory and moves files accordingly

        validate - Does the reverse of the above actions by rehashing and comparing each marked duplicate file to all files in all structures, moves matches to the validated directory

        daily -   Combines the above functions into a re-usable automated workflow for use with scheduled jobs

        Options:
        -m        Enable the monitoring interface (curses-based progress display)
        ''')
        parser.add_argument('-m', action='store_true', help='Enable the monitoring interface (curses-based progress display)')
        parser.add_argument('command', help='Subcommand to run')
        log.debug("Parsing command-line arguments")
        args = parser.parse_args()
        self.enable_monitor = args.m
        if not hasattr(self, args.command):
            print('Unrecognized command')
            parser.print_help()
            exit(1)

        # Re-initialize monitor based on -m flag
        if self.enable_monitor and self.monitor is None:
            try:
                log.debug("Attempting to initialize ProgressMonitor after -m flag")
                self.monitor = ProgressMonitor()
                self.monitor.start()
                log.debug("ProgressMonitor started successfully")
            except Exception as e:
                log.error(f"Failed to initialize ProgressMonitor: {e}")
                log.info("Falling back to logging-only mode")

        # Assess Command Argument
        log.info("Command: %s" % (args.command))
        try:
            log.debug(f"Executing command: {args.command}")
            getattr(self, args.command)()
            log.debug(f"Command {args.command} executed successfully")
        except Exception as e:
            log.error(f"Command {args.command} failed: {e}")
            raise
        finally:
            if self.monitor:
                log.debug("Stopping ProgressMonitor")
                self.monitor.stop()
                log.debug("ProgressMonitor stopped")

    def crawl(self):
        print("Crawling")
        log = logging.getLogger(__name__)
        log.debug("Starting crawl command")
        # the crawl function performs a hash index of all files in the target directories
        parser = argparse.ArgumentParser(description='Crawl the dirs and create a hash index')
        parser.add_argument('-f', '--force', action='store_true', default=False, help='forces indexing of all directories')
        parser.add_argument('-p', '--path', help='pass a directory to crawl')
        args = parser.parse_args(sys.argv[2:])
        # Crawl a provided directory
        if args.path:
            crawl.crawl(args.force, args.path, jsondatadir, monitor=self.monitor)
        else:
            ingestsum = crawl.crawl(args.force, ingestdir, jsondatadir, monitor=self.monitor)
            workingdirsum = crawl.crawl(args.force, workingdir, jsondatadir, monitor=self.monitor)
            archivedirsum = crawl.crawl(args.force, archivedir, jsondatadir, monitor=self.monitor)
        log.debug("Crawl command completed")

    def ingest(self):
        print("Ingesting Files")
        log = logging.getLogger(__name__)
        log.debug("Starting ingest command")
        # the ingest function sorts and moves files by date into the working/media directory
        ingest.ingest(ingestdir, workingdir, monitor=self.monitor)
        log.debug("Ingest command completed")

    def dedupe(self):
        print("Dedupping")
        log = logging.getLogger(__name__)
        log.debug("Starting dedupe command")
        # the dedupe function combines all hash indexes and analyzes the dataset for duplicates
        data_files = glob.glob(jsondatadir + '/*.json')
        # run the dedupe function
        dedupe.dedupe(data_files, duplicatedir, archivedir, monitor=self.monitor)
        log.debug("Dedupe command completed")

    def archive(self):
        print("Archiving")
        log = logging.getLogger(__name__)
        log.debug("Starting archive command")
        # the archive function pulls from the working/media directory and pools into sized volumes
        archive.archive(archivedir, jsondatadir, workingdir, mediasize, monitor=self.monitor)
        log.debug("Archive command completed")

    def validate(self):
        print("Validating - This can take awhile")
        log = logging.getLogger(__name__)
        log.debug("Starting validate command")
        validate.validate(duplicatedir, workingdir, archivedir, validateddir, monitor=self.monitor)
        log.debug("Validate command completed")

    def test(self):
        print("Running Full Test Sequence")
        log = logging.getLogger(__name__)
        log.debug("Starting test command")
        # the ingest function sorts and moves files by date into the working/media directory
        ingest.ingest(ingestdir, workingdir, monitor=self.monitor)

        # the crawl function performs a hash index of all files in the target directories
        workingdirsum = crawl.crawl(True, workingdir, jsondatadir, monitor=self.monitor)
        archivedirsum = crawl.crawl(False, archivedir, jsondatadir, monitor=self.monitor)

        # the dedupe function combines all hash indexes and analyzes the dataset for duplicates
        data_files = glob.glob(jsondatadir + '/*.json')
        # run the dedupe function
        dedupe.dedupe(data_files, duplicatedir, monitor=self.monitor)

        # after the dedupe function has moved duplicates out, reindex
        workingdirsum = crawl.crawl(True, workingdir, jsondatadir, monitor=self.monitor)

        # the archive function pulls from the working/media directory and pools into sized volumes
        archive.archive(archivedir, jsondatadir, workingdir, mediasize, monitor=self.monitor)

        # validate that all files in duplicates exist elsewhere before moving to validated
        validate.validate(duplicatedir, workingdir, archivedir, validateddir, monitor=self.monitor)

        print("Daily Job Completed Successfully")
        log.debug("Test command completed")

    def daily(self):
        print("Running Daily Job")
        log = logging.getLogger(__name__)
        log.debug("Starting daily command")
        # the ingest function sorts and moves files by date into the working/media directory
        ingest.ingest(ingestdir, workingdir, monitor=self.monitor)

        # the crawl function performs a hash index of all files in the target directories
        workingdirsum = crawl.crawl(True, workingdir, jsondatadir, monitor=self.monitor)
        archivedirsum = crawl.crawl(False, archivedir, jsondatadir, monitor=self.monitor)

        # the dedupe function combines all hash indexes and analyzes the dataset for duplicates
        data_files = glob.glob(jsondatadir + '/*.json')
        # run the dedupe function
        dedupe.dedupe(data_files, duplicatedir, monitor=self.monitor)

        # after the dedupe function has moved duplicates out, reindex
        # workingdirsum = crawl.crawl(True, workingdir, jsondatadir, monitor=self.monitor)

        # the archive function pulls from the working/media directory and pools into sized volumes
        # archive.archive(archivedir, jsondatadir, workingdir, mediasize, monitor=self.monitor)

        # validate that all files in duplicates exist elsewhere before moving to validated
        # validate.validate(duplicatedir, workingdir, archivedir, validateddir, monitor=self.monitor)
        log.debug("Daily command completed")

def main():
    """Entry point for the mediastruct command-line script."""
    log.debug("Entering main function")
    # Parse the -m flag early to pass to mediastruct_init
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('-m', action='store_true', help='Enable the monitoring interface (curses-based progress display)')
    args, remaining = parser.parse_known_args()
    sys.argv = [sys.argv[0]] + remaining  # Remove -m from sys.argv for subsequent parsing
    mediastruct_init(enable_monitor=args.m)
    log.debug("Exiting main function")

if __name__ == '__main__':
    log.debug("Script entry point reached")
    main()
    log.debug("Script execution completed")
