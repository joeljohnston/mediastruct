import os
import sys
import configparser
import glob
import io
import logging
import logging.config
import argparse

#add path of package
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

##############################################################
# Import local classes
##############################################################
import ingest
import crawl
import dedupe
import archive
import utils
import validate

#############################################################
# Config files - create with defaults if they don't exist
#############################################################
configfile_name = "conf/config.ini"
#create a template if the config.ini doesn't exist
if not os.path.isfile(configfile_name):
    cfgfile = open(configfile_name, 'w')
    appConfig = configparser.ConfigParser()
    appConfig.add_section('ingestdirs')
    appConfig.set('ingestdirs','ingestdir','/data/ingest')
    appConfig.add_section('workingdirs')
    appConfig.set('workingdirs','workingdir','/data/media')
    appConfig.add_section('archivedir')
    appConfig.set('archivedir','archivedir','/archive')
    appConfig.add_section('archivemedia')
    appConfig.set('archivemedia','mediasize','24')
    appConfig.set('archivemedia','burnedtag','wr')
    appConfig.add_section('duplicates')
    appConfig.set('duplicates','duplicatedir','/data/duplicates')
    appConfig.add_section('validated')
    appConfig.set('validated','validateddir','/validated')
    appConfig.add_section('datadir')
    appConfig.set('datadir','jsondatadir','data')
    appConfig.set('datadir','logdir','logs')
    appConfig.write(cfgfile)
    cfgfile.close()
else:
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

#############################################################
# Setup Paths
#############################################################
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
this_path = os.path.dirname(os.path.realpath(__file__))
one_up = os.path.dirname(os.path.realpath(__file__)) + '/../'
app_path = os.path.join(this_path, one_up)
config_path = app_path + 'conf/'

##############################################################
# Logging
##############################################################
log_path = logdir + '/mediastruct.log'
if not os.path.isfile(log_path):
    logfile = open(log_path,'w')
logging.basicConfig(filename=log_path, level=logging.DEBUG, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p %Z -')
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
console.setFormatter(formatter)

class mediastruct_init(object):
    def __init__(self):
        log = logging.getLogger(__name__)

        log.info("########################## Starting Medistruct ##########################")
        #arparse init
        parser = argparse.ArgumentParser(description='Manage media file structure for archiving.',usage='''mediastruct <command> [<args>]
        Commands:
        ingest -  Moves files from the ingest directory set in conf/config.ini to the working directory set in conf/config.ini in a date structure

        crawl -   Iterates through all configured directories (except duplicates) and creates a hash index json file in data/

        dedupe -  Combines all configured directory's json datasets and moves duplicates in the working directory or ingest into the duplicates directory

        archive - Uses the mediasize variable set in conf/config.ini to create sized volumes in the archive directory and moves files accordingly

        validate - Does the reverse of the above actions by rehashing and comparing each marked duplicate file to all files in all structures, moves matches to the validated directory

        daily -   Combines the above functions into a re-usable automated workflow for use with scheduled jobs
        ''')
        parser.add_argument('command', help='Subcommand to run')
        args = parser.parse_args(sys.argv[1:2])
        if not hasattr(self, args.command):
            print('Unrecognized command')
            parser.print_help()
            exit(1)
        #Assess Command Argument
        log.info("Command: %s" % (args.command))
        getattr(self, args.command)()


    def crawl(self):
        print("Crawling")
        #the crawl function performs a hash index of all files in the target directories
        parser = argparse.ArgumentParser(description='Crawl the dirs and create a hash index')
        parser.add_argument('-f','--force',action='store_true',default=False,help='forces indexing of all directories')
        parser.add_argument('-p','--path',help='pass a directory to crawl')
        args = parser.parse_args(sys.argv[2:])
        #Crawl a provided directory
        if args.path:
            crawl.crawl(args.force,args.path,jsondatadir)
        else:
            ingestsum = crawl.crawl(args.force,ingestdir,jsondatadir)
            workingdirsum = crawl.crawl(args.force,workingdir,jsondatadir)
            archivedirsum = crawl.crawl(args.force,archivedir,jsondatadir)

    def ingest(self):
        print("Ingesting Files")
        #the ingest function sorts and moves files by date into the working/media directory
        a = ingest.ingest(ingestdir,workingdir)

    def dedupe(self):
        print("Dedupping")
        #the dedupe function combines all hash indexes and analyzes the dataset for duplicates
        data_files = glob.glob(jsondatadir + '/*.json')
        #run the dedupe function
        dedupe.dedupe(data_files,duplicatedir,archivedir)

    def archive(self):
        print("Archiving")
        #the archive function pulls from the working/media directory and pools into sized volumes
        archive.archive(archivedir,jsondatadir, workingdir,mediasize)

    def validate(self):
        print("Validating - This can take awhile")
        validate.validate(duplicatedir,workingdir,archivedir,validateddir)

    def test(self):
        print("Running Full Test Sequence")
        #the ingest function sorts and moves files by date into the working/media directory
        ingest.ingest(ingestdir,workingdir)

        #the crawl function performs a hash index of all files in the target directories
        workingdirsum = crawl.crawl(True,workingdir,jsondatadir)
        archivedirsum = crawl.crawl(False,archivedir,jsondatadir)

        #the dedupe function combines all hash indexes and analyzes the dataset for duplicates
        data_files = glob.glob(jsondatadir + '/*.json')
        #run the dedupe function
        dedupe.dedupe(data_files,duplicatedir)

        #after the dedupe function has moved duplicaes out, reindex
        workingdirsum = crawl.crawl(True,workingdir,jsondatadir)

        #the archive function pulls from the working/media directory and pools into sized volumes
        archive.archive(archivedir,jsondatadir, workingdir,mediasize)

        #validate that all files in duplicates exist elsewhere before moving to validated
        validate.validate(duplicatedir,workingdir,archivedir,validateddir)

        print("Daily Job Completed Successfully")

    def daily(self):
        print("Running Daily Job")
        #the ingest function sorts and moves files by date into the working/media directory
        ingest.ingest(ingestdir,workingdir)

        #the crawl function performs a hash index of all files in the target directories
        workingdirsum = crawl.crawl(True,workingdir,jsondatadir)
        archivedirsum = crawl.crawl(False,archivedir,jsondatadir)

        #the dedupe function combines all hash indexes and analyzes the dataset for duplicates
        data_files = glob.glob(jsondatadir + '/*.json')
        #run the dedupe function
        dedupe.dedupe(data_files,duplicatedir)

        #after the dedupe function has moved duplicaes out, reindex
        #workingdirsum = crawl.crawl(True,workingdir,jsondatadir)

        #the archive function pulls from the working/media directory and pools into sized volumes
        #archive.archive(archivedir,jsondatadir, workingdir,mediasize)
        
        #validate that all files in duplicates exist elsewhere before moving to validated
        #validate.validate(duplicatedir,workingdir,archivedir,validateddir)

#launch on init
if __name__ == '__main__':
    mediastruct_init()
