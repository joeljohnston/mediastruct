import os
import sys
import configparser
import glob
import io
import logging
import logging.config

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
#import validate

#############################################################
# Setup Paths
#############################################################
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
this_path = os.path.dirname(os.path.realpath(__file__))
one_up = os.path.dirname(os.path.realpath(__file__)) + '/../'
app_path = os.path.join(this_path, one_up)
config_path = app_path + 'conf/'

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
    jsondatadir = config['datadir']['jsondatadir']
    logdir = config['datadir']['logdir']

##############################################################
# Logging
##############################################################
log_path = logdir + '/mediastruct.log' 
logging.basicConfig(filename=log_path, level=logging.DEBUG, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p %Z -')
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

def _launch():
    #the crawl function performs a hash index of all files in the target directories
    if sys.argv[1] == 'crawl':
        ingestsum = crawl.crawl(ingestdir,jsondatadir)
        workingdirsum = crawl.crawl(workingdir,jsondatadir)
        archivedirsum = crawl.crawl(archivedir,jsondatadir)
        #duplicatedirsum = crawl.crawl(duplicatedir,jsondatadir)

    #the ingest function sorts and moves files by date into the working/media directory
    if sys.argv[1] == 'ingest':
        a = ingest.ingest(ingestdir,workingdir)
    
    #the dedupe function combines all hash indexes and analyzes the dataset for duplicates
    if sys.argv[1] == 'dedupe':
        data_files = glob.glob(jsondatadir + '/*.json')
        #run the dedupe function
        dedupe.dedupe(data_files,duplicatedir)
        #after the dedupe function has moved duplicaes out, reindex
        ingestsum = crawl.crawl(ingestdir,jsondatadir)
        workingdirsum = crawl.crawl(workingdir,jsondatadir)
        archivedirsum = crawl.crawl(archivedir,jsondatadir)
        #duplicatedirsum = crawl.crawl(duplicatedir,jsondatadir)

    #the archive function pulls from the working/media directory and pools into sized volumes
    if sys.argv[1] == 'archive':
        archive.archive(archivedir,jsondatadir, workingdir,mediasize)

    if sys.argv[1] == 'validate':
        validate()

    if sys.argv[1] == '':
        print("You gotta gimme something here")

#launch on init
if __name__ == '__main__':
    _launch()
