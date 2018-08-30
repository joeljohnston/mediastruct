import os
import sys
import configparser
import io

#add path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

#import local classes
import ingest
import crawl
import dedupe
import sort
import archive
import validate

def _launch():
    #config files - create with defaults if they don't exist
    configfile_name = "conf/config.ini"
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

        appConfig.write(cfgfile)
        cfgfile.close()
    else:
        config = configparser.ConfigParser()
        config.read('conf/config.ini')
        ingestdir = config['ingestdirs']['ingestdir']
        workingdirs =  config['workingdirs']['workingdir']
        archivedir =  config['archivedir']['archivedir'] 
        mediasize = config['archivemedia']['mediasize']
        burnedtag = config['archivemedia']['burnedtag']
        duplicatedir = config['duplicates']['duplicatedir']

    if sys.argv[1] == 'crawl':
        crawl()

    if sys.argv[1] == 'ingest':
        a = ingest.ingest(ingestdir,archivedir)
        print("test")

    if sys.argv[1] == 'dedupe':
        dedupe()

    if sys.argv[1] == 'sort':
        sort()

    if sys.argv[1] == 'archive':
        archive()

    if sys.argv[1] == 'validate':
        validate()

    if sys.argv[1] == '':
        print("You gotta gimme something here")


if __name__ == '__main__':
    _launch()
