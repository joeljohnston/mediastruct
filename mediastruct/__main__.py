import os
import sys
import configparser
import io

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import ingest
import crawl
import dedupe
import sort
import archive
import validate

def _launch():
    print("Running MediaStruct")
    #importing config file
    configfile_name = "conf/config.ini"
    if not os.path.isfile(configfile_name):
        cfgfile = open(configfile_name, 'w')

        Config = configparser.ConfigParser()
        Config.add_section('ingestdirs')
        Config.set('ingestdirs','ingestdir','/data/ingest')
        Config.add_section('workingdirs')
        Config.set('workingdirs','workingdir','/data/media')
        Config.add_section('archivedir')
        Config.set('archivedir','archivedir','/archive')
        Config.add_section('archivemedia')
        Config.set('archivemedia','mediasize','24')
        Config.set('archivemedia','burnedtag','wr')
        Config.add_section('duplicates')
        Config.set('duplicates','duplicatedir','/data/duplicates')

        Config.write(cfgfile)
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
        a = ingest.ingest()
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
