import os
import sys

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
	
		Config = ConfigParser.ConfigParser()
		Config.add_section('ingestdirs')
		Config.set('ingestdirs','ingestdir','/data/drop')
		Config.add_section('workingdirs')
		Config.set('workingdirs','workingdir','/data/photos')
		Config.add_section('archivedir')
		Config.set('archivedir','archivedirs','/archive')
		Config.add_section('archivemedia')
		Config.set('archivemedia','mediasize','24')
		Config.set('archivemeida','burnedtag','wr')
		Config.add_section('duplicates')
		Config.set('duplicates','duplicate_dir','/data/duplicates')
		
		Config.write(cfgfile)
		cfgfile.close()
	
	if sys.argv[1] == 'ingest':
		ingest.run()
		
	if sys.argv[1] == 'crawl':
		crawl.run()
	
	if sys.argv[1] == 'dedupe':
		dedupe.run()	

	if sys.argv[1] == 'sort':
		sort.run()

	if sys.argv[1] == 'archive':
		archive.run()
	
	if sys.argv[1] == 'validate':
		validate.run()
	
	if sys.argv[1] == '':
		print("You gotta gimme something here")

	


	if __name__ == '__main__':
		_launch()
