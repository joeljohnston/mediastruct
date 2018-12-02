import os
import logging
import hashlib
import json
import glob

log = logging.getLogger(__name__)
log.info('Launching the Crawl Class')

class dedupe:
    def __init__(self, *args):
        try:
            self.args = dict(args)
        except:
            print("uh oh you broke it")
        finddupe(self, args)

    def finddupe(self, args):
        for i in args:
            dirname = re.split(r"\/", i)
            if os.path.isfile('%s_index.json' % (dirname[2])):
 24                 with open('data/%s_index.json' % (dirname[2]), 'r') as f: 


