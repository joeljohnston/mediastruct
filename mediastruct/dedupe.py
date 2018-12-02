import os
import logging
import hashlib
import json
import glob

log = logging.getLogger(__name__)
log.info('Launching the Dedupe Class')

class dedupe:
    def __init__(self, datadir, *args):
        try:
            self.args = dict(args)
        except:
            print("uh oh you broke it")
        finddupe(self, datadir, args)

    def finddupe(self, datadir, args):
        for i in args:
            dirname = re.split(r"\/", i)
            if os.path.isfile('%s/%s_index.json' % (datadir, dirname[2])):
                with open('%s/%s_index.json' % (datadir, dirname[2]), 'r') as f: 
                    array = json.load(f)
                    combined_array = {**x, **y}
