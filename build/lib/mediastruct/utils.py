"""Find duplicate files inside a directory tree."""
import os
import re
from os import walk, remove, stat
from os.path import join as joinpath
import logging

log = logging.getLogger(__name__)
log.info('Launching the Utils Class')

class utils:

    def getFolderSize(self,start_path = '.'):
        total_size = 0
        for dirpath, dirnames, filenames in os.walk(start_path):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                total_size += os.path.getsize(fp)
        return total_size

    #Basic Make Directory Function
    def mkdir_p(self, path):
        if os.path.exists(path):
            log.info("%s exists" % (path))
        else:
            log.info("Creating Directory: %s" % (path))
            os.makedirs(path)

