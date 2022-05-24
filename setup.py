from setuptools import setup, find_packages
 
setup(
     name='mediastruct',    # This is the name of your PyPI-package.
     version='0.1',                          # Update the version number for new releases
     #scripts=['mediastructr']                  # The name of your scipt, and also the command you'll be using for calling it
     packages=find_packages(include=['mediastruct', 'mediastruct.*'])
 )
