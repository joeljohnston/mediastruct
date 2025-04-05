from setuptools import setup, find_packages

setup(
    name='mediastruct',  # Name of the PyPI package
    version='0.1',       # Version number
    packages=find_packages(include=['mediastruct', 'mediastruct.*']),
    install_requires=[
        'xxhash',        # Required for hashing
        'windows-curses; platform_system=="Windows"',  # Required for curses on Windows
    ],
    entry_points={
        'console_scripts': [
            'mediastruct = mediastruct.__main__:main',  # Creates the `mediastruct` command
        ],
    },
)
