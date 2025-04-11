from setuptools import setup, find_packages

setup(
    name='mediastruct',
    version='0.2',  # Increment version to ensure the update is recognized
    packages=find_packages(include=['mediastruct', 'mediastruct.*']),
    install_requires=[
        'xxhash',
        'pyyaml',  # Add PyYAML dependency
        'windows-curses; platform_system=="Windows"',
    ],
    entry_points={
        'console_scripts': [
            'mediastruct = mediastruct.__main__:main',
        ],
    },
)
