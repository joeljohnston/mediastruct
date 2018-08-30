import os
import sys
import argparse

parser = argparse.ArgumentParser(description='generate a random directory structure and fileset')
parser.add_argument('base_directory', metavar='N', type=str, nargs='+',
                    help='the parent directory you want to generate inside of')
parser.add_argument('--sum', dest='accumulate', action='store_const',
                    const=sum, default=max,
                    help='sum the integers (default: find the max)')

args = parser.parse_args()
print(args.accumulate(args.integers))

