import argparse
import os

parser = argparse.ArgumentParser(prog='PROG')
parser.add_argument('--opt', nargs='?', help='foo help')
parser.add_argument('bar', nargs='+', help='bar help')
parser.print_help()

print ('SIM_DEBUG: ' + str(os.environ.get("SIM_DEBUG")))

if os.environ.get("SIM_DEBUG") == None:
    print('Debugging')
else:
    print('Not Debugging')


