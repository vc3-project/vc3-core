#
# __init__.py
#
#
"""
Generic __init__.py to allow 

        from Package.path import *

to work correctly. 

"""
from os.path import basename, dirname, splitext
import glob

allmods = []

leftpath = dirname(__file__)

for filename in glob.glob("%s/*.py" % leftpath):
    if filename.find("__") < 0:   # ignore __init__.py files
        #print "python module file: %s " % filename
        modfilename = basename(filename)
        modname = modname, ext = splitext(modfilename)
        allmods.append(modname)

# List of all module names valid to import from this package
__all__= allmods