#!/usr/bin/env python
#
# Setup prog for vc3core
#

import commands
import os
import re
import sys

from vc3 import core
release_version=core.__version__

from distutils.core import setup
from distutils.command.install import install as install_org
from distutils.command.install_data import install_data as install_data_org

#libexec_files = ['libexec/%s' %file for file in os.listdir('libexec') if os.path.isfile('libexec/%s' %file)]

systemd_files = [ 'etc/vc3-infoservice.service'
                 ]

etc_files = ['etc/vc3-core.conf'
             ]

logrotate_files = ['etc/logrotate/vc3-core',]

initd_files = ['etc/vc3-core.init',  ]

rpm_data_files=[
                #('/usr/libexec', libexec_files),
                ('/etc/vc3', etc_files),
                ('/etc/logrotate.d', logrotate_files),                                        
                ('/etc/init.d', initd_files),
                ('/usr/lib/systemd/system', systemd_files),                                     
               ]


home_data_files=[
                 #('etc', libexec_files),
                 ('etc', etc_files),
                 ('etc', initd_files),
                ]

def choose_data_files():
    rpminstall = True
    userinstall = False
     
    if 'bdist_rpm' in sys.argv:
        rpminstall = True

    elif 'install' in sys.argv:
        for a in sys.argv:
            if a.lower().startswith('--home'):
                rpminstall = False
                userinstall = True
                
    if rpminstall:
        return rpm_data_files
    elif userinstall:
        return home_data_files
    else:
        # Something probably went wrong, so punt
        return rpm_data_files
       
# ===========================================================

# setup for distutils
setup(
    name="vc3-core",
    version=release_version,
    description='vc3-core package',
    long_description='''This package contains the VC3 Core Service''',
    license='GPL',
    author='John Hover',
    author_email='jhover@bnl.gov',
    maintainer='John Hover',
    maintainer_email='jhover@bnl.gov',
    url='https://github.com/vc3-project',
    packages=['vc3'
              ],
    scripts = [ # Utilities and main script
               'scripts/vc3-core',
              ],
    
    data_files = choose_data_files()
)
