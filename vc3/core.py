#!/usr/bin/env python 
__author__ = "John Hover, Jose Caballero"
__copyright__ = "2017 John Hover"
__credits__ = []
__license__ = "GPL"
__version__ = "0.9.1"
__maintainer__ = "John Hover"
__email__ = "jhover@bnl.gov"
__status__ = "Production"


import logging
import logging.handlers
import os
import platform
import pwd
import random
import json
import shutil
import string
import socket
import sys
import threading
import time
import traceback
import urllib

from optparse import OptionParser
from ConfigParser import ConfigParser, NoOptionError

from multiprocessing import Process
import subprocess


# Since script is in package "vc3" we can know what to add to path for 
# running directly during development
(libpath,tail) = os.path.split(sys.path[0])
sys.path.append(libpath)

from pluginmanager.plugin import PluginManager
from infoclient import InfoClient

class VC3Core(object):
    
    def __init__(self, request_name, config):
        self.log = logging.getLogger()
        self.log.debug('VC3Core class init...')

        self.request_name = request_name

        self.processes    = {}

        self.config = config

        self.certfile  = os.path.expanduser(config.get('netcomm', 'certfile'))
        self.keyfile   = os.path.expanduser(config.get('netcomm', 'keyfile'))
        self.chainfile = os.path.expanduser(config.get('netcomm', 'chainfile'))

        self.builder_path        = os.path.expanduser(config.get('builder', 'path'))
        self.builder_install_dir = os.path.expanduser(config.get('builder', 'installdir'))
        self.builder_home_dir    = config.get('builder', 'homedir')
        self.builder_env         = config.get('builder', 'environment')

        self.request_log_dir     = os.path.join(self.builder_install_dir, self.builder_home_dir, '.' + self.request_name, 'logs')
        self.request_runtime_dir = os.path.join(self.builder_install_dir, self.builder_home_dir, '.' + self.request_name, 'runtime')
        self.request_main_conf   = os.path.join(os.path.expandvars('$VC3_SERVICES_HOME'), 'etc', 'vc3-master.conf')

        os.environ['VC3_REQUEST_NAME']        = self.request_name
        os.environ['VC3_REQUEST_LOG_DIR']     = self.request_log_dir
        os.environ['VC3_REQUEST_RUNTIME_DIR'] = self.request_runtime_dir
        os.environ['VC3_REQUEST_MAIN_CONF']   = self.request_main_conf

        # runtime is particular to a run, so we clean it up if it exists
        if os.path.isdir(self.request_runtime_dir):
            shutil.rmtree(self.request_runtime_dir)

        for dir in [self.request_log_dir, self.request_runtime_dir]:
            if not os.path.isdir(dir):
                os.makedirs(dir)

        try:
            self.builder_n_jobs = config.get('builder', 'n_jobs')
        except NoOptionError, e:
            self.builder_n_jobs = 4

        self.whitelist_services  = config.get('core', 'whitelist')
        self.whitelist_services  = self.whitelist_services.split(',')
        
        self.log.debug("certfile=%s"  % self.certfile)
        self.log.debug("keyfile=%s"   % self.keyfile)
        self.log.debug("chainfile=%s" % self.chainfile)
        
        self.infoclient = InfoClient(config)    
        self.__report_cluster()

        self.log.debug('VC3Core class done.')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            dir = self.request_runtime_dir
        except AttributeError:
            # __init__ did not finish running
            pass

        # runtime is particular to a run, so we clean it up if it exists
        if os.path.isdir(self.request_runtime_dir):
            shutil.rmtree(self.request_runtime_dir)
        return self

    def __del__(self):
        return self.__exit__(None, None, None)

    def __report_cluster(self):
        self.host_address = self.__my_host_address()

        report = {}
        report['hostname']               = self.host_address
        report['VC3_REQUEST_NAME']       = self.request_name
        report['VC3_REQUEST_RUNTIME_DIR'] = self.request_runtime_dir
        report['VC3_REQUEST_LOG_DIR']     = self.request_log_dir

        try:
            f = open(os.path.join(self.request_runtime_dir, 'cluster.conf'), 'w')

            # Section should be named 'cluster', but python does not allow interpolation inter sections.
            # or DEFAULT but apf removes all defaults. We use Factory for now,
            # since it is the section where we need it.  
            f.write('[Factory]\n\n')
            for key in report:
                f.write(key + ' = ' + report[key] + '\n')
            f.close()
        except Exception, e:
            self.log.info(str(e))
            raise e

        pretty = json.dumps({ self.request_name : report }, indent=4, sort_keys=True)
        self.infoclient.storedocument('runtime', pretty)

    def __my_host_address(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(('10.255.255.255', 0))
            addr = s.getsockname()[0]
        except:
            addr = '127.0.0.1'
        finally:
            s.close()
        return addr
        
    def run(self):
        self.log.debug('Core running...')
        while True:
            self.log.debug("Core polling....")
            d = self.infoclient.getdocument('request')

            rs = None
            try:
                rs = json.loads(d)
            except Exception, e:
                self.log.degub(e)
                raise e

            if   'request' in rs   and   self.request_name in rs['request']:
                self.perform_request(rs['request'][self.request_name])
            else:
                # There is not a request for this cluster. Should the cluster
                # start to clean up?
                raise Exception("My request went away")
            time.sleep(5)

    def terminate(self):
        '''
        Like an 'empty' request, to vacate all running services.
        '''
        self.terminate_old_services({})
        sys.exit(0)


    def perform_request(self, request):
        if not 'action' in request:
            self.log.info("malformed request for '%s'. no action specified." % (self.request_name))
            return

        if not 'services' in request:
            self.log.info("malformed request for '%s'. no services specified." % (self.request_name))
            return

        action   = request['action']
        services = request['services']

        if action == 'terminate':
            self.terminate()

        for service_name in services:
            service = services[service_name]
            self.perform_service_request(service_name, service)

    def perform_service_request(self, service_name, service):
        if not service_name in self.whitelist_services:
            self.log.info("service '%s' is not whitelisted for %s." % (service_name,self.request_name))
            return

        if not 'action' in service:
            self.log.info("malformed request for '%s:%s'. no action specified." % (self.request_name,service_name))
            return

        action = service['action']
        if not action == 'spawn':
            return

        if 'files' in service:
            files = service['files']
            for name in files:
                self.write_request_file(files[name])

        if not service_name in self.processes or not self.processes[service_name].is_alive():
            cmd = service_name
            if 'command' in service:
                cmd = service['command']

            self.processes[service_name] = self.execute(service_name, cmd)

        # terminate site requests that are no longer present
        #self.terminate_old_services(request)

    def write_request_file(self, file_spec):
        try:
            destination = os.path.expandvars(file_spec['destination'])
            contents    = file_spec['contents']
        except KeyError, e:
            self.log.info('Malformed file specification. Missing key: %s' % (str(e),))
            raise e

        dir = os.path.dirname(destination)
        if not os.path.isdir(dir):
            os.makedirs(dir)

        with open(destination, 'w') as f:
            decoded = urllib.unquote_plus(contents)
            f.write(decoded)

    def execute(self, service_name, payload):
        def service_factory():
            cmd = [self.builder_path,
                    '--var',       'VC3_REQUEST_NAME='        + self.request_name,
                    '--var',       'VC3_REQUEST_LOG_DIR='     + self.request_log_dir,
                    '--var',       'VC3_REQUEST_RUNTIME_DIR=' + self.request_runtime_dir,
                    '--var',       'VC3_REQUEST_MAIN_CONF='   + self.request_main_conf,
                    '--make-jobs', str(self.builder_n_jobs),
                    '--install',   self.builder_install_dir,
                    '--home',      self.builder_home_dir,
                    '--require',   self.builder_env,
                    '--require',   service_name,
                    '--']
            # until we have arrays in json, assume ' ' as argument separator
            cmd.extend(payload.split())
            try:
                self.log.info("Executing: %s" % (str(cmd),))
                subprocess.check_call(cmd)
                return 0
            except subprocess.CalledProcessError, ex:
                self.log.info("Service terminated: '" + self.request_name + ":" + service_name + "': " + str(ex))
                return ex.returncode

        p = Process(target = service_factory)
        self.log.info('Starting service ' + service_name)
        p.start()
        return p

    def terminate_old_services(self,request):
        '''
        Terminate those services that no longer appear on the request.
        '''
        services_to_delete = []
        for service_name in self.processes:

            action = None
            try:
                action = request[service_name]['action']
            except KeyError:
                action = "not-specified"

            if (not service_name in request) or (action == 'off'):
                self.log.info('Terminating service... ' + service_name)

                self.processes[service_name].terminate()
                self.processes[service_name].join(60)
                services_to_delete.append(service_name)

                self.log.info('Terminated: ' + service_name)

        for service_name in services_to_delete:
            del self.processes[service_name]

       
    

class VC3CoreCLI(object):
    """class to handle the command line invocation of service. 
       parse the input options,
       setup everything, and run VC3Core class
    """
    def __init__(self):
        self.options = None 
        self.args = None
        self.log = None
        self.config = None

        self.__presetups()
        self.__parseopts()
        self.__setuplogging()
        self.__platforminfo()
        self.__checkroot()
        self.__createconfig()

    def __presetups(self):
        '''
        we put here some preliminary steps that 
        for one reason or another 
        must be done before anything else
        '''

    
    def __parseopts(self):
        parser = OptionParser(usage='''%prog [OPTIONS]
vc3-Core is a information store for VC3

This program is licensed under the GPL, as set out in LICENSE file.

Author(s):
John Hover <jhover@bnl.gov>
''', version="%prog $Id: Core.py 3-3-17 23:58:06Z jhover $" )

        parser.add_option("-d", "--debug", 
                          dest="logLevel", 
                          default=logging.WARNING,
                          action="store_const", 
                          const=logging.DEBUG, 
                          help="Set logging level to DEBUG [default WARNING]")
        parser.add_option("-v", "--info", 
                          dest="logLevel", 
                          default=logging.WARNING,
                          action="store_const", 
                          const=logging.INFO, 
                          help="Set logging level to INFO [default WARNING]")
        parser.add_option("--console", 
                          dest="console", 
                          default=False,
                          action="store_true", 
                          help="Forces debug and info messages to be sent to the console")
        parser.add_option("--quiet", dest="logLevel", 
                          default=logging.WARNING,
                          action="store_const", 
                          const=logging.WARNING, 
                          help="Set logging level to WARNING [default]")
        parser.add_option("--conf", dest="confFiles", 
                          default="/etc/vc3/vc3-core.conf",
                          action="store", 
                          metavar="FILE1[,FILE2,FILE3]", 
                          help="Load configuration from FILEs (comma separated list)")
        parser.add_option("--log", dest="logfile", 
                          default="stdout", 
                          metavar="LOGFILE", 
                          action="store", 
                          help="Send logging output to LOGFILE or SYSLOG or stdout [default <syslog>]")
        parser.add_option("--runas", dest="runAs", 
                          #
                          # By default
                          #
                          default=pwd.getpwuid(os.getuid())[0],
                          action="store", 
                          metavar="USERNAME", 
                          help="If run as root, drop privileges to USER")
        (self.options, self.args) = parser.parse_args()

        self.options.confFiles = self.options.confFiles.split(',')

    def __setuplogging(self):
        """ 
        Setup logging 
        """
        self.log = logging.getLogger()
        if self.options.logfile == "stdout":
            logStream = logging.StreamHandler()
        else:
            lf = os.path.expanduser(self.options.logfile)
            logdir = os.path.dirname(lf)
            if not os.path.exists(logdir):
                os.makedirs(logdir)
            runuid = pwd.getpwnam(self.options.runAs).pw_uid
            rungid = pwd.getpwnam(self.options.runAs).pw_gid                  
            os.chown(logdir, runuid, rungid)
            logStream = logging.FileHandler(filename=lf)    

        # Check python version 
        major, minor, release, st, num = sys.version_info
        if major == 2 and minor == 4:
            FORMAT='%(asctime)s (UTC) [ %(levelname)s ] %(name)s %(filename)s:%(lineno)d : %(message)s'
        else:
            FORMAT='%(asctime)s (UTC) [ %(levelname)s ] %(name)s %(filename)s:%(lineno)d %(funcName)s(): %(message)s'
        formatter = logging.Formatter(FORMAT)
        formatter.converter = time.gmtime  # to convert timestamps to UTC
        logStream.setFormatter(formatter)
        self.log.addHandler(logStream)

        # adding a new Handler for the console, 
        # to be used only for DEBUG and INFO modes. 
        if self.options.logLevel in [logging.DEBUG, logging.INFO]:
            if self.options.console:
                console = logging.StreamHandler(sys.stdout)
                console.setFormatter(formatter)
                console.setLevel(self.options.logLevel)
                self.log.addHandler(console)
        self.log.setLevel(self.options.logLevel)
        self.log.info('Logging initialized at level %s.' % self.options.logLevel)


    def _printenv(self):

        envmsg = ''        
        for k in sorted(os.environ.keys()):
            envmsg += '\n%s=%s' %(k, os.environ[k])
        self.log.debug('Environment : %s' %envmsg)


    def __platforminfo(self):
        '''
        display basic info about the platform, for debugging purposes 
        '''
        self.log.info('platform: uname = %s %s %s %s %s %s' %platform.uname())
        self.log.info('platform: platform = %s' %platform.platform())
        self.log.info('platform: python version = %s' %platform.python_version())
        self._printenv()

    def __checkroot(self): 
        """
        If running as root, drop privileges to --runas' account.
        """
        starting_uid = os.getuid()
        starting_gid = os.getgid()
        starting_uid_name = pwd.getpwuid(starting_uid)[0]

        hostname = socket.gethostname()
        
        if os.getuid() != 0:
            self.log.info("Already running as unprivileged user %s at %s" % (starting_uid_name, hostname))
            
        if os.getuid() == 0:
            try:
                runuid = pwd.getpwnam(self.options.runAs).pw_uid
                rungid = pwd.getpwnam(self.options.runAs).pw_gid
                os.chown(self.options.logfile, runuid, rungid)
                
                os.setgid(rungid)
                os.setuid(runuid)
                os.seteuid(runuid)
                os.setegid(rungid)

                self._changehome()
                self._changewd()

                self.log.info("Now running as user %d:%d at %s..." % (runuid, rungid, hostname))
                self._printenv()

            
            except KeyError, e:
                self.log.error('No such user %s, unable run properly. Error: %s' % (self.options.runAs, e))
                sys.exit(1)
                
            except OSError, e:
                self.log.error('Could not set user or group id to %s:%s. Error: %s' % (runuid, rungid, e))
                sys.exit(1)

    def _changehome(self):
        '''
        Set environment HOME to user HOME.
        '''
        runAs_home = pwd.getpwnam(self.options.runAs).pw_dir 
        os.environ['HOME'] = runAs_home
        self.log.debug('Setting up environment variable HOME to %s' %runAs_home)


    def _changewd(self):
        '''
        changing working directory to the HOME directory of the new user,
        '''
        runAs_home = pwd.getpwnam(self.options.runAs).pw_dir
        os.chdir(runAs_home)
        self.log.debug('Switching working directory to %s' %runAs_home)


    def __createconfig(self):
        """Create config, add in options...
        """
        if self.options.confFiles != None:
            try:
                self.config = ConfigParser()
                self.config.read(self.options.confFiles)
            except Exception, e:
                self.log.error('Config failure')
                sys.exit(1)
        
        #self.config.set("global", "configfiles", self.options.confFiles)
           
    def run(self):
        """
        Create Daemon and enter main loop
        """

        try:
            self.log.info('Creating Daemon and entering main loop...')
            vc3m = VC3Core(self.config)
            vc3m.run()
            
        except KeyboardInterrupt:
            self.log.info('Caught keyboard interrupt - exitting')
            sys.exit(0)
        except ImportError, errorMsg:
            self.log.error('Failed to import necessary python module: %s' % errorMsg)
            sys.exit(1)
        except:
            self.log.error('''Unexpected exception!''')
            # The following line prints the exception to the logging module
            self.log.error(traceback.format_exc(None))
            print(traceback.format_exc(None))
            sys.exit(1)          

if __name__ == '__main__':
    mcli = VC3CoreCLI()
    mcli.run()

