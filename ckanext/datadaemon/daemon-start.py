#!/srv/ckan/py2env/bin/python
# -*- coding: UTF-8 -*-
import sys, time
import daemon
import traceback

# Add code to start notify function
import os, signal
import functools
import pyinotify

# Add code to be executed
import ckan
import ckanext
from ckan import plugins
from ckanext.datadaemon import datadaemon
from pylons import config
from datetime import datetime

# Daemon logfile
stderr = '/var/log/ckan/datadaemon-error.log'
pidfile = '/var/run/datadaemon/ckanext-datadaemon.pid'
directory = '/srv/ckan/sftp'

class NotifyEventHandler(pyinotify.ProcessEvent):
    def process_IN_MODIFY(self, event):
        """
        This class is the default execution for all events
        """
        date = datetime.today()
        message = date.__str__() + ' : ' +  'Alteracao no diretorio %s' % event.pathname
        print >> sys.stderr, message

        # Create datadaemon instance
        d = datadaemon.datadaemon()

        # Setup files list
        d.files = []
        d.files.append('file://' + event.pathname)
        #for file in event.pathname:
            # Add URL formatting
        #    d.files.append('file://' + event.pathname)

        # Run storage
        try:
            d.run()

            # Remove object as it's no longer necessary
            del d
        except:
            date = datetime.today()
            message = date.__str__() + ' : files storage error' + traceback.format_exc()
            print >> sys.stderr, message

            # Remove object as it's no longer necessary
            del d


#class MyDaemon(daemon.Daemon):
class MyDaemon():
    def __init__(self):
        #self.dir = config.get('ckanext.datadaemon.dir')
        self.dir = directory
        # FIXME: parametrize this
        self.pidfile = '/var/run/datadaemon/ckanext-datadaemon.pid'
        self.stdout = '/var/log/ckan/datadaemon.log'
        self.stderr = stderr

    #def run(self):
    def start(self):
        if self.dir is None:
            date = datetime.today()
            err = date.__str__() + ' : ' + 'Repositorio nao definido. Por favor, configure o parametro ckanext.datadaemon.dir no configuracao do sistema'

            fp = file(self.stderr, 'a+')
            print >> fp, err
            fp.close()
            return

        try:
            handler = NotifyEventHandler()
            wm = pyinotify.WatchManager()
            notifier = pyinotify.Notifier(wm, default_proc_fun=handler, read_freq=5)
            wm.add_watch(self.dir, pyinotify.ALL_EVENTS)
            notifier.loop(daemonize=True, 
                            pid_file=self.pidfile, stdout=self.stdout, stderr=self.stderr)
        except pyinotify.NotifierError, err:
            date = datetime.today()
            fp = file(self.stderr, 'a+')
            print >> fp, date.__str__() + ' : '
            print >> fp, err
            fp.close()

		#while True:
		#	time.sleep(1)

    def stop(self):
        """
		Stop the daemon
		"""
        import zipfile

        # Get the pid from the pidfile
        try:
            pf = file(self.pidfile,'r')
            pid = int(pf.read().strip())
            pf.close()

        except IOError:
            pid = None

        if not pid:
            message = "pidfile %s does not exist. Daemon not running?\n"
            print >> sys.stderr, message % self.pidfile
            return # not an error in a restart

        # Roll log file
        date = datetime.today()
        log_dir, log_file = os.path.split(self.stderr)
        name, extension = os.path.splitext(log_file)
        log_filename =  os.path.join(log_dir,(name + '-' + date.__str__().replace(' ', '-') + extension + '.zip'))

        # Create zipfile and store log contents
        z = zipfile.ZipFile(log_filename, 'w')
        z.write(self.stderr)
        z.close()

        # Now clean log
        f = open(self.stderr, 'w+')
        f.write('')
        f.close()

		# Try killing the daemon process	
        try:
            while 1:
                os.kill(pid, signal.SIGTERM)
                time.sleep(0.1)
        except OSError, err:
            err = str(err)
            if err.find("No such process") > 0:
                if os.path.exists(self.pidfile):
                    os.remove(self.pidfile)
            else:
                print str(err)
                sys.exit(1)

    def restart(self):
        """
		Restart the daemon
		"""
        self.stop()
        self.start()

if __name__ == "__main__":
    daemon = MyDaemon()
    if len(sys.argv) == 2:
        if 'start' == sys.argv[1]:
            daemon.start()
        elif 'stop' == sys.argv[1]:
            daemon.stop()
        elif 'restart' == sys.argv[1]:
            daemon.restart()
        else:
            print "Unknown command"
            sys.exit(2)
            
        sys.exit(0)
    else:
        print "usage: %s start|stop|restart" % sys.argv[0]
        sys.exit(2)
