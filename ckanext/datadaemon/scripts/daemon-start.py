#!/srv/ckan/py2env/bin/python
# -*- coding: UTF-8 -*-
import sys, time
import traceback
import subprocess
import os.path
import codecs

# Add code to start notify function
import os, signal
import functools
import pyinotify

# Add code to be executed
from datetime import datetime

# Parameters
stderr = '/var/log/ckan/datadaemon-error.log'
pidfile = '/var/run/datadaemon/ckanext-datadaemon.pid'
directory = '/srv/ckan/sftp'
stdout = '/var/log/ckan/datadaemon.log'

# Paster binary
paster = os.path.join(sys.prefix, 'bin/paster')

# Datadaemon dir
datadaemon_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../../')

# Ckan config path
config_path='/srv/ckan/py2env/src/ckan/development.ini'

# Fix encoding of stdout and stderr
reload(sys)
sys.setdefaultencoding('utf-8')

print sys.getdefaultencoding()

sys.stdout = codecs.getwriter('utf8')(sys.stdout)
sys.stderr = codecs.getwriter('utf8')(sys.stderr)


class NotifyEventHandler(pyinotify.ProcessEvent):
    def process_IN_MODIFY(self, event):
        """
        This class is the default execution for all events
        """
        date = datetime.today()
        message = date.__str__() + ' : ' +  'Alteracao no diretorio %s' % event.pathname
        print >> sys.stderr, message

        # Setup files list
        file_url = 'file://' + event.pathname

        # Run storage
        try:
            # Execute the python script
            command = 'cd ' + datadaemon_dir + ' && ' + paster + ' datadaemon execute -u ' + file_url + ' -c ' + config_path
            #result = subprocess.call(command, stderr=subprocess.STDOUT)
            result = os.system(command)

            # Remove object as it's no longer necessary
        except:
            date = datetime.today()
            message = date.__str__() + ' : files storage error' + traceback.format_exc()
            print >> sys.stderr, message


class MyDaemon():
    def __init__(self):
        self.dir = directory
        self.pidfile = pidfile
        self.stdout = stdout
        self.stderr = stderr

    def start(self):
        date = datetime.today()
        fp = file(self.stderr, 'a+')
        print >> fp, date.__str__() + ' : Starting...'
        fp.close()

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

        # Now roll the other log file
        log_dir, log_file = os.path.split(self.stdout)
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
