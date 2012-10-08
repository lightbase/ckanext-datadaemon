import os
import shutil
import paste.script

from ckan import model
from ckan.lib.cli import CkanCommand
from ckan.logic import get_action, NotFound
from pylons import config
from ckanext.datadaemon import es
from ckanext.datadaemon import datadaemon


import logging
log = logging.getLogger()

class MigrationCommand(CkanCommand):
    '''
    CKAN datadaemon Extension migration command

    Usage::

        paster datadaemon migrate -c <path to config file>
			- Execute migration scripts to store datasets on lbdf index

        paster datadaemon clean -c <path to config file>
            - Remove all data created in new index

        paster datadaemon commit -c <path to config file>
			- Remove old indexes from elastic search. CAUTION: this operation can't be undone


    The commands should be run from the ckanext-datadaemon directory.
    '''
    summary = __doc__.split('\n')[0]
    usage = __doc__

    def __init__(self, name):
        """
        Building method to extend parameters
        """
        # Adding this make it works. Don't know why
        super(MigrationCommand,self).__init__(name)

        # Add file option to load
        self.parser.add_option('-u', '--url',
          action="store",
          type="string",
          dest="file_url",
          help="URL to be loaded by daemon")


    def command(self):
        '''
        Parse command line arguments and call appropriate method.
        '''
        if not self.args or self.args[0] in ['--help', '-h', 'help']:
            print datadaemonCommand.__doc__
            return

        cmd = self.args[0]
        self._load_config()

        if cmd == 'migrate':
            self.migrate()
        if cmd == 'commit':
            self.commit()
        if cmd == 'clean':
            self.clean()
        if cmd == 'execute':
            self.execute()
        else:
            log.error('Command "%s" not recognized' % (cmd,))

    def migrate(self):
        '''
        Migrate existing data on elastic search in a new index called lbdf. 
		It will also move all stored datasets to types inside lbdf index.
        '''
        # Load elastic search class
        es_instance = es.ESIntegration()

        # Create indice
        es_instance.indice = 'lbdf'
        es_instance.indice_create()

        # Create types
        es_instance.types_create()

        # Export documents
        es_instance.export_old_documents(ckan=True)

    def clean(self):
        """
        Remove lbdf index and all data inside it
        """
        # Load elastic search class
        es_instance = es.ESIntegration()

        # Remove indice
        es_instance.indice = 'lbdf'
        es_instance.indice_remove()

    def commit(self):
        """
        Remove old datasets stored as index on Elastic Search.
        CAUTION: this operation can't be undone.
        """
        es_instance = es.ESIntegration()

        indices = es_instance.indices(old=1)
        for index in indices:
            # Removing indexes
            print('Removing index %s' % index)
            es_instance.indice_remove(index)


    def execute(self):
        """
        Run datadaemon
        """
        if not self.options.file_url:
            log.error('You have to supply URL for this option')
            return

        # You have to supply files full path
        d = datadaemon.datadaemon()

        # Have to supply file in URL format
        d.file_url = self.options.file_url

        # Execute it
        d.run()
