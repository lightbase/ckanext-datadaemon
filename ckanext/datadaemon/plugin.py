import os
from logging import getLogger

from pylons import request
from genshi.input import HTML
from genshi.filters.transform import Transformer

from ckan.plugins import implements, SingletonPlugin
from ckan.plugins import IConfigurable

log = getLogger(__name__)


class datadaemonPlugin(SingletonPlugin):
    """ 
    This plugin implements a daemon that gets a data source
    and insert into Ckan
    """

    implements(IConfigurable, inherit=True)

    def configure(self,config):
        """ 
        This IConfigurable implementation creates plugin configurations
        """
        self.dir = config.get('ckanext.datadaemon.dir', None)
        if self.dir is None:
            log.warn("No Data daemon directory found. Please,  \
                define 'ckanext.datadaemon.dir' in your .ini!")
            self.dir = ''

        self.repository = config.get('ckanext.datadaemon.repository', None)
        if self.repository is None:
            log.warn("No Data daemon repository found. Please,  \
                define 'ckanext.datadaemon.repository' in your .ini!")
            self.repository = ''
