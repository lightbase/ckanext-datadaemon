# -*- coding: UTF-8 -*-
from logging import getLogger

from sqlalchemy import event
from sqlalchemy import distinct
from sqlalchemy import Table
from sqlalchemy import Column
#from sqlalchemy import ForeignKey
#from sqlalchemy import types
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.orm import backref, relation

# FIXME: This is the only way to load datamodel.
# have to load the configuration without all this trouble

#from paste.deploy import appconfig
#from pylons import config
#from ckan.config.environment import load_environment
#import os

#filename = os.path.abspath('/srv/ckan/py2env/src/ckan/')
#conf = appconfig('config:development.ini', relative_to=filename)
#load_environment(conf.global_conf, conf.local_conf)

# Enf of configuration basic

# FIXME: Maneira errada de carregar, mas melhor use a anterior
import os
import ConfigParser
config = ConfigParser.ConfigParser()

#from ckan.lib.base import config
from ckan import model
#from ckan.model import Session
from ckan.model.meta import *
from ckan.model.domain_object import DomainObject
#from geoalchemy import *
#from geoalchemy.postgis import PGComparator

# Find config file
config_file = os.environ.get('CKAN_CONFIG')
if not config_file:
     config_file =  os.path.join(
          os.path.dirname(os.path.abspath(__file__)), '../../../ckan/development.ini')

config.read(config_file)

from sqlalchemy import create_engine
engine = create_engine(config.get('app:main','sqlalchemy.url'))
model.metadata.bind = engine
# Fim da segunda forma


log = getLogger(__name__)

__all__ = [
    'DataRepository', 'datadaemon_table',
    'ErrorRepository', 'error_table',
  ]


datadaemon_table = None
error_table = None

def setup():

    if datadaemon_table is None or error_table is None:
        define_tables()
        log.debug('Spatial tables defined in memory')

    if model.repo.are_tables_created():

        if not datadaemon_table.exists():
            try:
                datadaemon_table.create()
            except Exception,e:
                # Make sure the table does not remain incorrectly created
                # (eg without geom column or constraints)
                if datadaemon_table.exists():
                    Session.execute('DROP TABLE dt_files')
                    Session.commit()

                raise e

            log.debug('Datadaemon tables created')
        else:
            log.debug('Datadaemon tables already exist')
            # Future migrations go here

        if not error_table.exists():
            try:
                error_table.create()
            except Exception,e:
                # Make sure the table does not remain incorrectly created
                # (eg without geom column or constraints)
                if error_table.exists():
                    Session.execute('DROP TABLE dt_errors')
                    Session.commit()

                raise e

            log.debug('Datadaemon error tables created')
        else:
            log.debug('Datadaemon error tables already exist')
            # Future migrations go here

    else:
        log.debug('Datadaemon tables creation deferred')


class DataRepository(DomainObject):
    """
    The repository will hold information about every uploaded files
    """
    __tablename__ = 'dt_files'
    
    hash = Column(UnicodeText, primary_key=True,nullable=False)
    original_file = Column(UnicodeText,nullable=False)
    creation_date = Column(DateTime(timezone=True),server_default='now()')
    package_file = Column(UnicodeText)
    
    def __init__(self, hash, original_file, creation_date, package_file):
        self.hash = hash
        self.original_file = original_file
        self.creation_date = creation_date
        self.package_file = package_file
        
    def __repr__(self):
        return "<DataRepository('%s','%s', '%s', '%s')>" % (self.hash, self.original_file, self.creation_date, self.package_file)


class ErrorRepository(DomainObject):
    """
    Keep a repository with all import errors
    """
    __tablename__ = 'dt_errors'

    hash = Column(UnicodeText, primary_key=True,nullable=False)
    original_file = Column(UnicodeText,nullable=False)
    creation_date = Column(DateTime(timezone=True),server_default='now()')
    errmsg = Column(UnicodeText)
    error_type = Column(UnicodeText)
    package_file = Column(UnicodeText)
    
    def __init__(self, hash, original_file, creation_date, errmsg, error_type, package_file):
        self.hash = hash
        self.original_file = original_file
        self.creation_date = creation_date
        self.errmsg = errmsg
        self.error_type = error_type
        self.package_file = package_file
        
    def __repr__(self):
        return "<ErrorRepository('%s','%s', '%s', '%s', '%s', '%s')>" % (self.hash, self.original_file, self.creation_date, self.errmsg, self.error_type, self.package_file)


def define_tables():

    global error_table
    global datadaemon_table

    datadaemon_table = Table('dt_files', metadata,
                             Column('hash',UnicodeText, primary_key=True,nullable=False),
                             Column('original_file',UnicodeText,nullable=False),
                             Column('creation_date',DateTime(timezone=True),server_default='now()'),
                             Column('package_file',UnicodeText)
                             )

    mapper(DataRepository, datadaemon_table)

    error_table = Table('dt_errors', metadata,
                             Column('hash',UnicodeText, primary_key=True,nullable=False),
                             Column('original_file',UnicodeText,nullable=False),
                             Column('creation_date',DateTime(timezone=True),server_default='now()'),
                             Column('errmsg',UnicodeText),
                             Column('error_type',UnicodeText),
                             Column('package_file',UnicodeText)
                             )

    mapper(ErrorRepository, error_table)

