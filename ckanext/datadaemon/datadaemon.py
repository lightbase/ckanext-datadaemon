# -*- coding: UTF-8 -*-
import os
import requests
import urlparse
import json
from logging import getLogger
#from pylons import request
from pylons import config
from rdflib.graph import Graph
from ckan import plugins
from ckan.logic import get_action
from ckan import model
from paste.script.util.logging_config import fileConfig
import urllib
import ckanclient
from ckanclient.loaders import util
import traceback

from ckanext.lightbase import rdf
#from daemon import Daemon
from datastore.client import *

# Import model
from ckanext.datadaemon.model import setup as setup_model
from ckanext.datadaemon.model import DataRepository, ErrorRepository

log = getLogger(__name__)

# FIXME: Maneira errada de carregar, mas a única que funcionou
import ConfigParser
config = ConfigParser.ConfigParser()

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



#class datadaemon(Daemon):
class datadaemon:
    """ 
    Create a daemon to extract data from another Ckan instance
    """
    # Basic configuration
    
    def __init__(self):
        """
        Building method with standard parameters
        """
        #self.package_id = 'b94fb472-59da-4c96-84dc-0919c0f3df36'
        self.package_name = 'pareceres'
        self.repository = config.get('app:main','ckanext.datadaemon.repository')
        self.files = None

    #def _load_config(self):

    #    from paste.deploy import appconfig
    #    from ckan.config.environment import load_environment

        #self.filename = os.path.abspath('/srv/ckan/py2env/src/ckan/development.ini')
        #if not os.path.exists(self.filename):
        #    raise AssertionError('Config filename %r does not exist.' % self.filename)
        #fileConfig(self.filename)
        #conf = appconfig('config:' + self.filename)
        #load_environment(conf.global_conf, conf.local_conf)

    def log(self, message):
        """
        Log message time
        """
        from datetime import datetime
        import sys
        date = datetime.today()
        message = date.__str__() + ' : ' +  message
        print >> sys.stderr, message

    def run(self):
        """
        This proc will download the data file and keep a record about all
        downloaded files
        """
        # Etapas de execucao:
        # 1 - Baixar o arquivo com os dados

        # First we will need a file containing a list of files
        self.log('Loading file list %s' % self.files)

        # Check if we are forcing some URL
        if self.files is None:
            #log.error('You have to supply the file list. \nFile list: %s' % self.files)
            self.log( 'You have to supply the file list. \nFile list: %s' % self.files)
            return

        for linha in self.files:
            #log.warning('Loading file %s' % linha.rstrip())
            self.log( 'Loading file %s' % linha.rstrip())
            self.response = self.send(linha.rstrip())
        
            # Store the file and its hash somewhere
            exists = self.hash_control()
            if exists:
                # if The hash exists, abort operation
                continue

            # 2 - Fazer o parsing do arquivo e armazenar cada registro como um
            # recurso no Ckan em formato RDF. Os campos devem ser armazenados como
            # metadados do recurso
            r = rdf.lightbaseParser()

            # Tenta abrir o arquivo e fazer o parsing. Se houver erro rejeita e loga
            try:
                # Deve retornar uma coleção de registros para armazenamento
                registros = r.collection(self.response)
            except:
                # Armazena os arquivos que não foram importados com sucesso
                file_dict = {
                                'tmpfile': self.response,
                                'filename': self.response,
                                'errmsg' : traceback.format_exc(),
                                'error_type' : 'FileCollectionError',
                                'package_file' : self.response
                }
                self.log_error(file_dict)

            for registro in registros.get('registros'):
                self.datastore(registro['base_name'],registro['rdf_identifier'],registro['rdf_collection'],registro['metadata'],'rdf')
                #print('2222222222222222222222222222: %s' % registro['rdf_identifier'])

            # Log import errors
            for registro in registros['import_error']:
                self.log_error(registro)
                #print('111111111111111111111111111111: %s' % registro)

    def send(self, url):
        # Primeiro campo: URL. Segundo campo: parametros da requisicao

        # This will copy the remote URL to local file
        filename, headers=urllib.urlretrieve(url)

        return filename
        
    def datastore(self, package_name, identifier, data, metadata, format):
        """
        This method records the parsed data on database
        """
        # Dados de configuracao para gravacao do registro
        #self._load_config()

        # If we don't find the required field, just ignore it
        if metadata.get('grau_sigilo') is None:
            # If we don't find it, return
            error_dict = { 'tmpfile' : metadata['url'],
                           'filename' : metadata['arq_original'],
                           'errmsg' : u'Não há informação sobre o grau de sigilo do documento',
                           'error_type' : 'NotPublicError',
                           'package_file' : self.response
                            }
            self.log('Arquivo com grau de sigilo %s. Não ajustado ou não encontrado!' % metadata['grau_sigilo'])
            self.log_error(error_dict)
            return
        
        metadata['grau_sigilo'] = self.normalize_name(metadata['grau_sigilo'])

        # Now we import only publico
        if metadata['grau_sigilo'] != 'publico':
            # We only import publico
            error_dict = { 'tmpfile' : metadata['url'],
                           'filename' : metadata['arq_original'],
                           'errmsg' : 'O nome do campo grau de sigilo não foi informado como público: %s' % metadata['grau_sigilo'],
                           'error_type' : 'NotPublicError',
                           'package_file' : self.response
                            }
            self.log('Arquivo com grau de sigilo %s. Não importar!' % metadata['grau_sigilo'])
            self.log_error(error_dict)
            return

        # make sure this package exists
        self.package_name = self.normalize_name(package_name)

        # Grava um registro de cada vez
        #log.warning('Gravando registro %s no datastore' % (identifier))
        self.log( 'Gravando registro %s no datastore' % (identifier))

        # Get API user and website domain data
        user = get_action('get_site_user')({'model': model, 'ignore_auth': True}, {})
        ckan_url = config.get('app:main','ckan.site_url').rstrip('/')

        # Elastic search URL
        webstore_request_url = '%s/api/data/%s/%s' % (ckan_url, self.package_name, identifier)
        client = DataStoreClient(webstore_request_url)

        # Ckan client data and api URL
        api_url = urlparse.urljoin(config.get('app:main','ckan.site_url'), 'api')
        ckan = ckanclient.CkanClient(
                                     base_location=api_url,
                                     api_key=user.get('apikey'),
                                     is_verbose=True,
                                    )
        
        # If some of this item fails, there's an error in file
        # Cancel execution and log error
        hits = dict()
        try:
            # Search the string on elastic search
            q = {
                     "query": {
                                "query_string": {
                                                 "default_field" : "id",
                                                 "query": identifier
                                                 }
                               }
            }

            package_entity = ckan.package_entity_get(self.package_name)
            response = client.query(q)
            hits = response['hits']
        except:
            #log.error('Error in file probably because base_name %s doesn\'t exists. Logging error\n%s' % (self.package_name, traceback.format_exc()))
            errmsg = 'Error in file probably because base_name %s doesn\'t exists. Logging error\n%s' % (self.package_name, traceback.format_exc())
            hits['total'] = 0

            # Regular log
            self.log(errmsg)

            # Generate error log information and store it
            file_dict = {
                            'tmpfile': self.send(metadata.get('url')),
                            'filename': metadata.get('url'),
                            'errmsg' : errmsg,
                            'error_type' : 'DatasetNameError',
                            'package_file' : self.response
            }

            self.log_error(file_dict)

            return

        #response = client.query(q)

        # Se o registro existe, apaga e insere de novo
        if hits.get('total') > 0:
            self.delete(identifier)
            # Now remove file from filesystem
            for hits in hits.get("hits"):
                elastic_data = hits["_source"]

                # Remove file if it exists
                if elastic_data.get('url') is not None and elastic_data.get('url'):
                    self.log('Removing file |%s|' % elastic_data.get('url'))
                    result = util.delete_file(ckan,elastic_data.get('url'))

        # Make sure we don't have invalid chars on file name
        # FIXME: put original file name in lb RDF namespace
        arq_original, extension = os.path.splitext(metadata['arq_original'])
        metadata['arq_original'] = self.normalize_name(arq_original) + extension

        # Store the original file in repository creating a fixed URL
        metadata['url'] = self.store_file(ckan,metadata['url'],metadata['arq_original'])

        # Convert data to Json format
        #print('Inserindo dados %s' % json.dumps(metadata, ensure_ascii=False))
        metadata['id'] = identifier
        post_data = json.dumps(metadata, ensure_ascii=False)
        headers = dict()
        headers['Authorization'] = user.get('apikey')
        #req = urllib2.Request(webstore_request_url, post_data, headers)
        client._headers = headers
        list_data = list()
        list_data.append(post_data)

        # Insert data on Elastic Search
        self.response = client.upsert(list_data)
        
        # This data must be set somehow
        #package_name = self.package_id
        package_name = self.package_name

        # Insert on Ckan RDF data as extra attribute
        metadata['rdf_collection'] = data

        # Describe  
        resource_entity = { 
                           'name' : identifier,
                           'url' : metadata['url'],
                           'format' : format,
                           'extras' : metadata
        }
        
        #package_entity = ckan.package_entity_get(package_name)
        
        # Add resource as an element for package. It's the only way to save
        package_entity['resources'].append(resource_entity)
        
        # This will update the package in package_entity
        ckan.package_entity_put(package_entity)
        
        #print('Request result: %s' % self.response)
        #log.warning('Registro %s gravado com sucesso' % (identifier))
        self.log( 'Registro %s gravado com sucesso' % (identifier))
        
    def delete(self,identifier):
        """
        Remove resource from datastore
        """
        # Fix identifier because SQlAlchemy can't parse RDF Literals
        identifier = str(identifier)

        #self._load_config()
        self.log( 'Removing resource %s' % identifier)

        # Authenticate and remove resource from system 
        user = get_action('get_site_user')({'model': model, 'ignore_auth': True}, {})
        api_url = urlparse.urljoin(config.get('app:main','ckan.site_url'), 'api')

        ckan = ckanclient.CkanClient(
                                     base_location=api_url,
                                     api_key=user.get('apikey'),
                                     is_verbose=True,
                                    )
        
        # This data must be set somehow
        #package_name = self.package_id
        #package_name = self.package_name
        package_entity = ckan.package_entity_get(self.package_name)

        # FIXME: The only way is to loop through resources list
        # It's incredibly slow. Have to fix it

        resources_list = list()
        for resource in package_entity['resources']:
            if resource['name'] != identifier:
                # On this case add this resource to the list
                resources_list.append(resource)


        # Add resource as an element for package. It's the only way to save
        package_entity['resources'] = resources_list
        
        # This will update the package in package_entity
        ckan.package_entity_put(package_entity)
        
        # Remove um registro de cada vez
        #log.warning('Removendo registro %s do datastore' % (identifier))
        self.log( 'Removendo registro %s do datastore' % (identifier))
        
        ckan_url = config.get('app:main','ckan.site_url').rstrip('/')
        url = '%s/api/data/%s/%s' % (ckan_url, self.package_name, identifier)
        client = DataStoreClient(url)
        out = client.delete()
        data = json.loads(out)

        # Now remove resource from Ckan DB
        session = model.Session
        
        # First remove revisions
        revision_query = session.query(model.ResourceRevision)\
            .filter(model.ResourceRevision.name == identifier).all()

        for revision_obj in revision_query:
            session.delete(revision_obj)

        # We have to commit it before remove resource itself
        session.commit()

        # Now remove resources
        resource_query = session.query(model.Resource)\
            .filter(model.Resource.name == identifier)

        resource_obj = resource_query.first()
        session.delete(resource_obj)

        # Now we can safelly remove resource
        session.commit()

        #log.warning('Registro %s removido com sucesso' % identifier)
        self.log( 'Registro %s removido com sucesso' % identifier)

        return data

    def hash_control(self):
        """This method will create a table that keeps control about all the
        system uploaded data
        """
        import hashlib
        from datetime import datetime
        import shutil

        # Generate hash with file content
        h = hashlib.md5()
        f = open(self.response, 'r')
        h.update(f.read())

        # Copy file to repository
        session = model.Session
        #metadata = model.metadata

        # Create table if it doesn't exists
        setup_model()

        # First check if hash is already in database
        results = session.query(DataRepository.hash).filter_by(hash=h.hexdigest()).all()
        #self.log(results)

        if len(results) > 0:
            #log.error('This file %s has the same hash of a file already in\
            #    database. Aborting' % self.response)
            self.log( 'This file %s has the same hash of a file already in\
                database. Aborting' % self.response)
            os.remove(self.response)
            return True

        # Today's date
        file_date = datetime.today()

        # Filename hash to store
        filename, extension = os.path.splitext(os.path.basename(self.response))
        h2 = hashlib.md5()
        h2.update(file_date.__str__() + filename)
        filename = h2.hexdigest() + extension

        # Now add full repository path to filename
        filename2 = os.path.join(self.repository,filename)

        # Now insert data and copy file to repository
        #log.warning('Inserting file %s in repository' % self.response)
        self.log('Inserting file %s in repository' % self.response)

        # Copy file to repository
        shutil.copy2(self.response,filename2)

        # insert info in database
        repository = DataRepository(hash=h.hexdigest(), creation_date=file_date.today(), original_file = filename2, package_file=self.response)
        session.add(repository)
        session.commit()

        #log.warning('File inserted')
        self.log('File inserted')

        # Remove other file
        os.remove(self.response)

        self.response = filename2

        return False

    def store_file(self,client,url,orig_filename):
        """
        Store the file in a local repository
        """
        # Get rep_path to copy file
        try:
            filename, headers=urllib.urlretrieve(url)
        except:
            # if we fail here returns a None object
            self.log('File retrieval error for file %s' % url)
            return ''

        # Fix filename encoding
        udata=orig_filename.decode("utf-8")
        orig_filename=udata.encode("ascii","ignore")

        # Use ckanclient patch to upload file to storage
        url, msg=util.upload_file(client,filename,orig_filename)

        # Log error if True
        if msg is not '':
            #log.error('File upload error:\n %s' % msg)
            self.log( 'File upload error:\n %s' % msg)
            return None

        return url

    def log_error(self,file_dict):
        """
        Keep a repository of import errors in repository
        """
        import hashlib
        from datetime import datetime
        import shutil

        # Today's date
        file_date = datetime.today()

        # Generate hash with file content
        h = hashlib.md5()
        f = open(file_dict['tmpfile'], 'r')
        h.update(f.read() + file_date.__str__())
        f.close()

        # Copy file to repository
        session = model.Session

        # Create table if it doesn't exists
        setup_model()

        # First check if hash is already in database
        results = session.query(ErrorRepository.hash).filter_by(hash=h.hexdigest()).all()

        if len(results) > 0:
            #log.error('This file %s has the same hash of a file already in\
            #    database. Aborting' % file_dict['filename'])
            self.log( 'This file %s has the same hash of a file already in\
                database. Aborting' % file_dict['filename'])
            os.remove(file_dict['tmpfile'])
            return

        # Filename hash to store
        filename3, extension = os.path.splitext(os.path.basename(file_dict['filename']))
        filename3 = file_date.__str__() + '-' + filename3 + extension

        # Now add full repository path to filename
        filename2 =  os.path.join(self.repository,os.path.join('import_errors',filename3.replace(' ', '-')))

        # Now insert data and copy file to repository
        #log.error('Error parsing file %s. Inserting in repository' % file_dict['filename'])
        self.log('Error in file %s. Inserting in repository with message\n %s' % (file_dict['filename'],file_dict.get('errmsg')))

        # Create base dir if it doesn't exist
        if not os.path.exists(os.path.join(self.repository,'import_errors')):
            os.mkdir(os.path.join(self.repository,'import_errors'), 0770)

        # Copy file to repository
        shutil.copy2(file_dict['tmpfile'],filename2)

        # insert info in database
        repository = ErrorRepository(
          hash=h.hexdigest(),
          creation_date=file_date.today(),
          original_file=filename2,
          errmsg=file_dict.get('errmsg'),
          error_type=file_dict.get('error_type'),
          package_file=file_dict.get('package_file')
        )
        session.add(repository)
        session.commit()

        #log.warning('File inserted')
        self.log('File inserted')

        # Remove other file
        os.remove(file_dict['tmpfile'])

    def normalize_name(self, value):
        """
        Normalizes string, converts to lowercase, removes non-alpha characters,
        and converts spaces to hyphens.

        Copiado de http://stackoverflow.com/questions/295135/turn-a-string-into-a-valid-filename-in-python
        """
        import unicodedata
        import re

        self.log('Converting string %s' % value)
        
        # Double try in name conversion
        try:
            value = unicodedata.normalize('NFKD', u'%s' % value).encode('ascii', 'ignore')
            value = unicode(re.sub('[^\w\s-]', '', value).strip().lower())
            value = re.sub('[-\s]+', '-', value)
        except:
            self.log('Conversion error: \n%s' % traceback.format_exc())

            value = unicode(value, 'ascii', errors='ignore')
            value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore')
            value = unicode(re.sub('[^\w\s-]', '', value).strip().lower())
            value = re.sub('[-\s]+', '-', value)


        self.log('Conversion finished to %s' % value)

        return value
