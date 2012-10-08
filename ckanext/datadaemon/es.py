# -*- coding: UTF-8 -*-
# Import tools
import urlparse
from paste.script.util.logging_config import fileConfig
from logging import getLogger
from datetime import datetime

# Import Ckan classes
from ckan import plugins
from ckan.logic import get_action
from ckan import model

# Import Ckan plugins
import ckanclient
from ckanclient.loaders import util
from datastore.client import *

# It must work
from ckan.lib.base import config

log = getLogger(__name__)

class ESIntegration:
    """
    Integration operations for elastic search and Ckan
    """

    def __init__(self):
        """
        Constructor parameters
        """
        # FIXME: Get this from configuration or somewhere else
        self.indice = 'lbdf'

    def load_ckanclient(self):
        """
        Loads a ckan client instance
        
        @returns a CkanClient instance
        """
        user = get_action('get_site_user')({'model': model, 'ignore_auth': True}, {})
        api_url = urlparse.urljoin(config.get('ckan.site_url'), 'api')
        ckan = ckanclient.CkanClient(
          base_location=api_url,
          api_key=user.get('apikey'),
          is_verbose=True,
        )

        return ckan

    def load_elastic_config(self, url=None):
        """
        Load elastic search configuration
        
        @param url: URL to add to elastic search instance
        
        @returns A DataStoreClient instance
        """
        user = get_action('get_site_user')({'model': model, 'ignore_auth': True}, {})
        ckan_url = config.get('ckan.site_url').rstrip('/')
        es_url = '%s/api/data/' % (ckan_url)


        # Add url if supplied
        if url:
            es_url = urlparse.urljoin(es_url,url)
        client = DataStoreClient(es_url)
        headers = dict()
        headers['Authorization'] = user.get('apikey')
        client._headers = headers

        return client

    def indice_create(self):
        """
        Create supplied indice
        """
        # TODO: Find some way user can supply these options
        options = {
          "settings": {
           "number_of_shards":"5",
           "number_of_replicas":"1",
			"analysis.analyzer.default.filter.0":"lowercase",
			"analysis.analyzer.default.filter.1":"asciifolding",
            "analysis.analyzer.default.tokenizer":"standard",
            "analysis.analyzer.default.type":"custom",
            "analysis.filter.pt_stemmer.type":"stemmer",
            "analysis.filter.pt_stemmer.name":"portuguese"
           }
        }

        # Create a Ckan instance with this URL
        client = self.load_elastic_config(self.indice)

		# Now create index
        response = client.index_create(options)

        return response

    def indice_remove(self, indice=None):
        """
        Remove supplied indice
        """
        # Create a Ckan instance with this URL
        if indice:
            client = self.load_elastic_config(indice)
        else:
            # If not supplied, remove default indice
            client = self.load_elastic_config(self.indice)

		# Now create index
        response = client.delete()

        return response

    def indices(self, old=None):
        """
        Get ES indexes for ckan datasets
        
        @param old: supplied if you want to see old mapping
        """
        ckanclient = self.load_ckanclient()
        package_list = ckanclient.package_register_get()
        client = self.load_elastic_config()
        es_mapping = client.mapping()
        out_list = list()
        for package in package_list:
            # Do we want old or new mapping
            if old:
                if package in es_mapping.keys():
                    # Add it to package out list
                    out_list.append(package)
            else:
                if es_mapping.get(self.indice):
                    # Look for the package as a type inside index
                    if package in es_mapping[self.indice].keys():
                        out_list.append(package)
                    else:
                        # indice not created. Complain about it
                        raise Exception('You have to create indice first. Execute indice_create')

        # Return list of indices
        return out_list

    def document_list(self, old=None):
        """
        Get document list

        @param old: if supplied look dataset as an index. Otherwise look for types inside index

        @returns dict with indice name as key and a list of documents inside it
        """
        if not old:
            raise Exception('It works only with old index. Elastic Search doesn\'t support list ids')
        indices = self.indices(old)

        # Now get a list of documents por indices
        client = self.load_elastic_config()
        es_mapping = client.mapping()
        result_dict = dict()
        for index in indices:
            # If this is supplied, look for dataset as an index
            documents = dict()
            for elm in es_mapping[index].keys():
                # Get this document info from elastic search
                es_url = '%s/%s' % (index, elm)
                client = self.load_elastic_config(es_url)
                query = {
                        "query": {
                                  "match_all":{}
                        },
                        "size":"1"
                }
                response = client.query(query)
                hits = response.get('hits')
                resources = dict()
                for res in hits['hits']:
                    # Store it in extras
                    resources = res['_source']
                    # Return only first item

                documents[elm] = resources

            result_dict[index] = documents

        return result_dict

    def check_indice(self, indice):
        """
        Check if supplied index exists

        @param indice: indice to be checked

        @return indice information or None object if not found
        """
        client = self.load_elastic_config(indice)

        try:
            response = client.settings()
            return response
        except:
            # If not found return none
            return None

    def types_create(self):
        """
        Create datasets as types inside Elastic Search
        """
        # Check if default indice is already created
        indice_info = self.check_indice(self.indice)

        if not indice_info:
            # Fail here
            raise Exception('Indice not created to store datasets as types. Create this indice first: %s' % self.indice)

        # Now try to create types
        ckanclient = self.load_ckanclient()
        package_list = ckanclient.package_register_get()
        for package in package_list:
            # FIXME: I'm hard-coding some properties. Have to remove it from here fast
            mapping = {
                "properties": {
                    "data" : {
                        "type" : "date",
                        "index" : "not_analyzed"
                     }

                 }
              }
            # Now create each one separatelly as a type
            client = self.load_elastic_config('%s/%s' % (self.indice, package))

            # send the request
            response = client.mapping_update(mapping)

        # Return last response
        return response

    def insert_document(self, data, dataset, indice=None, ckan=False, rdf_data=None):
        """
        Insert a Json document on elastic search

        @param data: JSON document data
        @param dataset: Ckan dataset that the documento belongs to
        @param indice: Indice to store document. Default to self.indice
        @param ckan: Add it to ckan as resource too?

        @return Elastic Search response
        """
        log.debug('Inserting document with Id %s' % data.get('id'))
        # Default value for indice
        if not indice:
            indice = self.indice

        # Put mapping URL
        es_url = '%s/%s' % (indice, dataset)

        # Add document ID field
        #es_url = '%s/%s' % (es_url, data.get('id'))
        client = self.load_elastic_config(es_url)

        # FIXME: Remove this hard coded option
        try:
            data['data'] =  datetime.strptime(data.get('data'), '%d/%m/%Y %H:%M:%S').isoformat()
        except:
            # Try converting in another format
            data['data'] =  datetime.strptime(data.get('data'), '%d/%m/%Y').isoformat()

        # Make sure document is inserted without enforcing ascii and using UTF-8
        es_data = json.dumps(data, ensure_ascii=False, encoding='UTF-8')

        # Send it to Elastic Search as a list
        list_data = list()
        list_data.append(es_data)
        response = client.upsert(list_data)

        if not ckan:
            return response
        else:
            log.debug('Adding document with ID %s as a resource to dataset %s in Ckan' % (data.get('id'), dataset))
            # Get package entity
            ckanclient = self.load_ckanclient()
            package_entity = ckanclient.package_entity_get(dataset)

            # Add RDF data as extras on Ckan
            data['rdf_collection'] = rdf_data

            # Add it to ckan as a resource
            resource_entity = {
              'name': data.get('id'),
              'url': data.get('url'),
              'format': 'JSON',
              'extras': data
              }

            package_entity['resources'].append(resource_entity)

            # Add it using ckanclient
            response = ckanclient.package_entity_put(package_entity)

            return response

    def export_old_documents(self, ckan=False):
        """
        Export all documents in old indexes

        @param ckan: Export it to ckan also? Default to False
        """

        # Get all documents
        result_dict = self.document_list(old=1)

        for indice in result_dict.keys():
            # Export al indexes
            for document in result_dict[indice].keys():
                # Export one index at a time
                log.debug('Exporting document %s' % document)
                self.insert_document(data=result_dict[indice][document], dataset=indice, ckan=ckan)


    def get_document(self, identifier, dataset):
        """
        Get document information from Elastic Search

        @params identifier: Document name on Elastic Search
                dataset: CKAN dataset as mapper on Elastic Search

        @return JSON information about the document or empty dict if not found
        """
        # Query for id field in element
        query = {
                "query": {
                          "query_string": {
                                "default_field" : "id",
                                "query": identifier
                           }
                },
                "size":"1"
        }

        # Load ES instance
        es_url = '%s/%s' % (self.indice, dataset)
        client = self.load_elastic_config(es_url)

        # Query it on Es
        response = client.query(query)

        # Now we have to parse the result back to package dict
        hits = response.get('hits')
        resources = dict()
        for res in hits['hits']:
            # Store it in extras
            resources = res['_source']
            # Return only first item
            break

        if resources:
            return resources
        else:
            # Return empty dict if it's not found
            return dict()

    def delete_document(self, identifier, dataset, indice=None):
        """
        Remove document from Ckan and elastic search
        """
        # Load ES instance
        if not indice:
            indice = self.indice
        es_url = '%s/%s/%s' % (indice, dataset, identifier)
        client = self.load_elastic_config(es_url)
        response = client.delete()

        # Get Ckanclient instance
        ckan = self.load_ckanclient()

        # This data must be set somehow
        package_entity = ckan.package_entity_get(dataset)

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
        response = ckan.package_entity_put(package_entity)

        # Now remove resource from Ckan DB
        session = model.Session
        
        # First remove revisions
        revision_query = session.query(model.ResourceRevision)\
            .filter(model.ResourceRevision.name == identifier).all()

        for revision_obj in revision_query:
            #session.delete(revision_obj)
            model.repo.purge_revision(revision_obj, leave_record=False)

        # We have to commit it before remove resource itself
        session.commit()

        # Now remove resources
        resource_query = session.query(model.Resource)\
            .filter(model.Resource.name == identifier)

        resource_obj = resource_query.first()
        session.delete(resource_obj)

        # Now we can safelly remove resource
        session.commit()

        return response
