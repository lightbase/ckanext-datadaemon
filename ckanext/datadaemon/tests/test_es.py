# -*- coding: UTF-8 -*-
import os.path
import urlparse
import unittest
import pickle

from ckanext.datadaemon import es
from ckanext.lightbase import rdf
from ckan.lib.base import config
from ckan.logic import get_action
from ckan import model
import ckanclient

class es_test(unittest.TestCase):
    """
    Test if interation with elastic search is working ok
    """
    def setUp(self):
        """
        Set up test data 
        """
        self.es_instance = es.ESIntegration()

        # Teste indice
        self.es_instance.indice = 'lbdf-teste'

        # Index create
        response = self.es_instance.indice_create()

        # Document in object
        test_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static/document.data')
        f = open(test_file, 'r')
        self.document = pickle.load(f)
        f.close()

        # Dataset list from Ckan
        user = get_action('get_site_user')({'model': model, 'ignore_auth': True}, {})
        api_url = urlparse.urljoin(config.get('ckan.site_url'), 'api')
        ckan = ckanclient.CkanClient(
          base_location=api_url,
          api_key=user.get('apikey'),
          is_verbose=True,
          )

        self.dataset_list = ckan.package_register_get()

    def test_index(self):
        """
        Try to retrieve index information for datasets
        """
        package_list = self.es_instance.indices('old')


    def test_all_documents(self):
        """
        Test retrieval all documents for all indexes
        """
        #document_list = self.es_instance.document_list('old')
        pass

    def test_creating_index_type(self):
        """
        Test creating types in elastic search for dataset
        """
        response = self.es_instance.types_create()

    def test_loading_document(self):
        """
        Test store one example document in Elastic Search using data types
        """
        # Create types first
        self.es_instance.types_create()

        # Try to insert it in first dataset
        dataset = self.dataset_list[0]
        response = self.es_instance.insert_document(
          data = self.document,
          dataset = dataset
        )

    def test_export_indexes(self):
        """
        Test exporting all indexes as a type on the new index
        """
        # Create types first
        self.es_instance.types_create()

        self.es_instance.export_old_documents()

    def test_get_document(self):
        """
        Get get document information from elastic search
        """
        # Create types first
        self.es_instance.types_create()

        # Try to insert it in first dataset
        dataset = self.dataset_list[0]
        response = self.es_instance.insert_document(
          data = self.document,
          dataset = dataset
        )

        # Now try to retrieve info from elastic search
        document = self.es_instance.get_document(dataset=dataset, identifier=self.document.get('id'))

    def test_delete_document(self):
        """
        Remove document from Ckan and elastic search
        """
        # Create types first
        self.es_instance.types_create()

        # Try to insert it in first dataset
        dataset = self.dataset_list[0]
        response = self.es_instance.insert_document(
          data = self.document,
          dataset = dataset
        )

        # now delete document
        response = self.es_instance.delete_document(self.document.get('id'),dataset)

    def tearDown(self):
        """
        Remove test data used with commands
        """
        response = self.es_instance.indice_remove()
