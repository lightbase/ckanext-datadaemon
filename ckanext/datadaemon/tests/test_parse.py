# -*- coding: UTF-8 -*-
import os
import tempfile
from pylons import config
import unittest
import urllib
import pickle
from logging import getLogger
import urlparse
import shutil

from ckan import model
from ckan.lib.base import config
from ckan.logic import get_action
import ckanclient
from ckanclient.tests import test_ckanclient

from ckanext.datadaemon import datadaemon
from ckanext.datadaemon import es
from ckanext.lightbase import rdf

log = getLogger(__name__)

# Get Ckan client
client_test = test_ckanclient.TestCkanClient()


class datadaemonTest(unittest.TestCase):

    def setUp(self):
        """
        Create test necessary objects, opens database connections,
        find files, etc.
        """
        test_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static/test-zip/document.zip')

        # Copy it to tmp so we don't lose the file
        tmp_file = '/tmp/pareceres_zips.zip'
        shutil.copy2(test_file, tmp_file)

        # Add URL formatting to tile
        self.url_file = 'file://%s' % tmp_file

        class TestES(es.ESIntegration):
            # Overload ckanclient loading
            def load_ckanclient(self):
                """
                Load test instance
                """
                # Load ckanclient Test instance
                client_test.setup_class()

                return client_test.c

        # Setup ESIntegration test data
        self.es_instance = TestES()

        # Teste indice
        self.es_instance.indice = 'lbdf-teste'

        # Index and mappings create
        #response = self.es_instance.indice_create()
        self.es_instance.types_create()

        # Document in object
        test_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static/document.data')
        f = open(test_file, 'r')
        self.document = pickle.load(f)
        f.close()

        # Datadaemon instance
        self.d_instance = datadaemon.datadaemon()
        self.d_instance.es_instance.indice = 'lbdf-teste'

        # Dataset list from Ckan
        self.dataset_list = client_test.package_register_get()

    def tearDown(self):
        """
        Cleanup data after test
        """
        response = self.es_instance.indice_remove()
        client_test.teardown_class()

    def test_store_resource(self):
        """
        Store one file in LBDF format
        """
        response = self.d_instance.insert_resource(
          identifier=self.document['id'],
          metadata=self.document,
          dataset=self.dataset_list[0]
          )

    def test_delete_resource(self):
        """
        Remove one resource stored in LBDF format
        """
        response = self.d_instance.insert_resource(
          identifier=self.document['id'],
          metadata=self.document,
          dataset=self.dataset_list[0]
          )

        response = self.d_instance.delete(self.document['id'])

    def test_parsing(self):
        """
        Test opening and parsing LBDF data
        """
        self.d_instance.file_url = self.url_file
        self.d_instance.run()
