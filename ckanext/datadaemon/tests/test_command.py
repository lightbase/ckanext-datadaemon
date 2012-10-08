# -*- coding: UTF-8 -*-
import unittest
from .. import commands
from .. import datadaemon
from ckanext.ligtbase import rdf

class command_test(unittest.TestCase):
 	"""
	Test if commands are working ok
	"""
	def setUp(self):
	 	"""
		Set up test data for usage with commands
		"""
		pass

	def test_index(self):
	 	"""
		Try to retrieve index information for datasets
		"""

		pass

	def test_document_retrieval(self):
	 	"""
		Test retrieving one specific document from the supplied dataset
		"""

		pass

	def test_all_documents(self):
	 	"""
		Test retrieval all documents for all indexes
		"""

		pass
	
	def test_removing_index(self):
	 	"""
		Test removing one example index
		"""

		pass

	def test_creating_index_type(self):
	 	"""
		Test creating one type in elastic search for dataset
		"""

		pass

	def test_loading_document(self):
	 	"""
		Test store one example document in Elastic Search using data types
		"""

		pass

	def test_export_index(self):
	 	"""
		Test exporting one entire index as a type on the new index
		"""

		pass

	def tearDown(self):
	 	"""
		Remove test data used with commands
		"""
		pass
