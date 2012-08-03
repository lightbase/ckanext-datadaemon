from setuptools import setup, find_packages
import sys, os

version = '0.1'

setup(
	name='ckanext-datadaemon',
	version=version,
	description='datadaemon extension for customising CKAN',
	long_description='',
	classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
	keywords='',
	author='Eduardo Santos',
	author_email='eduardo.edusantos@gmail.com',
	url='',
	license='GPL 2.0',
	packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
	namespace_packages=['ckanext', 'ckanext.datadaemon'],
	include_package_data=True,
	zip_safe=False,
	install_requires=['pyinotify', 'ckanext-lightbase'],
	entry_points=\
	"""
        [ckan.plugins]
	    datadaemon=ckanext.datadaemon.plugin:datadaemonPlugin

	""",
)
