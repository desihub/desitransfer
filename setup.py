#!/usr/bin/env python
# Licensed under a 3-clause BSD style license - see LICENSE.rst
#
# Standard imports
#
import glob
import os
import sys
#
# setuptools' sdist command ignores MANIFEST.in
#
from distutils.command.sdist import sdist as DistutilsSdist
from setuptools import setup, find_packages
import desiutil.setup as ds
#
# Begin setup
#
setup_keywords = dict()
#
# THESE SETTINGS NEED TO BE CHANGED FOR EVERY PRODUCT.
#
setup_keywords['name'] = 'desitransfer'
setup_keywords['description'] = 'DESI data transfer infrastructure.'
setup_keywords['author'] = 'DESI Collaboration'
setup_keywords['author_email'] = 'desi-data@desi.lbl.gov'
setup_keywords['license'] = 'BSD'
setup_keywords['url'] = 'https://github.com/desihub/desitransfer'
#
# END OF SETTINGS THAT NEED TO BE CHANGED.
#
setup_keywords['version'] = ds.get_version(setup_keywords['name'])
#
# Use README.rst as long_description.
#
setup_keywords['long_description'] = ''
if os.path.exists('README.rst'):
    with open('README.rst') as readme:
        setup_keywords['long_description'] = readme.read()
#
# Set other keywords for the setup function.  These are automated, & should
# be left alone unless you are an expert.
#
# Treat everything in bin/ except *.rst as a script to be installed.
#
if os.path.isdir('bin'):
    setup_keywords['scripts'] = [fname for fname in glob.glob(os.path.join('bin', '*'))
        if not os.path.basename(fname).endswith('.rst')]
setup_keywords['provides'] = [setup_keywords['name']]
setup_keywords['python_requires'] = '>=3.5'
setup_keywords['zip_safe'] = False
setup_keywords['use_2to3'] = False
setup_keywords['packages'] = find_packages('py')
setup_keywords['package_dir'] = {'': 'py'}
setup_keywords['cmdclass'] = {'module_file': ds.DesiModule,
                              'version': ds.DesiVersion,
                              'test': ds.DesiTest,
                              # 'api': ds.DesiAPI,
                              'sdist': DistutilsSdist}
setup_keywords['test_suite']='{name}.test.{name}_test_suite'.format(**setup_keywords)
setup_keywords['classifiers'] = ['Development Status :: 4 - Beta',
                                 'Environment :: Console',
                                 'Intended Audience :: Science/Research',
                                 'License :: OSI Approved :: BSD License',
                                 'Operating System :: POSIX :: Linux',
                                 'Programming Language :: Python :: 3 :: Only',
                                 'Topic :: Scientific/Engineering :: Astronomy']
#
# Autogenerate command-line scripts.
#
# setup_keywords['entry_points'] = {'console_scripts':['desi_daily_transfer = desitransfer.daily:main',
#                                                      'desi_transfer_daemon = desitransfer.daemon:main',
#                                                      'desi_transfer_status = desitransfer.status:main']}
#
# Add internal data directories.
#
setup_keywords['package_data'] = {'desitransfer': ['data/*'],
                                  'desitransfer.test': ['t/*']}
#
# Run setup command.
#
setup(**setup_keywords)
