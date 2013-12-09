#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Created on 1 Dec 2013

@author: jyp

 
'''

from distutils.core import setup

setup(
    name='pyodbcOpenEdge',
    version='1.0',
    description='A Django Backend for OpenEdge databases (Progress)',
    long_description='''
This module allows the use of the OpenEdge (Progress) databases through Django. It provides the ability to create tables from Django models, or to access existing tables (4GL or SQL) in your Django application.
The South integration permits to migrate the database schema.
''',
    author='Jean-Yves Priou',
    author_email='jy.priou@free.fr',
    platforms="Independent",
    url='http://www.jypriou.fr/',
    packages=['OpenEdge','OpenEdge.OEmodels','OpenEdge.pyodbc','OpenEdge.south'],
    license="MIT",
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Django',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: DataBases :: Backends :: Django',
        'Topic :: Software Development :: Libraries',
        'Topic :: Utilities'
    ],
    keywords='django openedge progress',
)