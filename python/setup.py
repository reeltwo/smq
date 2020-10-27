#!/usr/bin/env python3
# encoding: utf-8

from distutils.core import setup, Extension

pysmq_module = Extension('pysmq',
                         include_dirs=['../smq'],
                         libraries=['zmq', 'uuid', 'json-c'],
                         library_dirs=['../lib'],
                         sources = ['pysmq.c','../src/smq.c'])

setup(name='pysmq',
      version='1.0.0',
      description='Serial Message Queue',
      author='Reeltwo',
      url='https://github.com/reeltwo/smq',
      long_description='''
Python bindings for SMQ (Serial Message Queue)
''',
      ext_modules=[pysmq_module])