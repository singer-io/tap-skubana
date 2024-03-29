#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='tap-skubana',
      version='0.0.3',
      description='Singer.io tap for extracting data from the Skubana API',
      author='scott.coleman@bytecode.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_skubana'],
      install_requires=[
          'singer-python==5.10.0',
          'backoff==1.8.0',
          'ratelimit==2.2.1',
          'requests==2.23.0',
          'pyhumps==1.6.1'
      ],
      extras_require={
          'dev': [
              'pylint',
              'ipdb',
              'nose',
          ]
      },
      python_requires='>=3.5.6',
      entry_points='''
          [console_scripts]
          tap-skubana=tap_skubana:main
      ''',
      packages=find_packages(),
      package_data={
          'tap_skubana': [
              'schemas/*.json'
          ]
      })
