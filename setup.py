#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [
    'docopt>=0.6.0',
    'pandas',
    'quandl',
    'xlsxwriter',
    'mock'
]

test_requirements = [
    'pytest',
]

setup(
    name='quandl_fund_xlsx',
    version='0.1.8',
    description="A CLI tool which uses the Quandl Fundmentals API and writes results to Excel Spreadsheets.",
    long_description=readme + '\n\n' + history,
    author="Robert Rennison",
    author_email='rob@robren.net',
    url='https://github.com/robren/quandl_fund_xlsx',
    packages=[
        'quandl_fund_xlsx',
    ],
    package_dir={'quandl_fund_xlsx':
                 'quandl_fund_xlsx'},
    entry_points={
        'console_scripts': [
            'quandl_fund_xlsx=quandl_fund_xlsx.cli:main'
        ]
    },
    include_package_data=True,
    install_requires=requirements,
    license="Apache Software License 2.0",
    zip_safe=False,
    keywords='quandl_fund_xlsx quandl finance',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
    test_suite='tests',
    tests_require=test_requirements
)
