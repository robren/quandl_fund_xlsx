================
quandl_fund_xlsx
================


.. image:: https://img.shields.io/pypi/v/quandl_fund_xlsx.svg
        :target: https://pypi.python.org/pypi/quandl_fund_xlsx

.. image:: https://img.shields.io/travis/robren/quandl_fund_xlsx.svg
        :target: https://travis-ci.org/robren/quandl_fund_xlsx

.. image:: https://readthedocs.org/projects/quandl_fund_xlsx/badge/?version=latest
        :target: https://quandl_fund_xlsx.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status

.. image:: https://pyup.io/repos/github/robren/quandl_fund_xlsx/shield.svg
     :target: https://pyup.io/repos/github/robren/quandl_fund_xlsx/
     :alt: Updates


A unofficial CLI tool which uses the Quandl API and the Sharadar Essential Fundamentals
Database to extract financial fundamentals, Sharadar provided ratios as
well as calculate additional ratios.  Results are
written to an Excel Workbook with a separate worksheet per ticker analysed.

Read the file called LICENCE and pay special attention to the terms of the
Apache 2.0 license.

* Free software: Apache Software License 2.0
* Documentation: https://quandl_fund_xlsx.readthedocs.io.


Features
--------

For a given ticker, fundamental data is obtained using the Quandl API and the
Sharadar Fundamentals database. This data is then used to calculate various
useful, financial ratios. The ratios provide profitability indicators, a
number of financial leverage indicators providing a sense of  the amount of
debt a company has on it's balance sheet as well as its ability to service
it's debt and pay a dividend.

Some REIT specific ratios  such as FFO and AFFO are roughly approximated.
These specific ratios are only roughly approximated since certain data, namely
Real estate sales data for the period does not appear to be available via the
API.


Within each ticker's excel worksheets it's  divided into three main areas:

- Quandl statement indicators. This is data obtained from the three main
  financial statements; the Income Statement, the Balance Sheet and the Cash Flow
  Statement. 

- Quandl Metrics and Ratio Indicators. These are quandl provided financial ratios.

- Calculated Metrics and Ratios. These are calculated by the package from the
  Sharadars data provided and tabulated by the statement indicators and the
  'Metrics and Ratio' indicators.

The python Quandl API provides the ability to return data within python pandas
dataframes. This makes calculating various ratios as simple as dividing two
variables by each other.

The calculations support the data offered by the free `SF0
<https://www.quandl.com/data/SF0-Free-US-Fundamentals-Data/documentation/about#indicators>`_
database, and the paid for `SF1
<https://www.quandl.com/data/SF1-Core-US-Fundamentals-Data/documentation/dimensions>`_
database, a richer set of data is available as well as a larger coverage
universe of stocks is supported by the paid SF1 database.

.. figure:: snip.png

    The generated Excel workbook with one sheet per ticker.

Installation 
------------

.. code:: bash

    pip install quandl_fund_xlsx

Usage:
------
.. code:: bash

	quandl_fund_xlsx -h
	quandl_fund_xlsx

	Usage:
	quandl_fund_xlsx (-i <ticker-file> | -t <ticker>) [-o <output-file>]
									[-y <years>] [-d <sharadar-db>]
									[--dimension <dimension>]

	quandl_fund_xlsx.py (-h | --help)
	quandl_fund_xlsx.py --version

	Options:
	-h --help             Show this screen.
	-i --input <file>     File containing one ticker per line
	-t --ticker <ticker>  Ticker symbol
	-o --output <file>    Output file [default: stocks.xlsx]
	-y --years <years>    How many years of results (max 7 with SF0) [default: 5]
	-d --database <database>    Sharadar Fundamentals database to use, SFO or
								SF1 [default: SF0]
	--dimension <dimension>     Sharadar database dimension, MRY, MRT, ART [default: MRY]
	--version             Show version.


.. code:: bash

	quandl_fund_xlsx -t INTC -o excel_files/intc.xlsx
	{'--database': 'SF0',
	'--input': None,
	'--output': 'excel_files/intc.xlsx',
	'--ticker': 'INTC',
	'--years': '5'}
	('Ticker =', 'INTC')
	2017-08-22 06:08:59,751 INFO     Processing the stock INTC
	2017-08-22 06:09:06,012 INFO     Processed the stock INTC

	ls -lh excel_files
	total 12K
	-rw-rw-r-- 1 test test 8.7K Aug 22 06:09 intc.xlsx

Credits
---------

This packge was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage

