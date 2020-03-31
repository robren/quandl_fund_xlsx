=======
History
=======

0.1.1 (2017-08-31)
------------------

* First release on PyPI.

0.1.2 (2017-08-31)
------------------
* Change logging to INFO from DEBUG

0.1.3 (2017-08-31)
------------------
* Minor tweak to Return the correct version

0.1.4 (2017-11-06)
------------------
* Removed the --dimension CLI keyword.
  Now uses Most Recent Year (MRY) for SF0 database
  and Most Recent Trailing 12 Months (MRT) for the SF1 database
* Fix to avoid the Pandas future warning about decrementing
  df.rename_axis and using df.rename

0.1.6 (2018-01-26)
-------------------
Now uses the get_table methods from the quandl_api.

0.1.7 (2018-05-10)
-------------------
* Fix bug where the dataframe returned from quandl qas not being sorted
* Added EPS and EPS diluted.

0.1.8 ( 2018-05-24)
-------------------
* Fix bug where the SF0 subscription data was not being returned.
* With the discontinuation of the Sharadar Time series API at the end of March
  2018, the codes for the free fundamental subscription SF0 database changed.
  Subscribers to the SF0 data now use the SHARADAR/SF1 code in the get_table
  accesses.

0.1.9 ( 2018-06-11)
-------------------
* Added back support for the --dimension CLI option.

0.1.10 (2018-10-29)
-------------------
* Added some  new Cash Flow related ratios and corrected the LTDEBT ratios
* Changed the default to be the paid SF1 Database as this is the one I'm using
  and testing. Requires a separate free SF0 subscription to test SFO. All of
  the API calls whether the user has an SFI paid membership or SF0 use the
  SF1 codes.


0.2.0 (2018-11-13)
-------------------
* After learning that the sample data API now allows _all_ of the same
  indicators as those available using the paid SF! aPI key I was able to
  remove a lot of special case code for the Sample data KEY.
  The paid KEY allows for many more dimensions to be queried.
* The CLI now defaults back to using the sample data SF0 API key.
* Added a number of Cash Flow from Operations  based metrics as well as some
  Free Cash Flow based metrics.
* Added a development test which uses the API and a sample data or SF0 API key
  to extract ratios for AAPL.
* Added Excess Cash Margin ratio.

0.2.1 (2018-11-13)
-------------------
* Minor security fix, requests version now >=2.20.0
*  Minor documentation cleanup


0.2.2 (2018-11-13)
-------------------
* Add support for the MRQ and ARQ dimensions.
* Correct error in calculating CAGR when the data was given in quarterly increments.
* Correctly reference the Excel spreadsheet example figures in the README.

0.2.3 (2018-12-29)
-------------------
* Check for the presence of the QUANDL_API_SF0_KEY or the QUANDL_API_SF1_KEY
  environment variable  depending on which database the user is requesting to use.


0.3.0 (2019-09-12)
------------------
* Refactored by using and manipulating  the pandas dataframe as it'a returned from
  quandl/Sharadar. The dates are rows and the columns are the "observations"
  ie the revenue, income etc. The dataframe is transposed prior to writing to
  excel so that the data is in the typically viewed format of dates as columns
  and the observations as rows.

0.3.1 (2019-11-11)
------------------
* Added some metrics favored by Kenneth J Marshall, author of
  "Good Stocks Cheap: Value investing with confidence for a lifetime of
  Stock Market Outperformance"

0.3.1 (2020-03-31)
------------------
* Added the working capital value from the balance sheet
