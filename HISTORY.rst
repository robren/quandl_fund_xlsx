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


