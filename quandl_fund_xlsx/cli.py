# -*- coding: utf-8 -*-
"""quandl_fund_xlsx

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
  -d --database <database>    Sharadar Fundamentals database to use, SFO (aka Sample Data) or
                              SF1 [default: SF0]
  --dimension <dimension>     Sharadar database dimension, ARY, MRY, ART, MRT, MRQ, ARQ
                              [default: MRY]

  --version             Show version.

"""
# the imports have to be under the docstring
# otherwise the docopt module does not work.
from docopt import docopt
from .fundamentals import stock_xlsx
import pathlib
import sys


def main(args=None):
    arguments = docopt(__doc__, version="version='0.4.1'")
    print(arguments)

    file = arguments["--input"]

    tickers = []
    if file is None:
        ticker = arguments["--ticker"]
        print("Ticker =", ticker)
        tickers.append(ticker)
    else:
        with open(file) as t_file:
            for line in t_file:  # Each line contains a ticker
                tickers.append(line.strip())
                print("Ticker =", line)

    years = arguments["--years"]
    years = int(years)

    outfile = arguments["--output"]
    database = arguments["--database"]
    dimension = arguments["--dimension"]

    path = pathlib.Path(outfile)
    if path.exists():
        print("Output file {} exists do you wish to replace it? (y/n)".format(outfile))
        response = input("Overwrite {} (y/n)".format(outfile)).lower()
        if response != "y":
            print("You replied {}, Exiting".format(response))
            sys.exit()

    print("Output will be written to {}".format(outfile))
    #  stock_xlsx(outfile, tickers, database, dimension, years)
    stock_xlsx(outfile, tickers, database, dimension, years)


if __name__ == "__main__":
    main()
