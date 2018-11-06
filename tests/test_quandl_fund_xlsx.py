#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_quandl_fund_xlsx
---------------------

Tests for `quandl_fund_xlsx` module.

Mainly mock and very simple object instantiation, should act as a smoke test
to ensure that that imports work in an installed package.

Cannot call the actual API since this will require exposing our quandl API
token!

"""

import pandas as pd
import pytest
import sys
#from mock import Mock
import os
import pathlib
import uuid


# Grrr  overcoming python import drama!
parentddir = os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))
sys.path.append(parentddir)


from  quandl_fund_xlsx import fundamentals as fun


# MAybe get rid of this mock portion
#def mock_SF0_fundamentals():
#    return Mock(spec=fun.SharadarFundamentals('SF0')

# New bit potenilly class SharadarFundamentals(Fundamentals):

#def test_mock_SF0_fundamentals(mock_SF0_fundamentals):
#    f = mock_SF0_fundamentals
#    stock = 'INTC'
#    periods = 7
#    f.get_indicators(stock, 'MRY', periods, "i_stmnt")
#    f.get_indicators.assert_called_with(stock, 'MRY', periods, "i_stmnt")

test_tmp_dir = './tests/test_tmp_dir'

def setup_module(module):
    print ("\nsetup_module      module:%s" % module.__name__)
    if not os.path.exists(test_tmp_dir):
        os.mkdir(test_tmp_dir)
 
def teardown_module(module):
    print ("\nteardown_module   module:%s" % module.__name__)
    
def setup_function(function):
    print ("\nsetup_function      function:%s" % function.__name__)
 

def test_fund_SF0_db_init():
    print(sys.path)
    writer = pd.ExcelWriter("",
                            engine='xlsxwriter',
                            date_format='d mmmm yyyy')
    f = fun.SharadarFundamentals('SF0',writer)
    assert f.database == "SF0"


def test_fund_SF1_db_init():
    print(sys.path)
    writer = pd.ExcelWriter("",
                            engine='xlsxwriter',
                            date_format='d mmmm yyyy')
    f = fun.SharadarFundamentals('SF1',writer)
    assert f.database == "SF1"

def test_fund_retreive_one():
    global test_tmp_dir
    if "QUANDL_API_SF0_KEY" not in os.environ:
        pytest.skip("QUANDL_API_SFO_KEY not set in the environment") 
    print(sys.path)
    writer = pd.ExcelWriter("",
                        engine='xlsxwriter',
                        date_format='d mmmm yyyy')
    outfile  = test_tmp_dir + '/' + str(uuid.uuid4()) + '.xlsx'
    path = pathlib.Path(outfile)
    assert(path.exists() == False)
    fun.stock_xlsx(outfile, ['AAPL'], "SF0", 'MRY', 5)
    assert(path.exists() == True)
    # TODO Remove the generated file

