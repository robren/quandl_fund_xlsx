#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_quandl_fund_xlsx
---------------------

Tests for `quandl_fund_xlsx` module.

Mainly mock and very simple object instantiation, should act as a smoke test
to ensure that that imports work in an installed package.

When we use travis-ci.org the Quandl API token can be set in our environment
allowing the actual Sharadar data set to be used.

"""

import pandas as pd
import pytest
import sys
import os
import pathlib
import uuid
from flaky import flaky


# Grrr  overcoming python import drama!
parentddir = os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))
sys.path.append(parentddir)


from quandl_fund_xlsx import fundamentals as fun


test_tmp_dir = "./tests/test_tmp_dir"


def setup_module(module):
    print("\nsetup_module      module:%s" % module.__name__)
    if not os.path.exists(test_tmp_dir):
        os.mkdir(test_tmp_dir)


def teardown_module(module):
    print("\nteardown_module   module:%s" % module.__name__)


def setup_function(function):
    print("\nsetup_function      function:%s" % function.__name__)


def test_fund_SF0_db_init():
    print(sys.path)
    f = fun.SharadarFundamentals("SF0" )
    assert f.database == "SF0"


def test_fund_SF1_db_init():
    print(sys.path)
    f = fun.SharadarFundamentals("SF1")
    assert f.database == "SF1"


# When using the SF0 KEY, quandl sometimes complains abou accessing the API
# too frequently
@flaky(max_runs=3)
def test_fund_SF0_retrieve_one_MRY():
    global test_tmp_dir
    if "QUANDL_API_SF0_KEY" not in os.environ:
        pytest.skip("QUANDL_API_SFO_KEY not set in the environment")
    print(sys.path)
    outfile = test_tmp_dir + "/" + str(uuid.uuid4()) + ".xlsx"
    path = pathlib.Path(outfile)
    assert path.exists() == False
    fun.stock_xlsx(outfile, ["AAPL"], "SF0", "MRY", 5)
    assert path.exists() == True
    path.unlink()


def test_fund_SF1_retrieve_one_MRY():
    global test_tmp_dir
    if "QUANDL_API_SF1_KEY" not in os.environ:
        pytest.skip("QUANDL_API_SF1_KEY not set in the environment")
    print(sys.path)
    outfile = test_tmp_dir + "/" + str(uuid.uuid4()) + ".xlsx"
    path = pathlib.Path(outfile)
    assert path.exists() == False
    fun.stock_xlsx(outfile, ["AAPL"], "SF1", "MRY", 5)
    assert path.exists() == True
    path.unlink()

def test_fund_SF1_retrieve_one_MRT():
    global test_tmp_dir
    if "QUANDL_API_SF1_KEY" not in os.environ:
        pytest.skip("QUANDL_API_SF1_KEY not set in the environment")
    print(sys.path)
    outfile = test_tmp_dir + "/" + str(uuid.uuid4()) + ".xlsx"
    path = pathlib.Path(outfile)
    assert path.exists() == False
    fun.stock_xlsx(outfile, ["MSFT"], "SF1", "MRT", 10)
    assert path.exists() == True
    path.unlink()
