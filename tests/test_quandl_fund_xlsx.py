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

from quandl_fund_xlsx import fundamentals as fun
import pandas as pd
import pytest
import sys
from mock import Mock


@pytest.fixture
def mock_SF0_fundamentals():
    return Mock(spec=fun.SF0Fundamentals)


def test_mock_SF0_fundamentals(mock_SF0_fundamentals):
    f = mock_SF0_fundamentals
    stock = 'INTC'
    periods = 7
    f.get_indicators(stock, 'MRY', periods, "i_stmnt")
    f.get_indicators.assert_called_with(stock, 'MRY', periods, "i_stmnt")


def test_fund_SF0_db_init():
    print(sys.path)
    writer = pd.ExcelWriter("",
                            engine='xlsxwriter',
                            date_format='d mmmm yyyy')
    f = fun.SF0Fundamentals(writer)
    assert f.database == "SF0"


def test_fund_SF1_db_init():
    print(sys.path)
    writer = pd.ExcelWriter("",
                            engine='xlsxwriter',
                            date_format='d mmmm yyyy')
    f = fun.SF1Fundamentals(writer)
    assert f.database == "SF1"
