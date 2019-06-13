# -*- coding: utf-8 -*-

"""This module provides functions to calculate fundamental ratios
for a stock potfolio.

The results are saved in an excel workbook with one sheet per stock
as well as a summary sheet

:copyright: (c) 2019 by Robert Rennison
:license: Apache 2, see LICENCE for more details

"""
import collections
import logging
import os
import pandas as pd
import quandl
import sys
from quandl.errors.quandl_error import (
    NotFoundError)
from xlsxwriter.utility import xl_range
from xlsxwriter.utility import xl_rowcol_to_cell
# from pdb import set_trace as bp

# Added this one line below  to get logging from the requests module,
# comment me out when done
#logging.basicConfig(level=logging.DEBUG)

logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

#logger.setLevel(logging.INFO)
logger.setLevel(logging.DEBUG)


class Fundamentals_ng(object):
    def __init__(self,
                 database,
                 i_ind,
                 cf_ind,
                 bal_ind,
                 metrics_and_ratios_ind,
                 calc_ratios,
                 writer):
        if (database == 'SF0') :
            if "QUANDL_API_SF0_KEY" in os.environ:
                quandl.ApiConfig.api_key = os.environ['QUANDL_API_SF0_KEY']
            else:
                print('Exiting: Please set the QUANDL_API_SF0_KEY environment variable.')
                sys.exit()
        elif (database == 'SF1') :
            if "QUANDL_API_SF1_KEY" in os.environ:
                quandl.ApiConfig.api_key = os.environ['QUANDL_API_SF1_KEY']
            else:
                print('Exiting Please set the QUANDL_API_SF1_KEY environment variable.')
                sys.exit()

        #self.database = 'SHARADAR/' + database
        self.database =  database
        self.all_inds_df = None

        self.i_stmnt_ind_dict = collections.OrderedDict(i_ind)
        self.i_stmnt_df = None
        self.cf_stmnt_ind_dict = collections.OrderedDict(cf_ind)
        self.cf_stmnt_df = None
        self.bal_stmnt_ind_dict = collections.OrderedDict(bal_ind)
        self.bal_stmnt_df = None

        self.metrics_and_ratios_ind_dict = \
            collections.OrderedDict(metrics_and_ratios_ind)
        self.metrics_and_ratios_df = None
        self.calc_ratios_dict = collections.OrderedDict(calc_ratios)
        self.calc_ratios_df = None
        self.dimension = None
        self.periods = None

        self.writer = writer
        self.workbook = writer.book
        self.format_bold = self.workbook.add_format()
        self.format_bold.set_bold()
        self.format_commas_2dec = self.workbook.add_format()
        self.format_commas_2dec.set_num_format('#,##0')
        self.format_commas = self.workbook.add_format()
        self.format_commas.set_num_format('0.00')
        self.format_justify = self.workbook.add_format()
        self.format_justify.set_align('justify')

        # Add all the other functions. Radically simplified

    def get_indicators(self, ticker, dimension, periods):
        """Obtains fundamental company indicators from the Quandl API.

        Uses the specified Quandl database to obtain a set of fundamental
        datapoints (or indicators in Quandl parlance) for the provided ticker.

        The formats accepted for the indicators and dimensions are described
        in: https://www.quandl.com/data/SF0-Free-US-Fundamentals-Data/documentation/about
        and
        https://www.quandl.com/data/SF1-Core-US-Fundamentals-Data/documentation/about

        This is vastly simpler than earlier versions where I got a subset of the indicators one
        by one.

        Args:
            ticker: A string representing the stock.
            dimension: A string representing the timeframe for which data is required.
                For the SF0 database only 'MRY' or most recent yearly is supported.
                For the SF1 database available options are: MRY, MRQ, MRT,ARY,ARQ,ART
            periods: An integer representing the number of years of data.
            
        Returns:
            A dataframe containing all of the indicators for this Ticker.
            The indicators are the columns and the time periods are the rows.
            This is after all the next gen refactored version 
        """

    #self.stmnt_df = quandl.get_table('SHARADAR/SF1', ticker=['AAPL','INTC'],dimension="MRY")
    # We'll get all of the data for a given ticker, then filter what we give back
    # Will need more than they ask for calculating CAGR values
    # TODO, there has to be an easy way to pass in how many periods.
        try:
            self.all_inds_df = quandl.get_table('SHARADAR/SF1', ticker=ticker,
                                            dimension=dimension)
            loc_df = self.all_inds_df.copy()

            logger.debug("get_indicators: df columns  = %s" % (self.all_inds_df.columns.tolist()))
            logger.debug("get_indicators: all_inds_df = %s" % (self.all_inds_df.head()))
            
        
        except NotFoundError:
            logger.warning('get_indicators: The ticker %s '
                            'is not supported quandl code was %s',
                            ticker, quandl_code)
            raise
        
        # Let's copy the relevant column data to the income statement df
        # the cash flow statementdf etc

        self.i_stmnt_df = self.all_inds_df[self.i_stmnt_ind_dict.keys()].copy()
        self.cf_stmnt_df = self.all_inds_df[self.cf_stmnt_ind_dict.keys()].copy()
        self.bal_stmnt_df = self.all_inds_df[self.bal_stmnt_ind_dict.keys()].copy()
        self.metrics_and_ratios_df = self.all_inds_df[self.metrics_and_ratios_ind_dict.keys()].copy()
        self.dimension = dimension
        self.periods = periods

        logger.debug("get_indicators: income dataframe = %s" % (self.i_stmnt_df.head()))

        return loc_df

    def get_trans_fmt_i_stmnt(self):
        """ Returns a transposed income statement dataframe with description added
        ready for printing to an excel sheet, or possible via html in the future.
        The Transposed dataframe with added description columns is much easier to read.

        Returns:
            A dataframe
        """
        stmnt_df = self.i_stmnt_df.copy()
        desc_dict = self.i_stmnt_ind_dict
        description = "Sharadar Income"
        return self.__trans_fmt_stmnt(stmnt_df, desc_dict, description)
        
    def get_trans_fmt_cf_stmnt(self):
        stmnt_df = self.cf_stmnt_df.copy()
        desc_dict = self.cf_stmnt_ind_dict
        description = "Sharadar Cash Flow"
        return self.__trans_fmt_stmnt(stmnt_df, desc_dict, description)
        
    def get_trans_fmt_bal_stmnt(self):
        stmnt_df = self.bal_stmnt_df.copy()
        desc_dict = self.bal_stmnt_ind_dict
        description = "Sharadar Balance"

        return self.__trans_fmt_stmnt(stmnt_df, desc_dict,description)

    def get_trans_fmt_metrics_and_ratios(self):
        stmnt_df = self.metrics_and_ratios_df.copy()
        desc_dict = self.metrics_and_ratios_ind_dict
        description = "Sharadar Metrics and Ratios"

        return self.__trans_fmt_stmnt(stmnt_df, desc_dict,description)
        
    def __trans_fmt_stmnt(self, stmnt_df,description_dict,description_of_indictors):
        """ Convert the df so that we have the indicators as rows and datefields as columns

            Side effects. Modifies the passed in dataframe.
        """
        # As a precursor to making the datefiels as comunt we set the datefield as the index.
        # We then transpose the df such that the index becomes the columns and tge columns become rows
        stmnt_df.set_index('datekey',inplace=True)

        # Transpose to get this dataframe ready for printing 
        # Convert the df so that we have the indicators as rows and datefields as columns
        ret_df = stmnt_df.transpose()

        # The columns are of a dateTime type, we need them to be text in order for the dataframe 
        # to excel module to work. 
        ret_df.columns = ret_df.columns.map(lambda t: t.strftime('%Y-%m-%d'))

        
        # Now we want two additional descriptive columns in the dataframe.
        # We want the  Description of the indicator in one column and the Sharadar code
        # in another.
        # Note that dictionary keys, in this case the Sharadar Indicator code
        # becomes the index of the newly created Pandas series. The values become the data associated 
        # with these keys. 
        description_s = pd.Series(description_dict)
        
        # The insert method is what enables us to place the column exactly where we want it.
        ret_df.insert(0, 'Description', description_s)

        # For the second column, the sharadar codes, we can get the manes of these from the index of our
        # dataframe. So a variation on the previous case where we inserted a column from a PD series. Here
        # we point to an array like item which the insert method accepts, that of the dataframe index. After
        # the transpose this contains  what were the column i.e the Sharadar indicators. 
        # 
        # Create a new column using the values from the index, similar to doing a .reset_index
        # but uses an explicit column instead of column 0  which  reset-index  does.
        ret_df.insert(1, description_of_indictors + ' ' + self.dimension, ret_df.index)

        return ret_df

    def write_df_to_excel_sheet(self, dframe, row, col,
                                sheetname,
                                dimension,
                                use_header=True,
                                num_text_cols=2):
        """Writes a dataframe to an excel worksheet.
        Args:
            dframe: A Pandas dataframe. The index must have been promoted to
                a column (using df.) prior to calling.
            row: An int, the row to start writing at, zero based.
            col: An int, the col to start writing at, zero based.
            sheetname: A string, the desired name for the sheet.
            dimension: A string representing the timeframe for which data is required.
                For the SF0 sample database only 'MRY' or most recent yearly is supported.
                For the SF1 database available options are: MRY, MRQ, MRT,ARY,ARQ,ART
            use_header: Whether to print the header of the dataframe
            num_text_cols: The number of columns which contain text. The remainder
                of the columns are assumed to create numeric values.
        Returns:
            rows_written: The number of rows written.

        """

        # logging.debug("write_df_to_excel_sheet: dataframe = %s" % ( dframe.info()))
        # We need to write out the df first using to_excel to obtain a
        # worksheet object which we'll then operate on for formatting.
        # We do not write the header using to_excel but explicitly write
        # later with Xlsxwriter.

        if use_header is True:
            start_row = row + 1
        else:
            start_row = row
        dframe.to_excel(self.writer, sheet_name=sheetname, startcol=col,
                        startrow=start_row, index=False, header=False)
        worksheet = self.writer.sheets[sheetname]
        rows_written = len(dframe.index)

        num_cols = len(dframe.columns.values)

        # Format the text columns and the numeric ones following these.
        worksheet.set_column(0, num_text_cols - 1, 40, self.format_justify)
        worksheet.set_column(num_text_cols, num_cols, 16, self.format_justify)

        numeric_data_range = xl_range(start_row, col + num_text_cols,
                                      start_row + rows_written, col + num_cols)
        worksheet.conditional_format(numeric_data_range, {
            'type': 'cell',
            'criteria': 'between',
            'minimum': -100,
            'maximum': 100,
            'format': self.format_commas
        })
        worksheet.conditional_format(numeric_data_range, {
            'type': 'cell',
            'criteria': 'not between',
            'minimum': -100,
            'maximum': 100,
            'format': self.format_commas_2dec
        })

        # Lets figure out CAGR for a given row item
        num_numeric_cols = num_cols - num_text_cols
        cagr_col = col + num_cols
        begin_cagr_calc_col = num_text_cols
        end_cagr_calc_col = cagr_col -1
        for cagr_row in range(start_row, start_row + rows_written):
            # looks like I'll need to use  xl_rowcol_to_cell()
            beg_val = xl_rowcol_to_cell(cagr_row,begin_cagr_calc_col)
            end_val = xl_rowcol_to_cell(cagr_row,end_cagr_calc_col)

            if dimension == 'MRY' or dimension == 'ARY':
                # We want the number of periods between the years.
                years = end_cagr_calc_col - begin_cagr_calc_col 
            else: 
                # Theres a quarter between each reporting period
                years = (end_cagr_calc_col - begin_cagr_calc_col)/4
                
            #formula = '=({end_val}/{beg_val})^(1/{years}) - 1'.format(beg_val=beg_val,end_val=end_val,years=years)
            formula = '=IFERROR(({end_val}/{beg_val})^(1/{years}) - 1,\"\")'.format(beg_val=beg_val,end_val=end_val,years=years)
            worksheet.write(cagr_row, cagr_col,formula)


        # Sparklines make data trends easily visible
        spark_col = cagr_col + 1
        worksheet.set_column(spark_col, spark_col, 20)

        for spark_row in range(start_row, start_row + rows_written):
            numeric_data_row_range = xl_range(spark_row, col + num_text_cols,
                                              spark_row, col + cagr_col -1)
            worksheet.add_sparkline(spark_row, spark_col, {'range': numeric_data_row_range,
                                                            'markers': 'True'})

        if use_header is True:
            for column, hdr in zip(range(col, num_cols + col), dframe.columns.values.tolist()):
                worksheet.write_string(row, column, hdr, self.format_bold)

        rows_written += 1
        return rows_written


class Fundamentals(object):
    def __init__(self,
                 database,
                 i_ind,
                 cf_ind,
                 bal_ind,
                 metrics_and_ratios_ind,
                 calc_ratios,
                 writer):
        if (database == 'SF0') :
            if "QUANDL_API_SF0_KEY" in os.environ:
                quandl.ApiConfig.api_key = os.environ['QUANDL_API_SF0_KEY']
            else:
                print('Exiting: Please set the QUANDL_API_SF0_KEY environment variable.')
                sys.exit()
        elif (database == 'SF1') :
            if "QUANDL_API_SF1_KEY" in os.environ:
                quandl.ApiConfig.api_key = os.environ['QUANDL_API_SF1_KEY']
            else:
                print('Exiting Please set the QUANDL_API_SF1_KEY environment variable.')
                sys.exit()

        #self.database = 'SHARADAR/' + database
        self.database =  database
        self.i_stmnt_ind_dict = collections.OrderedDict(i_ind)
        self.i_stmnt_df = None
        self.cf_stmnt_ind_dict = collections.OrderedDict(cf_ind)
        self.cf_stmnt_df = None
        self.bal_stmnt_ind_dict = collections.OrderedDict(bal_ind)
        self.bal_stmnt_df = None

        self.metrics_and_ratios_ind_dict = \
            collections.OrderedDict(metrics_and_ratios_ind)
        self.metrics_and_ratios_df = None
        self.calc_ratios_dict = collections.OrderedDict(calc_ratios)
        self.calc_ratios_df = None

        self.writer = writer
        self.workbook = writer.book
        self.format_bold = self.workbook.add_format()
        self.format_bold.set_bold()
        self.format_commas_2dec = self.workbook.add_format()
        self.format_commas_2dec.set_num_format('#,##0')
        self.format_commas = self.workbook.add_format()
        self.format_commas.set_num_format('0.00')
        self.format_justify = self.workbook.add_format()
        self.format_justify.set_align('justify')

    def get_indicators(self, ticker, dimension, periods, category):
        """Obtains fundamental company indicators from the Quandl API.

        Uses the specified Quandl database to obtain a set of fundamental
        datapoints (or indicators in Quandl parlance) for the provided ticker.

        The formats accepted for the indicators and dimensions are described
        in: https://www.quandl.com/data/SF0-Free-US-Fundamentals-Data/documentation/about
        and
        https://www.quandl.com/data/SF1-Core-US-Fundamentals-Data/documentation/about

        Args:
            ticker: A string representing the stock.
            dimension: A string representing the timeframe for which data is required.
                For the SF0 database only 'MRY' or most recent yearly is supported.
                For the SF1 database available options are: MRY, MRQ, MRT,ARY,ARQ,ART
            periods: An integer representing the number of years of data.
            category: A string representing the type of indicator i_stmnt, cf_stmnt,
            bal_stmnt or metrics_and_ratios.

        Returns:
            An ordered dictionary of pandas dataframes containing a timeseries
            of datapoints for each of the indicators requested. The dictionary is
            keyed by the indicator name.
        """
        assert category == 'i_stmnt' or category == 'cf_stmnt' or \
            category == 'bal_stmnt' or category == 'metrics_and_ratios'
        if category == "i_stmnt":
            ind = self.i_stmnt_ind_dict
            self.i_stmnt_df = self._get_dataset_indicators(ticker, ind,
                                                           dimension,
                                                           rows=periods)
            loc_df = self.i_stmnt_df.copy()
            logger.debug("get_indicators: dataframe = %s" %
                         (self.i_stmnt_df.head()))
        elif category == 'cf_stmnt':
            ind = self.cf_stmnt_ind_dict
            self.cf_stmnt_df = self._get_dataset_indicators(ticker, ind,
                                                            dimension,
                                                            rows=periods)
            # fixup the dividend payment to be a positive payment
            self.cf_stmnt_df.loc['ncfdiv'] *= -1
            loc_df = self.cf_stmnt_df.copy()
            logger.debug("get_indicators: dataframe = %s" % (self.cf_stmnt_df.head()))
        elif category == 'bal_stmnt':
            # for the SF1 database the balance sheet data is only supported
            # for MRY MRQ, ARY and ARQ dimensions(Balance sheet is point in
            # time not period so this sorta makes sense). This causes a problem if
            # the user wants to get trailing 12 months income and
            # cf data e.g MRT or ART.
            # We force the balance sheet data to be the quarterly values,
            # which are the most up to date.
            ind = self.bal_stmnt_ind_dict
            if self.database == 'SF1':
                if dimension == 'MRT':
                    dimension = 'MRQ'
                elif dimension == 'ART':
                    dimension = 'ARQ'

            self.bal_stmnt_df = self._get_dataset_indicators(ticker, ind,
                                                             dimension,
                                                             rows=periods)
            loc_df = self.bal_stmnt_df.copy()
            logger.debug("get_indicators: dataframe = %s" % (self.bal_stmnt_df.head()))

        elif category == 'metrics_and_ratios':
            ind = self.metrics_and_ratios_ind_dict
            self.metrics_and_ratios_df = self._get_dataset_indicators(ticker,
                                                                      ind,
                                                                      dimension,
                                                                      rows=periods)
            loc_df = self.metrics_and_ratios_df.copy()
            logger.debug("get_indicators: dataframe = %s" %
                         (self.metrics_and_ratios_df.head()))

        return loc_df

    def write_df_to_excel_sheet(self, dframe, row, col,
                                sheetname,
                                dimension,
                                use_header=True,
                                num_text_cols=2):
        """Writes a dataframe to an excel worksheet.
        Args:
            dframe: A Pandas dataframe. The index must have been promoted to
                a column (using df.) prior to calling.
            row: An int, the row to start writing at, zero based.
            col: An int, the col to start writing at, zero based.
            sheetname: A string, the desired name for the sheet.
            dimension: A string representing the timeframe for which data is required.
                For the SF0 sample database only 'MRY' or most recent yearly is supported.
                For the SF1 database available options are: MRY, MRQ, MRT,ARY,ARQ,ART
            use_header: Whether to print the header of the dataframe
            num_text_cols: The number of columns which contain text. The remainder
                of the columns are assumed to create numeric values.
        Returns:
            rows_written: The number of rows written.

        """

        # logging.debug("write_df_to_excel_sheet: dataframe = %s" % ( dframe.info()))
        # We need to write out the df first using to_excel to obtain a
        # worksheet object which we'll then operate on for formatting.
        # We do not write the header using to_excel but explicitly write
        # later with Xlsxwriter.

        if use_header is True:
            start_row = row + 1
        else:
            start_row = row
        dframe.to_excel(self.writer, sheet_name=sheetname, startcol=col,
                        startrow=start_row, index=False, header=False)
        worksheet = self.writer.sheets[sheetname]
        rows_written = len(dframe.index)

        num_cols = len(dframe.columns.values)

        # Format the text columns and the numeric ones following these.
        worksheet.set_column(0, num_text_cols - 1, 40, self.format_justify)
        worksheet.set_column(num_text_cols, num_cols, 16, self.format_justify)

        numeric_data_range = xl_range(start_row, col + num_text_cols,
                                      start_row + rows_written, col + num_cols)
        worksheet.conditional_format(numeric_data_range, {
            'type': 'cell',
            'criteria': 'between',
            'minimum': -100,
            'maximum': 100,
            'format': self.format_commas
        })
        worksheet.conditional_format(numeric_data_range, {
            'type': 'cell',
            'criteria': 'not between',
            'minimum': -100,
            'maximum': 100,
            'format': self.format_commas_2dec
        })

        # Lets figure out CAGR for a given row item
        num_numeric_cols = num_cols - num_text_cols
        cagr_col = col + num_cols
        begin_cagr_calc_col = num_text_cols
        end_cagr_calc_col = cagr_col -1
        for cagr_row in range(start_row, start_row + rows_written):
            # looks like I'll need to use  xl_rowcol_to_cell()
            beg_val = xl_rowcol_to_cell(cagr_row,begin_cagr_calc_col)
            end_val = xl_rowcol_to_cell(cagr_row,end_cagr_calc_col)

            if dimension == 'MRY' or dimension == 'ARY':
                # We want the number of periods between the years.
                years = end_cagr_calc_col - begin_cagr_calc_col 
            else: 
                # Theres a quarter between each reporting period
                years = (end_cagr_calc_col - begin_cagr_calc_col)/4
                
            #formula = '=({end_val}/{beg_val})^(1/{years}) - 1'.format(beg_val=beg_val,end_val=end_val,years=years)
            formula = '=IFERROR(({end_val}/{beg_val})^(1/{years}) - 1,\"\")'.format(beg_val=beg_val,end_val=end_val,years=years)
            worksheet.write(cagr_row, cagr_col,formula)


        # Sparklines make data trends easily visible
        spark_col = cagr_col + 1
        worksheet.set_column(spark_col, spark_col, 20)

        for spark_row in range(start_row, start_row + rows_written):
            numeric_data_row_range = xl_range(spark_row, col + num_text_cols,
                                              spark_row, col + cagr_col -1)
            worksheet.add_sparkline(spark_row, spark_col, {'range': numeric_data_row_range,
                                                            'markers': 'True'})

        if use_header is True:
            for column, hdr in zip(range(col, num_cols + col), dframe.columns.values.tolist()):
                worksheet.write_string(row, column, hdr, self.format_bold)

        rows_written += 1
        return rows_written

    def get_calc_ratios(self):
        """Obtain some financial ratios and metrics skewed towards credit analysis.
        - Some suggested as useful in the book by Fridson and Alvarez:
        'Financial Statement Analysis'.
        - Others  are credit sanity checking or rough approximations to REIT
          specific ratios.

        Returns:
            A dataframe containing financial ratios.
        """
        # Uses the column names from one of the previously returned dataframes
        # These being the dates.
        self.calc_ratios_df = pd.DataFrame(columns=self.i_stmnt_df.columns)

        for ratio in self.calc_ratios_dict:
            logger.debug("get_calc_ratios: ratio = %s" % (ratio))
            self._calc_ratios(ratio)

        logger.debug("get_calc_ratios: dataframe = %s" % (self.calc_ratios_df))
        return self.calc_ratios_df

    def _calc_ratios(self, ratio):
        # Debt to Cash Flow From Operations
        def _debt_cfo_ratio():
            logger.debug("_calc_ratios._debt_cfo_ratio: debt = %s" % (self.bal_stmnt_df.loc['debt']))

            self.calc_ratios_df.loc[ratio] = \
                self.bal_stmnt_df.loc['debt']/self.cf_stmnt_df.loc['ncfo']
            return

        # Debt to Equity
        def _debt_equity_ratio():
            logger.debug("_calc_ratios._debt_equity_ratio: debt = %s" % (self.bal_stmnt_df.loc['debt']))
            logger.debug("_calc_ratios._debt_equity_ratio: equity = %s" %
                    (self.bal_stmnt_df.loc['equity']))
            self.calc_ratios_df.loc[ratio] = \
                self.bal_stmnt_df.loc['debt']/self.bal_stmnt_df.loc['equity']
            return
        def _liabilities_equity_ratio():
            logger.debug("_calc_ratios._liabilities_equity:_ratio liabilities = %s" %
                    (self.bal_stmnt_df.loc['liabilities']))
            logger.debug("_calc_ratios._liabilities_equity_ratio: equity = %s" %
                    (self.bal_stmnt_df.loc['equity']))
            self.calc_ratios_df.loc[ratio] = \
                self.bal_stmnt_df.loc['liabilities']/self.bal_stmnt_df.loc['equity']
            return

        # Debt to ebitda
        def _debt_ebitda_ratio():
            self.calc_ratios_df.loc[ratio] = \
                self.bal_stmnt_df.loc['debt']/self.metrics_and_ratios_df.loc['ebitda']
            return

        # Debt to ebitda minus CapEx
        def _debt_ebitda_minus_capex_ratio():

            # capex is returned from Sharadar as a -ve number, hence we need to add this to
            # subtract capex
            self.calc_ratios_df.loc[ratio] = \
                self.bal_stmnt_df.loc['debt']/ \
                (self.metrics_and_ratios_df.loc['ebitda'] + self.cf_stmnt_df.loc['capex'])
            return

        # Net Debt to ebitda
        def _net_debt_ebitda_ratio():
            self.calc_ratios_df.loc[ratio] = \
                (self.bal_stmnt_df.loc['debt'] - self.bal_stmnt_df.loc['cashnequsd']) / self.metrics_and_ratios_df.loc['ebitda']
            return

        # Net Debt to ebitda minus CapEx
        def _net_debt_ebitda_minus_capex_ratio():
            # capex is returned from Sharadar as a -ve number, hence we need to add this to
            # subtract capex
            self.calc_ratios_df.loc[ratio] = \
                (self.bal_stmnt_df.loc['debt'] - self.bal_stmnt_df.loc['cashnequsd']) /  \
                (self.metrics_and_ratios_df.loc['ebitda'] + self.cf_stmnt_df.loc['capex'])
            return


        # Depreciation to Cash Flow From Operations Pg 278.
        def _depreciation_cfo_ratio():
            self.calc_ratios_df.loc[ratio] = \
                self.cf_stmnt_df.loc['depamor']/self.cf_stmnt_df.loc['ncfo']
            return

        def _depreciation_revenue_ratio():
            self.calc_ratios_df.loc[ratio] = \
                self.cf_stmnt_df.loc['depamor']/self.i_stmnt_df.loc['revenue']
            return

        def _debt_to_total_capital():
            self.calc_ratios_df.loc[ratio] = \
                self.bal_stmnt_df.loc['debt']/self.metrics_and_ratios_df.loc['invcapavg']
            return

        def _roic():
            self.calc_ratios_df.loc[ratio] = \
                self.i_stmnt_df.loc['ebit']/self.metrics_and_ratios_df.loc['invcapavg'] 
#        self.database =  database

        # Times Interest coverage aka fixed charge coverage Pg 278.
        # (Net Income + Income taxes + Interest Expense)/(Interest expense + Capitalized Interest)
        # Cannot see how to get capitalized interest from the API so that term is excluded.
        # This is the same as ebit to Interest Expense
        def _ebit_interest_coverage():
            self.calc_ratios_df.loc[ratio] = \
                self.i_stmnt_df.loc['ebit']/self.i_stmnt_df.loc['intexp']
            return

        def _ebitda_interest_coverage():
            self.calc_ratios_df.loc[ratio] = \
                self.metrics_and_ratios_df.loc['ebitda']/self.i_stmnt_df.loc['intexp']
            return

        def _ebitda_minus_capex_interest_coverage():
            # Recall that capex is returned from Sharadar as a -ve number.
            self.calc_ratios_df.loc[ratio] = \
            (self.metrics_and_ratios_df.loc['ebitda'] + self.cf_stmnt_df.loc['capex']) / \
                self.i_stmnt_df.loc['intexp']
            return

        def _rough_ffo():
            self.calc_ratios_df.loc[ratio] = \
                self.i_stmnt_df.loc['netinc'] + self.cf_stmnt_df.loc['depamor']
            return

        def _rough_affo():
            # capex is returned from Quandl as a -ve number, hence we add this to
            # subtract capex
            self.calc_ratios_df.loc[ratio] = \
                self.i_stmnt_df.loc['netinc'] + self.cf_stmnt_df.loc['depamor'] + \
                self.cf_stmnt_df.loc['capex']
            return

        def _rough_ffo_dividend_payout_ratio():
            self.calc_ratios_df.loc[ratio] = \
               self.cf_stmnt_df.loc['ncfdiv'] / \
               (self.i_stmnt_df.loc['netinc'] + self.cf_stmnt_df.loc['depamor'])
            return

        def _rough_affo_dividend_payout_ratio():
            self.calc_ratios_df.loc[ratio] = \
                self.cf_stmnt_df.loc['ncfdiv'] / \
                (self.i_stmnt_df.loc['netinc'] + self.cf_stmnt_df.loc['depamor'] +
                 self.cf_stmnt_df.loc['capex'])
            return

        def _income_dividend_payout_ratio():
            self.calc_ratios_df.loc[ratio] = \
                self.cf_stmnt_df.loc['ncfdiv'] / self.i_stmnt_df.loc['netinc']
            return

        def _price_rough_ffo_ps_ratio():
            self.calc_ratios_df.loc[ratio] = \
                self.i_stmnt_df.loc['price'] /  \
                (self.calc_ratios_df.loc['rough_ffo'] / \
                        self.bal_stmnt_df.loc['shareswa'])
            return

        def _rough_ffo_ps():
            self.calc_ratios_df.loc[ratio] = \
                (self.calc_ratios_df.loc['rough_ffo'] / \
                        self.bal_stmnt_df.loc['shareswa'])
            return

        def _cfo_ps():
            self.calc_ratios_df.loc[ratio] = \
                (self.cf_stmnt_df.loc['ncfo'] / \
                        self.bal_stmnt_df.loc['shareswa'])
            return

        def _fcf_ps():
            self.calc_ratios_df.loc[ratio] = \
                self.metrics_and_ratios_df.loc['fcf']  / \
                        self.bal_stmnt_df.loc['shareswa']
            return

        def _ev_opinc_ratio():
            self.calc_ratios_df.loc[ratio] = \
                self.metrics_and_ratios_df.loc['ev']/self.i_stmnt_df.loc['opinc']
            return

        # Kenneth Jeffrey Marshal, author of Good Stocks Cheap, definition
        # of capital employed. He has two defnitions, one where cash is
        # subtracted and one where it's not. Accrued expenses should be
        # substracted but Is not available in the Sharadar API, probably a
        # scour the footnotes thing if really wanted to include this.
        def _kjm_capital_employed_1():
            self.calc_ratios_df.loc[ratio] = \
                self.bal_stmnt_df.loc['assets'] - \
                self.bal_stmnt_df.loc['cashnequsd'] - \
                self.bal_stmnt_df.loc['payables'] - \
                self.bal_stmnt_df.loc['deferredrev']
            return

        def _kjm_capital_employed_2():
            self.calc_ratios_df.loc[ratio] = \
                self.bal_stmnt_df.loc['assets'] - \
                self.bal_stmnt_df.loc['payables'] - \
                self.bal_stmnt_df.loc['deferredrev']
            return

        def _kjm_return_on_capital_employed_1():
            self.calc_ratios_df.loc[ratio] = \
                self.i_stmnt_df.loc['opinc'] / self.calc_ratios_df.loc['kjm_capital_employed_1']
            return

        def _kjm_return_on_capital_employed_2():
            self.calc_ratios_df.loc[ratio] = \
                self.i_stmnt_df.loc['opinc'] / self.calc_ratios_df.loc['kjm_capital_employed_2']
            return

        def _dividends_free_cash_flow_ratio():
            self.calc_ratios_df.loc[ratio] = \
                self.cf_stmnt_df.loc['ncfdiv'] / self.metrics_and_ratios_df.loc['fcf']
            return
        def _preferred_free_cash_flow_ratio():
            self.calc_ratios_df.loc[ratio] = \
                self.i_stmnt_df.loc['prefdivis'] / self.metrics_and_ratios_df.loc['fcf']
            return

        def _operating_margin():
            self.calc_ratios_df.loc[ratio] = \
                self.i_stmnt_df.loc['opinc']/self.i_stmnt_df.loc['revenue']
            return

        def _sg_and_a_gross_profit_ratio():
            self.calc_ratios_df.loc[ratio] = \
                self.i_stmnt_df.loc['sgna'] / self.i_stmnt_df.loc['GP']
            return

        def _ltdebt_cfo_ratio():
            self.calc_ratios_df.loc[ratio] = \
                self.bal_stmnt_df.loc['debtnc'] / self.cf_stmnt_df.loc['ncfo']
            return

        def _ltdebt_earnings_ratio():
            self.calc_ratios_df.loc[ratio] = \
                self.bal_stmnt_df.loc['debtnc'] / self.i_stmnt_df.loc['netinc']
            return

        def _free_cash_flow_conversion_ratio():
            self.calc_ratios_df.loc[ratio] = \
                self.metrics_and_ratios_df.loc['fcf'] / self.metrics_and_ratios_df.loc['ebitda']
            return

        # Pg 290 of Creative Cash Flow Reporting, Mumford et al.
        def _excess_cash_margin_ratio():
            self.calc_ratios_df.loc[ratio] = \
                (self.cf_stmnt_df.loc['ncfo'] - self.i_stmnt_df.loc['opinc']) * 100/ \
                self.i_stmnt_df.loc['revenue']
            return

        def _interest_to_cfo_plus_interest_coverage():
            self.calc_ratios_df.loc[ratio] = \
                self.i_stmnt_df.loc['intexp'] / \
                    (self.cf_stmnt_df.loc['ncfo'] + self.i_stmnt_df.loc['intexp'])
            return
        def _dividends_cfo_ratio():
            self.calc_ratios_df.loc[ratio] = \
                self.cf_stmnt_df.loc['ncfdiv'] / self.cf_stmnt_df.loc['ncfo'] 
            return
        def _preferred_cfo_ratio():
            self.calc_ratios_df.loc[ratio] = \
                self.i_stmnt_df.loc['prefdivis'] / self.cf_stmnt_df.loc['ncfo'] 
            return

        switcher = {
            "debt_equity_ratio": _debt_equity_ratio,
            "liabilities_equity_ratio": _liabilities_equity_ratio,
            "debt_ebitda_ratio": _debt_ebitda_ratio,
            "debt_ebitda_minus_capex_ratio": _debt_ebitda_minus_capex_ratio,
            "net_debt_ebitda_ratio": _net_debt_ebitda_ratio,
            "net_debt_ebitda_minus_capex_ratio": _net_debt_ebitda_minus_capex_ratio,
            "debt_to_total_capital": _debt_to_total_capital,
            "return_on_invested_capital": _roic,
            "ebit_interest_coverage": _ebit_interest_coverage,
            "ebitda_interest_coverage": _ebitda_interest_coverage,
            "ebitda_minus_capex_interest_coverage": _ebitda_minus_capex_interest_coverage,
            "debt_cfo_ratio": _debt_cfo_ratio,
            "depreciation_cfo_ratio": _depreciation_cfo_ratio,
            "depreciation_revenue_ratio": _depreciation_revenue_ratio,
            "rough_ffo": _rough_ffo,
            "rough_affo": _rough_affo,
            "rough_ffo_dividend_payout_ratio": _rough_ffo_dividend_payout_ratio,
            "rough_affo_dividend_payout_ratio": _rough_affo_dividend_payout_ratio,
            "income_dividend_payout_ratio": _income_dividend_payout_ratio,
            "price_rough_ffo_ps_ratio": _price_rough_ffo_ps_ratio,
            "rough_ffo_ps": _rough_ffo_ps,
            "ev_opinc_ratio": _ev_opinc_ratio,
            "kjm_capital_employed_1": _kjm_capital_employed_1,
            "kjm_capital_employed_2": _kjm_capital_employed_2,
            "kjm_return_on_capital_employed_1": _kjm_return_on_capital_employed_1,
            "kjm_return_on_capital_employed_2": _kjm_return_on_capital_employed_2,
            "dividends_free_cash_flow_ratio" : _dividends_free_cash_flow_ratio,
            "preferred_free_cash_flow_ratio" : _preferred_free_cash_flow_ratio,
            "operating_margin": _operating_margin,
            "sg_and_a_gross_profit_ratio": _sg_and_a_gross_profit_ratio,
            "ltdebt_cfo_ratio": _ltdebt_cfo_ratio,
            "ltdebt_earnings_ratio": _ltdebt_earnings_ratio,
            "free_cash_flow_conversion_ratio": _free_cash_flow_conversion_ratio,
            "cfo_ps": _cfo_ps,
            "fcf_ps": _fcf_ps,
            "excess_cash_margin_ratio": _excess_cash_margin_ratio,
            "interest_to_cfo_plus_interest_coverage": _interest_to_cfo_plus_interest_coverage,
            "dividends_cfo_ratio" : _dividends_cfo_ratio,
            "preferred_cfo_ratio" : _preferred_cfo_ratio
        }

        # Get the function from switcher dictionary
        func = switcher.get(ratio, lambda: NotImplementedError)
        # Execute the function
        return func()

    def _get_dataset_indicators(self, ticker, ind, dimension, rows):
        first = True
        dframe = None
        # With the API change, of early 2018  the SF0 and SF1 datasets
        # use the same  database code of SHARADAR/SF!. Different dimensions
        # tickers supported apply between the free SF0 dataset and the paid
        # SF1. Confirmed via # email from vincent at Sharadar.
        database = 'SHARADAR/SF1'
        for indicator in ind.keys():
            logger.debug('_get_dataset_indicators: db %s, ticker %s, indicator %s, dimension %s', self.database, ticker, indicator, dimension)

            try:
                qdframe = quandl.get_table(database, \
                                    paginate = True, \
                                    #paginate = False, \
                                    qopts =\
                                    {"columns":[indicator,'datekey']},\
                                    ticker=ticker, dimension=dimension)

                assert len(qdframe.index) > 0, "Sharadar returning zero len table for ticker %r indicator %r" % (ticker, indicator)

                # We've now got one  column named after the indicator,
                # e.g revenue and a datekey column too
                # Create a copy of this quandle dataframe
                if first is True:
                    dframe = qdframe.copy()

                    # The old API returned uppercase column names, this new
        # get_table form returns lowercase.
                    # So .. make em upper again to avoid
                    # having to modify all existing strings.
                    dframe.rename(columns={indicator.lower(): indicator.upper()},inplace=True)
                    first = False
                # Build up our dframe by copying the indicator column from the
                # quandl dataframe.
                else:
                    qdframe.rename(columns={indicator.lower(): indicator.upper()},inplace=True)
                    dframe[indicator] = qdframe[indicator]

            except NotFoundError:
                logger.warning('_get_dataset_indicators: The ticker %s '
                               'or indicator %s is not supported quandl code was %s',
                               ticker, indicator, quandl_code)
                raise

        #logger.debug("_get_dataset_indicators: dataframe = %s" % (dframe))
        # It explicitly mentions in the API documentation that the values
        # returned are not sorted.
        # So .. sort our dataframe by the date
        dframe = dframe.sort_values('datekey')
        # Truncate to the desired number of periods
        dframe = dframe.tail(rows)
        #logger.debug("_get_dataset_indicators: dataframe aftrer sort and truncate = %s" % (dframe))

        # We now have a bunch of indicator columns and a single datekey column
        # What we want is the data to have a set of date columns with
        # indicators as each row.
        # Make the datekey column the index.
        dframe.set_index('datekey',inplace=True)
        # So... transpose such that the indicator  columns  become the rows
        # and dates are the columns
        dframe = dframe.transpose()
        dframe.columns = dframe.columns.map(lambda t: t.strftime('%Y-%m-%d'))

        return dframe


class SharadarFundamentals_ng(Fundamentals_ng):

    # the refactored version of SharadarFundamenals
    # Locally calculated by this package. For each ratio or metric in this
    # table, there's a routine to calculate the value from the quandl API provided
    # statement indicator value.

    I_STMNT_IND = [
        ('datekey','SEC filing date'),
        ('revenue', 'Revenues'),
        ('gp', 'Gross Profit'),
        ('sgna', 'Sales General and Admin'),
        ('intexp', 'Interest Expense'),
        ('taxexp', 'Tax Expense'),
        ('opinc', 'Operating Income'),
        ('ebit', 'Earnings Before Interest and Taxes'),
        ('netinc', 'Net Income'),
        ('prefdivis', "Preferred Dividends"),
        ('netinccmn', 'Net Income to Common (after prefs paid)'),
        ('epsdil', 'Earnings Per Share Diluted'),
        ('price','Price per Share'),
        ('shareswadil', 'Weighted Average Shares Diluted'),
        ('dps', 'Dividends per Basic Common Share'),
    ]

    # Cash Flow Statement Indicator Quandl/Sharadar Codes
    CF_STMNT_IND = [
        ('datekey','SEC filing date'),
        ('depamor', 'Depreciation and Amortization'),
        ('ncfo', 'Net Cash Flow From Operations'),
        ('ncfi', 'Net Cash Flow From Investing'),
        ('capex', 'Capital Expenditure'),
        ('ncff', 'Net Cash Flow From Financing'),
        ('ncfdiv', 'Payment of Dividends and Other Cash Distributions')
    ]

    # Balance Statement Indicator Quandl/Sharadar Codes
    BAL_STMNT_IND = [
        ('datekey','SEC filing date'),
        ('assets', 'Total Assets'),
        ('assetsnc', 'Non Current Assets'),
        ('cashnequsd', 'Cash and Equivalents (USD)'),
        ('deferredrev', 'Deferred Revenue'),
        ('intangibles', 'Intangibles'),
        ('debt', 'Total Debt'),
        ('debtnc', 'Long Term  Debt'),
        ('liabilities', 'Total Liabilities'),
        ('payables', 'Trade and Non Trade Payables'),
        ('receivables', 'Trade and Non Trade Receivables'),
        ('retearn', 'Retained Earnings'),
        ('equity', 'Shareholders Equity'),
        ('shareswa', 'Weighted Average Shares')
    ]
    # Metrics and Ratio  Indicator Quandl/Sharadar Codes
    METRICS_AND_RATIOS_IND = [
        ('datekey','SEC filing date'),
        #    ('DE', 'Debt to Equity Ratio'), Needs to be locally calculated when
        #    using TTM figures
        ('ev', 'Enterprise Value'),
        # evebitda only returned for the MRT period, the default for SF1
        ('evebitda', 'Enterprise Value divided by ebitda'),
        ('pe', 'Price Earnings Damodaran: Market Cap / Net Income'),
        ('ps', 'Price Sales Damodaran: Market Cap / Revenue'),
        ('assetturnover', 'Revenue / Assets average'),
        ('roa', 'Return on Assets: Net Income / Average Assets'),
        ('roe', 'Return on Equity: Net Income / Average Equity'),
        ('ros', 'Return on Sales: ebit / Revenue'),
        ('ebitda', 'Earnings Before Interest Taxes & Depreciation & Amortization'),
        ('fcf', 'Free Cash Flow: CFO - CapEx'),
        ('invcapavg', 'Invested Capital'),
        ('roic', 'Return On Invested Capital'),
        ('grossmargin', 'Gross Margin: Gross Profit/ Revenue'),
        ('netmargin', 'Net Margin: Net Income/ Revenue')
    ]


    CALCULATED_RATIOS = [
        ("operating_margin", 'Operating Margin: (Gross Profit - Opex)/ Revenue'),
        ("sg_and_a_gross_profit_ratio", 'SG&A to Gross Profit Ratio'),
        ("depreciation_revenue_ratio", 'Depreciation / Revenue'),
        ("depreciation_cfo_ratio", 'Depreciation / Cash Flow From Operations'),
        ("ev_opinc_ratio", 'Acquirers Multiple: Enterprise Value / Operating Income'),
        ("debt_ebitda_ratio", 'Total Debt / ebitda'),
        ("debt_ebitda_minus_capex_ratio", 'Total Debt / (ebitda - CapEx)'),
        ("net_debt_ebitda_ratio", 'Net Debt / ebitda'),
        ("net_debt_ebitda_minus_capex_ratio", 'Net Debt / (ebitda - CapEx)'),
        ("debt_equity_ratio", 'Total Debt / Shareholders Equity'),
        ("liabilities_equity_ratio", 'Total Liabilities / Shareholders Equity'),
        ("ebit_interest_coverage", 'ebit / Interest Expense'),
        ("ebitda_interest_coverage", 'ebitda / Interest Expense'),
        ("ebitda_minus_capex_interest_coverage", 'ebitda - CapEx / Interest Expense'),
        ("interest_to_cfo_plus_interest_coverage", 'Interest / (CFO + Interest'), 
        ("debt_to_total_capital", 'Total Debt / Invested Capital'),
        ("return_on_invested_capital", 'Return on Invested Capital: ebit / Invested Capital'),
        ("kjm_capital_employed_1", 'Kenneth J  Marshal Capital Employed Subtract CASH'),
        ("kjm_capital_employed_2", 'Kenneth J  Marshal Capital Employed'),
        ("kjm_return_on_capital_employed_1", 'KJM Return on Capital Employed subtract CASH'),
        ("kjm_return_on_capital_employed_2", 'KJM Return on Capital Employed'),
        # fcf is already levered since CFO  already includes the effect of interest
        # payments.
#        ("free_cash_flow_levered", 'fcf-Levered: fcf - Interest Expenses'),
        ("debt_cfo_ratio", 'Total Debt / Cash Flow From Operations'),
        ("ltdebt_cfo_ratio", 'Long Term Debt / Cash Flow From Operations'),
        ("ltdebt_earnings_ratio", 'Long Term Debt / Income'),
        ("rough_ffo", 'Rough FFO: Net Income plus Depreciation (missing cap gain from RE sales adjust)'),
        ('rough_ffo_ps', 'Rough FFO per Share'),
        ('price_rough_ffo_ps_ratio', 'Price divided by rough_ffo_ps'),
        ('rough_ffo_dividend_payout_ratio', 'Dividends / rough_ffo'),
        ('income_dividend_payout_ratio', 'Dividends / Net Income'),
        ('cfo_ps', 'Cash Flow from Operations  per Share'),
        ('dividends_cfo_ratio', 'Dividends/CFO'), 
        ('preferred_cfo_ratio', 'Preferred Payments/CFO'), 
        ('fcf_ps', 'Free Cash Flow per Share'),
        ('dividends_free_cash_flow_ratio', 'Dividends/fcf'),
        ('preferred_free_cash_flow_ratio', 'Preferred Payments/fcf'),
        ('free_cash_flow_conversion_ratio', 'Free Cash Flow Conversion Ratio'),
        ('excess_cash_margin_ratio', 'Excess Cash Margin Ratio')
    ]

    def __init__(self, database, writer):
            Fundamentals_ng.__init__(self,
                                database,
                                self.I_STMNT_IND,
                                self.CF_STMNT_IND,
                                self.BAL_STMNT_IND,
                                self.METRICS_AND_RATIOS_IND,
                                self.CALCULATED_RATIOS,
                                writer
                                )

class SharadarFundamentals(Fundamentals):
    # Income Statement Indicator Quandl/Sharadar Codes
    I_STMNT_IND = [
        ('revenue', 'Revenues'),
        ('GP', 'Gross Profit'),
        ('sgna', 'Sales General and Admin'),
        ('intexp', 'Interest Expense'),
        ('TAXEXP', 'Tax Expense'),
        ('opinc', 'Operating Income'),
        ('ebit', 'Earnings Before Interest and Taxes'),
        ('netinc', 'Net Income'),
        ('prefdivis', "Preferred Dividends"),
        ('NETINCCMN', 'Net Income to Common (after prefs paid)'),
        ('EPSDIL', 'Earnings Per Share Diluted'),
        ('price','Price per Share'),
        ('SHARESWADIL', 'Weighted Average Shares Diluted'),
        ('DPS', 'Dividends per Basic Common Share'),
    ]

    # Cash Flow Statement Indicator Quandl/Sharadar Codes
    CF_STMNT_IND = [
        ('depamor', 'Depreciation and Amortization'),
        ('ncfo', 'Net Cash Flow From Operations'),
        ('NCFI', 'Net Cash Flow From Investing'),
        ('capex', 'Capital Expenditure'),
        ('NCFF', 'Net Cash Flow From Financing'),
        ('ncfdiv', 'Payment of Dividends and Other Cash Distributions')
    ]

    # Balance Statement Indicator Quandl/Sharadar Codes
    BAL_STMNT_IND = [
        ('assets', 'Total Assets'),
        ('ASSETSNC', 'Non Current Assets'),
        ('cashnequsd', 'Cash and Equivalents (USD)'),
        ('deferredrev', 'Deferred Revenue'),
        ('INTANGIBLES', 'Intangibles'),
        ('debt', 'Total Debt'),
        ('debtnc', 'Long Term  Debt'),
        ('liabilities', 'Total Liabilities'),
        ('payables', 'Trade and Non Trade Payables'),
        ('RECEIVABLES', 'Trade and Non Trade Receivables'),
        ('RETEARN', 'Retained Earnings'),
        ('equity', 'Shareholders Equity'),
        ('shareswa', 'Weighted Average Shares')
    ]
    # Metrics and Ratio  Indicator Quandl/Sharadar Codes
    METRICS_AND_RATIOS_IND = [
        #    ('DE', 'Debt to Equity Ratio'), Needs to be locally calculated when
        #    using TTM figures
        ('ev', 'Enterprise Value'),
        # EVEBITDA only returned for the MRT period, the default for SF1
        ('EVEBITDA', 'Enterprise Value divided by ebitda'),
        ('PE', 'Price Earnings Damodaran: Market Cap / Net Income'),
        ('PS', 'Price Sales Damodaran: Market Cap / Revenue'),
        ('ASSETTURNOVER', 'Revenue / Assets average'),
        ('ROA', 'Return on Assets: Net Income / Average Assets'),
        ('ROE', 'Return on Equity: Net Income / Average Equity'),
        ('ROS', 'Return on Sales: ebit / Revenue'),
        ('ebitda', 'Earnings Before Interest Taxes & Depreciation & Amortization'),
        ('fcf', 'Free Cash Flow: CFO - CapEx'),
        ('invcapavg', 'Invested Capital'),
        ('ROIC', 'Return On Invested Capital'),
        ('GROSSMARGIN', 'Gross Margin: Gross Profit/ Revenue'),
        ('NETMARGIN', 'Net Margin: Net Income/ Revenue')
    ]

    # Locally calculated by this package. For each ratio or metric in this
    # table, there's a routine to calculate the value from the quandl API provided
    # statement indicator value.
    CALCULATED_RATIOS = [
        ("operating_margin", 'Operating Margin: (Gross Profit - Opex)/ Revenue'),
        ("sg_and_a_gross_profit_ratio", 'SG&A to Gross Profit Ratio'),
        ("depreciation_revenue_ratio", 'Depreciation / Revenue'),
        ("depreciation_cfo_ratio", 'Depreciation / Cash Flow From Operations'),
        ("ev_opinc_ratio", 'Acquirers Multiple: Enterprise Value / Operating Income'),
        ("debt_ebitda_ratio", 'Total Debt / ebitda'),
        ("debt_ebitda_minus_capex_ratio", 'Total Debt / (ebitda - CapEx)'),
        ("net_debt_ebitda_ratio", 'Net Debt / ebitda'),
        ("net_debt_ebitda_minus_capex_ratio", 'Net Debt / (ebitda - CapEx)'),
        ("debt_equity_ratio", 'Total Debt / Shareholders Equity'),
        ("liabilities_equity_ratio", 'Total Liabilities / Shareholders Equity'),
        ("ebit_interest_coverage", 'ebit / Interest Expense'),
        ("ebitda_interest_coverage", 'ebitda / Interest Expense'),
        ("ebitda_minus_capex_interest_coverage", 'ebitda - CapEx / Interest Expense'),
        ("interest_to_cfo_plus_interest_coverage", 'Interest / (CFO + Interest'), 
        ("debt_to_total_capital", 'Total Debt / Invested Capital'),
        ("return_on_invested_capital", 'Return on Invested Capital: ebit / Invested Capital'),
        ("kjm_capital_employed_1", 'Kenneth J  Marshal Capital Employed Subtract CASH'),
        ("kjm_capital_employed_2", 'Kenneth J  Marshal Capital Employed'),
        ("kjm_return_on_capital_employed_1", 'KJM Return on Capital Employed subtract CASH'),
        ("kjm_return_on_capital_employed_2", 'KJM Return on Capital Employed'),
        # fcf is already levered since CFO  already includes the effect of interest
        # payments.
#        ("free_cash_flow_levered", 'fcf-Levered: fcf - Interest Expenses'),
        ("debt_cfo_ratio", 'Total Debt / Cash Flow From Operations'),
        ("ltdebt_cfo_ratio", 'Long Term Debt / Cash Flow From Operations'),
        ("ltdebt_earnings_ratio", 'Long Term Debt / Income'),
        ("rough_ffo", 'Rough FFO: Net Income plus Depreciation (missing cap gain from RE sales adjust)'),
        ('rough_ffo_ps', 'Rough FFO per Share'),
        ('price_rough_ffo_ps_ratio', 'Price divided by rough_ffo_ps'),
        ('rough_ffo_dividend_payout_ratio', 'Dividends / rough_ffo'),
        ('income_dividend_payout_ratio', 'Dividends / Net Income'),
        ('cfo_ps', 'Cash Flow from Operations  per Share'),
        ('dividends_cfo_ratio', 'Dividends/CFO'), 
        ('preferred_cfo_ratio', 'Preferred Payments/CFO'), 
        ('fcf_ps', 'Free Cash Flow per Share'),
        ('dividends_free_cash_flow_ratio', 'Dividends/fcf'),
        ('preferred_free_cash_flow_ratio', 'Preferred Payments/fcf'),
        ('free_cash_flow_conversion_ratio', 'Free Cash Flow Conversion Ratio'),
        ('excess_cash_margin_ratio', 'Excess Cash Margin Ratio')
    ]

    


def stock_xlsx(outfile, stocks, database, dimension, periods):
    # Excel Housekeeping first
    # The writer contains books and sheets
    writer = pd.ExcelWriter(outfile,
                            engine='xlsxwriter',
                            date_format='d mmmm yyyy')

    # Get a stmnt dataframe, a quandl ratios dataframe and our calculated ratios dataframe
    # for each of these frames write into a separate worksheet per stock
    for stock in stocks:
        fund = SharadarFundamentals(database,writer)

        logger.info('Processing the stock %s', stock)

        shtname = '{}'.format(stock)

        try:
            i_stmnt_df = fund.get_indicators(stock, dimension, periods, "i_stmnt")
        except NotFoundError:
            # This is the only place where we can simply continue to another stock
            # further down we will have already written things to a worksheet so not going to be
            # easy to unravel, hence do not attempt to catch these.
            logger.warning('NotFoundError when getting income stmnt indicators for the stock %s', stock)
            continue
        row, col = 0, 0

        # Create a series containing the dataset descriptions and add as a column to our dataframe
        # FIX ME this peeking at privates is potentially cheesy
        # TODO migrate this to the write_df-to-excel_sheet fn
        description_s = pd.Series(fund.i_stmnt_ind_dict)
        # The insert method is what enables us to place the column exactly where we want it.
        i_stmnt_df.insert(0, 'Description', description_s)
        # Create a new column using the values from the index, similar to doing a .reset_index
        # but uses an explicit column instead of column 0  which  reset-index  does.
        i_stmnt_df.insert(1, 'Sharadar Fundamental Indicators' + ' ' + dimension, i_stmnt_df.index)

        rows_written = fund.write_df_to_excel_sheet(i_stmnt_df, row, col,
                                                    shtname, dimension,
                                                    use_header=True)
        row = row + rows_written

        cf_stmnt_df = fund.get_indicators(stock, dimension, periods, "cf_stmnt")
        description_s = pd.Series(fund.cf_stmnt_ind_dict)
        cf_stmnt_df.insert(0, 'Description', description_s)
        cf_stmnt_df.insert(1, 'Sharadar Fundamental Indicators', cf_stmnt_df.index)
        rows_written = fund.write_df_to_excel_sheet(cf_stmnt_df, row, col,
                                                    shtname,dimension,
                                                    use_header=False)
        row = row + rows_written

        bal_stmnt_df = fund.get_indicators(stock, dimension, periods, "bal_stmnt")
        description_s = pd.Series(fund.bal_stmnt_ind_dict)
        bal_stmnt_df.insert(0, 'Description', description_s)
        bal_stmnt_df.insert(1, 'Sharadar Fundamental Indicators', bal_stmnt_df.index)
        rows_written = fund.write_df_to_excel_sheet(bal_stmnt_df, row, col,
                                                    shtname,dimension,
                                                    use_header=False)
        row = row + rows_written

        # Now for the metrics and ratios from the quandl API
        metrics_and_ratios_ind = fund.get_indicators(stock, dimension, periods,
                                                     'metrics_and_ratios')

        description_s = pd.Series(fund.metrics_and_ratios_ind_dict)
        metrics_and_ratios_ind.insert(0, 'Description', description_s)
        metrics_and_ratios_ind.insert(1, 'Sharadar Metrics and Ratio Indicators',
                                      metrics_and_ratios_ind.index)

        row = row + 2
        rows_written = fund.write_df_to_excel_sheet(metrics_and_ratios_ind, row, col,
                                                    shtname, dimension)
        row = row + rows_written

        # Now calculate some of the additional ratios for credit analysis
        calculated_ratios_df = fund.get_calc_ratios()
        description_s = pd.Series(fund.calc_ratios_dict)
        calculated_ratios_df.insert(0, 'Description', description_s)
        calculated_ratios_df.insert(1, 'Calculated Metrics and Ratios', calculated_ratios_df.index)

        row = row + 2
        rows_written = fund.write_df_to_excel_sheet(calculated_ratios_df, row, col,
                                                    shtname, dimension)
        logger.info('Processed the stock %s', stock)

    writer.save()
 
def stock_xlsx_refactor(outfile, stocks, database, dimension, periods):
    # Excel Housekeeping first
    # The writer contains books and sheets
    writer = pd.ExcelWriter(outfile,
                            engine='xlsxwriter',
                            date_format='d mmmm yyyy')

    # Get a stmnt dataframe, a quandl ratios dataframe and our calculated ratios dataframe
    # for each of these frames write into a separate worksheet per stock
    for stock in stocks:
        fund = SharadarFundamentals_ng(database,writer)

        logger.info('Processing the stock %s', stock)

        shtname = '{}'.format(stock)

        try:
            fund.get_indicators(stock, dimension, periods)
        except NotFoundError:
            logger.warning('NotFoundError when getting indicators for the stock %s', stock)
            continue
        
        row, col = 0, 0
        
        
        i_stmnt_trans_df =  fund.get_trans_fmt_i_stmnt()
        rows_written = fund.write_df_to_excel_sheet(i_stmnt_trans_df, row, col,
                                                    shtname, dimension,
                                                    use_header=True)
        row = row + rows_written + 1
       
        # UPTO TODO implement the get_trans_fmt_i_stmnt call it and continue
        # Perhaps get rid of teh spark lines since we're going back in time
        # And get rid of the CAGR excel calcs.
        # Have to fgure out how to incorporate CAGRS and YOY. 
        # as long as we add all of these to the relevant df e.g the fund.i_stmnt_df then the rest should fll  out.
        # I would need to add to the  initial tuple lists to ensure we had a description for each cagr tuple.
        # would also have to build these columns up bit by bit.
        # So maybe go sparing on CAGRS, and look for ones I want to compare in the future summary sheet work
  
        cf_stmnt_trans_df =  fund.get_trans_fmt_cf_stmnt()
        rows_written = fund.write_df_to_excel_sheet(cf_stmnt_trans_df, row, col,
                                                    shtname,dimension,
                                                    use_header=True)
        row = row + rows_written + 1
        
        bal_stmnt_trans_df =  fund.get_trans_fmt_bal_stmnt()
        rows_written = fund.write_df_to_excel_sheet(bal_stmnt_trans_df, row, col,
                                                    shtname,dimension,
                                                    use_header=True)
        row = row + rows_written + 1
        
        # Now for the metrics and ratios from the quandl API
        metrics_and_ratios_df =  fund.get_trans_fmt_metrics_and_ratios()
        rows_written = fund.write_df_to_excel_sheet(metrics_and_ratios_df, row, col,
                                                    shtname,dimension,
                                                    use_header=True)
        row = row + rows_written + 1

        writer.save()


        row = row + 2
        rows_written = fund.write_df_to_excel_sheet(metrics_and_ratios_ind, row, col,
                                                    shtname, dimension)
        row = row + rows_written

        # Now calculate some of the additional ratios for credit analysis
        calculated_ratios_df = fund.get_calc_ratios()
        description_s = pd.Series(fund.calc_ratios_dict)
        calculated_ratios_df.insert(0, 'Description', description_s)
        calculated_ratios_df.insert(1, 'Calculated Metrics and Ratios', calculated_ratios_df.index)

        row = row + 2
        rows_written = fund.write_df_to_excel_sheet(calculated_ratios_df, row, col,
                                                    shtname, dimension)
        logger.info('Processed the stock %s', stock)

    writer.save()
 

def main():

    #stocks = ['SPG', 'WPC', 'KIM', 'SKT', 'NNN', 'STOR']
    stocks = ['AAPL']

    periods = 5

    outfile = 'quandl_ratios.xlsx'
    # stock_xlsx(outfile, stocks, "SF0", 'MRY', periods)
    stock_xlsx_refactor(outfile, stocks, "SF0", 'MRY', periods)
    # Refactor: Do this by leaving the existing stock_xlsx intact so we can refer
    # to how do write to teh excel sheet for example.
    # First off, do similar to what I did in Juniper notebook
    # Get the whole df at one time.
    # Transpose as the snippet in Save-goodies shows
    # Call our write_df_to_excel_sheet, coercing the correct parameters.
    # Don't tryt to perform custom calcs or CAGR calcs at this time. 
    # ( recall we can use shift for CAGR, when we need to do it)
    # When we do need to compute cagr we should do it with the time series as the column and save off a new pandas 
    # series with all of the CAGR values.

    # do one stock per df at this time, hold off on the fancy groupby and getting all stocks together.
    # we can, later, store each ticker DF in an ordered dict indexed by ticker.  Maybe! Would a multi-index df help or 
    # be overkill

    # the end goal will be to have a dataframe with  ratios along the top and with CAGR values as some of these
    # columns e.g  OCF-5-CAGR. The rows will be tickers and this combined df will be written to a table.
    # In fact I do not need to optimize my writing of the data to teh individual ticker sheets. I should but that's 
    # going to be just for reference.
    # Use term QoQ for the quarterly change in a value.
    # Use the term YoY for the yearly change
    # The for some there's a longer trend, the 5YrCAGR

    # Do I want to have a CAGR value displayed for each ratio I calculate ?


if __name__ == '__main__':
    main()
