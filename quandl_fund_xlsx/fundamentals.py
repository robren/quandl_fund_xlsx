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
from quandl.errors.quandl_error import NotFoundError
from xlsxwriter.utility import xl_range
from xlsxwriter.utility import xl_rowcol_to_cell

# from pdb import set_trace as bp

# Added this one line below  to get logging from the requests module,
# comment me out when done
# logging.basicConfig(level=logging.DEBUG)

logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s %(levelname)-8s %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

# logger.setLevel(logging.INFO)
logger.setLevel(logging.DEBUG)


class Fundamentals_ng(object):
    def __init__(
        self,
        database,
        i_ind,
        cf_ind,
        bal_ind,
        metrics_and_ratios_ind,
        calc_ratios,
        writer,
    ):
        if database == "SF0":
            if "QUANDL_API_SF0_KEY" in os.environ:
                quandl.ApiConfig.api_key = os.environ["QUANDL_API_SF0_KEY"]
            else:
                print(
                    "Exiting: Please set the QUANDL_API_SF0_KEY environment variable."
                )
                sys.exit()
        elif database == "SF1":
            if "QUANDL_API_SF1_KEY" in os.environ:
                quandl.ApiConfig.api_key = os.environ["QUANDL_API_SF1_KEY"]
            else:
                print("Exiting Please set the QUANDL_API_SF1_KEY environment variable.")
                sys.exit()

        # self.database = 'SHARADAR/' + database
        self.database = database
        self.all_inds_df = None

        self.i_stmnt_ind_dict = collections.OrderedDict(i_ind)
        self.i_stmnt_df = None
        self.cf_stmnt_ind_dict = collections.OrderedDict(cf_ind)
        self.cf_stmnt_df = None
        self.bal_stmnt_ind_dict = collections.OrderedDict(bal_ind)
        self.bal_stmnt_df = None

        self.metrics_and_ratios_ind_dict = collections.OrderedDict(
            metrics_and_ratios_ind
        )
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
        self.format_commas_2dec.set_num_format("#,##0")
        self.format_commas = self.workbook.add_format()
        self.format_commas.set_num_format("0.00")
        self.format_justify = self.workbook.add_format()
        self.format_justify.set_align("justify")

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

        # self.stmnt_df = quandl.get_table('SHARADAR/SF1', ticker=['AAPL','INTC'],dimension="MRY")
        # We'll get all of the data for a given ticker, then filter what we give back
        # Will need more than they ask for calculating CAGR values
        # TODO, there has to be an easy way to pass in how many periods.
        try:
            self.all_inds_df = quandl.get_table(
                "SHARADAR/SF1", ticker=ticker, dimension=dimension
            )
            # Earliest dates now first
            self.all_inds_df.sort_values("datekey", inplace=True)
            self.all_inds_df = self.all_inds_df.tail(periods)

            loc_df = self.all_inds_df.copy()

            logger.debug(
                "get_indicators: df columns  = %s" % (self.all_inds_df.columns.tolist())
            )
            logger.debug("get_indicators: all_inds_df = %s" % (self.all_inds_df.head()))

        except NotFoundError:
            logger.warning("get_indicators: The ticker %s " "is not supported", ticker)
            raise

        # Let's copy the relevant column data to the income statement dataframe
        # the cash flow statement dataframe etc

        self.i_stmnt_df = self.all_inds_df[self.i_stmnt_ind_dict.keys()].copy()
        self.cf_stmnt_df = self.all_inds_df[self.cf_stmnt_ind_dict.keys()].copy()
        self.bal_stmnt_df = self.all_inds_df[self.bal_stmnt_ind_dict.keys()].copy()
        self.metrics_and_ratios_df = self.all_inds_df[
            self.metrics_and_ratios_ind_dict.keys()
        ].copy()
        self.dimension = dimension
        self.periods = periods

        logger.debug("get_indicators: income dataframe = %s" % (self.i_stmnt_df.head()))

        return loc_df

    def get_transposed_and_formatted_i_stmnt(self):
        """ Returns a transposed and formatted partial income statement dataframe with description added
        ready for printing to an excel sheet, or possible via html in the future.

        Returns:
            A dataframe
        """
        stmnt_df = self.i_stmnt_df.copy()
        desc_dict = self.i_stmnt_ind_dict
        description = "Sharadar Income"
        return self._transpose_and_format_stmnt(stmnt_df, desc_dict, description)

    def get_transposed_and_formatted_cf_stmnt(self):
        """ Returns a transposed and formatted subset of the  cash flow statement dataframe
        with description added ready for printing to an excel sheet, or possible via html in the future.

        Returns:
            A dataframe
        """
        stmnt_df = self.cf_stmnt_df.copy()
        desc_dict = self.cf_stmnt_ind_dict
        description = "Sharadar Cash Flow"
        return self._transpose_and_format_stmnt(stmnt_df, desc_dict, description)

    def get_transposed_and_formatted_bal_stmnt(self):
        """ Returns a transposed and formatted subset of the balance sheet statement dataframe
        with description addedready for printing to an excel sheet, or possible via html in the future.

        Returns:
            A dataframe
        """
        stmnt_df = self.bal_stmnt_df.copy()
        desc_dict = self.bal_stmnt_ind_dict
        description = "Sharadar Balance"

        return self._transpose_and_format_stmnt(stmnt_df, desc_dict, description)

    def get_transposed_and_formatted_metrics_and_ratios(self):
        """ Returns a transposed and formatted subset of sharadar metrics and ratios statement dataframe
        with description added ready for printing to an excel sheet, or possible via html in the future.

        Returns:
            A dataframe
        """
        stmnt_df = self.metrics_and_ratios_df.copy()
        desc_dict = self.metrics_and_ratios_ind_dict
        description = "Sharadar Metrics and Ratios"
        return self._transpose_and_format_stmnt(stmnt_df, desc_dict, description)

    def get_transposed_and_formatted_calculated_ratios(self):
        """ Returns a transposed and formatted calculated ratios dataframe with description added
        ready for printing to an excel sheet, or possible via html in the future.

        Returns:
            A dataframe
        """
        stmnt_df = self.calc_ratios_df.copy()
        desc_dict = self.calc_ratios_dict
        description = "Calculated Metrics and Ratios"

        return self._transpose_and_format_stmnt(stmnt_df, desc_dict, description)

    def _transpose_and_format_stmnt(
        self, stmnt_df, description_dict, description_of_indicators
    ):
        """ Transpose the df so that we have the indicators as rows and datefields as columns

            Side effects. Modifies the passed in dataframe.
        """
        # As a precursor to making the datefields as columns we set the datefield as the index.
        # We then transpose the dataframe such that the index becomes the columns and the columns become rows
        stmnt_df.set_index("datekey", inplace=True)

        # Transpose to get this dataframe ready for printing
        # Convert the df so that we have the indicators as the index and datefields as columns
        ret_df = stmnt_df.transpose()

        # The columns are of a dateTime type, we need them to be text in order for the dataframe
        # to excel module to work.
        ret_df.columns = ret_df.columns.map(lambda t: t.strftime("%Y-%m-%d"))

        # Now we want two additional descriptive columns in the dataframe.
        # We want the description of the indicator in one column and the Sharadar code
        # in another.
        # Note that dictionary keys, in this case the Sharadar Indicator code
        # becomes the index of the newly created Pandas series. The values become the data associated
        # with these keys.
        description_s = pd.Series(description_dict)

        # The insert method is what enables us to place the column exactly where we want it.
        ret_df.insert(0, "Description", description_s)

        # For the second column, the sharadar codes, we can get the manes of these from the index of our
        # dataframe. So a variation on the previous case where we inserted a column from a PD series. Here
        # we point to an array like item which the insert method accepts, that of the dataframe index. After
        # the transpose this contains  what were the column i.e the Sharadar indicators.
        #
        # Create a new column using the values from the index, similar to doing a .reset_index
        # but uses an explicit column instead of column 0  which  reset-index  does.
        ret_df.insert(1, description_of_indicators + " " + self.dimension, ret_df.index)

        return ret_df

    def write_df_to_excel_sheet(
        self, dframe, row, col, sheetname, dimension, use_header=True, num_text_cols=2
    ):
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
        dframe.to_excel(
            self.writer,
            sheet_name=sheetname,
            startcol=col,
            startrow=start_row,
            index=False,
            header=False,
        )
        worksheet = self.writer.sheets[sheetname]
        rows_written = len(dframe.index)

        num_cols = len(dframe.columns.values)

        # Format the text columns and the numeric ones following these.
        worksheet.set_column(0, num_text_cols - 1, 40, self.format_justify)
        worksheet.set_column(num_text_cols, num_cols, 16, self.format_justify)

        numeric_data_range = xl_range(
            start_row, col + num_text_cols, start_row + rows_written, col + num_cols
        )
        worksheet.conditional_format(
            numeric_data_range,
            {
                "type": "cell",
                "criteria": "between",
                "minimum": -100,
                "maximum": 100,
                "format": self.format_commas,
            },
        )
        worksheet.conditional_format(
            numeric_data_range,
            {
                "type": "cell",
                "criteria": "not between",
                "minimum": -100,
                "maximum": 100,
                "format": self.format_commas_2dec,
            },
        )

        # Lets figure out CAGR for a given row item
        cagr_col = col + num_cols
        begin_cagr_calc_col = num_text_cols
        end_cagr_calc_col = cagr_col - 1
        for cagr_row in range(start_row, start_row + rows_written):
            # looks like I'll need to use  xl_rowcol_to_cell()
            beg_val = xl_rowcol_to_cell(cagr_row, begin_cagr_calc_col)
            end_val = xl_rowcol_to_cell(cagr_row, end_cagr_calc_col)

            if dimension == "MRY" or dimension == "ARY":
                # We want the number of periods between the years.
                years = end_cagr_calc_col - begin_cagr_calc_col
            else:
                # Theres a quarter between each reporting period
                years = (end_cagr_calc_col - begin_cagr_calc_col) / 4

            formula = '=IFERROR(({end_val}/{beg_val})^(1/{years}) - 1,"")'.format(
                beg_val=beg_val, end_val=end_val, years=years
            )
            worksheet.write(cagr_row, cagr_col, formula)

        # Sparklines make data trends easily visible
        spark_col = cagr_col + 1
        worksheet.set_column(spark_col, spark_col, 20)

        for spark_row in range(start_row, start_row + rows_written):
            numeric_data_row_range = xl_range(
                spark_row, col + num_text_cols, spark_row, col + cagr_col - 1
            )
            worksheet.add_sparkline(
                spark_row,
                spark_col,
                {"range": numeric_data_row_range, "markers": "True"},
            )

        if use_header is True:
            for column, hdr in zip(
                range(col, num_cols + col), dframe.columns.values.tolist()
            ):
                worksheet.write_string(row, column, hdr, self.format_bold)

        rows_written += 1
        return rows_written

    def calc_ratios(self):
        """Obtain some financial ratios and metrics skewed towards credit analysis.
        - Some suggested as useful in the book by Fridson and Alvarez:
        'Financial Statement Analysis'.
        - Others are credit sanity checking or rough approximations to REIT
          specific ratios.

        Returns:
            A dataframe containing financial ratios.
        """
        # Note updated to work on our data in the form where the rows as the dates and the columns are the metricss.
        # we build up each metric as a new column in the calc_ratios df.

        # initialize an empty calc_ratios_df but using the same indexing as our existing dataframes which we've pulled
        # in from sharadar
        self.calc_ratios_df = pd.DataFrame(index=self.i_stmnt_df.index)

        for ratio in self.calc_ratios_dict:
            logger.debug("get_calc_ratios: ratio = %s" % (ratio))
            self._calc_ratios(ratio)

        # This datekey column will be needed later when we transpose the dataframe
        # The sharadar returned dataframes included a datekey column as part of the results.
        self.calc_ratios_df["datekey"] = self.i_stmnt_df["datekey"]

        logger.debug("get_calc_ratios: dataframe = %s" % (self.calc_ratios_df))
        return self.calc_ratios_df.copy()

    def _calc_ratios(self, ratio):
        # Debt to Cash Flow From Operations
        def _debt_cfo_ratio():
            logger.debug(
                "_calc_ratios._debt_cfo_ratio: debt = %s" % (self.bal_stmnt_df["debt"])
            )

            self.calc_ratios_df[ratio] = (
                self.bal_stmnt_df["debt"] / self.cf_stmnt_df["ncfo"]
            )
            return

        # Debt to Equity
        def _debt_equity_ratio():
            logger.debug(
                "_calc_ratios._debt_equity_ratio: debt = %s"
                % (self.bal_stmnt_df["debt"])
            )
            logger.debug(
                "_calc_ratios._debt_equity_ratio: equity = %s"
                % (self.bal_stmnt_df["equity"])
            )
            self.calc_ratios_df[ratio] = (
                self.bal_stmnt_df["debt"] / self.bal_stmnt_df["equity"]
            )
            return

        def _liabilities_equity_ratio():
            logger.debug(
                "_calc_ratios._liabilities_equity:_ratio liabilities = %s"
                % (self.bal_stmnt_df["liabilities"])
            )
            logger.debug(
                "_calc_ratios._liabilities_equity_ratio: equity = %s"
                % (self.bal_stmnt_df["equity"])
            )
            self.calc_ratios_df[ratio] = (
                self.bal_stmnt_df["liabilities"] / self.bal_stmnt_df["equity"]
            )
            return

        # Debt to ebitda
        def _debt_ebitda_ratio():
            self.calc_ratios_df[ratio] = (
                self.bal_stmnt_df["debt"] / self.metrics_and_ratios_df["ebitda"]
            )
            return

        # Debt to ebitda minus CapEx
        def _debt_ebitda_minus_capex_ratio():

            # capex is returned from Sharadar as a -ve number, hence we need to add this to
            # subtract capex
            self.calc_ratios_df[ratio] = self.bal_stmnt_df["debt"] / (
                self.metrics_and_ratios_df["ebitda"] + self.cf_stmnt_df["capex"]
            )
            return

        # Net Debt to ebitda
        def _net_debt_ebitda_ratio():
            self.calc_ratios_df[ratio] = (
                self.bal_stmnt_df["debt"] - self.bal_stmnt_df["cashnequsd"]
            ) / self.metrics_and_ratios_df["ebitda"]
            return

        # Net Debt to ebitda minus CapEx
        def _net_debt_ebitda_minus_capex_ratio():
            # capex is returned from Sharadar as a -ve number, hence we need to add this to
            # subtract capex
            self.calc_ratios_df[ratio] = (
                self.bal_stmnt_df["debt"] - self.bal_stmnt_df["cashnequsd"]
            ) / (self.metrics_and_ratios_df["ebitda"] + self.cf_stmnt_df["capex"])
            return

        # Depreciation to Cash Flow From Operations Pg 278.
        def _depreciation_cfo_ratio():
            self.calc_ratios_df[ratio] = (
                self.cf_stmnt_df["depamor"] / self.cf_stmnt_df["ncfo"]
            )
            return

        def _depreciation_revenue_ratio():
            self.calc_ratios_df[ratio] = (
                self.cf_stmnt_df["depamor"] / self.i_stmnt_df["revenue"]
            )
            return

        def _debt_to_total_capital():
            self.calc_ratios_df[ratio] = (
                self.bal_stmnt_df["debt"] / self.metrics_and_ratios_df["invcapavg"]
            )
            return

        def _roic():
            self.calc_ratios_df[ratio] = (
                self.i_stmnt_df["ebit"] / self.metrics_and_ratios_df["invcapavg"]
            )

        #        self.database =  database

        # Times Interest coverage aka fixed charge coverage Pg 278.
        # (Net Income + Income taxes + Interest Expense)/(Interest expense + Capitalized Interest)
        # Cannot see how to get capitalized interest from the API so that term is excluded.
        # This is the same as ebit to Interest Expense
        def _ebit_interest_coverage():
            self.calc_ratios_df[ratio] = (
                self.i_stmnt_df["ebit"] / self.i_stmnt_df["intexp"]
            )
            return

        def _ebitda_interest_coverage():
            self.calc_ratios_df[ratio] = (
                self.metrics_and_ratios_df["ebitda"] / self.i_stmnt_df["intexp"]
            )
            return

        def _ebitda_minus_capex_interest_coverage():
            # Recall that capex is returned from Sharadar as a -ve number.
            self.calc_ratios_df[ratio] = (
                self.metrics_and_ratios_df["ebitda"] + self.cf_stmnt_df["capex"]
            ) / self.i_stmnt_df["intexp"]
            return

        def _rough_ffo():
            self.calc_ratios_df[ratio] = (
                self.i_stmnt_df["netinc"] + self.cf_stmnt_df["depamor"]
            )
            return

        def _rough_affo():
            # capex is returned from Quandl as a -ve number, hence we add this to
            # subtract capex
            self.calc_ratios_df[ratio] = (
                self.i_stmnt_df["netinc"]
                + self.cf_stmnt_df["depamor"]
                + self.cf_stmnt_df["capex"]
            )
            return

        def _rough_ffo_dividend_payout_ratio():
            self.calc_ratios_df[ratio] = self.cf_stmnt_df["ncfdiv"] / (
                self.i_stmnt_df["netinc"] + self.cf_stmnt_df["depamor"]
            )
            return

        def _rough_affo_dividend_payout_ratio():
            self.calc_ratios_df[ratio] = self.cf_stmnt_df["ncfdiv"] / (
                self.i_stmnt_df["netinc"]
                + self.cf_stmnt_df["depamor"]
                + self.cf_stmnt_df["capex"]
            )
            return

        def _income_dividend_payout_ratio():
            # negating since ncfdiv is returned as a negative number
            self.calc_ratios_df[ratio] = (
                -self.cf_stmnt_df["ncfdiv"] / self.i_stmnt_df["netinc"]
            )
            return
# TODO add some conditional logig to use the fullydiluted shares value when it
# is provided
        def _price_rough_ffo_ps_ratio():
            self.calc_ratios_df[ratio] = self.i_stmnt_df["price"] / (
                self.calc_ratios_df["rough_ffo"] / self.bal_stmnt_df["shareswa"]
            )
            return

        def _rough_ffo_ps():
            self.calc_ratios_df[ratio] = (
                self.calc_ratios_df["rough_ffo"] / self.bal_stmnt_df["shareswa"]
            )
            return

        def _cfo_ps():
            self.calc_ratios_df[ratio] = (
                self.cf_stmnt_df["ncfo"] / self.bal_stmnt_df["shareswa"]
            )
            return

        def _opinc_ps():
            self.calc_ratios_df[ratio] = (
                self.i_stmnt_df["opinc"] / self.bal_stmnt_df["shareswa"]
            )
            return


        def _fcf_ps():
            self.calc_ratios_df[ratio] = (
                self.metrics_and_ratios_df["fcf"] / self.bal_stmnt_df["shareswa"]
            )
            return

        def _ev_opinc_ratio():
            self.calc_ratios_df[ratio] = (
                self.metrics_and_ratios_df["ev"] / self.i_stmnt_df["opinc"]
            )
            return

        # Kenneth Jeffrey Marshal, author of Good Stocks Cheap, definition
        # of capital employed. He has two defnitions, one where cash is
        # subtracted and one where it's not. Accrued expenses should be
        # substracted but Is not available in the Sharadar API, probably a
        # scour the footnotes thing if really wanted to include this.
        def _kjm_capital_employed_sub_cash():
            self.calc_ratios_df[ratio] = (
                self.bal_stmnt_df["assets"]
                - self.bal_stmnt_df["cashnequsd"]
                - self.bal_stmnt_df["payables"]
                - self.bal_stmnt_df["deferredrev"]
            )
            return

        def _kjm_capital_employed_with_cash():
            self.calc_ratios_df[ratio] = (
                self.bal_stmnt_df["assets"]
                - self.bal_stmnt_df["payables"]
                - self.bal_stmnt_df["deferredrev"]
            )
            return

        def _kjm_return_on_capital_employed_sub_cash():
            self.calc_ratios_df[ratio] = (
                self.i_stmnt_df["opinc"] / self.calc_ratios_df["kjm_capital_employed_sub_cash"]
            )
            return

        def _kjm_return_on_capital_employed_with_cash():
            self.calc_ratios_df[ratio] = (
                self.i_stmnt_df["opinc"] / self.calc_ratios_df["kjm_capital_employed_with_cash"]
            )
            return

        def _kjm_fcf_return_on_capital_employed_sub_cash():
            self.calc_ratios_df[ratio] = (
                self.metrics_and_ratios_df["fcf"] / self.calc_ratios_df["kjm_capital_employed_sub_cash"]
            )
            return

        def _kjm_fcf_return_on_capital_employed_with_cash():
            self.calc_ratios_df[ratio] = (
                self.metrics_and_ratios_df["fcf"] / self.calc_ratios_df["kjm_capital_employed_with_cash"]
            )
            return

        def _kjm_delta_oi_fds():
            self.calc_ratios_df[ratio] = (
                    self.calc_ratios_df["opinc_ps"].pct_change()
            )
            return

        def _kjm_delta_fcf_fds():
            self.calc_ratios_df[ratio] = (
                    self.calc_ratios_df["fcf_ps"].pct_change()
            )
            return

        def _kjm_delta_bv_fds():
            self.calc_ratios_df[ratio] = (
                    self.bal_stmnt_df["equity"].pct_change()
            )
            return

        def _kjm_delta_tbv_fds():
            self.calc_ratios_df[ratio] = (
                    (self.bal_stmnt_df["equity"] - self.bal_stmnt_df["intangibles"]).pct_change()
            )
            return

        def _dividends_free_cash_flow_ratio():
            self.calc_ratios_df[ratio] = (
                -self.cf_stmnt_df["ncfdiv"] / self.metrics_and_ratios_df["fcf"]
            )
            return

        def _preferred_free_cash_flow_ratio():
            self.calc_ratios_df[ratio] = (
                self.i_stmnt_df["prefdivis"] / self.metrics_and_ratios_df["fcf"]
            )
            return

        def _operating_margin():
            self.calc_ratios_df[ratio] = (
                self.i_stmnt_df["opinc"] / self.i_stmnt_df["revenue"]
            )
            return

        def _sg_and_a_gross_profit_ratio():
            self.calc_ratios_df[ratio] = self.i_stmnt_df["sgna"] / self.i_stmnt_df["gp"]
            return

        def _ltdebt_cfo_ratio():
            self.calc_ratios_df[ratio] = (
                self.bal_stmnt_df["debtnc"] / self.cf_stmnt_df["ncfo"]
            )
            return

        def _ltdebt_earnings_ratio():
            self.calc_ratios_df[ratio] = (
                self.bal_stmnt_df["debtnc"] / self.i_stmnt_df["netinc"]
            )
            return

        def _free_cash_flow_conversion_ratio():
            self.calc_ratios_df[ratio] = (
                self.metrics_and_ratios_df["fcf"] / self.metrics_and_ratios_df["ebitda"]
            )
            return

        # Pg 290 of Creative Cash Flow Reporting, Mumford et al.
        def _excess_cash_margin_ratio():
            self.calc_ratios_df[ratio] = (
                (self.cf_stmnt_df["ncfo"] - self.i_stmnt_df["opinc"])
                * 100
                / self.i_stmnt_df["revenue"]
            )
            return

        def _interest_to_cfo_plus_interest_coverage():
            self.calc_ratios_df[ratio] = self.i_stmnt_df["intexp"] / (
                self.cf_stmnt_df["ncfo"] + self.i_stmnt_df["intexp"]
            )
            return

        def _dividends_cfo_ratio():
            # negating since ncfdiv is returned as a negative number
            self.calc_ratios_df[ratio] = (
                -self.cf_stmnt_df["ncfdiv"] / self.cf_stmnt_df["ncfo"]
            )
            return

        def _preferred_cfo_ratio():
            self.calc_ratios_df[ratio] = (
                self.i_stmnt_df["prefdivis"] / self.cf_stmnt_df["ncfo"]
            )
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
            "opinc_ps": _opinc_ps,
            "cfo_ps": _cfo_ps,
            "fcf_ps": _fcf_ps,
            "ev_opinc_ratio": _ev_opinc_ratio,
            "dividends_free_cash_flow_ratio": _dividends_free_cash_flow_ratio,
            "preferred_free_cash_flow_ratio": _preferred_free_cash_flow_ratio,
            "operating_margin": _operating_margin,
            "sg_and_a_gross_profit_ratio": _sg_and_a_gross_profit_ratio,
            "ltdebt_cfo_ratio": _ltdebt_cfo_ratio,
            "ltdebt_earnings_ratio": _ltdebt_earnings_ratio,
            "free_cash_flow_conversion_ratio": _free_cash_flow_conversion_ratio,
            "excess_cash_margin_ratio": _excess_cash_margin_ratio,
            "interest_to_cfo_plus_interest_coverage": _interest_to_cfo_plus_interest_coverage,
            "dividends_cfo_ratio": _dividends_cfo_ratio,
            "preferred_cfo_ratio": _preferred_cfo_ratio,
            "kjm_capital_employed_sub_cash": _kjm_capital_employed_sub_cash,
            "kjm_capital_employed_with_cash": _kjm_capital_employed_with_cash,
            "kjm_return_on_capital_employed_sub_cash": _kjm_return_on_capital_employed_sub_cash,
            "kjm_return_on_capital_employed_with_cash": _kjm_return_on_capital_employed_with_cash,
            "kjm_fcf_return_on_capital_employed_sub_cash": _kjm_fcf_return_on_capital_employed_sub_cash,
            "kjm_fcf_return_on_capital_employed_with_cash": _kjm_fcf_return_on_capital_employed_with_cash,
            "kjm_delta_oi_fds": _kjm_delta_oi_fds,
            "kjm_delta_fcf_fds": _kjm_delta_fcf_fds,
            "kjm_delta_bv_fds": _kjm_delta_bv_fds,
            "kjm_delta_tbv_fds": _kjm_delta_tbv_fds
        }

        # Get the function from switcher dictionary
        func = switcher.get(ratio, lambda: NotImplementedError)
        # Execute the function
        return func()


class SharadarFundamentals(Fundamentals_ng):

    # Locally calculated by this package. For each ratio or metric in this
    # table, there's a routine to calculate the value from the quandl API provided
    # statement indicator value.
    # The first item in each tuple is the Sharadar Code, the second is
    # a description.

    # Income Statement Indicator Quandl/Sharadar Codes
    I_STMNT_IND = [
        ("datekey", "SEC filing date"),
        ("revenue", "Revenues"),
        ("cor","Cost of Revenue"),
        ("gp", "Gross Profit"),
        ("sgna", "Sales General and Admin"),
        ("rnd", "Research and Development Expense"),
        ("opex", "Operating Expenses"),
        ("intexp", "Interest Expense"),
        ("taxexp", "Tax Expense"),
        ("netincdis", "Net Loss Income from Discontinued Operations "),
        ("netincnci", "Net Income to Non-Controlling Interests"),
        ("opinc", "Operating Income"),
        ("ebit", "Earnings Before Interest and Taxes"),
        ("netinc", "Net Income"),
        ("prefdivis", "Preferred Dividends"),
        ("netinccmn", "Net Income to Common (after prefs paid)"),
        ("epsdil", "Earnings Per Share Diluted"),
        ("price", "Price per Share"),
        ("shareswadil", "Weighted Average Shares Diluted"),
        ("dps", "Dividends per Basic Common Share"),
    ]

    # Cash Flow Statement Indicator Quandl/Sharadar Codes
    CF_STMNT_IND = [
        ("datekey", "SEC filing date"),
        ("depamor", "Depreciation and Amortization"),
        ("ncfo", "Net Cash Flow From Operations"),
        ("ncfi", "Net Cash Flow From Investing"),
        ("capex", "Capital Expenditure"),
        ("ncff", "Net Cash Flow From Financing"),
        ("ncfdiv", "Payment of Dividends and Other Cash Distributions"),
    ]

    # Balance Statement Indicator Quandl/Sharadar Codes
    BAL_STMNT_IND = [
        ("datekey", "SEC filing date"),
        ("assets", "Total Assets"),
        ("assetsnc", "Non Current Assets"),
        ("cashnequsd", "Cash and Equivalents (USD)"),
        ("deferredrev", "Deferred Revenue"),
        ("intangibles", "Intangibles"),
        ("debt", "Total Debt"),
        ("debtnc", "Long Term  Debt"),
        ("liabilities", "Total Liabilities"),
        ("payables", "Trade and Non Trade Payables"),
        ("receivables", "Trade and Non Trade Receivables"),
        ("retearn", "Retained Earnings"),
        ("equity", "Shareholders Equity"),
        ("shareswa", "Weighted Average Shares"),
        ("workingcapital", "Working Capital"),
    ]
    # Metrics and Ratio  Indicator Quandl/Sharadar Codes
    METRICS_AND_RATIOS_IND = [
        ("datekey", "SEC filing date"),
        #    ('DE', 'Debt to Equity Ratio'), Needs to be locally calculated when
        #    using TTM figures
        ("ev", "Enterprise Value"),
        # evebitda only returned for the MRT period, the default for SF1
        ("evebitda", "Enterprise Value divided by ebitda"),
        ("pe", "Price Earnings Damodaran: Market Cap / Net Income"),
        ("ps", "Price Sales Damodaran: Market Cap / Revenue"),
        ("assetturnover", "Revenue / Assets average"),
        ("roa", "Return on Assets: Net Income / Average Assets"),
        ("roe", "Return on Equity: Net Income / Average Equity"),
        ("ros", "Return on Sales: ebit / Revenue"),
        ("ebitda", "Earnings Before Interest Taxes & Depreciation & Amortization"),
        ("fcf", "Free Cash Flow: CFO - CapEx"),
        ("invcapavg", "Invested Capital"),
        ("roic", "Return On Invested Capital"),
        ("grossmargin", "Gross Margin: Gross Profit/ Revenue"),
        ("netmargin", "Net Margin: Net Income/ Revenue"),
    ]

    CALCULATED_RATIOS = [
        ("kjm_capital_employed_sub_cash", "Kenneth J Marshal Capital Employed Subtract Cash"),
        ("kjm_capital_employed_with_cash", "Kenneth J Marshal Capital Employed With Cash"),
        ("kjm_return_on_capital_employed_sub_cash", "KJM Return on Capital Employed subtract Cash"),
        ("kjm_return_on_capital_employed_with_cash", "KJM Return on Capital Employed With Cash"),
        ("kjm_fcf_return_on_capital_employed_with_cash", "KJM Free Cash Flow ROCE With Cash"),
        ("kjm_fcf_return_on_capital_employed_sub_cash", "KJM Free Cash FLow Subtract Cash"),
        ("opinc_ps", "Operating Income Per Share"),
        ("cfo_ps", "Cash Flow from Operations Per Share"),
        ("fcf_ps", "Free Cash Flow per Share"),
        ("kjm_delta_oi_fds", "YoY change in Operating Income per Fully Diluted Share"),
        ("kjm_delta_fcf_fds", "YoY change in Free Cash Flow per Fully Diluted Share"),
        ("kjm_delta_bv_fds", "YoY change in Book Value per Fully Diluted Share"),
        ("kjm_delta_tbv_fds", "YoY change in Tangible Book Value per Fully Diluted Share"),
        ("liabilities_equity_ratio", "Total Liabilities / Shareholders Equity"),
        ("debt_ebitda_ratio", "Total Debt / ebitda"),
        ("debt_ebitda_minus_capex_ratio", "Total Debt / (ebitda - CapEx)"),
        ("net_debt_ebitda_ratio", "Net Debt / ebitda"),
        ("net_debt_ebitda_minus_capex_ratio", "Net Debt / (ebitda - CapEx)"),
        ("debt_equity_ratio", "Total Debt / Shareholders Equity"),
        ("ebit_interest_coverage", "ebit / Interest Expense"),
        ("ebitda_interest_coverage", "ebitda / Interest Expense"),
        ("ebitda_minus_capex_interest_coverage", "ebitda - CapEx / Interest Expense"),
        ("interest_to_cfo_plus_interest_coverage", "Interest / (CFO + Interest"),
        ("debt_to_total_capital", "Total Debt / Invested Capital"),
        ("debt_cfo_ratio", "Total Debt / Cash Flow From Operations"),
        ("ltdebt_cfo_ratio", "Long Term Debt / Cash Flow From Operations"),
        ("ltdebt_earnings_ratio", "Long Term Debt / Income"),
        ("income_dividend_payout_ratio", "Dividends / Net Income"),
        ("dividends_cfo_ratio", "Dividends/CFO"),
        ("preferred_cfo_ratio", "Preferred Payments/CFO"),
        ("dividends_free_cash_flow_ratio", "Dividends/fcf"),
        ("preferred_free_cash_flow_ratio", "Preferred Payments/fcf"),
        ("operating_margin", "Operating Margin: (Gross Profit - Opex)/ Revenue"),
        ("sg_and_a_gross_profit_ratio", "SG&A to Gross Profit Ratio"),
        ("ev_opinc_ratio", "Acquirers Multiple: Enterprise Value / Operating Income"),
        (
            "return_on_invested_capital",
            "Return on Invested Capital: ebit / Invested Capital",
        ),
        
        ("free_cash_flow_conversion_ratio", "Free Cash Flow Conversion Ratio"),
        ("excess_cash_margin_ratio", "Excess Cash Margin Ratio"),
        ("depreciation_revenue_ratio", "Depreciation / Revenue"),
        ("depreciation_cfo_ratio", "Depreciation / Cash Flow From Operations"),
        # fcf is already levered since CFO  already includes the effect of interest
        # payments.
        #        ("free_cash_flow_levered", 'fcf-Levered: fcf - Interest Expenses'),
        (
            "rough_ffo",
            "Rough FFO: Net Income plus Depreciation (missing cap gain from RE sales adjust)",
        ),
        ("rough_ffo_ps", "Rough FFO per Share"),
        ("price_rough_ffo_ps_ratio", "Price divided by rough_ffo_ps"),
        ("rough_ffo_dividend_payout_ratio", "Dividends / rough_ffo"),
    ]

    def __init__(self, database, writer):
        Fundamentals_ng.__init__(
            self,
            database,
            self.I_STMNT_IND,
            self.CF_STMNT_IND,
            self.BAL_STMNT_IND,
            self.METRICS_AND_RATIOS_IND,
            self.CALCULATED_RATIOS,
            writer,
        )


def stock_xlsx(outfile, stocks, database, dimension, periods):
    # Excel Housekeeping first
    # The writer contains books and sheets
    writer = pd.ExcelWriter(outfile, engine="xlsxwriter", date_format="d mmmm yyyy")

    # Get a stmnt dataframe, a quandl ratios dataframe and our calculated ratios dataframe
    # for each of these frames write into a separate worksheet per stock
    for stock in stocks:
        fund = SharadarFundamentals(database, writer)

        logger.info("Processing the stock %s", stock)

        shtname = "{}".format(stock)

        try:
            fund.get_indicators(stock, dimension, periods)
        except NotFoundError:
            logger.warning(
                "NotFoundError when getting indicators for the stock %s", stock
            )
            continue

        # Now calculate some of the additional ratios for credit analysis
        fund.calc_ratios()

        row, col = 0, 0

        i_stmnt_trans_df = fund.get_transposed_and_formatted_i_stmnt()
        rows_written = fund.write_df_to_excel_sheet(
            i_stmnt_trans_df, row, col, shtname, dimension, use_header=True
        )
        row = row + rows_written + 1


        cf_stmnt_trans_df = fund.get_transposed_and_formatted_cf_stmnt()
        rows_written = fund.write_df_to_excel_sheet(
            cf_stmnt_trans_df, row, col, shtname, dimension, use_header=True
        )
        row = row + rows_written + 1

        bal_stmnt_trans_df = fund.get_transposed_and_formatted_bal_stmnt()
        rows_written = fund.write_df_to_excel_sheet(
            bal_stmnt_trans_df, row, col, shtname, dimension, use_header=True
        )
        row = row + rows_written + 1

        # Now for the metrics and ratios from the quandl API
        metrics_and_ratios_df = fund.get_transposed_and_formatted_metrics_and_ratios()
        rows_written = fund.write_df_to_excel_sheet(
            metrics_and_ratios_df, row, col, shtname, dimension, use_header=True
        )
        row = row + rows_written + 2

        calculated_ratios_df = fund.get_transposed_and_formatted_calculated_ratios()
        rows_written = fund.write_df_to_excel_sheet(
            calculated_ratios_df, row, col, shtname, dimension
        )
        logger.info("Processed the stock %s", stock)

    writer.save()

    # TODO CAGR values for some indicators e.g. Revenue,FCF,OI. Want to have a
    # dataframe with ratios along the top and with CAGR values as some of these
    # columns e.g  OCF-5-CAGR. The rows will be tickers and this combined df will 
    # be written to a table.
    # - Use term QoQ for the quarterly change in a value.
    # - Use the term YoY for the yearly change
    # - Then for some indicators  there's a longer trend, the 5YrCAGR
    # - Do we get rid of the CAGR excel calcs, probably.
    # - Have to fgure out how to incorporate CAGRS and YOY.
    # - Would need to add to the initial tuple lists to ensure we had a description for each cagr tuple.
    # So maybe go sparing on CAGRS, and look for ones I want to compare in the future summary sheet work

def main():

    # stocks = ['SPG', 'WPC', 'KIM', 'SKT', 'NNN', 'STOR']
    stocks = ["AAPL"]

    periods = 5

    outfile = "quandl_ratios.xlsx"
    # stock_xlsx(outfile, stocks, "SF0", 'MRY', periods)
    stock_xlsx(outfile, stocks, "SF0", "MRY", periods)


if __name__ == "__main__":
    main()
