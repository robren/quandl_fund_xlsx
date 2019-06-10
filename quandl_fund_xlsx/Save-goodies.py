As part of our refactoring, apply this step after we've now got the df in one shot.

Later on we'll only do the transpose before writing to excel.



       # We now have a bunch of indicator columns and a single datekey column
        # What we want is the data to have a set of date columns with
        # indicators as each row.
        # Make the datekey column the index.
        dframe.set_index('datekey',inplace=True)
        # So... transpose such that the indicator  columns  become the rows
        # and dates are the columns
        dframe = dframe.transpose()
        dframe.columns = dframe.columns.map(lambda t: t.strftime('%Y-%m-%d'))
