# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# MIT License, included in this distribution as LICENSE.txt

""" """

from rowgenerators.source import Source

class ExcelSource(Source):
    """Generate rows from an excel file"""

    @staticmethod
    def srow_to_list(row_num, s):
        """Convert a sheet row to a list"""

        values = []

        try:
            for col in range(s.ncols):
                values.append(s.cell(row_num, col).value)
        except:
            raise

        return values

    def __iter__(self):
        """Iterate over all of the lines in the file"""
        from xlrd import open_workbook

        self.start()

        wb = open_workbook(filename=self.url.target_file)

        try:
            s = wb.sheets()[int(self.url.target_segment) if self.url.target_segment else 0]
        except ValueError:  # Segment is the workbook name, not the number
            s = wb.sheet_by_name(self.url.target_segment)

        for i in range(0, s.nrows):
            yield self.srow_to_list(i, s)

        self.finish()

    @property
    def children(self):
        """Return the sheet names from the workbook """
        from xlrd import open_workbook

        wb = open_workbook(filename=self.url.target_file)

        sheets = wb.sheet_names()

        return sheets

    @staticmethod
    def make_excel_date_caster(file_name):
        """Make a date caster function that can convert dates from a particular workbook. This is required
        because dates in Excel workbooks are stupid. """

        from xlrd import open_workbook

        wb = open_workbook(file_name)
        datemode = wb.datemode

        def excel_date(v):
            from xlrd import xldate_as_tuple
            import datetime

            try:

                year, month, day, hour, minute, second = xldate_as_tuple(float(v), datemode)
                return datetime.date(year, month, day)
            except ValueError:
                # Could be actually a string, not a float. Because Excel dates are completely broken.
                from dateutil import parser

                try:
                    return parser.parse(v).date()
                except ValueError:
                    return None

        return excel_date


