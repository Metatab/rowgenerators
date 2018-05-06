# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# MIT License, included in this distribution as LICENSE.txt

""" """

from rowgenerators.source import Source
from xlrd import open_workbook, XLRDError
from rowgenerators.exceptions import RowGeneratorError

class ExcelSource(Source):
    """Generate rows from an excel file"""

    def __init__(self, ref, cache=None, working_dir=None, env=None, **kwargs):
        super().__init__(ref, cache, working_dir, **kwargs)

        self.url = ref

        # It is supposed to be segment. Or file. Probably segment. Well, one of them.
        #ts = self.url.target_segment or self.url.target_file
        #if not ts:
        #    raise RowGeneratorError("URL does not include target file in fragment: {}".format(self.url))

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


        self.start()

        wb = open_workbook(filename=str(self.url.fspath))

        ts = self.url.target_segment

        # Without this check, failure to provide a target_segment will cause the return
        # of the first worksheet.

        #if not ts:
        #    raise RowGeneratorError("URL does not include target file in fragment: {}".format(self.url))

        try:
            try:
                s = wb.sheets()[int(ts) if self.url.target_segment else 0]
            except ValueError:  # Segment is the workbook name, not the number
                s = wb.sheet_by_name(ts)
        except XLRDError as e:
            raise RowGeneratorError("Failed to open Excel workbook: '{}' ".format(e))

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


