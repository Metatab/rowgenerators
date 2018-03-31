# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# MIT License, included in this distribution as LICENSE.txt

""" """

from rowgenerators.generator.csv import CsvSource
from rowgenerators.source import Source


class GooglePublicSource(CsvSource):
    url_template = 'https://docs.google.com/spreadsheets/d/{key}/export?format=csv&gid={gid}'

    @classmethod
    def download_url(cls, spec):
        return cls.url_template.format(key=spec.netloc)



class GoogleAuthenticatedSource(Source):
    """Generate rows from a Google spreadsheet source that requires authentication

    To read a GoogleSpreadsheet, you'll need to have an account entry for google_spreadsheets, and the
    spreadsheet must be shared with the client email defined in the credentials.

    Visit http://gspread.readthedocs.org/en/latest/oauth2.html to learn how to generate the credential file, then
    copy the entire contents of the file into the a 'google_spreadsheets' key in the accounts file.

    Them share the google spreadsheet document with the email addressed defined in the 'client_email' entry of
    the credentials.

    """

    def __iter__(self):
        """Iterate over all of the lines in the file"""

        self.start()

        for row in self._fstor.get_all_values():
            yield row

        self.finish()

