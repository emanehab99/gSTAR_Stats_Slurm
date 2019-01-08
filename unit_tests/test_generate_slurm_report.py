import usagereport.export.reportFromDB

import pytest

from usagereport.export import reportFromDB


def test_db_connection(self, accountsdbcursor):
    accountsdbcursor.execute('select Version')
    print(accountsdbcursor.fetchone())
    print('Connected')

@pytest.fixture(scope='class')
def screport(accountsdbcon, reportingperiod, slurmstats):
    screport = reportFromDB.Report(accountsdbcon, reportingperiod['startdate'], reportingperiod['enddate'], 'slurm', slurmstats)
    print("Total usage in quarter: {} ".format(screport.totalusage))
    yield screport
    screport.finalize()

class TestGenerateSlurmReport():

    def test_users_information_dataframe_shape(self, screport):
        print(screport.usersinfo.shape)