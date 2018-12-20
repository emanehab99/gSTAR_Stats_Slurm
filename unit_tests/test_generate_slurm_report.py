import usagereport.export.reportFromDB

import pytest

from usagereport.export import reportFromDB


@pytest.fixture(scope='class')
def report(dbconfig):
    report = reportFromDB.Report(dbconfig=dbConfig)
    yield report.finalize()

class TestGenerateSlurmReport():

    def test_db_connection():
        print('Connected')