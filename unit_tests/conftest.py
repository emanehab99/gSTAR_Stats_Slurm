import pytest
import mysql
import pandas as pd


from usagereport.statsConfig import readdbconfig
from usagereport.statsConfig import read_reportingperiod
from usagereport.statsConfig import read_path


@pytest.fixture(scope='module')
def dbconfig():
    dbconfig = readdbconfig('db_config.ini')
    yield dbconfig


@pytest.fixture(scope='module')
def accountsdbcon(accountsdbconfig):
    mysqldb = accountsdbconfig['mysql']
    accountsdbcon = mysql.connector.connect(**mysqldb)
    yield accountsdbcon
    accountsdbcursor.close()
    accountsdbcon.close()


@pytest.fixture
def accountsdbcursor(accountsdbcon):
    accountsdbcursor = accountsdbcon.cursor()
    yield accountsdbcursor


@pytest.fixture(scope='module')
def reportingperiod():
    reportingperiod = read_reportingperiod('config.ini')
    return reportingperiod


@pytest.fixture(scope='module')
def slurmstats():
    filename = read_path()
    slurmstats = pd.read_csv(filename, sep="|", header=None)
    slurmstats.columns = ['Cluster', 'Login', 'Name', 'Account', 'Used', 'Energy']
    return slurmstats



