import sys
import datetime
from builtins import Exception, len

import pandas as pd

from reportFromDB import UserInfoReport
from statsConfig import readdbconfig

from generateReport import ReportFormat

from reportFromDB import Report, MALE, FEMALE, ASTRONOMY, STUDENT


if __name__=='__main__':

    filename = 'files/user_info.csv'

    data = pd.read_csv(filename, sep=";", header=None)
    data.columns = ['username', 'jobs', 'groups', 'astro']
    astrouserdata = data[data.astro == True]
    print(astrouserdata.shape)

    dbconfig = readdbconfig('db_config.ini')

    try:
        myreport = UserInfoReport(dbconfig, astrouserdata)
        usersinfo = myreport.get_users_info()

        for u in astrouserdata.username:
            if u not in usersinfo.username.unique():
                print(u)
        myreport.finalize()

    except Exception as exp:
        raise exp




