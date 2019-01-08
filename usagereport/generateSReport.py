import sys
import datetime

import pandas as pd
import mysql.connector

from usagereport.export.taoreportfromdb import TAOreport
from usagereport.statsConfig import readdbconfig

from usagereport.generateReport import ReportFormat

from usagereport.export.reportFromDB import Report
from usagereport import statsConfig
from usagereport import database

if __name__ == '__main__':

    # calculate reporting period/quarter based on current date

    filename = ''
    startdate = ''
    enddate = ''

    if len(sys.argv) > 1:
        filename = (sys.argv[1])
    # else:
    #     filename = input('Enter File Name: ')
    #     startdate = input('Enter start date (YYYY-mm-dd): ')
    #     enddate = input('Enter end date (YYYY-mm-dd): ')

    # print(filename)

    # print(len(text.splitlines()))

    # filename = 'files/utilisation_q4.txt'

    # data = pd.read_csv(filename, sep="|", header=None)
    # data.columns = ['Cluster', 'Account', 'Login', 'Proper Name', 'Used', 'Energy']
    # print(data.shape)
    #
    # print(data['Account'])

    filename = statsConfig.read_path('config.ini') + '/user_utilisation_2018_q4.txt'
    print(filename)
    startdate = '2018-10-01'
    enddate = '2018-12-31'

    data = pd.read_csv(filename, sep="|", header=None)
    data.columns = ['Cluster', 'Login', 'Name', 'Account', 'Used', 'Energy']
    print(data.shape)

    # data.set_index(['Login', 'Account'])
    # print(data[['Login', 'Account', 'Used']])
    # print(data[data.Login =='msinha'].Used.sum())


    #
    # Generating LaTex file (tex and pdf)
    # ReportFormat().generateReport(Report(dbconfig, startdate, enddate, type='slurm', slurmdata=data),
    #                               TAOreport(dbconfig, startdate, enddate))

    try:
        # myreport = Report(dbconfig, startdate, enddate, type='slurm', slurmdata=data)
        # myreport.getSlurmProjectUsagePercent()
        # print("Total usage: {0}".format(myreport.totalusage))
        # print("OzSTAR users: {0}".format(myreport.getOzSTARUsersCount()))
        # print("OzSTAR Swinburne Users: {0}".format(myreport.getOzSTARSwinAstronomersCount()))
        # print("Swinburne Active users: {0}".format(myreport.getSlurmActiveSwinAstronomersCount()))
        # print("Active users count: {0}".format(len(myreport.slurmactiveusers)))
        # print("Active astronomy males count: {0}".format(myreport.getSlurmActiveUsersCount([
        #         ('gum_department.is_astronomy', ASTRONOMY), ('gum_user.gender', MALE)])))

        # print(myreport.getSlurmProjectUsagePercent())
        # print(myreport.getSlurmInstitutionUsagePercent())
        # print(myreport.getSlurmUsageByDemographic())

        dbconfig = readdbconfig('db_config.ini')
        accountsdb = mysql.connector.connect(**dbconfig['mysql'])
        print('connected to Accounts DB')
        with Report(accountsdb, startdate, enddate, type='slurm', slurmdata=data) as screport:
            taoreport = TAOreport(dbconfig, datetime.date(2018, 10, 1), datetime.date(2018, 12, 31))
            ReportFormat().generateSlurmReport(screport, taoreport)

    except Exception as exp:
        print("Error generating report {}".format(exp))
        raise exp