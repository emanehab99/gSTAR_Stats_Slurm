import sys
import datetime
from builtins import Exception, len

import pandas as pd

from reportFromDB import Report
from taoreportfromdb import TAOreport, TAO5Report
from statsConfig import readdbconfig

from generateReport import ReportFormat

from reportFromDB import Report, MALE, FEMALE, ASTRONOMY, STUDENT


if __name__=='__main__':

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

    filename = 'files/user_utilisation_2019_q3.txt'
    startdate = '2019-07-01'
    enddate = '2019-09-30'

    data = pd.read_csv(filename, sep="|", header=None)
    data.columns = ['Cluster', 'Login', 'Name', 'Account', 'Used', 'Energy']
    print(data.shape)

    # data.set_index(['Login', 'Account'])
    # print(data[['Login', 'Account', 'Used']])
    # print(data[data.Login =='msinha'].Used.sum())


    dbconfig = readdbconfig('db_config.ini')
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

        # ReportFormat().generateSlurmReport(Report(dbconfig, startdate, enddate, type='slurm', slurmdata=data),
        #                                    TAOreport(dbconfig, datetime.date(2019, 1, 1), datetime.date(2019, 3, 31)))


        ReportFormat().generateSlurmReport(Report(dbconfig, startdate, enddate, type='slurm', slurmdata=data),
        TAO5Report(dbconfig, datetime.date(2019, 7, 1), datetime.date(2019, 9, 30), ['files/report_FY1920_Q1_NCI.txt', 'files/report_FY1920_Q1_OZSTAR.txt']))

    except Exception as exp:
        raise exp




