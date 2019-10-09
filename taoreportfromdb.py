from builtins import round, Exception

import psycopg2
import mysql.connector
import pandas as pd

class TAOreport:

    def __init__(self, dbconfig, startdate, enddate):

        """
        Initialize TAO MySQL and Postgres database connections, report start and end dates
        
        :param dbconfig: a dictionary of dictionaries, all database configurations
        :param startdate: report start date, start of quarter
        :param enddate: report start date, end of quarter
        """

        try:
            # TAO Postgres database configuration from dbconfig dictionary
            postgresdb = dbconfig['tao-postgres']

            # TAO MySQL database configuration from dbconfig dictionary
            mysqldb = dbconfig['tao-mysql']

            # connect to Postgres database
            self.pgconn = psycopg2.connect(**postgresdb)
            self.pgcursor = self.pgconn.cursor()

            # connect to MySQL database
            self.mysqlcon = mysql.connector.connect(**mysqldb)
            self.mysqlcursor = self.mysqlcon.cursor()
            print('connected to DB')

            # TAO admin users, to be discarded from Stats
            self.adminusers = dbconfig['tao-admins']
            self.startdate = startdate
            self.enddate = enddate

            print(startdate)
            print(enddate)

            print('Connected')


        except psycopg2.DatabaseError as err:
            print(err)
            raise(err)


    def finalize(self):

        """
        Close database connections
        :return: None
        """

        self.pgcursor.close()
        self.pgconn.close()
        print('Postgres Connection closed')

        self.mysqlcursor.close()
        self.mysqlcon.close()
        print('MySQL Connection closed')

    def getnoofjobs(self):
        """
        Get total no of jobs during quarter from Postgres DB
        :return: no of jobs
        """
        select_noofjobs = (
            "select count(*) from public.jobs where latestjobversion=True "
            "and insertdate between Date(%s) and Date(%s) "
            "and (username not in (%s))"
        )

        self.pgcursor.execute(select_noofjobs, (self.startdate, self.enddate, self.adminusers))

        noofjobs = 0
        count = self.pgcursor.fetchone()
        if count is not None:
            noofjobs = count[0]

        # print(str.format("total no of jobs: {0}", noofjobs))
        return noofjobs

    def getregisteredusers(self):

        """
        Get no of registered users in TAO, from MySQL DB
        :return: 
        """

        select_registeredusers = (
            "SELECT count(*) FROM tao_taouser "
            "WHERE username NOT IN (%s) "

        )

        select_registeredusers = select_registeredusers % self.adminusers
        print(select_registeredusers)
        self.mysqlcursor.execute(select_registeredusers, self.adminusers)

        users = 0
        x = self.mysqlcursor.fetchone()
        if x is not None:
            users = x[0]

        # print("No of registered users: {0}".format(users))
        return users

    def getactiveusers(self):

        """
        Get no of active users during quarter, users who submitted jobs
        :return: 
        """

        select_activeusers = (
            "SELECT count(DISTINCT username) FROM public.jobs "
            "WHERE latestjobversion = True AND insertdate BETWEEN Date(%s) AND Date(%s) "
            "AND (username NOT IN (%s)) "
        )


        self.pgcursor.execute(select_activeusers, (self.startdate, self.enddate, self.adminusers))

        activeusers = 0
        x = self.pgcursor.fetchone()
        if x is not None:
            activeusers = x[0]

        # print("No of active users: {0}".format(activeusers))
        return activeusers

    def getdatasize(self):

        select_datasize = (
            "SELECT sum(filesize)/(1024.0*1024*1024), sum(recordscount) FROM public.jobs "
            "WHERE latestjobversion = True AND insertdate BETWEEN Date(%s) AND Date(%s) "
            "AND (username NOT IN (%s)) "
        )

        self.pgcursor.execute(select_datasize, (self.startdate, self.enddate, self.adminusers))

        datasize = 0.0
        totalrecords = 0

        x = self.pgcursor.fetchone()
        if x is not None:
            datasize = "{0} GB".format(round(x[0], 3))
            totalrecords = "{:,}".format(x[1])

        # print("Data size: {0} \nTotal records: {1}".format(datasize, totalrecords))
        return (datasize, totalrecords)

    def getjobsbydatabase(self):
        """
        Get total no of jobs for each dataset, non premade datasets detailed
        :return: dictionary with dataset name as key and no of job as value
        """
        select_jobsbydb = (
            "SELECT count(*), database FROM jobs "
            "WHERE latestjobversion=True AND insertdate BETWEEN %s AND %s "
            "AND (username NOT IN (%s)) GROUP BY database"
        )

        self.pgcursor.execute(select_jobsbydb, (self.startdate, self.enddate, self.adminusers))

        databasejobs = {}

        rows = self.pgcursor.fetchall()
        for (jobs, database) in rows:
            if database != '':      #discard jobs without database
                # Multidark dataset
                if 'multidark' in database:
                    if 'multidark' in databasejobs:
                        # add to jobs count if already exists
                        databasejobs['Multidark'] += jobs
                    else:
                        # add new key if it doesn't exist
                        databasejobs['Multidark'] = jobs
                # Vishnu Bolshoi dataset
                elif 'vishnu_bolshoi' in database:
                    if 'Vishnu Bolshoi' in databasejobs:
                        databasejobs['Vishnu Bolshoi'] += jobs
                    else:
                        databasejobs['Vishnu Bolshoi'] = jobs
                # Bolshoi Planck dataset
                elif 'bolshoi_planck' in database:
                    if 'Bolshoi Planck' in databasejobs:
                        databasejobs['Bolshoi Planck'] += jobs
                    else:
                        databasejobs['Bolshoi Planck'] = jobs
                # Bolshoi dataset
                elif 'bolshoi' in database:
                    if 'Bolshoi' in databasejobs:
                        databasejobs['Bolshoi'] += jobs
                    else:
                        databasejobs['Bolshoi'] = jobs
                # Millenium dataset, including mini Millenium
                elif 'millennium' in database:
                    if 'Millennium' in databasejobs:
                        databasejobs['Millennium'] += jobs
                    else:
                        databasejobs['Millennium'] = jobs
                # All pre-made datasets
                else:
                    if 'Ready-Made' in databasejobs:
                        databasejobs['Ready-Made'] += jobs
                    else:
                        databasejobs['Ready-Made'] = jobs

        # print(databasejobs)
        # print("Database: {0}, Jobs: {1}".format(database, jobs))
        return databasejobs

    def getactiveusersdata(self, startdate, enddate):

        """
        Get active users data during quarter, users who submitted jobs
        :return: 
        """
        try:

            select_activeusers = (
                "SELECT DISTINCT username FROM public.jobs "
                "WHERE latestjobversion = True AND insertdate BETWEEN Date(%s) AND Date(%s) "
                "AND username NOT IN ('amr','Amr.Hassan@swin.edu.au','yfenner','luke','ldeslandes') "
            )

            self.pgcursor.execute(select_activeusers, (startdate, enddate))

            activeusers = {}
            usernames = self.pgcursor.fetchall()

            select_userdata = (
                "SELECT gender, institution, country, is_student FROM tao_taouser "
                "WHERE username = %s "
            )

            for (username) in usernames:

                self.mysqlcursor.execute(select_userdata, username)

                userdata = self.mysqlcursor.fetchone()
                if userdata is not None:
                    # for (gender, institution, country, is_student) in userdata:
                    #     print("{0},{1},{2},{3},{4}".format(username, gender, institution, country, is_student))
                    print("{0},{1},{2},{3},{4}".format(username[0], userdata[0], userdata[1], userdata[2], userdata[3]))

        except Exception as exp:
            raise (exp)
        finally:
            self.finalize()

class TAO5Report:
    """
    This is a variation of TAO report that reads stats from text file(s)
    instead of the Postgres DB.
    """
    def __init__(self, db_config, start_date, end_date, files=[]):
        """
        Initialize TAO MySQL and Postgres database connections, report start and end dates

        :param dbconfig: a dictionary of dictionaries, all database configurations
        :param startdate: report start date, start of quarter
        :param enddate: report start date, end of quarter
        """

        try:
            # TAO MySQL database configuration from dbconfig dictionary
            mysqldb = db_config['tao-mysql']
            # print(mysqldb)
            # connect to MySQL database
            self.mysqlcon = mysql.connector.connect(**mysqldb)
            self.mysqlcursor = self.mysqlcon.cursor()
            print('connected to DB')

            # TAO admin users, to be discarded from Stats
            self.adminusers = db_config['tao-admins']
            self.startdate = start_date
            self.enddate = end_date

            print(start_date)
            print(end_date)

            self.adminusers_list = db_config['tao-admins'].split(',')
            self.adminusers_list = [i[1:-1] for i in self.adminusers]

            self.stats = self.read_stats_files(files)
            # Discard non-successful jobs
            self.stats = self.stats[(self.stats.Status.isin(['COMPLETED']) == True)]
            # Discard usage of admin users
            self.stats = self.stats[(self.stats.email.isin(self.adminusers_list) == False)]

        except mysql.connector.Error as err:
            print(err)
            raise (err)

    def finalize(self):

        """
        Close database connections
        :return: None
        """

        self.mysqlcursor.close()
        self.mysqlcon.close()
        print('MySQL Connection closed')


    def read_stats_files(self, files=[]):
        """
        reads cluster stats from files as a pandas dataframe
        :param files:
        :return: DataFrame contains stats from all TAO clusters
        """
        data = None
        df_list = [pd.read_table(f) for f in files]
        data = pd.concat(df_list)
        print(data)
        print(data.columns)
        return data

    def getnoofjobs(self):
        return len(self.stats.index)

    def getregisteredusers(self):
        select_registeredusers = (
            "SELECT count(*) FROM TAO.tao_taouser "
            "WHERE username NOT IN (%s) "

        )
        select_registeredusers = select_registeredusers % self.adminusers
        print(select_registeredusers)
        self.mysqlcursor.execute(select_registeredusers)

        users = 0
        x = self.mysqlcursor.fetchone()
        if x is not None:
            users = x[0]
        return users

    def getactiveusers(self):
        return len(self.stats.email.unique())

    def getdatasize(self):
        total_records = 0
        datasize = f'{round(self.stats.output_size.sum()/(1024.0 * 1024), 3)} GB'

        return (datasize, total_records)

    def getjobsbydatabase(self):
        databasejobs = dict()
        grouped_jobs = self.stats.groupby(by='database')
        print(grouped_jobs)
        for database, jobs in grouped_jobs:
            # print(f'database: {database}', len(jobs))
            if database != '':      #discard jobs without database
                # Multidark dataset
                if 'multidark' in database:
                    if 'multidark' in databasejobs:
                        # add to jobs count if already exists
                        databasejobs['Multidark'] += len(jobs)
                    else:
                        # add new key if it doesn't exist
                        databasejobs['Multidark'] = len(jobs)
                # Vishnu Bolshoi dataset
                elif 'vishnu_bolshoi' in database:
                    if 'Vishnu Bolshoi' in databasejobs:
                        databasejobs['Vishnu Bolshoi'] += len(jobs)
                    else:
                        databasejobs['Vishnu Bolshoi'] = len(jobs)
                # Bolshoi Planck dataset
                elif 'bolshoi_planck' in database:
                    if 'Bolshoi Planck' in databasejobs:
                        databasejobs['Bolshoi Planck'] += len(jobs)
                    else:
                        databasejobs['Bolshoi Planck'] = len(jobs)
                # Bolshoi dataset
                elif 'bolshoi' in database:
                    if 'Bolshoi' in databasejobs:
                        databasejobs['Bolshoi'] += len(jobs)
                    else:
                        databasejobs['Bolshoi'] = len(jobs)
                # Millenium dataset, including mini Millenium
                elif 'millennium' in database:
                    if 'Millennium' in databasejobs:
                        databasejobs['Millennium'] += len(jobs)
                    else:
                        databasejobs['Millennium'] = len(jobs)
                # All pre-made datasets
                else:
                    if 'Ready-Made' in databasejobs:
                        databasejobs['Ready-Made'] += len(jobs)
                    else:
                        databasejobs['Ready-Made'] = len(jobs)
        return databasejobs

