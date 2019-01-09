"""

"""
import mysql.connector
import pandas as pd

FEMALE = 1
MALE = 0
ASTRONOMY = 1
STUDENT = 1

GENDER = {2: "None", 1:"Female", 0:"Male"}
IS_ASTRONOMY = {1: "Astronomy", 0:"Non-astronomy"}
IS_STUDENT = {1:"Student", 0: "Staff"}
IS_AUSTRALIA = {'AU':"National Astronomy", '': "Other"}


class Project(object):

    def __init__(self, project_data):
        self.code = project_data[0][0:4]
        self.type = project_data[0][5:]

        self.is_astronomy = project_data[1]

        self.name = project_data[2]
        self.admin_email = project_data[3]

    def __str__(self):
        """
        Formats Project object as string to be printed
        :return: formatted string of project attributes
        """
        return "{0:5s} {1:8s} {2:3s} {3:100s} {4:100}".format(self.code, self.type, str(self.is_astronomy) if self.is_astronomy is not None else '-', self.name, self.admin_email)

class User(object):

    def __init__(self, user_data):
        self.name = user_data[0] + " " + user_data[1]
        self.username = user_data[2]
        self.gender = GENDER[user_data[3]]

        self.student = IS_STUDENT[user_data[4]]
        self.is_astronomy = IS_ASTRONOMY[user_data[5]]
        self.institution = user_data[6]
        self.country = user_data[7]
        country = '' if user_data[7] != 'AU' else 'AU'
        self.is_australia = IS_AUSTRALIA[country]
        self.usage = user_data[8]

    def __str__(self):
        """
        Formats Project object as string to be printed
        :return: formatted string of project attributes
        """
        return "{0:50} {1:15} {2} ".format(self.name, self.username, self.usage)

    def __add__(self, other):
        return self.usage + other.usage

    def __radd__(self, other):
        if other == 0:
            return self
        else:
            return self.__add__(other)


class SlurmUser(object):

    def __init__(self, user_data):
        '''
        creates a User object using data from database represented in a tuple
        :param user_data: Tuple contains user raw data from database
            (firstname, lastname, username, gender, is_student, is_astronomy, institution, country)
        '''
        self.name = user_data[0] + " " + user_data[1]
        self.username = user_data[2]
        self.gender = GENDER[user_data[3]]

        self.student = IS_STUDENT[user_data[4]]
        self.is_astronomy = IS_ASTRONOMY[user_data[5]]
        self.institution = user_data[6]
        self.country = user_data[7]
        country = '' if user_data[7] != 'AU' else 'AU'
        self.is_australia = IS_AUSTRALIA[country]
        self.usage = 0

    def __str__(self):
        """
        Formats Project object as string to be printed
        :return: formatted string of project attributes
        """
        return "{0:50} {1:15} {2} ".format(self.name, self.username, self.usage)

    def __add__(self, other):
        return self.usage + other.usage

    def __radd__(self, other):
        if other == 0:
            return self
        else:
            return self.__add__(other)


class Report(object):
    """
    Enclose all operations to extract gSTAR quarterly report data from DB

    """

    def __init__(self, dbcon, startdate, enddate, type='moab', slurmdata=None):

        """

        :param dbconfig: a dictionary of dictionaries, all database configurations
        :param startdate: report start date, start of quarter
        :param enddate: report end date, end of quarter
        :param type: MOAB or Slurm report. default is moab
        :param slurmdata: Dataframe _contains Slurm user utilisation information per project in CPU hours.
            needed only if it is Slurm report
        """
        # connect to database schema using dbconfig dictionary
        # gSTAR stats MySQL DB entry in dbconfig

        # mysqldb = dbconfig['mysql']
        # self._con = mysql.connector.connect(**mysqldb)
        self._con = dbcon
        self._cursor = self._con.cursor()
        self._startdate = startdate
        self._enddate = enddate

        # Dataframe contains Slurm user utilisation information per project in CPU hours,
        # drop [hpc, testers, root] projects
        discardprojects = ['hpcadmin', 'testers', 'root']
        self.slurmdata = slurmdata

        print("Start Date {0}, end date {1}".format(self._startdate, self._enddate))

        print(str.format('Generating Report for the period from {0} to {1} ...', self._startdate, self._enddate))

        # Total usage from startdate to enddate, will be used in other parts of the report
        if type == 'moab':
            self._totalusage = self.getTotalUsage()
            self.usersinfo = self.getUsersInfo()
        elif type == 'slurm':
            if not(slurmdata is None):
                self.slurmdata = self.slurmdata[(self.slurmdata.Account.isin(discardprojects) == False)]
                self._totalusage = self.gettotalusage_slurm()
                self.slurmactiveusers = self.slurmdata.Login.unique()
                return
            else:
                raise Exception("Slurm data is empty")

        print(self._totalusage/1000)

    def finalise(self):
        # closing connection and cursor
        if self._con:
            self._cursor.close()
            self._con.close()
            print("Resources released successfully")

    # def __del__(self):
    #     self.finalise()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.finalise()
        print('Report generated successfully')

    def __enter__(self):
        return self

    @property
    def totalusage(self):
        return self._totalusage

    @property
    def startdate(self):
        return self._startdate

    @property
    def enddate(self):
        return self._enddate

    def getAllProjects(self):
        """
        Query the database for all projects
        :return: list of Project objects
        """

        select_projs = (
            "SELECT gum_project.code, gum_project.name, gum_user.email_address "
            "FROM gum_project INNER JOIN gum_user on gum_user.id = gum_project.project_administrator "
            "ORDER BY gum_project.code"
        )

        projects = []
        self._cursor.execute(select_projs)

        for project in self._cursor:
            projects.append(Project(project))

        return projects

    def getInstitutionAstronomers(self):
        """
        Query the database for no of astronomy users group by institutions
        :return:
        """
        select_astro = (
            "SELECT gum_userdepartment.department_id, gum_institution.name, gum_department.name, count(gum_userdepartment.user_id) no_of_users FROM gum_userdepartment "
            "INNER JOIN gum_department ON gum_userdepartment.department_id = gum_department.id "
            "INNER JOIN gum_institution ON gum_department.institution_id = gum_institution.id "
            "WHERE gum_userdepartment.end_date IS NULL "
            "AND gum_department.is_astronomy = 1 "
            "GROUP BY gum_userdepartment.department_id "
            "ORDER BY no_of_users desc, gum_userdepartment.department_id"
        )

        result = []

        self._cursor.execute(select_astro)
        for (deptid, institution, deptname, noofusers) in self._cursor:
            result.append((institution, noofusers))

        return result

    def getTotalUsage(self):
        """
        Get total usage over the quarter
        :return:
        """
        select_totalusage = "SELECT sum(service_units) from job_event WHERE type = 'JOBEND' AND DATE(time) BETWEEN DATE(%s) AND DATE(%s)"
        self._cursor.execute(select_totalusage, (self._startdate, self._enddate))

        return self._cursor.fetchone()[0]


    def getProjectUsage(self):
        """
        Query the database for total usage per project over the quarter
        :return:
        """
        select_projectusage = "SELECT account, sum(service_units)/1000 FROM job_event "
        select_projectusage += "WHERE type='JOBEND' AND DATE(job_event.time) BETWEEN DATE(%s) AND DATE(%s) "
        select_projectusage += "GROUP BY account ORDER BY account"

        self._cursor.execute(select_projectusage, (self._start, self._enddate))

        result = []

        for (proj, usage) in self._cursor:
            result.append("{0:15s} {1} ".format(proj, usage))

        return "\n".join(result)

    def getProjectUsagePercent(self):

        select_projectusage = (
            "SELECT gum_project.code, a.prj_usage "
            "FROM gum_project LEFT OUTER JOIN "
            "(SELECT account as acc, round((sum(service_units)/ %s )*100,3) as prj_usage FROM job_event "
            "WHERE type='JOBEND' AND DATE(time) BETWEEN DATE(%s) AND DATE(%s) group by account) "
            "As a ON gum_project.code = a.acc "
            "WHERE gum_project.system_id = 1  "
        )

        self._cursor.execute(select_projectusage, (self._totalusage, self._startdate, self._enddate))

        result = []
        for (proj, usage) in self._cursor:
            result.append((proj, str(usage) + "%" if usage is not None else '-'))

        return result

    def getUsersInfo(self):

        print("Getting users information")
        select_usersinfo = (
            "SELECT distinct gum_user.first_name first, gum_user.last_name last, gum_user.username username, "
            "gum_user.gender, gum_user.is_student is_student, gum_department.is_astronomy is_astronomy, "
            "gum_institution.name inst_name,  gum_institution.country country "
            "FROM gum_userdepartment "
            "INNER JOIN gum_department ON gum_userdepartment.department_id = gum_department.id "
            "INNER JOIN gum_institution ON gum_department.institution_id = gum_institution.id "
            "INNER JOIN gum_user ON gum_userdepartment.user_id = gum_user.id "
            "INNER JOIN gum_usersystem ON gum_usersystem.user_id = gum_user.id "
            "WHERE gum_usersystem.system_id = 1 "
        )

        users = dict()
        self._cursor.execute(select_usersinfo)
        for userdata in self._cursor:
            u = SlurmUser(userdata)
            users[u.username] = u.__dict__

        users_df = pd.DataFrame.from_dict(data=users, orient='index')

        return users_df

    def isInternationalCollaboration(self, projectuser_df, project_code):
        if len(projectuser_df[projectuser_df.project_code == project_code].country.unique()) > 1:
            print('international collaboration')
            return True
        return False

    def isMultipleAustralianInsitutions(self, projectuser_df, project_code):
        if len(projectuser_df[((projectuser_df.project_code == project_code) & (projectuser_df.country =='AU'))].institution.unique()) > 1:
            print("Multiple Australian Institutions")
            return True
        return False

    def getProjectCollaborationStats(self):

        ismultipleaus = 0
        isinternational = 0

        select_projectusers = (
            "SELECT gum_userproject.id id, gum_project.code project_code, gum_user.username username FROM gum_project "
            "INNER JOIN gum_userproject ON gum_project.id = gum_userproject.project_id "
            "INNER JOIN gum_user ON gum_userproject.user_id = gum_user.id "
            "WHERE gum_project.system_id = 1 "
            "ORDER BY project_code"
            # "WHERE gum_project.code =  %s "
        )

        self._cursor.execute(select_projectusers)

        projectusers_dict = dict()
        for (id, code, username) in self._cursor:
            projectusers_dict[id] = (code, username)

        projectusers_df = pd.DataFrame.from_dict(data=projectusers_dict, orient='index', columns=['project_code', 'username'])
        # print(projectusers_df)
        project_users_merged = pd.merge(projectusers_df, self.usersinfo[['username', 'institution', 'country']], on='username')
        # print(project_users_merged)

        projects_count = len(projectusers_df['project_code'].unique())

        for project in project_users_merged.project_code.unique():
            print("Project {0}: {1}/{2}". format(project, self.isInternationalCollaboration(project_users_merged, project), self.isMultipleAustralianInsitutions(project_users_merged, project)))
            if self.isMultipleAustralianInsitutions(project_users_merged, project):
                ismultipleaus += 1

            if self.isInternationalCollaboration(project_users_merged, project):
                isinternational += 1

        print("Total Projects: {0}".format(projects_count))
        print('{0} Projects with Multiple Australian Institutions, Percentage {1}'.format(ismultipleaus, ismultipleaus/projects_count * 100.0))
        print('{0} Projects with Australian and International Institutions, Percentage {1}'.format(isinternational, isinternational/projects_count * 100.0))



    def getInstitutionUsagePercent(self):
        """
        Query the database for percentage of usage per institution over the quarter

        :param totalusage:
        :return:
        """
        select_institutionusage = (
            "SELECT user_inst.inst_name , round((sum(service_units)/ %s )*100,2) as percentage "
            "FROM job_event inner join "
            "(SELECT distinct gum_userdepartment.department_id, gum_institution.name inst_name, "
            "gum_department.name dept_name, gum_user.username username, "
            "gum_userdepartment.start_date startd, gum_userdepartment.end_date endd FROM gum_userdepartment "
            "INNER JOIN gum_department ON gum_userdepartment.department_id = gum_department.id "
            "INNER JOIN gum_institution ON gum_department.institution_id = gum_institution.id "
            "INNER JOIN gum_user ON gum_userdepartment.user_id = gum_user.id "
            "INNER JOIN gum_usersystem on gum_userdepartment.user_id = gum_usersystem.user_id "
            "AND gum_usersystem.system_id = 1 "
            ") AS user_inst ON job_event.user = user_inst.username "
            "WHERE type='JOBEND' AND Date(job_event.time) between Date(%s) AND Date(%s) "
            "AND job_event.time >= user_inst.startd AND (job_event.time <= user_inst.endd OR user_inst.endd IS NULL) "
            "GROUP BY user_inst.inst_name "
            "ORDER BY percentage DESC"
        )

        count = self._cursor.execute(select_institutionusage, (self._totalusage, self._startdate, self._enddate))

        result = []
        for (inst, usage) in self._cursor:
            result.append((inst, str(usage) + "%"))

        return result

    def getUsersCount(self, filters=[]):
        """
        Query the database for no of users who meet certain criteria (filter)

        :return:
        """
        select_nousers = (
            "SELECT count(*) FROM gum_userdepartment "
            "INNER JOIN gum_department ON gum_userdepartment.department_id = gum_department.id "
            "INNER JOIN gum_institution ON gum_department.institution_id = gum_institution.id "
            "INNER JOIN gum_user ON gum_userdepartment.user_id = gum_user.id "
            "INNER JOIN gum_usersystem on gum_userdepartment.user_id = gum_usersystem.user_id "
            "WHERE gum_userdepartment.end_date IS NULL "
            "AND gum_usersystem.system_id = 1 "
        )

        for i in range(len(filters)):
            select_nousers += "AND {0} = {1} ".format(filters[i][0], filters[i][1])

        self._cursor.execute(select_nousers)

        nousers = 0
        for (x) in self._cursor:
            nousers = x[0]

        return nousers

    def getActiveUsersCount(self, filters=[]):
        """
        Query the database for no of active users over the quarter, satisfying specified filters

        :return: no of active users
        """
        select_noactiveusers = (
            "SELECT count(*) FROM gum_userdepartment "
            "INNER JOIN gum_department ON gum_userdepartment.department_id = gum_department.id "
            "INNER JOIN gum_institution ON gum_department.institution_id = gum_institution.id "
            "INNER JOIN gum_user ON gum_userdepartment.user_id = gum_user.id "
            "INNER JOIN gum_usersystem on gum_userdepartment.user_id = gum_usersystem.user_id "
            "WHERE gum_userdepartment.end_date IS NULL "
            "AND gum_usersystem.system_id = 1 "
            "AND gum_user.username in (select distinct user from job_event WHERE DATE(time) BETWEEN DATE(%s) AND DATE(%s)) "
        )

        for filter in filters:
            select_noactiveusers += "AND {0} = {1} ".format(filter[0], filter[1])

        self._cursor.execute(select_noactiveusers, (self._startdate, self._enddate))

        nousers = 0
        for (x) in self._cursor:
            nousers = x[0]

        return nousers

    def getActiveSwinAstronomersCount(self):
        """
        Query the database for no of active astronomer users from Swinburne Uni, during the quarter
        :return: no of active swinburne astronomers
        """
        select_actswinastro = (
            "SELECT count(*) FROM gum_userdepartment "
            "INNER JOIN gum_user on gum_userdepartment.user_id=gum_user.id "
            "INNER JOIN gum_usersystem on gum_userdepartment.user_id = gum_usersystem.user_id "
            "WHERE gum_userdepartment.department_id = 6 "
            "AND gum_usersystem.system_id = 1 "
            "AND gum_user.username IN (SELECT DISTINCT user FROM job_event WHERE DATE(time) BETWEEN DATE(%s) AND DATE(%s) ) "
        )

        self._cursor.execute(select_actswinastro, (self._startdate, self._enddate))

        return self._cursor.fetchone()[0]

    def getSwinAstronomersCount(self):
        """
        Query the database for no of all astronomer users from Swinburne Uni
        :return:
        """
        select_swinastro = (
            "SELECT count(*) FROM gum_userdepartment "
            "INNER JOIN gum_usersystem on gum_userdepartment.user_id = gum_usersystem.user_id "
            "WHERE gum_userdepartment.department_id = 6 "
            "AND gum_usersystem.system_id = 1 "
        )

        self._cursor.execute(select_swinastro)

        return self._cursor.fetchone()[0]

    def getAusUsersUsage(self):
        """

        :return:
        """
        select_aususage = (
            "SELECT inst_users.first, inst_users.last, inst_users.inst_name, job_event.user, "
            "round((sum(service_units)/ %s )*100,2) AS percentage FROM job_event INNER JOIN ( " 
            "SELECT distinct gum_userdepartment.department_id, gum_institution.name inst_name, "    
            "gum_department.name dept_name, gum_user.username username, gum_user.first_name first, gum_user.last_name last, "
            "gum_userdepartment.start_date startd, gum_userdepartment.end_date endd FROM gum_userdepartment "
            "INNER JOIN gum_department ON gum_userdepartment.department_id = gum_department.id "
            "INNER JOIN gum_institution ON gum_department.institution_id = gum_institution.id "
            "INNER JOIN gum_user ON gum_userdepartment.user_id = gum_user.id "
            "WHERE gum_department.is_astronomy = 1 "
            "AND gum_institution.country = 'AU' ) as inst_users ON job_event.user = inst_users.username "
            "WHERE type='JOBEND' and Date(job_event.time) BETWEEN Date(%s) AND Date(%s) "
            "AND job_event.time >= inst_users.startd AND ( job_event.time <= inst_users.endd OR inst_users.endd IS NULL) "
            "GROUP BY job_event.user, inst_users.first, inst_users.last, inst_users.inst_name "
            "ORDER BY percentage desc"
        )

        self._cursor.execute(select_aususage, (self._totalusage, self._startdate, self._enddate,))

        result = []
        for (first, last, institution, username, usage) in self._cursor:
            result.append("{0:30s} {1:50s} {2:20s} {3} ".format(first + " " + last, institution, username, usage))

        return "\n".join(result)

    def getUsageByDemographic(self):
        """
        Query the database for percentage of usage based on different demographic criteria; gender, is student, is astronomy, is national
        :return:
        """
        select_userusage = (
            "SELECT inst_users.first, inst_users.last, job_event.user, inst_users.gender, inst_users.is_student, inst_users.is_astronomy, "
            "inst_users.inst_name, inst_users.country, round((sum(service_units)/ %s )*100,2) AS percentage "
            "FROM job_event INNER JOIN ( "
            "SELECT distinct gum_userdepartment.department_id, gum_institution.name inst_name,  gum_institution.country country, "
            "gum_department.name dept_name, gum_department.is_astronomy is_astronomy, gum_user.username username, "
            "gum_user.gender, gum_user.is_student is_student, gum_userdepartment.start_date startd, gum_userdepartment.end_date endd, "
            "gum_user.first_name first, gum_user.last_name last "
            "FROM gum_userdepartment "
            "INNER JOIN gum_department ON gum_userdepartment.department_id = gum_department.id "
            "INNER JOIN gum_institution ON gum_department.institution_id = gum_institution.id "
            "INNER JOIN gum_user ON gum_userdepartment.user_id = gum_user.id) as inst_users ON job_event.user = inst_users.username "
            "WHERE job_event.type='JOBEND' and DATE(time) BETWEEN DATE(%s) AND DATE(%s) "
            "AND job_event.time >= inst_users.startd AND ( job_event.time <= inst_users.endd OR inst_users.endd IS NULL) "
            "GROUP BY inst_users.first, inst_users.last, job_event.user, inst_users.gender, inst_users.is_student, inst_users.is_astronomy, inst_users.inst_name, inst_users.country "
            "ORDER BY percentage desc"
        )

        users = []
        self._cursor.execute(select_userusage, (self._totalusage, self._startdate, self._enddate,))
        for userdata in self._cursor:
            users.append(User(userdata))

        # Dictionary of (demographic criteria, list of groups and usages)
        demographic = {}
        # Percentage of usage based on gender; Male and Female totals
        demographic["gender"] = self.getFilteredUsage(users, "gender")
        # Percentage of usage based on is_student; Staff and Student totals
        demographic["student"] = self.getFilteredUsage(users, "student")
        # Percentage of usage based on is_astronomy; Astronomy and Non-astronomy totals
        demographic["is_astronomy"] = self.getFilteredUsage(users, "is_astronomy")
        # Percentage of usage based on location; National and Other totals
        demographic["is_australia"] = self.getFilteredUsage(users, "is_australia")

        return demographic

    def getFilteredUsage(self, usersusage, filter=""):
        """
        Group users based on specified criteria and calculates total usage for each group

        :param usersusage: list of percentage of usage for all users during the quarter
        :return: List of groups and corresponding usage totals
        """
        filtered = {}

        # Filtering list based on specified criteria, create groups based on available values

        for user in usersusage:
            value = user.__dict__[filter]

            if value in filtered:
                filtered[value].append(user.usage)
            else:
                filtered[value] = [user.usage]

        # calculate total usage for each group
        result = []
        usage = 0
        for group in filtered.keys():
            if usage == 0:
                group_usage = int(sum(filtered[group]))
                usage = group_usage
            else:
                group_usage = 100 - usage
                usage += group_usage

            result.append((group, str(group_usage)))

        return result


    ################# Slurm methods ###########

    def gettotalusage_slurm(self):

        '''
        Get Slurm total usage as the sum of CPU hours available in the dataframe
        :return: total usage in CPU hours
        '''

        stotalusage = 0

        stotalusage = self.slurmdata.Used.sum()

        return stotalusage


    def getSlurmActiveUsersCount(self, filters=[]):
        """
        Query the database for no of active slurm users satisfying specified filters

        :return: no of active users
        """


        select_noactiveusers = (
            "SELECT count(*) FROM gum_userdepartment "
            "INNER JOIN gum_department ON gum_userdepartment.department_id = gum_department.id "
            "INNER JOIN gum_institution ON gum_department.institution_id = gum_institution.id "
            "INNER JOIN gum_user ON gum_userdepartment.user_id = gum_user.id "
            "WHERE gum_userdepartment.end_date IS NULL "
            "AND gum_user.username in (%s) "
        )

        for filter in filters:
            select_noactiveusers += "AND {0} = {1} ".format(filter[0], filter[1])

        format_strings = ','.join(['%s'] * len(self.slurmactiveusers))
        self._cursor.execute(select_noactiveusers % format_strings, tuple(self.slurmactiveusers))

        # select_noactiveusers = (
        #     "SELECT count(*) FROM gum_userdepartment "
        #     "INNER JOIN gum_department ON gum_userdepartment.department_id = gum_department.id "
        #     "INNER JOIN gum_institution ON gum_department.institution_id = gum_institution.id "
        #     "INNER JOIN gum_user ON gum_userdepartment.user_id = gum_user.id "
        #     "WHERE gum_userdepartment.end_date IS NULL "
        #     "AND gum_user.username in " + str(tuple(self.slurmactiveusers)) + " "
        # )
        #
        # for filter in filters:
        #     select_noactiveusers += "AND {0} = {1} ".format(filter[0], filter[1])
        #
        # print(select_noactiveusers)
        #
        # self._cursor.execute(select_noactiveusers)

        nousers = 0
        for (x) in self._cursor:
            nousers = x[0]

        return nousers

    def getSlurmProjectUsagePercent(self):

        projectusage = round(self.slurmdata.groupby(by='Account').Used.sum() * 100.0 / self._totalusage, 3)
        projectusagelist = []

        select_ozstarprojects = "select code from gum_project where system_id = 2 and gum_project.code like 'oz%' order by code"
        self._cursor.execute(select_ozstarprojects)

        for project in self._cursor.fetchall():
            usage = '{0}%'.format(projectusage[project[0]]) if project[0] in projectusage else '-'
            projectusagelist.append((project[0], usage))

        return projectusagelist

    def getSlurmActiveSwinAstronomersCount(self):
        """
        Query the database for no of active astronomer users from Swinburne Uni, during the quarter
        :return: no of active swinburne astronomers
        """
        select_actswinastro = (
            "SELECT count(*) FROM gum_userdepartment "
            "INNER JOIN gum_user on gum_userdepartment.user_id=gum_user.id "
            "INNER JOIN gum_usersystem on gum_usersystem.user_id = gum_user.id "
            "WHERE gum_userdepartment.department_id = 6 AND gum_usersystem.system_id = 2 AND gum_user.username IN (%s) "
        )

        format_strings = ','.join(['%s'] * len(self.slurmactiveusers))
        self._cursor.execute(select_actswinastro % format_strings, tuple(self.slurmactiveusers))

        return self._cursor.fetchone()[0]

    def getSlurmInstitutionUsagePercent(self):
        """
        Query the database for percentage of usage per institution over the quarter

        :param totalusage:
        :return:
        """


        select_institutionusers = (
            "select DISTINCT ustartdate.username, gum_institution.name from gum_institution "
            "LEFT JOIN gum_department on gum_department.institution_id = gum_institution.id "
            "INNER JOIN gum_userdepartment on gum_userdepartment.department_id = gum_department.id "
            "INNER JOIN ( "
            "select distinct gum_user.id uid, max(start_date) sdate, gum_user.username username "
            "from gum_userdepartment " 
            "INNER JOIN gum_department on gum_userdepartment.department_id  = gum_department.id "
            "INNER JOIN gum_user on gum_userdepartment.user_id = gum_user.id "
            "INNER JOIN gum_usersystem ON gum_userdepartment.user_id = gum_usersystem.user_id "
            "where gum_userdepartment.end_date is null and gum_usersystem.system_id = 2 "
            "group by uid) as ustartdate on  ustartdate.uid = gum_userdepartment.user_id "
            "where gum_userdepartment.start_date = ustartdate.sdate;"
        )

        userusage = self.slurmdata.groupby(by='Login').Used.sum()

        self._cursor.execute(select_institutionusers)

        userinst_dict = dict()

        for (username, inst) in self._cursor.fetchall():
            userinst_dict[username] = [inst, userusage[username] if username in userusage.keys() else 0]



        userinst_df = pd.DataFrame.from_dict(userinst_dict, orient='index', columns=['institution', 'usage'])
        instusage = (userinst_df.groupby(by='institution').usage.sum()).sort_values(ascending=False)


        result = []
        for inst in instusage.keys():
            result.append((inst, str(round((instusage[inst] * 100.0)/self._totalusage, 3)) + "%"))

        return result

    def getSlurmUsageByDemographic(self):
        """
        Query the database for percentage of usage based on different demographic criteria; gender, is student, is astronomy, is national
        :return:
        """

        select_userusage = (
            "SELECT distinct gum_user.first_name first, gum_user.last_name last, gum_user.username username, "
            "gum_user.gender, gum_user.is_student is_student, gum_department.is_astronomy is_astronomy, "
            "gum_institution.name inst_name,  gum_institution.country country "
            "FROM gum_userdepartment "
            "INNER JOIN gum_department ON gum_userdepartment.department_id = gum_department.id "
            "INNER JOIN gum_institution ON gum_department.institution_id = gum_institution.id "
            "INNER JOIN gum_user ON gum_userdepartment.user_id = gum_user.id "
            "INNER JOIN gum_usersystem ON gum_usersystem.user_id = gum_user.id "
            "WHERE gum_usersystem.system_id = 2 "
        )

        userusage = self.slurmdata.groupby(by='Login').Used.sum()

        users = dict()
        self._cursor.execute(select_userusage)
        for userdata in self._cursor:
            u = SlurmUser(userdata)
            u.usage = userusage[u.username] if u.username in userusage.keys() else 0
            users[u.username] = u.__dict__

        users_df = pd.DataFrame.from_dict(data=users, orient='index')

        # Dictionary of (demographic criteria, list of groups and usages)
        demographic = {}
        # Percentage of usage based on gender; Male and Female totals
        demographic["gender"] = self.getGroupUsage(users_df[users_df.gender.isin([GENDER[MALE],GENDER[FEMALE]])], "gender")
        # Percentage of usage based on is_student; Staff and Student totals
        demographic["student"] = self.getGroupUsage(users_df, "student")
        # Percentage of usage based on is_astronomy; Astronomy and Non-astronomy totals
        demographic["is_astronomy"] = self.getGroupUsage(users_df, "is_astronomy")
        # Percentage of usage based on location; National and Other totals
        demographic["is_australia"] = self.getGroupUsage(users_df, "is_australia")

        return demographic

    def getGroupUsage(self, usersusage, filter=""):
        """
        Group users based on specified criteria and calculates total usage for each group

        :param usersusage: user information and usage in a dataframe
        :return: List of groups and corresponding usage totals
        """
        groupusage = round(usersusage.groupby(by=filter).usage.sum()*100.0/self._totalusage, 2)
        # print(groupusage)

        return groupusage

    def getOzSTARUsersCount(self, filters=[]):
        """
        Query the database for no of users who meet certain criteria (filter)

        :return:
        """
        select_nousers = (
            "SELECT count(DISTINCT gum_user.username) FROM gum_userdepartment "
            "INNER JOIN gum_department ON gum_userdepartment.department_id = gum_department.id "
            "INNER JOIN gum_institution ON gum_department.institution_id = gum_institution.id "
            "INNER JOIN gum_user ON gum_userdepartment.user_id = gum_user.id "
            "INNER JOIN gum_usersystem ON gum_usersystem.user_id = gum_user.id "
            "WHERE gum_userdepartment.end_date IS NULL AND gum_usersystem.system_id = 2 "
        )

        for i in range(len(filters)):
            select_nousers += "AND {0} = {1} ".format(filters[i][0], filters[i][1])

        self._cursor.execute(select_nousers)

        nousers = 0
        for (x) in self._cursor:
            nousers = x[0]

        return nousers

    def getOzSTARSwinAstronomersCount(self):
        """
        Query the database for no of all astronomer users from Swinburne Uni
        :return:
        """
        select_swinastro = (
            "SELECT count(DISTINCT gum_userdepartment.user_id) FROM gum_userdepartment "
            "INNER JOIN gum_usersystem ON gum_userdepartment.user_id = gum_usersystem.user_id "
            "WHERE gum_userdepartment.department_id = 6 AND gum_usersystem.system_id = 2 "
        )

        self._cursor.execute(select_swinastro)

        return self._cursor.fetchone()[0]

    def getOzSTARInstitutionAstronomers(self):
        """
        Query the database for no of astronomy users group by institutions
        :return:
        """
        # select_astro = (
        #     "SELECT DISTINCT gum_userdepartment.department_id, gum_institution.name, gum_department.name, count(gum_userdepartment.user_id) no_of_users FROM gum_userdepartment "
        #     "INNER JOIN gum_department ON gum_userdepartment.department_id = gum_department.id "
        #     "INNER JOIN gum_institution ON gum_department.institution_id = gum_institution.id "
        #     "INNER JOIN gum_usersystem ON gum_userdepartment.user_id = gum_usersystem.user_id "
        #     "WHERE gum_userdepartment.end_date IS NULL "
        #     "AND gum_department.is_astronomy = 1 AND gum_usersystem.system_id = 2 "
        #     "GROUP BY gum_userdepartment.department_id "
        #     "ORDER BY no_of_users desc, gum_userdepartment.department_id"
        # )

        # result = []
        #
        # self._cursor.execute(select_astro)
        # for (deptid, institution, deptname, noofusers) in self._cursor:
        #     result.append((institution, noofusers))

        select_astro = (
            "select Distinct gum_institution.name, count(ustartdate.uid) usercount from gum_institution "
            "LEFT JOIN gum_department on gum_department.institution_id = gum_institution.id "
            "INNER JOIN gum_userdepartment on gum_userdepartment.department_id = gum_department.id "
            "INNER JOIN ( "
            "select distinct gum_user.id uid, max(start_date) sdate "
            "from gum_userdepartment "
            "INNER JOIN gum_department on gum_userdepartment.department_id  = gum_department.id "
            "INNER JOIN gum_user on gum_userdepartment.user_id = gum_user.id "
            "INNER JOIN gum_usersystem ON gum_userdepartment.user_id = gum_usersystem.user_id "
            "where gum_userdepartment.end_date is null and gum_usersystem.system_id = 2  "
            "and gum_department.is_astronomy = 1 "
            "group by uid) as ustartdate on  ustartdate.uid = gum_userdepartment.user_id "  
            "where gum_userdepartment.start_date = ustartdate.sdate " 
            "group by gum_institution.name "
            "order by usercount desc "
        )


        result = []

        self._cursor.execute(select_astro)
        for (institution, noofusers) in self._cursor:
            result.append((institution, noofusers))

        return result











