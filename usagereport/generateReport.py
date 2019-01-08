#!usr/bin/python3

"""
Entry point to generate quarterly report to AAL from Stats database
Report list gSTAR and TAO stats

Usage:
    python3 generateReport.py <quarter> <year>
    python3 generateReport.py 3 2017
    
    if not specified current date will be used to determine last reporting period/quarter

"""

import sys
import datetime

from pylatex import Document, Section, Subsection, LongTabu, Command
from pylatex.utils import bold, NoEscape


from usagereport.export.reportFromDB import Report, MALE, FEMALE, ASTRONOMY, STUDENT

from usagereport.statsConfig import readdbconfig



class ReportFormat(object):

    def __init__(self):

        try:
            self.doc = Document(page_numbers=True)

            self.doc.preamble.append(Command('title', 'AAL Quarterly Report'))
            self.doc.preamble.append(Command('author', 'Jarrod Hurley'))
            self.doc.preamble.append(Command('date', NoEscape(r'\today')))

            self.doc.append(NoEscape(r'\maketitle'))
            self.doc.append(NoEscape(r"\newpage"))

        except Exception as exp:

            raise exp


    def generateReport(self, report, taoreport):
        try:
            myfilename = "latex_files/" + datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

            print("Extracting percentage of usage by institution ...")
            self.doc.append(Section("Usage by Institution "))
            self.formatTable(header=["Institution", "Usage"], indent="X[l] X[r]",
                             data=report.getInstitutionUsagePercent())

            print("Extracting astronomy account holders information...")
            self.doc.append(Section("Astronomy account holders(total/active for quarter): "))

            # Creating a list of tuples to add to table
            accounts_data = []

            users = report.getUsersCount()
            activeusers = report.getActiveUsersCount()
            astronomymales = report.getUsersCount([
                ('gum_department.is_astronomy', ASTRONOMY), ('gum_user.gender', MALE)])
            activeastronomymales = report.getActiveUsersCount(
                [('gum_department.is_astronomy', ASTRONOMY), ('gum_user.gender', MALE)])
            astronomyfemales = report.getUsersCount(
                [('gum_department.is_astronomy', ASTRONOMY), ('gum_user.gender', FEMALE)])
            activeastronomyfemales = report.getActiveUsersCount(
                [('gum_department.is_astronomy', ASTRONOMY), ('gum_user.gender', FEMALE)])
            astronomystudents = report.getUsersCount(
                [('gum_department.is_astronomy', ASTRONOMY), ('gum_user.is_student', STUDENT)])
            activeastronomystudents = report.getActiveUsersCount(
                [('gum_department.is_astronomy', ASTRONOMY), ('gum_user.is_student', STUDENT)])
            swinastro = report.getSwinAstronomersCount()
            activeswinastro = report.getActiveSwinAstronomersCount()

            accounts_data.append(("All", users, activeusers))
            accounts_data.append(("Male", astronomymales, activeastronomymales))
            accounts_data.append(("Female", astronomyfemales, activeastronomyfemales))
            accounts_data.append(("PhD Student", astronomystudents, activeastronomystudents))
            accounts_data.append(("Swinburne", swinastro, activeswinastro))

            self.formatTable(header=["", "Total", "Active"], indent="X[l] X[r] X[r]", data=accounts_data)

            print("Extracting no of astronomy users in all institutions...")
            self.doc.append(Section("Institutions other than Swinburne with astronomy account holders"))
            self.formatTable(header=["Institution", "No of Users"], indent="X[l] X[r]",
                             data=report.getInstitutionAstronomers())

            print("Extracting percentage of usage by project ...")
            self.doc.append(Section("Share of CPU hours usage by project"))
            self.formatTable(header=["Project", "Usage by CPU hours"], indent="X[l] X[r]",
                             data=report.getProjectUsagePercent())

            print("Extracting percentage of usage by Demographic ...")
            self.doc.append(Section("Usage by Demographic"))
            accounts_data = []
            demographic = report.getUsageByDemographic()

            for (groups, values) in demographic.values():
                accounts_data.append((":".join(groups), ":".join(values)))

            self.formatTable(header=[], indent="X[l] X[l]", data=accounts_data)

            #-----------------------TAO Stats---------------------------------------------------
            # Start a new page for TAO Stats
            self.doc.append(NoEscape(r"\newpage"))
            self.doc.append(Section("TAO Usage Statistics"))

            print("Extracting General TAO Usage ...")
            sectiontitle = "General TAO Usage (total for {0} to {1})".format(taoreport.startdate.strftime("%d/%m/%Y"), taoreport.enddate.strftime("%d/%m/%Y"))
            self.doc.append(Subsection(sectiontitle))
            accounts_data = []
            accounts_data.append(("Number of jobs", taoreport.getnoofjobs()))
            accounts_data.append(("Number of active users", taoreport.getactiveusers()))

            datasize = taoreport.getdatasize()

            accounts_data.append(("Total records returned", datasize[1]))
            accounts_data.append(("Total data-size returned", datasize[0]))

            accounts_data.append(("Registered users", taoreport.getregisteredusers()))

            accounts_data.append(("Page views (Google analytics)", ""))
            accounts_data.append(("Unique users (Google analytics)", ""))

            self.formatTable(header=[], indent="X[l] X[l]", data=accounts_data)

            print("Extracting Data access breakdown per database ...")
            sectiontitle = "General Data access breakdown per database ({0} to {1})".format(taoreport.startdate.strftime("%d/%m/%Y"), taoreport.enddate.strftime("%d/%m/%Y"))
            self.doc.append(Subsection(sectiontitle))

            databasejobs = taoreport.getjobsbydatabase()
            accounts_data = databasejobs.items()

            self.formatTable(header=[], indent="X[l] X[l]", data=accounts_data)

            print("TAO site access by location from Google analytics ...")
            sectiontitle = "TAO site access by location from Google analytics ({0} to {1})".format(taoreport.startdate.strftime("%d/%m/%Y"),
                                                                                            taoreport.enddate.strftime("%d/%m/%Y"))
            self.doc.append(Subsection(sectiontitle))

            accounts_data = []
            accounts_data.append(("Australia", ""))
            accounts_data.append(("USA", ""))

            self.formatTable(header=[], indent="X[l] X[l]", data=accounts_data)

            # Generating PDF file
            self.doc.generate_pdf(myfilename, clean_tex=False)

        except Exception as exp:
            raise exp
        finally:
            report.finalize()
            taoreport.finalize()

    def addTAOStats(self, taoreport):
        # -----------------------TAO Stats---------------------------------------------------
        # Start a new page for TAO Stats
        self.doc.append(NoEscape(r"\newpage"))
        self.doc.append(Section("TAO Usage Statistics"))

        print("Extracting General TAO Usage ...")
        sectiontitle = "General TAO Usage (total for {0} to {1})".format(taoreport.startdate.strftime("%d/%m/%Y"),
                                                                         taoreport.enddate.strftime("%d/%m/%Y"))
        self.doc.append(Subsection(sectiontitle))
        accounts_data = []
        accounts_data.append(("Number of jobs", taoreport.getnoofjobs()))
        accounts_data.append(("Number of active users", taoreport.getactiveusers()))

        datasize = taoreport.getdatasize()

        accounts_data.append(("Total records returned", datasize[1]))
        accounts_data.append(("Total data-size returned", datasize[0]))

        accounts_data.append(("Registered users", taoreport.getregisteredusers()))

        accounts_data.append(("Page views (Google analytics)", ""))
        accounts_data.append(("Unique users (Google analytics)", ""))

        self.formatTable(header=[], indent="X[l] X[l]", data=accounts_data)

        print("Extracting Data access breakdown per database ...")
        sectiontitle = "General Data access breakdown per database ({0} to {1})".format(
            taoreport.startdate.strftime("%d/%m/%Y"), taoreport.enddate.strftime("%d/%m/%Y"))
        self.doc.append(Subsection(sectiontitle))

        databasejobs = taoreport.getjobsbydatabase()
        accounts_data = databasejobs.items()

        self.formatTable(header=[], indent="X[l] X[l]", data=accounts_data)

        print("TAO site access by location from Google analytics ...")
        sectiontitle = "TAO site access by location from Google analytics ({0} to {1})".format(
            taoreport.startdate.strftime("%d/%m/%Y"),
            taoreport.enddate.strftime("%d/%m/%Y"))
        self.doc.append(Subsection(sectiontitle))

        accounts_data = []
        accounts_data.append(("Australia", ""))
        accounts_data.append(("USA", ""))

        self.formatTable(header=[], indent="X[l] X[l]", data=accounts_data)

    def generateSlurmReport(self, report, taoreport):
        try:
            myfilename = "latex_files/" + datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

            print("Extracting percentage of usage by institution ...")
            self.doc.append(Section("Usage by Institution "))
            self.formatTable(header=["Institution", "Usage"], indent="X[l] X[r]",
                             data=report.getSlurmInstitutionUsagePercent())

            print("Extracting astronomy account holders information...")
            self.doc.append(Section("Astronomy account holders(total/active for quarter): "))

            # Creating a list of tuples to add to table
            accounts_data = []

            astronomyusers = report.getOzSTARUsersCount([('gum_department.is_astronomy', ASTRONOMY)])
            activeastronomyusers = report.getSlurmActiveUsersCount([('gum_department.is_astronomy', ASTRONOMY)])
            astronomymales = report.getOzSTARUsersCount([
                ('gum_department.is_astronomy', ASTRONOMY), ('gum_user.gender', MALE)])
            activeastronomymales = report.getSlurmActiveUsersCount(
                [('gum_department.is_astronomy', ASTRONOMY), ('gum_user.gender', MALE)])
            astronomyfemales = report.getOzSTARUsersCount(
                [('gum_department.is_astronomy', ASTRONOMY), ('gum_user.gender', FEMALE)])
            activeastronomyfemales = report.getSlurmActiveUsersCount(
                [('gum_department.is_astronomy', ASTRONOMY), ('gum_user.gender', FEMALE)])
            astronomystudents = report.getOzSTARUsersCount(
                [('gum_department.is_astronomy', ASTRONOMY), ('gum_user.is_student', STUDENT)])
            activeastronomystudents = report.getSlurmActiveUsersCount(
                [('gum_department.is_astronomy', ASTRONOMY), ('gum_user.is_student', STUDENT)])
            swinastro = report.getOzSTARSwinAstronomersCount()
            activeswinastro = report.getSlurmActiveSwinAstronomersCount()

            accounts_data.append(("All", astronomyusers, activeastronomyusers))
            accounts_data.append(("Male", astronomymales, activeastronomymales))
            accounts_data.append(("Female", astronomyfemales, activeastronomyfemales))
            accounts_data.append(("PhD Student", astronomystudents, activeastronomystudents))
            accounts_data.append(("Swinburne", swinastro, activeswinastro))

            self.formatTable(header=["", "Total", "Active"], indent="X[l] X[r] X[r]", data=accounts_data)

            print("Extracting no of astronomy users in all institutions...")
            self.doc.append(Section("Institutions other than Swinburne with astronomy account holders"))
            self.formatTable(header=["Institution", "No of Users"], indent="X[l] X[r]",
                             data=report.getOzSTARInstitutionAstronomers())

            print("Extracting percentage of usage by project ...")
            self.doc.append(Section("Share of CPU hours usage by project"))
            self.formatTable(header=["Project", "Percentage of Total Usage"], indent="X[l] X[r]",
                             data=report.getSlurmProjectUsagePercent())

            print("Extracting percentage of usage by Demographic ...")
            self.doc.append(Section("Usage by Demographic"))
            accounts_data = []
            demographic = report.getSlurmUsageByDemographic()

            for groups in demographic.values():
                grouptxt = ["{0}:  {1}%".format(group, groups[group]) for group in groups.keys()]
                accounts_data.append(tuple(grouptxt))

            self.formatTable(header=[], indent="X[l] X[l]", data=accounts_data)

            # ######## TAO STATS
            self.addTAOStats(taoreport)

            # Generating PDF file
            self.doc.generate_pdf(myfilename, clean_tex=False)

        except Exception as exp:
            raise exp
        finally:
        #     report.finalize()
            taoreport.finalize()



    def formatTable(self, header, data, indent):

        with self.doc.create(LongTabu(indent)) as data_table:
            if header != []:
                data_table.add_row(header, mapper=[bold])
                data_table.add_hline()
                data_table.add_empty_row()

            for row in data:
                data_table.add_row(list(row))
                data_table.add_hline()

    def generateTxtFile(self, report):

        try:
            # create a text file named with current timestamp
            filename = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S") + ".txt"

            txtfile = open(filename, "w")
            print("file opened: " + filename)

            #Get Total usage per quarter
            print("Extracting percentage of usage by institution ...")
            txtfile.write("Usage by Institution: \n\n")
            mylist = report.getInstitutionUsagePercent()
            for (inst, usage) in mylist:
                txtfile.write("{0:100s} {1} \n".format(inst, usage))
            txtfile.write("\n\n")


            print("Extracting astronomy account holders information...")
            txtfile.write("Astronomy account holders(total/active for quarter): \n")
            txtfile.write("\n")
            txtfile.write("All:           {0}/{1} \n".format(report.getUsersCount(), report.getActiveUsersCount()))
            txtfile.write("Male:          {0}/{1} \n".format(report.getUsersCount([('is_astronomy', 1), ('gender', 2)]), report.getActiveUsersCount([('is_astronomy', 1), ('gender', 2)])))
            txtfile.write("Female:        {0}/{1} \n".format(report.getUsersCount([('is_astronomy', 1), ('gender', 1)]), report.getActiveUsersCount([('is_astronomy', 1), ('gender', 1)])))
            txtfile.write("PhD Student:   {0}/{1} \n".format(report.getUsersCount([('is_astronomy', 1), ('student', 1)]), report.getActiveUsersCount([('is_astronomy', 1), ('student', 1)])))
            txtfile.write("Swinburne:     {0}/{1} \n".format(report.getSwinAstronomersCount(), report.getActiveSwinAstronomersCount()))
            txtfile.write("\n\n")

            print("Extracting no of astronomy users in all institutions...")
            txtfile.write("Institutions other than Swinburne with astronomy account holders: \n\n")
            mylist = report.getInstitutionAstronomers()
            for (inst, noofusers) in mylist:
                txtfile.write("{0:70s} {1:5} \n".format(inst, noofusers))
                txtfile.write("\n\n")


            print("Extracting percentage of usage by project ...")
            txtfile.write("Share of CPU hours usage by project: \n\n")
            mylist = report.getProjectUsagePercent()
            for (proj, usage) in mylist:
                txtfile.write("{0:50s} {1} \n".format(proj, usage))
                txtfile.write("\n\n")


            print("Extracting percentage of usage by Demographic ...")
            txtfile.write("Usage by Demographic: \n")
            demographic = report.getUsageByDemographic()

            for (groups, values) in demographic.values():
                txtfile.write("{0} {1} \n".format("/".join(groups), "/".join(values)))

            txtfile.write("\n\n")

        except Exception as exp:
            raise(exp)
        finally:
            # closing file
            txtfile.close()
            report.finalize()


if __name__=='__main__':

    # calculate reporting period/quarter based on current date

    if (len(sys.argv) > 1):  # if month and year were provided as arguments
        quarter = int(sys.argv[1])
        year = int(sys.argv[2])
    else:
        today_date = datetime.date.today()
        quarter = int(today_date.month/3) + 2
        year = today_date.year

    startdate = None
    enddate = None

    # Setting up report start and end dates
    if quarter == 1:
        startdate = datetime.date(year, 7, 1)
        enddate = datetime.date(year, 9, 30)
    elif quarter == 2:
        startdate = datetime.date(year, 10, 1)
        enddate = datetime.date(year, 12, 31)
    elif quarter == 3:
        startdate = datetime.date(year, 1, 1)
        enddate = datetime.date(year, 3, 31)
    else:
        startdate = datetime.date(year, 4, 1)
        enddate = datetime.date(year, 6, 30)


    dbconfig = readdbconfig('db_config.ini')

    # Generating LaTex file (tex and pdf)

    # ReportFormat().generateReport(Report(dbconfig, startdate, enddate),
    #                               TAOreport(dbconfig, startdate, enddate))

    # dbconfig = readdbconfig('db_config.ini')
    # startdate = datetime.date(2017, 7, 1)
    # enddate = datetime.date(2018, 3, 31)
    # Report(dbconfig, startdate, enddate).getProjectCollaborationStats()
    report = Report(dbconfig, startdate, enddate)

    # taorep = TAOreport(dbconfig, startdate, enddate)
    # taorep.getactiveusersdata('2016-07-01', '2017-06-30')






