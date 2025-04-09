# gSTAR_STATS

3 Python utilities to:

- readStats.py, statsToDB: analyze Moab queue stat files and insert Stats in MySQL database. This script should run automatically everyday; check for unprocessed/partially processed files, parse and update database with new Job Stats.
    readStats.py is the entry point. statsToDB.py defines Job class and encloses all DB operations

- generateReport.py, reportFromDB.py: reads stats of a certain quarter from MySQL DB to generate gSTAR usage report, in LaTex format. The script to be run manually and will, by default, generate report of the most recent quarter.
    generateReport.py is the entry point, in addition to defining ReportFormat class which generates the Latex file in Latex_files/ directory. reportFromDB.py encloses all queries to get required data from DB.

- updateUsersInfo.py: used once-off to update existing users and institutions data from an up-to-date text file which was used to generate the report manually

- config/config.ini: specify the path to read Moab stats files from.

- config/db_config.ini: for DB connection details

- statsConfig: reads config files

- taoreportfromdb.py: reads TAO stats of a certain quarter from Postgres &  MySQL databases to generate the TAO usage part of the gSTAR report.
    generateReport.py is the entry point, in addition to defining ReportFormat class which generates the Latex file in Latex_files/ directory. taoreportfromdb.py encloses all TAO queries to get required data from DB.