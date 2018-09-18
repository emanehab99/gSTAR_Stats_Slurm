"""
Read gSTAR Job stats from Moab event log files and insert in Stats MySQL database

"""

import glob
import time
import os

from mysql.connector import errorcode
import mysql.connector


class Job(object):
    """
    class to hold Job data read from log file
    """
    def __init__(self, jobStats):

        """
        maps event data from log file to Job object 
        
        :param jobStats: vector of strings - contains job data as parsed from log file
        
        """
        try:

            # Check if it is a job event. Found few other types
            if jobStats[2] != 'job':
                raise TypeError

            if jobStats[4] != 'JOBEND':
                raise ValueError

            # Event information
            self.eventTime = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(jobStats[1].split(':')[0])))
            self.eventID = jobStats[1].split(':')[1]

            # Job status
            self.eventType = jobStats[4]
            self.nodes = int(jobStats[5])
            self.cpus = int(jobStats[6])

            # User information
            self.user = jobStats[7]
            self.group = jobStats[8]
            self.account = jobStats[28]

            # Job information
            self.jobID = jobStats[3]
            self.submit = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(jobStats[12])))
            self.start = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(jobStats[14])))
            self.end = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(jobStats[15])))
            self.eligible = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(jobStats[55])))

            self.serviceUnits = (int(jobStats[15]) - int(jobStats[14])) * self.cpus/60./60.

            self.features = Job.getStatValue(jobStats[22])
            if jobStats[26]:
                self.qosRequested = Job.getStatValue(jobStats[26].split(':')[0])
                self.qosDelivered = Job.getStatValue(jobStats[26].split(':')[1])

            self.partition = jobStats[33]
            self.memory = int(jobStats[37].split('M')[0]) #in Megabytes
            self.rsv = Job.getStatValue(jobStats[43])
            self.reqwall = int(jobStats[9])
            self.queue = Job.getStatValue(jobStats[11]).replace('[', '').replace(':1]','')

            self.complete = True

        except TypeError as err:
            self.complete = False
            # print(err)
            # print('Not a job event')
        except ValueError:
            self.complete = False
            pass




    def getStatValue(value):
        """
        In event log file, stat values of type String are set to '-' if empty.
        This method returns the actual stat value if exists or otherwise an empty string
        
        :param value: Stat value from file as string
        :return: stat value
        """
        if value == '-':
            return ''
        return value

class Stats(object):
    """
    Enclose all operations to parse stats log files and insert job stats into database 
    
    """
    def __init__(self, path, dbconfig):
        """
        Initialize log file path to read stats from as well as DB connection
        
        :param path: log file path
        :param dbConfig: db connection dictionary
        
        """
        # Set Stats file path
        self.path = path

        try:
            # connect to database schema using dbConfig dictionary
            mysqldb = dbconfig['mysql']
            self.con = mysql.connector.connect(**mysqldb)
            self.cursor = self.con.cursor()
            print('connected')

            self.admins = dbconfig['admins']

        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)


    def parseStats(self):
        """
        Loop through log files, parse each file into Job objects (if it wasn't already processed) and insert into database
        
        :return: None 
        """
        try:
            files = glob.glob(self.path)
            print('no of files: ' + str(len(files)))

            for filepath in files:
                if self.updateProcessedFiles(filepath):  # if the file was not or was partially processed, parse and process
                    with open(filepath) as myfile:
                         [self.insertEvent(Job(l.split())) for l in (line.strip() for line in myfile) if l] # insert jobs in database

            if(self.admins != []):
                self.deleteAdminUsage()

        except Exception as e:
            raise e
        finally:
            self.cursor.close()
            self.con.close()


    def insertEvent(self, job):
        """
        Insert Job object in job_event table
        
        :return: None
         
        """

        if job:
            if not job.complete:
                return
            try:

                addEventStatement = (
                    'INSERT INTO job_event(ID,`time`,`type`,nodes,cpus,`user`,`group`,`account`,'
                    'job_id,submit_time,start_time,end_time,eligible_time,queue,'
                    'reqwall,features,`memory`,`partition`,rsv,qos_requested,qos_delivered,service_units) '
                    'VALUES( %(eventID)s , %(eventTime)s ,  %(eventType)s , %(nodes)s , %(cpus)s ,'
                    ' %(user)s , %(group)s ,  %(account)s , %(jobID)s , %(submit)s ,'
                    ' %(start)s , %(end)s ,  %(eligible)s , %(queue)s , %(reqwall)s ,'
                    ' %(features)s ,  %(memory)s , %(partition)s , %(rsv)s , %(qosRequested)s, %(qosDelivered)s, %(serviceUnits)s )'
                )
                # eventData = (job.eventID, job.eventTime, job.eventType, job.nodes, job.cpus, job.user, job.group, job.account, job.jobID, job.submit, job.start, job.end, job.eligible, job.queue, job.reqwall, job.features, job.memory, job.partition, job.rsv, job.qosRequested, job.qosDelivered, job.serviceUnits)


                try:

                    self.cursor.execute(addEventStatement, job.__dict__)
                    self.con.commit()

                except mysql.connector.Error as err:
                    if err.errno == errorcode.ER_DUP_KEY:
                        pass
                    else:
                        print(err)
            except AttributeError as err:
                pass

    def updateProcessedFiles(self, path):

        """
        Updates Processed files table in database. Insert new record if file wasn't processed before. 
        Updates existing record if only part of the file was processed. Does nothing if the whole file was processed
        
        :param path: Log file path
        :return: True if updates were done. False if no change
        
        """
        # Get filesize
        filesize = os.path.getsize(path)

        try:
            # print("update")
            selectFileStatement = "SELECT size FROM processed_log_file WHERE name= %s"

            self.cursor.execute(selectFileStatement, (path,))
            size = self.cursor.fetchone()

            if not size:    # file wasn't processed before
                insertFileStatement = "INSERT INTO processed_log_file(name, size) VALUES(%s, %s)" # insert in database
                self.cursor.execute(insertFileStatement, (path, filesize))

            elif size[0] == filesize:   # whole file was processed before
                return False
            else: # part of the file was processed
                updateFileStatement = "UPDATE processed_log_file SET size= %s WHERE name= %s"  # update file size in table
                self.cursor.execute(updateFileStatement, (filesize, path))

            self.con.commit()
            return True

        except mysql.connector.Error as err:
            # print(err)
            return False

    def deleteAdminUsage(self):
        """
        delete usage(events) by system admins, so not to be count in Stats and report. admin usernames are listed in config.ini
        :return: 
        """
        try:

            print(tuple(self.admins))

            deleteAdminUsageStat = "DELETE FROM job_event WHERE job_event.user IN " + str(tuple(self.admins))


            print(deleteAdminUsageStat)
            self.cursor.execute(deleteAdminUsageStat)
            self.con.commit()


        except mysql.connector.Error as err:
            print(err)
            raise(err)







