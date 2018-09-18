import sys
import io
import mysql.connector
from mysql.connector import errorcode

def readUsersFile():

    dbUsers = readUsersTable()
    filename = "files/ulist.dat"
    mismatchGCount = 0
    mismatchSCount = 0
    mismatchICount = 0

    genderDic = {2:'Male', 1:'Female', 0:'None',
                 'M':'Male', 'F':'Female'}

    instDic = {'SUT':'Swinburne', 'MON':'monash', 'MEL':'UM',
               'SYD':'sydney',
               'UQ':'UQ',
               'MQ':'MQ',
               'ANU':'ANU',
               'UWA':'UWA',
               'AAO':'AAO',
               'TAS':'UTAS',
               'VRJ':'vrije',
               'KUM':'Kumamoto',
               'NAG':'Nagoya',
               'PAD':'unipd',
               'LYN':'Lyon',
               'NRL':'NRL',
               'RRI':'RRI',
               'MAN':'manchester',
               'BON':'bonn',
               'GLA':'GLA',
               'AUT':'aut',
               'CSI':'CSIRO',
               'NMS':'NMSU',
               'INA':'INA',
               'CAM':'cambridge',
               'STA':'Stanford',
               'IND':'IND',
               'UMB':'UMB',
               'ETH':'ETHZ',
               'NIR':'astron',
               'TOR':'toronto',
               'UCS':'ucsc',
               'TAM':'TAMU',
               'UCD':'ucdavis',
               'YAL':'YAL',
               'PMH':'Portsmouth',
               'TUB':'TUB',
               'GGU':'GGU',
               'LED':'leiden',
               'TOK':'TOK',
               'CUR':'curtin',
               'UCB':'UCB',
               'WVU':'WVU',
               'MPI':'MPI',
               'NSW':'UNSW'}

    #try:
    with open(filename) as usersinfo:
        for line in usersinfo:
            user = line.strip().split()
            print('file:{} institution {} student {} gender {}'.format(user[0], user[1], user[4], genderDic[user[3]]))

            try:
            #get corresponding user info from database
                dbUser = dbUsers[user[0]]
                if dbUser in dbUsers:
                    print('database:{} institution {} student {} gender {}'.format(dbUser[0], dbUser[1], dbUser[2], genderDic[dbUser[3]]))

                    # compare student
                    if (user[4] == 'Y' and dbUser[2] == 1) or (user[4] == 'N' and dbUser[2] == 0):
                        print('Student match')
                    else:
                        print('Student mismatch')
                        mismatchSCount += 1

                    # compare Institution
                    if user[1] == dbUser[1] or instDic[user[1]] == dbUser[1]:
                        print('Institution match')
                    else:
                        print('Institution mismatch')
                        mismatchICount += 1

                    # compare gender
                    if genderDic[user[3].strip()] == genderDic[dbUser[3]]:
                        print('Gender match')
                    else:
                        print('Gender mismatch')
                        mismatchGCount += 1

                else:
                    print('not in database')
                print()
            except KeyError:
                print('user not in database')
                pass

        print("no of gender mismatches = " + str(mismatchGCount))
        print("no of institution mismatches = " + str(mismatchICount))
        print("no of student mismatches = " + str(mismatchSCount))
    #except:
    #    pass


def readUsersTable():
    try:
        con = mysql.connector.connect(user='eman', password='TestPassword', host='127.0.0.1',
                                           database='gSTAR_Accounts')
        cursor = con.cursor()
        print('Connected')

        selectUsers = (
            "SELECT username, institution_code, student, gender "
            "FROM gSTAR_Accounts.user, gSTAR_Accounts.institution, user_institutions "
            "WHERE user.user_id = user_institutions.user_id "
            "AND institution.institution_id = user_institutions.institution_id; "
        )

        cursor.execute(selectUsers)
        users = cursor.fetchall()
        usersDict = {}

        if users:
            print('Database users list')
            for user in users:
                print(user)
                usersDict[user[0]] = user

        cursor.close()
        con.close()

        return usersDict

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)


if __name__=='__main__':
    print("reading users file")
    readUsersFile()


