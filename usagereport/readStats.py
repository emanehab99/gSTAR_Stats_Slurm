"""
Entry point to read gSTAR stats from log files and insert into database for the purpose of generating quarterly report to AAL
Usage:
    python3 readStats.py
    OR
    python3 readStats.py Mar 2017
"""
import sys

from usagereport import statsConfig
from statsToDB import Stats


if __name__=='__main__':

    filter = 'events*'

    if(len(sys.argv) > 1): #if month and year were provided as arguments
        filter +=  sys.argv[1] + '*' + sys.argv[2] + '*' #filter = Month*Year*

    path = statsConfig.read_path('config.ini') + filter
    Stats(path, statsConfig.readdbconfig('db_config.ini')).parseStats()

    print("Done")
