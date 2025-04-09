from configparser import ConfigParser
import os


def readdbconfig(filename='db_config.ini'):
    """
    Read db_config.ini file and generate all required database connection configuration and other settings; admin users list to be discarded
    :param filename: DB configuration filename
    :return: Dictionary of dictionaries, each dictionary represents a single DB configuration settings, 
            as well as admin users to be discarded from stats from both TAO and gSTAR database
    """
    # create parser and read ini configuration file
    parser = ConfigParser()
    parser.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'config', filename))

    # vector of all configs (dictionaries)
    myconfig = {}

    # gSTAR stats mysql
    section = "mysql"
    myconfig[section] = getsectionitems(parser, section)

    # TAO stats mysql
    section = "tao-mysql"
    myconfig[section] = getsectionitems(parser, section)

    # TAO stats postgres
    section = "tao-postgres"
    myconfig[section] = getsectionitems(parser, section)

    # TAO stats admins
    section = "tao-admins"
    myconfig[section] = getsectionitems(parser, section)['admin_users']

    # gSTAR stats admins
    section = "admins"
    myconfig[section] = getsectionitems(parser, section)['admin_users'].split(',')

    return myconfig

def getsectionitems(myparser, mysection):
    """
    returns all items in a single section as a dictionary
    
    :param myparser: Config file parser
    :param mysection: section name to be read
    :return: dictionary of section items
    """
    mydict = {}
    if myparser.has_section(mysection):
        items = myparser.items(mysection)
        for item in items:
            mydict[item[0]] = item[1]
    else:
        raise Exception('{0} not found'.format(mysection))

    return mydict

def read_path(filename='config.ini', section='statspath'):

    # create parser and read ini configuration file
    parser = ConfigParser()
    mypath = parser.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'config', filename))
    print(mypath)

    path = ""
    if parser.has_section(section):
        items = parser.items(section)
        for item in items:
            path = item[1]
    else:
        raise Exception('{0} not found in the {1} file'.format(section, filename))

    return path