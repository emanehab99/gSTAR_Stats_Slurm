import mysql


class DatabaseConnection(object):

    def __new__(cls, dbconfig):
        _instance = None

        if cls._instance is None:

            cls._instance = object.__new__(cls)

            sqldb = dbconfig['mysql']

            try:
                print('Connecting to MySQL Database...')
                _conn = DatabaseConnection._instance._conn = mysql.connector.connect(**sqldb)
                _cursor = DatabaseConnection._instance._cursor = _conn.cursor()
                _cursor.execute('SELECT VERSION()')
                version = _cursor.fetchone()

            except Exception as error:
                print('Error: MySQL connection is not established {}'.format(error))
                _instance = None
            else:
                print('Connection established\n{}'.format(version))

        return cls._instance

    def __del__(self):
        self.connection = self._instance._conn
        self.cursor = self._instance._cursor

    def __enter__(self):
        return self

    def __init__(self):
        self.connection = self._instance._conn
        self.cursor = self._instance._cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        self.connection.close()

    @property
    def connection(self):
        return self._conn

    @property
    def cursor(self):
        return self._cursor

    def fetchone(self):
        return self.cursor.fetchone()

    def fetchall(self):
        return self.cursor.fetchall()

    def query(self, statement, params):
        try:
            result = self.cursor.execute(statement, params)
        except Exception as error:
            print('Error executing query {} \n{}'.format(statement, error))
            return None
        else:
            return result



