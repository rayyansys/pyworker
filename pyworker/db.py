import urlparse
import psycopg2

class DBConnector(object):
    def __init__(self, dbstring, logger):
        super(DBConnector, self).__init__()
        url = urlparse.urlparse(dbstring)
        self._database = url.path[1:]
        self._username = url.username
        self._passwd = url.password
        self._host = url.hostname
        self._port = url.port
        self.logger = logger

    def connect_database(self): 
        self._connection = psycopg2.connect(database=self._database,
            user=self._username, password=self._passwd,
            host=self._host, port=self._port)
        cursor = self._connection.cursor()
        self.logger.info("Connected to DelayedJob database")
        return cursor
        
    def commit(self):
        self._connection.commit()

    def disconnect_database(self):
        self._connection.close(); 
        self.logger.info("Disconnected from DelayedJob database")

