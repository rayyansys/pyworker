import urlparse
import psycopg2

class DBConnector(object):
    def __init__(self, dbstring, logger):
        super(DBConnector, self).__init__()
        url = urlparse.urlparse(dbstring)
        self._database = url.path[1:]
        self._username = url.username.replace('%40', '@')
        self._passwd = url.password
        self._host = url.hostname
        self._port = url.port
        self.logger = logger

    def connect(self): 
        self._connection = psycopg2.connect(database=self._database,
            user=self._username, password=self._passwd,
            host=self._host, port=self._port)
        cursor = self._connection.cursor()
        self.logger.info("Connected to database")
        return self
        
    def cursor(self):
        return self._connection.cursor()

    def commit(self):
        self._connection.commit()

    def disconnect(self):
        self._connection.close(); 
        self.logger.info("Disconnected from database")
