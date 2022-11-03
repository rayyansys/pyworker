import sys
major_version = sys.version_info.major
if major_version == 2:
    from urlparse import urlparse, parse_qs
elif major_version == 3:
    from urllib.parse import urlparse, parse_qs
import psycopg2

class DBConnector(object):
    def __init__(self, dbstring, logger):
        super(DBConnector, self).__init__()
        url = urlparse(dbstring)
        self._database = url.path[1:]
        self._username = url.username.replace('%40', '@')
        self._passwd = url.password
        self._host = url.hostname
        self._port = url.port
        qs = parse_qs(url.query)
        self._sslmode = qs.get('sslmode', ['prefer'])[0]
        self.logger = logger

    def connect(self): 
        self._connection = psycopg2.connect(database=self._database,
            user=self._username, password=self._passwd,
            host=self._host, port=self._port, sslmode=self._sslmode)
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
