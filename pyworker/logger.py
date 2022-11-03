class Logger(object):
    def __init__(self, logger):
        self.logger = logger

    def debug(self, message):
        try:
            self.logger.debug(message)
        except:
            print("DEBUG: %s" % message)

    def info(self, message):
        try:
            self.logger.info(message)
        except:
            print("INFO: %s" % message)

    def warning(self, message):
        try:
            self.logger.warning(message)
        except:
            print("WARNING: %s" % message)

    def error(self, message):
        try:
            self.logger.error(message)
        except:
            print("ERROR: %s" % message)
