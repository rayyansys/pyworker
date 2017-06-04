import datetime
import dateutil.relativedelta

def get_current_time():
    # TODO return timezone or utc? get config from user?
    return datetime.datetime.utcnow()

def get_time_delta(**kwargs):
    return dateutil.relativedelta.relativedelta(**kwargs)

