import time
import datetime
import dateutil.relativedelta

def get_current_time():
    # TODO return timezone or utc? get config from user?
    return datetime.datetime.utcnow()

def get_time_delta(**kwargs):
    return dateutil.relativedelta.relativedelta(**kwargs)

def time_string_to_epoch(string, format):
    return time.mktime(time.strptime(string, format))

def datetime_to_epoch(dt):
    return time.mktime(dt.timetuple())
