'''
Created on 16-Sep-2021

@author: rithomas
'''
from hashlib import sha224
import os
import os.path
import datetime


class Datetime(object):
    def __init__(self, date=None):
        '''
        Constructor of time
        :param date: the datetime
        '''
        if date is not None and not isinstance(date, datetime.date):
            raise TypeError('The parameter date must be a datetime.date')
        self._date = date

    def tostring(self):
        '''
        Convert to a string in RFC3339 timestamp format in UTC timezone, e.g. 2018-01-23T08:09:27.064Z
        :return: the string of timestamp
        '''
        if self._date is None:
            self._date = datetime.datetime.utcnow()
        # Convert YYYY-MM-DDTHH:MM:SS.mmmmmm to YYYY-MM-DDTHH:MM:SS.sss
        return self._date.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'


class Timezone(object):
    tz_env_name = 'TZ'
    localtime_path = '/etc/localtime'
    info_dir = '/usr/share/zoneinfo/'
    info_dir = os.path.join(info_dir, '')
    default_tz = 'UTC'
    tz = None

    @classmethod
    def get_buffered_timezone(cls):
        '''
        Get the buffered host's timezone in the form of Area/Location
        :return: the string of timezone
        '''
        if cls.tz is None:
            cls.tz = cls.get_timezone()
        return cls.tz

    @classmethod
    def get_timezone(cls):
        '''
        Get the host's timezone in the form of Area/Location.
        This function will fetch the value from the TZ environment variable firstly.
        If TZ environment variable is not defined, this function will fetch the value from /etc/localhost.
        This function will update the buffered timezone
        :return: the string of timezone
        '''
        tz_env = cls._get_timezone_from_env(cls.tz_env_name)
        tzname = None
        if tz_env:
            tzname = tz_env
        else:
            if os.path.islink(cls.localtime_path):
                tzname = cls._get_timezone_from_link(cls.localtime_path)
            elif os.path.isfile(cls.localtime_path):
                tzname = cls._get_timezone_from_file(cls.localtime_path)
        if tzname is None:
            tzname = cls.default_tz
        cls.tz = tzname
        return cls.tz

    @staticmethod
    def _get_timezone_from_env(env):
        return os.environ.get(env)

    @classmethod
    def _get_timezone_from_link(cls, link):
        if not os.path.islink(link):
            return None
        path = os.path.realpath(os.readlink(link))
        if path.rfind(cls.info_dir, 0, len(cls.info_dir)) == 0:
            return path[len(cls.info_dir):]
        else:
            return cls._get_timezone_from_file(path)

    @classmethod
    def _get_timezone_from_file(cls, path):
        try:
            with open(path, 'rb') as f:
                tzfile_digest = sha224(f.read()).hexdigest()
        except IOError:
            return None

        def test_match(filepath):
            try:
                with open(filepath, 'rb') as f:
                    return tzfile_digest == sha224(f.read()).hexdigest()
            except IOError:
                return False

        def walk_over(dirpath):
            for root, dirs, filenames in os.walk(dirpath):
                for fname in filenames:
                    fpath = os.path.join(root, fname)
                    if test_match(fpath):
                        return fpath[len(cls.info_dir):]
            return None

        for dname in os.listdir(cls.info_dir):
            if dname in ('posix', 'right', 'SystemV', 'Etc'):
                continue
            dpath = os.path.join(cls.info_dir, dname)
            if not os.path.isdir(dpath):
                continue
            tzname = walk_over(dpath)
            if tzname is not None:
                return tzname
        # if does not find in the sub-directories of info_dir, try to find it in info_dir
        return walk_over(cls.info_dir)