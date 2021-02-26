from .message_abc import MessageABC, LogType


class Measurement(MessageABC):
    KEY_COUNTER_MID = 'counter.mid'
    KEY_COUNTER_OBJECT = 'counter.object'
    KEY_COUNTER_ID = 'counter.id'
    KEY_COUNTER_VALUE = 'counter.value'
    KEY_COUNTER_VALUE_TYPE = "counter.value_type"
    KEY_COUNTER_TYPE = 'counter.type'
    KEY_COUNTER_BUCKETS = 'counter.bucket_quantile'

    TYPE=['Counter', 'Gauge', 'Histogram', 'Summary']

    def __init__(self, level=None, neid=None, host=None, system=None):
        '''
        Constructor of Measurement
        :param level: log level, of value as enum SyslogLevel, which is a mandatory field
        :param neid: unique ID assigned to the system producing the event, which is a mandatory field
        :param host: name of the host on which the container producing the event is running, whic is a mandatory field
        :param system: name of the system in which the event occurred, which is a mandatory field
        '''
        super(Measurement, self).__init__(level, neid, host, system)
        self._mandatory_fileds.append(self.KEY_COUNTER_MID)
        self._mandatory_fileds.append(self.KEY_COUNTER_OBJECT)
        self._mandatory_fileds.append(self.KEY_COUNTER_ID)
        self._mandatory_fileds.append(self.KEY_COUNTER_VALUE)
        self.set_type(LogType.COUNTER)

    def set_counter_mid(self, mid):
        '''
        Set measurement ID
        :param mid: Measurement ID, which is a mandatory field
        :return: self
        :raise TypeError: if the parameter mid is not an str
        '''
        if not isinstance(mid, str):
            raise TypeError('The parameter mid must be a str')
        self._msg.set(self.KEY_COUNTER_MID, mid)
        return self

    def set_counter_object(self, obj):
        '''
        Set measured object identification
        :param obj: Measured object identification, which is a mandatory field.
        :return: self
        :raise TypeError: if the parameter obj is not an str
        '''
        if not isinstance(obj, str):
            raise TypeError('The parameter obj must be a str')
        self._msg.set(self.KEY_COUNTER_OBJECT, obj)
        return self

    def set_counter_id(self, id):
        '''
        Set ID of the particular PM counter.
        :param id: ID of the particular PM counter, which is a mandatory field
        :return: self
        :raise TypeError: if the parameter id is not an str
        '''
        if not isinstance(id, str):
            raise TypeError('The parameter id must be a str')
        self._msg.set(self.KEY_COUNTER_ID, id)
        return self

    def set_counter_value(self, value, type=None):
        '''
        Set value of the particular PM counter
        :param value: Value of the particular PM counter, which is a mandatory field.
        :param type: Type of value "int" and "float".
        :return: self
        :raise TypeError: if the parameter value is not an int or type is not "int"/"float"
        '''
        if not isinstance(value, int) and not isinstance(value, float):
            raise TypeError('The parameter value must be an int or float')
        if type:
            type_list=["int","float"]
            if type not in type_list:
                raise TypeError('The parameter type must be %s' %type_list)
            self._msg.set(self.KEY_COUNTER_VALUE_TYPE, value)
        self._msg.set(self.KEY_COUNTER_VALUE, value)
        return self

    def set_counter_type(self, type):
        '''
        Set type of the particular PM counter
        :param type: Type of the particular PM counter, ['Counter', 'Gauge', 'Histogram', 'Summary'].
        :return: self
        :raise TypeError: if the parameter is not valid
        '''
        if type not in self.TYPE:
            raise TypeError('The parameter type must be in %s' %self.TYPE)
        self._msg.set(self.KEY_COUNTER_TYPE, type)
        return self

    def set_counter_buckets(self, buckets):
        '''
        Set buckets of the particular PM counter
        :param buckets: This field is MUST if the counter.type is Hostogram or Summary. Example value is '0.1, 1, 5, 10'.
        :return: self
        :raise TypeError: if the parameter is not valid
        '''
        if not isinstance(buckets, str):
            raise TypeError('The parameter buckets must be str')
        self._msg.set(self.KEY_COUNTER_BUCKETS, buckets)
        return self

    def validate(self):
        '''
        Check if any of the mandatory fields are missing.
        :return: None
        :raise MissingMandatoryField: if any of the mandatory fields is missing
        '''
        self._msg.validate(self._mandatory_fileds)

    def set_measurement_mandatory_fields(self, mid, id, obj, value, type=None):
        '''
        Set some mandatory fields of the counter message, mid, object, id, and value.
        :param mid: Measurement ID, which is a mandatory field
        :param id: ID of the particular PM counter, which is a mandatory field
        :param obj: Measured object identification, which is a mandatory field.
        :param value: Value of the particular PM counter, which is a mandatory field.
        :param type: type of value, "int" or "float"
        :return: self
        :raise TypeError: if the parameters type mismatch
        '''
        self.set_counter_mid(mid)
        self.set_counter_id(id)
        self.set_counter_object(obj)
        self.set_counter_value(value, type)
        self.set_counter_type('Gauge')
        return self