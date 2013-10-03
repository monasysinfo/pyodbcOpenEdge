from django.db.backends.creation import BaseDatabaseCreation
import base64
#from django.utils.hashcompat import md5_constructor
from hashlib import md5
import random

class DataTypesWrapper(dict):
    def __getitem__(self, item):
        if item in ('PositiveIntegerField', 'PositiveSmallIntegerField'):
            # The check name must be unique for the database. Add a random
            # component so the regresion tests don't complain about duplicate names
            fldtype = {'PositiveIntegerField': 'int', 'PositiveSmallIntegerField': 'smallint'}[item]
            #rnd_hash = md5_constructor(str(random.random())).hexdigest()
            rnd_hash = md5(str(random.random())).hexdigest()
            unique = base64.b64encode(rnd_hash, '__')[:6]
            return '%(fldtype)s ' % locals()
        return super(DataTypesWrapper, self).__getitem__(item)

class DatabaseCreation(BaseDatabaseCreation):
    # This dictionary maps Field objects to their associated MS SQL column
    # types, as strings. Column-type strings can contain format strings; they'll
    # be interpolated against the values of Field.__dict__ before being output.
    # If a column type is set to None, it won't be included in the output.
    #
    # Any format strings starting with "qn_" are quoted before being used in the
    # output (the "qn_" prefix is stripped before the lookup is performed.

    data_types = DataTypesWrapper({
    #data_types = {
        'AutoField':         'int' ,
        'BigIntegerField':   'bigint',
        'BooleanField':      'int',
        'CharField':         'varchar(%(max_length)s)',
        'CommaSeparatedIntegerField': 'varchar(%(max_length)s)',
        'DateField':         'date',        
        'DateTimeField':     'timestamp',
        'DecimalField':      'decimal(%(max_digits)s, %(decimal_places)s)',
        'FileField':         'varchar(%(max_length)s)',
        'FilePathField':     'varchar(%(max_length)s)',
        'FloatField':        'float',
        'IntegerField':      'int',
        'IPAddressField':    'varchar(15)',
        'GenericIPAddressField': 'varchar(20)',
        'NullBooleanField':  'int',
        'OneToOneField':     'int',
        #'PositiveIntegerField': 'integer CONSTRAINT [CK_int_pos_%(column)s] CHECK ([%(column)s] >= 0)',
        #'PositiveSmallIntegerField': 'smallint CONSTRAINT [CK_smallint_pos_%(column)s] CHECK ([%(column)s] >= 0)',
        'SlugField':         'varchar(%(max_length)s)',
        'SmallIntegerField': 'smallint',
        'TextField':         'varchar(255)',        
        'TimeField':         'time',
    #}
    })

    