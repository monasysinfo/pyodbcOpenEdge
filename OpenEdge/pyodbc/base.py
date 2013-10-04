# -*- coding: utf-8 -*-
"""
OpenEdge backend for Django.
"""

try:
    import pyodbc as Database
except ImportError, e:
    from django.core.exceptions import ImproperlyConfigured
    raise ImproperlyConfigured("Error loading pyodbc module: %s" % e)

import re
m = re.match(r'(\d+)\.(\d+)\.(\d+)(?:-beta(\d+))?', Database.version)
vlist = list(m.groups())
if vlist[3] is None: vlist[3] = '9999'
pyodbc_ver = tuple(map(int, vlist))
if pyodbc_ver < (2, 0, 38, 9999):
    from django.core.exceptions import ImproperlyConfigured
    raise ImproperlyConfigured("pyodbc 2.0.38 or newer is required; you have %s" % Database.version)

from django.db.backends import BaseDatabaseWrapper, BaseDatabaseFeatures, BaseDatabaseValidation
from django.db.backends.signals import connection_created
from django.conf import settings
from django import VERSION as DjangoVersion
if DjangoVersion[:2] == (1,2) :
    from django import get_version
    version_str = get_version()
    if 'SVN' in version_str and int(version_str.split('SVN-')[-1]) < 11952: # django trunk revision 11952 Added multiple database support.
        _DJANGO_VERSION = 11
    else:
        _DJANGO_VERSION = 12
elif DjangoVersion[:2] == (1,1):
    _DJANGO_VERSION = 11
elif DjangoVersion[:2] == (1,0):
    _DJANGO_VERSION = 10
elif DjangoVersion[0] == 1:
    _DJANGO_VERSION = 13
else:
    _DJANGO_VERSION = 9

#===============================================================================
# Maximum length of table name in OpenEdge
#===============================================================================
MAX_TABLE_NAME=32
MAX_INDEX_NAME=32
    
from OpenEdge.pyodbc.operations import DatabaseOperations
from OpenEdge.pyodbc.client import DatabaseClient
from OpenEdge.pyodbc.creation import DatabaseCreation
from OpenEdge.pyodbc.introspection import DatabaseIntrospection
import os
import warnings

warnings.filterwarnings('error', 'The DATABASE_ODBC.+ is deprecated', DeprecationWarning, __name__, 0)

#TODO: is usefull to OE ?
collation = 'Latin1_General_CI_AS'

deprecated = (
    ('DATABASE_ODBC_DRIVER', 'driver'),
    ('DATABASE_ODBC_DSN', 'dsn'),
    ('DATABASE_ODBC_EXTRA_PARAMS', 'extra_params'),
)
for old, new in deprecated:
    if hasattr(settings, old):
        warnings.warn(
            "The %s setting is deprecated, use DATABASE_OPTIONS['%s'] instead." % (old, new),
            DeprecationWarning
        )

DatabaseError = Database.DatabaseError
IntegrityError = Database.IntegrityError

class DatabaseFeatures(BaseDatabaseFeatures):
    uses_custom_query_class = True
    can_use_chunked_reads = False
    can_return_id_from_insert = True
    #uses_savepoints = True
    has_bulk_insert = True

class DatabaseWrapper(BaseDatabaseWrapper):
    drv_name = None
    driver_needs_utf8 = True
    MARS_Connection = False
    unicode_results = False
    datefirst = 7

    operators = {
        #TODO: is usefull to OE ?
        # Since '=' is used not only for string comparision there is no way
        # to make it case (in)sensitive. It will simply fallback to the
        # database collation.
        'exact': '= %s',
        'iexact': "= UPPER(%s)",
        'contains': "LIKE %s ESCAPE '\\' COLLATE " + collation,
        'icontains': "LIKE UPPER(%s) ESCAPE '\\' COLLATE "+ collation,
        'gt': '> %s',
        'gte': '>= %s',
        'lt': '< %s',
        'lte': '<= %s',
        'startswith': "LIKE %s ESCAPE '\\' COLLATE " + collation,
        'endswith': "LIKE %s ESCAPE '\\' COLLATE " + collation,
        'istartswith': "LIKE UPPER(%s) ESCAPE '\\' COLLATE " + collation,
        'iendswith': "LIKE UPPER(%s) ESCAPE '\\' COLLATE " + collation,

        # TODO: remove, keep native T-SQL LIKE wildcards support
        # or use a "compatibility layer" and replace '*' with '%'
        # and '.' with '_'
        'regex': 'LIKE %s COLLATE ' + collation,
        'iregex': 'LIKE %s COLLATE ' + collation,

        # TODO: freetext, full-text contains...
    }

    def __init__(self, *args, **kwargs):
        super(DatabaseWrapper, self).__init__(*args, **kwargs)
        
        #TODO: is usefull to OE ?
        if 'OPTIONS' in self.settings_dict:
            self.MARS_Connection = self.settings_dict['OPTIONS'].get('MARS_Connection', False)
            self.datefirst = self.settings_dict['OPTIONS'].get('datefirst', 7)
            self.unicode_results = self.settings_dict['OPTIONS'].get('unicode_results', False)

        if _DJANGO_VERSION >= 13:
            self.features = DatabaseFeatures(self)
        else:
            self.features = DatabaseFeatures()
        self.ops = DatabaseOperations(self)
        self.client = DatabaseClient(self)
        self.creation = DatabaseCreation(self)
        self.introspection = DatabaseIntrospection(self)
        if _DJANGO_VERSION >= 12:
            self.validation = BaseDatabaseValidation(self)
        else:
            self.validation = BaseDatabaseValidation()

        self.connection = None
        self.owner = None

    def _cursor(self):
        new_conn = False
        settings_dict = self.settings_dict
        #=======================================================================
        # DSN=eslemien;HOST=localhost;DB=eslemien;UID=jyp;PWD=jyp;PORT=50000
        #=======================================================================
        db_str, user_str, passwd_str, port_str = None, None, "", None
        dual_str='DUAL'
        
        if settings_dict.has_key('TYPECNX'):
            if settings_dict['TYPECNX'].has_key('DSN'):
                #===================================================================
                # DSN
                #===================================================================
                
                typecnx_str = 'DSN=%s'%settings_dict['TYPECNX']['DSN']

            elif settings_dict['TYPECNX'].has_key('DRIVER'):
                #===================================================================
                # DRIVER
                #===================================================================
                typecnx_str = 'DRIVER={%s}'%settings_dict['TYPECNX']['DRIVER']        
        
            
        #===================================================================
        # DUAL TABLE
        #===================================================================
        if settings_dict.has_key('DUALTABLE'):
            dual_str = settings_dict['DUALTABLE']
                
        #===================================================================
        # Default Schema
        #===================================================================
        if settings_dict['DEFAULTSCHEMA']:
            defschema_str = settings_dict['DEFAULTSCHEMA']
        else:
            defschema_str = settings_dict['USER']
                
                
        if _DJANGO_VERSION >= 12:
            options = settings_dict['OPTIONS']
            if settings_dict['NAME']:
                db_str = settings_dict['NAME']
                
            if settings_dict['HOST']:
                host_str = settings_dict['HOST']
            else:
                host_str = 'localhost'
            if settings_dict['USER']:
                user_str = settings_dict['USER']
                
                
            if settings_dict['PASSWORD']:
                passwd_str = settings_dict['PASSWORD']
            if settings_dict['PORT']:
                port_str = settings_dict['PORT']
            
            self.introspection.uid = defschema_str
            self.owner = defschema_str
        else:
            options = settings_dict['DATABASE_OPTIONS']
                            
            if settings_dict['DATABASE_NAME']:
                db_str = settings_dict['DATABASE_NAME']
            if settings_dict['DATABASE_HOST']:
                host_str = settings_dict['DATABASE_HOST']
            else:
                host_str = 'localhost'
            if settings_dict['DATABASE_USER']:
                user_str = settings_dict['DATABASE_USER']
            if settings_dict['DATABASE_PASSWORD']:
                passwd_str = settings_dict['DATABASE_PASSWORD']
            if settings_dict['DATABASE_PORT']:
                port_str = settings_dict['DATABASE_PORT']
        if self.connection is None:
            new_conn = True
            if not db_str:
                from django.core.exceptions import ImproperlyConfigured
                raise ImproperlyConfigured('You need to specify NAME in your Django settings file.')

            
            connstr='%s;HOST=%s;DB=%s;UID=%s;PWD=%s;PORT=%s'%(typecnx_str,host_str,db_str,user_str,passwd_str,port_str)
            
            self.connection = Database.connect(connstr)
            connection_created.send(sender=self.__class__)

        #=======================================================================
        # Set default schema
        #=======================================================================
        cursor = self.connection.cursor()
        cursor.execute("SET SCHEMA '%s'"%defschema_str)
        self.connection.commit()
        if len(cursor.execute("SELECT * FROM SYSPROGRESS.SYSTABLEs WHERE OWNER = '%s' AND TBL = '%s'"%(defschema_str,dual_str)).fetchall()) == 0 :        
            cursor.execute('CREATE TABLE "%s"."%s" (SEQACCESS integer)'%(defschema_str,dual_str))
            self.connection.commit()
            cursor.execute('INSERT INTO "%s"."%s" VALUES (1)'%(defschema_str,dual_str))
            self.connection.commit()
        
        return CursorWrapper(cursor, self.driver_needs_utf8, defschema_str)


class CursorWrapper(object):
    """
    A wrapper around the pyodbc's cursor that takes in account a) some pyodbc
    DB-API 2.0 implementation and b) some common ODBC driver particularities.
    """
    def __init__(self, cursor, driver_needs_utf8,defschema_str):
        self.cursor = cursor
        self.driver_needs_utf8 = driver_needs_utf8
        self.last_sql = ''
        self.last_params = ()
        self.defaultSchema = defschema_str

    def format_sql(self, sql, n_params=None):
        if self.driver_needs_utf8 and isinstance(sql, unicode):            
            sql = sql.encode('utf-8')
        # pyodbc uses '?' instead of '%s' as parameter placeholder.
        if n_params is not None:
            sql = sql % tuple('?' * n_params)
        else:
            if '%s' in sql:
                sql = sql.replace('%s', '?')
            
        return sql

    def format_params(self, params):
        
        #import pdb; pdb.set_trace()
        
        fp = []
        
        for p in params:
            if isinstance(p, unicode):
                if self.driver_needs_utf8:                    
                    fp.append(p.encode('utf-8'))
                else:
                    fp.append(p)
            elif isinstance(p, str):
                if self.driver_needs_utf8:
                    # TODO: use system encoding when calling decode()?
                    fp.append(p.decode('utf-8').encode('utf-8'))
                else:
                    fp.append(p)
            elif isinstance(p, type(True)):
                if p:
                    fp.append(1)
                else:
                    fp.append(0)
            else:
                fp.append(p)
        return tuple(fp)

    def execute(self, sql, params=()):
        #import pdb; pdb.set_trace()                
        self.last_sql = sql        
        sql = self.format_sql(sql, len(params))
        params = self.format_params(params)
        self.last_params = params
        
        #=======================================================================
        # OpenEdge no ; at the end
        #=======================================================================
        if sql.endswith(';') is True:
            sql=sql[:-1]
        
        
        sqlUniqueIndex=None
        idSequence=None
        sql=sql.replace('\n','')
        
        if re.search('CREATE TABLE ',sql) is not None or re.search('ALTER TABLE ',sql) is not None:
            
            if re.search('CREATE TABLE ',sql) is not None :
                Statement='CREATE TABLE "'
            elif re.search('ALTER TABLE ',sql) is not None :
                Statement='ALTER TABLE "'
            
            motif='%s(?P<TName>\w+)"'%Statement    
            tn=re.search(motif, sql)
            if tn is not None:
                OETblName=tn.group('TName')[:MAX_TABLE_NAME]
            
            motif='%s\w+"'%Statement
            sql=re.sub(motif,'', sql)
            if Statement == 'CREATE TABLE "':
                uniqueKw=re.search('(?P<uniqueClause>UNIQUE *\(.*\))', sql)
                if uniqueKw is not None:
                    sql=re.sub('(?P<uniqueClause>, *UNIQUE *\(".*"\))','', sql)
                    fidx=re.search('("\w+"[, ]*)+',uniqueKw.group('uniqueClause'))
                    if fidx is not None:
                        idxnum=1
                        FieldIdx=fidx.group().split(',')
                        sqlUniqueIndex='CREATE UNIQUE INDEX %s_%s ON "%s" ('%(OETblName[:MAX_TABLE_NAME-2],str(idxnum),OETblName)
                        for fieldName in FieldIdx:
                            sqlUniqueIndex+='%s ,'%fieldName
                        
                        sqlUniqueIndex='%s)'%sqlUniqueIndex[:-1]
                
                idSequence='CREATE SEQUENCE PUB.SEQ_ID_%s START WITH 0, INCREMENT BY 1, MINVALUE 0, NOCYCLE'%OETblName[:MAX_TABLE_NAME-7]
            
                
            sql='%s%s" %s'%(Statement,OETblName,sql)                
        
        
        #=======================================================================
        # Reduce index name to 32 Char.
        #=======================================================================
        if re.search('CREATE INDEX ',sql) is not None:
            indexName=re.search('CREATE INDEX "(?P<indexname>\w+)"',sql).group('indexname')
            tableName=re.search('CREATE INDEX "\w+" ON "(?P<tablename>\w+)" ',sql).group('tablename')
            
            if len(indexName) > MAX_INDEX_NAME:
                indexName=indexName[(len(indexName)-MAX_INDEX_NAME)-1:-1]
            
            if len(tableName) > MAX_TABLE_NAME:
                tableName=tableName[:MAX_TABLE_NAME]
                
            beginsql=re.sub('CREATE INDEX "\w+"','',sql)
            trailsql=re.sub('ON "\w+"','',beginsql)
            sql='CREATE INDEX "%s" ON "%s" %s'%(indexName,tableName,trailsql)
         
        #=======================================================================
        # Change LIMIT to TOP used by opendedge
        #=======================================================================
        hasLimit=re.search('LIMIT (\d+)',sql)
        if hasLimit is not None :
            sql=re.sub('LIMIT \d+',' ',sql)
            sql=re.sub('SELECT','SELECT TOP %s'%hasLimit.group(1),sql)
               
        #import pdb; pdb.set_trace()
        #print 'OpenEdge Base %s values : %s' % (sql,params)
        
        
        rcode=self.cursor.execute(sql,params)
                
        if idSequence is not None:
            self.cursor.execute(idSequence)
        if sqlUniqueIndex is not None:
            self.cursor.execute(sqlUniqueIndex)
        self.connection.commit()
        #import pdb; pdb.set_trace()
        return rcode
    
    def executemany(self, sql, params_list):
        sql = self.format_sql(sql)
        # pyodbc's cursor.executemany() doesn't support an empty param_list
        if not params_list:
            if '?' in sql:
                return
        else:
            raw_pll = params_list
            params_list = [self.format_params(p) for p in raw_pll]
                    
        return self.cursor.executemany(sql, params_list)

    def format_results(self, rows):
        """
        Decode data coming from the database if needed and convert rows to tuples
        (pyodbc Rows are not sliceable).
        """
        #import pdb; pdb.set_trace()
        
        if not self.driver_needs_utf8:
            return tuple(rows)
        
        fr = []
        for row in rows:
            if isinstance(row, str):
                fr.append(row.decode('utf-8'))
            else:
                fr.append(row)
        return tuple(fr)

    def fetchone(self):
        row = self.cursor.fetchone()
        if row is not None:
            return self.format_results(row)
        return []

    def fetchmany(self, chunk):
        return [self.format_results(row) for row in self.cursor.fetchmany(chunk)]

    def fetchall(self):
        return [self.format_results(row) for row in self.cursor.fetchall()]

    def __getattr__(self, attr):
        if attr in self.__dict__:
            return self.__dict__[attr]
        return getattr(self.cursor, attr)
    
    def __iter__(self):
        return iter(self.cursor)