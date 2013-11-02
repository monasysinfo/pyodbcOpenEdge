from django.db.backends import BaseDatabaseIntrospection
import pyodbc as Database
import decimal,datetime

SQL_AUTOFIELD = -777555

class DatabaseIntrospection(BaseDatabaseIntrospection):
    # Map type codes to Django Field types.
    
    data_types_reverse = {
        int:                'IntegerField',
        bool:               'BooleanField',
        str:                'CharField',
        decimal.Decimal :   'DecimalField',
        long:               'BigIntegerField',
        datetime.date:      'DateField',
        datetime.datetime : 'DateTimeField',
    }
    
    def table_name_converter(self, name):
        """Apply a conversion to the name for the purposes of comparison.

        The default table name converter is for case sensitive comparison.
        """
        return name[:32]
    
    def installed_models(self, tables):
        "Returns a set of all models represented by the provided list of table names."
        from django.db import models, router
        all_models = []
        for app in models.get_apps():
            for model in models.get_models(app):
                if router.allow_syncdb(self.connection.alias, model):
                    all_models.append(model)
        tables = list(map(self.table_name_converter, tables))
        
        return set([
            m for m in all_models
            if self.table_name_converter(m._meta.db_table)[:32] in tables
        ])
            
    def get_table_list(self, cursor):
        """
        Returns a list of table names in the current database.
        """
                        
        cursor.execute("SELECT TBL FROM SYSPROGRESS.SYSTABLES WHERE OWNER = '%s'"%self.uid)
        return [row[0] for row in cursor.fetchall()]


    
    def get_relations(self, cursor, table_name):
        """
        Returns a dictionary of {field_index: (field_index_other_table, other_table)}
        representing all relationships to the given table. Indexes are 0-based.
        """
        relations={}
        
        cursor.execute("SELECT CNSTRNAME,REFTBLNAME FROM SYSPROGRESS.SYS_REF_CONSTRS WHERE OWNER = '%s' AND TBLNAME = '%s' "%(self.uid,table_name))
        cnstrList=cursor.fetchall()
        if len(cnstrList)>0:
            for cnstr in cnstrList:
                cursor.execute("SELECT IDXNAME , TBLNAME , CNSTRTYPE FROM SYSPROGRESS.SYS_TBL_CONSTRS WHERE CNSTRNAME = '%s' "%cnstr[0])
                cnstrFull=cursor.fetchall()
                for cnstrCur in cnstrFull:
                    if cnstrCur[2] == 'F':
                        cnstrKey=cnstrCur[0]   
                    elif cnstr[2] == 'P':
                        cnstrPrimary=(cnstrCur[0],cnstrCur[1])
                relations[cnstrKey]=cnstrPrimary
        
        return relations

    def get_indexes(self, cursor, table_name):
        """
        Returns a dictionary of fieldname -> infodict for the given table,
        where each infodict is in the format:
            {'primary_key': boolean representing whether it's the primary key,
             'unique': boolean representing whether it's a unique index,
             'db_index': boolean representing whether it's a non-unique index}
        """
        
        indexes = {}
        
        cursor.execute("SELECT IDXNAME,IDXTYPE FROM SYSPROGRESS.SYSINDEXES WHERE IDXOWNER = '%s' AND TBL = '%s'"%(self.uid,table_name))
        indexFull=cursor.fetchall()
        for index in indexFull:
            if indexes.has_key(index[0]) is False:
                indexes[index[0]] = {'primary_key': False, 'unique': False}
                if index[1]=='U':
                    indexes[index[0]]['unique']=True
                    
                indexes[index[0]]['primary_key']=self._is_primary(table_name,index[0],cursor)    
        
        return indexes
        

    def _is_primary(self,table,indexname,cursor):
        cursor.execute("SELECT CNSTRNAME FROM SYSPROGRESS.SYS_TBL_CONSTRS WHERE OWNER = '%s' AND TBLNAME = '%s' AND IDXNAME = '%s' AND CNSTRTYPE = 'P'"%(self.uid,table,indexname))
        primary=cursor.fetchall()        
        if len(primary) > 0:
            return True
        else:
            return False


    #===========================================================================
    # def _is_auto_field(self, cursor, table_name, column_name):
    #     """
    #     Checks whether column is Identity
    #     """
    #     
    #     cursor.execute("SELECT COLUMNPROPERTY(OBJECT_ID(%s), %s, 'IsIdentity')",
    #                      (self.connection.ops.quote_name(table_name), column_name))
    #     return cursor.fetchall()[0][0]
    #===========================================================================

    def _test_null(self,data):
        if data == 'Y':
            return True
        else:
            return False
        
    def _table_info(self, cursor, name):
        cursor.execute("SELECT COL,COLTYPE,WIDTH,NULLFLAG FROM SYSPROGRESS.SYSCOLUMNS WHERE OWNER = '%s' AND TBL = '%s'"%(self.uid,name))
        
        return [{'name': field[0],
                 'type': field[1],
                 'size': field[3],
                 'null_ok': self._test_null(field[4])
                 } for field in cursor.fetchall()]
    
    def get_table_description(self,cursor,name):
        "Returns a description of the table, with the DB-API cursor.description interface."
        cursor.execute("SELECT TOP 1 * FROM %s " % self.connection.ops.quote_name(name))
        description = []
        for desc in cursor.description:
            description.append((desc[0].lower(),) + desc[1:])
        #print description
        return description

    def get_field_type(self, data_type, description):
        # If it's a NUMBER with scale == 0, consider it an IntegerField
        # print data_type,'----',description
        return super(DatabaseIntrospection, self).get_field_type(
                data_type, description)
       