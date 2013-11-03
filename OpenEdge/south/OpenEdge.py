from __future__ import print_function

import os.path
import sys
import re
import warnings


from django.db import connection, models
from django.db.backends.util import truncate_name
from django.core.management.color import no_style
from django.db.models.fields import NOT_PROVIDED
from django.db.utils import DatabaseError
from south.utils.py3 import string_types, text_type
#from OpenEdge.pyodbc import operations

from south.db import generic

class DatabaseOperations(generic.DatabaseOperations):    
    """
    OpenEdge implementation of database operations.    
    """
    backend_name = 'OpenEdge'
    alter_string_set_type = 'ALTER COLUMN %(column)s TYPE %(type)s'
    alter_string_set_null = 'ALTER COLUMN %(column)s DROP NOT NULL'
    alter_string_drop_null = 'ALTER COLUMN %(column)s SET NOT NULL'
    delete_check_sql = 'ALTER TABLE %(table)s DROP CONSTRAINT %(constraint)s'
    add_column_string = 'ALTER TABLE %s ADD COLUMN %s;'
    delete_unique_sql = "ALTER TABLE %s DROP CONSTRAINT %s"
    delete_foreign_key_sql = 'ALTER TABLE %(table)s DROP CONSTRAINT %(constraint)s'
    create_table_sql = 'CREATE TABLE %(table)s (%(columns)s)'
    max_index_name_length = 32
    drop_index_string = 'DROP INDEX %(index_name)s'
    delete_column_string = 'ALTER TABLE %s DROP COLUMN %s CASCADE;'
    create_primary_key_string = "ALTER TABLE %(table)s ADD CONSTRAINT %(constraint)s PRIMARY KEY (%(columns)s)"
    delete_primary_key_sql = "ALTER TABLE %(table)s DROP CONSTRAINT %(constraint)s"
    add_check_constraint_fragment = "ADD CONSTRAINT %(constraint)s CHECK (%(check)s)"
    rename_table_sql = "ALTER TABLE %s RENAME TO %s;"
    default_schema_name = "public"
    
    # Features
    allows_combined_alters = True
    supports_foreign_keys = True
    has_check_constraints = True
    has_booleans = True
    raises_default_errors = True    
    
    def column_sql(self, table_name, field_name, field, tablespace='', with_name=True, field_prepared=False):
        """
        Creates the SQL snippet for a column. Used by add_column and add_table.
        """

        # If the field hasn't already been told its attribute name, do so.
        if not field_prepared:
            field.set_attributes_from_name(field_name)

        # hook for the field to do any resolution prior to it's attributes being queried
        if hasattr(field, 'south_init'):
            field.south_init()

        # Possible hook to fiddle with the fields (e.g. defaults & TEXT on MySQL)
        field = self._field_sanity(field)

        try:
            sql = field.db_type(connection=self._get_connection())
        except TypeError:
            sql = field.db_type()
        
        if sql:
            
            # Some callers, like the sqlite stuff, just want the extended type.
            if with_name:
                field_output = [self.quote_name(field.column), sql]
            else:
                field_output = [sql]
            
            field_output.append('%s' % (not field.null and 'NOT NULL ' or ''))
            if field.primary_key:
                field_output.append('PRIMARY KEY')
            elif field.unique:
                # Just use UNIQUE (no indexes any more, we have delete_unique)
                field_output.append('UNIQUE')

            tablespace = field.db_tablespace or tablespace
            if tablespace and getattr(self._get_connection().features, "supports_tablespaces", False) and field.unique:
                # We must specify the index tablespace inline, because we
                # won't be generating a CREATE INDEX statement for this field.
                field_output.append(self._get_connection().ops.tablespace_sql(tablespace, inline=True))
            
            sql = ' '.join(field_output)
            sqlparams = ()
            # if the field is "NOT NULL" and a default value is provided, create the column with it
            # this allows the addition of a NOT NULL field to a table with existing rows
            if not getattr(field, '_suppress_default', False):
                if field.has_default():
                    default = field.get_default()
                    # If the default is actually None, don't add a default term
                    if default is not None:
                        # If the default is a callable, then call it!
                        if callable(default):
                            default = default()
                            
                        default = field.get_db_prep_save(default, connection=self._get_connection())
                        default = self._default_value_workaround(default)
                        # Now do some very cheap quoting. TODO: Redesign return values to avoid this.
                        if isinstance(default, string_types):
                            default = "'%s'" % default.replace("'", "''")
                        # Escape any % signs in the output (bug #317)
                        if isinstance(default, string_types):
                            default = default.replace("%", "%%")
                        # Add it in
                        sql += " DEFAULT %s"
                        sqlparams = (default)
                elif (not field.null and field.blank) or (field.get_default() == ''):
                    if field.empty_strings_allowed and self._get_connection().features.interprets_empty_strings_as_nulls:
                        sql += " DEFAULT ''"
                    # Error here would be nice, but doesn't seem to play fair.
                    #else:
                    #    raise ValueError("Attempting to add a non null column that isn't character based without an explicit default value.")

            if field.rel and self.supports_foreign_keys:
                self.add_deferred_sql(
                    self.foreign_key_sql(
                        table_name,
                        field.column,
                        field.rel.to._meta.db_table,
                        field.rel.to._meta.get_field(field.rel.field_name).column
                    )
                )

        # Things like the contrib.gis module fields have this in 1.1 and below
        if hasattr(field, 'post_create_sql'):
            for stmt in field.post_create_sql(no_style(), table_name):
                self.add_deferred_sql(stmt)
        
        # In 1.2 and above, you have to ask the DatabaseCreation stuff for it.
        # This also creates normal indexes in 1.1.
        if hasattr(self._get_connection().creation, "sql_indexes_for_field"):
            # Make a fake model to pass in, with only db_table
            model = self.mock_model("FakeModelForGISCreation", table_name)
            for stmt in self._get_connection().creation.sql_indexes_for_field(model, field, no_style()):
                self.add_deferred_sql(stmt)
        
        if sql:
            return sql % sqlparams
        else:
            return None

    @generic.invalidate_table_constraints
    def create_unique(self, table_name, columns):        
        """
        Creates a UNIQUE index on the columns on the given table.
        """

        if not isinstance(columns, (list, tuple)):
            columns = [columns]

        name = self.create_index_name(table_name, columns, suffix="_uniq")
                
        cols = ", ".join(map(self.quote_name, columns))
         
        self.execute('CREATE UNIQUE INDEX %s ON "%s" (%s)'%(name,table_name,cols))
           
        return name

    def _createSequence(self,table,column):
        """
        Use django.db.backends.creation.BaseDatabaseCreation._digest
        to create index name in Django style. An evil hack :(
        """
        return self._get_connection().ops.autoinc_sql(table, column)
        
    
    @generic.invalidate_table_constraints
    def create_table(self, table_name, fields):
        """
        Creates the table 'table_name'. 'fields' is a tuple of fields,
        each repsented by a 2-part tuple of field name and a
        django.db.models.fields.Field object
        """

        if len(table_name) > 63:
            print("   ! WARNING: You have a table name longer than 63 characters; this will not fully work on PostgreSQL or MySQL.")

        # avoid default values in CREATE TABLE statements (#925)
        for field_name, field in fields:
            field._suppress_default = True

        columns = [
            self.column_sql(table_name, field_name, field)
            for field_name, field in fields
        ]

        self.execute(self.create_table_sql % {
            "table": self.quote_name(table_name),
            "columns": ', '.join([col for col in columns if col]),
        })
        
        self.execute(self._createSequence(table_name,'id')[0])
    
    @generic.invalidate_table_constraints
    def add_column(self, table_name, name, field, keep_default=True):
        """
        Adds the column 'name' to the table 'table_name'.
        Uses the 'field' paramater, a django.db.models.fields.Field instance,
        to generate the necessary sql

        @param table_name: The name of the table to add the column to
        @param name: The name of the column to add
        @param field: The field to use
        """
        sql = self.column_sql(table_name, name, field)        
              
        if sql:
            params = (
                self.quote_name(table_name),
                sql,
            )
            sql = self.add_column_string % params
            self.execute(sql)


    @generic.invalidate_table_constraints
    def delete_table(self, table_name, cascade=True):
        """
        Deletes the table 'table_name'.
        """
        params = (self.quote_name(table_name), )
        self.execute('DROP TABLE %s;' % params)
        # Drop associated sequence
        self.execute('DROP SEQUENCE PUB.%s_%s'%('ID',table_name[:self.max_index_name_length-3]))
        
    