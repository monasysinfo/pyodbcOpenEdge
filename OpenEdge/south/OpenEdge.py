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
        
        #=======================================================================
        # self.execute("ALTER TABLE %s ADD CONSTRAINT %s UNIQUE (%s)" % (
        #     self.quote_name(table_name),
        #     self.quote_name(name),
        #     cols,
        # ))
        #=======================================================================
        #import pdb; pdb.set_trace()        
        return name