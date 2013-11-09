# -*- coding: utf-8 -*-
'''
Created on 9 nov. 2013

@author: jyp

Custom fields for OpenEdge extent compatibility

This module allows the type conversion of fields with extents (list equiv) in a OpenEdge Db.
These fields are returned as a character string where each extent is separated with a semicolon.

In the case of a character type field, the ';' char is escaped with the ~ char and, the ~ char is escape with a ~ char.
So, if original value in the field is : 'ABC~D;E~;, the returned value is : ABC~~D~;E~~~;.

This module is designed to handle this case and to renders the original value.

Utilization in django models :
    
    from OpenEdge.OEmodels.OpenEdgeExtentField import OpenEdgeExtentField
    
    class Mytable(models.Model):
        nom = models.CharField(max_length=60, blank=True,primary_key=True)
        fieldext = OpenEdgeExtentField(max_length=8, blank=True,OEextents=5,OEtype='char',verbose_name='fieldext')
        intext = OpenEdgeExtentField(max_length=8, blank=True,OEextents=7,OEtype='int')
        datext = OpenEdgeExtentField(max_length=8, blank=True,OEextents=4,OEtype='date')
        datetimext = OpenEdgeExtentField(max_length=40, blank=True,OEextents=4,OEtype='datetime')
        logext = OpenEdgeExtentField(max_length=32, blank=True,OEextents=4,OEtype='log')
        decext = OpenEdgeExtentField(max_length=88, blank=True,OEextents=4,OEtype='dec')
        int64ext = OpenEdgeExtentField(max_length=88, blank=True,OEextents=4,OEtype='int64')
    
    OEextents is the extent count in the original OpenEdge definition
    OEtype is the date type in the original OpenEdge definition. 
        Supported types are : 
            'char'
            'int'
            'log'
            'dec'                 
            'int64'            
            'datetime'                
            'date'
    
        Untested types :        
            'blob'
            'raw'
            'clob'
        
        Uncompatible types :            
            'datetime-tz'  
    
    
Unknown value are converted to these default values :
date et datetime : 0/0/0001
int,int64 and dec : 0
log : False
char : No default value, unknow value returns a question mark (?).
    
    CAREFUL : You have to be aware that unknown values in characters type 
    fields may be confused with the real question mark character
     
'''
from django.db import models
from django.utils.encoding import smart_str
from django.core import exceptions

#===============================================================================
# OeEdge Extents support import
#===============================================================================
import re,decimal,datetime

class OpenEdgeExtentField(models.Field):
    
    description = "OpenEdge Extent object"
    
    #===========================================================================
    # oetypedict :
    # Dict for OpenEdge dataType conversion
    # The key is the OE-4GL dataType, data is a tab with 3 cols:
    #     1 - Conversion python dataType
    #     2 - Sql corresponding dataType
    #     3 - max Lenght of the corresponding type, 0 means no max length
    #===========================================================================
         
    
    oetypedict = {'char':[str,'varchar',0],
                  'int':[int,'int',0],
                  'log':[bool,'int',0],
                  'dec':[decimal.Decimal,'decimal',0],
                  'blob':[buffer,'varchar',0],
                  'clob':[str,'varchar',4194304],
                  'int64':[long,'bigint',0],
                  'raw':[buffer,'varchar',0],
                  'datetime':[datetime.datetime,'timestamp',0],
                  #=============================================================
                  # 20131109 Non-readable type from sql, for future use 
                  #
                  # 'datetime-tz':[str,'varchar',32],  
                  #=============================================================
                  'date':[datetime.date,'datefield',0]}
        
    __metaclass__ = models.SubfieldBase

    def __init__(self,help_text=("A semi-colon separated list values"), *args,**kwargs):
        
        self.name = 'Oeextfield'
        if 'verbose_name' in kwargs:
            self.name = kwargs['verbose_name']
            
        self.through = None
        self.help_text = help_text
        self.blank = True
        self.editable = True
        self.creates_table = False
        self.db_column = None
        self.serialize = False
        self.null = True
        self.extents = 0
        
        self.extent_separator=';'
        self.extent_escape='~'
        
        self.oeseparator=re.compile('.*;.*')
        self.oeconvfield=str
        self.oetype = kwargs['OEtype']
        if 'OEtype' in kwargs:
            self.oeconvfield=self.oetypedict[self.oetype][0]
            kwargs.pop('OEtype')
            
        if 'OEextents' in kwargs:
            self.extents = kwargs['OEextents']
            kwargs.pop('OEextents')
            
        if self.oetypedict[self.oetype][2] > 0 :
            kwargs['max_length'] = self.oetypedict[self.oetype][2]
        
        super(OpenEdgeExtentField, self).__init__(*args, **kwargs)
        
    def db_type(self):
        return self.oetypedict[self.oetype][1]

    def to_python(self, value):
        if value is None or value == '':
            return []
        
        if isinstance(value,(str,unicode)):
            value=smart_str(value)
        
        if isinstance(value, str):
            if self.oetype == 'date' :
                try: 
                    return [ datetime.date(int(u[2]),int(u[0]),int(u[1])) for u in [ z.split('/') for z in re.split(';',value.replace('?','01/01/01')) ]]
                except (ValueError, TypeError):
                    raise exceptions.ValidationError("Invalid %s format conversion for %s in a OpenEdgeExtentField instance")%(self.oetype,self.name)
                
            elif self.oetype == 'datetime':
                try:
                    return [ datetime.date(int(u[2]),int(u[0]),int(u[1])) for u in [ z.split('/') for z in re.split(';',value.replace('?','01/01/01')) ]]
                except (ValueError, TypeError):
                    raise exceptions.ValidationError("Invalid %s format conversion for %s in a OpenEdgeExtentField instance")%(self.oetype,self.name)
            
            elif self.oetype == 'log':
                try:
                    return [ self.oeconvfield(int(y))
                            for y in re.split(';',value.replace('?','0'))
                            ]
                except (ValueError, TypeError):
                    raise exceptions.ValidationError("Invalid %s format conversion for %s in a OpenEdgeExtentField instance")%(self.oetype,self.name)
            
            elif self.oetype in ['int','int64','dec']:
                try:
                    return [ self.oeconvfield(y) 
                            for y in re.split(';',value.replace('?','0'))
                            ]
                except (ValueError, TypeError):
                    raise exceptions.ValidationError("Invalid %s format conversion for %s in a OpenEdgeExtentField instance")%(self.oetype,self.name)
                
            else:
                try:
                    return [ self.oeconvfield(re.sub('<SEMICOLON>',';',y)) for y in [re.sub('<TILDE>','~', x) for x in re.split(';',re.sub('~~','<TILDE>',re.sub('~;','<SEMICOLON>',value)))]]
                except (ValueError, TypeError):
                    raise exceptions.ValidationError("Invalid %s format conversion for %s in a OpenEdgeExtentField instance")%(self.oetype,self.name)
        
        elif isinstance(value,list):
            if len(value) > self.extents:
                raise exceptions.ValidationError("Invalid extents count for %s in a OpenEdgeExtentField instance"%self.name)
            return value
        
        
    def get_prep_value(self, value):
        if len(value) > self.extents:
            raise exceptions.ValidationError("Invalid extents count in %s for a OpenEdgeExtentField instance"%self.name)
        
        return ';'.join([ smart_str(x).replace('~','~~').replace(';','~;') for x in value ])       
        
