import json
from os.path import abspath, dirname, join, isfile
import os.path
from os import listdir

WHERE_AM_I = abspath(dirname(__file__))

class DataCatalog(object):
    
    def __init__(self):
        self.db = 0
        self.sysCat = 0
                
    """READ"""
    
    def openSysCat(self,table):
        with open(table + '.json', 'r') as self.sysCat:
            self.db = json.load(self.sysCat)
            
    def hasIndex(self, table):  
        self.openSysCat(table)
        cat = self.db['index']
        self.sysCat.close()       
        return cat
            
    def getType(self, table, column):
        self.openSysCat(table)
        num = 0                
        for columnsParam in self.db["columns"]:
            if columnsParam["name"] != column:
                num += 1
            else:
                break   
        cat = self.db["columns"][num]["type"]
        self.sysCat.close()
        return cat
            
    def getsPK(self, table):
        self.openSysCat(table)
        cat = self.db["PK"]
        self.sysCat.close()
        return cat
    
    def getColNames(self, table): 
        self.openSysCat(table)
        cat = []
        num = 0
        for columnsParam in self.db["columns"]:
            cat.append(columnsParam["name"])
            num += 1
        self.sysCat.close()
        return cat
    
    def getTabNames(self): #TODO check all json files in dir
        onlyfiles = [ f for f in listdir(WHERE_AM_I) if isfile(join(WHERE_AM_I,f)) ]
        tables = []
        for jason in onlyfiles:            
            if jason.endswith('.json'):
                tables.append(jason)
        
        tabs = []
        for tab in tables:
            tabs.append(tab.split(".",1)[0])
        
        return tables
                 
    def getTypes(self, table):
        self.openSysCat(table)
        cat = []
        num = 0
        for columnsParam in self.db["columns"]:
            cat.append(columnsParam["type"])
            num += 1
        self.sysCat.close()
        return cat
    
    def getIndex(self, table, column):
        self.openSysCat(table)
        num = 0                
        for columnsParam in self.db["columns"]:
            if columnsParam["name"] != column:
                num += 1
            else:
                break   
        self.sysCat.close()
        return num
                
    """WRITE"""
    
    def setNewDB(self, directory):        
        with open(directory + 'sysCat.json', 'w') as outfile:
            json.dump({'Index':'false'}, outfile)
          
                
    def setNewTable(self, table_name, columns_names, column_type, FK, column_nullability, PK):
        
        tName = table_name + '.json'
        
        """Check for PK in columns"""
        if PK in columns_names:
            if not(FK in columns_names):
                FK = 'None'
                            
            """Not enough names for types"""
            if len(columns_names) != len(column_type) or len(columns_names) != len(column_nullability):
                #send error
                return
            
            else:
                cols = []                           
                while len(columns_names) != 0:                
                    cols.append({'name':columns_names.pop(-1),'type':column_type.pop(-1),
                                 'null':column_nullability.pop(-1)})
                
                table = {'PK':PK,
                     'FK':FK, 
                     'index':False,
                     'enabled':True,
                     'columns':cols}
                                    
                if os.path.isfile(tName):
                    #TO DO: Return error
                    print('error')
                else:
                    with open(tName, 'w') as sysCat:
                        json.dump(table,sysCat)
        return
            
    def dropTable(self, table_name):
        table = table_name + '.json'
        with open(table , 'r') as sysCat:
            db = json.load(sysCat)
        sysCat.close()
                
        db["enabled"] = False
        
        with open(table, 'w') as sysCat:
            json.dump(db,sysCat)
        
        return