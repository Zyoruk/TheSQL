import json
from os.path import abspath, dirname, join, isfile
import os.path
from os import listdir

EVM_LIST = abspath(dirname('../evm/'))
VARFILE = EVM_LIST + '/' + 'VARIABLES.json'

class DataCatalog(object):
    
    def __init__(self):
        self.db = 0
        self.sysCat = 0
        self.evm = 0
        self.metaPath = 0
                
    """READ"""
    def getEVM(self):
        tmp = self.db
        try:
            with open(VARFILE, 'r') as self.sysCat:
                self.db = json.load(self.sysCat)
        except IOError:
            self.db = {'db':0}
            with open(VARFILE , 'w') as TMP:
                json.dump(self.db,TMP)
        
        self.evm = self.db["db"] 
        self.db = tmp
        if self.evm != 0:
            self.metaPath = EVM_LIST + '/' + str(self.evm) + '/metadata/'
        else:
            print("Error: EVM not set up")
        self.sysCat.close()
    
    def openSysCat(self,table):
        self.getEVM()
        path = self.metaPath + table + '.json'
        try:
            with open(path, 'r') as self.sysCat:
                self.db = json.load(self.sysCat)
        except IOError:
            print('Error: no table on DB')        
        else:
            return                
            
    def hasIndex(self, table):  
        self.openSysCat(table)
        if self.db == 0:
            return
        cat = self.db['index']
        self.sysCat.close()       
        return cat
    
    def getFK(self, table):  
        self.openSysCat(table)
        if self.db == 0:
            return
        cat = self.db['FK']
        self.sysCat.close()       
        return cat
            
    def getType(self, table, column):
        self.openSysCat(table)
        if self.db == 0:
            return
        num = 0                
        for columnsParam in self.db["columns"]:
            if columnsParam["name"] != column:
                num += 1
            else:
                break   
        cat = self.db["columns"][num]["type"]
        self.sysCat.close()
        return cat
    
    def getNulls(self, table):
        self.openSysCat(table)
        if self.db == 0:
            return
        cat = []
        num = 0
        for columnsParam in self.db["columns"]:
            cat.append(columnsParam["null"])
            num += 1
        self.sysCat.close()
        return cat
    
    def getNull(self, table, column):
        self.openSysCat(table)
        if self.db == 0:
            return
        num = 0                
        for columnsParam in self.db["columns"]:
            if columnsParam["name"] != column:
                num += 1
            else:
                break   
        cat = self.db["columns"][num]["null"]
        self.sysCat.close()
        return cat
    
    def getTypes(self, table):
        self.openSysCat(table)
        if self.db == 0:
            return
        cat = []
        num = 0
        for columnsParam in self.db["columns"]:
            cat.append(columnsParam["type"])
            num += 1
        self.sysCat.close()
        return cat
    
    def getsPK(self, table):
        self.openSysCat(table)
        if self.db == 0:
            return
        cat = self.db["PK"]
        self.sysCat.close()
        return cat
    
    def getColNames(self, table): 
        self.openSysCat(table)
        if self.db == 0:
            return
        cat = []
        num = 0
        for columnsParam in self.db["columns"]:
            cat.append(columnsParam["name"])
            num += 1
        self.sysCat.close()
        return cat
    
    def getTabNames(self): #TODO check all json files in dir
        self.getEVM()
        onlyfiles = [ f for f in listdir(self.metaPath) if isfile(join(self.metaPath,f)) ]
        tables = []
        for jason in onlyfiles:
            if jason.endswith('.json'):
                tables.append(jason.split('.')[0])
        
        tabs = []
        for tab in tables:
            tabs.append(tab.split(".",1)[0])
        
        return tables

    def getIndex(self, table, column):
        self.openSysCat(table)
        if self.db == 0:
            return
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
        with open(str(directory) + 'sysCat.json', 'w') as outfile:
            json.dump({'Index':'false'}, outfile)
          
                
    def setNewTable(self, table_name, columns_names, column_type, column_nullability, PK):
        self.getEVM()
        tName = table_name + '.json'
        
        """Check for PK in columns"""
        if PK in columns_names:
                            
            """Not enough names for types"""
            if len(columns_names) != len(column_type) or len(columns_names) != len(column_nullability):
                #send error
                return            
            else:
                cols = []                           
                while len(columns_names) != 0:                
                    cols.append({'name':columns_names.pop(-1),'type':column_type.pop(-1),
                                 'null':column_nullability.pop(-1), 'index':False})
                
                table = {'PK':PK,
                         'FK':False, 
                         'index':False,
                         'enabled':True,
                         'columns':cols}
                                    
                if os.path.isfile(tName):
                    #TO DO: Return error
                    print('error')
                else:
                    if self.evm != 0:
                        with open(str(self.metaPath) + '/' + str(tName), 'w') as sysCat:
                            json.dump(table,sysCat)
        return
            
    def dropTable(self, table_name):
        self.getEVM()
        table = table_name + '.json'
        self.openSysCat(table_name)
        self.sysCat.close()               
        self.db["enabled"] = False
        
        if self.evm != 0:
            with open(table, 'w') as sysCat:
                json.dump(self.db,sysCat)
        
        return
    
    def createIndex(self,  indexName, table_name, column):
        self.getEVM()    
        table = str(table_name) + '.json'                
        self.openSysCat(table)
        if self.db["index"] == False:
            self.db["index"] = [{"index":indexName}]
        else:
            self.db["index"].append({"index":indexName})
        self.sysCat.close()
        
        with open(table, 'w') as sysCat:
            json.dump(self.db,sysCat)
        
        return
    
    def setFK(self, table_name, refTable, column):
        self.getEVM()
        table = str(table_name) + '.json'
        self.openSysCat(table_name)
        self.sysCat.cslose()
        
        if self.db["FK"] == False:
            self.db["FK"] = column
        else:
            print('Error, table has already a FK')
            return
        if self.evm != 0:
            with open(table, 'w') as self.sysCat:
                json.dump(self.db,self.sysCat)
        
        return