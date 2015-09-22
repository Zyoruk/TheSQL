import json
from os.path import abspath, dirname, join, isfile
import os.path
from os import listdir
from Logs import Logs

EVM_LIST = abspath(dirname('../evm/'))
VARFILE = EVM_LIST + '/' + 'VARIABLES.json'

class DataCatalog(object):
    
    def __init__(self):
        self.db = 0
        self.sysCat = 0
        self.evm = 0
        self.metaPath = 0
                
    """ ***************** Setup EVM ***************** """
    def getEVM(self):
        tmp = self.db
        try:
            with open(VARFILE, 'r') as self.sysCat:
                self.db = json.load(self.sysCat)
        except IOError:
            self.db = {'db':0}
            with open(VARFILE , 'w') as TMP:
                json.dump(self.db,TMP)
        else:
            self.sysCat.close()
            
        self.evm = self.db["db"] 
        self.db = tmp
        
        if self.evm != 0:
            self.metaPath = EVM_LIST + '/' + str(self.evm) + '/metadata/'
        else:
            log = 'Error 11: EVM not set up'
            self.sendError(log)
    
    def openSysCat(self,table):
        self.getEVM()
        path = self.metaPath + table + '.json'
        try:
            with open(path, 'r') as self.sysCat:
                self.db = json.load(self.sysCat)
        except IOError:
            log = 'Error 1: no' + str(table) + 'table on DB'
            self.sendError(log)
        else:
            self.sysCat.close()               
            
    """ ********************************************* """
    
    """ ***************** READ ***************** """
    
    '''Return array of index names'''        
    def hasIndex(self, table):  
        self.openSysCat(table)
        if self.db == 0:
            return 0
        cat = self.db['index']
        self.sysCat.close()       
        return cat
    
    '''Return False or an array of {'table':ref_table , 'Column':column}'''
    def getFK(self, table):  
        self.openSysCat(table)
        if self.db == 0:
            return
        cat = self.db['FK']
        self.sysCat.close()       
        return cat
    
    ''' Return the type of a column '''        
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
    
    ''' Return all the constrains of a table '''
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
    
    ''' Return the constrains of a column '''
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
    
    '''Return all the types of the columns in a table'''
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
    
    '''Return the name of the Pks table'''
    def getsPK(self, table):
        self.openSysCat(table)
        if self.db == 0:
            return
        cat = self.db["PK"]
        self.sysCat.close()
        return cat
    
    ''' Return all the names of the columns of a table '''
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
    
    ''' Return all the tables of a DB '''
    def getTabNames(self): 
        self.getEVM()
        tables = []
        tabs = []
        if self.evm != 0:
            onlyfiles = [ f for f in listdir(self.metaPath) if isfile(join(self.metaPath,f)) ]
            for jason in onlyfiles:         
                if jason.endswith('.json'):
                    tables.append(jason)            
            for tab in tables:
                tabs.append(tab.split(".",1)[0])
        else:
            log = 'Error 11: EVM not set up'
            self.sendError(log)
        return tabs
    
    ''' Return all the names of index of a table '''
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
                
    """ ***************** WRITE ***************** """
    
    ''' Creates new table '''            
    def setNewTable(self, table_name, columns_names, column_type, column_nullability, PK):
        self.getEVM()
        tName = table_name + '.json'
        log = 'Table created successfully.'
        state = False
        
        if PK in columns_names:
                                        
            if len(columns_names) != len(column_type) or len(columns_names) != len(column_nullability):
                log = 'Error 21: Not enough names for types'
                self.sendError(log)
                return log
            else:
                cols = []                           
                while len(columns_names) != 0:                
                    cols.append({'name':columns_names.pop(-1),'type':column_type.pop(-1),
                                 'null':column_nullability.pop(-1), 'index':False})
                
                table = {'PK':PK,
                         'FK':False, 
                         'index':False,
                         'columns':cols}
                                    
                if os.path.isfile(tName):
                    log = 'Error 1: File not found'
                    self.sendError(log)
                    return log
                else:
                    if self.evm != 0:
                        with open(str(self.metaPath) + '/' + str(tName), 'w') as sysCat:
                            json.dump(table,sysCat)
                        state = True
                
        else:
            log = 'Error 22: Check for PK in columns'
            self.sendError(log)
        
        return state
    
    """
    ''' Drops table '''            
    def dropTable(self, table_name):
        self.openSysCat(table_name)               
        self.db["enabled"] = False
        
        if self.evm != 0:
            with open(table, 'w') as sysCat:
                json.dump(self.db,sysCat)
        
        log = 'Table ' + table_name +  ' dropped successfully.'
        return log
    """
    
    ''' Create Index '''
    def createIndex(self, indexName, table_name, column):
        self.getEVM()
        table = table_name + '.json'                
        self.openSysCat(table_name)
        
        if self.db["index"] == False:
            self.db["index"] = [{"index":indexName}]
        else:
            self.db["index"].append({"index":indexName})
        print self.metaPath
        with open(self.metaPath + '/' + table, 'w') as sysCat:
            json.dump(self.db,sysCat)
        
        log = 'Index updated successfully.'
        return log
    
    ''' Set new FK '''
    def setFK(self, table_name, refTable, column):
        self.getEVM()
        self.openSysCat(table_name)
        table = table_name + '.json'
        log = 'Error 11: EVM not set up'
        
        #TODO: Types are not validated since is out of requirements.
        if column in self.getColNames(refTable):
            if self.evm != 0:                
                if self.db["FK"] == False:
                    self.db["FK"] = [{'Table':refTable,'Column':column}]
                    print(self.db)
                else:
                    arr = self.db["FK"]
                    arr.append({'table':refTable,'column':column})
                    self.db["FK"] = arr
                
                log = 'FK successfully updated'
                print self.metaPath
                            
                with open(self.metaPath + '/' + table, 'w') as self.sysCat:
                    json.dump(self.db,self.sysCat)
            else:
                self.sendError(log)
            
        return log
    
    def sendError(self, log):
        errorModule = Logs()
        errorModule.Error(log)