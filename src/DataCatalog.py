import json
from os.path import abspath, dirname, join, isfile
import os.path
from os import listdir
from Logs import Logs
from GetEVM import GetEVM

EVM_LIST = abspath(dirname('../evm/'))
VARFILE = EVM_LIST + '/' + 'VARIABLES.json'

class DataCatalog(object):
    
    def __init__(self):
        self.db = 0         #Data from table
        self.sysCat = 0     #RAM file
        self.evm = 0        #Name of the DB
        self.metaPath = 0   #Path to metadata
        self.meta = []      #List of DB Tables
        
    
    '''--------------------------------------------------------------'''
                
    """ ***************** Setup EVM ***************** """
    '''To use on set database'''
    def setEVM(self):
        evm = GetEVM()
        self.evm = evm.getEVM()
        self.metaPath = EVM_LIST + '/' + str(self.evm) + '/metadata/'
    
    '''Load table info in RAM. Get table from list'''
    def getTableMetadata(self,table):
        for tb in self.meta:
            if tb["name"] == table:
                return tb
        return 0        
    
    '''Get all the tables into a variable. To use on start.'''
    def loadMetadatas(self):
        onlyfiles = [ f for f in listdir(str(self.metaPath)) if isfile(join(str(self.metaPath),f)) ]
        for jason in onlyfiles:         
                if jason.endswith('.json'):
                    tablePath = self.metaPath + jason
                    #print tablePath
                    with open(str(tablePath), 'r') as self.sysCat:
                        tableData = json.load(self.sysCat)
                    self.meta.append(tableData)
                    self.sysCat.close()
    
    '''Dumps all the data into files'''    
    def saveMetadatas(self):
        for tb in self.meta:
            name = tb["name"]
            with open(self.metaPath + '/' + str(name) + '.json', 'w') as sysCat:
                    json.dump(tb, sysCat)
    
    '''Dumps and reset. To use with stop'''
    def changeDB(self):
        self.saveMetadatas()
        self.db = 0         
        self.sysCat = 0     
        self.evm = 0        
        self.metaPath = 0   
        self.meta = []
        
            
    """ ********************************************* """
    
    '''--------------------------------------------------------------'''
    
    """ ******************** READ ******************* """
    
    '''Return array of index names'''        
    def hasIndex(self, table):
        log = 'Error 11: EVM is not set'
        if self.evm == 0:
            self.sendError(log)
            return 0
        else:
            self.db = self.getTableMetadata(table)
            if self.db != 0:
                cat = self.db['index']
                return cat
            else:
                log = 'Error 1: No table in DB'
                self.sendError(log)
                return 0
                
    
    '''Return False or an array of {'table':ref_table , 'Column':column}'''
    def getFK(self, table):
        if self.evm == 0:
            return 0
        self.db = self.getTableMetadata(table)
        cat = self.db['FK']
#        self.sysCat.close()       
        return cat
    
    def getFKChilds(self, table):
        ls = []
        for meta in self.meta:
            
            if meta["FK"] != False:
                for child in meta["FK"]:
                    if child["reftable"] == table:
                        ls.append([meta["name"],child["refcolumn"]])                

        return ls
        
    
    ''' Return the type of a column '''        
    def getType(self, table, column):
        cat = 0
        if self.evm != 0:
            
            boo = False
            num = 0
            
            for meta in self.meta:
                if meta["name"] == table:
                    boo = True                            
                    for columnsParam in meta["columns"]:
                        if columnsParam["name"] != column:
                            num += 1
                        else:
                            cat = meta["columns"][num]["type"]
                            break
            if  boo != True:
                log = 'Error 1: No table in DB'
                self.sendError(log)
                
        return cat
    
    '''Return all the types of the columns in a table'''
    def getTypes(self, table):
        ls = []        
        
        if self.evm != 0:            
            for tb in self.meta:
                if tb["name"] == table:
                    for columns in tb["columns"]:
                        ls.append(columns["type"])
        
        return ls
    
    ''' Return the constrains of a column '''
    def getNull(self, table, column):
        cat = 0
        if self.evm != 0:
            
            boo = False
            num = 0
            
            for meta in self.meta:
                if meta["name"] == table:
                    boo = True                          
                    for columnsParam in meta["columns"]:
                        if columnsParam["name"] != column:
                            num += 1
                        else:
                            cat = meta["columns"][num]["null"]
                            break
                    
            if boo != True:
                log = 'Error 1: No table in DB'
                self.sendError(log)

        return cat    
    
    ''' Return all the constrains of a table '''
    def getNulls(self, table):
        ls = []        
        
        if self.evm != 0:            
            for tb in self.meta:
                if tb["name"] == table:
                    for columns in tb["columns"]:
                        ls.append(columns["null"])
        
        return ls
    
    '''Return the name of the Pks table'''
    def getsPK(self, table):
        cat = 0
        if self.evm != 0:            
            
            for meta in self.meta:
                if meta["name"] == table:                            
                    return meta["PK"]
                
        return cat
    
    ''' Return all the names of the columns of a table '''
    def getColNames(self, table): 
        ls = []        
        
        if self.evm != 0:            
            for tb in self.meta:
                if tb["name"] == table:
                    for columns in tb["columns"]:
                        ls.append(columns["name"])
        
        return ls
    
    ''' Return all the tables of a DB '''
    def getTabNames(self): 
        ls = []        
        
        if self.evm != 0:            
            for tb in self.meta:
                ls.append(tb["name"])
        
        return ls
    
    ''' Return all the names of index of a table '''
    def getIndex(self, table, column):
        num = 0
        if self.evm == 0:
            return num
                        
        for tb in self.meta:
            if tb["name"] == table:
                for col in tb["columns"]:
                    if col["name"] == column:
                        num += 1
                        break 
                    else:               
                        num += 1   
        
        return num
                
    """ ***************** WRITE ***************** """
    
    ''' Creates new table '''            
    def setNewTable(self, table_name, columns_names, column_type, column_nullability, PK):
        state = False
        if self.evm != 0:
            
            tName = table_name + '.json'
            if os.path.isfile(self.metaPath + '/' + tName):
                log = 'Error 1: DB exist'
                self.sendError(log)
                return log            
            
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
                             'name':table_name,
                             'FK':False, 
                             'index':False,
                             'columns':cols}
                    
                    self.meta.append(table)
                    
                    with open(self.metaPath + '/' + str(table_name) + '.json', 'w') as sysCat:
                        json.dump(table, sysCat)
                        
                    state = True
                    
                    
                    
            else:
                log = 'Error 22: Check for PK in columns'
                self.sendError(log)
        else:
            log = 'Error 11: EVM not set'
            self.sendError(log)
        
        return state
    
    ''' Create Index '''
    def createIndex(self, indexName, table_name, column):
        boo = False
        log = 1
              
        for tb in self.meta:
            if tb["name"] == table_name:
                boo = True             
                if tb["index"] == False:
                    tb["index"] = [{"name":indexName}]
                else:
                    tb["index"].append({"name":indexName})
                
        if boo == False:
            log = 0 
            #'Error 11: No table in DB'
                      
        return log
    
    ''' Set new FK '''
    def setFK(self, table_name, column, refTable, refColumn):
        log = 'Error 11: EVM not set up'
        boo = False
        
        if self.evm != 0:
            
            if self.getType(table_name, column) == self.getType(refTable, refColumn):
                if refColumn in self.getColNames(refTable):
                    q = {'column':column,'reftable':refTable,'refcolumn':refColumn}
                    if q not in self.getFK(table_name):                
                        for tb in self.meta:
                            if tb["name"] == table_name:
                                boo = True
                                if tb["FK"] == False:
                                    tb["FK"] = [q]
                                else:
                                    tb["FK"].append(q)
                                
                                log = 'FK successfully updated'
                                return log
                        
                        if boo == False:
                            log = 0
                    else:
                        log =  "Error 11: FK already exist."
                else:
                    log = "Error 11: No column in table."
                    self.sendError(log)
            else:
                log = "Error 11: Columns not the same type"
                self.sendError(log)
        else:
            self.sendError(log)
        
        return 0
    
    def sendError(self, log):
        errorModule = Logs()
        errorModule.Error(log)
        