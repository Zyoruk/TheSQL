from DataCatalog import DataCatalog
from SDManager import StoredDataManager
from os.path import abspath, dirname
from Logs import Logs
import os.path
import json

EVM_LIST = abspath(dirname('../evm/'))

class DDL(object):
    
    def __init__(self, DC, sdman):
        self.dato = DC
        self.sdman = sdman
        self.varfile = EVM_LIST + '/' + 'VARIABLES.json'
        self.evm = 0
        self.metaPath = 0
        self.infoPath = 0
        self.indexPath  = 0
        self.log = 'Error 11: EVM not set up.'
        
    def setDataBase(self, db):
        log = 'Database: ' + str(db) + ' exist already.'
        if os.path.isdir(EVM_LIST + '/' + db):
            evm = {'db':db}
            self.evm = str(db)
            self.metaPath = EVM_LIST + '/' + str(db) + '/metadata/'
            self.infoPath = EVM_LIST + '/' + str(db) + '/info/'
            self.indexPath = EVM_LIST + '/' + str(db) + '/index/'
            with open(self.varfile , 'w') as TMP:
                json.dump(evm,TMP)
            log = 'Environment ' + str(db) + ' set.'
        else:
            self.sendError(log)
        return log
            
    def createTable(self, table_name, columns_names, column_type, column_nullability, PK):
        log = 'Error 11: EVM not set'        
        if self.evm != 0:

            if os.path.isdir(self.metaPath):
                state = self.dato.setNewTable(table_name, columns_names, column_type, column_nullability, PK)
                if state:
                    with open(str(self.infoPath) + '/' + str(table_name) + '.json', 'w') as TMP2:
                        json.dump({},TMP2)   
                    log = 'Table ' + str(table_name) + ' created successfully.'
                else:
                    log = 'Error 4: Could not write metadata.'
                    self.sendError(log)
            else:
                log = 'Error 11: EVM has not been started'
                self.sendError(log)
        else:
            self.sendError(log)
        
        return log
    
    def dropTable(self, table_name):
        log = 'Error 11: EVM not set.'
        
        if self.evm != 0:
            info = self.infoPath + '/' + str(table_name) + '.json'
            meta = self.metaPath + '/' + str(table_name) + '.json'
            
            if self.dato.getFK() == False:
                #Se eliminan archivos para mantener la posibilidad de la creacion de nuevas tablas.
                if os.path.exists(info):
                    os.remove(info)
                    os.remove(meta)
                    log = 'Table ' + str(table_name)  + ' dropped successfully.'
                else:
                    log = "Error 1: No such table in this environment."
                    self.sendError(log)
            else:
                log =  'Error 23: Table has FK'
                self.sendError(log)           
        
        return log
            
    def createIndex(self, indexName, table_name, column):
        
        log = 'Error 11: EVM not set up.'
        if self.evm != 0:
            
            print table_name
            print column
            keys = self.sdman.getAllKeys(table_name)
            vals = self.sdman.getAllValues(table_name, column)
            
            if len(keys) != len(vals):
                print("Error, len mismatch")
                return
            
            regs = {}
            while len(keys) != 0:
                regs[vals.pop(0) ] = keys.pop(0)
                
            regs = json.JSONEncoder(encoding = "ISO-8859-1").encode(regs)            
        
            with open(self.indexPath + '/' + indexName + '.json', 'w+') as sysCat:
                #json.dump(regs,sysCat)
                sysCat.write(regs)
            
            self.dato.createIndex(indexName, table_name, column)
        else:
            self.sendError(log)
            
        return log
                
    def alterTable(self, table_name, column, refTable, refColumn):
        if self.evm != 0:
            Re = self.dato.setFK(table_name, column, refTable, refColumn)
            if Re == 0:
                return 'FK could not be set.'
            else:
                return 'FK set correctly.'
        else:
            self.sendError('Error 1: EVM not set up.')
            
    def sendError(self, log):
        errorModule = Logs()
        errorModule.Error(log)
        
    def getEVM(self):
        with open(self.varfile, 'r') as TMP:
            FF = json.load(TMP)
        self.evm = FF["db"]
        TMP.close()
