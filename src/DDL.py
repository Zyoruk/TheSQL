from DataCatalog import DataCatalog
from SDManager import StoredDataManager
from os.path import abspath, dirname
from Logs import Logs
import os.path
import json

EVM_LIST = abspath(dirname('../evm/'))

class DDL(object):
    
    def __init__(self,sdman):
    #def __init__(self):
        self.dato = DataCatalog()
        self.sdman = sdman
        self.varfile = EVM_LIST + '/' + 'VARIABLES.json'
        self.evm = 0
        self.metaPath = 0
        self.infoPath = 0
        self.log = 'Error 11: EVM not set up.'
        
    def setDataBase(self, db):
        if os.path.isdir(EVM_LIST + '/' + db):
            evm = {'db':db}
            self.evm = str(db)
            self.metaPath = EVM_LIST + '/' + str(db) + '/metadata/'
            self.infoPath = EVM_LIST + '/' + str(db) + '/info/'
            #self.indexPath = EVM_LIST + '/' + str(db) + '/index/'
            with open(self.varfile , 'w') as TMP:
                json.dump(evm,TMP)
            return 'Environment ' + str(db) + ' set.'
        else:
            log = 'Error 1: No existing DB'
            self.sendError(log)
            return log
            
    def createTable(self, table_name, columns_names, column_type, column_nullability, PK):
        self.getEVM()
        
        if self.evm != 0:
            state = self.dato.setNewTable(table_name, columns_names, column_type, column_nullability, PK)
            if state:
                with open(str(self.infoPath) + '/' + str(table_name) + '.json', 'w') as TMP2:
                    json.dump({},TMP2)   
                log = 'Table ' + str(table_name) + ' created successfully.'
            else:
                log = 'Error 4: Could not write metadata.'
                self.sendError(log)
        else:
            self.sendError(self.log)
        
        return log
    
    def dropTable(self, table_name):
        if self.evm != 0:
            info = self.infoPath + '/' + str(table_name) + '.json'
            meta = self.metaPath + '/' + str(table_name) + '.json'
            
            #Se eliminan archivos para mantener la posibilidad de la creación de nuevas tablas.
            if os.path.exists(info):
                os.remove(info)
                os.remove(meta)
            else:
                log = "Error 1: No such table in this environment."            
        else:
            return log
            
    def createIndex(self, indexName, table_name, column):
        
        log = 'Error 11: EVM not set up.'
        if self.evm != 0:
            
            keys = self.sdman.getAllKeys()
            vals = self.sdman.getAllValues(table_name, column)
            
            if len(keys) != len(vals):
                print("Error, len mismatch")
                return
            regs = []
            while len(keys) != 0:
                regs.append( [keys.pop(-1),vals.pop(-1)] )
                
            regs = json.JSONEncoder().encode(regs)   
        
            with open(self.indexPath + '/' + indexName + '.json', 'w') as sysCat:
                json.dump(regs,sysCat)
            
            self.dato.createIndex(indexName, table_name, column)
        else:
            self.sendError(log)
            
        return log
                
    def alterTable(self, table_name, refTable, column):
        if self.evm != 0:
            self.dato.setFK(table_name, refTable, column)
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
