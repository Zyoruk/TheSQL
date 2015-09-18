from DataCatalog import DataCatalog
from sdmanager import StoredDataManager
from os.path import abspath, dirname
import os.path
import json
from serial.tools.list_ports_windows import REGSAM

EVM_LIST = abspath(dirname('../evm/'))

class DDL():
    
    def __init__(self, sdman):
        self.dato = DataCatalog()
        self.sdman = sdman
        self.varfile = EVM_LIST + '/' + 'VARIABLES.json'
        self.evm = 0
        self.metaPath = 0
        self.infoPath = 0
        self.indexPath = 0
        
    def setDataBase(self,db):
        self.evm = {'db':db}
        self.metaPath = EVM_LIST + '/' + str(db) + '/metadata/'
        self.infoPath = EVM_LIST + '/' + str(db) + '/info/'
        self.infoPath = EVM_LIST + '/' + str(db) + '/index/'
        with open(self.varfile , 'w') as TMP:
            json.dump(self.evm,TMP)
        
    def createTable(self,table_name, columns_names, column_type, column_nullability, PK):
        if self.evm != 0:
            with open(self.varfile, 'r') as TMP:
                FF = json.load(TMP)
            cat = FF["db"]
            TMP.close()
            if cat != 0:
                self.dato.setNewTable(table_name, columns_names, column_type, column_nullability, PK)
                with open(str(self.infoPath) + '/' + str(table_name) + '.json', 'w') as TMP2:
                    json.dump({},TMP2)
        else:
            print('Error: EVM not set up.')
    
    def dropTable(self, table_name):
        if self.evm != 0:
            #with open(str(self.varfile), 'r') as TMP:
                #FF = json.load(TMP)
            #cat = FF["db"]
            #TMP.close()
            #if cat != 0:
            info = self.infoPath + '/' + str(table_name) + '.json'
            meta = self.metaPath + '/' + str(table_name) + '.json'
            index = self.indexPath + '/' + str(table_name) + '_index.json'
            if os.path.exists(info):
                os.remove(info)
                os.remove(meta)
                os.remove(index)
            else:
                print("Error: No such table in this environment.")
            
        else:
            print('Error: EVM not set up.')
            
    def createIndex(self, indexName, table_name, column):
        if self.evm != 0:
            #CREATE INDEX <index-name> ON <table-name>(<column-name>)
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
            print('Error: EVM not set up.')
                
    def alterTable(self, table_name, refTable, column):
        if self.evm != 0:
            self.dato.setFK(table_name, refTable, column)
        else:
            print('Error: EVM not set up.')