from DataCatalog import DataCatalog
from os.path import abspath, dirname, join, isdir
import os.path
from os import listdir
import shutil
import json

EVM_LIST = abspath(dirname('../evm/'))

class DDL():
    
    def __init__(self):
        self.dato = DataCatalog()
        self.varfile = EVM_LIST + '/' + 'VARIABLES.json'
        
    def setDataBase(self,db):
        evm = {'db':db}
        with open(self.varfile , 'w') as vars:
            json.dump(evm,vars)
        
    def createTable(self,table_name, columns_names, column_type, column_nullability, PK):
        with open(self.varfile, 'r') as vars:
            file = json.load(vars)
        cat = self.db["db"]
        vars.close()
        if cat != 0:
            self.dato.setNewTable(table_name, columns_names, column_type, column_nullability, PK)
        else:
            print('Error: EVM not set up.')
    
    def dropTable(self, table_name):
        with open(self.varfile, 'r') as vars:
            file = json.load(vars)
        cat = self.db["db"]
        vars.close()
        if cat != 0:
            self.dato.dropTable(table_name)
        else:
            print('Error: EVM not set up.')
            
    def createIndex(self, indexName, table, column):
        #CREATE INDEX <index-name> ON <table-name>(<column-name>)
        self.dato.createIndex(indexName, table, column)    
                
    def alterTable(self, table_name, refTable, column):
        self.dato.setFK(table_name, refTable, column)