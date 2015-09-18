from DataCatalog import DataCatalog
from os.path import abspath, dirname, join, isdir
import os.path
from os import listdir
import shutil

EVM_LIST = abspath(dirname('../evm/'))

class CLP():
    
    def __init__(self):
        self.data = DataCatalog()
        self.var = 0
        
    def listDatabases(self):
        dbs = [ f for f in listdir(EVM_LIST) if isdir(join(EVM_LIST,f)) ]
        return dbs
        
    def createDatabase(self,db):
        directory = EVM_LIST + '/' + db + '/'
        if not os.path.exists(directory):
            os.makedirs(directory)
        else:
            print("error: db exist already")
        self.data.setNewDB(directory)
    
    def dropDatabase(self, db):
        directory = EVM_LIST + db + '/'
        shutil.rmtree(directory)
        
    def displayDatabase(self):
        print('hello')
        
    '''    
    def start(self, db):
        self.var = db
        
    def stop(self): 
        self.var = 0
        
    def getStatus(self):
        if self.var == 0:
            return "Main directory" 
        else:
            return self.var
    '''    
        
        