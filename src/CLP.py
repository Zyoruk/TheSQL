from DataCatalog import DataCatalog
from os.path import abspath, dirname, join, isdir
import os.path
from os import listdir
import shutil

EVM_LIST = abspath(dirname("../evm/"))

class CLP():
    
    def __init__(self, evm):
        self.evm = evm
        self.data = DataCatalog()
        
    def listDatabases(self):
        dbs = [ f for f in listdir(EVM_LIST) if isdir(join(EVM_LIST,f)) ]
        return dbs
        
    def createDatabase(self,db):
        directory = EVM_LIST + db + '/'
        if not os.path.exists(directory):
            os.makedirs(directory)
        self.data.setNewDB(directory)
    
    def dropDatabase(self, db):
        directory = EVM_LIST + db + '/'
        shutil.rmtree(directory)
        
    '''
    def start(self):
        
    def stop(self):
        
    def getStatus(self):    
        
    def displayDatabase(self):
        
    def exist(self):
    '''
    
if __name__ == '__main__':
    print("This is 'Bases de Datos'!")
    tests = CLP("number")
        