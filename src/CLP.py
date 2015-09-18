from DataCatalog import DataCatalog
from os.path import abspath, dirname, join, isdir
import os.path
from os import listdir
import shutil
<<<<<<< HEAD
=======
import json
>>>>>>> 68636a3833f75ad770cd2d1d592a0efcd41c1dad

EVM_LIST = abspath(dirname('../evm/'))

class CLP():
    
    def __init__(self):
        self.data = DataCatalog()
        self.varfile = EVM_LIST + '/' + 'VARIABLES.json'
        
    def listDatabases(self):
        dbs = [ f for f in listdir(EVM_LIST) if isdir(join(EVM_LIST,f)) ]
        return dbs
        
    def createDatabase(self,db):
        directory = EVM_LIST + '/' + db + '/'
        if not os.path.exists(directory):
            os.makedirs(directory)
            os.makedirs(directory + '/metadata')
            os.makedirs(directory + '/info')
            os.makedirs(directory + '/index')
        else:
            print("error: db exist already")
        #self.data.setNewDB(directory)
    
    def dropDatabase(self, db):
        directory = EVM_LIST + '/' + db + '/'
        shutil.rmtree(directory,True)
        evm = {'db':0}
        with open(self.varfile , 'w') as TMP:
            json.dump(evm,TMP)
        
    def displayDatabase(self):
        datadb = {'db':0}
        try:
            with open(self.varfile, 'r') as sysVAR:
                evm = json.load(sysVAR)
        
        except IOError:
            with open(self.varfile , 'w') as TMP:
                json.dump(datadb,TMP)
        else:
            datadb = evm
            sysVAR.close()        
        
        if datadb['db'] != 0:            
            return datadb['db']
        else:
            return 'No working database set'
            
    def getStatus(self):
        status = []
        status.append(os.geteuid())
        status.append(self.displayDatabase())
        return status
        
    '''    
    def start(self, db):
        self.var = db
        
    def stop(self): 
        self.var = 0
    '''    
        
        