from DataCatalog import DataCatalog
from os.path import abspath, dirname, join, isdir
import os.path
from os import listdir
import shutil
import json
from Logs import Logs

EVM_LIST = abspath(dirname('../evm/'))

class CLP():
    
    def __init__(self):
        self.data = DataCatalog()
        self.varfile = EVM_LIST + '/' + 'VARIABLES.json'
        
    def start(self, db):
        
        
    def stop(self): 
        self.var = 0
        
    def listDatabases(self):
        dbs = [ f for f in listdir(EVM_LIST) if isdir(join(EVM_LIST,f)) ]
        if dbs == []:
            dbs = 'No DBs for you.'
        return dbs
        
    def createDatabase(self,db):
        directory = EVM_LIST + '/' + db + '/'
        if not os.path.exists(directory):
            os.makedirs(directory)
            os.makedirs(directory + '/metadata')
            os.makedirs(directory + '/info')
            os.makedirs(directory + '/index')
            successC = 'Hard driver erased successfully...' + "/n" + 'Sorry, database: ' + db + ' created successfully' 
            return successC
        else:
            log = 'Error 3: A ' + db + ' db exist already'
            self.sendError(log)
            return log
        #self.data.setNewDB(directory)
    
    def dropDatabase(self, db):
        directory = EVM_LIST + '/' + db + '/'
        shutil.rmtree(directory,True)
        evm = {'db':0}
        with open(self.varfile , 'w') as TMP:
            json.dump(evm,TMP)
        return 'Database: ' + db + ' dropped successfully' 
        
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
            log = 'No working database set'
            self.sendError(log)            
            return log
            
    def getStatus(self):
        status = []
        status.append(os.geteuid())
        status.append(self.displayDatabase())
        return status
    
    def sendError(self, log):
        errorModule = Logs()
        errorModule.Error(log)    
        
        