from DataCatalog import DataCatalog
from SDManager import StoredDataManager
from os.path import abspath, dirname, join, isdir
import os.path
from os import listdir
import shutil
import json
from Logs import Logs
from GetEVM import GetEVM

EVM_LIST = abspath(dirname('../evm/'))

class CLP():
    
    def __init__(self, DC, SD):
        self.data = DC
        self.dato = SD
        self.varfile = EVM_LIST + '/' + 'VARIABLES.json'
        
    def start(self):
        evm = self.getevm()
        log = 'Error 11: EVM not set' 
        
        if evm != 0:
            self.data.setEVM()      
            evm = EVM_LIST + '/' + str(evm)
                  
            if os.path.exists(evm):
                if os.path.exists(evm + '/metadata'):
                    #load data
                    self.data.loadMetadatas()
                    #self.dato.loadData()
                    log = 'Data loaded.'
                    
                else:    
                    os.makedirs(evm + '/metadata')
                    os.makedirs(evm + '/info')
                    evm0 = evm + '/ERRORS.txt'
                    f = open(evm0,'w+')
                    f.write(evm0)
                    f.close()            
                    log = 'Hard driver erased successfully... \nStarting database: ' + evm + ' successful'
            else:                
                log = 'Error 3: The ' + evm + ' does not exist...\nCalling the JLA for help.'
                self.sendError(log)
        else:
            self.sendError(log)
        
        return log
        
        
    def stop(self): 
        evm = {'db':0}
        with open(self.varfile , 'w') as TMP:
            json.dump(evm,TMP)
        
        self.data.changeDB()
        #self.dato.destruct()
        
        return 'Dont drink and root. Working on /.'
        
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
        return self.data.getTabNames()
            
    def getStatus(self):
        status = []
        status.append(os.getpid())
        evm = self.getevm()
        ls = self.displayDatabase()
        
        if evm == 0:
            status.append('Working at root. You should be doing more work!')
        else:
            status.append('Working on : ' + str(evm) + ' DB.' )
        
            if ls == []:
                status.append('No tables on current Database.')
            else:
                status.append(ls)
            
        return status
    
    def sendError(self, log):
        errorModule = Logs()
        errorModule.Error(log)    
    
    def getevm(self):
        EVM = GetEVM()
        return EVM.getEVM()
        