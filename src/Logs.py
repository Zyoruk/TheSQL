from time import gmtime, strftime
from os.path import abspath, dirname
import json

EVM_LIST = abspath(dirname('../evm/'))
VARFILE = EVM_LIST + '/' + 'VARIABLES.json'

class Logs(object):
    
    def __init__(self):
        self.varfile = EVM_LIST + '/' + 'CMD_Log.txt'
        self.errorPath = 0
       
    def getEVM(self):
        try:
            with open(VARFILE, 'r') as sysCat:
                db = json.load(sysCat)
        except IOError:
            self.History('Error 1: EVM not set')
            #print("Error: EVM not set up")
        else:
            sysCat.close()
            if db["db"] != 0:
                self.errorPath = EVM_LIST + '/' + str(db["db"]) + '/ERRORS.txt'            
        
    def Error(self, Erroline):
        if self.errorPath == 0:
            self.getEVM()
            
        if self.errorPath != 0:
            ErrorlineTime = strftime("%Y-%m-%d %H:%M:%S", gmtime()) + ":" + Erroline + '\n'
            try:
                f = open(self.errorPath,'a')
            except IOError:                
                with open(self.errorPath, 'w') as sysVAR:
                    json.dump(ErrorlineTime,sysVAR)
            else:
                f.write(ErrorlineTime)
                f.close()
        else:
            self.History('Error 2: EVM not set')
        
            
    def History(self, CMD):
        lineCMD = strftime("%Y-%m-%d %H:%M:%S", gmtime()) + ":" + CMD
        try:
            f = open(self.varfile,'a')
        except IOError:
            with open(self.varfile, 'w') as sysVAR:
                    json.dump(lineCMD,sysVAR)
        else:
            f.write(self.varfile)
            f.close()
            

                