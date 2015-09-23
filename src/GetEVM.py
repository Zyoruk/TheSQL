from os.path import abspath, dirname
import json

EVM_LIST = abspath(dirname('../evm/'))
VARFILE = EVM_LIST + '/' + 'VARIABLES.json'

class GetEVM():
    
    def __init__(self):
        return 0
        
    def getEVM(self):
        
        try:
            with open(VARFILE, 'r') as sysCat:
                db = json.load(sysCat)
        except IOError:
            db = {'db':0}
            with open(VARFILE , 'w') as TMP:
                json.dump(db,TMP)
        else:
            self.sysCat.close()
            
        return db["db"]
        
        