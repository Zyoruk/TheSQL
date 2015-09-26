from os.path import abspath, dirname
import json

EVM_LIST = abspath(dirname('../evm/'))
VARFILE = EVM_LIST + '/' + 'VARIABLES.json'

class GetEVM():
    
    def __init__(self):
        return None
        
    def getEVM(self):
        evm = 0
        try:
            with open(VARFILE, 'r') as sysCat:
                db = json.load(sysCat)
        except IOError:
            db = {'db':0}
            with open(VARFILE , 'w') as TMP:
                json.dump(db,TMP)
        else:
            evm = db["db"]
            sysCat.close()
        
        return evm
    
    def setEVM(self,db):
        DB = {'db':db}
        with open(VARFILE , 'w') as TMP:
            json.dump(DB,TMP)
        TMP.close()
        