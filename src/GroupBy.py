import copy
from GetEVM import GetEVM
import json
from os.path import abspath, dirname, isfile

EVM_LIST = abspath(dirname('../evm/'))

class GroupBy(object):
    
    def __init__(self):
        return None
    
    def groupby (self, data, bygroup):
        
        cat = copy.copy(data[0])
        result = [cat]
        
        if len(bygroup) == 1:
            result = self.levelone(data, bygroup)
            head = result.pop(-1)
            num = 0
            for i in cat:
                if i == head:
                    cat.pop(num)
                else:
                    num += 1
            cat = [head] + cat
            result = [cat] + result
        else:
            result = self.levelone( data, [bygroup[0]] )
            for i in range(len(bygroup) - 1):
                result = [cat] + self.leveltwo(result, [bygroup[i + 1]])
        
        return result
    
    def leveltwo(self, res):
        #Next level
        return 
    
    def levelone(self, data, bygroup):
        result = []
        name = 0
        for group in bygroup:
                
                index = 0        
                for name in data[0]:
                    
                    subCol = []
                    if name == group:
                        
                        for elem_to_group in data:
                            subCol.append(elem_to_group.pop(index))
                            
                        name = subCol.pop(0)
                        ls = list(set(subCol))
                        
                        for el in ls:
                            arr = [el]
                            indexes = self.getIndeXes(subCol, el)
                            
                            lon = len(data[0])
                            
                            while lon > 0:
                                tmp = []
                                for ind in indexes:
                                    tmp.append(data[ind].pop(0))
                                
                                arr.append(tmp)
                                lon -= 1
                            
                            result.append(arr)
                    else:
                        index += 1
                        
        result.append(name)                
        return result
        
    def getIndeXes (self, ls, pk):
        
        result = []
        num = 1
        for elem00 in ls:
            
            if elem00 == pk:
                result.append(num)
                
            num += 1
        
        return result
    
<<<<<<< HEAD
=======
    '''Return all columns who has an index''' 
    def getAllIndexesCol(self, table):
        ls = []
        if self.evm != 0:
            
            ls = self.getAllIndexes(table, [])
                
        if ls != []:
                       
            onlyfiles = [ f for f in listdir(str(self.indexPath)) if isfile(join(str(self.indexPath),f)) ]
            tmp = []
            
            for jason in onlyfiles:         
                tmp.append(jason.split(".",1)[0])
                
            onlyfiles = []
            for jason in tmp:
                if jason.split("_",1)[1] in ls:
                    onlyfiles.append(jason.split("_",1)[0])
            
            ls = onlyfiles
        
        return ls
        
    '''Destroys all indexes of a table'''    
    def dropAllIndexes(self, tabla):
        ls = []
        if self.evm != 0:
            ls = self.getAllIndexes(tabla, [])
            
        if ls != []:
                       
            onlyfiles = [ f for f in listdir(str(self.indexPath)) if isfile(join(str(self.indexPath),f)) ]
            tmp = []
            
            for jason in onlyfiles:         
                tmp.append(jason.split(".",1)[0])
                
            for jason in tmp:
                if jason.split("_",1)[1] in ls:
                    os.remove(self.indexPath + '/' + str(jason) + '.json')
            
            
            
        
    def getAllIndexes(self, table, ls):
        boo = False            
            
        for meta in self.meta:
            if meta["name"] == table:
                boo = True
                if meta["index"]:
                    for columnsParam in meta["index"]:
                        ls.append(columnsParam["name"])
                else:
                    log = 'Error 11: No index on table.'
                    self.sendError(log)
        if  boo != True:
            log = 'Error 1: No table in DB'
            self.sendError(log)
            
        return ls


>>>>>>> 31e35ed13d6267edaf99882617ed3d8b7f8b8c28
if __name__ == '__main__':
    gb = GroupBy()
    
    data= [ ['Nom', 'Art', 'Com'], 
            ['L', 'A', 10],
            ['D', 'B', 20],
            ['L', 'A', 10],
            ['L', 'B', 20]
         ]
    
    bygroup = ['Art']
    
    print gb.groupby(data, bygroup)
        
        