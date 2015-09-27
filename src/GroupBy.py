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
        
        else:
            result = self.levelone( data, [bygroup[0]] )
            for i in range(len(bygroup) - 1):
                result = self.leveltwo(result, [bygroup[i + 1]])
        return result
    
    def leveltwo(self, res):
        print 'hello'
    
    def levelone(self, data, bygroup):
        result = []
        for group in bygroup:
                
                index = 0        
                for name in data[0]:
                    
                    subCol = []
                    if name == group:
                        
                        for elem_to_group in data:
                            subCol.append(elem_to_group.pop(index))
                            
                        subCol.pop(0)
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
        return result
        

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
        
        