'''
@license:     This file is part of TheSQL.

    TheSQL is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    TheSQL is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with TheSQL.  If not, see <http://www.gnu.org/licenses/>.
Created on Sep 12, 2015

@author: zyoruk
'''
import storedData as SD
class StoredDataManager(object):
    '''
    classdocs
    '''

    
    def __init__(self):
        '''
        Constructor
        '''
        self.sysCat = None #hay que cambiarlo
        self.tableList = [] #hay que rellenarlo de alguna manera conjunto al SysCat
        
    def search(self, table, key):
        if (self.exists(table)):
            return SD.StoredData(20 , table).search(key)
        return -1 

    def update(self, table, key, columns, values ):
        if self.exists(table):
            if (isinstance(columns, list) == False and isinstance (values,list)) or (isinstance(columns, list) and isinstance (values,list) == False):
                return -1
            elif isinstance(columns, list) and isinstance (values,list):            
                indexes = [] # metodo del system catalog para devolver el indice de una columna
                for column in columns:
                    indexes.append(NaN) #metodo de indice
                return SD.StoredData(20, table).udpateMultiple(key, indexes, values)
            else:
                index = NaN # metodo del system catalog para devolver el indice de una columna
                return SD.StoredData(20, table).udpateSingle(key, index, values)
        return -1
    
    def insert(self, table, key, columns, values ):
        if self.exists(table):
            return SD.StoredData(20,table).insert(key, values)
        else:
            return -1
    
    def getAll(self,table):
        if self.exists(table):
            return SD.StoredData(20, table).getAll()
        return -1
    
    def getAllKeys(self, table):
        if self.exists(table):
            res = []
            l =  SD.StoredData(20,table).getAll()
            for item in l:
                res.append(item[0][0])
        return -1
    
    def remove(self,table,key):
        if self.exists(table):
            return SD.StoredData(20, table).remove(key)
        return -1
    def erase(self,table):
        if self.exists(table):
            return SD.StoredData(20, table).erase()
        return -1
    
    def exists(self,table):
        
        for t in self.tableList:
            if t == table:
                return True
        return False