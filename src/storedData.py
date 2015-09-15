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
    
Created on Sep 7, 2015

@author: zyoruk
'''


from btree import BPlusTree as dataFormat
from os.path import abspath, dirname, join
import json

class StoredData(dataFormat):
    '''
    classdocs
    '''
    
    def __init__(self,order,path):
        '''
        Constructor
        '''
        dataFormat.__init__(self, order)
        self.tablename = path
        self.path = abspath(join(dirname(__file__), path))
        try:
            fh = open(self.path,'r')
            t = fh.readline()
            j = json.JSONDecoder().decode(t)
            for item in j:
                formated = json.JSONDecoder().decode(j[item])
                formated2 = json.JSONDecoder().decode(item)
                self.insert(formated2, formated)
        except IOError:
            print("Table not found")
        else:
            fh.close()
    '''
    Abrir el archivo y tomar cada elemento del arbol y dumpearlo
    '''
    def dump(self):
        try:
            fh = open(self.path, 'w+')
            towrite = {}
            for item in self.items():
                snd = item[1]
                snd = json.JSONEncoder().encode(snd)
                towrite[item[0]] = snd
            fh.write(json.JSONEncoder().encode(towrite))
        except IOError:
            return -1
        else:
            fh.close()
            return 0
            
    def udpateMultiple(self,key,indexes, values):
        temp = self.get(key)
        if temp != None:
            i = 0
            for index in indexes:
                try:
                    temp[index] = values[i]
                except IndexError:
                    return -1
                else:
                    self.insert(key, temp)
                    i+=1
            return 0                
        return -1
    
    def updateSingle(self,key,index,value):        
        temp = self.get(key)
        if temp != None:
            try:
                temp[index] = value
            except IndexError:
                return -1
            else:
                self.insert(key, temp)
                return 0                
        return -1
        
    def search(self,key):
        res = self.get(key)
        if res != None:
            toret = [key]
            toret.append(res)
            return toret
        return -1
    
    def insert (self,key,data):
        if self.get(key) == None:
            dataFormat.insert(self, key, data)
            return 0 
        return -1
            
    def getAll(self):
        return self.items()
    
    def erase(self):
        try:
            fh = open(self.path, 'w+')
            towrite = {}
            fh.write(json.JSONEncoder().encode(towrite))
        except IOError:
            return -1
        else:
            fh.close()
            return 0
    
    def remove(self,key):
        if self.get(key) != None:
            dataFormat.remove(self, key)
            return 0
        return -1