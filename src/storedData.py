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
from json import JSONDecoder, JSONEncoder

class StoredData(dataFormat):
        
    def __init__(self,order,path):
        dataFormat.__init__(self, order)
        self.order = order
        self.tablename = path
        self.path = abspath(join(dirname(__file__), path))
        try:
            fh = open(self.path,'r')
            t = fh.readline()
            j = JSONDecoder().decode(t)
            for item in j:
                formated = JSONDecoder().decode(j[item])
                formated2 = JSONDecoder().decode(item)
                self.insert(formated2, formated)
        except IOError:
            print("Table not found")
        else:
            fh.close()
            
    def dump(self):
        try:
            fh = open(self.path, 'w+')
            towrite = {}
            for item in self.items():
                snd = item[1]
                snd = JSONEncoder().encode(snd)
                towrite[item[0]] = snd
            fh.write(JSONEncoder().encode(towrite))
        except IOError:
            return -1
        else:
            fh.close()
            return 0
        
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
            fh.write(JSONEncoder().encode(towrite))
            dataFormat.__init__(self, self.order)
        except IOError:
            return -1
        else:
            fh.close()
            return 0

from struct import pack, unpack      
class TestSDClass(object):

    def __init__(self):
        self.sd = None
        
    def create(self):
        self.sd = StoredData(20, 'test.json')
        
    def clear(self):
        self.sd.erase()     
        self.sd.dump()
        
    def randomInsert(self, a = 1):
        for i in range (0 , 10*a ):            
            self.sd.insert(i, [101010101])
        self.sd.dump()

    def showAll(self):
        print self.sd.getAll()
      
    def insertBinary(self):        
        self.sd.insert(0, pack('3sii','Rey', 5,10))
        self.sd.insert(1, pack('3sii','Rey', 5,10))
        self.sd.insert(2, pack('3sii','Rey', 5,10))
        self.sd.dump()
        
    def readall(self):
        result = []
        for item in  self.sd.getAll():
            t = []
            t.append(item[0])
            for i in unpack('3sii', item[1]):
                t.append(i)
            result.append(t)
                
        print result
    
if __name__ == '__main__':
    print 'Performing test for Stored Data ...'
    
    fh = open ('test.json', 'w+')
    fh.write('{}')
    fh.close()
    test = TestSDClass()
    test.create()
    test.insertBinary()
    test.showAll()
    test.readall()
    test.clear()
    test.showAll()