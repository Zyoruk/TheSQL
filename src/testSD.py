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
    
Created on Sep 15, 2015

@author: zyoruk
'''

import storedData as SD
from struct import  *
class TestSDClass(object):
    '''
    classdocs
    '''


    def __init__(self):
        '''
        Constructor
        '''
        self.sd = None
        
    def create(self):
        self.sd = SD.StoredData(20, 'test.json')
    
    def clear(self):
        self.sd.erase()
        if len(self.sd.keys()) != 0:
            for key in self.sd.keys():
                self.sd.remove(key)
        self.sd.dump()
        
    def randomInsert(self, a = 1):
        for i in range (0 , 10*a ):            
            self.sd.insert(i, [101010101])
        self.sd.dump()
        
    def InsertDupl(self):
        self.sd.insert(5, [101010101])
        
    def showAll(self):
        return self.sd.getAll()
    
    def convertToBinary(self):
        for item in self.sd.getAll():
            print pack('iiii',1.23,2.23,3.23,4.23)
            
    
    def binaryFiles(self):
        fh = open('binarytest', 'w+b')
        a = 5
        s = chr(5)
        fh.write(s.encode())
        
        fh.close()
        fh = open('binarytest','rb')
        print fh.readline()
        
    def insertBinary(self):
        
        self.sd.insert(0, pack('3sii','Rey', 5,10))
        self.sd.insert(1, pack('3sii','Rey', 5,10))
        self.sd.insert(2, pack('3sii','Rey', 5,10))
        return self.sd.dump()
        
    def readall(self):
        result = []
        for item in  self.sd.getAll():
            t = []
            t.append(item[0])
            print unpack('3sii', item[1])
                
        print result
                          
if __name__ == '__main__':
    a = '234324324.2323'
    
    print '2' in a
    
    
    