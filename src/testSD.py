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
        self.sd = SD.StoredData(20, 'test.json') #Crea una tabla
        
    def randomInsert(self):
        for i in range (0 , 10 ):            
            self.sd.insert(i, [101010101])
        
    def InsertDupl(self):
        self.sd.insert(5, [101010101])
        
