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
        self.path = abspath(join(dirname(__file__), path))
        file = open(self.path,'r')
        if (file.readline()!= ""):
            j= json.load(file)
            print(j)
        file.close()
            
    '''
    Abrir el archivo y tomar cada elemento del arbol y dumpearlo
    '''
    def dump(self):
        file = open(self.path, 'w+')
        file.write('{')
        for item in self.items():
            print(json.JSONEncoder().encode(item))
        file.write('}')
        file.close()
            