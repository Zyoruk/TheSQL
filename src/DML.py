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
    
Created on Sep 13, 2015

@author: zyoruk
'''

import sdmanager as SDM
import os.path
import DataCatalog as DC
class DML(object):
    '''
    classdocs
    '''
    def __init__(self, sdman, syscat):
        '''
        Constructor
        '''           
        self.sdm = sdman
        self.syscat = syscat
        
    def insertInto(self, table, columns, values):
        if table in self.syscat.getTabNames():
            lKey = self.sdm.getAllKeys(table)
            for key in lKey:
                self.sdm.update(table, key, columns, values)
        return -1
    
    def deleteFrom(self, table, where = {}):
        if table in self.syscat.getTabNames():   
            if where == {}:
                return self.sdm.erase(table)
            else:
                toDelete = self.whereRipper(table, where)
                if toDelete != []:
                    for item in toDelete:
                        self.sdm.remove(table, item[0])
                    return 0
        return -1
    
    def update(self, table, columns, values, where = []):
        #hay que Verificar que no se esta tratando de hacer update a un PK de otra tabla
        if (self.syscat.getsPK(table) in columns) or (os.path.isfile(table) == False):
            return -1
                
        rows = []     
        if where == []:
            rows = self.sdm.getAll(table)
        else:
            rows = self.whereRipper(table, where)

        for row in rows:
            self.sdm.update(table, row[0], columns, values)
                
        return 0
        
    def whereRipper(self, table, where = []):
        if self.syscat.getType(table, where[0]) == self.syscat.getType(table, where[2]):
            index = self.syscat.getIndex(table, where[0])
            rows = self.sdm.getAll(table)
            resultset = []
            for row in rows:
                if self.compare(where[1], row[1][index], where[2]):
                    resultset.append(row)
            return resultset
        return -1
            
    def compare (self, operator, tableVal, compVar = None):
        if operator == '=':
            return tableVal == compVar
        elif operator == '>':
            return tableVal > compVar
        elif operator == '<':
            return tableVal < compVar
        elif operator == 'LIKE':
            return self.like(tableVal, compVar)
        elif operator == 'IS NOT NULL':
            return tableVal != 'NULL'
        elif operator == 'IS NULL':
            return tableVal == 'NULL'
        elif  operator == 'NOT':
            return tableVal != compVar
        return -1
        
    def like (self, val1 , val2):
        t = str(val1)
        #No aceptamos combinaciones de ambos
        if '%' in val2 and '_' in val2:
            return -1
        if (len(t) >= len(val2)):                
            if val2[0] == '%':
                return val2[1:] == t[(len(t) - len(val2)):]
            elif val2[-1] == '%':
                return val2[:-1] == t[0:(len(val2)-1)]
            elif val2[0] == '%' and val2[-1] == '%':
                return val2[1:-1] in t
            elif '_' in val2:
                for letter in t:
                    for char in val2:
                        if char == '_':
                            break
                        elif char == letter:
                            break
                        else:
                            return False
        return -1
            
    def exists(self,tables):
        for table in tables:
            if os.path.isfile(table) == False: 
                return False 
   
    #TODO
    def join(self,tables):
        result =  []
        i = 1
        
        #arreglar el formato de las listas para mejor manipulacion
        return self.join_aux()
     
    #TODO
    def Select(self, tables, columns = [], form = '0', where = [], on=[]):
        #si son varias tablas.
        if isinstance(tables, list):
            workWith= self.join(tables, on)
        if self.exists(tables):
            if columns == []:
                if form != '0':
                    return self.FormatXML(self.sdm.getAll(tables))
                return self.sdm.getAll(tables)
            else:
                #Verificar que existan las columnas en la tabla.
                indexes = []
                for column in columns:
                    #indexes.append(syscat.getindex(column))
                    indexes.append(NaN)
                ResultSet ={}
                tempResultSet = self.sdm.getAll(tables)
                for item in tempResultSet:
                    t = []
                    for index in indexes:
                        ResultSet[item[0]] = (item[0], item[1][index])
                        t.append            
                return 1
            
    def FormatXML(self, resultSet):
        print 'a'
        
class DMLTest(object):
    def __init__(self):
        self.sdman = SDM.StoredDataManager()
        self.syscat = DC.DataCatalog()
        self.dml = DML(self.sdman, self.syscat)
    
    def test1(self):
        self.dml.insertInto('test1', 'Nom', 'Luis')
    
    def test2(self):
        self.dml.deleteFrom('test1')
    
    def test3(self):
        return 0
        
if __name__ == '__main__':
    dmlt = DMLTest()
    dmlt.test2()