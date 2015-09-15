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

import sdmanager, xml.etree
class DML(object):
    '''
    classdocs
    '''
    def __init__(self):
        '''
        Constructor
        '''
        self.sdm = sdmanager.StoredDataManager()
        
    def insertInto(self, table, columns, values):
        #if syscat.existsTable(table):
        
        if True:
            lKey = self.sdm.getAllKeys(table)
            for key in lKey:
                return self.sdm.update(table, key, columns, values)
        return -1
    
    def deleteFrom(self, table, where = {}):
        #if syscat.existsTable(table):        
        if True:
            if where == {}:
                return self.sdm.erase(table)
            else:
                #index = syscat.getIndex(col)
                t = self.sdm.getAll(table)
                for item in t :
                    #if item[1][index] <operador> operando:
                    #    self.smd.remove(table, key)
                    print 'algo'
        return -1
    
    def compare (self, operator, tableVal, compVar):
        #Verificar que los tipos de datos sean iguales 
        if True:
            if operator == '=':
                return tableVal == compVar
            elif operator == '>':
                return tableVal > compVar
            elif operator == '<':
                return tableVal < compVar
            elif operator == 'like':
                return tableVal == compVar
            elif operator == 'is not null':
                return tableVal != 'NULL'
            elif operator == 'is null':
                return tableVal == 'NULL'
            elif  operator == 'not':
                return tableVal != compVar
        return -1
        
    def Select(self, table, columns = {}, form = '0'):
        if columns == {}:
            if form != '0':
                return self.FormatXML(self.sdm.getAll(table))
            return self.sdm.getAll(table)
        else:
            #Verificar que existan las columnas en la tabla.
            indexes =[]
            for column in columns:
                #indexes.append(syscat.getindex(column))
                indexes.append(NaN)
            ResultSet ={}
            tempResultSet = self.sdm.getAll(table)
            for item in tempResultSet:
                t = []
                for index in indexes:
                    ResultSet[item[0]] = (item[0], item[1][index])
                    t.append            
            return 1
        
    def FormatXML(self, resultSet):
        print 'a'