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
from DataCatalog import DataCatalog
from struct import  *

class StoredDataManager(object):
    '''
    classdocs
    '''

    
    def __init__(self):
        '''
        Constructor
        '''
        self.sysCat = DataCatalog() 
        self.tableList = self.sysCat.getTabNames()
        
    def search(self, table, key):
        if (self.exists(table)):
            return SD.StoredData(20 , table).search(key)
        return -1 

    def update(self, table, key, columns, values ):
        if self.exists(table):
            if (isinstance(columns, list) == False and isinstance (values,list)) or (isinstance(columns, list) and isinstance (values,list) == False):
                return -1
            elif isinstance(columns, list) and isinstance (values,list):            
                indexes = [] 
                for column in columns:
                    indexes.append(self.sysCat.getIndex(table, column))
                sd = SD.StoredData(20,table)
                sd.udpateMultiple(key, indexes, values)
                sd.dump()
                return 0                
            else:
                index = self.sysCat.getIndex(table, column)
                sd = SD.StoredData(20,table)
                sd.updateSingle(key, index, values)
                sd.dump()
                return 0 
        return -1
    
    def insert(self, table, key, columns, values ):
        
        if self.exists(table) and len(columns) == len(values): 
            Types = []
            for column in columns:
                #Existe la columna?
                if self.sysCat.getIndex(table, column) == -1: return -1
                #Armar las que hacen falta    
                for value in values:
                    # Hay que verificar que los tipos de datos sean correctos para columna.
                    valtype = self.sysCat.getType(table, column)
                    if self.validType(valtype,value):
                        Types.append(valtype)
                        break
                    else:
                        return -1
                    '''
                    # Si alguna columna a insertar tiene valor NULL, hay que verificar si lo acepta.
                    elif value == 'NULL':
                        if self.sysCat.allowsNull(table, column):
                            pass
                        else:
                            return -1
                    '''
            cols = columns
            vals = values
            
            #Verificar columnas que hacen falta, y si hacen falta, ver si aceptan null
            for col in self.sysCat.getColNames(table):
                if col in columns:
                    pass
                else: 
                    if self.sysCat.allowsNull(table, col):
                        cols.append(col)
                        Types.append(self.sysCat.getType(table, col))
                        vals.append('NULL')
                    else:
                        return -1
                    

            #ordenar los datos con respecto al orden de la tabla
            temp  = self.sysCat.getColNames(table)
            for i in range(0, len(cols)):
                for j in range (0, len(cols)):
                    if temp[j] == cols[i]:
                        t = cols[j]                        
                        cols[j] = cols[i]
                        cols[i] = t
                        
                        t = vals[j]
                        vals[j] = vals[i]
                        vals[i] = t
                        
                        t = Types[j]
                        Types[j] = Types[i]
                        Types[i] = t
            #con los datos ordenados, hay que tomar cada uno y con su tipo, convertirlos a bytes
            convertedDataList = self.packData(Types, values)
            sd = SD.StoredData(20,table) 
            sd.insert(key, convertedDataList)
            sd.dump()
        else:
            return -1
    
    def getPackFormat(self, types):
        a = ''
        if types == 'INTEGER':
            a = 'i'
        elif 'DECIMAL' in types:
            x = int(types[types.find('(')+1:types.find(',')]) 
            x += int(types[types.find(',')+1:types.find(')')])
            a = x + 's'
        elif 'CHAR' in types:
            a = types[types.find('(')+1:types.find(')')]
            a += 's'
        elif types == 'VARCHAR':
            a = '100s'
        elif types == 'DATETIME':
            a = '10s'
        return a
    
    def packData(self, types, values):
        #VALORES NULOS CON TIPOS DE DATOS ENTEROS ???
        result= []
        for i in range(0, len(types)):
            result.append(pack(self.getPackFormat(types[i]),values[i]))
        return result
    
        
    def getAll(self,table):
        if self.exists(table):
            result = []
            unpackFormat = ''
            for ty in self.sysCat.getTypes(table):
                unpackFormat += self.getPackFormat(ty)
            
            for item in SD.StoredData(20,table).getAll():
                t = []                
                t.append(item)
                t.append(unpack(unpackFormat, item[1]))
                result.append(t)                
            return t
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
            sd = SD.StoredData(20,table)
            sd.remove(key)
            sd.dump()
        return -1
    
    def erase(self,table):
        if self.exists(table):
            sd = SD.StoredData(20, table)
            sd.erase()
            sd.dump()            
        return -1
    
    def exists(self,table):        
        for t in self.tableList:
            if t == table:
                return True
        return False