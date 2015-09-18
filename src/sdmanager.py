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
from struct import  pack, unpack

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
                
                #Verificar que los datos cumplan con los tipos de datos
                for i in range (0, len(columns)):
                    if self.validType(self.sysCat.getType(table, columns[i]),values[i]):
                        continue
                    else:
                        return -1
                    
                for column in columns:
                    indexes.append(self.sysCat.getIndex(table, column))
                
                #Abrimos la tabla
                sd = SD.StoredData(20,table)
                #Data Format to read/write
                dataFormat = ''
                for ty in self.sysCat.getTypes(table):
                    dataFormat += self.getPackFormat(ty) 
                #Data
                dataKey = unpack( dataFormat, sd.get(key))
                for i in indexes:
                    dataKey[i] = values[i]
                
                #Data to bytes
                dataKey = self.packData(self.sysCat.getTypes(table), dataKey)
                
                #Get all previous elements
                temp = sd.getAll()
                #Erase the file 
                sd.erase()
                #Re- construct the tree with an empty file
                sd = SD.StoredData(20,table)
                #Insert each item != removed
                for item in temp:
                    if item[0] != key:
                        sd.insert(item[0], item[1])
                sd.insert(key , dataKey)
                sd.dump()
                return 0                
            else:
                if self.validType(self.sysCat.getType(table, columns),values):
                    index = self.sysCat.getIndex(table, column)
                    sd = SD.StoredData(20,table)
                    dataFormat = ''
                    for ty in self.sysCat.getTypes(table):
                        dataFormat += self.getPackFormat(ty) 
                    dataKey = unpack( dataFormat, sd.get(key))
                    dataKey[index] = values
                    dataKey = self.packData(self.sysCat.getTypes(table), dataKey)
                    #Get all previous elements
                    temp = sd.getAll()
                    #Erase the file 
                    sd.erase()
                    #Re- construct the tree with an empty file
                    sd = SD.StoredData(20,table)
                    #Insert each item != removed
                    for item in temp:
                        if item[0] != key:
                            sd.insert(item[0], item[1])
                    sd.insert(key , dataKey)
                    sd.dump()
                    return 0 
                else:
                    return -1 
        return -1
    
    def validType (self, ty, value):
        if (('DECIMAL' in ty) or (ty == 'INTEGER')) and isinstance(value, int):
            return True
        elif (('CHAR' in ty) or 'DATETIME' == ty) and isinstance(ty, str):
            return True
        elif value == 'NULL':
            return True
        else: return -1 
        
    def insert(self, table, key, columns, values ):
        
        if self.exists(table) and len(columns) == len(values): 
            Types = []
            for column in columns:
                #Existe la columna?
                if self.sysCat.getIndex(table, column) == -1: return -1
                #Armar las que hacen falta    
                for value in values:
                    if value == 'NULL':
                        if self.sysCat.allowsNull(table, column):
                            # Hay que verificar que los tipos de datos sean correctos para columna.
                            valtype = self.sysCat.getType(table, column)
                            if self.validType(valtype,value):
                                Types.append(valtype)
                                break
                            else:
                                return -1
                        else:
                            return -1
                    else:
                        # Hay que verificar que los tipos de datos sean correctos para columna.
                        valtype = self.sysCat.getType(table, column)
                        if self.validType(valtype,value):
                            Types.append(valtype)
                            break
                        else:
                            return -1

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
            a = 'd'
        elif 'CHAR' in types and types != 'VARCHAR':
            a = types[types.find('(')+1:types.find(')')]
            a += 's'
        elif types == 'VARCHAR':
            a = '100s'
        elif types == 'DATETIME':
            a = '10s'
        return a
    
    def packData(self, types, values):
        result= []
        for i in range(0, len(types)):
            if values[i] == 'NULL':
                result.append('4s', 'NULL')
                continue
            if 'DECIMAL' in types[i]:
                a = str(values[i]).split('.')
                print a
                intpart = int(types[i][types[i].find('(')+1:types[i].find(',')])
                print intpart
                floatpart = int(types[i][types[i].find(',')+1:types[i].find(')')])
                print floatpart
                b = a[0][len(a[0])-intpart:]
                b += '.'
                b += a[1][len(a[1])-floatpart:]
                b = float(b)
                result.append(pack(self.getPackFormat(types[i]),b))
            else:                
                result.append(pack(self.getPackFormat(types[i]),values[i]))
        return result
    
    def getAllValues(self,table, column):
        if self.exists(table):
            res = []
            unpackFormat = ''
            for ty in self.sysCat.getTypes(table):
                unpackFormat += self.getPackFormat(ty)
            
            i = self.sysCat.getIndex(table, column)
            sd = SD.StoredData(20, table)
            for item in sd.getAll():
                res.append(unpack(unpackFormat, item[1])[i])
            return res
        return -1
        
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
                return res
        return -1
    
    def remove(self,table,key):
        if self.exists(table):
            sd = SD.StoredData(20,table)
            t = sd.getAll()
            sd.erase()
            sd = SD.StoredData(20,table) 
            for item in t :
                if item[0] != key:
                    sd.insert(item[0], item[1])
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
    
if __name__ == '__main__':
    tester = StoredDataManager()
    tester.exists('test.json')
    tester.getPackFormat('INTEGER')
    types = ['INTEGER','DATETIME','CHAR(5)', 'DECIMAL(5,2)', 'VARCHAR']
    a = tester.packData(types, [5,'23/07/1993','Angel',2929292929.2323,'hola'])
    print a