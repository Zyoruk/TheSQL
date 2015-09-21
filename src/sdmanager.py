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

    def search(self, table, key):
        if (self.exists(table)):
            return SD.StoredData(5 , '' + table +'.json').search(key)
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
                    indexes.append(self.sysCat.getIndex(table, column)-1)
                
                #Abrimos la tabla
                sd = SD.StoredData(5,'' + table +'.json')
                #Data Format to read/write
                dataFormat = ''
                types = self.sysCat.getTypes(table)[:-1]
                types.sort(cmp=None, key=None, reverse=True)
                for ty in types:
                    dataFormat += self.getPackFormat(ty) 
                #Data                
                dataKey = list(unpack( dataFormat, sd.get(key)))
                for i in indexes:
                    dataKey[i] = values[i]
                
                #Data to bytes
                dataKey = self.packData(self.sysCat.getTypes(table)[1:], dataKey)
                
                #Get all previous elements
                temp = sd.getAll()
                #Erase the file 
                sd.erase()
                #Re- construct the tree with an empty file
                sd = SD.StoredData(5,'' + table +'.json')
                #Insert each item != removed
                for item in temp:
                    if item[0] != key:
                        sd.insert(item[0], item[1])
                sd.insert(key , dataKey)
                sd.dump()
                return 0                
            else:
                if self.validType(self.sysCat.getType(table, columns),values):
                    index = self.sysCat.getIndex(table, columns)-1
                    sd = SD.StoredData(5,'' + table +'.json')
                    dataFormat = ''
                    types = self.sysCat.getTypes(table)[:-1]
                    types.sort(cmp=None, key=None, reverse=True)
                    for ty in types:
                        dataFormat += self.getPackFormat(ty)
                    dataKey = list(unpack( dataFormat, sd.get(key)))
                    dataKey[index] = values
                    dataKey = self.packData(types, dataKey)
                    #Get all previous elements
                    temp = sd.getAll()
                    #Erase the file 
                    sd.erase()
                    #Re- construct the tree with an empty file
                    sd = SD.StoredData(5,'' + table +'.json')
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
                        if self.sysCat.getNull(table, column):
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
            for col in self.sysCat.getColNames(table)[:-1]:
                if col in columns:
                    pass
                else: 
                    if self.sysCat.getNull(table, col):
                        cols.append(col)
                        Types.append(self.sysCat.getType(table, col))
                        vals.append('NULL')
                    else:
                        return -1
            
            #ordenar los datos con respecto al orden de la tabla
            temp  = self.sysCat.getColNames(table)
            temp.sort(cmp=None, key=None, reverse=True)            
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
            sd = SD.StoredData(5,'' + table +'.json') 
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
        result= ''
        for i in range(0, len(types)):
            if values[i] == 'NULL':
                result+= (pack('4s', 'NULL'))
                continue
            if 'DECIMAL' in types[i]:
                a = str(values[i]).split('.')
                intpart = int(types[i][types[i].find('(')+1:types[i].find(',')])
                floatpart = int(types[i][types[i].find(',')+1:types[i].find(')')])
                b = a[0][len(a[0])-intpart:]
                b += '.'
                b += a[1][len(a[1])-floatpart:]
                b = float(b)
                result += (pack(self.getPackFormat(types[i]),b))
            else:    
                fmt = self.getPackFormat(types[i])
                toadd = pack(fmt,values[i])            
                result += toadd
        return result
    
    def getAllValues(self,table, column):
        if self.exists(table):
            res = []
            unpackFormat = ''
            for ty in self.sysCat.getTypes(table):
                unpackFormat += self.getPackFormat(ty)
            
            i = self.sysCat.getIndex(table, column)
            sd = SD.StoredData(5, table)
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
            
            for item in SD.StoredData(5,'' + table +'.json').getAll():
                t = []                
                t.append(item)
                t.append(unpack(unpackFormat, item[1]))
                result.append(t)                
            return t
        return -1
    
    def getAllKeys(self, table):
        if self.exists(table):
            res = []
            l =  SD.StoredData(5,'' + table +'.json').getAll()
            for item in l:
                res.append(item[0][0])
                return res
        return -1
    
    def remove(self,table,key):
        if self.exists(table):
            sd = SD.StoredData(5,'' + table +'.json')
            t = sd.getAll()
            sd.erase()
            sd = SD.StoredData(5,'' + table +'.json') 
            for item in t :
                if item[0] != key:
                    sd.insert(item[0], item[1])
            sd.dump()
        return -1
    
    def erase(self,table):
        if self.exists(table):
            sd = SD.StoredData(5, table)
            sd.erase()
            sd.dump()            
        return -1
    
    def getAllasArray(self,table):
        if self.exists(table):
            sd = SD.StoredData(5, '' + table +'.json')
            fmt = ''
            for ty in self.sysCat.getTypes(table)[1:]:
                fmt += self.getPackFormat(ty)
            return self.fixList(sd.getAll(), fmt)
        return -1
     
    def fixList(self,l,fmt):
        result  = []
        for item in l:
            print [item[1]]
            t = []
            data = unpack(fmt, item[1])
            for i in data:
                if '\x00' in str(i):
                    t.append(str(i)[0:str(i).index('\x00')])
                else:
                    t.append(i)
                    
            
            
            result.append([item[0]] + t)
        return result
    
    def exists(self,table):
        a = self.sysCat.getTabNames()        
        if table in a:
            return True
        return False
    
import DDL
class TesterClass(object):
    def __init__(self):
        self.sdman = StoredDataManager()
        self.ddl = DDL.DDL(self.sdman)
        self.syscat = DataCatalog()
        
    def test1(self):
        self.ddl.setDataBase('naDB')
        self.syscat.setNewTable('test1', ['ID', 'Nom', 'Age'], ['INTEGER', 'VARCHAR', 'INTEGER'], ['NOT NULL','NOT NULL','NOT NULL'],'ID')
        self.syscat.setNewTable('test2', ['ID', 'Nom', 'Age'], ['INTEGER', 'VARCHAR', 'INTEGER'], ['NOT NULL','NOT NULL','NOT NULL'],'ID')
        self.sdman.insert('test1', 0, ['Nom','Age'], ['A', 1])
        self.sdman.insert('test1', 1, ['Nom','Age'], ['B', 2])
        self.sdman.insert('test1', 2, ['Nom','Age'], ['C', 3])
        self.sdman.insert('test1', 3, ['Nom','Age'], ['D', 4])
        self.sdman.insert('test1', 4, 'Nom', 'D')
        print self.sdman.getAllasArray('test1')
    def test2(self):
        self.sdman.update('test1', 0, ['Nom','Age'], ['Andres',54])
        print self.sdman.getAllasArray('test1')
    
    def test3(self):
        self.sdman.update('test1', 0, 'Nom', 'asas')
        self.sdman.update('test1', 0, 'Age', 100)
        print self.sdman.getAllasArray('test1')
        
    def test4(self):
        strs = "LASB"
        p = pack ('100s', strs)
        y = unpack ('100s', p )
        print y

    def test5(self):
        self.syscat.setNewTable('test3', ['ID', 'Nom', 'Age'], ['INTEGER', 'VARCHAR', 'INTEGER'], ['NOT NULL','NOT NULL','NULL'],'ID')
        self.sdman.insert('test3', 1, ['Nom','Age'], ['B', 2])
        self.sdman.insert('test3', 2, ['Nom','Age'], ['C', 3])
        self.sdman.insert('test3', 3, ['Nom','Age'], ['D', 4])
        self.sdman.insert('test3', 4, ['Nom'], ['D'])
        self.sdman.insert('test3', 5, ['Age'],[0])
        self.sdman.getAllasArray('test3')
if __name__ == '__main__':
    t = TesterClass()
    t.test5()
