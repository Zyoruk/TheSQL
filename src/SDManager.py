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
import StoredData as SD
from DataCatalog import DataCatalog
from struct import  pack, unpack
from json import JSONDecoder, JSONEncoder, load
from os.path import abspath,dirname
NULLINT = -2147483647

EVM_LIST = abspath(dirname('../evm/'))
class StoredDataManager(object):
    '''
    classdocs
    '''
    def __init__(self):
        '''
        Constructor
        '''
        self.sysCat = None
        self.dbname = None
        self.env = None

    def loadData(self, sysCat):
        self.sysCat = sysCat
        fh = open ('' + EVM_LIST + '/VARIABLES.json', 'r' )
        self.dbname = JSONDecoder().decode(fh.readline())['db']
        self.env = '' + EVM_LIST +'/' + self.dbname + '/info/'
        
    def destruct(self):
        self.sysCat = None
        self.dbname = None
        self.env = None
        
    def search(self, table, key):
        if (self.exists(table)):
            return SD.StoredData(5 , '' + self.env + table +'.json').search(key)
        return -1 

    def update(self, table, key, columns, values ):
        if self.exists(table):
            if (isinstance(columns, list) == False and isinstance (values,list)) or (isinstance(columns, list) and isinstance (values,list) == False):
                return -1
            elif isinstance(columns, list) and isinstance (values,list):            
                indexes = [] 
                
                fks = self.sysCat.getFKChilds(table)
                if fks == False: pass
                else:
                    for fk in fks:
                        if fk[1] in columns: return 'Cannot update parents referenced data'
                
                fks = self.sysCat.getFK(table)
                if fks == False: pass
                else:
                    for fk in fks:
                        tableRef = fk["reftable"]
                        columnRef = fk["refcolumn"]
                        currCol =  fk["column"]
                        
                        referencedValues = self.getAllValues(tableRef, columnRef)
                        i = self.sysCat.getIndex(table, currCol)
                        
                        if not (values[i] in referencedValues): return 'Violation of FK'                
 
                #Check data types
                for i in range (0, len(columns)):
                    ty = self.sysCat.getType(table, columns[i])
                    if self.validType(ty,values[i]):
                        if ty == 'INTEGER' or 'DECIMAL' in ty and values[i] == 'NULL':
                            values[i] = NULLINT
                        continue
                    else:
                        return -1
                    
                for column in columns:
                    indexes.append(self.sysCat.getIndex(table, column)-1)
                
                sd = SD.StoredData(5,'' + self.env+ table +'.json')
                
                #Data Format to read/write
                dataFormat = ''
                types = list(self.sysCat.getTypes(table)[:-1])
                types.reverse()
                for ty in types:
                    dataFormat += self.getPackFormat(ty)
                     
                #Data                
                tounpack = str(sd.get(key)).encode(encoding = "ISO-8859-1")
                dataKey = list(unpack( dataFormat, tounpack))
                for i in indexes:
                    dataKey[i-1] = values[i-1]
                
                #Data to bytes
                dataKey = self.packData(self.sysCat.getTypes(table)[1:], dataKey)
                
                #Get all previous elements
                temp = sd.getAll()
                #Erase the file 
                sd.erase()
                #Re- construct the tree with an empty file
                sd = SD.StoredData(5,'' + self.env+ table +'.json')
                
                #Insert each item != updated
                for item in temp:
                    if item[0] != key:
                        sd.insert(item[0], item[1])
                        
                sd.insert(key , dataKey)
                sd.dump()
                return 0                
            
            else:
                
                if self.validType(self.sysCat.getType(table, columns),values):
                    
                    fks = self.sysCat.getFKChilds(table)
                    
                    if fks == False: pass
                    else:
                        for fk in fks:
                            if fk[1] == columns: return 'Cannot update parents referenced data'
                    
                    fks = self.sysCat.getFK(table)
                    
                    if fks == False: pass
                    else:
                        
                        for fk in fks:
                            tableRef = fk["reftable"]
                            columnRef = fk["refcolumn"]
                            currCol =  fk["column"]
                            
                            referencedValues = self.getAllValues(tableRef, columnRef)
                            if not (values  in referencedValues): return 'Violation of FK'  
  
                    index = self.sysCat.getIndex(table, columns)-1
                    sd = SD.StoredData(5,'' + self.env+ table +'.json')
                    dataFormat = ''
                    types = list(self.sysCat.getTypes(table)[:-1])
                    types.reverse()
                    
                    for ty in types:
                        dataFormat += self.getPackFormat(ty)
                    
                    tounpack = str(sd.get(key)).encode(encoding = "ISO-8859-1")
                    dataKey = list(unpack( dataFormat, tounpack))
                    
                    if values == 'NULL' and types[index] == 'INTEGER': values = NULLINT
                    

                    dataKey[index-1] = values
                    dataKey = self.packData(types, dataKey)
                    #Get all previous elements
                    temp = sd.getAll()
                    #Erase the file 
                    sd.erase()
                    #Re- construct the tree with an empty file
                    sd = SD.StoredData(5,'' + self.env+ table +'.json')
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
            fks = self.sysCat.getFK(table)
            if fks == False: pass
            else:
                for fk in fks:
                    tableRef = fk["table"]
                    columnRef = fk["columnsRef"]
                    currCol =  fk["column"]
                    
                    referencedValues = self.getAllValues(tableRef, columnRef)
                    i = self.sysCat.getIndex(table, currCol)
                    
                    if not (values[i] in referencedValues): return 'Violation of FK'
                    
            fks = self.sysCat.getFKChilds(table)
            
            if fks == False:
                pass
            else:
                for fk in fks :
                    tableRef = fk[0]
                    colRef = fk[1]
                    i = self.sysCat.getIndex(table, colRef)
                    myColdata = self.getAllValues(table, colRef)
                    if values[i] in  myColdata: return 'Father\'s referenced column must have unique values'
                    
                
            Types = []
            for i in range(0, len(columns)):
                
                if self.sysCat.getIndex(table, columns[i]) == -1: return 'Check columns existance'
                
                coltype = self.sysCat.getType(table, columns[i])
                Types.append(coltype)
                
                if values[i] == 'NULL' and self.sysCat.getNull(table, columns[i]):
                    
                    if coltype == 'INTEGER' or 'DECIMAL'in coltype:
                        values[i] = NULLINT
                        
                elif values[i] == 'NULL' and not (self.sysCat.getNull(table, columns[i])):
                    return 'Nullability Constraint'             
                    
            cols = columns
            vals = values
            
            #Check for missing columns
            for col in self.sysCat.getColNames(table)[:-1]:
                if col in cols:
                    continue
                else: 
                    if self.sysCat.getNull(table, col) == 'NULL':
                        cols.append(col)
                        ty = self.sysCat.getType(table, col)
                        Types.append(ty)
                        if ty == 'INTEGER' or 'DECIMAL' in ty:
                            vals.append(NULLINT)
                        else:
                            vals.append('NULL')
                    else:
                        return 'Nullability Constraint'
            
            #sort columns according to table order
            temp  = list(self.sysCat.getColNames(table))     
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
            
            #metadata stores data reversed
            Types.reverse()
            values.reverse()
            #Convert to bytes
            convertedDataList = self.packData(Types, values)
            sd = SD.StoredData(5,'' + self.env+ table +'.json')
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
            
            if values[i] == 'NULL' and types[i] == 'INTEGER':
                result+= (pack('i', NULLINT))
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
            types = self.sysCat.getTypes(table)[:-1]
            types.reverse()
            
            for ty in types:
                unpackFormat += self.getPackFormat(ty)
            i = self.sysCat.getIndex(table, column)
            sd = SD.StoredData(5, '' + self.env + table +'.json')
            
            for item in sd.getAll():
                tounpack = str(item[1]).encode(encoding = "ISO-8859-1")
                tounpack = list(unpack(unpackFormat, tounpack))
                tounpack.reverse()
                toadd = tounpack[i-1]
                
                if toadd == NULLINT:
                    res.append('NULL')
                else:
                    res.append(toadd)
            
            if res != [] and isinstance(res[0], str) and  '\x00' in res[0]:
                t = []
                index = res[0].index('\x00')
                for item in res:
                    t.append(item[0:index])
                res = t
            return res
        return -1
        
    def getAll(self,table):
        if self.exists(table):
            result = []
            unpackFormat = ''
            for ty in self.sysCat.getTypes(table):
                unpackFormat += self.getPackFormat(ty)
            
            for item in SD.StoredData(5,'' + self.env + table +'.json').getAll():
                t = []                
                t.append(item)
                tounpack = str(item[1]).encode(encoding = "ISO-8859-1")
                t.append(unpack(unpackFormat,tounpack))
                result.append(t)                
            return result
        return -1
    
    def getAllKeys(self, table):
        if self.exists(table):
            res = []
            l =  SD.StoredData(5,'' + self.env + table +'.json').getAll()
            for item in l:
                res.append(item[0])
            return res
        return -1
    
    def remove(self,table,key):
        
        if self.exists(table):
            
            if self.sysCat.getFKChilds(table) != False: return 'Cannot remove data from a parent'
            
            sd = SD.StoredData(5,'' + self.env+ table +'.json')
            t = sd.getAll()
            sd.erase()
            sd = SD.StoredData(5,'' + self.env+ table +'.json')
             
            for item in t :
                if item[0] != key:
                    sd.insert(item[0], item[1])
            sd.dump()
        return -1
    
    def erase(self,table):
        
        if self.exists(table):

            if self.sysCat.getFKChilds(table) != []: return 'Cannot remove data from a parent'
            
            sd = SD.StoredData(5,'' + self.env+ table +'.json')
            sd.erase()
            sd.dump()            
        return -1
    
    def getAllasArray(self,table):
        if self.exists(table):
            sd = SD.StoredData(5, '' + self.env + table +'.json')
            fmt = ''
            for ty in self.sysCat.getTypes(table)[1:]:
                fmt += self.getPackFormat(ty)
            return self.fixList(sd.getAll(), fmt)
        return -1
     
    def fixList(self,l,fmt):
        result  = []
        for item in l:
            t = []
            tounpack = str(item[1]).encode(encoding = "ISO-8859-1")
            data = unpack(fmt,tounpack)
            for i in data:
                if '\x00' in str(i):
                    t.append(str(i)[0:str(i).index('\x00')])
                else:
                    if i == NULLINT:
                        t.append('NULL')
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
import unittest
import CLP
class TesterClass(unittest.TestCase):
    def test1(self):
        self.sdman = StoredDataManager()
        self.syscat = DataCatalog()
        self.ddl = DDL.DDL(self.syscat, self.sdman)
        self.clp = CLP.CLP(self.syscat, self.sdman)
        self.clp.createDatabase('naDB')
        self.ddl.setDataBase('naDB')
        self.clp.start()
        self.ddl.createTable('test1', ['ID', 'Nom', 'Age'], ['INTEGER', 'VARCHAR', 'INTEGER'], ['NOT NULL','NOT NULL','NOT NULL'], 'ID')    
        self.sdman.insert('test1', 0, ['Nom','Age'], ['A', 1])
        self.sdman.insert('test1', 1, ['Nom','Age'], ['B', 2])
        self.sdman.insert('test1', 2, ['Nom','Age'], ['C', 3])
        self.sdman.insert('test1', 3, ['Age', 'Nom'], [ 4, 'D'])
        self.sdman.insert('test1', 4, 'Nom', 'D')
        print self.sdman.getAllasArray('test1')
        
    def test2(self):
        self.sdman = StoredDataManager()
        self.syscat = DataCatalog()
        self.ddl = DDL.DDL(self.syscat, self.sdman)
        self.clp = CLP.CLP(self.syscat, self.sdman)
        self.clp.createDatabase('naDB')
        self.ddl.setDataBase('naDB')
        self.clp.start()
        self.sdman.update('test1', 0, ['Nom','Age'], ['Andres',54])
        print self.sdman.getAllasArray('test1')
    
    def test3(self):
        self.sdman = StoredDataManager()
        self.syscat = DataCatalog()
        self.ddl = DDL.DDL(self.syscat, self.sdman)
        self.clp = CLP.CLP(self.syscat, self.sdman)
        self.clp.createDatabase('naDB')
        self.ddl.setDataBase('naDB')
        self.clp.start()
        self.sdman.update('test1', 0, 'Nom', 'asas')
        self.sdman.update('test1', 0, 'Age', 100)
        print self.sdman.getAllasArray('test1')
        
    def test4(self):

        self.sdman = StoredDataManager()
        self.syscat = DataCatalog()
        self.ddl = DDL.DDL(self.syscat, self.sdman)
        self.clp = CLP.CLP(self.syscat, self.sdman)
        self.clp.createDatabase('naDB')
        self.ddl.setDataBase('naDB')
        self.clp.start()
        self.ddl.createTable('test3', ['ID', 'Nom', 'Age'], ['INTEGER', 'VARCHAR', 'INTEGER'], ['NOT NULL','NOT NULL','NULL'],'ID')
        self.sdman.insert('test3', 1, ['Nom','Age'], ['B', 2])
        self.sdman.insert('test3', 2, ['Nom','Age'], ['C', 3])
        self.sdman.insert('test3', 3, ['Nom','Age'], ['D', 4])
        self.sdman.insert('test3', 4, ['Nom', 'Age'], ['D', 'NULL'])
        self.sdman.insert('test3', 5, ['Nom'], ['E'])
        self.sdman.insert('test3', 6, ['Age'], [22])
        
        print self.sdman.getAllasArray('test3')
        
    def test5(self):     
        data = pack('100si','Rey', 2147483647)
        print [data]
        data = unpack ('100si', data)
        print data
        pack('3sii','Rey', 5,10)
        
    def test6(self):
        self.sdman = StoredDataManager()
        self.syscat = DataCatalog()
        self.ddl = DDL.DDL(self.syscat, self.sdman)
        self.clp = CLP.CLP(self.syscat, self.sdman)
        self.ddl.setDataBase('naDB')
        self.clp.start()
        print self.sdman.getAllKeys('test3')
        print self.sdman.getAllValues('test3', 'Nom')
        print self.sdman.getAllValues('test3', 'Age')
        
if __name__ == '__main__':
    unittest.main()
