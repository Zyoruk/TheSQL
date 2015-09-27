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

import SDManager as SDM
import os.path
import DataCatalog as DC
from json import dumps
import lxml.etree
#Do not remove import lxml.builder
import lxml.builder

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
        
    def insertInto(self, table, columns, values, index = False):
        if table in self.syscat.getTabNames():
            if columns == []:
                return -1 
            else:
                sets = []
                for i in range(0, len(values)):
                    t = [columns[i], '=', values[i]]
                    sets.append(t)
                
                if self.checkCond(table, sets) == False: return  'Check data types, values or columns.'

                pk = self.syscat.getsPK(table)
                if pk in columns: return 'Primary Key must not be included.'
                
                for key in self.sdm.getAllKeys(table):
                    self.sdm.update(table, key, columns, values) 
                
                if index == True:
                    self.syscat.reDoIndex(table)
                return 0
        return -1
    
    def deleteFrom(self, table, where = {}, index = False):
        if table in self.syscat.getTabNames():   
            if where == {}:
                self.sdm.erase(table)
            else:
                if self.checkCond(table, where) == -1 : return 'Check where statement.'
                toDelete = self.whereRipper(table, where)
                if toDelete != []:
                    for item in toDelete:
                        self.sdm.remove(table, item[0])
                        
            if index == True:
                self.syscat.reDoIndex(table)
            return 0
        return -1
    
    def update(self, table, columns, values, where = [], index = False):
        #hay que Verificar que no se esta tratando de hacer update a un PK
        if (self.syscat.getsPK(table) in columns) or (not(table in self.syscat.getTabNames())):
            return -1
        
        #Verificar que los valores cumplas con los tipos de datos de las columnas
        sets = []
        
        for i in range(0, len(values)):
            t = [columns[i], '=', values[i]]
            sets.append(t)
        
        if self.checkCond(table, sets) == False: return 'Check data types, values or columns.'         
        
        rows = []     
        
        if where == []:
            rows = self.sdm.getAll(table)
        else:
            if self.checkCond(table, where) == -1 : return 'Check where statement.'
            rows = self.whereRipper(self.join(table), where)
            
        for row in rows:
            self.sdm.update(table, row[0], columns, values)
            
        if index == True:
            self.syscat.reDoIndex(table)                
                        
        return 0
        
    def join(self,tables):
        result = []
        temp = []
        colsnames = []
        if isinstance(tables, list):
            for t in tables :
                if not (t in self.syscat.getTabNames()): return -1
                names = list(self.syscat.getColNames(t))
                names.reverse()
                for name in names:
                    colsnames.append('' + t + '.' + name)
                if result == []:
                    result = self.sdm.getAllasArray(t)
                else:
                    temp = result
                    result = [] 
                    for reg1 in temp:
                        for reg2 in self.sdm.getAllasArray(t):
                            if self.compare('=', reg1[0], reg2[0]):
                                result.append(reg1 + reg2)
            result = [colsnames] + result
        else:
            colsnames = list(self.syscat.getColNames(tables))
            colsnames.reverse()
            result = [colsnames] + self.sdm.getAllasArray(tables)            
        return result
            
    def whereRipper(self, listset , where = []):
        if where== []:
            return listset

        names = list(listset[0])
        regs = list(listset[1:])
    
        if len(where) == 1 :
            return self.singleCond(names, regs, where[0])
        else:
            result =[]
            for reg in regs:
                strg = ''
                for cond in where:
                    if len(strg) != 0: strg += ' '
                    if cond == 'AND' or cond == 'OR':
                        strg += str(cond).lower()
                    else:
                        if len(cond) == 2:
                            if cond[0] in names:                     
                                strg +=  str(self.compare(cond[1], reg[list(names).index(cond[0])]))
                            else:
                                strg += str(self.compare(cond[1], cond[0]))
                        else:
                            if cond[0] in names and cond[2] in names:
                                strg += str(self.compare(cond[1], reg[list(names).index(cond[0])], reg[list(names).index(cond[2])]))
                                    
                            elif cond[0] in names:
                                strg += str(self.compare(cond[1], reg[list(names).index(cond[0])], cond[2]))
                            elif cond[2] in names:
                                strg += str(self.compare(cond[1], cond[0], reg[list(names).index(cond[2])]))
                            else:
                                strg += str(self.compare(cond[1], cond[0], cond[2]))
                if eval (strg) == True:
                    result.append(reg)
            return result
                    
    def singleCond(self, names, regs, cond):
        result = []

        if len(cond) == 2:
            for reg in regs:
                if cond[0] in names:                     
                    if (self.compare(cond[1], reg[list(names).index(cond[0])])):
                        result.append(reg)
                else:
                    if (self.compare(cond[1], cond[0])):
                        result.append(reg)
        else:
            for reg in regs: 
                if cond[0] in names and cond[2] in names:
                    if (self.compare(cond[1], reg[list(names).index(cond[0])], reg[list(names).index(cond[2])])):
                        result.append(reg)
                elif cond[0] in names:
                    if (self.compare(cond[1], reg[list(names).index(cond[0])], cond[2])):
                        result.append(reg)
                elif cond[2] in names:
                    if (self.compare(cond[1], cond[0], reg[list(names).index(cond[2])])):
                        result.append(reg)
                else:
                    if self.compare(cond[1], cond[0], cond[2]):
                        result.append(reg)
        return result
                 
    def isDateTime(self, val):
        if len(val) == "10":
            nums = str(val).split('/')
            if len(nums) == 3:
                if len(nums[0]) == 2 and len(nums[0]) == len(nums[1]) and len(nums[2]) == 4:
                    for num in nums:
                        if num.isnumeric():
                            continue
                        else:
                            return False
                        return True
        return False
    
    def checkCond(self, tables, conds):
        for cond in conds:
            type1 = None
            type2 = None
            if len(cond) == 1:
                continue
            if len(cond) == 2:
                if cond [1] != 'IS NOT NULL' or cond[1] != 'IS NULL': return False
                else:
                    val1 = cond[0]
                    if isinstance(val1, str):
                        if '.' in val1:
                            if val1.split('.')[0] in tables:
                                for t in tables : 
                                    if t == val1.split('.')[0]:
                                        if val1.split('.')[1] in self.syscat.getColNames(t):
                                            continue
                                        else:
                                            return False
            if len(cond) == 3 :  
                     
                val1 = cond[0]
                val2 = cond[2]
                if isinstance(val1, int) and isinstance(val2, int):
                    continue
                elif isinstance(val1, str) and isinstance(val2, str):
                    if '.' in val1 and '.' in val2: 
                        if val1.split('.')[0] in tables and val2.split('.')[0] in tables:                        
                            type1 = self.syscat.getType(val1.split('.')[0], val1.split('.')[1])
                            type2 = self.syscat.getType(val2.split('.')[0], val2.split('.')[1])
                        elif  val1.split('.')[0] in tables:
                            type1 = self.syscat.getType(val1.split('.')[0], val1.split('.')[1])
                            if self.isDateTime(val2):
                                type2 = 'DATETIME'
                            else: 
                                type2 = 'VARCHAR'
                        elif val2.split('.')[0] in tables:
                            type2 = self.syscat.getType(val2.split('.')[0], val2.split('.')[1])
                            if self.isDateTime(val1):
                                type1 = 'DATETIME'
                            else: 
                                type1 = 'VARCHAR'
                    elif '.' in val1:
            
                        if val1.split('.')[0] in tables :
                            type1 = self.syscat.getType(val1.split('.')[0], val1.split('.')[1])
                            if self.isDateTime(val2):
                                type2 = 'DATETIME'
                            else: 
                                type2 = 'VARCHAR'
                    elif '.' in val2:
                        if val2.split('.')[0] in tables:
                            type2 = self.syscat.getType(val2.split('.')[0], val2.split('.')[1])
                            if self.isDateTime(val1):
                                type1 = 'DATETIME'
                            else: 
                                type1 = 'VARCHAR'
                    else:
                        if len(tables)== 1:
                            
                            if val1 in self.syscat.getColNames(tables[0]) and val2 in self.syscat.getColNames(tables[0]):
                                type1 = self.syscat.getType(tables, val1)
                                type2 = self.syscat.getType(tables, val2)
                            elif val1 in self.syscat.getColNames(tables[0]):
                                type1 = self.syscat.getType(tables[0], val1)
                                if isinstance(val2, str):
                                    if self.isDateTime(val2):
                                        type2 = 'DATETIME'
                                    else:
                                        type2 = 'VARCHAR'
                                elif isinstance(val2 , int):
                                    type2 = 'INTEGER'
                            elif val2 in self.syscat.getColNames(tables[0]):
                                type2 = self.syscat.getType(tables[0], val2)
                                if isinstance(val1, str):
                                    if self.isDateTime(val1):
                                        type1 = 'DATETIME'
                                    else:
                                        type1 = 'VARCHAR'
                                elif isinstance(val1 , int):
                                    type1 = 'INTEGER'
                            else:
                                type1 = 'VARCHAR'
                                type2 = 'VARCHAR'

                elif isinstance(val1, str):
                    if '.' in val1:
                        if val1.split('.')[0] in tables and val1.split('.')[1] in self.syscat.getColNames(val1.split('.')[0]):
                            type1 = self.syscat.getType(val1.split('.')[0], val1.split('.')[1])
                            if isinstance(val2, str):
                                    if self.isDateTime(val2):
                                        type2 = 'DATETIME'
                                    else:
                                        type2 = 'VARCHAR'
                            else:
                                type2 = 'INTEGER'
                        else:
                            type1 = 'VARCHAR'
                            if isinstance(val2, str):
                                if self.isDateTime(val2):
                                    type2 = 'DATETIME'
                                else:
                                    type2 = 'VARCHAR'
                            else:
                                type2 = 'INTEGER'
                    else:
                            if isinstance(val2, str):
                                if self.isDateTime(val2):
                                    type2 = 'DATETIME'
                                else:
                                    type2 = 'VARCHAR'
                            else:
                                type2 = 'INTEGER'
                            if isinstance(val1, str):
                                if self.isDateTime(val1):
                                    type1 = 'DATETIME'
                                else:
                                    type1 = 'VARCHAR'
                            else:
                                type1 = 'INTEGER'
                elif isinstance(val2, str):
                    if '.' in val2:
                        if val2.split('.')[0] in tables and val2.split('.')[1] in self.syscat.getColNames(val2.split('.')[0]):
                            type2 = self.syscat.getType(val2.split('.')[0], val2.split('.')[1])
                            if isinstance(val1, str):
                                    if self.isDateTime(val1):
                                        type1 = 'DATETIME'
                                    else:
                                        type1 = 'VARCHAR'
                            else:
                                type1 = 'INTEGER'
                        else:
                            type2 = 'VARCHAR'
                            if isinstance(val1, str):
                                if self.isDateTime(val1):
                                    type1 = 'DATETIME'
                                else:
                                    type1 = 'VARCHAR'
                            else:
                                type1 = 'INTEGER'
                    else:
                            if isinstance(val2, str):
                                if self.isDateTime(val2):
                                    type2 = 'DATETIME'
                                else:
                                    type2 = 'VARCHAR'
                            else:
                                type2 = 'INTEGER'
                            if isinstance(val1, str):
                                if self.isDateTime(val1):
                                    type1 = 'DATETIME'
                                else:
                                    type1 = 'VARCHAR'
                            else:
                                type1 = 'INTEGER'
            if self.valid(type1, type2):
                continue
            else: 
                return False
        return True
    
    def valid(self, type1, type2):
        if type1 == 0 or type2 == 0:
            return False
        elif (type2 == 'DATETIME' and ('CHAR' in type1 )) or (type1 == 'DATETIME' and ('CHAR' in type2 )):
            return False
        elif (type1 == 'INTEGER' and type2 != 'INTEGER') or (type2 == 'INTEGER' and type1 != 'INTEGER'):
            return False
        else:
            return True
                
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
    def Select(self, columns = [], tables = [], where = [], groupBy = [], form = '0', index = False):
        if tables == []: return 'At least one table must be used'
        
        #Perform Join       
        resultSet = self.join(tables)
        
        #Perform Where Statement
        if where != []:
            #Check where data types
            if self.checkCond(tables, where) == -1 : return 'Check where statement.'
            #Filtering.
            resultSet = self.whereRipper(resultSet, where)
        
        #Perform Group By
        if groupBy != [] and not (groupBy in columns): return 'group by columns must be inside the select'
        elif groupBy != [] and groupBy in columns:
            print 
    
    def groupBy(self,result_set, group_cols):
        return 0 
            
    def FormatXML(self, resultSet):        
        colnames = resultSet [0]
        regs = resultSet[1:]
        
        st = 'lxml.builder.ElementMaker().root(lxml.builder.ElementMaker().doc('
        
        for reg in regs:
            st += 'lxml.builder.ElementMaker().field('
            for i in range (0, len(colnames)):
                if i == len(colnames) -1 :
                    st += '' + str(colnames[i]) + '=' + '\"' + str(reg[i]) + '\"'
                else:
                    st += '' + str(colnames[i]) + '=' + '\"' + str(reg[i]) + '\"' + ','
            st += '' + '),'
        st = st[0:-1]
        st += '' + '))'    
        the_doc = eval (st)
        
        print lxml.etree.tostring(the_doc, pretty_print=True)
    
    def FormatJSON(self,resultset):
        result = []
        col_names = resultset[0]
        regs = resultset[1:]
        
        for reg in regs:
            
            t = {}            
            for i in range(0, len(col_names)):
                t[col_names[i]] = reg[i]
            result.append(t)
            
        return dumps(result,  sort_keys = False, indent = 2, separators=(',', ': '))
            
        
        
        
from struct import pack, unpack
import unittest
class Test(unittest.TestCase):
    
    def notest1(self):
        self.sdman = SDM.StoredDataManager()
        self.syscat = DC.DataCatalog()
        self.sdman.loadData(self.syscat)
        self.dml = DML(self.sdman, self.syscat)
        print self.dml.insertInto('test1', ['Nom'], ['Luis'])
    
    def nottest2(self):
        self.sdman = SDM.StoredDataManager()
        self.syscat = DC.DataCatalog()
        self.sdman.loadData(self.syscat)
        self.dml = DML(self.sdman, self.syscat)
        self.dml.deleteFrom('test1')
    
    def ntest3(self):
        self.sdman = SDM.StoredDataManager()
        self.syscat = DC.DataCatalog()
        self.sdman.loadData(self.syscat)
        self.dml = DML(self.sdman, self.syscat)
        listset = self.dml.join(['test1','test3'])
        print listset
        print self.dml.whereRipper(listset, [['test1.Age', '=', 3]])
        print self.dml.whereRipper(listset, [['test1.ID', '>', 1], 'OR', ['test3.Age', '>', 0]])
        print self.dml.whereRipper(listset, [['test1.ID', '>', 5], 'AND', ['test3.Age', '>', 0], 'OR', ['test3.Age', '>', 5]])
    
    def notest4(self):
        self.sdman = SDM.StoredDataManager()
        self.syscat = DC.DataCatalog()
        self.sdman.loadData(self.syscat)
        self.dml = DML(self.sdman, self.syscat)
        listset = self.dml.join('test1')
        print listset
        print self.dml.whereRipper(listset, [[5, '=', 5]])
    
    def nottest5(self):
        self.sdman = SDM.StoredDataManager()
        self.syscat = DC.DataCatalog()
        self.sdman.loadData(self.syscat)
        self.dml = DML(self.sdman, self.syscat)
        print self.dml.checkCond(['test1'], [['test1.Age', '=', 'Luis']])
        print self.dml.checkCond(['test1'], [['test1.Age', '=', 'test1.ID']])
        print self.dml.checkCond(['test1'], [['test1.Age', '=', 1]])
        print self.dml.checkCond(['test1'], [[1, '=', 'test1.Age']])
        print self.dml.checkCond(['test1'], [[3, '=', 'test1.Nom']])
        print self.dml.checkCond(['test1'], [['Luis', '=', 'Luis']])
        print self.dml.checkCond(['test1'], [['Luis', '=', 5]])
    
    def nomtest6(self):
        print 'Test6: Update method'
        self.sdman = SDM.StoredDataManager()
        self.syscat = DC.DataCatalog()
        self.sdman.loadData(self.syscat)
        self.dml = DML(self.sdman, self.syscat)
        print self.dml.update('test1', ['Nom'], ['Luis'], [['Nom', '=', 'Andres']])
        print self.dml.update('test1', ['Nom'], ['Luis'], [['Nom', '=', 5]])

    def nomtest7(self):
        self.sdman = SDM.StoredDataManager()
        self.syscat = DC.DataCatalog()
        self.sdman.loadData(self.syscat)
        self.dml = DML(self.sdman, self.syscat)
        listset = self.dml.join('test1')
        print self.dml.FormatJSON(listset)
        
    def nomtest8(self):
        
        self.sdman = SDM.StoredDataManager()
        self.syscat = DC.DataCatalog()
        self.sdman.loadData(self.syscat)
        self.dml = DML(self.sdman, self.syscat)
        listset = self.dml.join('test1')
        print self.dml.FormatXML(listset)

if __name__ == '__main__':
    unittest.main()
    
