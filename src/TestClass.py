from DataCatalog import DataCatalog
from CLP import CLP
from DDL import DDL
from Logs import Logs

class DCTestClass(object):
    
    def __init__(self):
        self.data = DataCatalog()
    
    def test1(self):
                   
        names = ['julia','maria','demonios']
        types = ['int', 'double', 'char']
        isNull = ['not null', 'null', 'null']    
        
        names1 = ['julia','maria','demonios']
        types1 = ['int', 'double', 'char']
        isNull1 = ['not null', 'null', 'null']
        
        self.data.setNewTable('mugre', names, types, isNull , 'julia')
        self.data.setNewTable('moo', names1, types1, isNull1 , 'julia')
        
    def test2(self):
        self.data.dropTable("mugre")
    
    def test3(self):
        print(self.data.getType("mugre","demonios"))
        print(self.data.getsPK("mugre"))
        
    def test4(self):
        print(self.data.getColNames("mugre"))
        print(self.data.getTypes("mugre"))
        print(self.data.getTabNames())
        print(self.data.getIndex("mugre", "julia"))
        
    def test5(self):
        return self.data.getTabNames()
    
    def test6(self):
        self.test1()
        print(self.data.getNulls('mugre'))
        print(self.data.getNull('mugre','maria'))
        print(self.data.setFK('mugre', 'moo', 'demonios' ))
        print(self.data.getFK('mugre'))
        
    def test7(self):
        print(self.data.createIndex('ByName', 'mugre', 'demonios'))

class CLPTestClass(object):
        
    def __init__(self):
        self.data = CLP()
        
    def test1(self):
        print(self.data.listDatabases())
        print(self.data.listDatabases())
        self.data.createDatabase("third")
        self.data.dropDatabase("first")
        print(self.data.getStatus())
        
    def test2(self):
        print(self.data.listDatabases())
        
    def test3(self):
        self.data.createDatabase("third")        

class DDLTestClass(object):
        
    def __init__(self):
        self.dato = DDL()
        
    def test0(self):
        self.dato.setDataBase('third')
        
    def test1(self):
        self.dato.setDataBase('third')
        names = ['julia','maria','demonios']
        types = ['int', 'double', 'char']
        isNull = ['not null', 'null', 'null']    
        
        names1 = ['julia','maria','demonios']
        types1 = ['int', 'double', 'char']
        isNull1 = ['not null', 'null', 'null']
        
        self.dato.createTable('mugre', names, types, isNull , 'julia')
        self.dato.createTable('moo', names1, types1, isNull1 , 'julia')
        #self.dato.setDataBase(0)
        
    def test2(self):
        self.dato.setDataBase('third')
        self.dato.dropTable('moo')
        self.dato.setDataBase(0)
        
    def test3(self):
        self.dato.alterTable('mugre', 'moo', 'demonios')
        

class LogsTestClass(object):
        
    def __init__(self):
        self.dato = Logs()
        
    def test1(self):
        self.dato.Error('This is a test')
        

if __name__ == '__main__':
    print("This is for Science")
    clp = CLPTestClass()
    ddl = DDLTestClass()
    log = LogsTestClass()
    dc = DCTestClass()
    clp.test3()
    ddl.test0()
    #log.test1()
    dc.test7()
    
    
    
    
    
    
    
    