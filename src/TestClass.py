from DataCatalog import DataCatalog
from SDManager import StoredDataManager
from CLP import CLP
from DDL import DDL
from Logs import Logs
from XQTerPlan import XQTerPlan

class DCTestClass(object):
    
    def __init__(self, DC):
        self.data = DC
    
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
        print(self.data.getTypes("mugre"))
        print(self.data.getNull('mugre','maria'))
        print(self.data.getNulls('mugre'))
        print(self.data.getColNames("mugre"))
        print(self.data.getIndex("mugre", "julia"))
        
    def test4(self):
        print(self.data.getsPK("mugre"))
        print(self.data.getTabNames())
        
        
    def test5(self):        
        print(self.data.setFK('mugre', 'moo', 'demonios' ))
        print(self.data.getFK('mugre'))
        
    def test7(self):
        print(self.data.createIndex('ByName', 'mugre', 'demonios'))
        
    def test8(self):
        print self.data.getFK('mugre')

class CLPTestClass(object):
        
    def __init__(self, DC, SD):
        self.data = CLP(DC, SD)
        
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
    
    def test4(self):
        print(self.data.listDatabases())
        print(self.data.getStatus())
        
    def test5(self):
        print(self.data.dropDatabase("third"))
        
    def test6(self):    
        print(self.data.createDatabase("second"))
        
    def test7(self):
        print self.data.start()
        
    def test8(self):
        print self.data.stop()

class DDLTestClass(object):
        
    def __init__(self, DC, SD):
        self.dato = DDL(DC, SD)
        
    def test0(self):
        print self.dato.setDataBase('second')
        
    def test1(self):
        names = ['julia','maria','demonios']
        types = ['int', 'double', 'char']
        isNull = ['not null', 'null', 'null']    
        
        names1 = ['julia','maria','demonios']
        types1 = ['int', 'double', 'char']
        isNull1 = ['not null', 'null', 'null']
        
        self.dato.createTable('mugre', names, types, isNull , 'julia')
        self.dato.createTable('moo', names1, types1, isNull1 , 'julia')
        
    def test2(self):
        self.dato.setDataBase('third')
        self.dato.dropTable('moo')
        self.dato.setDataBase(0)
        
    def test3(self):
        print self.dato.alterTable('mugre', 'demonios', 'moo', 'demonios')
        self.dato.createIndex('ByColumn', 'moo', 'maria')
        
        

class LogsTestClass(object):
        
    def __init__(self):
        self.dato = Logs()
        
    def test1(self):
        self.dato.Error('This is a test')

class XQTestClass(object):
        
    def __init__(self,DC):
        self.XQ = XQTerPlan(DC)
        
    def test1(self):
        self.XQ.thePlan('moo')        

if __name__ == '__main__':
    print("This is for Science")
    SD = StoredDataManager()
    DC = DataCatalog()
    
    clp = CLPTestClass(DC,SD)
    ddl = DDLTestClass(DC,SD)
    dc = DCTestClass(DC)
    
    #clp.test2()
    #clp.test3()
    #clp.test4()
    #clp.test5()
    #clp.test3()
    clp.test6()
    ''' SET DB AND START '''
    ddl.test0()
    clp.test7()
    '''------------------'''
    ddl.test1()
    ddl.test3()
    dc.test8()
    
    
    #dc = DCTestClass(DC)
    #dc.test1()
    #dc.test3()
    
    
    ''' STOP DB '''
    clp.test8()
    ''' --------'''
    #clp.test7()
    

    
    
    
    
    
    
    
    