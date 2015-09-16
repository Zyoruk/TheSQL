from DataCatalog import DataCatalog

class TestClass(object):
    
    def __init__(self):
        self.data = DataCatalog()
    
    def test1(self):
                   
        names = ['julia','maria','demonios']
        types = ['int', 'double', 'char']
        isNull = ['not null', 'null', 'null']    
        
        names1 = ['julia','maria','demonios']
        types1 = ['int', 'double', 'char']
        isNull1 = ['not null', 'null', 'null']
        
        self.data.setNewTable('mugre', names, types, 'None', isNull , 'julia')
        self.data.setNewTable('moo', names1, types1,'julia', isNull1 , 'julia')
        
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


if __name__ == '__main__':
    print("This is 'Bases de Datos'!")
    tests = TestClass()
    tests.test1()
    
    
    