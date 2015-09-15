from DataCatalog import DataCatalog

class XQTerPlan(object):
    
    def __init__(self):
        self.data = DataCatalog()
    
    def thePlan(self,tables):
        plan = []
        for table in tables:
            dt = self.data.hasIndex(table)
            if dt == True:
                plan.append([table,00])
            else:
                plan.append([table,01])
        return plan

            
    