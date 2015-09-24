from DataCatalog import DataCatalog

class XQTerPlan(object):
    
    def __init__(self, DC):
        self.data = DC
    
    def thePlan(self,table):
        plan = 0
        dt = self.data.hasIndex(table)
        if dt != False:           
            if dt != 0:
                plan = 1
        else:
            plan = 2
        return plan

            
    