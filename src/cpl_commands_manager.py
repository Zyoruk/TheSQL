#!/usr/bin/python

'''
This file is part of SQLantro.

    SQLantro is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    SQLantro is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.
'''

# SQLantro CPL commands
# Author Julio Sanchez Jimenez
# email: jsnchzjmnz@gmail.com
# website: jsnchzjmnz.wordpress.com


from Logs import Logs
from CLP import CLP
import SDManager as SDM
import DataCatalog as DC
from XQTerPlan import XQTerPlan

class cpl_manager:
	def __init__(self):
		self.sdman = SDM.StoredDataManager()
		self.syscat = DC.DataCatalog()
		self.theclp = CLP(self.syscat, self.sdman)

		self.logs=[];

	def create_database(self,dbname):
		print dbname;
		return self.theclp.createDatabase(dbname);

	def drop_database(self,dbname):
		return self.theclp.dropDatabase(dbname);
		
	def list_databases(self):
		return self.theclp.listDatabases();

	def get_status(self):
		return self.theclp.getStatus();

	def stop(self):
		return self.theclp.stop();
		
	def start(self):
		return self.theclp.start();
		
	def display_database(self,dbname):
		return self.theclp.displayDatabase();
	
