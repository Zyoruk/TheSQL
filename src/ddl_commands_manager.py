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

# SQLantro DDL commands
# Author Julio Sanchez Jimenez
# email: jsnchzjmnz@gmail.com
# website: jsnchzjmnz.wordpress.com

from DDL import DDL
import SDManager as SDM
import DataCatalog as DC


class ddl_manager:	
	
	def __init__(self):
		self.logs=[];
		self.sdman = SDM.StoredDataManager()
		self.syscat = DC.DataCatalog()
		self.theddl = DDL(self.syscat, self.sdman)
		
	def set_database(self,schemaname):
		return self.theddl.setDataBase(schemaname[0]);

	def drop_table(self,tablename):
		return self.theddl.dropTable(tablename[0]);
		
	def create_table(self,parametros):
		print "se crea la tabla: ";
		print parametros[0];
		contador=0;
		while contador<len(parametros[1][0]):
			print "columna "
			print contador;
			print "nombre :"+parametros[1][0][contador]
			print "tipo :"+parametros[1][1][contador]
			print "nombre :"+parametros[1][2][contador]
			contador=contador+1
		if len(parametros[1][3])>0:
			print "el pk es: "+parametros[1][3][0];
			
	def alter_table(self,parametros):
		print "se altera una tabla";
		
	def create_index(self,parametros):
		#print "Se crea un indice";
		#print "nombre del indice: " + parametros[0];
		#print "nombre de la tabla: " + parametros[1];
		#print "columna: " + parametros[2];
		return self.theddl.createIndex(parametros[0],parametros[1],parametros[2]);
