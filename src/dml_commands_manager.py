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

from DML import DML
import SDManager as SDM
import DataCatalog as DC

class dml_manager:	
	
	def __init__(self,sdman,syscat):
		self.logs=[];
		self.sdman = sdman
		self.syscat = syscat
		self.thedml = DML( self.sdman,self.syscat)
	
	def select():
		print "aqui es donde hacemos el select"
		
	def update(self,parametros):
		print parametros;
		print "aqui es donde hacemos el update"
		print "table: "
		table= parametros[0];
		columns=[];
		values=[];
		where=[];
		print table;
		print "Columnas: "
		contador=0;
		while contador<len(parametros[1]):
			columns.append(parametros[1][contador][0])
			values.append(parametros[1][contador][2])
			contador=contador+1;
			
		print "Where : "
		contador=0;
		while contador<len(parametros[2]):
			if parametros[2][contador]=="and" or parametros[2][contador]=="or":
				where.append(parametros[2][contador]);
			elif len(parametros[2][contador])==3:
				where.append(parametros[2][contador][0]);
				where.append(parametros[2][contador][1]);
				where.append(parametros[2][contador][2]);
							
			contador=contador+1;
		
		print columns
		print values
		print where
		
		return self.thedml.setDataBase(table, columns, values, where);
		
		
	def insert(self,parametros):
		print "aqui es donde hacemos el insert"
		print "tabla: " + parametros[0];
		print "columnas: ";
		table=parametros[0];
		columns=[];
		values=[];
		contador=0;
		while contador<len(parametros[1]):
			columns.append(parametros[1][contador]);
			contador=contador+1;
		print "valores: ";
		contador=0;
		while contador<len(parametros[2]):
			values.append(parametros[2][contador]);
			contador=contador+1;
		
		
		#print table;
		#print columns;
		#print values;
		return self.thedml.insertInto(table, columns, values);
		
		
	def delete(self,parametros):
		print "aqui es donde hacemos el delete"
		print "tabla: " + parametros[0];
		print "where statement: "
		contador=0;
		while contador<len(parametros[1]):
			print parametros[1][contador];
			contador=contador+1;
			
		
