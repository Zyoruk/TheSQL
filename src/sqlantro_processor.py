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

# SQLantro Syntax Analyzer interface
# Author Julio Sanchez Jimenez
# email: jsnchzjmnz@gmail.com
# website: jsnchzjmnz.wordpress.com

from syntax_analyzer import *
from cpl_commands_manager import *
from ddl_commands_manager import *
from dml_commands_manager import *

from DML import DML
from Logs import Logs
import SDManager as SDM
import DataCatalog as DC


class interprete:
	
	def __init__(self):
		self.syscat = DC.DataCatalog()
		self.sdman = SDM.StoredDataManager()
		self.cpl=cpl_manager(self.sdman,self.syscat);
		self.ddl=ddl_manager(self.sdman,self.syscat);
		self.dml=dml_manager(self.sdman,self.syscat);
		
		
	def execute(self,parameters):
		
		e , words, parametros =syntax_analyzer(parameters)
		answer="";
		if e<0 :
			answer=self.error_(e);
		elif e>0:
			answer=self.query_(e,words,parametros);
			
		return answer;

	#Selecciona la accion que se debe ejecutar	
	def query_(self,e,words,parametros):
		answer="";
		if e==1:# CLP - CREATE DATABASE
			answer=self.cpl.create_database(parametros);
			#self.logs.append(answer);
		elif e==2:# CLP - START
			answer=self.cpl.start();
		elif e==3:#CLP - STOP
			answer=self.cpl.stop();
		elif e==4:#DDL - DROP DATABASE
			answer=self.cpl.drop_database(parametros);
		elif e==5:#
			answer="hi"	
		elif e==6:# CLP - DISPLAY DATABASE
			answer=self.cpl.display_database(parametros);
		elif e==7:#CLP - DROP TABLE
			answer=self.ddl.drop_table(parametros);
		elif e==8:# CLP - LIST DATABASES
			answer=self.cpl.list_databases();
		elif e==9:# CLP - GET STATUS
			answer=self.cpl.get_status();
		elif e==10:
			#DDL - SET DATABASE
			answer=self.ddl.set_database(parametros);
		elif e==11:#DML - DELETE
			answer= self.dml.delete(parametros);
		elif e==12:#DML - UPDATE
			answer= self.dml.update(parametros);
		elif e==13:
			#select();
			answer= "Select";
		elif e==14:#DDL - CREATE TABLE
			answer=self.ddl.create_table(parametros);
		elif e==15:#DDL - CREATE INDEX
			answer=self.ddl.create_index(parametros);
		elif e==16:
			answer =self.dml.insert(parametros);
		return answer;

	#selecciona el mensaje de error que se debe mostrar
	def error_(self,e):
		print e;
		comentario="";
		if e==-1:
			comentario = "Command not fount";
			#self.logs.append(comentario);
		elif e==-2:
			comentario = "There are an invalid amount of arguments";
		elif e==-3:
			comentario = "Invalid name for database";
		elif e==-4:
			comentario = "Invalid name for schema";
		elif e==-5:
			comentario = "Invalid name for table";
		elif e==-6:
			comentario = "Invalid list syntax";
		elif e==-7:
			comentario = "Invalid get syntax";
		elif e==-8:
			comentario = "Invalid create syntax";
		elif e==-9:
			comentario = "Invalid drop syntax";
		elif e==-10:
			comentario = "you can not use reserved words";
		elif e==-11:
			comentario = "Invalid display syntax";
		elif e==-12:
			comentario = "Invalid set syntax";
		elif e==-13:
			comentario = "Invalid set syntax";
		elif e==-14:
			comentario = "Invalid update syntax";
		elif e==-15:
			comentario = "you should use form"
		elif e==-16:
			comentario = "Invalid syntax in  select syntax"
		elif e==-17:
			comentario = "Invalid aggregate functions syntax"
		elif e==-18:
			comentario = "Invalid select functions syntax"
		elif e==-19:
			comentario = "Invalid delete syntax"
		elif e==-19:
			comentario = "La cantidad de columnas no coincide con la cantidad de valores"
		else:
			comentario = "Unknown Error";
			
		return comentario;
