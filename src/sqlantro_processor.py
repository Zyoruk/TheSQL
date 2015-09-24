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


class interprete:
	
	def __init__(self):
		self.logs=[];
		self.cpl=cpl_manager();
		
	def execute(self,parameters):
		
		e , words=syntax_analyzer(parameters)
		answer="";
		if e<0 :
			answer=self.error_(e);
		elif e>0:
			answer=self.query_(e,words);
			
		return answer;

	#Selecciona la accion que se debe ejecutar	
	def query_(self,e,words):
		answer="";
		if e==1:
			answer=self.cpl.create_database(words[2]);
			self.logs.append(answer);
		elif e==2:
			answer=self.cpl.start();
		elif e==3:
			answer=self.cpl.stop();
		elif e==4:
			answer=self.cpl.drop_database(words[2]);
		elif e==5:
			answer=self.cpl.set_database(words[2]);
		elif e==6:
			answer=self.cpl.display_database(words[2]);
		elif e==7:
			answer=self.cpl.drop_table(words[2]);
		elif e==8:
			answer=self.cpl.list_databases();
		elif e==9:
			answer=self.cpl.get_status();
		elif e==10:
			#set_database();
			answer= "set database";
		elif e==11:
			#delete_database();
			answer= "delete database";
		elif e==12:
			#update_table();
			answer= "update table";
		elif e==13:
			#select();
			answer= "Select";
		elif e==14:
			answer ="Create table";
		elif e==15:
			#create
			answer ="Create index";
		elif e==16:
			#create
			answer ="insert";
		return answer;

	#selecciona el mensaje de error que se debe mostrar
	def error_(self,e):
		print e;
		comentario="";
		if e==-1:
			comentario = "Command not fount";
			self.logs.append(comentario);
		elif e==-2:
			comentario = "There are an invalid amount of arguments";
		elif e==-3:
			comentario = "Invalid name for database";
		elif e==-4:
			comentario = "Invalid name for schema";
		elif e==-5:
			comentario = "Invalid name for table";
		elif e==-6:
			comentario = "Invalid command list syntax";
		elif e==-7:
			comentario = "Invalid command get syntax";
		elif e==-8:
			comentario = "Invalid command create syntax";
		elif e==-9:
			comentario = "Invalid command drop syntax";
		elif e==-10:
			comentario = "you can not use reserved words";
		elif e==-11:
			comentario = "Invalid command display syntax";
		elif e==-12:
			comentario = "Invalid command set syntax";
		elif e==-13:
			comentario = "Invalid command set syntax";
		elif e==-14:
			comentario = "Invalid command update syntax";
		elif e==-15:
			comentario = "you should use form"
		elif e==-16:
			comentario = "Invalid syntax in  select syntax"
		elif e==-17:
			comentario = "Invalid aggregate functions syntax"
		elif e==-18:
			comentario = "Invalid select functions syntax"
		else:
			comentario = "Unknown Error";
			
		return comentario;
