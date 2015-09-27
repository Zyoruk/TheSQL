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

# SQLantro CLI interface
# Author Julio Sanchez Jimenez
# email: jsnchzjmnz@gmail.com
# website: jsnchzjmnz.wordpress.com

from sqlantro_processor import * 
import textwrap
from textos import *



class cli:
	
	def __init__(self):
		self.history=[];
		self.flag=1;
		self.entrada="";
		self.i=interprete();
	
	def bienve(self):
		print "";
		print "Bienvenido seas";
		print "This is SQLantro shell version 0.1 for TheSQL DBMS";
		print "Licenced Under GPLv2";
		print "";
		print "Have fun!";
		print "";

	def print_history(self):
		contador=0;
		if len(self.history)>0:
			print self.history[contador];
			contador=contador+1;


	def hilo(self):
		while self.flag:
			self.entrada=raw_input('>');
			self.history.append(self.entrada);
			if self.entrada=="exit":
				self.flag=0;
			elif self.entrada=="help":
				print textwrap.fill(ayuda);
			elif self.entrada=="license":
				print textwrap.fill(license);
			elif len(self.entrada) != 0:
				print self.i.execute(self.entrada);
		
command_line_interface=cli()
command_line_interface.bienve()
command_line_interface.hilo()		
