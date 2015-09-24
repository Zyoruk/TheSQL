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

		
def set_database(schemaname):
	print "aqui es donde asignamos la base de datos "+ schemaname;

def drop_table(tablename):
	print "aqui es donde eliminamos la tabla "+tablename;
	
def create_table():
	print "se crea una tabla";

def alter_table():
	print "se altera una tabla";
	
def create_index():
	print "Se crea un indice";