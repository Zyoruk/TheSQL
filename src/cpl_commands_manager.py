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

	
def create_database(dbname):
	return "aqui es donde creamos la base de datos "+dbname;

def drop_database(dbname):
	return "aqui es donde eliminamos la base de datos "+dbname;
	
def set_database(schemaname):
	return "aqui es donde asignamos la base de datos "+schemaname;

def drop_table(tablename):
	return "aqui es donde eliminamos la tabla "+tablename;


def list_databases():
	return "aqui se listan las bases de datos";

def get_status():
	return "aqui se obtiene el status";

def stop():
	return "aqui se detienen los procesos";
	
def start():
	return "aqui se inician los procesos";
	
def display_database(dbname):
	return "aqui se muestra la info de la base "+dbname;
	
