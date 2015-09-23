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


'''
Analiza de forma general el comando recibido para evaluar su validez sintactica.
Devuelve resultados con valores numericos que se asignan a cada error:
	comando desconocido: -1
	cantidad invalida de parametros: -2
	nombre invalido para la BD
'''

import json
 
data = '{ "one": 1, "two": { "list": [ {"item":"A"},{"item":"B"} ] } }'


def writer(data, nombre_archivo):
	try:
		data=json.loads(data)
		archivo=nombre_archivo+".json"
		with open(archivo, "w") as outfile:
			json.dump(data, outfile, indent=4)
	 
	except (ValueError, KeyError, TypeError):
		print "JSON format error"




def reader(nombre_archivo):
	with open(nombre_archivo) as file:
		result = json.load(file)
	file.close()
	print (type(result))
	print (result.keys())
	print (result)	
