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

print "";
print "Bienvenido seas";
print "This is SQLantro shell version 0.1";
print "Licenced Under GPLv2";
print "Writted by Julio S.";
print "";
print "Have fun!";
print "";

flag=1;
input="";

while flag:
	input=raw_input('>');
	if input=="exit":
		flag=0;
	elif input=="help":
		print "toma tu ayuda";
	elif input=="license":
		print "toma tu licencia";
	elif len(input) != 0:
		print execute(input);
