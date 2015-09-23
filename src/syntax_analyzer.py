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
syntax_analyzer analiza de forma general el comando recibido para evaluar su validez sintactica.
Devuelve resultados con valores numericos que se asignan a cada error:
	comando desconocido: -1
	cantidad invalida de parametros: -2
	nombre invalido para la BD
	parameters: parametro de entrada. Comando SQL a ejecutar
	
	Lista de comandos:
	
		stop
		start
		create database
		drop database
		drop table
		display database
		set database
		delete from
		list databases
		get status
		
'''
def syntax_analyzer( parameters ):
	answer=0; #variable que contiene el valor a retornar
	aux=[]; 
	words=parameters.split();
	if len(words)>0:

		
#evalua el comando stop.
		if words[0].lower()=="stop":
			if len(words)==1:
				answer=3;
			else:
				answer = -2;
#evalua el comando start.
		elif words[0].lower()=="start":
			if len(words)==1:
				answer=2;
			else:
				answer = -2;
		elif len(words)==1:
#asigna codigo de error correspondiente.
			if reserved_word(words[0]):
				answer=-2;
			else:
				answer=-1;
#evalua el create database.
		elif words[0].lower() == "create":
			if words[1].lower()=="database":
				counter=2;
				if len(words) == 3 :
					if reserved_word(words[2]):
						answer =-10;
					else:
						answer=1;
				else:
					answer=-3
			elif words[1].lower()=="table":
				if len(words)>4:
					if reserved_word(words[2])==False:
						if words[3].lower()=="as":
							aux="";
							counter=4;
							while counter<len(words):
								aux=aux+words[counter];
								counter=counter+1;
							aux.replace(" ","");
							aux1=aux.lower();
							if create_table_analyzer(aux1):
								answer=15;
							else:
								error=-18;

						else:
							answer=-18;
					else:
						answer=-10;
				else:
					answer=-2;
			elif words[1].lower()=="index":
				if len(words)>4:
					if reserved_word(words[2])==False:
						if words[3].lower()=="on":
							aux="";
							counter=4;
							while counter<len(words):
								aux=aux+words[counter];
								counter=counter+1;
							aux.replace(" ","");
							if create_index_analyzer(aux.lower()):
								answer=15;
							else:
								error=-18;

						else:
							answer=-18;
					else:
						answer=-10;
				else:
					answer=-2;
			else:
				answer=-8;


#evalua el drop
		elif words[0].lower()== "drop" :
			#drop database
			if words[1].lower()=="database":
				counter=2;
				if len(words) == 3:
					if reserved_word(words[2]):
						answer=-10;
					else:
						answer=4;
				else:
					answer=-3
			#drop table
			elif words[1].lower()=="table":
				counter=2;
				if len(words) == 3:
					if reserved_word(words[2]):
						answer=-10;
					else:
						answer=7;
				else:
					answer=-5
			else:
				answer=-9;
#evalua display database
		elif words[0].lower()== "display" :
			if words[1].lower()=="database":
				counter=2;
				if len(words) == 3:
					if reserved_word(words[2]):
						answer=-10;
					else:
						answer=5;
				else:
					answer=-3
			else:
				answer=-11;
					   
# evalua el set database
		elif words[0].lower() == "set" :
			if words[1].lower()=="database":
				counter=2;
				if len(words) == 3:
					if reserved_word(words[2]):
						answer=-10;
					else:
						answer=10;
				else:
					answer=-4
			else:
				answer=-12;	
		elif words[0].lower() == "insert" :
			if len(words)>3:
				if words[1].lower() == "into":
					if tablenames_columns_analyzer(words[2].lower()):
						if values_analyzer(words[3].lower()):
							answer=16;
			else:
				answer=-2
#evalua el delete		
		elif words[0].lower() == "delete" :
			if words[1].lower()=="from":
				counter=2;
				if len(words) >= 3:
					if reserved_word(words[2]):
						answer=-10;
					else:
						if len(words)==3:
							answer=11;
						else:
							if words[3].lower()=="where" :
								counter=4;
								
								while len(words)>counter:
									if reserved_word(words[counter]):
										answer=-10;
									else:
										aux.append(words[counter]);
										#print words[counter];
									counter=counter+1;
								where_statement_analyzer(aux);
#evalua el update
		elif words[0].lower() == "update" :
			if len(words)>2:
				if reserved_word(words[1]):
					answer=-10;
				else:
					if len(words)>3:
						
						if words[2]=="set":
							aux=[];
							counter=3;
							while counter<len(words):
								aux.append(words[counter]);
								counter=counter+1;
							if set_analyzer(aux):
								answer=12;
							else:
								answer=-14;
						else:
							answer=-14;
					else:
						answer=-2;
			else:
				answer=-2;	

#ahora si, aqui se viene el select
		
		elif words[0].lower()=="select":
			answer=select_analyzer(words);

#evalua list databases y get status		
		elif len(words)==2:
			if words[0].lower()=="list":
				if words[1].lower()!="databases":
					answer=-6;
				else:
					answer=8;
			elif words[0].lower()=="get":
				if words[1].lower()!="status":
					answer=-6;
				else:
					answer=9;
			else:
				answer= -1;
					

				
#asigna error, ya que no se reconoce el comando.
		else:
			answer = -1;
			
	return answer,words;
   
 
 
def create_index_analyzer(parameters):
	r_parentesis_pos=word_finder(parameters,")");
	l_parentesis_pos=word_finder(parameters,"(");
	contador=0;
	table_name="";
	answer=False;
	while contador<l_parentesis_pos:
		table_name=table_name+parameters[contador]
		contador=contador+1;
	contador=contador+1;
	while contador<r_parentesis_pos:
		column_name=table_name+parameters[contador]
		contador=contador+1;
	if len(parameters)==r_parentesis_pos+1:
		if reserved_word(table_name)==False and reserved_word(column_name)==False:
				answer=True;
	return answer;
   


def tablenames_columns_analyzer(parameters):
	r_parentesis_pos=word_finder(parameters,")");
	l_parentesis_pos=word_finder(parameters,"(");
	contador=0;
	table_name="";
	answer=False;
	while contador<l_parentesis_pos:
		table_name=table_name+parameters[contador]
		contador=contador+1;
	contador=contador+1;
	while contador<r_parentesis_pos:
		column_name=table_name+parameters[contador]
		contador=contador+1;
	if len(parameters)==r_parentesis_pos+1:
		if reserved_word(table_name)==False and reserved_word(column_name)==False:
				answer=True;
	return answer;

def values_analyzer(parameters):
	r_parentesis_pos=word_finder(parameters,")");
	l_parentesis_pos=word_finder(parameters,"(");
	contador=0;
	table_name="";
	answer=False;
	while contador<l_parentesis_pos:
		value=table_name+parameters[contador]
		contador=contador+1;
	contador=contador+1;
	while contador<r_parentesis_pos:
		valores=table_name+parameters[contador]
		contador=contador+1;
	if len(parameters)==r_parentesis_pos+1:
		if value.lower()=="value" and reserved_word(valores)==False:
				answer=True;
	return answer;

def create_table_analyzer(parameters):
	r_parentesis_pos=word_finder(parameters,");");
	l_parentesis_pos=word_finder(parameters,"(");
	contador=l_parentesis_pos+1;
	answer=False;
	aux="";
	

	if r_parentesis_pos!=-1 and l_parentesis_pos!=-1: 
		while contador<l_parentesis_pos:
			aux=aux+parameters[contador];
			contador=contador+1;
		
		aux.replace(" ","");
		aux_list=aux.split(",");

	return True#answer;
       

def valid_type(parameters):
	answer==False;
	if parameters.lower()=="integer":
		answer=True;
	elif parameters.lower()=="varchar":
		answer=True;
	elif parameters.lower()=="datetime":
		answer=True;
	else:

		r_parentesis_pos=word_finder(parameters,");");
		l_parentesis_pos=word_finder(parameters,"(");
		contador=l_parentesis_pos+1;
		answer=False;
		aux="";
		if r_parentesis_pos!=-1 and l_parentesis_pos!=-1: 
			tipo="";
			detalle="";
			while contador<l_parentesis_pos:
				tipo=tipo+parameters[contador];
				contador=contador+1;
			contador=contador+1;
			while contador<r_parentesis_pos:
				detalle=detalle+parameters[contador];
				contador=contador+1;
			if tipo.lower()=="char":
				if is_num(detalle):
					answer=True;
			elif tipo.lower()=="decimal":
				aux_detalle=detall.split(",");
				if is_num(aux_detalle[0]) and is_num(aux_detalle[1]):
					answer=True;
							
	return True;                        


def is_num(parameters):
	contador=0;
	answer=True;
	lista=["1","2","3","4","5","6","7","8","9","0"]
	while contador<len(parameters) and answer:
		contador_aux=0;
		while contador_aux<10:
			if parameters[contador]==lista[contador_aux]:
				contador_aux=contador_aux+1;
			else:
				answer=False;
		contador=contador+1;
	return answer


        
def valid_nullability_constraint(parameters):
	answer=False;
	if parameters.lower()=="null" or parameters.lower()=="notnull":
			answer=True;
	return answer;




def valid_primary_key(parameters):
	r_parentesis_pos=word_finder(parameters,")");
	l_parentesis_pos=word_finder(parameters,"(");
	contador=0;
	primarykey_tag="";
	column="";
	answer=False;
	while contador<l_parentesis_pos:
		primarykey_tag=primarykey_tag+parameters[contador];
		contador=contador+1;
	contador=contador+1;
	while contador<r_parentesis_pos:
		column=column+parameters[contador];
		contador=contador+1;
	if primarykey_tag.lower()=="primarykey":
		if reserved_word(column)==False:
				answer=True;

	return answer;  
   

   
   
def select_analyzer(parameters):
	answer=-16;
	flag=False;
	banderisha=True;
	aux_list=[];
	aux_string="";
	select_pos=word_finder(parameters,"select");
	from_pos=word_finder(parameters,"from");
	where_pos=word_finder(parameters,"where");
	group_pos=word_finder(parameters,"group");
	by_pos=word_finder(parameters,"by");
	for_pos=word_finder(parameters,"for");
	json_pos=word_finder(parameters,"json");
	xml_pos=word_finder(parameters,"xml");
	menor=select_pos;
	menor_lista=[];
	menor_lista.append(group_pos);
	menor_lista.append(by_pos);
	menor_lista.append(for_pos);
	menor_lista.append(json_pos);
	menor_lista.append(xml_pos);
	counter=0;
	while counter<len(menor_lista):
		if menor_lista[counter]<menor and menor_lista[counter]!=-1:
			menor=menor_lista[counter]
		counter=counter+1;
	if 	group_pos==-1 and group_pos==-1 and group_pos==-1 and group_pos==-1 and group_pos==-1:
		menor=len(parameters)
	print select_pos;
	print from_pos;
	print where_pos;
	print group_pos;
	print by_pos;
	print for_pos;
	print json_pos;
	print xml_pos;
	print "!";
	if from_pos<0:
		answer=-15;
	else:
		if parameters[1]=='*' and from_pos==2:
			flag=True;
		else:
			counter=1;
			while counter<from_pos:
				#aux.append(parametrers[counter]);
				aux_string=aux_string+parameters[counter];
				aux_string.replace(" ","");
				counter=counter+1;
			
			aux_list=aux_string.split(',');
			counter=0;
			while counter<len(aux_list) and banderisha:
				#print aux_list[counter];
				if is_aggregate_function_or_valid_word(aux_list[counter]):
					flag=True;
				else:
					banderisha=False;
					flag=False;
					answer=-17;
				counter=counter+1;
			
			aux=[];
			if where_pos>-1:
				counter=where_pos+1;
				print "#";
				while banderisha and counter<menor:
					print parameters[counter];
					aux.append(parameters[counter]);
					counter=counter+1;
				banderisha=where_statement_analyzer(aux);
				print "banderisha ";
				print banderisha;
				if banderisha==False:
					answer=-18;
			
		if flag:
			answer=13;
					

	return answer;

	

def is_aggregate_function_or_valid_word(parameters):
	answer=False;
	if len(parameters)>5:
		aux=parameters[0]+parameters[1]+parameters[2]+parameters[3];
		if aux.lower=="min(" or "max(":
			if parameters[len(parameters)-1]==")":
				counter=4;
				aux="";
				while counter<len(parameters)-1:
					aux=aux+parameters[counter];
					counter=counter+1;
		else:
			if len(parameters)>7:
				aux=aux+parameters[4]+parameters[5];
				if aux.lower=="count(":
					if parameters[len(parameters)-1]==")":
						counter=6;
						aux="";
						while counter<len(parameters)-1:
							aux=aux+parameters[counter];
							counter=counter+1;
			else:
				if len(parameters)>9:
					aux=aux+parameters[4]+parameters[5];
					if aux.lower=="count(":
						if parameters[len(parameters)-1]==")":
							counter=6;
							aux="";
							while counter<len(parameters)-1:
								aux=aux+parameters[counter];
								counter=counter+1;
		if reserved_word(aux)==False:
			answer=True;
	return answer;
	
	
def word_finder(words,word):
	counter=0;
	answer=-1;
	flag=True;
	
	while counter < len(words) and flag:
		if words[counter].lower()==word.lower():
			flag=False;
		else:
			counter=counter+1;
	if flag:
		counter=-1;
		
	return counter;
   
'''
Evalua si el parametro de entrada es una palabra reservada.
retorna 1 en caso de que sea palabra reservada
retorna 0 en caso de que no sea palabra reservada
parameters: lista de strings que se analizara para determinar si contiene palabras reservadas.
'''
def reserved_word( parameters):
	reserved_words_list=["select","drop","create","set","database","list","databases","start","get","status","stop","display","set","table","integer","decimal","char","varchar","datetime","null","not","primary","key","alter","add","constraint","foreign","references","index","from","*","where","group","by","for","json","xml","join","count","average","min","max","update","delete","insert","into","values"];
	counter=0;
	answer=False;
	while counter < len(reserved_words_list) and answer==0 :
		if parameters.lower()==reserved_words_list[counter]:
			answer=True;
		counter=counter+1;
	return answer;
	
	
def set_analyzer(expression):
	result=False;
	counter=0;
	aux=[];
	where_pos=0;
	flag=True;
	resultado=False;
	while where_pos<len(expression) and flag:
		if expression[counter].lower()=="where":
			flag=False;
		else:
			where_pos=where_pos+1;
	
	if flag==False :
		
		counter=where_pos+1;
		while counter<len(expression):
			aux.append(expression)
			counter=counter+1
		resultado=where_statement_analyzer(aux);
	if where_pos==1:
		columname="";
		newvalue="";
		counter=0;
		banderisha=False;
		while counter<len(expression[0]):
			if expression[0][counter]=="=" and banderisha==False:
				banderisha=True;
			elif expression[0][counter] != "=" and banderisha == False:
				columname=columname+expression[0][counter];
			elif expression[0][counter] != "=" and banderisha:
				newvalue=newvalue+expression[0][counter];
			else:
				counter=len(expression[0]);
				banderisha=False;
			counter=counter+1;
		r1=reserved_word(columname);
		r2=reserved_word(newvalue);
		if r1==False and r2==False and banderisha and newvalue!="" and columname!="":
			result=True;
		
	elif where_pos==2:
		if expression[0][len(expression[0])-1]=='=':
			counter=0;
			columname="";
			while counter<len(expression[0])-1:
				columname=columname+expression[0][counter]
				counter=counter+1;
			r1=reserved_word(columname);
			r2=reserved_word(expression[1]);
			if r1==False and r2==False and expression[1]!="":
				result=True;
		elif expression[1][0]=='=':
			counter=1;
			columname="";
			while counter<len(expression[0]):
				columname=columname+expression[1][counter]
				counter=counter+1;
			r1=reserved_word(columname);
			r2=reserved_word(expression[0]);
			if r1==False and r2==False and columname!="":
				result=True;
		elif expression[0][0]=='=':
			result==False;
			
	elif where_pos==3:
		if expression[1]=='=':
			if reserved_word(expression[0])==False:
				result=True;
			elif reserved_word(expression[0])==False:
				result=True;
			else:
				result=False;
	
	if flag==False :
		if result==True and resultado==True:
			result=True;
		else:
			result=False;
	
	print result;
	return result;



#Analiza la validez de las where statements	
def where_statement_analyzer(expression):
	counter=0;
	state=0;
	conj=and_or_finder(expression);
	conj_pos=0;
	flag=False;
	error=False;
	respuesta=False;
	while counter<len(expression) and error==False :
		counter2=0;

		if conj_pos<len(conj):
			if counter==conj[conj_pos]:
				state=0;
				flag=False;
				conj_pos=conj_pos+1;
		else:
			while counter2<len(expression[counter]):
				c=expression[counter][counter2];
				if c=='=' or c=='<' or c=='>':
					flag=True;
					if expression[counter][counter2+1] != None:
						c=expression[counter][counter2+1]
						if c=='=' or c=='<' or c=='>':
							if expression[counter][counter2]=='<' and c=='>':
								counter2=counter2+1;
							elif expression[counter][counter2]=='<' and c=='=':
								counter2=counter2+1;
							elif expression[counter][counter2]=='>' and c=='=':
								counter2=counter2+1;
							else:
								error=True;									
				else:
					if flag:
						state=2;
					else:
						state=1;
				counter2=counter2+1;
		counter=counter+1;	
	if error==False and state==2:
		if conj_pos==0 or conj_pos==len(conj):
			respuesta=True;
			
	
	
	print error;
	print state;
	print conj_pos;
	print len(conj);
	print respuesta;
	return respuesta;

def and_or_finder(expression):
	resultado=[];
	pos=0;
	counter=0;
	while counter<len(expression):
		if expression[counter].lower()=="and" or expression[counter].lower()=="or":
			resultado.append(counter);
		counter=counter+1;
	return resultado;
		
		
		
	
