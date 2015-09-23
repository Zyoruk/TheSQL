     #!/usr/bin/python
# -*- coding: utf-8 -*-
'''
GUI del proyecto SQLantro
Autor: Frander Granados V.
email: frandergv@gmail.com
'''

#imports necesarios para el programa
import webbrowser, subprocess, code, os, threading, locale
from gi.repository import Gtk, Gdk
from sqlantro_processor import * 

encoding = locale.getpreferredencoding()
utf8conv = lambda x : unicode(x, encoding).encode('utf8')
     
def bt_about(self, widget):#Boton para about, nos abre el browser con el repositorio
    webbrowser.open('https://github.com/Zyoruk/TheSQL')

def env_clic(self, view, buff, inicio):#Funcion que nos manda el comando
    command = buff.get_text(buff.get_iter_at_line(buff.get_line_count()), buff.get_end_iter(),include_hidden_chars=True)
    f = os.popen(command)#mandamos el comando al sistema
    now = f.read()#realizamos la lectura del comando
    now=execute(command);#ejecutamos el comando 
    textbuffer.insert(buff.get_end_iter(),utf8conv('\n' + now + '\n'))#Se imprime la respuesta del comando

builder = Gtk.Builder()#Creamos el constructor que nos importa el archivo glade con la interfaz
builder.add_from_file("gui.glade")#se agrega el arhivo .glade

textview = builder.get_object("textview")#se construye el cuadro de texto
textbuffer = textview.get_buffer()#Se crea un buffer del texto para leer los datos
inicio = textbuffer.get_start_iter()#Se crea un puntero para las lineas leidas

about = builder.get_object("about")#se construye el boton About
about.connect('clicked', bt_about, None)#Se conecta el evento del clic

enviar = builder.get_object("enviar")#se construye el boton enviar
enviar.connect("clicked", env_clic, textview, textbuffer, inicio)#Se conecta el evento del clic

def call(widget, event):#Funcion para el evento del boton "Enter"
    if event.keyval == 65293:#codigo para verificar que es el boton de enter
        env_clic(None,textview, textbuffer, inicio)#ejecutamos el comando

textview.connect("key-press-event", call)#conectamos el evento

window = builder.get_object("gui")#definimos el nombre de la ventana
window.set_default_size(580, 500)#se definen valores por defecto del largo y ancho de la ventana
window.connect("delete-event", Gtk.main_quit)#Se cierra el programa

window.show_all()#se muestra todo
Gtk.main()#se levanta el programa

