#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
GUI del proyecto SQLantro
Autor: Frander Granados V.
email: frandergv@gmail.com
'''

import webbrowser

from gi.repository import Gtk

class Window:

    def bt_about(self, widget, data=None):
        webbrowser.open('https://github.com/Zyoruk/TheSQL')        

builder = Gtk.Builder()
builder.add_from_file("gui.glade")
builder.connect_signals(Window())

window = builder.get_object("gui")
window.show_all()

Gtk.main()

