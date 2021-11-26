#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Nov 18 11:59:08 2021

@author: mockingbird
"""
from csv import reader, writer
from os import path, remove
from threading import Thread

sourceFile = {
    "SANTE" : "../data_archive/data_sante.csv",
    "EMPLOIE" : "../data_archive/data_emploie.csv" , 
    "FINANCE" : "../data_archive/data_finance.csv"
}
    
destinationFolder = {
    "SANTE"   : "../data/sante/"  , 
    "EMPLOIE" : "../data/emploie/" , 
    "FINANCE" : "../data/finance/"
}

class Spliter:
    def __init__(self):
        self.Lignes = 100
        self.fileId = 1
        self.nameFile = "file-"
        
    def split_csv(self, source_filepath, dest_folder):
        if self.Lignes <= 0: raise Exception('records_per_file must be > 0')
    
        with open(source_filepath, 'r') as source:
            read = reader(source)
            
            records_exist = True
    
            while records_exist:
                i = 0
                target_filepath = path.join(dest_folder, f'{self.nameFile}{self.fileId}.csv')
    
                with open(target_filepath, 'w') as target:
                    writ = writer(target)
                    while i < self.Lignes:
                        if i == 0: writ.writerow(next(read))
                        try:
                            writ.writerow(next(read))
                            i += 1
                        except:
                            records_exist = False
                            break
                if i == 0:
                    # we only wrote the header, so delete that file
                    remove(target_filepath)
                self.fileId += 1
                
if __name__ == "__main__":
    
    sante_splitter = Thread(target = Spliter().split_csv, args=(sourceFile['SANTE'], destinationFolder['SANTE'],))
    empoie_splitter = Thread(target = Spliter().split_csv, args=(sourceFile['EMPLOIE'], destinationFolder['EMPLOIE'],))
    finance_splitter = Thread(target = Spliter().split_csv, args=(sourceFile['FINANCE'], destinationFolder['FINANCE'],))
    
    """ Lancement des threads IOTs """
    sante_splitter.start()
    empoie_splitter.start()
    finance_splitter.start()
    
    """ Attendre que tou les threads terminent """
    sante_splitter.join()
    empoie_splitter.join()
    finance_splitter.join()