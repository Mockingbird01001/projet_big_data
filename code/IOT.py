#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Nov  4 15:22:48 2021
@author: mockingbird
"""

from subprocess import run
from os import listdir
from threading import Thread
from platform import system
from time import sleep

dicoPath = {
    "SANTE"   : "../data/sante/"  , 
    "EMPLOIE" : "../data/emploie/" , 
    "FINANCE" : "../data/finance/"
}

dicoToPath = {
    "SANTE"   : "/home/projet-BD/work/data/sante"  , 
    "EMPLOIE" : "/home/projet-BD/work/data/emploie" , 
    "FINANCE" : "/home/projet-BD/work/data/finance"
}

class IOT:
    def __init__(self):
        self.run = True
        self.timer = 1 # le temps d'attente avant chaque envoie en (s)
        self.fileCount = 1
        
    def codeExec(self, pathFile, fileId, newFolder, linux=True):
        if linux: # for linux
            return "sudo docker cp {}/file-{}.csv projetbd_spark-master_1:{}".format(pathFile, fileId, newFolder)
        else: # for windows
            return "docker cp {}/file-{}.csv projetbd_spark-master_1:{}".format(pathFile, fileId, newFolder)
        
    def iotSender(self, folder, newFolder):
        while self.run:
            if len(listdir(folder))+1 != self.fileCount :
                try:
                    run(self.codeExec(folder, self.fileCount, newFolder, system()), shell=True, check=True)
                    print("Data file {} Sent evry {}s -- 200 OK".format(self.fileCount, self.timer))
                    self.fileCount += 1
                    sleep(self.timer)
                except: 
                    self.run = False
                    print("Bad authentification happend !"); 
            else: 
                self.run = False
                print("All files sent !"); 
            
if __name__ == "__main__":
    """ Creation des threads """
    sante_iot = Thread(target = IOT().iotSender, args=(dicoPath['SANTE'], dicoToPath['SANTE'],))
    empoie_iot = Thread(target = IOT().iotSender, args=(dicoPath['EMPLOIE'], dicoToPath['EMPLOIE'],))
    finance_iot = Thread(target = IOT().iotSender, args=(dicoPath['FINANCE'], dicoToPath['FINANCE'],))
    
    """ Lancement des threads IOTs """
    sante_iot.start()
    empoie_iot.start()
    finance_iot.start()
    
    """ Attendre que tou les threads terminent """
    sante_iot.join()
    empoie_iot.join()
    finance_iot.join()