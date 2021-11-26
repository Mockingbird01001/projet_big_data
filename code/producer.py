#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 22 10:49:25 2021

@author: mockingbird
"""
import pandas as pd
from kafka import KafkaProducer    
from threading import Thread
from json import dumps
from time import sleep
import os

dicoToPath = {
    "SANTE"   : "/home/projet-BD/work/data/sante"  , 
    "EMPLOIE" : "/home/projet-BD/work/data/emploie" , 
    "FINANCE" : "/home/projet-BD/work/data/finance"
}
 
class Producer:
    def __init__(self):
        self.server = 'kafka:9093'
        self.producer = KafkaProducer(bootstrap_servers = self.server)
        
        self.topic_sante = "projetBD_sante-topic"
        self.topic_emploie = "projetBD_emploie-topic"
        self.topic_finance = "projetBD_finance-topic"
        self.run = True
        
    def senderProducer(self, path, topic):
        while self.run:
            for fileID in range(len([name for name in os.listdir('.') if os.path.isfile(name)])):
                data = pd.read_csv("{}/file-{}.csv".format(path, fileID+1)).to_dict(orient='records')
                self.producer.send(topic, dumps(data).encode('utf-8'))
                print(data)
                sleep(1)
                    
    def runProducer(self):
        santeProducer = Thread(target = self.senderProducer, args=(dicoToPath['SANTE'], self.topic_sante, ))
        emploieProducer = Thread(target = self.senderProducer, args=(dicoToPath['EMPLOIE'], self.topic_emploie, ))
        financeProducer = Thread(target = self.senderProducer, args=(dicoToPath['FINANCE'], self.topic_finance, ))
         
        """ Lancement des threads IOTs """
        santeProducer.start()
        emploieProducer.start()
        financeProducer.start()
        
        """ Attendre que tou les threads terminent """
        santeProducer.join()
        emploieProducer.join()
        financeProducer.join()
        
    def shutdown(self):
        self.run = False    
        
if __name__ == "__main__":
    Producer().runProducer()
