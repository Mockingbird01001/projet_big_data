#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 22 11:28:49 2021

@author: mockingbird
"""
import pandas as pd
from threading import Thread
from kafka import KafkaConsumer
import json
from time import sleep

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType, LongType


class Consummer:
    def __init__(self):
        self.server = 'localhost:9092'
        
        self.topic_sante = "projetBD_sante-topic"
        self.topic_emploie = "projetBD_emploie-topic"
        self.topic_finance = "projetBD_finance-topic"
        
        self.spark = SparkSession.builder.master("local[1]").appName('projetBD__').getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")
        
        self.consumer_sante = KafkaConsumer(self.topic_sante, auto_offset_reset='earliest', enable_auto_commit=True, group_id='my-group-1', bootstrap_servers=self.server)
        self.consumer_emploie = KafkaConsumer(self.topic_emploie, auto_offset_reset='earliest', enable_auto_commit=True, group_id='my-group-2', bootstrap_servers=self.server)
        self.consumer_finance = KafkaConsumer(self.topic_finance, auto_offset_reset='earliest', enable_auto_commit=True, group_id='my-group-3', bootstrap_servers=self.server)
        
        self.schema_sante = StructType([ \
            StructField("ID",StringType(), True), \
            StructField("dep",IntegerType(), True), \
            StructField("sexe",IntegerType(), True), \
            StructField("Date", IntegerType(), True), \
            StructField("Hospitalisation", IntegerType(), True), \
            StructField("Reanimation", IntegerType(), True), \
            StructField("HospConv", FloatType(), True), \
            StructField("Radio", IntegerType(), True), \
            StructField("Deces", IntegerType(), True), \
            StructField("code_region", IntegerType(), True), \
            StructField("incid_reanimation", IntegerType(), True) \
        ])
            
        self.schema_emploie = StructType([ \
            StructField("ID",IntegerType(), True), \
            StructField("libelle_zone_d_emploi",StringType(), True), \
            StructField("code_region",IntegerType(), True), \
            StructField("region",StringType(), True), \
            StructField("code_zone_d_emploi",IntegerType(), True), \
            StructField("zone_d_emploi",StringType(), True), \
            StructField("Date",IntegerType(), True), \
            StructField("effectifs_salaries_brut",IntegerType(), True), \
                StructField("effectifs_salaries_cvs",IntegerType(), True), \
            StructField("masse_salariale_brut",LongType(), True), \
            StructField("masse_salariale_cvs",LongType(), True), \
        ])
            
        self.schema_finance = StructType([ \
            StructField("ID", StringType(), True),\
            StructField("type", StringType(), True),\
            StructField("natureObjetMarche", StringType(), True),\
            StructField("referenceCPV", StringType(), True),\
            StructField("datePublicationDonnees", StringType(), True),\
            StructField("Montant", FloatType(), True),\
            StructField("formePrix", StringType(), True),\
            StructField("nomAcheteur", StringType(), True),\
            StructField("Latitude_Acheteur", FloatType(), True),\
            StructField("Longitude_Acheteur", FloatType(), True),\
            StructField("categorieEtablissement", StringType(), True),\
            StructField("distanceAcheteurEtablissement", FloatType(), True),\
            StructField("Latitude_Etablissement", FloatType(), True),\
            StructField("Longitude_Etablissement", FloatType(), True),\
            StructField("date", IntegerType(), True)\
        ])
    
    def consume_loop(self, consumer, topic, schema):
        for msg in consumer:
            data = pd.DataFrame(json.loads(msg.value))
            df = self.spark.createDataFrame(data = data, schema = schema)
            df.show(10)
            
            if topic == self.topic_sante :
                print("< nombre de cas par date >")
                df.groupby(df.date).sum().collect()
                
                print("< total des déces>")
                df.agg({"Deces" :"sum"}).collect()
                
                print("< total des admis en reanimation par sexe>")
                df.groupBy(df.sexe).agg({"Reanimation" :"sum"}).collect()
                
                print("< total des admis en reanimation par date>")
                df.groupBy(df.date).agg({"Reanimation" :"sum"}).collect()
                
                print("< moyenne des déces par sexe>")
                df.groupBy(df.sexe).agg({"Deces" :"mean"}).collect()
                
                print("< total des déces par region>")
                df.groupBy(df.code_region).agg({"Deces" :"sum"}).collect()
    
            if topic == self.topic_emploie:
                print("< correlation entre masse_salariale_brut et effectifs_salaries_brut >")
                df.corr("masse_salariale_brut","effectifs_salaries_brut").collect()
                
                print("< total par date >")
                df.groupby(df.Date).sum().collect()
                
                print("< moyenne de masse_salariale_brut >")
                df.agg({"masse_salariale_brut" : "mean"}).collect()
                
                print("< moyenne de masse_salariale_brut par region >")
                df.groupBy(df.region).agg({"masse_salariale_brut":"mean"}).collect()
    
            if topic == self.topic_finance :
                print("< total par date>")
                df.groupby(df.date).sum().collect()
            
            sleep(5)
        
    def runConsumer(self):
         """ Creation des threads """
         santeConsumer = Thread(target = self.consume_loop, args=(self.consumer_sante, self.topic_sante, self.schema_sante, ))
         emploieConsumer = Thread(target = self.consume_loop, args=(self.consumer_emploie, self.topic_emploie, self.schema_emploie, ))
         financeConsumer = Thread(target = self.consume_loop, args=(self.consumer_finance, self.topic_finance, self.schema_finance, ))
         
         """ Lancement des threads IOTs """
         santeConsumer.start()
         emploieConsumer.start()
         financeConsumer.start()
        
         """ Attendre que tou les threads terminent """
         santeConsumer.join()
         emploieConsumer.join()
         financeConsumer.join()
    
if __name__ == "__main__":
    Consummer().runConsumer()