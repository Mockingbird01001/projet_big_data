## Réalisé par :
OULD OULHADJ Lisa  
BOUFALA Yacine
HAROUN Rayane

## Objectif du projet : 
L'objectif de notre projet est d'analyser l'impact de la santé sur le marché du travail et de la finance en développant une pipeline big data.
  

## Etapes du projet : 

1 - Création de l'image docker: 
	Fichier "docker-compose.yml"

2 - Récupération des fichiers santé.csv, Emploie.csv et Finance.csv :
	- Emploie : https://www.insee.fr/fr/statistiques/series/103167884
	- Santé : lien ----------
	- Finance : lien -----------

3 - Nettoyage des données.

4 - Découpage des 3 fichiers nettoyés (data_emploie.csv, data_finance.csv, data_sante.csv) en plusieurs 		fichiers à l'aide d'un splitter :
	3.1 - création de 3 dossiers destinataires vides :

		Emploie :  data/emploie 
		Finance : data/finance
		Santé : data/sante

    3.2 - Exécution du splitter : 

		fichier "code/splitter.py"

	**Résultat** : le splitter va découpé Les fichiers en plusieurs fichiers de 100 lignes chacun, en précisant les fichiers sources (data_emploie.csv, data_finance.csv, data_sante.csv) et les dossiers destinataires (data/emploie, data/finance, data/sante).

5 - Stockage des dossiers dans HDFS. 

6 - producer qui permet de : 

	1 - lancer Kafka.
	2 - créer nos différents topic.
	3 - envoyer les données dans les topics grâce a la fonction senderProducer(). 
	4 - démarrer des thread Iot pour récupérer les petits fichiers des trois dossiers (Sante, Travail et Finance) grâce a une fonction runProducer.

7 - Traitement des données : 

		- application des traitements(code/traitements.py) avec le consumer (code/consumer.py). 

8 - La visualisation : 

		- mise en place d'un tableau de bord avec streamlit (partie indépendante "Batch")


## Technologies utilisées : 

	- Hadoop
	- Spark
	- Kafka
	- Streamlit

## Exécution du projet : 

1 - docker image :

		docker-compose up 

2 - Splitter : 

		python splitter.py 

3 - IOT : 
	
		python IOT.py

4 - Producer : 
	
		python producer.py

5 - Consumer : 

		python consumer.py

6 - Streamlit : 

		streamlit run dash.py