Exercices : Traitement parallèle et distribué avec Spark 
Exercice 1 : 
On considère le graphe suivant qui représente le lignée des RDDS :  Le premier RDD (RDD1) est créé en parallélisant une collection contenant des noms des étudiants. Ecrivez une application Spark qui permet de réaliser ce 
lignage.
![image](https://user-images.githubusercontent.com/80116765/164466557-2d33255d-539c-4923-b905-f5e3e56fa6ba.png)
![image](https://user-images.githubusercontent.com/80116765/164466595-79858070-b621-48ff-a9ce-d6b36ac1e0d2.png)

 Résultat:
 
 ![image](https://user-images.githubusercontent.com/80116765/164466732-8c6f24df-bb56-4622-bfc5-2297b6bdb634.png)

Exercice 2 : 
1. On souhaite développer une application Spark permettant, à partir d’un fichier texte (ventes.txt) en entré, contenant les ventes d’une entreprise dans les différentes villes, de déterminer le total des ventes par ville. La structure du fichier ventes.txt est de la forme suivante : 
date ville produit prix 
Vous testez votre code en local avant de lancer le job sur le cluster.
 ![image](https://user-images.githubusercontent.com/80116765/164466764-f6157e10-c837-44b7-9fb4-9103e175491d.png)

Input: 

 ![image](https://user-images.githubusercontent.com/80116765/164466805-a5c78dd9-9824-4078-b8e3-5aafd10e0e08.png)

Résultat:

 ![image](https://user-images.githubusercontent.com/80116765/164466835-2ca4c23a-e249-4e96-a1e7-b06d045426bf.png)

2. Vous créez une deuxième application permettant de calculer le prix total des ventes des produits par ville pour une année donnée.
 ![image](https://user-images.githubusercontent.com/80116765/164466867-acb2257d-f428-4320-960d-39a36e2e0561.png)

Résultat: 

 ![image](https://user-images.githubusercontent.com/80116765/164466951-d4997e92-297a-4269-86b7-22ed1df83c8f.png)

Exercice 3 : 
Nous souhaitons, dans cet exercice d’analyser les données météorologiques fournies par NCEI (National Centers for Environmental Information) à l'aide de Spark. 
Mr. Abdelmajid BOUSSELHAM 1 
Big Data: Fondements et Architectures de stockage 2022 
Le jeu de données est mis à la disposition du public par le NCEI. L'ensemble de données séparé par année peut être téléchargé à partir du lien suivant : Index of /pub/data/ghcn/daily/by_year (noaa.gov). 
L'ensemble de données contient les attributs ID, date, element, value, m-flag, q flag, s-flag et OBS-TIME. Vous trouverez ci-dessous des informations sur ses attributs. 
• ID = 11-character station identification code 
• YEAR/MONTH/DAY = 8-character date in YYYYMMDD format (e.g. 19860529 = May 29, 1986) 
• ELEMENT = 4-character indicator of element type 
• DATA VALUE = 5-character data value for ELEMENT 
• M-FLAG = 1-character Measurement Flag 
• Q-FLAG = 1-character Quality Flag 
• S-FLAG = 1-character Source Flag
• OBS-TIME = 4-character time of observation in hour-minute format (i.e. 0700 =7:00 am) 
Afficher les statistiques suivantes pour 2020 : 
Input: 1750.csv 
• Température minimale moyenne. 
 
 
• Température maximale moyenne.

![image](https://user-images.githubusercontent.com/80116765/164466966-d0215cd9-41ba-4710-8d37-e0899be632ee.png)
![image](https://user-images.githubusercontent.com/80116765/164466987-5e78eff4-d745-4c1e-9a58-a61f1d90f078.png)

  
• Température maximale la plus élevée. 

  ![image](https://user-images.githubusercontent.com/80116765/164467058-4180a494-2c26-4398-8c41-ebb8d1d46df5.png)
![image](https://user-images.githubusercontent.com/80116765/164467100-b06a198d-a477-4185-8af4-0bb83f6bfec6.png)

• Température minimale la plus basse.
![image](https://user-images.githubusercontent.com/80116765/164470166-9224df22-5746-4bb3-a88c-159df1ef5c70.png)
![image](https://user-images.githubusercontent.com/80116765/164470392-ab927e5b-d096-432f-9a42-9c43c2f51823.png)


• Top 5 des stations météo les plus chaudes. 
![image](https://user-images.githubusercontent.com/80116765/164470487-75601c3b-807c-4e62-9794-7ae094630222.png)
![image](https://user-images.githubusercontent.com/80116765/164470595-e237a6e4-caa1-4a7b-850b-2427c24554e4.png)


• Top 5 des stations météo les plus froides. 
![image](https://user-images.githubusercontent.com/80116765/164470667-5625c2d5-b8a3-43dd-8128-155741a9d8a8.png)
![image](https://user-images.githubusercontent.com/80116765/164470756-c32437db-e0a7-4128-a23b-f38b6d66bacc.png)


Mr. Abdelmajid BOUSSELHAM 2
