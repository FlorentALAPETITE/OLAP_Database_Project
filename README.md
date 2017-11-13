# Projet de réalisation d'un entrepôt de données OLAP


Pour lancer le projet, dans un terminal :
* $ make

La commande **make** lance le script de création de l'entrepôt de données puis compile et execute le programme Java qui réalise l'intégration des données.


Pour executer les requêtes OLAP d'interrogation de l'entrepôt, dans un terminal :
* make ExecuteQueries
* L'execution des requêtes fourni en sortie un fichier queryResult.txt.


Pour executer les requêtes OLAP et avoir les résultats sous forme de fichier CSV, dans un terminal :
* make ExecuteCSVQueries
* L'execution fourni en sortie un dossier comprenant un fichier .csv par requête exectuée.