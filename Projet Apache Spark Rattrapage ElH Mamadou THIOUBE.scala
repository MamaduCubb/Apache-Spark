// Databricks notebook source
// MAGIC %md
// MAGIC **Nom Etudiant :**  THIOUBE
// MAGIC
// MAGIC **Prenom Etudiant:** El Hadji Mamadou   
// MAGIC
// MAGIC **Classe :** Master 1 Intelligence artificielle **promo 2**
// MAGIC
// MAGIC **Date limite de dépot : 10/06/2023 avant 23h 59**
// MAGIC
// MAGIC **Merci de partager le projet dans un repos Git public**
// MAGIC
// MAGIC
// MAGIC # Travail à Faire:
// MAGIC Télécharger le Datasets sur le lien Drive : https://drive.google.com/file/d/1-yxZ7BcPyLXl5uhGTjlFY-7-EL_Ol_yR/view?usp=share_link 
// MAGIC
// MAGIC Repondre les questions ci-dessous avec le maximum de precisions et de détails.   
// MAGIC Remplir `FILL_IN` avec les methodes qui correspondent à la réponse adéquate
// MAGIC
// MAGIC ### Revenus des achats
// MAGIC 1. Extraire les revenus d'achat pour chaque événement
// MAGIC 2. Filtrer les événements dont le revenu n'est pas nul
// MAGIC 3. Vérifiez quels sont les types d'événements qui générent des revenus
// MAGIC 4. Supprimez la colonne inutile
// MAGIC
// MAGIC ### Revenus par Traffic  
// MAGIC Obtenir les 3 sources de trafic générant le revenu total le plus élevé.  
// MAGIC 5. Revenus cumulés par source de trafic  
// MAGIC 7. Obtenir les 3 principales sources de trafic par revenu total  
// MAGIC 6. Nettoyer les colonnes de revenus pour avoir deux décimales  
// MAGIC 8. Sauvegarder les données  
// MAGIC
// MAGIC ### Obtenir les utilisateurs les plus actifs (active_users) par jour
// MAGIC
// MAGIC ### Obtenir le nombre moyen d'utilisateurs actifs par jour de la semaine
// MAGIC
// MAGIC
// MAGIC
// MAGIC
// MAGIC ##### Methods
// MAGIC - DataFrame: `select`, `drop`, `withColumn`, `filter`, `dropDuplicates`,  `groupBy`, `sort`, `limit`
// MAGIC - Column: `isNotNull`, `alias`, `desc`, `cast`, `operators`

// COMMAND ----------

import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder.
                        appName("Projet Spark").
                        config("spark.ui.port", "0").
                        master("local[*]").
                        getOrCreate()

// COMMAND ----------

val eventsPath = "/FileStore/tables/events.json"

// COMMAND ----------

val eventsDF = spark.read.json(eventsPath)
eventsDF.show()

// COMMAND ----------

display(eventsDF)

// COMMAND ----------

// MAGIC %md
// MAGIC Après chargement et affichage des données, on a un base de 10000 lignes.

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1. Extraire les revenus d'achat pour chaque événement
// MAGIC Ajouter une nouvelle colonne **`revenue`** en faisant l'extration de **`ecommerce.purchase_revenue_in_usd`**

// COMMAND ----------

import org.apache.spark.sql.functions._
val revenueDF = eventsDF.withColumn("revenu",col("ecommerce.purchase_revenue_in_usd"))
revenueDF.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2. Filtrer les événements dont le revenu n'est pas null

// COMMAND ----------

val purchasesDF = revenueDF.filter(col("revenu").isNotNull)
purchasesDF.show()
display(purchasesDF)

// COMMAND ----------

// MAGIC %md
// MAGIC Après extraction et filtre des revenus non nul, il y a maintenant 98 lignes dans le tableau qui avait 10000 lignes.

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3. Vérifiez quels sont les types d'événements qui générent des revenus
// MAGIC Trouvez des valeurs **`event_name`** uniques dans **`purchasesDF`**. Il y a deux façons de faire :
// MAGIC - Sélectionnez "event_name" et recupérer les enregistrements distincts

// COMMAND ----------

val distinctDF = purchasesDF.select(col("event_name")).distinct()
distinctDF.show

// COMMAND ----------

// MAGIC %md
// MAGIC On a trouvé finalize qui signifie que tous les données dans cette colonne **event_name** sont les mêmes. Il y a un seul type d'événement. 

// COMMAND ----------

// MAGIC %md
// MAGIC ### 4. Supprimez la colonne inutile
// MAGIC Puisqu'il n'y a qu'un seul type d'événement, supprimez **`event_name`** de **`purchasesDF`**.

// COMMAND ----------

val cleanDF = purchasesDF.drop("event_name")
cleanDF.show
display(cleanDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ### 5. Revenus cumulés par source de trafic
// MAGIC - Obtenir la somme de **`revenue`** comme **`total_rev`**
// MAGIC - Obtenir la moyenne de **`revenue`** comme **`avg_rev`**
// MAGIC
// MAGIC N'oubliez pas d'importer toutes les fonctions intégrées nécessaires.

// COMMAND ----------

val trafficDF = cleanDF.groupBy("traffic_source").agg(sum("revenu").alias("total_rev"),avg("revenu").alias("avg_rev"))
trafficDF.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ### 6. Obtenir les cinqs principales sources de trafic par revenu total

// COMMAND ----------

val topTrafficDF = trafficDF.sort(col("total_rev").desc).limit(5)
topTrafficDF.show()

// COMMAND ----------

// MAGIC %md
// MAGIC On constate que **email** est la principale source de trafic et **instagram** est la 5ème source.

// COMMAND ----------

// MAGIC %md
// MAGIC ### 7. Limitez les colonnes de revenus à deux décimales pointés
// MAGIC - Modifier les colonnes **`avg_rev`** et **`total_rev`** pour les convertir en des nombres avec deux décimales pointés

// COMMAND ----------

val finalDF = topTrafficDF.withColumn("avg_rev",round(col("avg_rev"),2))
                          .withColumn("total_rev",round(col("total_rev"),2))
finalDF.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ### 8. Sauvegarder les données 
// MAGIC Sauvegarder les données sous le format parquet

// COMMAND ----------

finalDF.write.parquet("finaldf.parquet")

// COMMAND ----------

// MAGIC %md
// MAGIC ### 9. Obtenir les utilisateurs les plus actifs (active_users) par jour

// COMMAND ----------

// Convertir la colonne "event_timestamp" en format de date
val activeUsersDF = eventsDF.withColumn("event_date", to_date(from_unixtime(col("event_timestamp")/1000000)))
display(activeUsersDF)

// COMMAND ----------

// Grouper les données par la date et le "user_id" pour compter le nombre d'événements par utilisateur par jour
val activeUsersByDayDF = activeUsersDF.groupBy("event_date", "user_id")
                                     .agg(count("*").alias("event_count"))
// Trier les données par ordre décroissant du nombre d'événements
val sortedActiveUsersDF = activeUsersByDayDF.sort(col("event_count").desc)

// Sélectionner les utilisateurs les plus actifs par jour
val topActiveUsersDF = sortedActiveUsersDF.groupBy("event_date")
                                          .agg(first("user_id").alias("active_users"))

topActiveUsersDF.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ### 10. Obtenir le nombre moyen d'utilisateurs actifs par jour de la semaine 

// COMMAND ----------

// Ajouter une nouvelle colonne "day_of_week" qui représente le jour de la semaine
val activeUsersByDayOfWeekDF = topActiveUsersDF.withColumn("day_of_week", date_format(col("event_date"), "EEEE"))

// Grouper les données par "day_of_week" et compter le nombre unique d'utilisateurs actifs pour chaque jour
val activeUsersCountByDayOfWeekDF = activeUsersByDayOfWeekDF.groupBy("day_of_week")
                                                            .agg(countDistinct("active_users").alias("active_users_count"))

// Calculer la moyenne du nombre d'utilisateurs actifs par jour de la semaine
val avgActiveUsersByDayOfWeekDF = activeUsersCountByDayOfWeekDF.agg(avg("active_users_count").alias("avg_active_users"))

avgActiveUsersByDayOfWeekDF.show()


// COMMAND ----------

// MAGIC %md
// MAGIC ### 11. Sauvegarder les données 
// MAGIC Sauvegarder les données sous le format parquet

// COMMAND ----------

//activeDowDF.FILL_IN
finalDF.write.format("final.parquet")

// COMMAND ----------

// MAGIC %md
// MAGIC ### 12. Industrialiser et Deployer le code source dans le cluster Hadoop  
// MAGIC En s'appuyant sur le code source https://drive.google.com/drive/folders/1AWjgscAdtlpemf-Qj1lEecxg3jvsMSIj?usp=sharing, industrialiser ce notebook en code Spark et le d�ployer job en mode cluster dans HadoopVagrant en stockant le dataframe final dans Hive en format parquet

// COMMAND ----------


