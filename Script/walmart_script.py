#1. Instancier le client spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as F

spark=SparkSession.builder\
                .master("local[*]")\
                .appName("walmark_stock")\
                .getOrCreate()

#q2:Importation du fichier csv
df=spark.read\
    .option("header",True)\
    .csv("walmart_stock.csv")

#Affichage des trois premières lignes
df.show(3)

#q6: Le jour  correspondant au prix le plus élevé
df.orderBy(col("High")\
  .desc())\
  .select(col("Date"))\
  .head(1)

#Transformer le dataframe en table
df.createOrReplaceTempView("stock")

# En sql
print("*************************************************** En sql***************************************")
spark.sql(""" select Date 
            from stock 
            order By High desc   """)\
.show(1)

#q7: moyenne de close
df.select(avg(col("Close")).alias("Moyenne_close")).show()
#En sql
print("*************************************************** En sql***************************************")
spark.sql(""" select mean(Close) as Moyenne_close
                    from stock """)\
.show()

#q8: :max et min de Volume
df.select(min(col("Volume")).alias("Minimum_volume"),
        max(col("Volume")).alias("Maximum_volume")).show()
#En sql
print("*************************************************** En sql***************************************")
spark.sql(""" select min(Volume) as Minimum_volume,
            max(Volume) as Maximum_volume 
            from stock""").show()

#q9: Nombre de jours dont Close est < 60
df.filter("Close < 60").select(F.count("Date").alias("Nombre_de_jour")).show()

#sql
print("*************************************************** En sql***************************************")
spark.sql(""" select count(Date) as Nombre_de_jour
            from stock 
            where Close < 60""").show()

#q10: pourcentage du nombre de jours dont "High" > 80
count=df.filter("High > 80").count()*100/df.count()
print(count)
#En sql
print("*************************************************** En sql***************************************")
spark.sql(""" select count(Date)*100/(select count(*) 
            from stock) as Pourcentage
            from stock 
            where High > 80""").show()


#q11: selection du max(High) par année
df.groupBy(year(col("Date")).alias("year"))\
    .agg(max(col("High")).alias("Max_high_per_year"))\
    .orderBy(col("year"))\
    .show()


#En sql 
print("*************************************************** En sql***************************************")
spark.sql(""" select year(Date) as Annee,
            max(High) as Max_high_per_year 
            from stock  
            group by (Annee) 
            order by Annee""").show()


#q12: Moyenne close par mois
df.select(col("Date"),col("Close"))\
  .withColumn("mois",F.when(month(col("Date"))==1,F.lit("Janvier"))\
                .when(month(col("Date"))==2,F.lit("Février"))\
                .when(month(col("Date"))==3,F.lit("Mars"))\
                .when(month(col("Date"))==4,F.lit("Avril"))\
                .when(month(col("Date"))==5,F.lit("Mai"))\
                .when(month(col("Date"))==6,F.lit("Juin"))\
                .when(month(col("Date"))==7,F.lit("Juillet"))\
                .when(month(col("Date"))==8,F.lit("Août"))\
                .when(month(col("Date"))==9,F.lit("Septembre"))\
                .when(month(col("Date"))==10,F.lit("Octobre"))\
                .when(month(col("Date"))==11,F.lit("Novembre"))\
                .otherwise(F.lit("Décembre")))\
  .groupBy(year(col("Date")).alias("Annee"),col("mois"))\
  .agg(F.mean(col("Close")).alias("Average_close"))\
  .orderBy(year(col("Date")))\
  .show(5)
print("*************************************************** En sql***************************************")        
#q12: En sql
spark.sql(""" select year(Date) as Annee,
    case  month(Date) 
         WHEN 1 THEN 'janvier'
         WHEN 2 THEN 'février'
         WHEN 3 THEN 'mars'
         WHEN 4 THEN 'avril'
         WHEN 5 THEN 'mai'
         WHEN 6 THEN 'juin'
         WHEN 7 THEN 'juillet'
         WHEN 8 THEN 'août'
         WHEN 9 THEN 'septembre'
         WHEN 10 THEN 'octobre'
         WHEN 11 THEN 'novembre'
         ELSE 'décembre'
   END as mois,
   avg(Close) as Average_close 
   from stock 
   group by Annee,month(Date) 
   order by Annee, month(Date)""")\
.show(5)

