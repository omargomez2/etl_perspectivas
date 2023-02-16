# -----------------------
# Author: Omar G.
# Date: Feb 2023
# Description: Apache Spark ETL implementation for processing csv data from the OJS.
# -----------------------

# Import Libraries
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.functions import count, year


spark = SparkSession.builder.appName('Perspectivas').config('spark.sql.debug.maxToStringFields', 2000).config('spark.driver.extraClassPath','C:/Users/omarg/Documents/spark-3.3.1-bin-hadoop3/jars/postgresql-42.5.2.jar').getOrCreate()
#spark = SparkSession.builder.appName('Perspectivas').config('spark.sql.debug.maxToStringFields', 2000).config('spark.driver.extraClassPath','/omar/Downloads/spark-3.2.3-bin-hadoop3.2/jars/postgresql-42.5.2.jar').getOrCreate()

dataFile = 'articles-RP-20230212.csv'
dataFileR = 'reviews-20230212.csv'

df_papers = spark.read.options(header='True', delimiter=',', encoding='UTF-8', inferSchema='True', multiLine='True', wholeFile='True').csv(dataFile).cache() 
df_papers.createOrReplaceTempView('papers')
df_papers = spark.sql('select * from papers where isnotnull(`Fecha de envío`)')
df_papers.createOrReplaceTempView('papers')
df_papers.printSchema()


df_reviews = spark.read.options(header='True', delimiter=',', encoding='UTF-8', inferSchema='True', multiLine='True', wholeFile='True').csv(dataFileR).cache() 
df_reviews.createOrReplaceTempView('reviews')
#df_reviews = spark.sql('select * from reviews where isnotnull(`Fecha de envío`)')
#df_papers.createOrReplaceTempView('reviews')
df_reviews = df_reviews.drop('Comentarios sobre el envío')
df_reviews.printSchema()


rows = df_papers.count()
print(f"DataFrame Rows count: {rows}")

#-- Enviados total envs+revisión+publicados+rechazados
df_env = spark.sql('select date_part(\'Year\', `Fecha de envío`) as `año`, count(*) as enviado' +
    ' from papers group by `año` order by `año`')
df_env = df_env.withColumn('part',lit(1))
df_env.createOrReplaceTempView('enviados')
df_env.show()

df_env = spark.sql('select `año`, enviado, enviado - lag(enviado,1) over (partition by `part` order by `año`) as env_d from enviados')
df_env.createOrReplaceTempView('enviados')
df_env.show()


#-- Publicados
df_pub = spark.sql('select date_part(\'Year\', `Fecha de envío`) as `año`, count(*) as publicado' +
    ' from papers where Estado like \'Publicado\' group by `año` order by `año`')
df_pub = df_pub.withColumn('part',lit(2))
df_pub.createOrReplaceTempView('publicados')
df_pub.show()

df_pub = spark.sql('select `año`, publicado, publicado - lag(publicado,1) over (partition by `part` order by `año`) as pub_d from publicados')
df_pub.createOrReplaceTempView('publicados')
df_pub.show()


#-- Rechazados
df_rech = spark.sql('select date_part(\'Year\', `Fecha de envío`) as `año`, count(*) as rechazado' +
    ' from papers where Estado like \'Rechazado\' group by `año` order by `año`')
df_rech = df_rech.withColumn('part',lit(3))
df_rech.createOrReplaceTempView('rechazados')
df_rech.show()

df_rech = spark.sql('select `año`, rechazado, rechazado - lag(rechazado,1) over (partition by `part` order by `año`) as rech_d from rechazados')
df_rech.createOrReplaceTempView('rechazados')
df_rech.show()



df_mart = spark.sql('select e.`año`, e.enviado as enviados, e.env_d, p.publicado as publicados, p.pub_d, r.rechazado as rechazados,'+
    ' r.rech_d from enviados as e left join publicados as p on e.`año` = p.`año` left join rechazados as r on e.`año` = r.`año`')
df_mart.createOrReplaceTempView('resumen')

df_mart = spark.sql('select `año`, enviados, env_d, publicados, pub_d, rechazados,'+
    ' rech_d, round(publicados/enviados*100) as tasa_a from resumen')
df_mart = df_mart.withColumn('part',lit(4))
df_mart.createOrReplaceTempView('resumen')
df_mart.show()



#-- Resumen de papers
df_mart = spark.sql('select `año`, enviados, env_d, publicados, pub_d, rechazados, rech_d, tasa_a,'+
    ' tasa_a - lag(tasa_a,1) over (partition by `part` order by `año`) as tasa_d from resumen')
df_mart.show()


#-- Papers activos, quitar diferencia de días, calcular en streamlit
df_activo = spark.sql('select `Id. del envío` as id, `Título` as `título`, `Apellidos (Autor/a 1)` as autor, date(`Fecha de envío`) as `envío`,'+ 
    ' Estado as `estado`, date(`Última modificación`) as `ult. modificación`,'+
    ' `Decisión del editor/a 1  (Editor/a 1)` as `decisión`, date(`Fecha decidida 1  (Editor/a 1)`) as `fecha decisión` from papers where Estado like \'Revi%\''+
    ' or Estado like \'Env%\' or Estado like \'Produ%\' or Estado like \'Edi%\'')
df_activo.createOrReplaceTempView('activos')
df_activo.show()


#-- Palabras clave de publicados
df_keywords = spark.sql('select `Palabras clave` as `palabras clave` from papers where Estado like \'Publicado\' and isnotnull(`palabras clave`)')
df_keywords.show()


#-- Agrupados por estado
df_papers.groupBy('estado').agg(count('*').alias('count')).show()


#--Revisores
df_reviews.groupBy('Revisor/a').agg(count('*').alias('count')).orderBy('count', ascending=False).show(200)



#-- Calcular diferencia de días en streamlit
df_revs = spark.sql('select `ID del envío` as id, `Revisor/a` as revisor, date(`Fecha asignada`) as asignada, date(`Fecha confirmada14`) as confirmada,' +
	'  date(`Fecha completada`) as completado from reviews'
	' group by id, revisor, asignada, confirmada, completado order by revisor')
df_revs.createOrReplaceTempView('agrupado_revs')
df_revs.show(200)

df_revs_act = spark.sql('select a.id, a.`título`, a.autor, a.`envío`, a.estado, a.`decisión`, a.`fecha decisión`, r.revisor, r.asignada, r.completado '+
	' from activos as a left join agrupado_revs as r on a.id = r.id where a.estado like \'Revi%\' or a.estado like \'Env%\' '+
	' or a.estado like \'Produ%\' or a.estado like \'Edi%\'')
df_revs_act.show(100)


#df_reviews.dropDuplicates(["Fecha asignada"]).select("ID del envío","Revisor/a","Nombre","Fecha asignada","Fecha completada","Sin considerar").show(100)

url_db='jdbc:postgresql://localhost:5432/dbtest'
usr_db='postgres'
usr_pass='postgres'

df_mart.write.format('jdbc').options(
        url=url_db,
        driver='org.postgresql.Driver',
        dbtable='datamart',
        user=usr_db,
        password=usr_pass).mode('overwrite').save()
        
#df_activo.write.format('jdbc').options(
#        url=url_db,
#        driver='org.postgresql.Driver',
#        dbtable='activos',
#        user=usr_db,
#        password=usr_pass).mode('overwrite').save()
        
df_keywords.write.format('jdbc').options(
        url=url_db,
        driver='org.postgresql.Driver',
        dbtable='keywords',
        user=usr_db,
        password=usr_pass).mode('overwrite').save()
        
df_revs_act.write.format('jdbc').options(
        url=url_db,
        driver='org.postgresql.Driver',
        dbtable='activos_rev',
        user=usr_db,
        password=usr_pass).mode('overwrite').save()
