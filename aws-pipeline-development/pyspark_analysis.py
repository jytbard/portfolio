# load dependencies
from pyspark.sql.types import LongType, StringType, StructField, StructType, BooleanType, ArrayType,
IntegerType
import re
import pandas as pd
from pyspark.sql.functions import col


# Read the data file from NiFi from HDFS into Pyspark
df_load =
spark.read.format("csv").option("delimiter",",").option("header","false").load("s3://mynifioutput3105/1aaa6c26-7
76d-47ca-b8e3-0f5ced5d8aab.txt")
df_load.show(5)


# We can compute the statistics by calling .describe() on the column of df_load. The .describe()
# function returns the count, mean, stddev, min, and max of a given column.
statistics_df=df_load.select(col("_c4").alias("number_injured")).describe()
statistics_df.toPandas()

# Which borough occurred how many times during accidents- total distinct number of boroughs
borough_freq_df =
(df_load.select(col("_c10").alias("borough")).groupBy('borough').count().sort('borough').cache())
print('Total distinct Boroughs:', borough_freq_df.count())


# Count of boroughs individual
borough_freq_pd_df = (borough_freq_df.toPandas().sort_values(by=['count'],ascending=False))
borough_freq_pd_df


#Top 20 frequent on_street_name
paths_df = (df_load.select(col("_c3").alias("on_street_name")).groupBy('on_street_name').count().sort('count',
ascending=False).limit(20))
paths_pd_df = paths_df.toPandas()
paths_pd_df


# Top 3 vehicle types excluding sedan - which are responsible for accidents
notsedan_df = (df_load.filter(df_load['_c9'] != 'Sedan'))

freq_vehicle_df =
(notsedan_df.select(col("_c9").alias("vehicle_type")).groupBy('vehicle_type').count().sort('count',
ascending=False).limit(5))
freq_vehicle_df.show(truncate=False)


# Number of unique daily collisions
from pyspark.sql import functions as F


# only showing top 5 rows
collision_day_df = df_load.select(df_load._c0.alias('collision_id'), F.dayofmonth('_c1').alias('day'))
collision_day_df.show(5, truncate=False)


# Write data to S3
final_df =
df_load.select(df_load._c0.alias('collision_id'),df_load._c1.alias('crash_date'),df_load._c2.alias('crash_time')
,df_load._c10.alias('borough'),,df_load._c3.alias('on_street_name'),df_load._c4.alias('number_of_persons_i
njured'),df_load._c5.alias('number_of_persons_killed'),df_load._c9.alias('vehicle_type'))
final_df.coalesce(1).write.format("csv").save("s3://mynifioutput3105/pyspark_results")
