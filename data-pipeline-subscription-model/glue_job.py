import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, coalesce, to_date, expr, date_format, sum
import boto3
from pyspark.sql import DataFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session.builder.appName("***").config("spark.sql.legacy.timeParserPolicy", "LEGACY").getOrCreate()
s3_client = boto3.client('s3')

def readCSV(spark, file):
    df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(file)
    df.show()
    cols = [x.name.lower() for x in df.schema.fields]
    
    non_date_cols = [y for y in cols if y[0] >= 'a' and y[0] <= 'z']
    date_cols = [y for y in cols if y[0] >= '0' and y[0] <= '9']
    
    date_cols_val = ",".join(date_cols)
    
    stack_exp = ",".join([f"'{x}',`{x}`" for x in date_cols])
    stack_exp = f"stack({len(date_cols)},{stack_exp}) as (dates,value)"
    
    df.createOrReplaceTempView("df")
    
    df_final = spark.sql("""REMOVED for Confidentiality""")
    df_final.createOrReplaceTempView("df_final")
    
    return spark.sql("""REMOVED for Confidentiality""")

activeMembersDf=readCSV(spark, "s3://***.csv")

deathDf=readCSV(spark,"s3a://***.csv")

outputPath = "s3a://***processedData"

def get_bucket_name_and_key(s3_path):
    bucket_name, key = s3_path.replace("s3a://", "").split("/", 1)
    file_key = key[0:]
    return bucket_name, file_key

def get_s3_object_count(file_path):
    bucket_name, key = get_bucket_name_and_key(file_path)
    key_count = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=key)['KeyCount']
    return key_count

def import_df(spark, active_df, inactive_df, output_path):
    active_df.createOrReplaceTempView("activeMembersDf")
    inactive_df.createOrReplaceTempView("inactiveMembersDf")
    
    spark.sql("""REMOVED for Confidentiality
                """).createOrReplaceTempView("sourceDf")

    obj_count = get_s3_object_count(output_path)
    if obj_count == 0:
        return spark.sql("""REMOVED for Confidentiality""")
    else:
        spark.read.parquet(output_path).createOrReplaceTempView("outputDf")
        return spark.sql("""REMOVED for Confidentiality
                            """)

inputDf=import_df(spark, activeMembersDf, inactiveMembersDf, outputPath)

def exportDf(spark, inputDf, outputPath) -> None:
    inputDf.write.mode('append').parquet(outputPath)
    
exportDf(spark,inputDf,outputPath)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()