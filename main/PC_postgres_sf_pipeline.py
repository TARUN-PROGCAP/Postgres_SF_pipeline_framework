from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import *
from pyspark import SparkConf, SparkContext

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.jars", "/path_to_postgresDriver/postgresql-42.2.5.jar") \
    .getOrCreate()

df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://<POSTGRES_ENDPOINT>:5432/databasename") \
    .option("dbtable", "tablename") \
    .option("user", "username") \
    .option("password", "password") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df.printSchema()

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config('spark.jars','/Users/Hana/spark-sf/snowflake-jdbc-3.12.9.jar,/Users/Hana/spark-sf/spark-snowflake_2.12-2.8.1-spark_3.0.jar') \
    .getOrCreate()
    
sfOptions = {
 "sfSource" : "net.snowflake.spark.snowflake",
 "sfURL" : "wa29709.ap-south-1.aws.snowflakecomputing.com",
 "sfAccount" : "xxxxxxx",
 "sfUser" : "xxxxxxxxx",
 "sfPassword" : "xxxxxxxx",
 "sfDatabase" : "learning_db",
 "sfSchema" : "public",
 "sfWarehouse" : "compute_wh",
 "sfRole" : "sysadmin",
}

df.write.format(“snowflake”).options(**sfOptions).option(“dbtable”, “emp_dept”).mode(‘append’).options(header=True).save()
