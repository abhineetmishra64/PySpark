import pyspark
import pandas
from pyspark.sql import SparkSession

print(pandas.read_csv("Book1.csv")) #  type will be pandas.core.dataframe.dataframe
spark = SparkSession.builder.appName("Abhineet").getOrCreate() # it will create spark session
print(spark)


df_pyspark = spark.read.csv("Book1.csv") #it will read c0 and c1 as default coloumn name
print(df_pyspark)
print(df_pyspark.show())
df_pyspark = spark.read.option('header','true').csv("Book1.csv", inferSchema=True) #it will change the row name as the first row  in the csv
print(df_pyspark)
print(df_pyspark.show())
df_pyspark.printSchema()
# type will be pyspark.sql.dataframe.dataframe