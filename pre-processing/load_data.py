from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, IntegerType, DoubleType
from pyspark.sql.functions import col, to_date

#begin spark session, using config as given in config file
spark = SparkSession.builder.appName("Load data test").getOrCreate()
config = spark.sparkContext.getConf().getAll()
#print(config)

#init a spark datafrane
df = (spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("hdfs://okeanos-master:54310/project_data/Crime_Data_from_2010_to_2019.csv"))

#print schema as a tree
#df.printSchema()
print("This is the original schema")
schemaString = df._jdf.schema().treeString()
print(schemaString)
print("-"*30)

#change column types

df = df.withColumn("Date Rptd", to_date(df["Date Rptd"], "MM-dd-yyyy").cast(DateType()))
df = df.withColumn("DATE OCC", to_date(df["DATE OCC"], "MM-dd-yyyy").cast(DateType()))
#df.withColumn("DATE OCC",col("DATE OCC").cast(DateType()))
df.withColumn("Vict Age",col("Vict Age").cast(IntegerType()))
df.withColumn("LAT",col("LAT").cast(DoubleType()))
df.withColumn("LON",col("LON").cast(DoubleType()))
print("This is the updated schema")
schemaString = df._jdf.schema().treeString()
print(schemaString)
print("-"*30)
