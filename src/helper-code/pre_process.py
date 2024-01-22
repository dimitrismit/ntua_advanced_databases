from pyspark.sql import SparkSession
#from pyspark.conf import SparkConf
from pyspark.sql.types import DateType, IntegerType, DoubleType
from pyspark.sql.functions import col, to_date
from lib import store_file
from pyspark.sql.functions import lit, create_map
from itertools import chain
import sys

hdfs_file_dir = "hdfs://okeanos-master:54310/"
file_name = sys.argv[0]
file_name.removeprefix('/home/user/opt/src/helper-code/')

#begin spark session
spark = SparkSession.builder.appName("Load data test").getOrCreate()
print('-'*100)
print("Spark session started!")
print('-'*100)
spark.sparkContext.setLogLevel("WARN")
#config = spark.sparkContext.getConf().getAll()
#conf = SparkConf()
#print(config)
#conf.get("spark.master")

sc_variables = {
    'sc' : spark.sparkContext,
    'FileSystem' : spark.sparkContext._gateway.jvm.org.apache.hadoop.fs.FileSystem,
    'URI' : spark.sparkContext._gateway.jvm.java.net.URI,
    'Path' : spark.sparkContext._gateway.jvm.org.apache.hadoop.fs.Path,
    'Configuration' : spark.sparkContext._gateway.jvm.org.apache.hadoop.conf.Configuration
}

#create a dataframe for each file
df_2010_to_2019 = (spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(hdfs_file_dir + "project_data/Crime_Data_from_2010_to_2019.csv"))

df_2020_to_present = (spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(hdfs_file_dir + "project_data/Crime_Data_from_2020_to_Present.csv"))

#Remove duplicate rows
df_2010_to_2019.distinct()
df_2020_to_present.distinct()

#print schema as a tree
#df.printSchema()
print("This is the original schema")
schemaString = df_2010_to_2019._jdf.schema().treeString()
print(schemaString)
print("-"*100)

#change column types

df_2010_to_2019.withColumn("Date Rptd", to_date(df_2010_to_2019["Date Rptd"], "MM-dd-yyyy").cast(DateType()))
df_2010_to_2019.withColumn("DATE OCC", to_date(df_2010_to_2019["DATE OCC"], "MM-dd-yyyy").cast(DateType()))
#df.withColumn("DATE OCC",col("DATE OCC").cast(DateType()))
df_2010_to_2019.withColumn("Vict Age",col("Vict Age").cast(IntegerType()))
df_2010_to_2019.withColumn("LAT",col("LAT").cast(DoubleType()))
df_2010_to_2019.withColumn("LON",col("LON").cast(DoubleType()))
print("This is the updated schema")
schemaString = df_2010_to_2019._jdf.schema().treeString()
print(schemaString)
print("-"*100)

df_2020_to_present.withColumn("Date Rptd", to_date(df_2020_to_present["Date Rptd"], "MM-dd-yyyy").cast(DateType()))
df_2020_to_present.withColumn("DATE OCC", to_date(df_2020_to_present["DATE OCC"], "MM-dd-yyyy").cast(DateType()))
#df.withColumn("DATE OCC",col("DATE OCC").cast(DateType()))
df_2020_to_present.withColumn("Vict Age",col("Vict Age").cast(IntegerType()))
df_2020_to_present.withColumn("LAT",col("LAT").cast(DoubleType()))
df_2020_to_present.withColumn("LON",col("LON").cast(DoubleType()))

#This is the dataframe that combines the other two dataframes
df = df_2010_to_2019.union(df_2020_to_present)

print("Number of rows of original merged file is: ", df.count())

#Now that we have the merged dataframe ready we will do some operations to our data
#Firstly, we check if there are data before 2010
df.filter(df['DATE OCC'] < '2010-01-01') 
print("Number of rows after filtering out crimes before 2010 is : ", df.count())

#Then we check for duplicates
df.dropDuplicates()
print("Number of rows after filtering out duplicates is : ", df.count())

#We will change Vict Descent letters to the respective strings (eg. B - Black)
#This is necessary for query 3
descent_dict = {
    'A' : 'Other Asian',
    'B' : 'Black',
    'C' : 'Chinese',
    'D' : 'Cambodian',
    'F' : 'Filipino',
    'G' : 'Guamanian',
    'H' : 'Hispanic/Latin/Mexican',
    'I' : 'American Indian/Alaskan Native',
    'J' : 'Japanese',
    'K' : 'Korean',
    'L' : 'Laotian',
    'O' : 'Other',
    'P' : 'Pacific Islander',
    'S' : 'Samoan',
    'U' : 'Hawaiian',
    'V' : 'Vietnamese',
    'W' : 'White',
    'X' : 'Unknown',
    'Z' : 'Asian Indian'
}
mapping_expr = create_map([lit(x) for x in chain(*descent_dict.items())])
df = df.withColumn('Vict Descent', mapping_expr[df['Vict Descent']])
df.select('Vict Descent').show(10)
print('-'*100)

#Combine both files into one big file
#First check if merged file already exists
# -> if yes, do not create it again, for faster development
# -> if no, create it
fs = sc_variables['FileSystem'].get(sc_variables['URI'](hdfs_file_dir), sc_variables['Configuration']())
if not fs.exists(sc_variables['Path'](hdfs_file_dir+'project_data/Crime_Data_from_2010_to_Present.csv')):
    print('The merged file did not exist')
    print('Starting file saving...')
    store_file(
        file_format = 'csv',
        hdfs_URI = hdfs_file_dir,
        folder = 'project_data',
        file_name = 'Crime_Data_from_2010_to_Present.csv',
        timers = False,
        spark = spark,
        df = df
        )
    if fs.exists(sc_variables['Path'](hdfs_file_dir+'project_data/Crime_Data_from_2010_to_Present.csv')):
        print("Merged file has been created successfully")
else: 
    print("Merged file exists")
    df = (spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option('delimiter', '|')
      .load(hdfs_file_dir + "project_data/Crime_Data_from_2010_to_Present.csv"))

df.show(3 ,truncate = True)