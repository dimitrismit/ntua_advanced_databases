from pyspark.sql import SparkSession
#from pyspark.conf import SparkConf
from pyspark.sql.types import DateType, IntegerType, DoubleType
from pyspark.sql.functions import col, to_date


#code taken from https://medium.com/towards-data-engineering/pyspark-write-a-data-frame-with-the-specific-file-name-994b91d7f77d
#sc = spark.sparkContext
def store_file (file_format: str, hdfs_URI : str, folder : str, file_name : str, df, sc):
    
    print("checkpoint1")
    URI           = sc._gateway.jvm.java.net.URI
    Path          = sc._gateway.jvm.org.apache.hadoop.fs.Path
    FileSystem    = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    Configuration = sc._gateway.jvm.org.apache.hadoop.conf.Configuration
    fs = FileSystem.get(URI(hdfs_file_dir), Configuration())
    print("checkopoint2")
    temp_folder = hdfs_URI+folder+"/temp"
    df.coalesce(1).write.format(file_format).mode("overwrite").option("header", True).option("compression", "none").save(temp_folder)
    #df.coalesce(1).write(path + file_name, header = True).mode('overwrite').format(file_format)
    "checkpoint3"
    
    if fs.exists(Path(temp_folder)):
        print("Created a temporary folder to save the new file")
    destination_folder = hdfs_URI+folder


    temp_folder_files = fs.listStatus(Path(temp_folder))
    
    #Find the partxxx.csv file and rename it with desired name
    temp_file_name = None
    for file_iter in temp_folder_files:
        curr_file_name = file_iter.getPath().getName()
        if curr_file_name.endswith("."+file_format):
            print("Found the desired file with name: ", curr_file_name)
            target_temp_file = curr_file_name
            fs.rename(Path(temp_folder, target_temp_file), Path(temp_folder, file_name))
            break
    
    #move file from temp folder to desired folder
    fs.rename(Path(temp_folder, file_name), Path(destination_folder, file_name))
    if fs.exists(Path(destination_folder+file_name)):
        print("Moved successfully the file")
    
    # Delete all other files in the temp folder
    temp_file_name = None
    for file_iter in temp_folder_files:
        temp_file_name = file_iter.getPath().getName()
        if not temp_file_name.endswith('.'+file_format):
            fs.delete(Path(temp_folder, temp_file_name), True)

    # Delete the temp folder itself
    if not fs.listStatus(Path(temp_folder)):
        fs.delete(Path(temp_folder), True)
    return 


hdfs_file_dir = "hdfs://okeanos-master:54310/"

#begin spark session, using config as given in config file
spark = SparkSession.builder.appName("Load data test").getOrCreate()
print("Spark session started!")
#config = spark.sparkContext.getConf().getAll()
#conf = SparkConf()
#print(config)
#conf.get("spark.master")

#init a spark datafrane
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
#df_2010_to_2019.distinct()
#df_2010_to_present.distinct()

#print schema as a tree
#df.printSchema()
print("This is the original schema")
schemaString = df_2010_to_2019._jdf.schema().treeString()
print(schemaString)
print("-"*30)

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
print("-"*30)

df_2020_to_present.withColumn("Date Rptd", to_date(df_2020_to_present["Date Rptd"], "MM-dd-yyyy").cast(DateType()))
df_2020_to_present.withColumn("DATE OCC", to_date(df_2020_to_present["DATE OCC"], "MM-dd-yyyy").cast(DateType()))
#df.withColumn("DATE OCC",col("DATE OCC").cast(DateType()))
df_2020_to_present.withColumn("Vict Age",col("Vict Age").cast(IntegerType()))
df_2020_to_present.withColumn("LAT",col("LAT").cast(DoubleType()))
df_2020_to_present.withColumn("LON",col("LON").cast(DoubleType()))

#Combine both files into one big file
df = df_2010_to_2019.union(df_2020_to_present)
print("Number of rows is: ", df.count())

#this is not working yet, check video on chrome
store_file('csv', hdfs_file_dir, 'project_data', 'Crime_Data_from_2010_to_Present.csv', df, spark.sparkContext)
