import sys
sys.path.insert(0, '/home/user/opt/src/helper-code/')
from pyspark.sql import SparkSession
from lib import store_file, store_file_timers
import time


hdfs_file_dir = "hdfs://okeanos-master:54310/"
file_name = sys.argv[0]
file_name = file_name.removeprefix('/home/user/opt/src/queries/')
print(file_name)

#set number of executors and number of cores for spark
num_execs = sys.argv[1]
int_num_execs = int(num_execs)

timer = time.time()
timers_list = [[0]*4]
timers_list[0][3] = int_num_execs

#begin a Spark session
app_name = file_name.removesuffix('.py')+"_"+num_execs+"_num_executors"
spark = SparkSession.builder.appName(app_name)\
      .config("spark.executorEnv.PYTHONPATH", "/home/user/.local/lib/python3.10/site-packages")\
      .getOrCreate()
print('-'*100)
print("Spark session started for", app_name, "!")
print('-'*100)
spark.sparkContext.setLogLevel("WARN")
spark.sparkContext.setLogLevel("WARN")

create_spark_time = time.time() - timer
timers_list[0][0] = (create_spark_time)
#read the crime dataset as a DataFrame
crime_df = (spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option('delimiter', '|')
      .load("hdfs://okeanos-master:54310/project_data/Crime_Data_from_2010_to_Present.csv"))

read_data_time = time.time() - (timer+create_spark_time)
timers_list[0][1] = (read_data_time)
#register the DataFrame as a temporary SQL table
crime_df.createOrReplaceTempView("crime_table")

# Execute SQL queries
result = spark.sql("""
    SELECT
        Year,
        Month,
        CrimeCount,
        Rank
    FROM (
        SELECT
            YEAR(`DATE OCC`) as Year,
            MONTH(`DATE OCC`) as Month,
            COUNT(*) as CrimeCount,
            DENSE_RANK() OVER (PARTITION BY YEAR(`DATE OCC`) ORDER BY COUNT(*) DESC) as Rank
        FROM crime_table
        GROUP BY Year, Month
    ) ranked
    WHERE Rank <= 3
    ORDER BY Year ASC, Rank
""")
query_time = time.time() - (timer+create_spark_time+read_data_time)
timers_list[0][2] = (query_time)

#show the result and save it
result.show(truncate=False)
store_file(
    file_format = 'csv',
    hdfs_URI = hdfs_file_dir,
    folder = 'results/',
    file_name =  file_name.removesuffix('.py') +'_results.csv',
    timers = False,
    spark = spark,
    df = result
)

#print(timers_list)
store_file_timers(
    file_format = 'csv',
    hdfs_URI = hdfs_file_dir,
    folder = 'results/',
    file_name =  file_name.removesuffix('.py') +'_timers.csv',
    timers_list = timers_list,
    spark = spark
)

# Stop the Spark session
spark.stop()
