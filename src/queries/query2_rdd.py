from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import sys
sys.path.insert(0, '/home/user/opt/src/helper-code')
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

create_spark_time = time.time() - timer
timers_list[0][0] = (create_spark_time)

#create the rdd
crime_rdd = (spark.sparkContext.textFile("hdfs://okeanos-master:54310/project_data/Crime_Data_from_2010_to_Present.csv")
             .map(lambda line: line.split("|")))
             #.filter(lambda row: row[15] == "STREET"))  # Assuming 'LocationType' is at index 15

read_data_time = time.time() - (timer+create_spark_time)
timers_list[0][1] = (read_data_time)

# Assuming header is the first line of the file containing column names
header = crime_rdd.first()

#filtered out the rows where row[15] (Premis Desc) is not STREET
filtered_rdd = crime_rdd.filter(lambda row: row[15] == "STREET")


#print the first 5 rows where Premis Desc == 'STREET'
#print("LocationType Column (where STREET is):")
#for line in filtered_rdd.take(5):
#    print(line)

#create a new column 'day_part' based on row[3] (Time OCC)
def map_to_day_part(row):
    time_occ = row[3]  # Assuming 'TIME OCC' is at index 3
    if 500 <= int(time_occ) < 1200:
        return ('Morning', 1)
    elif 1200 <= int(time_occ) < 1700:
        return ('Afternoon', 1)
    elif 1700 <= int(time_occ) < 2100:
        return ('Evening', 1)
    else:
        return ('Night', 1)

#apply the mapping function and reduce by key to count crimes in each 'day_part'
result_rdd = (filtered_rdd
              .map(map_to_day_part)
              .reduceByKey(lambda x, y: x + y)
              .sortBy(lambda x: x[1], ascending=False))

query_time = time.time() - (timer+create_spark_time+read_data_time)
timers_list[0][2] = (query_time)

#print the result
for day_part, crime_count in result_rdd.collect():
    print(f"{day_part}: {crime_count}")

store_file(
    file_format = 'csv',
    hdfs_URI = hdfs_file_dir,
    folder = 'results/',
    file_name =  file_name.removesuffix('.py') +'_results.csv',
    timers = False,
    spark = spark,
    df = result_rdd.toDF()
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
