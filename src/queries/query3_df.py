import sys
sys.path.insert(0, '/home/user/opt/src/helper-code')
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, year, month, count, dense_rank, desc
from pyspark.sql.functions import col, regexp_extract, regexp_replace
from pyspark.sql.types import IntegerType
from lib import store_file, store_file_timers
import time

hdfs_file_dir = "hdfs://okeanos-master:54310/"
file_name = sys.argv[0]
file_name = file_name.removeprefix('/home/user/opt/src/queries/')
print(file_name)
hint_type = sys.argv[2]
mode = sys.argv[3]
print(mode)

#set number of executors and number of cores for spark
num_execs = sys.argv[1]
int_num_execs = int(num_execs)

timer = time.time()
timers_list = [[0]*4]
timers_list[0][3] = int_num_execs

#begin a Spark session
if hint_type != 'None':
      app_name = file_name.removesuffix('.py')+"_"+hint_type
else:
      app_name = file_name.removesuffix('.py')+"_"+num_execs+"_num_executors"

spark = SparkSession.builder.appName(app_name)\
    .config("spark.executorEnv.PYTHONPATH", "/home/user/.local/lib/python3.10/site-packages")\
    .config("spark.dynamicAllocation.enabled", "false")\
    .config("spark.executor.instances", int_num_execs)\
    .getOrCreate()

print('-'*100)
print("Spark session started for", app_name, "!")
print('-'*100)
spark.sparkContext.setLogLevel("WARN")

create_spark_time = time.time() - timer
timers_list[0][0] = (create_spark_time)

# Read the CSV file into a DataFrame
crime_df = (spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option('delimiter', '|')
      .load("hdfs://okeanos-master:54310/project_data/Crime_Data_from_2010_to_Present.csv"))

# Read the CSV file into a DataFrame
incomes_df = (spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("hdfs://okeanos-master:54310/project_data/income/LA_income_2015.csv"))

# Read the CSV file into a DataFrame
revgeo_df = (spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("hdfs://okeanos-master:54310/project_data/revgecoding.csv"))

read_data_time = time.time() - (timer+create_spark_time)
timers_list[0][1] = (read_data_time)

# Filter out rows where "Vict Descent" is not empty in the crime_data csv
crime_df = crime_df.filter(crime_df["Vict Descent"].isNotNull())

# Filter data for the year 2015
crime_2015_df = crime_df.filter(year("DATE OCC") == 2015)

# Rename the "ZIPcode" column in coordinates_df to "Zip Code"
revgeo_df = revgeo_df.withColumnRenamed("ZIPcode", "Zip Code")

# Define a regular expression pattern to extract the first set of digits
pattern = r'^(\d+)'

# Apply the regexp_extract function to the "zip_code" column
revgeo_df = revgeo_df.withColumn(
    "Zip Code",
    regexp_extract(col("Zip Code"), pattern, 1)
)

# Perform a left semi-join based on "Zip Code" column, 
if hint_type != 'None' and mode != 'None':
    real_incomes_df = incomes_df.join(revgeo_df.hint(hint_type), "Zip Code", "left_semi")
    real_incomes_df.explain(mode = mode)
else:
    real_incomes_df = incomes_df.join(revgeo_df, "Zip Code", "left_semi")


# Select necessary columns for both top and bottom salary DataFrames
top_zipcodes_df = real_incomes_df.orderBy(col("Estimated Median Income").desc()).limit(3).select("Zip Code")
bottom_zipcodes_df = real_incomes_df.orderBy(col("Estimated Median Income")).limit(3).select("Zip Code")

# Show the resulting DataFrames
#print("Top 3 Zip Codes with Highest Salaries:")
#top_zipcodes_df.show(truncate=False)

#print("Top 3 Zip Codes with Lowest Salaries:")
#bottom_zipcodes_df.show(truncate=False)

# Combine the two DataFrames into one
zipcodes_df = top_zipcodes_df.union(bottom_zipcodes_df)

#Show the resulting combined DataFrame
#print("Combined DataFrame:")
#zipcodes_df.show(truncate=False)

# Perform a left semi-join based on "Zip Code" column
#filtered_revgeo_df = revgeo_df.join(zipcodes_df, "Zip Code", "left_semi")
if hint_type != 'None' and mode != 'None':
    filtered_revgeo_df = revgeo_df.join(zipcodes_df.hint(hint_type), "Zip Code", "left_semi")
    filtered_revgeo_df.explain(mode = mode)
else:
    filtered_revgeo_df = revgeo_df.join(zipcodes_df, "Zip Code", "left_semi")

# Show the resulting DataFrame
#filtered_revgeo_df.show(truncate=False)

# Left semi join between crime_2015_df and filtered_revgeo_df based on Latitude and Longitude
#result_df = crime_2015_df.join(filtered_revgeo_df, ["LAT", "LON"], "left_semi")
if hint_type != 'None' and mode != 'None':
    result_df = crime_2015_df.join(filtered_revgeo_df.hint(hint_type), ["LAT", "LON"], "left_semi")
    result_df.explain(mode = mode)
else:
    result_df = crime_2015_df.join(filtered_revgeo_df, ["LAT", "LON"], "left_semi")

# Group by "Vict Descent" and count the number of victims
victim_counts_df = result_df.groupBy("Vict Descent").agg(count("*").alias("Victim Count"))

# Order the result in descending order based on "Victim Count"
victim_counts_df = victim_counts_df.orderBy(desc("Victim Count"))

query_time = time.time() - (timer+create_spark_time+read_data_time)
timers_list[0][2] = (query_time)

# Show the resulting DataFrame
victim_counts_df.show(truncate=False)
if hint_type == 'None' and mode == 'None':
      store_file(
      file_format = 'csv',
      hdfs_URI = hdfs_file_dir,
      folder = 'results/',
      file_name =  file_name.removesuffix('.py') +'_results.csv',
      timers = False,
      spark = spark,
      df = victim_counts_df
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
