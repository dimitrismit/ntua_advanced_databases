from pyspark.sql import SparkSession
from geopy.distance import geodesic
from pyspark.sql import functions as F
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import col, year, when
from pyspark.sql.types import DoubleType
import sys
from pyspark.sql.window import Window
import sys
sys.path.insert(0, '/home/user/opt/src/helper-code')
from lib import store_file, store_file_timers

# calculate the distance between two points [lat1,long1], [lat2,long2] in km
@F.udf(DoubleType())
def get_distance (lat1, long1, lat2, long2) :
      return geodesic((lat1,long1),(lat2,long2)).km

hdfs_file_dir = "hdfs://okeanos-master:54310/"
file_name = sys.argv[0]
file_name = file_name.removeprefix('/home/user/opt/src/queries/')
print(file_name)
hint_type = sys.argv[2]
mode = sys.argv[3]

#set number of executors and number of cores for spark
num_execs = sys.argv[1]
int_num_execs = int(num_execs)

#start a spark session
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

#create a dataframe from the crime data
crime_df = (spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option('delimiter', '|')
      .load("hdfs://okeanos-master:54310/project_data/Crime_Data_from_2010_to_Present.csv"))

#filter out rows where LAT or LON is equal to 0
crime_df = crime_df.filter((col("LAT") != 0) & (col("LON") != 0))

#count crimes where "Weapon Used Cd" starts with 1, in order to keep crimes
#where the weapon was a firearm
crimes_with_firearms = crime_df.filter(col("Weapon Used Cd").startswith("1"))

'''
Note: There is a difference in how the two datasets refer to some divisions
This is why these division names were manually changed to match so that data 
would not be lost during the join operation
'''
crimes_with_firearms = crimes_with_firearms.withColumn(
        "AREA NAME",
        when(col("AREA NAME") == 'N Hollywood', 'NORTH HOLLYWOOD')\
        .when(col("AREA NAME") == 'West LA', 'WEST LOS ANGELES')\
        .otherwise(col("AREA NAME"))
)
crimes_with_firearms = crimes_with_firearms.withColumn("AREA NAME", F.upper(col("AREA NAME")))
#crimes_with_firearms.select('DIVISION').distinct().show()

#create a dataframe from the LAPD police stations data
police_dpt_df = (spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("hdfs://okeanos-master:54310/project_data/LAPD_Police_Stations.csv"))

#keep the "X", "Y", "DIVISION" columns as these are the relevant columns for this query
## This is done to save memory and time (due to the cross join that follows)
police_dpt_df = police_dpt_df.select("X", "Y", "DIVISION")

#cross join the two dataframes
if hint_type != 'None' and mode != 'None':
      joined_df = crimes_with_firearms.crossJoin(police_dpt_df.hint(hint_type))
      joined_df.explain(mode = mode)
else:
      joined_df = crimes_with_firearms.crossJoin(police_dpt_df)

#calculate distance for each pair of crime location and police department
joined_df = joined_df.withColumn("distance", get_distance('LAT','LON','Y','X'))

#define a window to rank distances for each crime
window_spec = Window.partitionBy("DR_NO").orderBy("distance")

#rank the distances for each crime
ranked_df = joined_df.withColumn("rank", F.row_number().over(window_spec))

#filter for rows where rank is 1 (minimum distance)
nearest_place_df = ranked_df.filter("rank = 1").select("DR_NO", "DIVISION", "distance")

if hint_type != 'None' and mode != 'None':
      result_df = joined_df.join(nearest_place_df.hint(hint_type), "DR_NO", "left")
      result_df.explain(mode = mode)
else:
      result_df = joined_df.join(nearest_place_df, "DR_NO", "left")

#Group by 'year' and calculate the mean distance and count of rows
avg_per_year_df = result_df.groupBy(year("DATE OCC").alias("year")).agg(
      F.mean("distance").alias("average_distance"),
      F.count("*").alias("crime_count")
).orderBy("Year")

avg_per_year_df.show()

# Calculate average distance and count of crimes per police department
avg_per_division_df = result_df.groupBy("DIVISION").agg(
    F.mean("distance").alias("average_distance"),
    F.count("*").alias("crime_count")
).orderBy("crime_count", ascending=False)

avg_per_division_df.show()

if hint_type == 'None' and mode == 'None':
      store_file (
            file_format = 'csv',
            hdfs_URI = hdfs_file_dir,
            folder = 'results/',
            file_name =  file_name.removesuffix('.py') +'_a_results.csv',
            timers = False,
            spark = spark,
            df = avg_per_year_df
      )

      store_file (
            file_format = 'csv',
            hdfs_URI = hdfs_file_dir,
            folder = 'results/',
            file_name =  file_name.removesuffix('.py') +'_b_results.csv',
            timers = False,
            spark = spark,
            df = avg_per_division_df
      )

spark.stop()
