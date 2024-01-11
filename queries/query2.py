from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, when, count
from pyspark.sql.utils import HadoopConfiguration

# Create a Spark session

spark = SparkSession.builder.appName("query2").getOrCreate()

# Read the crime dataset
crime_df = spark.read.parquet("your_dataset_path")

# Consider only crimes that happened on the STREET
crime_df = crime_df.filter(col("Premis Desc") == "STREET")

# Extract the hour, handling cases without leading zeros
crime_df = crime_df.withColumn("hour_of_day", expr("cast(substring(lpad(TIME OCC, 4, '0'), 1, 2) as int)"))

# Define parts of the day
crime_df = crime_df.withColumn("day_part", 
                               expr("CASE WHEN hour_of_day >= 500 AND hour_of_day < 1200 THEN 'Morning'"
                                    " WHEN hour_of_day >= 1200 AND hour_of_day < 1700 THEN 'Afternoon'"
                                    " WHEN hour_of_day >= 1700 AND hour_of_day < 2100 THEN 'Evening'"
                                    " ELSE 'Night' END"))

# Group by day_part and count the crimes
result = crime_df.groupBy("day_part").agg(count("*").alias("crime_count"))

# Select only relevant columns for display
result_display = result.select("day_part", "crime_count")

# Sort the result based on crime_count
result_display = result_display.orderBy("crime_count", ascending=False)

# Show the result without displaying the crime_count column
result_display.show(truncate=False)

# Stop the Spark session
spark.stop()
