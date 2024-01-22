from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType


#sc = spark.sparkContext
def store_file (file_format: str, hdfs_URI : str, folder : str, file_name : str, timers : bool, spark, df):
    '''
	This is a function that stores the dataframes to the HDFS system set up.

	:file_format: file format that the file will be saved in
    :hdfs_URI: the URI of the hdfs system
    :folder: the folder name inside the hdfs, that the file will be saved in
    :file_name: the name of the file
    :timers: a boolean value that shows if the function called will be to store timers (times for performance measurements) or to store the results
    :spark: spark session created
    :df: the dataframe that will be saved

    If timers is true, then that means that timers will be saved. In this case, to ensure more accurate results, we run each query 5 times.
    When using df.write(), you do not write into a file, rather to a directory with the given path. For this reason, each time we have recorded new measurements
    and we want to save them, we take the existing file, combine the previous and the new measurements into a dataframe and then follow the steps as if it was a
    new dataframe given to be stored.

    Regardless of the timers value, the given dataframe is stored in a temp folder, inside the folder given as a parameter. The writing of the dataframe is done 
    using the coalesce(1) parameter, in order to save the dataframe in one file. Otherwise, it will be saved into multiple files of size 134217728 Bytes.
    Then, inside the temp folder there will be two files, one named "_SUCCESS" and another one named "part....", the latter being the file we want.

    The useful file is being renamed with the named given to the function as a parameter and then moved to the folder as it was initially intended. Therefore, in this stage
    the file has been created, named as intended and saved in the path the user gave. 

    Lastly, the temp folder, along with any files that have been left inside, are deleted.

	'''
    
    #sc            = spark.sparkContext
    URI           = spark.sparkContext._gateway.jvm.java.net.URI
    Path          = spark.sparkContext._gateway.jvm.org.apache.hadoop.fs.Path
    FileSystem    = spark.sparkContext._gateway.jvm.org.apache.hadoop.fs.FileSystem
    Configuration = spark.sparkContext._gateway.jvm.org.apache.hadoop.conf.Configuration
    fs = FileSystem.get(URI(hdfs_URI), Configuration())
    print("Storing file", file_name, "...")
    temp_folder = hdfs_URI+folder+"/temp"

    if timers == True:
        if fs.exists(Path(hdfs_URI+folder+file_name)):
            timers = (spark.read
                        .format("csv")
                        .option("header", "true")
                        .option("inferSchema", "true")
                        .load("hdfs://okeanos-master:54310/results/"+file_name))
            df.show()
            timers.show()
            df = df.union(timers)
            df.show()
            if fs.exists(Path(hdfs_URI+folder+file_name)):
                print("-"*100)
                print("Deleting timers file")
                #fs.delete(Path(hdfs_URI+folder+file_name), True)
            df.coalesce(1).write.format(file_format).mode('overwrite').option("header", True).save(temp_folder)
            #df.coalesce(1).write.option('header', True).csv(temp_folder)
        else:
            df.coalesce(1).write.format(file_format).mode("overwrite").option("header", True).option("compression", "none").save(temp_folder)
    else:
        if fs.exists(Path(hdfs_URI+folder+file_name)):
            print("File", hdfs_URI+folder+file_name, "exists already")
            return
        df.coalesce(1).write.format(file_format).mode("overwrite").option('sep', '|').option("header", True).option("compression", "none").save(temp_folder)
    #df.coalesce(1).write(path + file_name, header = True).mode('overwrite').format(file_format)
    "checkpoint3"
    
    if fs.exists(Path(temp_folder)):
        print("   1.Created a temporary folder to save the new file")
    destination_folder = hdfs_URI+folder


    temp_folder_files = fs.listStatus(Path(temp_folder))
    
    #Find the partxxx.csv file and rename it with desired name
    temp_file_name = None
    for file_iter in temp_folder_files:
        curr_file_name = file_iter.getPath().getName()
        if curr_file_name.endswith("."+file_format):
            print("   2.Found the desired file with name: ", curr_file_name)
            target_temp_file = curr_file_name
            fs.rename(Path(temp_folder, target_temp_file), Path(temp_folder, file_name))
            break
    
    #move file from temp folder to desired folder
    if fs.exists(Path(destination_folder+file_name)):
        fs.delete(Path(destination_folder+file_name), True)
    fs.rename(Path(temp_folder, file_name), Path(destination_folder, file_name))
    if fs.exists(Path(destination_folder+file_name)):
        print("   3.Moved successfully the file")
    
    # Delete all other files in the temp folder
    temp_file_name = None
    print('   4.Deleting unnecessary files')
    for file_iter in temp_folder_files:
        temp_file_name = file_iter.getPath().getName()
        if not temp_file_name.endswith('.'+file_format):
            fs.delete(Path(temp_folder, temp_file_name), True)

    # Delete the temp folder itself
    if not fs.listStatus(Path(temp_folder)):
        fs.delete(Path(temp_folder), True)
        print('   5.Delete temp folder')
    print('-'*100)
    return

def store_file_timers (file_format: str, hdfs_URI : str, folder : str, file_name : str, timers_list : list, spark):
    '''
	This is a function that takes a list of the timers recorded, along with the names of the columns. A dataframe is created and then store_file function is
    being called, as if the timers is any other dataframe to be saved.

    Note: as this function is made for the timers, the "timers" parameter of store_files has to be True

	:file_format: file format that the file will be saved in
    :hdfs_URI: the URI of the hdfs system
    :folder: the folder name inside the hdfs, that the file will be saved in
    :file_name: the name of the file
    :timers_list: a list with the times recorded
    :spark: spark session created

	'''

    
    #timers_columns = ['Spark creation time', 'Data read time', 'Query time']
    timers_schema = StructType([
    StructField("Spark creation time", DoubleType(), True),
    StructField("Data read time", DoubleType(), True),
    StructField("Query time", DoubleType(), True),
    StructField('Number of executors', IntegerType(), True)
    ])
    timers_df = spark.createDataFrame(data = timers_list, schema = timers_schema)
    store_file(
        file_format = file_format,
        hdfs_URI = hdfs_URI,
        folder = folder,
        file_name = file_name,
        timers = True,
        spark = spark,
        df = timers_df
    )

    return
