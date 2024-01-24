# Advanced Topics in Database Systems -- Queries --

## Table of contents
* [General Information](#general-information)
* [Pre-processing](#pre-processing)
* [Running the queries](#running-the-queries)
* [Query parameters](#query-parameters)

## General information

The query description, along with other project information, can be found in the "advanced_db_project.pdf" file. Since the queries have been written for Spark, each query can be individually executed using 
```
spark-submit /path/to/query/query.py
```
Spark-submit generally requires some configuration information, however, most of the necessary configurations are already set up in the /spark/config/spark-defaults.conf. Some configurations need to be passed as parameters when submitting the Python file.

## Pre-processing

Before executing the queries, the /helper-code/pre-process.py needs to be executed. This Python code combines the two separate datasets of LA crimes (one is 2010-2019 and the other is 2020-Present) if it does not already exist. If it does exist, we do not need to write it again. Then the pre_process.py file checks if there are data before 2010 and filters them out and also checks for duplicates. After the filtering stage, it saves it in the HDFS. 

Running the file can be done using:
```
spark-submit /path/to/folder/pre_process.py
```


## Running the queries

Running all of the queries can be done using the "run_queries.sh" file which does the following:
1) Remove the results from the HDFS, if any
2) The first three queries, for which we are required to keep track of the query time, are being submitted 5 times for more accurate results.
3) Submit query4_1.py and query4_2.py
4) Get the results from the HDFS, store them locally, and plot the query times of the first three queries

In the assignment, there is a question that requires to use of join operations using different hints. For that reason, the file "run_hint_and_explain.sh" is used, which runs queries 3 and 4, using all the different hints and explain modes and stores each different combination of hint and mode in a different file.

## Query parameters

* Query 1: for the first query when submitting the file we also need the number of spark executors
* Query 2: for the second query when submitting the file we also need the number of spark executors
* Query 3: for the third query when submitting the file we also need
  * The number of spark executors
  * A string that signifies the hint used for joins
  * A string that signifies the mode of the join explanation
* Query 4: for the third query when submitting the file we also need
  * The number of spark executors
  * A string that signifies the hint used for joins
  * A string that signifies the mode of the join explanation
 
> [!NOTE]
> For queries 3 and 4, one can use None for the second and the third parameter, when it is desired to execute the program
> without any hints for the join operation. In that case, the program will calculate the result and save it to a file in HDFS.
> When these parameters are not 'None' (the hint and the mode to be used), it is advised to redirect the
> output to a file, as it is done in the "run_hint_and_analyze.sh".

> [!NOTE]
> The output file of the query results uses "|" as separator, while timers uses "," as separator. If you want to change the separator for the results file, you can use change_separator.py inside
> the helper-code folder. For this to work you need to create a different folder, where you paste all four folders from the results folder.
> Therefore, when the change_separator.py finishes, there will be the original folder with the results (where some csv files will have | delimiter) and another folder, where "," will be the separator for all csv files.
