# Advanced Topics in Database Systems -- Data --

## Table of contents
* [Main Dataset](#main-dataset)
* [Secondary Datasets](#secondary-datasets)
* [Setup](#setup)


## Main Dataset

In this project, the main data set used is the crimes in LA from [2010-2019](https://catalog.data.gov/dataset/crime-data-from-2010-to-2019) and from [2020-Present](https://catalog.data.gov/dataset/crime-data-from-2020-to-present). Alternatively, the following links can be used:
* [2010-2019](https://data.lacity.org/Public-Safety/Crime-Data-from-2010-to-2019/63jg-8b9z)
* [2020-Present](https://catalog.data.gov/dataset/crime-data-from-2020-to-present)

In those links, the files are offered in various formats. However, for this project, the .csv versions were used.

> [!NOTE]
> The two files were too big to upload to this repository, this is why the links are provided. The remaining files used in the
> project can be found in this repository

In the project, the queries described in the assignment assume that there is one dataset from 2010-Present. This is why there is a file in the helper-code folder, that combines the two datasets into one big dataset named "Crime_Data_from_2010_to_Present". For 

## Secondary Datasets

* *LA Police stations* : This dataset includes the 21 police stations in LA, which can be accessed [here](https://geohub.lacity.org/datasets/lahub::lapd-police-stations/explore).
> [!NOTE]
> It is worth noting that most of the DIVISIONS match that of the crimes dataset, except for "North Hollywood" and "West Los Angeles", which are "N Hollywood" and "West LA" in the crimes dataset. To ensure an accurate result, both were changed manually in query4.
* *Median Household Income by Zip Code (Los Angeles County)* : this dataset includes the median income per zip code in the LA county. In the /income/ folder one can find the data from [2015](http://www.laalmanac.com/employment/em12c_2015.php), [2017](http://www.laalmanac.com/employment/em12c_2017.php), [2019](http://www.laalmanac.com/employment/em12c_2015.php) and [2021](http://www.laalmanac.com/employment/em12c_2015.php). However, only that of 2015 is required for query 3.
* *Reverse geocoding*: In query 3 we are required to match zip codes from the *Median Household Income by Zip Code (Los Angeles County)* dataset and coordinates from the *LA Police stations* dataset. This is why, we were given a reverse geocoding dataset that matches each coordinate in LA its zip code.

## Setup
The queries found in the /src/queries folder, access the datasets through the HDFS. This is why simply downloading the files locally from the repo/links is not enough. After downloading the files the following steps are required:
1) Create a folder with the different datasets that will be used
2) Run the following command to upload files into the HDFS
   ```
   hadoop fs -put /path/to/local/folder /path/to/save/folder/in/hdfs
   ```
