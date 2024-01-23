# Advanced Topics in Database Systems

## Table of contents
* [General Information](#general-information)
* [Technologies](#technologies)
* [Cluster configuration](#cluster-configuration)
* [Setup](#setup)
* [Dataset](#dataset)

## General Information
**Team Members:** <br> 
Dimitris Mitropoulos, el18608 <br>
Dimitris Houpas, el18116

**Subject:** <br>
This is a project for 'Advanced Topics in Database Systems', National Technical University of Athens for the winter semester 2023-2024.

## Technologies 
The project is created with:
* OpenJDK 11.0.21 ![Java](https://img.shields.io/badge/java-%23ED8B00.svg?style=for-the-badge&logo=openjdk&logoColor=white)
* Apache Hadoop 3.3.6 ![Apache Hadoop](https://img.shields.io/badge/Apache%20Hadoop-66CCFF?style=for-the-badge&logo=apachehadoop&logoColor=black)
* Apache Spark 3.3.1 ![Apache Spark](https://img.shields.io/badge/Apache%20Spark-FDEE21?style=flat-square&logo=apachespark&logoColor=black)
* Python 3.10.12: ![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)
  * py4j==0.10.9.7
  * geopy==2.4.1
  * pandas==2.2.0 ![Pandas](https://img.shields.io/badge/pandas-%23150458.svg?style=for-the-badge&logo=pandas&logoColor=white)
  * pyspark==3.5.0

> [!NOTE]  
> Python modules should be downloaded on both nodes, for the imports to work properly. In addition, for some queries,
> in the creation of the spark session, there is a config parameter that points to the location of the python
> packages on each node. This should be changed accordingly

## Cluster configuration:
For this project, we set up a cluster, comprised of two Virtual Machines, one had the role of the Master and the other of the Worker. The resources for those two VMs were on the Okeanos, GRNET's cloud service. The configuration is as follows:

| OS  |  CPUs | RAM | Disk space | 
| :------------: | :------------: | :------------: | :------------: |
| Ubuntu 22.04.3 LTS | 4 |  8GB | 30GB | 

| VM  |  Local Net IPv4 |
| :------------: |  :------------: |
| Master  |  192.168.0.1 | 
| Worker  |  192.168.0.2 |

Each VM was also assigned a public IPv6 address, while the master was assigned a public IPv4 address. For the communication of the two VMs a local network was set up, using the IPv4 addresses mentioned above. <br>

| **HDFS** |  **HDFS** | **Spark** | **Spark** | 
| :--------: |  :--------: | :--------: | :--------: |
| *Master node* | *Worker node* | *Master node* | *Worker node* |
| datanode | datanode | master | worker |
| namenode |          | worker |        |


## Setup 
To install and configure the cluster in okeanos, the instructions [here](https://colab.research.google.com/drive/1pjf3Q6T-Ak2gXzbgoPpvMdfOHd1GqHZG?usp=sharing#scrollTo=I0jwIL1Ba-DU) were followed.

> [!NOTE]  
> The following changes were made in the HDFS and Spark config files:
> * In the /hdfs/etc/hdfs-site.xml file the following lines were added:
>  ```
>   <property>
>        <name>dfs.webhdfs.enabled</name>
>        <value>true</value>
>    </property>
>    <property>
>        <name>dfs.permissions.enabled</name>
>        <value>false</value>
>    </property>
>    </configuration>
>   ```
>   This was done so that downloading and deleting files could be performed using the Web UI.
> * In the /spark/conf/ file there was a log4j2.properties.template file. This files was copied to another file, named log4j2.properties and the following lines were changed:
>  ```
>     rootLogger.level = warn
>     logger.replexprTyper.level = warn
>     logger.replSparkILoopInterpreter.level = warn
>  ```
> These variables initially had "info" instead of "warn". However, when submitting a python file to spark, we did not need to see all the information
> displayed, so we changed it to "warn" to only display warnings and errors during the application running.

## Dataset
[Read the respective README file in the data folder](https://github.com/dimitrismit/ntua_advanced_databases/blob/main/data/README.md)
