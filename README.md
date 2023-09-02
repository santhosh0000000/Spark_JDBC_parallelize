# Spark_JDBC_parallelize.java
This Java code contains two classes, App2 and App, which are used to read data from a SAP database into a Spark DataFrame and then write it to an HDFS (Hadoop Distributed File System) location in ORC (Optimized Row Columnar) format.
Class App2
Variable Declarations: This class declares several variables related to the SAP database configuration (host, port, username, password, etc.) and Spark (spark, df).

Constructor App2:

Initializes a SparkSession with various configurations:
appName: The name of the Spark application.
master: The Spark master URL.
executor.memory: Amount of memory per executor.
executor.cores: Number of cores per executor.
executor.instances: Number of executor instances.
dynamicAllocation.enabled: Whether to enable dynamic allocation of executors.
default.parallelism: The default number of partitions.
jars: External jars to be used (SAP JDBC driver in this case).
cores.max: The maximum number of cores that can be used.
Sets log level to "ERROR".
Reads data from the SAP database into a DataFrame (df) using JDBC.
Partition column is set to "KWERT".
Number of partitions is set to "100".
The column type is "decimal(15,2)".
Lower and upper bounds for partitioning are set to 0 and 800000, respectively.
Class App
Inherits App2: This class extends App2, so it inherits all its functionalities.

Main Method:

An object app of the App class is created.
The DataFrame's count is printed to the console.
The DataFrame is written to an HDFS location in ORC format.
Save mode is set to "Overwrite".
A header is included in the output.
The Spark session is stopped.
The code is organized in a way that allows for modularization and separation of concerns. App2 focuses on setting up the Spark session and reading data into a DataFrame, while App handles what to do with that DataFrame.
