## Table of Contents
<!-- TOC -->
* [Table of Contents](#table-of-contents)
    * [Introduction](#introduction)
    * [Pre-requirement](#pre-requirement)
    * [Analysis](#analysis)
        * [MapReduce](#mapreduce)
            * [Cluster config](#map-reduce-cluster-config)
            * [Steps](#map-reduce-cluster-config-steps)
        * [Spark](#spark)
            * [Cluster config](#spark-cluster-config)
            * [Steps](#spark-cluster-config-steps)
    * [Evaluation](#evaluation)
        * [Heterogeneous query evaluation](#heterogeneous-query-evaluation)
        * [Homogeneous query evaluation](#homogeneous-query-evaluation)
            * [Average comparison](#average-comparison)
    * [Conclusion](#conclusion)
<!-- TOC -->

### Introduction
The README file summarizes the evaluation of MapReduce and Spark for the given dataset/queries, and it includes references.

The directories below contain source code, log output, statistics, images, and recordings.

| Directory  | Content                                                 |
|------------|---------------------------------------------------------|
| test_data  | Original data set, iteration configurations, query data |
| map_reduce | MapReduce source code, statistics and images            |
| spark      | Spark source code, statistics and images                |
| evaluation | MapReduce and Spark analysis raw data and recordings.   |

### Pre-requirement
- Make sure you have the `map_reduce_spark_assignment_lap.pem` file in the root directory.
- The source code is written in Python and HiveQL.
- After setting up the cluster make sure to update the `EC2_HOST` environment variable. To do so, run the following command in the terminal:
    ```bash
    set EC2_HOST=<ec2-public-dns>.compute-1.amazonaws.com
    ```
- The `EC2_HOST` environment variable is used to replace the `%EC2_HOST%` placeholder in the commands.
- To connect from SSH in windows, open the command prompt and run the following command:
    ```bash
    ssh -i ./map_reduce_spark_assignment_lap.pem hadoop@%EC2_HOST%
    ```
- After connecting make sure to update the `HIVE_HOST` environment variable. To do so, run the following command in the terminal:
    ```bash
    echo 'export HIVE_HOST="<ec2instance-ip>.compute-1.amazonaws.com"' >> ~/.bashrc
    source ~/.bashrc
    ```

### Analysis

#### MapReduce

##### Cluster config
![map-reduce-emr-cluster-configuration.png](map_reduce%2Fconfig%2Fmap-reduce-emr-cluster-configuration.png)

##### Steps
- Make sure to complete the **Pre-Requirement** steps.
- Upload the data to the hadoop cluster.
    ```bash
    scp -v -i "./map_reduce_spark_assignment_lap.pem" ./test_data/DelayedFlights-updated.csv hadoop@%EC2_HOST%:/home/hadoop
    ```
- Create a directory for the test data.
    ```bash
    ssh -i ./map_reduce_spark_assignment_lap.pem hadoop@%EC2_HOST% "mkdir -p /home/hadoop/test_data && exit"
    ```
- Upload the test data configuration to the hadoop cluster.
    ```bash
    scp -v -i "./map_reduce_spark_assignment_lap.pem"  ./test_data/*.py hadoop@%EC2_HOST%:/home/hadoop/test_data
    ```
- Create a directory for the MapReduce job.
    ```bash
    ssh -i ./map_reduce_spark_assignment_lap.pem hadoop@%EC2_HOST% "mkdir -p /home/hadoop/map_reduce && mkdir -p /home/hadoop/map_reduce/charts && mkdir -p /home/hadoop/map_reduce/csv && mkdir -p /home/hadoop/map_reduce/tables && exit"
    ```
- Upload the source code to the hadoop cluster.
    ```bash
    scp -v -i "./map_reduce_spark_assignment_lap.pem" ./map_reduce/*.hql ./map_reduce/*.py hadoop@%EC2_HOST%:/home/hadoop/map_reduce
    ```
- Install the required packages.
    ```bash
    pip install pandas PyHive thrift thrift_sasl tabulate python-dotenv matplotlib plotly kaleido
    ```
- Put the data to the hadoop file system.
    ```bash
    hadoop fs -mkdir /flights 
    hadoop fs -put DelayedFlights-updated.csv /flights/ 
    hadoop fs -ls /flights 
    ```
- Run the Hive shell and execute the HiveQL script.
    ```bash
    hive
    source ./map_reduce/load-delayed-flights.hql;
    exit;
    ```
- Run the MapReduce job.
    ```bash
    spark-submit ./map_reduce/load-and-process.py
    ```
- Download .csv and .png files from the hadoop file system.
    ```bash
    scp -r -i "./map_reduce_spark_assignment_lap.pem" hadoop@%EC2_HOST%:/home/hadoop/map_reduce/csv/*.csv ./map_reduce/csv
    scp -r -i "./map_reduce_spark_assignment_lap.pem" hadoop@%EC2_HOST%:/home/hadoop/map_reduce/charts/*.png ./map_reduce/charts
    scp -r -i "./map_reduce_spark_assignment_lap.pem" hadoop@%EC2_HOST%:/home/hadoop/map_reduce/tables/*.png ./map_reduce/tables
    ```  


#### Spark

##### Cluster config
![spark-emr-cluster-configuration.png](spark%2Fconfig%2Fspark-emr-cluster-configuration.png)

##### Steps
- Make sure to complete the **Pre-Requirement** steps.
- Upload the data to the hadoop cluster.
    ```bash
    scp -v -i "./map_reduce_spark_assignment_lap.pem" ./test_data/DelayedFlights-updated.csv hadoop@%EC2_HOST%:/home/hadoop
    ```
- Create a directory for the test data.
    ```bash
    ssh -i ./map_reduce_spark_assignment_lap.pem hadoop@%EC2_HOST% "mkdir -p /home/hadoop/test_data && exit"
    ```
- Upload the test data configuration to the hadoop cluster.
    ```bash
    scp -v -i "./map_reduce_spark_assignment_lap.pem"  ./test_data/*.py hadoop@%EC2_HOST%:/home/hadoop/test_data
    ```
- Create a directory for the MapReduce job.
    ```bash
    ssh -i ./map_reduce_spark_assignment_lap.pem hadoop@%EC2_HOST% "mkdir -p /home/hadoop/spark && mkdir -p /home/hadoop/spark/charts && mkdir -p /home/hadoop/spark/csv && mkdir -p /home/hadoop/spark/tables && exit"
    ```
- Upload the source code to the hadoop cluster.
    ```bash
    scp -v -i "./map_reduce_spark_assignment_lap.pem" ./spark/*.hql ./spark/*.py hadoop@%EC2_HOST%:/home/hadoop/spark
    ```
- Install the required packages.
    ```bash
    pip install pandas PyHive thrift thrift_sasl tabulate python-dotenv matplotlib plotly kaleido
    ```
- Put the data to the hadoop file system.
    ```bash
    hadoop fs -mkdir /flights 
    hadoop fs -put DelayedFlights-updated.csv /flights/
    hadoop fs -ls /flights
    ```
- Run the Hive shell and execute the HiveQL script.
    ```bash
    hive
    source ./spark/load-delayed-flights.hql;
    exit;
    ```
- Run the MapReduce job.
    ```bash
    spark-submit ./spark/load-and-process.py
    ```
- Download .csv and .png files from the hadoop file system.
    ```bash
    scp -r -i "./map_reduce_spark_assignment_lap.pem" hadoop@%EC2_HOST%:/home/hadoop/spark/csv/*.csv ./spark/csv
    scp -r -i "./map_reduce_spark_assignment_lap.pem" hadoop@%EC2_HOST%:/home/hadoop/spark/charts/*.png ./spark/charts
    scp -r -i "./map_reduce_spark_assignment_lap.pem" hadoop@%EC2_HOST%:/home/hadoop/spark/tables/*.png ./spark/tables
    ``` 
