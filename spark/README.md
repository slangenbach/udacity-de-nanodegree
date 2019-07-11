# Data lakes with Amazon EMR

## About
The script within this repository processes song- and log data found in the context of a music-streaming service 
(Sparkify) using Apache [Spark]. After data has been read from AWS S3, fact- and dimension tables are constructed and 
saved in a user-defined location as [Parquet](https://parquet.apache.org/) files for further usage.  

## Prerequisites
* Access to an (running) AWS EMR cluster (5.24.1+) running Spark 2.4.2, where access means:
    - Dedicated AWS key pair to set up SSH connections to the cluster
    - Security group with inbound rule allowing SSH access on port 22
* Access to an AWS S3 bucket reachable from AWS EMR
* Python 3.6+
* Python package pyspark

## Usage
1. SSH into the AWS EMR cluster using your key pair  
2. If not already available install pyspark via ```sudo /usr/bin/pip-3.6 install pyspark```
3. Clone this repository and navigate into the spark directory  
4. Open the ```dl.cfg```, provide your **AWS_ACCESS_KEY_ID** and **AWS_SECRET_ACCESS_KEY** and edit the remaining 
options according to your needs, i.e. change the output path from HDFS to S3  
5. Start the data pipeline via ```/usr/bin/python3 etl.py``` 
 
## Limitations
* Running the pipeline with the default configuration will save all processed data to the distributed file system of the 
cluster (HDFS). This is done on purpose to avoid well known issues with writing Parquet files to S3. Although steps 
have been taken to [improve](https://aws.amazon.com/blogs/big-data/improve-apache-spark-write-performance-on-apache-parquet-formats-with-the-emrfs-s3-optimized-committer/)
this issue, I still recommend saving output to HDFS and then copying it to S3 via 
[S3DistCp](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/UsingEMR_s3distcp.html).

## Resources
* [Using PyCharm with Spark/EMR](https://www.linkedin.com/pulse/attention-pyspark-developers-simplifying-code-kartik-bhatnagar)
* [Using Python 3 with EMR](https://aws.amazon.com/premiumsupport/knowledge-center/emr-pyspark-python-3x/)
* [Accessing file systems from EMR](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-plan-file-systems.html)
* [Dealing with "Instance type not supported" errors](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-INSTANCE_TYPE_NOT_SUPPORTED-error.html)
* [Udacity knowledge portal](https://knowledge.udacity.com/)