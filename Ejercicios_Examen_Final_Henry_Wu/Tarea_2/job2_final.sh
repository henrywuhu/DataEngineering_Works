#!/bin/bash

# Add Hadoop environment setup at the top of job1.sh
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64  # Adjust path as needed
export HADOOP_HOME=/home/hadoop/hadoop  # Adjust path as needed
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH

# Variables
FILE_1_URL="https://data-engineer-edvai-public.s3.amazonaws.com/CarRentalData.csv"
FILE_2_URL="https://data-engineer-edvai-public.s3.amazonaws.com/georef-united-states-of-america-state.csv"

# Downloading file number 1 from site
echo "Downloading File Number 1"
wget -O /home/hadoop/landing/cars_data.csv $FILE_1_URL

# Check if first file was downloaded successfully
if [ $? -eq 0 ]; then
    echo "File number 1 sucessfully downloaded"
else
	echo "Error downloading File number 1"
	exit 1
fi

# Downloading file number 2 from site
echo "Downloading File Number 2"

wget -O /home/hadoop/landing/geo_data.csv $FILE_2_URL

# Check if second file was downloaded successfully
if [ $? -eq 0 ]; then
    echo "File number 2 sucessfully downloaded"
else
	echo "Error downloading File number 2"
	exit 1
fi

echo "Moving files to ingest directory"



echo "Sending files to HDFS..."

hdfs dfs -put /home/hadoop/landing/cars_data.csv /ingest
hdfs dfs -put /home/hadoop/landing/geo_data.csv /ingest


if [ $? -eq 0 ]; then
	echo "Files successfully moved to HDFS!!"
else
	echo "Error moving files to HDFS"
	exit 1
fi
