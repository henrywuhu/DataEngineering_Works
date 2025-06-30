#!/bin/bash

# Variables
FILE_1_URL="https://data-engineer-edvai-public.s3.amazonaws.com/2021-informe-ministerio.csv"
FILE_2_URL="https://data-engineer-edvai-public.s3.amazonaws.com/202206-informe-ministerio.csv"
FILE_3_URL="https://data-engineer-edvai-public.s3.amazonaws.com/aeropuertos_detalle.csv"

# Downloading file number 1 from site
echo "Downloading File Number 1"
wget -O /home/hadoop/landing/informe_1.csv $FILE_1_URL

# Check if first file was downloaded successfully
if [ $? -eq 0 ]; then
    echo "File number 1 sucessfully downloaded"
else
	echo "Error downloading File number 1"
	exit 1
fi

# Downloading file number 2 from site
echo "Downloading File Number 2"

wget -O /home/hadoop/landing/informe_2.csv $FILE_2_URL

# Check if second file was downloaded successfully
if [ $? -eq 0 ]; then
    echo "File number 2 sucessfully downloaded"
else
	echo "Error downloading File number 2"
	exit 1
fi

# Downloading file number 2 from site
echo "Downloading File Number 2"

wget -O /home/hadoop/landing/aeropuerto.csv $FILE_3_URL

# Check if third file was downloaded successfully
if [ $? -eq 0 ]; then
    echo "File number 3 sucessfully downloaded"
else
	echo "Error downloading File number 3"
	exit 1
fi

echo "Moving files to ingest directory"



echo "Deleting existing files if exist..."

hdfs dfs -test -e /ingest/aeropuerto.csv && hdfs dfs -rm /ingest/aeropuerto.csv
hdfs dfs -test -e /ingest/informe_1.csv && hdfs dfs -rm /ingest/informe_1.csv
hdfs dfs -test -e /ingest/informe_2.csv && hdfs dfs -rm /ingest/informe_2.csv

if [ $? -eq 0 ]; then
	echo "Files successfully removed for HDFS!!"
else
	echo "Error deleting files from HDFS"
	exit 1
fi

echo "Sending files to HDFS..."

hdfs dfs -put /home/hadoop/landing/aeropuerto.csv /ingest
hdfs dfs -put /home/hadoop/landing/informe_1.csv /ingest
hdfs dfs -put /home/hadoop/landing/informe_2.csv /ingest

if [ $? -eq 0 ]; then
	echo "Files successfully moved to HDFS!!"
else
	echo "Error moving files to HDFS"
	exit 1
fi
