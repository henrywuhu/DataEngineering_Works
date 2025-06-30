from pyspark.sql import SparkSession
from pyspark.sql.functions import round
from pyspark.sql.functions import lower, col
import sys

def main():
    spark = SparkSession.builder.master("spark://localhost:7077").appName("HiveLocalConnection").config("hive.metastore.uris", "thrift://localhost:9083").enableHiveSupport().getOrCreate()
    # spark = SparkSession.builder.master("spark://localhost:7077").getOrCreate()
    try:
        print("Starting the Spark session...")
        print("Processing cars data csv and geo data csv files...")
        
        df = spark.read.option("header","true").csv("hdfs://172.17.0.2:9000/ingest/cars_data.csv",sep=',')
        df_geo = spark.read.option("header","true").csv("hdfs://172.17.0.2:9000/ingest/geo_data.csv",sep=';')
        
        print("File cars_data.csv and geo_data.csv read successfully.")
        
        print("Processing data...")
        
        cols_to_drop = ['location.latitude','location.country','location.longitude','vehicle.type']
        df_select = df.drop(*cols_to_drop)
        df_geo_select = df_geo.select("United States Postal Service state abbreviation","Official Name State")
        df_geo_select = df_geo_select.withColumnRenamed("United States Postal Service state abbreviation","state_code").withColumnRenamed("Official Name State","state_name")
        
        new_col_names = ['fueltype','rating','rentertripstaken','reviewcount','city','state_code','owner_id','rate_daily','make','model','year']
        df_select = df_select.toDF(*new_col_names)
        
        
        print("Data processing completed. Merging dataframes...")
        merged_df = df_select.join(df_geo_select,"state_code","left")
        merged_df = merged_df.filter(merged_df.state_name != "Texas")
        merged_df = merged_df.withColumn("fueltype",lower(col("fueltype")))
        merged_df = merged_df.na.drop(subset=['rating'])
        merged_df = merged_df.withColumn('rating',merged_df['rating'].cast('float'))
        merged_df = merged_df.withColumn('rentertripstaken',merged_df['rentertripstaken'].cast('int'))
        merged_df = merged_df.withColumn('owner_id',merged_df['owner_id'].cast('int'))
        merged_df = merged_df.withColumn('rate_daily',merged_df['rate_daily'].cast('int'))
        merged_df = merged_df.withColumn('reviewcount',merged_df['reviewcount'].cast('int'))
        merged_df = merged_df.withColumn('year',merged_df['year'].cast('int'))
        merged_df = merged_df.withColumn('rating',round(merged_df['rating'],0).cast('int'))
        col_to_drop = ['state_code']
        merged_df = merged_df.drop(*col_to_drop)
        column_order = ['fueltype','rating','rentertripstaken','reviewcount','city','state_name','owner_id','rate_daily','make','model','year']
        merged_df= merged_df.select(*column_order)
        print("Dataframes merged successfully.")
        
        print("Writing the merged dataframe to the car_rental_analytics table...")

        merged_df.write.mode('append').insertInto('car_rental_db.car_rental_analytics')
        
        print("Data written successfully to the car_rental_analytics table.")
        
    except Exception as e:
        print(f"An error occurred: {e}", str(e.args))
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
    print("Spark session ended.")
