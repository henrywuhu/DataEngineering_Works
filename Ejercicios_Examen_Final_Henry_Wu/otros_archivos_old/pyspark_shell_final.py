
from pyspark.sql.functions import to_date
from pyspark.sql.functions import coalesce,col
from pyspark.sql import SparkSession
import sys

def main():
    
    #This works
    spark = SparkSession.builder.master("spark://localhost:7077").appName("HiveLocalConnection").config("hive.metastore.uris", "thrift://localhost:9083").enableHiveSupport().getOrCreate()
    
    # spark = SparkSession.builder.master("spark://localhost:7077").getOrCreate()
    try:
        print("Starting the Spark session...")
        #PARTE CERO - AEROPUERTOS DETALLES
        print("Processing aeropuerto.csv...")
        df = spark.read.option("header","true").csv("hdfs://172.17.0.2:9000/ingest/aeropuerto.csv",sep=';')
        cols_drop = ['fir','inhab']
        df_select = df.drop(*cols_drop)
        df_select = df_select.withColumn('distancia_ref',df_select['distancia_ref'].cast('float'))
        df_select = df_select.withColumn('elev',df_select['elev'].cast('float'))
        df_select = df_select.na.fill(0,subset=['distancia_ref'])
        df_select = df_select.na.fill(0,subset=['elev'])
        new_col_names = ['aeropuerto','oac','iata','tipo','denominacion','coordenadas','latitud','longitud','elev','uom_elev','ref','distancia_ref','direccion_ref','condicion','control','region','uso','trafico','sna','concesionado','provincia']
        df_select = df_select.toDF(*new_col_names)
        df_select.write.mode('append').insertInto('airport_trips_data.aeropuerto_detalles_tabla')
        
        print("File aeropuerto.csv processed successfully.")

        #PARTE 1 - INFORME 1
        print("Processing informe_1.csv...")
        df2 = spark.read.option("header","true").csv("hdfs://172.17.0.2:9000/ingest/informe_1.csv",sep=';')
        cols_drop = ['Calidad dato']
        df2_select = df2.drop(*cols_drop)
        df2_select = df2_select.withColumn('Fecha',coalesce(
        to_date(col('Fecha'), 'MM/dd/yyyy'),
        to_date(col('Fecha'), 'dd/MM/yyyy'),
        to_date(col('Fecha'), 'yyyy-MM-dd')))
        df2_select = df2_select.withColumn('Pasajeros',df2_select['Pasajeros'].cast('int'))
        df2_select = df2_select.na.fill(0,subset=['Pasajeros'])
        new_col_names = ['fecha','horautc','clase_de_vuelo','clasificacion_de_vuelo','tipo_de_movimiento','aeropuerto','origen_destino','aerolinea_nombre','aeronave','pasajeros']
        df2_select = df2_select.toDF(*new_col_names)
        df2_select = df2_select.filter(df2_select['clasificacion_de_vuelo'].isin(["Domestico","Doméstico"]))
        df2_select.write.mode('append').insertInto('airport_trips_data.aeropuerto_tabla')
        
        print("File informe_1.csv processed successfully.")

        #PARTE 2 - INFORME 2
        print("Processing informe_2.csv...")
        df3 = spark.read.option("header","true").csv("hdfs://172.17.0.2:9000/ingest/informe_2.csv",sep=';')
        cols_drop = ['Calidad dato']
        df3_select = df3.drop(*cols_drop)
        df3_select = df3_select.withColumn('Fecha',to_date(df3_select['Fecha'],'mm/dd/yyyy'))
        df3_select = df3_select.withColumn('Pasajeros',df3_select['Pasajeros'].cast('int'))
        df3_select = df3_select.na.fill(0,subset=['Pasajeros'])
        new_col_names = ['fecha','horautc','clase_de_vuelo','clasificacion_de_vuelo','tipo_de_movimiento','aeropuerto','origen_destino','aerolinea_nombre','aeronave','pasajeros']
        df3_select = df3_select.toDF(*new_col_names)
        df3_select = df3_select.filter(df3_select['clasificacion_de_vuelo'].isin(["Domestico","Doméstico"]))
        df3_select.write.mode('append').insertInto('airport_trips_data.aeropuerto_tabla')
        print("File informe_2.csv processed successfully.")
    except Exception as e:
        print(f"An error occurred: {e}", str(e.args))
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
    print("Spark session ended.")
