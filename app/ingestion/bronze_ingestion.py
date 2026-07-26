from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType
import pyspark.sql.functions as f
from pyspark.sql import Row
from py4j.java_gateway import java_import
from pathlib import Path
import argparse

spark = (
    SparkSession.builder
    .appName("raw_ingestion")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
sc = spark.sparkContext
java_import(sc._jvm, "org.apache.hadoop.fs.Path")
fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
    sc._jsc.hadoopConfiguration()
)

parser = argparse.ArgumentParser(
    description="Raw ingestion Chicago Crime data"
)

parser.add_argument(
    "--input",
    required=True,
    help="Path del archivo CSV de entrada"
)

path_file = Path(parser.parse_args().input)
path_destination = "/opt/data/bronze_level"

# Define the schema of the raw data
# Definir el esquema de los datos en crudo
schema_chicago_crime = StructType([
    StructField("ID", IntegerType(), True),
    StructField("Case Number", StringType(), True),
    StructField("Date", StringType(), True),
    StructField("Block", StringType(), True),
    StructField("IUCR", StringType(), True),
    StructField("Primary Type", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Location Description", StringType(), True),
    StructField("Arrest", BooleanType(), True),
    StructField("Domestic", BooleanType(), True),
    StructField("Beat", IntegerType(), True),
    StructField("District", IntegerType(), True),
    StructField("Ward", IntegerType(), True),
    StructField("Community Area", IntegerType(), True),
    StructField("FBI Code", StringType(), True),
    StructField("X Coordinate", IntegerType(), True),
    StructField("Y Coordinate", IntegerType(), True),
    StructField("Year", IntegerType(), True),
    StructField("Updated On", StringType(), True),
    StructField("Latitude", DoubleType(), True),
    StructField("Longitude", DoubleType(), True),
    StructField("Location", StringType(), True)
])

# Read the raw data / Leer los datos en crudo
bronze_df = (
    spark.read
    .option("header", "true")
    .schema(schema_chicago_crime)
    .csv(str(path_file))
)

# Add metadata columns / Agregar columnas de metadatos
bronze_df = bronze_df.withColumn("ingestion_date", f.current_timestamp())
bronze_df = bronze_df.withColumn("file_source", f.lit(path_file.name))

# Write data to partitioned parquet files by year / Escribir datos en archivos parquet particionados por año
bronze_df.write.mode("append").partitionBy("Year").parquet(path_destination)

print(f"File processed, partition by Year y save in raw level: {path_file} with {bronze_df.count()} records")

# Register files in the control table for the following steps / Registrar archivos en la tabla de control para las siguientes etapas
path_table_control = sc._jvm.Path("/opt/data/silver_level/table_control")
schema_table_control = StructType([
    StructField("file_source", StringType(), True),
    StructField("status_bronze", BooleanType(), True),
    StructField("status_silver", BooleanType(), True),
    StructField("status_gold", BooleanType(), True)
])

if fs.exists(path_table_control):
    df_table_control = spark.read.schema(schema_table_control).parquet("/opt/data/silver_level/table_control")
    new_control_record = Row(file_source = path_file.name, status_bronze = True, status_silver = False, status_gold = False)
else:
    new_control_record = spark.createDataFrame([Row(file_source = path_file.name, status_bronze = True, status_silver = False, status_gold = False)])

new_control_record.write.mode("append").parquet("/opt/data/metadata/table_control")
print(f"The files were added to the control table for silver processing.")