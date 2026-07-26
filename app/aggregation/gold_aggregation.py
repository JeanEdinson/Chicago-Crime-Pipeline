from pyspark.sql import SparkSession
from pathlib import Path
import pyspark.sql.functions as f
from pyspark.sql.window import Window
from py4j.java_gateway import java_import

spark = (
    SparkSession.builder
    .appName("silver_transformation")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

sc = spark.sparkContext
java_import(sc._jvm, "org.apache.hadoop.fs.Path")

fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
    sc._jsc.hadoopConfiguration()
)

path_table_control = sc._jvm.Path("/opt/data/metadata/table_control")
path_table_control_tmp = sc._jvm.Path("/opt/data/metadata/table_control_tmp")
path_local_table_control = Path("/opt/data/metadata/table_control")
path_local_table_control_tmp = Path("/opt/data/metadata/table_control_tmp")
path_silver_level = Path("/opt/data/silver_level")
path_gold_level = Path("/opt/data/gold_level")

# Read data from the control table
# Leer datos de la tabla control
df_table_control = spark.read.parquet(str(path_local_table_control))

# Read data from the silver layer / Leer datos de la capa silver
df_data_silver = spark.read.parquet(str(path_silver_level))

# Number of crimes and arrests per year, first and second description of the crime
# Número de crímenes y arrestos por año, primera y segunda descripción del delito
df_n_crimes_arrests_year = df_data_silver.groupBy("year", "primary_type", "description").agg(
    f.count(f.col("id")).alias("number_of_crimes"),
    f.sum(f.col("arrest").cast("int")).alias("number_of_arrests")
)

# Number of crimes, arrests, and domestic offenses by year and weekday
# Número de crímenes, arrestos y delitos domésticos por año y día de semana
df_n_indicators_day = df_data_silver.groupBy("year", "day_name_of_week").agg(
    f.count(f.col("id")).alias("number_of_crimes"),
    f.sum(f.col("arrest").cast("int")).alias("number_of_arrests"),
    f.sum(f.col("domestic").cast("int")).alias("number_of_domestic_crime")
)

# Number of crimes, arrests, and domestic offenses by year and geographic location
# Número de crímenes, arrestos y delitos domésticos por año y ubicación geográfica
df_n_indicators_location = df_data_silver.groupBy("year", "community_area", "ward", "district", "beat", "block").agg(
    f.count(f.col("id")).alias("number_of_crimes"),
    f.sum(f.col("arrest").cast("int")).alias("number_of_arrests"),
    f.sum(f.col("domestic").cast("int")).alias("number_of_domestic_crime")
)

# Ranking of crimes with the lowest incidence by year
# Ranking de delitos con menor incidencia por año
df_n_crimes_year_primary_type = df_data_silver.groupBy("year", "primary_type").agg(
    f.count(f.col("id")).alias("number_of_crimes")
)

specific_window = (
    Window
    .partitionBy("year")
    .orderBy(f.asc("number_of_crimes"))
)

df_ranking_year_primary_type = df_n_crimes_year_primary_type.withColumn(
    "ranking_year",
    f.dense_rank().over(specific_window)
)

# Writing Aggregate Tables / Escritura de tablas agregadas
df_n_crimes_arrests_year.write.mode("overwrite").parquet(str(path_gold_level/"n_crimes_arrests_year"))
df_n_indicators_day.write.mode("overwrite").parquet(str(path_gold_level/"n_indicators_day"))
df_n_indicators_location.write.mode("overwrite").parquet(str(path_gold_level/"n_indicators_location"))
df_ranking_year_primary_type.write.mode("overwrite").parquet(str(path_gold_level/"ranking_year_primary_type"))

# Control Table Update / Actualización de tabla control
# Delete the temporary one if it already existed / Eliminar el temporal si ya existía
if fs.exists(path_table_control_tmp):
    fs.delete(path_table_control_tmp, True)

# Write to the temporary table / Escribir en la tabla temporal
df_table_control_update = df_table_control.withColumn("status_gold", f.lit(True))
df_table_control_update.write.mode("overwrite").parquet(str(path_local_table_control_tmp))

# Delete the original folder / Eliminar la carpeta original
if fs.exists(path_table_control):
    fs.delete(path_table_control, True)

# Rename temporary to original / Renombrar temporal a original
fs.rename(path_table_control_tmp, path_table_control)
