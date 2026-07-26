from pyspark.sql import SparkSession
from pathlib import Path
import pyspark.sql.functions as f
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
path_local_table_control = Path("/opt/data/metadata/table_control")
path_destination = Path("/opt/data/silver_level")

if fs.exists(path_table_control) and spark.read.parquet(str(path_local_table_control)).filter(f.col("status_silver") == False).count() > 0:

    # Read files that have not yet been processed on the silver layer from the control table
    # Leer los archivos que aún no han sido procesados en la capa de plata desde la tabla de control
    df_table_control = spark.read.parquet(str(path_local_table_control)).filter(f.col("status_silver") == False)

    # Read from the bronze layer only records that belong to files that have not yet been processed in the silver layer
    # Leer de la capa bronce solo los registros que pertenezcan a archivos que aun no han sido procesados en la capa de plata
    df_bronze_data = spark.read.parquet("/opt/data/bronze_level").join(f.broadcast(df_table_control.select("file_source")), on="file_source", how="inner")

    # Rename columns / Renombrar columnas
    df_silver_data_rename_columns = df_bronze_data.withColumnsRenamed({
        'ID': 'id',
        'Case Number': 'case_number',
        'Date': 'date',
        'Block': 'block',
        'IUCR': 'iucr',
        'Primary Type': 'primary_type',
        'Description': 'description',
        'Location Description': 'location_description',
        'Arrest': 'arrest',
        'Domestic': 'domestic',
        'Beat': 'beat',
        'District': 'district',
        'Ward': 'ward',
        'Community Area': 'community_area',
        'FBI Code': 'fbi_code',
        'X Coordinate': 'x_coordinate',
        'Y Coordinate': 'y_coordinate',
        'Year': 'year',
        'Updated On': 'updated_on',
        'Latitude': 'latitude',
        'Longitude': 'longitude',
        'Location': 'location'
    })

    # Selection of relevant columns / Seleccion de columnas relevantes
    df_silver_columns_selected = df_silver_data_rename_columns.select('file_source', 'id', 'date', 'block', 'primary_type', 'description', 'arrest', 'domestic', 'beat', 'district', 'ward', 'community_area', 'latitude', 'longitude', 'year')

    # Remove duplicate records / Eliminar registros duplicados
    df_silver_columns_selected.dropDuplicates(["id"])

    # Change the data type of the date column / Cambiar tipo de dato a la columna de fecha
    df_silver_columns_selected = df_silver_columns_selected.withColumn("date", f.to_date(f.col("date"), "MM/dd/yyyy hh:mm:ss a"))

    # Filter records that have no date or where the combination of columns 'block', 'primary_type', 'description', 'arrest', 'domestic' is null
    # Filtrar registros que no tienen fecha o la combinación de columnas 'block', 'primary_type', 'description', 'arrest', 'domestic' es nula
    df_silver_without_null = df_silver_columns_selected.filter(
        ~(f.col('date').isNull()) |
        ~(
            (f.col('block').isNull()) &
            (f.col('primary_type').isNull()) &
            (f.col('description').isNull()) &
            (f.col('arrest').isNull()) &
            (f.col('domestic').isNull())
        )
    )

    # Remove double spaces and leading and trailing spaces
    # Eliminar dobles espacios y espacios al inicio y al final
    text_columns_spaces = ['block', 'primary_type', 'description']

    for column in text_columns_spaces:
        df_silver_without_null = df_silver_without_null.withColumn(
            column, 
            f.regexp_replace(
                f.trim(f.col(column)),
                r"\s+",
                " "
            )
        )

    # Capitalize text column values
    # Capitalizar valores de columnas de texto
    text_columns_lower = ['primary_type', 'description']

    for column in text_columns_lower:
        df_silver_without_null = df_silver_without_null.withColumn(
            column,
            f.initcap(f.col(column))
        )

    # Add day of the week / Agregar día de la semana
    df_silver_final = df_silver_without_null.withColumn(
        'day_name_of_week',
        f.date_format(f.col('date'), "EEEE")
    )

    # Add Metadata / Agregar metadata
    df_silver_final = df_silver_final.withColumn(
        "ingestion_date", 
        f.current_timestamp()
    )

    # Save processed data / Guardar datos procesados
    df_silver_final.write.mode("append").partitionBy("Year").parquet(str(path_destination))

    # Processed files / Archivos procesados
    df_files_processed = df_silver_final.select('file_source').distinct().withColumn('new_status_silver', f.lit(True))

    # Update control table / Actualizar tabla de control
    df_table_control_update = df_table_control.alias('ct').join(
        df_files_processed.alias('fp'),
        on = 'file_source',
        how = 'left'
    ).withColumn(
        'status_silver',
        f.coalesce(
            f.col('new_status_silver'),
            f.col('status_silver')
        )
    ).drop("new_status_silver")

    df_table_control_update.write.mode("overwrite").parquet(str(path_local_table_control))

else:
    raise Exception("There are no files to process.")
