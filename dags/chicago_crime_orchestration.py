from airflow.sensors.filesystem import FileSensor
from airflow.providers.http.operators.http import HttpOperator
from airflow.hooks.filesystem import FSHook
from airflow.decorators import task, dag
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from pathlib import Path
from datetime import datetime
import json
import shutil

@dag(
    dag_id = "chicago_crime_dag",
    start_date = datetime(2026,7,10),
    schedule = None, #"*/5 * * * *",
    catchup = False,
    tags = ["chicago_crime"]
)
def chicago_crime_dag():

    # Sensor that checks for files in the /opt/data/pre-raw/pending/ folder
    # A connection needs to be set up in the Airflow UI with the folder path /opt/data/pre-raw/
    # Sensor que verifica si existen archivos en la carpeta /opt/data/pre-raw/pending/
    # Es necesario configurar una conexión en la UI de Airflow con la ruta de la carpeta /opt/data/pre-raw/
    wait_for_file = FileSensor(
        task_id = "wait_for_file",
        fs_conn_id = "pre_raw_fs",
        filepath = "pending/*.csv",
        poke_interval = 30,
        timeout = 3600,
        mode = "reschedule"
    )

    # Task that creates a list with the paths of all .csv files in the /opt/data/pre-raw/pending/ folder
    # Tarea que crea una lista con las rutas de todos los archivos .csv en la carpeta /opt/data/pre-raw/pending/
    @task
    def get_pending_files():
        conn = FSHook(fs_conn_id="pre_raw_fs")
        base_path = Path(conn.get_path())
        list_of_files = sorted((base_path/"pending/").glob("*.csv"))

        return [str(file) for file in list_of_files]
    
    files = get_pending_files()

    # Task that executes the processing of the bronze layer, on a per-file basis, using a POST request to an API in the Spark Master
    # A connection needs to be configured in the Airflow UI with the API host and port
    # Tarea que ejecuta el procesamiento de la capa bronce, por archivo, mediante una solicitud POST a una API en el Spark Master
    # Es necesario configurar una conexión en la UI de Airflow con el host y el puerto de la API
    execute_bronze_ingestion = HttpOperator.partial(
        task_id = "execute_bronze_ingestion",
        http_conn_id = "spark_api",
        endpoint = "submit",
        method = "POST",
        headers = {
            "Content-Type": "application/json"
        },
        response_filter = lambda response: response.json(),
    ).expand(
        data = files.map(
            lambda file: json.dumps(
                {
                    "input_file": file,
                    "phase": "bronze"
                }
            )
        )
    )

    # Task that moves the processed files on the bronze layer to the path /opt/data/pre-raw/processed/
    # Tarea que mueve los archivos procesados en la capa bronce hacia la ruta /opt/data/pre-raw/processed/
    @task
    def move_processed_file(result):
        if result['status'] != "SUCCESS":
            raise Exception(
                f"The file cannot be moved because processing failed: {result['input_file']}"
            )
        
        source = Path(result['input_file'])
        destination = source.parent.parent/"processed"/source.name
        shutil.move(source, destination)

        print(f"File moved to: {destination}")

    move_processed = move_processed_file.expand(
        result = execute_bronze_ingestion.output
    )

    # Task that executes silver layer processing using a POST request to an API in the Spark Master
    # Tarea que ejecuta el procesamiento de la capa plata mediante una solicitud POST a una API en el Spark Master
    execute_silver_transformation = HttpOperator(
        task_id = "execute_silver_transformation",
        http_conn_id = "spark_api",
        endpoint = "submit",
        method = "POST",
        headers = {
            "Content-Type": "application/json"
        },
        response_filter = lambda response: response.json(),
        data = json.dumps(
                {
                    "input_file": "",
                    "phase": "silver"
                }
            )
    )

    # Task that executes gold layer processing using a POST request to an API in the Spark Master
    # Tarea que ejecuta el procesamiento de la capa oro mediante una solicitud POST a una API en el Spark Master
    execute_gold_aggregation = HttpOperator(
        task_id = "execute_gold_aggregation",
        http_conn_id = "spark_api",
        endpoint = "submit",
        method = "POST",
        headers = {
            "Content-Type": "application/json"
        },
        response_filter = lambda response: response.json(),
        data = json.dumps(
                {
                    "input_file": "",
                    "phase": "gold"
                }
            )
    )

    # Task that uploads the data aggregated in the gold layer to a bucket in Google Cloud Storage
    # Tarea que carga los datos agregados en la capa gold a un bucket en Google Cloud Storage
    @task
    def upload_gold_to_gcs():

        bucket_name = "crime-chicago-bucket"
        prefix = "gold"
        local_path = Path("/opt/data/gold_level")

        # GCP connection / Conexión a GCP
        hook = GCSHook(gcp_conn_id="gcs_connection_chicago_crime")

        # Remove all current content from gold/ / Eliminar todo el contenido actual de gold/
        objects = hook.list(bucket_name=bucket_name, prefix=prefix)

        if objects:
            for object_name in objects:
                hook.delete(
                    bucket_name=bucket_name,
                    object_name=object_name
                )

        # Raise all the parquet floors again / Subir nuevamente todos los parquet
        for file in local_path.rglob("*.parquet"):

            object_name = (
                Path(prefix)
                / file.relative_to(local_path)
            ).as_posix()

            hook.upload(
                bucket_name=bucket_name,
                object_name=object_name,
                filename=str(file)
            )

        print("Gold uploaded successfully.")

    upload_gold = upload_gold_to_gcs()

    # Task dependencies
    # Dependencias de tareas
    wait_for_file >> files >> execute_bronze_ingestion >> move_processed >> execute_silver_transformation >> execute_gold_aggregation >> upload_gold

chicago_crime_dag()


