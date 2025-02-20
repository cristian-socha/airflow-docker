"""
### DAG Tutorial copy and move Objects to Cloud Storage
DAG para copiar objectos desde la maquina local a Cloud Storage y moverlos entre Buckets
"""

# import module
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
import os


# Define the Python function to upload files to GCS
def upload_to_gcs(data_folder, gcs_path,**kwargs):
    data_folder = data_folder
    # bucket_name="rsg_test_airflow",
    # gcp_conn_id="google_cloud_default",
    # List all CSV files in the data folder
    csv_files = [file for file in os.listdir(data_folder) if file.endswith('.csv')]

    # Upload each CSV file to GCS
    for csv_file in csv_files:
        local_file_path = os.path.join(data_folder, csv_file)
        gcs_file_path = f"{gcs_path}/{csv_file}"

        upload_task = LocalFilesystemToGCSOperator(
            task_id=f'upload_to_gcs',
            src=local_file_path,
            dst=gcs_file_path,
            bucket="rsg_test_airflow",
            gcp_conn_id="google_cloud_default",
        )
        upload_task.execute(context=kwargs)

# definition DAG
dag = DAG(
    'Tutorial_Cargar_Archivos_GCS',
    start_date=datetime(2025, 1, 1),
    doc_md=__doc__,
    schedule_interval=None,
    catchup=False,
    tags=["Regional Services Group", "Upload", "Google Cloud Storage"],
)

upload_to_gcs = PythonOperator(
    task_id='upload_to_gcs',
    python_callable=upload_to_gcs,
    op_args=['/opt/airflow/validation', 'gs://rsg_test_airflow'],
    provide_context=True,
    dag=dag, 
)

upload_to_gcs
