
"""
### DAG Tutorial Google Cloud Storage
DAG para listar objetos de un Bucket defindo en Google Cloud Storage
"""

# import_module
from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from datetime import datetime, timedelta

# instantiate_dag
with DAG(
    "Tutorial_Listar_Objetos",
    default_args={"start_date": datetime(2025, 1, 1)},
    schedule_interval=None,
    catchup=False,
    tags=["Regional Services Group", "Other Project", "Google Cloud Storage"],
) as dag:
    
    # documentation
    dag.doc_md = __doc__
    
    # task list buckets
    list_buckets = GCSListObjectsOperator(
        task_id="List_Buckets",
        bucket="{{ var.value.bucket_id_1 }}",
        gcp_conn_id="google_cloud_default_1",
    )
