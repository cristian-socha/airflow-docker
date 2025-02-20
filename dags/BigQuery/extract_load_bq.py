"""
### DAG Tutorial EL Extract/Load Pipeline using BigQuery
DAG para extraer/cargar con comprobación de calidad de datos utilizando BigQuery
"""

# import_module
import json

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.sensors.bigquery import (
    BigQueryTableExistenceSensor, 
    BigQueryTablePartitionExistenceSensor,
)
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryInsertJobOperator,
    BigQueryCheckOperator,
    BigQueryValueCheckOperator,
    BigQueryDeleteDatasetOperator,
)
from airflow.utils.dates import datetime
from airflow.utils.task_group import TaskGroup

DATASET = "forestfires_example_dag"
TABLE = "forestfires"
PARTITION = "2025-01-01"

with DAG(
    "Tutorial_Extraer_Cargar_BigQuery",
    start_date=datetime(2025, 1, 1),
    doc_md=__doc__,
    schedule_interval=None,
    catchup=False,
    template_searchpath="/usr/local/airflow/sql/",
    tags=["Regional Services Group", "BigQuery", "Extract Load"],
) as dag:
    
    """
    #### BigQuery dataset creation
    Create the dataset to store the sample data tables.
    """
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset", 
        dataset_id=DATASET
    )

    """
    #### BigQuery table creation
    Create the table to store sample forest fire data.
    """
    create_table = BigQueryCreateEmptyTableOperator(
        task_id="create_table",
        dataset_id=DATASET,
        table_id=TABLE,
        schema_fields=[
            {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "y", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "month", "type": "STRING", "mode": "NULLABLE"},
            {"name": "day", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ffmc", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "dmc", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "dc", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "isi", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "temp", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "rh", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "wind", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "rain", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "area", "type": "FLOAT", "mode": "NULLABLE"},
        ],
    )

    """
    #### BigQuery table check
    Ensure that the table was created in BigQuery before inserting data.
    """
    check_table_exists = BigQueryTableExistenceSensor(
        task_id="check_for_table",
        project_id="{{ var.value.gcp_project_id }}",
        dataset_id=DATASET,
        table_id=TABLE,
    )

    """
    #### BigQuery table partition check
    Ensure that the table partition was created in BigQuery before inserting data.
    """
    check_table_partition_exists = BigQueryTablePartitionExistenceSensor(
        task_id="check_for_table_partition",
        project_id="{{ var.value.gcp_project_id }}",
        dataset_id=DATASET,
        table_id=TABLE,
        partition_id=PARTITION
    )

    """
    #### Insert data
    Insert data into the BigQuery table using an existing SQL query (stored in
    a file under dags/sql).
    """
    load_data = BigQueryInsertJobOperator(
        task_id="insert_query",
        trigger_rule= 'all_done',
        configuration={
            "query": {
                "query": "CALL forestfires_example_dag.insert_forestfire_data();",
                "useLegacySql": False,
            }
        },
    )

    """
    #### Table-level data quality check
    Run a row count check to ensure all data was uploaded to BigQuery properly.
    """
    check_bq_row_count = BigQueryValueCheckOperator(
        task_id="check_row_count",
        sql=f"SELECT COUNT(*) FROM {DATASET}.{TABLE}",
        pass_value=9,
        use_legacy_sql=False,
    )

    """
    #### Delete test dataset and table
    Clean up the dataset and table created for the example.
    """
    delete_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete_dataset", 
        dataset_id=DATASET, 
        delete_contents=True
    )

    begin = DummyOperator(task_id="begin")
    end = DummyOperator(task_id="end")

    chain(
        begin,
        create_dataset,
        create_table,
        [check_table_exists, check_table_partition_exists],
        load_data,
        check_bq_row_count,
        delete_dataset,
        end,
    )