import sys
sys.path.insert(0, "/home/airflow/gcs/data")

from airflow import DAG
from datetime import datetime
from airflow.decorators import task
from airflow.utils.trigger_rule import TriggerRule

from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator,BranchPythonOperator
from airflow.operators.bash import BashOperator


from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from python_functions import generate_number,branch_function,get_variables,print_params,print_triggered_configs


PROJECT_ID="my_project_name"
REGION="us-central1"
CLUSTER_NAME="cluster-78"
PYSPARK_SCRIPT = "gs://dataproc-staging-us-central1-48345-pqr3a5dk/pyspark-scripts/pyspark_script.py"

dataproc_submit_cmd=f"""
gcloud dataproc jobs submit pyspark {PYSPARK_SCRIPT} \
    --cluster={CLUSTER_NAME} \
    --region={REGION} \
    --project={PROJECT_ID}
"""

dag=DAG(
    "control_flow",
    default_args={
        "owner":"airflow",
        "start_date":datetime(2024,1,1),
        "retries":1
    },
    params={"name":"akshay","age":25},
    catchup=False
)

start_task=DummyOperator(
    task_id="starting",
    dag=dag
)
 
task_1=PythonOperator(
    task_id="generate_number",
    python_callable=generate_number,
    dag=dag,
    provide_context=True
)

task_2=BranchPythonOperator(
    task_id="xcom_branching",
    python_callable=branch_function,
    dag=dag,
    provide_context=True
)

task_3=BashOperator(
    task_id="bash_printing_larger",
    bash_command="echo i am greater than 5",
    dag=dag
)

task_4=BashOperator(
    task_id="bash_printing_lower",
    bash_command="echo i am lesser than 5",
    dag=dag
)

task_5=BashOperator(
    task_id="pyspark_bash",
    bash_command=dataproc_submit_cmd,
    dag=dag
)

task_6 = DataprocSubmitJobOperator(
    task_id="dataproc_print",
    job={
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {"main_python_file_uri": PYSPARK_SCRIPT},
    },
    region=REGION,
    project_id=PROJECT_ID,
    dag=dag
)

task_7=PythonOperator(
    task_id="Enviroment_and_Context_Variables",
    python_callable=get_variables,
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS, 
    dag=dag
)

@task
def task_8():
    print("executing task 8 with taskflow api")


task_9 = GCSObjectExistenceSensor(
    task_id="check_gcs_object",
    bucket="sumne_bucket",
    object="demo.txt",  
    google_cloud_conn_id="google_cloud_default",
    timeout=600, 
    poke_interval=30,  
    mode="poke",  
    dag=dag
)

task_10 = GCSToBigQueryOperator(
    task_id="load_gcs_to_bq",
    bucket="sumne_bucket",
    source_objects=["demo.txt"],  
    destination_project_dataset_table="my_project_name.python_dataset.dag_table",
    source_format="CSV",
    skip_leading_rows=1,
    write_disposition="WRITE_APPEND",
    create_disposition="CREATE_IF_NEEDED", 
    gcp_conn_id="google_cloud_default", 
    dag=dag
)

task_11=PythonOperator(
    task_id="dag_level_params",
    python_callable=print_params,
    dag=dag
)

task_12=PythonOperator(
    task_id="trigger_configs",
    python_callable=print_triggered_configs,
    dag=dag
)

end_task=DummyOperator(
    task_id="ending",
    dag=dag
)

start_task >> task_1 >> task_2 >> [task_3,task_4]
task_3 >> task_5 
task_4 >> task_6
[task_5,task_6] >> task_7 >> [task_8(),task_9] >> task_10 >> task_11 >> task_12 >> end_task