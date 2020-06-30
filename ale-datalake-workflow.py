import airflow

from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2),
    'retries': 0
}

dag = DAG(
    dag_id='PHM-ALE-Datalake-Workflow',
    default_args=default_args,
    schedule_interval=timedelta(minutes=120),  # '0 0 * * *',
    dagrun_timeout=timedelta(minutes=119),
    catchup=False
)

fill_datalake = KubernetesPodOperator(
    namespace="airflow",
    name="datalake-prep",
    task_id='prep-datalake',
    dag=dag,
    default_args=default_args,
    image='mragesh/phm-ale-py-code:latest',
    image_pull_secrets="docker-secret",
    cmds=["python"],
    arguments=[u'prep-data-lake.py'],
    get_logs=True)

run_spark = KubernetesPodOperator(
    namespace="airflow",
    name="ETL-spark",
    service_account_name="airflow",
    task_id='run-transform',
    dag=dag,
    default_args=default_args,
    image='mragesh/spark-py:latest',
    image_pull_policy='Always',
    cmds=["/opt/entrypoint.sh"],
    arguments=['/opt/spark/python/app_src_code/submit-job.sh',
               's3a://application-code/spark_jobs/phm_alinity_i_205_results_etl.py'],
    get_logs=True)

fill_datalake >> run_spark

sync_partitions = KubernetesPodOperator(
    namespace="airflow",
    name="sync-partitions",
    service_account_name="airflow",
    task_id='run-sync-partitions',
    dag=dag,
    default_args=default_args,
    image='mragesh/presto-cli:latest',
    image_pull_policy='Always',
    cmds=["/opt/sync_partitions.sh"],
    get_logs=True)

run_spark >> sync_partitions

if __name__ == "__main__":
    dag.cli()
