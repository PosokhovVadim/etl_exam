import uuid
import datetime
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.yandex.operators.yandexcloud_dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
    DataprocDeleteClusterOperator,
)

YC_DP_AZ = 'ru-central1-a'
YC_DP_SSH_PUBLIC_KEY = 'ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIBChFrJo1ElExHe0nPgKo3zV49ALke+apmgtguVlA0/X data-proc-key'
YC_DP_SUBNET_ID = 'e9btfq1v2s5x4wz8mnp0'
YC_DP_SA_ID = 'ajer12lc7rplnxe39f2m'
YC_BUCKET = 'dataproc'

with DAG(
        'DATA_INGEST',
        schedule_interval='@hourly',
        tags=['data-processing-and-airflow'],
        start_date=datetime.datetime.now(),
        max_active_runs=1,
        catchup=False
) as ingest_dag:
    create_cluster = DataprocCreateClusterOperator(
        task_id='dp-cluster-create-task',
        cluster_name=f'tmp-dp-{uuid.uuid4()}',
        ssh_public_keys=YC_DP_SSH_PUBLIC_KEY,
        service_account_id=YC_DP_SA_ID,
        subnet_id=YC_DP_SUBNET_ID,
        s3_bucket=YC_BUCKET,
        zone=YC_DP_AZ,
        cluster_image_version='2.1',
        masternode_resource_preset='s2.small', 
        masternode_disk_type='network-hdd',
        masternode_disk_size=32,  
        computenode_resource_preset='s2.small',  
        computenode_disk_type='network-hdd',
        computenode_disk_size=32,  
        computenode_count=1,  
        computenode_max_hosts_count=3,
    )

    run_spark = DataprocCreatePysparkJobOperator(
        task_id='dp-cluster-pyspark-task',
        main_python_file_uri=f's3a://{YC_BUCKET}/scripts/spark_batch_job.py',
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id='dp-cluster-delete-task',
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_cluster >> run_spark >> delete_cluster