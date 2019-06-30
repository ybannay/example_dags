from datetime import datetime
from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
from airflow.models import Variable

ENV_VARS = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'TR_FTP_HOST', 'TR_FTP_USER', 'TR_FTP_PASS', 'TR_FTP_PATH']
environment = dict((env_var, Variable.get(env_var)) for env_var in ENV_VARS)

dag = DAG(
    'tr_esg',
    description='Thomson Reuters ESG DAG',
    schedule_interval='0 12 * * *',
    start_date=datetime(2014, 12, 1)
)

tr_ftp_to_s3 = DockerOperator(
    environment=environment,
    image='064436394451.dkr.ecr.eu-central-1.amazonaws.com/clarity/data/etls/ftp2s3:latest',
    task_id='tr_ftp_to_s3',
    dag=dag
)

tr_load = DockerOperator(
    environment=environment,
    image='clarity/tr_load:latest',
    task_id='tr_load',
    dag=dag
)

tr_ftp_to_s3 >> tr_load

