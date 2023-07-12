
"""
Creado por: Yohan Alfonso Hernandez
Fecha 05-07-2023
Tema: dag en python para ETL en dataflow
"""
import datetime
from airflow import models
from airflow.models import DAG
from e_Apache_beam_data_to_bq import run
from datetime import timedelta, datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


#YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

dag_id = 'my_dag'

default_dag_args = {
    "owner": "Yohan Alfonso",
    "depends_on_past": False,
    "email": ["yohan.alher@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": 2,
    "start_date": datetime(2023, 7, 11)
}

dag = DAG(dag_id, catchup=False, default_args=default_dag_args, schedule_interval=None)


begin = DummyOperator(
                    task_id='Begin',
                    dag=dag)

ejecuta_dataflow = PythonOperator(
                    task_id='ejecuta_dataflow',
                    python_callable=run,
                    provide_context=True,
                    dag=dag
                    )

end = DummyOperator(
                    task_id='End',
                    dag=dag)

begin >> ejecuta_dataflow >> end

