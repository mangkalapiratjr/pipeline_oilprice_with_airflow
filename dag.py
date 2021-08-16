from tasks.tasks import get_oilprice, get_exchange_rate, transform_data, load_db
from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

# Config default DAG args
default_args = {
    'owner': 'mangkalapirat',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Schedule to execute every midnight
with DAG (
    'pipeline_oilprice_usd',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval='0 0 * * *',
    start_date=datetime(2021,7,15)
)as dag:

    # Define Task
    start_task = DummyOperator(task_id='Start')

    get_oilprice_task = PythonOperator(
        task_id='Scraping_Oilprice',
        python_callable=get_oilprice
    )

    get_exchange_task = PythonOperator(
        task_id='Scraping_ExchangeRate',
        python_callable=get_exchange_rate
    )

    transform_task = PythonOperator(
        task_id='Transform_Data',
        python_callable=transform_data
    )

    load_to_db_task = PythonOperator(
        task_id='Load_to_DB',
        python_callable=load_db
    )

    end_task = DummyOperator(task_id='End')

    # Set Depedencies
    start_task >> [get_oilprice_task ,get_exchange_task]

    [get_oilprice_task, get_exchange_task] >> transform_task

    transform_task >> load_to_db_task >> end_task





