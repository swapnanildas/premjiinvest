from airflow import DAG # type: ignore
from airflow.operators import BashOperator,PythonOperator # type: ignore
from datetime import datetime, timedelta

seven_days_ago = datetime.combine(datetime.today() - timedelta(7),
                                    datetime.min.time())

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': seven_days_ago,
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    }

dag = DAG('simple', default_args=default_args)
t1 = BashOperator(  
        task_id='getfinancedata',
        bash_command='python pipelines/pipeline1.py',
        dag=dag
        )
t2 = BashOperator(  
        task_id='moviedata',
        bash_command='python pipelines/pipeline2.py',
        dag=dag
        )

t1.set_downstream(t2)

