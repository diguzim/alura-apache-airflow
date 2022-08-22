from datetime import datetime, date
from os import getcwd
from os.path import join
from airflow.models import DAG
from airflow.operators.alura import TwitterOperator

with DAG(dag_id="titter_dag", start_date=datetime.now()) as dag:
    twitter_operator = TwitterOperator(
        task_id="twitter_alura_online",
        query="AluraOnline",
        file_path=join(
            getcwd(),
            "datalake",
            "twitter_aluraonline",
            f"extract_date={date.today()}",
            f"AluraOnline_{ date.today().strftime('%Y%m%d') }.json"
        ),
    )
