import json
from datetime import datetime, date
from pathlib import Path
from os.path import join
from os import getcwd

from airflow.models import BaseOperator, DAG, TaskInstance
from airflow.utils.decorators import apply_defaults
from hooks.twitter_hook import TwitterHook

class TwitterOperator(BaseOperator):

    template_fields = [
        "query",
        "file_path",
        "start_time",
        "end_time"
    ]

    @apply_defaults
    def __init__(
        self,
        query,
        file_path,
        conn_id = None,
        start_time = None,
        end_time = None,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.query = query
        self.file_path = file_path
        self.conn_id = conn_id
        self.start_time = start_time
        self.end_time = end_time


    def create_parent_folder(self):
        Path(Path(self.file_path).parent).mkdir(parents=True, exist_ok=True)


    def execute(self, context):
        hook = TwitterHook(
            query=self.query,
            conn_id=self.conn_id,
            start_time=self.start_time,
            end_time=self.end_time,
        )

        self.create_parent_folder()
        with open(self.file_path, "w") as output_file:
            for page in hook.run():
                json.dump(page, output_file, ensure_ascii=False)
                output_file.write("\n")

if __name__ =="__main__":
    with DAG(dag_id="TwitterTest", start_date=datetime.now()) as dag:
        twitter_operator = TwitterOperator(
            query="AluraOnline",
            file_path=join(
                getcwd(),
                "datalake",
                "twitter_aluraonline",
                f"extract_date={date.today()}",
                f"AluraOnline_{ date.today().strftime('%Y%m%d') }.json"
            ),
            task_id="test_run"
        )

        task_instance = TaskInstance(
            task=twitter_operator
        )

        twitter_operator.execute(task_instance)
