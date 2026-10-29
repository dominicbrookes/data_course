from datetime import datetime
from airflow.sdk import dag, task


@dag(schedule=None, start_date=datetime(2026, 1, 1), catchup=False, tags=["setup"])
def hello_setup():

    @task
    def say_hello():
        return "Hello, Airflow!"

    @task
    def show(message):
        print(message)

    show(say_hello())


hello_setup()