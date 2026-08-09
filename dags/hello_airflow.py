from airflow.sdk import dag, task
import pendulum

@dag(
    dag_id="hello_airflow",
    schedule=None,
    start_date=pendulum.datetime(2026, 8, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["study"],
)
def hello_airflow():

    @task
    def extract():
        return {"message": "Hello, Airflow!"}

    @task
    def transform(data):
        return data["message"].upper()

    @task
    def load(message):
        print(f"결과: {message}")

    data = extract()
    message = transform(data)
    load(message)

hello_airflow()
