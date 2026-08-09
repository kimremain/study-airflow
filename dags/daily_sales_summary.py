from datetime import timedelta
import pendulum

from airflow.sdk import dag, get_current_context, task

@dag(
    dag_id="daily_sales_summary",
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(2026, 8, 10, tz="Asia/Seoul"),
    catchup=False,
    tags=["study"],
)
def daily_sales_summary():

    @task
    def show_run_context():
        context = get_current_context()

        print(f"logical_date: {context['logical_date']}")
        print(f"data_interval_start: {context['data_interval_start']}")
        print(f"data_interval_end: {context['data_interval_end']}")

    @task(
        retries=2,
        retry_delay=timedelta(seconds=30),
    )
    def retry_demo():
        context = get_current_context()
        try_number = context["ti"].try_number

        print(f"현재 시도 횟수: {try_number}")

        if try_number == 1:
            raise ValueError("재시도 동작 확인을 위한 첫 번째 실패")

        print("재시도 후 성공!")

    @task
    def extract_sales():
        sales = [12000, 18000, 15000]
        print(f"추출한 매출: {sales}")
        return sales

    @task
    def calculate_summary(sales):
        summary = {
            "total": sum(sales),
            "average": sum(sales) / len(sales),
        }
        print(f"매출 요약: {summary}")
        return summary

    @task
    def print_summary(summary):
        print(f"총매출: {summary['total']}")
        print(f"평균매출: {summary['average']}")        

    @task
    def fail_demo():
        raise ValueError("실패 전파 확인을 위한 의도적인 오류")

    @task.branch
    def choose_sales_path(summary):
        if summary["total"] >= 40000:
            return "high_sales"
        return "low_sales"

    @task
    def high_sales():
        print("매출이 목표 이상입니다.")

    @task
    def low_sales():
        print("매출이 목표 미만입니다.")

    @task(trigger_rule="none_failed_min_one_success")
    def finish():
        print("모든 작업이 종료됐습니다.")

    context_task = show_run_context()
    retry_task = retry_demo()
    context_task >> retry_task
    sales = extract_sales()
    summary = calculate_summary(sales)
    summary_task = print_summary(summary)

    branch_task = choose_sales_path(summary)
    high_task = high_sales()
    low_task = low_sales()

    summary_task >> branch_task
    branch_task >> [high_task, low_task]

    finish_task = finish()
    [retry_task, high_task, low_task] >> finish_task

daily_sales_summary()