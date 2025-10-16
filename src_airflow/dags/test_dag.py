"""
test_dag.py - A comprehensive test DAG for your Airflow setup
Place this in: warehouse/airflow/dags/test_dag.py
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

# Default arguments for all tasks
default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
        'test_dag',
        default_args=default_args,
        description='A test DAG to verify Airflow setup',
        start_date=datetime(2025, 10, 16),
        schedule=None,
        catchup=False,  # Don't backfill
        tags=['test', 'example'],
) as dag:
    # Task 1: Simple Python function
    def print_hello():
        """Simple Python task"""
        print("Hello from Airflow!")
        print(f"Execution date: {datetime.now()}")
        return "Success!"


    hello_task = PythonOperator(
        task_id='say_hello',
        python_callable=print_hello,
    )


    # Task 2: Python task that does calculations
    def do_calculations():
        """Task that performs calculations"""
        import random
        numbers = [random.randint(1, 100) for _ in range(10)]
        result = {
            'numbers': numbers,
            'sum': sum(numbers),
            'average': sum(numbers) / len(numbers),
            'max': max(numbers),
            'min': min(numbers),
        }
        print(f"Results: {result}")
        return result


    calc_task = PythonOperator(
        task_id='do_calculations',
        python_callable=do_calculations,
    )

    # Task 3: Bash command
    bash_task = BashOperator(
        task_id='run_bash_command',
        bash_command='echo "Running in pod: $HOSTNAME" && date && pwd && ls -la',
    )

    # Task 4: KubernetesPodOperator - runs in a separate pod
    k8s_task = KubernetesPodOperator(
        task_id='kubernetes_pod_task',
        name='test-pod',
        namespace='airflow',
        image='python:3.9-slim',
        cmds=['python', '-c'],
        arguments=[
            'print("Running in a separate Kubernetes pod!"); '
            'import sys; print(f"Python version: {sys.version}"); '
            'print("This pod will be deleted after completion")'
        ],
        labels={'app': 'airflow-test'},
        get_logs=True,
        is_delete_operator_pod=True,  # Clean up pod after completion
    )


    # Task 5: Task that uses XCom (inter-task communication)
    def process_data(**context):
        """Task that retrieves data from previous task"""
        # Get the result from calc_task
        ti = context['ti']
        calc_result = ti.xcom_pull(task_ids='do_calculations')

        if calc_result:
            print(f"Received data from previous task: {calc_result}")
            print(f"Processing sum: {calc_result['sum']}")
        else:
            print("No data received from previous task")

        return "Data processed successfully"


    process_task = PythonOperator(
        task_id='process_data',
        python_callable=process_data,
        provide_context=True,
    )


    # Task 6: Final summary task
    def print_summary():
        """Final task that summarizes the DAG run"""
        print("=" * 50)
        print("DAG Execution Summary")
        print("=" * 50)
        print("✅ All tasks completed successfully!")
        print(f"Completed at: {datetime.now()}")
        print("=" * 50)


    summary_task = PythonOperator(
        task_id='print_summary',
        python_callable=print_summary,
    )

    # Define task dependencies (execution order)
    # hello_task runs first, then calc and bash run in parallel
    # Then k8s_task, then process_task, finally summary_task
    hello_task >> [calc_task, bash_task] >> k8s_task >> process_task >> summary_task