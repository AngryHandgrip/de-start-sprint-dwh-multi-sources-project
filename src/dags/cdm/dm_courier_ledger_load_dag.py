from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
import pendulum

with DAG(
    schedule_interval='0 0 1 * *',  # Витрина - ежемесячная сводка по курьерам. Поэтому обновлять ее надо в начале нового месяца
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'cdm'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True,  # Остановлен/запущен при появлении. Сразу запущен.
    dag_id='load_courier_ledger_datamart_dag'
) as dag:

    load_datamart = PostgresOperator(
        task_id="load_datamart",
        postgres_conn_id="PG_WAREHOUSE_CONNECTION",
        sql="dm_courier_ledger_load.sql"
    )