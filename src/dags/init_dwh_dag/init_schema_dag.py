import pendulum
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
    dag_id="dwh_init_ddl_dag",
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'stg', 'schema', 'ddl'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
) as dag:
    run_ddl = PostgresOperator(
        task_id="run_postgres_ddl",
        postgres_conn_id="PG_WAREHOUSE_CONNECTION",
        sql="scripts/dwh_ddl.sql",
    )