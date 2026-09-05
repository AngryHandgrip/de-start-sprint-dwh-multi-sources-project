import logging

import pendulum
from airflow.decorators import dag, task
from examples.stg.delivery_system_dag.couriers_load import CourierLoader
from examples.stg.delivery_system_dag.deliveries_load import DeliveryLoader
from lib import ConnectionBuilder
from airflow.models.variable import Variable

from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)   

@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'stg', 'origin', 'api'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def stg_delivery_system_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    headers = {
        'X-Nickname': Variable.get('X-NICKNAME'),
        'X-Cohort': Variable.get('X-COHORT'),
        'X-API-KEY': Variable.get('X-API-KEY')
    }
    base_url = Variable.get('BASE_URL')

    # Объявляем таск, который загружает данные.
    @task(task_id="couriers_load")
    def load_couriers():
        url = base_url + 'couriers'

        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = CourierLoader(url, headers, dwh_pg_connect, log)
        rest_loader.load_couriers()  # Вызываем функцию, которая перельет данные.

    @task(task_id="deliveries_load")
    def load_deliveries():
        url = base_url + 'deliveries'

        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = DeliveryLoader(url, headers, dwh_pg_connect, log)
        rest_loader.load_deliveries()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    couriers_dict = load_couriers()
    deliveries_dict = load_deliveries()

    # Далее задаем последовательность выполнения тасков.
    [couriers_dict, deliveries_dict]


delivery_system_dag_instance = stg_delivery_system_dag()
