import logging

import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from dds.pg_saver import PgSaver
from dds.user_loader import UserLoader
from dds.restaurant_loader import RestaurantLoader
from dds.ts_loader import TimestampLoader
from dds.product_loader import ProductLoader
from dds.order_loader import OrderLoader
from dds.sale_loader import SaleLoader
from dds.courier_loader import CourierLoader
from dds.delivery_loader import DeliveryLoader
from lib import ConnectionBuilder, MongoConnect

from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)   



@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'dds'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def load_dds_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    cert_path = Variable.get("MONGO_DB_CERTIFICATE_PATH")
    db_user = Variable.get("MONGO_DB_USER")
    db_pw = Variable.get("MONGO_DB_PASSWORD")
    rs = Variable.get("MONGO_DB_REPLICA_SET")
    db = Variable.get("MONGO_DB_DATABASE_NAME")
    host = Variable.get("MONGO_DB_HOST")

    @task(task_id="users_load")
    def load_users():
        pg_saver = PgSaver()
        # # создаем экземпляр класса, в котором реализована логика.
        rest_loader = UserLoader(dwh_pg_connect, dwh_pg_connect, pg_saver, log)
        rest_loader.load_users()  # Вызываем функцию, которая перельет данные.

    @task(task_id="restaurants_load")
    def load_restaurants():
        pg_saver = PgSaver()
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

        rest_loader = RestaurantLoader(dwh_pg_connect, dwh_pg_connect, pg_saver, mongo_connect, log)
        rest_loader.load_restaurants()  # Вызываем функцию, которая перельет данные.

    @task(task_id="timestamps_load")
    def load_timestamps():
        pg_saver = PgSaver()
        # # создаем экземпляр класса, в котором реализована логика.
        rest_loader = TimestampLoader(dwh_pg_connect, dwh_pg_connect, pg_saver, log)
        rest_loader.load_timestamps()  # Вызываем функцию, которая перельет данные.

    @task(task_id="products_load")
    def load_products():
        pg_saver = PgSaver()
        # # создаем экземпляр класса, в котором реализована логика.
        rest_loader = ProductLoader(dwh_pg_connect, dwh_pg_connect, pg_saver, log)
        rest_loader.load_products()  # Вызываем функцию, которая перельет данные.

    @task(task_id="orders_load")
    def load_orders():
        pg_saver = PgSaver()
        # # создаем экземпляр класса, в котором реализована логика.
        rest_loader = OrderLoader(dwh_pg_connect, dwh_pg_connect, pg_saver, log)
        rest_loader.load_orders()  # Вызываем функцию, которая перельет данные.

    @task(task_id="sales_load")
    def load_sales():
        pg_saver = PgSaver()
        # # создаем экземпляр класса, в котором реализована логика.
        rest_loader = SaleLoader(dwh_pg_connect, dwh_pg_connect, pg_saver, log)
        rest_loader.load_sales()  # Вызываем функцию, которая перельет данные.

    @task(task_id="couriers_load")
    def load_couriers():
        # # создаем экземпляр класса, в котором реализована логика.
        rest_loader = CourierLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_couriers()  # Вызываем функцию, которая перельет данные.

    @task(task_id="deliveries_load")
    def load_deliveries():
        # # создаем экземпляр класса, в котором реализована логика.
        rest_loader = DeliveryLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_deliveries()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    users_dict = load_users()
    restaurants_dict = load_restaurants()
    timestamps_dict = load_timestamps()
    products_dict = load_products()
    orders_dict = load_orders()
    sales_dict = load_sales()
    couriers_dict = load_couriers()
    deliveries_dict = load_deliveries()

    # Далее задаем последовательность выполнения тасков.
    [users_dict, restaurants_dict, timestamps_dict, couriers_dict] >> deliveries_dict >> [products_dict, orders_dict] >> sales_dict


stg_order_system_ranks_dag = load_dds_dag()