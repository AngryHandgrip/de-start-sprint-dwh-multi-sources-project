import logging

import pendulum
from airflow.decorators import dag, task
from examples.stg.bonus_system_ranks_dag.ranks_loader import RankLoader
from examples.stg.bonus_system_ranks_dag.users_loader import UserLoader
from examples.stg.bonus_system_ranks_dag.events_load import OutboxLoader
from lib import ConnectionBuilder

from airflow.providers.postgres.hooks.postgres import PostgresHook
import json

log = logging.getLogger(__name__)   


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'stg', 'origin', 'example'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def stg_bonus_system_ranks_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Создаем подключение к базе подсистемы бонусов.
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="ranks_load")
    def load_ranks():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = RankLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_ranks()  # Вызываем функцию, которая перельет данные.

    @task(task_id="users_load")
    def load_users():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = UserLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_users()  # Вызываем функцию, которая перельет данные.

    @task(task_id="events_load")
    def load_events():
        # # создаем экземпляр класса, в котором реализована логика.
        source_hook = PostgresHook(postgres_conn_id='PG_ORIGIN_BONUS_SYSTEM_CONNECTION')
        target_hook = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')
        cursor_row = target_hook.get_records("""
            SELECT workflow_settings FROM stg.srv_wf_settings 
            WHERE workflow_key = 'outbox_load'
        """)

        last_id = 0
        if cursor_row:
            last_id = int(cursor_row[0][0].get('last_id', 0))

        rows = source_hook.get_records("""
            SELECT id, event_ts, event_type, event_value 
            FROM outbox 
            WHERE id > %s 
            ORDER BY id ASC
        """, parameters=(last_id,))

        if not rows:
            print("Нет новых событий для загрузки")
            return

        with target_hook.get_conn() as conn:
            with conn.cursor() as cur:
                for row in rows:
                    cur.execute("""
                        INSERT INTO stg.bonussystem_events (id, event_ts, event_type, event_value)
                        VALUES (%s, %s, %s, %s)
                    """, (row[0], row[1], row[2], row[3]))

                new_last_id = rows[-1][0]
                workflow_settings = json.dumps({'last_id': new_last_id})
                cur.execute("""
                    INSERT INTO stg.srv_wf_settings (workflow_key, workflow_settings)
                    VALUES ('outbox_load', %s)
                    ON CONFLICT (workflow_key) DO UPDATE SET
                        workflow_settings = EXCLUDED.workflow_settings
                """, (workflow_settings,))

                conn.commit()

        print(f"Загружено {len(rows)} событий. Последний id: {new_last_id}")

    # Инициализируем объявленные таски.
    ranks_dict = load_ranks()
    users_dict = load_users()
    events_dict = load_events()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    # [ranks_dict, users_dict, events_dict]   # type: ignore
    [ranks_dict, users_dict, events_dict]


stg_bonus_system_ranks_dag = stg_bonus_system_ranks_dag()
