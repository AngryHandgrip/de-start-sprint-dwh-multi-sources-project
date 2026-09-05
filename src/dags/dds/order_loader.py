from logging import Logger
from typing import List

from dds.stg_settings_repository import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from dds.pg_saver import PgSaver
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime


class OrderObj(BaseModel):
    id: int
    order_id: str
    order_status: str
    restaurant_id: str
    timestamp: str
    user_id: str
    update_ts: datetime


class OrdersOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_orders(self, order_threshold: int, limit: int) -> List[OrderObj]:
        with self._db.client().cursor(row_factory=class_row(OrderObj)) as cur:
            cur.execute(
                """
                    SELECT id, object_id AS order_id, object_value::json->>'final_status' AS order_status, object_value::json#>>'{restaurant, id}' AS restaurant_id, object_value::json->>'date' AS timestamp, object_value::json#>>'{user, id}' AS user_id, update_ts
                    FROM stg.ordersystem_orders
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": order_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class OrderDestRepository:

    def insert_order(self, conn: Connection, order: OrderObj) -> None:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TEMP TABLE tmp_orders (
                    order_id VARCHAR,
                    order_status VARCHAR,
                    restaurant_uuid VARCHAR,
                    restaurant_id INT,
                    timestamp TIMESTAMP,
                    timestamp_id INT,
                    user_uuid VARCHAR,
                    user_id INT,
                    update_ts TIMESTAMP,
                    delivery_uuid VARCHAR,
                    delivery_id INT
                ) ON COMMIT DROP;
            """)

            cur.execute(
                """
                    INSERT INTO tmp_orders(order_id, order_status, restaurant_uuid, timestamp, user_uuid, update_ts)
                    VALUES (%(order_id)s, %(order_status)s, %(restaurant_uuid)s, %(timestamp)s, %(user_uuid)s, %(update_ts)s);
                """,
                {
                    "order_id": order.order_id,
                    "order_status": order.order_status,
                    "restaurant_uuid": order.restaurant_id,
                    "timestamp": order.timestamp,
                    "user_uuid": order.user_id,
                    "update_ts": order.update_ts
                },
            )

            cur.execute("""
                UPDATE tmp_orders tmp
                SET delivery_uuid = d.delivery_id
                FROM stg.deliverysystem_deliveries d
                WHERE tmp.order_id = d.order_id
            """)

            cur.execute("""
                UPDATE tmp_orders tmp
                SET restaurant_id = r.id
                FROM dds.dm_restaurants r
                WHERE tmp.restaurant_uuid = r.restaurant_id
                AND tmp.update_ts < r.active_to
                AND tmp.update_ts >= r.active_from; 
            """)

            cur.execute("""
                UPDATE tmp_orders tmp
                SET restaurant_id = r.id
                FROM (
                    SELECT DISTINCT ON (restaurant_id) id, restaurant_id
                    FROM dds.dm_restaurants
                    ORDER BY restaurant_id, active_from ASC
                ) r
                WHERE tmp.restaurant_uuid = r.restaurant_id
                AND tmp.restaurant_id IS NULL;
            """)

            cur.execute("""
                UPDATE tmp_orders tmp
                SET timestamp_id = t.id
                FROM dds.dm_timestamps t
                WHERE tmp.timestamp = t.ts;
            """)

            cur.execute("""
                UPDATE tmp_orders tmp
                SET user_id = u.id
                FROM dds.dm_users u
                WHERE tmp.user_uuid = u.user_id;
            """)

            cur.execute("""
                UPDATE tmp_orders tmp
                SET delivery_id = d.id
                FROM dds.dm_deliveries d
                WHERE tmp.delivery_uuid = d.delivery_id;
            """)

            cur.execute("""
                INSERT INTO dds.dm_orders (
                    user_id, restaurant_id, timestamp_id, order_key, order_status, delivery_id
                )
                SELECT 
                    tmp.user_id, tmp.restaurant_id, tmp.timestamp_id, 
                    tmp.order_id, tmp.order_status, tmp.delivery_id
                FROM tmp_orders tmp
                LEFT JOIN dds.dm_orders target 
                ON target.user_id = tmp.user_id
                AND target.timestamp_id = tmp.timestamp_id
                AND target.order_key = tmp.order_id
                AND target.order_status = tmp.order_status
                AND target.restaurant_id = tmp.restaurant_id
                WHERE target.user_id IS NULL OR target.timestamp_id IS NULL OR target.restaurant_id IS NULL OR target.delivery_id IS NULL;
            """)

        conn.commit()


class OrderLoader:
    WF_KEY = "orders_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 9999 # Условность для быстрой загрузки всех доступных данных

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, pg_saver: PgSaver, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = OrdersOriginRepository(pg_origin)
        self.pg_saver = pg_saver
        self.stg = OrderDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_orders(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_orders(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} orders to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for order in load_queue:
                self.stg.insert_order(conn, order)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
    