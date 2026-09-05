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
import json


class SaleObj(BaseModel):
    id: int
    order_id: str
    timestamp: datetime
    products: str


class SalesOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_sales(self, sale_threshold: int, limit: int) -> List[SaleObj]:
        with self._db.client().cursor(row_factory=class_row(SaleObj)) as cur:
            cur.execute(
                """
                    SELECT id, event_value::json->>'order_id' AS order_id, event_value::json->>'order_date' AS timestamp, event_value::json->>'product_payments' AS products
                    FROM stg.bonussystem_events
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                        AND event_type = 'bonus_transaction'
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": sale_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class SaleDestRepository:

    def insert_sale(self, conn: Connection, sale: SaleObj) -> None:
        raw_products = sale.products
        sale_items = json.loads(raw_products) if isinstance(raw_products, str) else raw_products    

        rows_to_insert = [
            (
                sale_item['product_id'],
                sale_item['product_cost'],
                sale_item['quantity'],
                sale_item['price'],
                sale_item['bonus_payment'],
                sale_item['bonus_grant'],
                sale.order_id,
                sale.timestamp
            )
            for sale_item in sale_items
        ]

        if not rows_to_insert:
            return

        with conn.cursor() as cur:
            cur.execute("""
                CREATE TEMP TABLE tmp_sales (
                    product_uuid VARCHAR,
                    product_id INT,
                    price NUMERIC,
                    quantity NUMERIC,
                    product_cost NUMERIC,
                    bonus_payment NUMERIC,
                    bonus_grant NUMERIC,
                    timestamp TIMESTAMP,
                    timestamp_id INT,
                    order_uuid VARCHAR,
                    order_id INT
                ) ON COMMIT DROP;
            """)

            cur.executemany(
                """
                INSERT INTO tmp_sales (
                    product_uuid, price, quantity, product_cost, bonus_payment, bonus_grant, order_uuid, timestamp
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                rows_to_insert
            )

            cur.execute("""
                UPDATE tmp_sales tmp
                SET timestamp_id = t.id
                FROM dds.dm_timestamps t
                WHERE tmp.timestamp = t.ts
            """) 

            cur.execute("""
                UPDATE tmp_sales tmp
                SET product_id = p.id
                FROM dds.dm_products p
                WHERE tmp.product_uuid = p.product_id
                AND tmp.timestamp >= p.active_from
                AND tmp.timestamp < p.active_to;
            """) 

            cur.execute("""
                UPDATE tmp_sales tmp
                SET product_id = p.id
                FROM (
                    SELECT DISTINCT ON (product_id) id, product_id
                    FROM dds.dm_products
                    ORDER BY product_id, active_from ASC
                ) p
                WHERE tmp.product_uuid = p.product_id
                AND tmp.product_id IS NULL;
            """)

            cur.execute("""
                UPDATE tmp_sales tmp
                SET order_id = o.id
                FROM dds.dm_orders o
                WHERE tmp.order_uuid = o.order_key
            """) 


            cur.execute("""
                INSERT INTO dds.fct_product_sales (
                    product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant
                )
                SELECT 
                    tmp.product_id, tmp.order_id, tmp.quantity, 
                    tmp.product_cost, tmp.price, tmp.bonus_payment, tmp.bonus_grant
                FROM tmp_sales tmp
                LEFT JOIN dds.fct_product_sales target 
                ON target.product_id = tmp.product_id
                AND target.order_id = tmp.order_id
                AND target.count = tmp.quantity
                AND target.price = tmp.product_cost
                AND target.total_sum = tmp.price
                AND target.bonus_payment = tmp.bonus_payment
                AND target.bonus_grant = tmp.bonus_grant
                WHERE (target.product_id IS NULL OR target.order_id IS NULL) AND tmp.order_id IS NOT NULL;
            """)

        conn.commit()


class SaleLoader:
    WF_KEY = "sales_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 9999  # Условность для быстрой загрузки всех доступных данных

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, pg_saver: PgSaver, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = SalesOriginRepository(pg_origin)
        self.pg_saver = pg_saver
        self.stg = SaleDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_sales(self):
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
            load_queue = self.origin.list_sales(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} sales to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for sale in load_queue:
                self.stg.insert_sale(conn, sale)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
    