from logging import Logger
from typing import List

from dds.stg_settings_repository import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from dds.pg_saver import PgSaver
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
import json


class ProductObj(BaseModel):
    id: int
    restaurant_id: str
    restaurant_menu: str
    update_ts: str


class ProductsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_products(self, product_threshold: int, limit: int) -> List[ProductObj]:
        with self._db.client().cursor(row_factory=class_row(ProductObj)) as cur:
            cur.execute(
                """
                    SELECT id, object_id AS restaurant_id, object_value::json->>'menu' AS restaurant_menu, object_value::json->>'update_ts' AS update_ts
                    FROM stg.ordersystem_restaurants
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": product_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class ProductDestRepository:

    def insert_product(self, conn: Connection, product_obj: ProductObj) -> None:
        raw_menu = product_obj.restaurant_menu
        menu_items = json.loads(raw_menu) if isinstance(raw_menu, str) else raw_menu

        rows_to_insert = [
            (
                product['_id'],
                product['name'],
                product['price'],
                product_obj.update_ts,
                '2099-12-31 00:00:00.000',
                product_obj.restaurant_id
            )
            for product in menu_items
        ]

        if not rows_to_insert:
            return

        with conn.cursor() as cur:
            cur.execute("""
                CREATE TEMP TABLE tmp_products (
                    product_id VARCHAR,
                    product_name VARCHAR,
                    product_price NUMERIC,
                    active_from TIMESTAMP,
                    active_to TIMESTAMP,
                    restaurant_uuid VARCHAR,
                    restaurant_id INT
                ) ON COMMIT DROP;
            """)

            cur.executemany(
                """
                INSERT INTO tmp_products (
                    product_id, product_name, product_price, active_from, active_to, restaurant_uuid
                ) VALUES (%s, %s, %s, %s, %s, %s)
                """,
                rows_to_insert
            )

            cur.execute("""
                UPDATE tmp_products tmp
                SET restaurant_id = r.id
                FROM dds.dm_restaurants r
                WHERE tmp.restaurant_uuid = r.restaurant_id
                AND tmp.active_from >= r.active_from
                AND tmp.active_from < r.active_to;
            """)

            cur.execute("""
                UPDATE dds.dm_products target
                SET active_to = tmp.active_from
                FROM tmp_products tmp
                WHERE target.product_id = tmp.product_id
                AND target.active_to = '2099-12-31 00:00:00.000'
                AND (
                    target.product_name != tmp.product_name 
                OR target.product_price != tmp.product_price 
                OR target.restaurant_id != tmp.restaurant_id
                );
            """)

            cur.execute("""
                INSERT INTO dds.dm_products (
                    product_id, product_name, product_price, active_from, active_to, restaurant_id
                )
                SELECT 
                    tmp.product_id, tmp.product_name, tmp.product_price, 
                    tmp.active_from, tmp.active_to, tmp.restaurant_id
                FROM tmp_products tmp
                LEFT JOIN dds.dm_products target 
                ON target.product_id = tmp.product_id
                AND target.active_to = '2099-12-31 00:00:00.000'
                AND target.product_name = tmp.product_name
                AND target.product_price = tmp.product_price
                AND target.restaurant_id = tmp.restaurant_id
                WHERE target.product_id IS NULL;
            """)

        conn.commit()


class ProductLoader:
    WF_KEY = "products_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 999  # Условность для быстрой загрузки всех доступных данных

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, pg_saver: PgSaver, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = ProductsOriginRepository(pg_origin)
        self.pg_saver = pg_saver
        self.stg = ProductDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_products(self):
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
            load_queue = self.origin.list_products(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} products to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for product in load_queue:
                self.stg.insert_product(conn, product)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
    