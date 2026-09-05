from logging import Logger
from typing import List, Any

from dds.stg_settings_repository import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect, MongoConnect
from lib.dict_util import json2str
from dds.pg_saver import PgSaver
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from bson.objectid import ObjectId
from datetime import datetime


class RestaurantObj(BaseModel):
    id: int
    restaurant_id: str
    restaurant_name: str


class RestaurantsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_restaurants(self, restaurant_threshold: int, limit: int) -> List[RestaurantObj]:
        with self._db.client().cursor(row_factory=class_row(RestaurantObj)) as cur:
            cur.execute(
                """
                    SELECT id, object_value::json->>'_id' AS restaurant_id, object_value::json->>'name' AS restaurant_name 
                    FROM stg.ordersystem_restaurants
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": restaurant_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class RestaurantDestRepository:

    def insert_restaurant(self, conn: Connection, restaurant: RestaurantObj, active_from: Any) -> None:
        high_date = datetime(2099, 12, 31)
    
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH current_rec AS (
                    SELECT restaurant_name
                    FROM dds.dm_restaurants
                    WHERE restaurant_id = %(restaurant_id)s
                    AND active_to = %(high_date)s
                ),
                close_old AS (
                    UPDATE dds.dm_restaurants
                    SET active_to = %(active_from)s
                    WHERE restaurant_id = %(restaurant_id)s
                    AND active_to = %(high_date)s
                    AND restaurant_name IS DISTINCT FROM %(restaurant_name)s
                    RETURNING restaurant_id
                )
    
                INSERT INTO dds.dm_restaurants (id, restaurant_id, restaurant_name, active_from, active_to)
                SELECT 
                    %(id)s, 
                    %(restaurant_id)s, 
                    %(restaurant_name)s, 
                    %(active_from)s, 
                    %(high_date)s
                WHERE NOT EXISTS (SELECT 1 FROM current_rec)
                OR EXISTS (SELECT 1 FROM close_old);
                """,
                {
                    "id": restaurant.id,
                    "restaurant_id": restaurant.restaurant_id,
                    "restaurant_name": restaurant.restaurant_name,
                    "active_from": active_from,
                    "high_date": high_date
                },
            )


class RestaurantLoader:
    WF_KEY = "restaurants_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 999   # Условность для быстрой загрузки всех доступных данных

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, pg_saver: PgSaver,  mc: MongoConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = RestaurantsOriginRepository(pg_origin)
        self.pg_saver = pg_saver
        self.stg = RestaurantDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.mongo_db = mc.client()
        self.log = log

    def load_restaurants(self):
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
            load_queue = self.origin.list_restaurants(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} restaurants to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for restaurant in load_queue:
                self.log.info(self.mongo_db.get_collection("restaurants"))
                filter = {'_id': ObjectId(restaurant.restaurant_id)}
                active_from = self.mongo_db.get_collection("restaurants").find_one(filter=filter).get('update_ts')
                self.log.info(active_from)
                self.stg.insert_restaurant(conn, restaurant, active_from)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
    