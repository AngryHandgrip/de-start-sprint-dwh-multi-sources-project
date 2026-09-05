from logging import Logger
from typing import List

from dds.stg_settings_repository import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime


class DeliveryObj(BaseModel):
    id: int
    delivery_id: str
    courier_uuid: str
    address: str
    delivery_ts: datetime
    rate: int
    tip_sum: float


class DeliveriesOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_deliveries(self, courier_threshold: int, limit: int) -> List[DeliveryObj]:
        with self._db.client().cursor(row_factory=class_row(DeliveryObj)) as cur:
            cur.execute(
                """
                    SELECT id, delivery_id, courier_id AS courier_uuid, address, delivery_ts, rate, tip_sum
                    FROM stg.deliverysystem_deliveries
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": courier_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class DeliveryDestRepository:
    def insert_delivery(self, conn: Connection, delivery: DeliveryObj) -> None:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TEMP TABLE tmp_deliveries (
                    delivery_id VARCHAR,
                    courier_uuid VARCHAR,
                    courier_id INT,
                    address VARCHAR,
                    delivery_ts TIMESTAMP,
                    rate INT,
                    tip_sum NUMERIC(14, 2)
                ) ON COMMIT DROP;
            """)

            cur.execute(
                """
                    INSERT INTO tmp_deliveries(delivery_id, courier_uuid, address, delivery_ts, rate, tip_sum)
                    VALUES (%(delivery_id)s, %(courier_uuid)s, %(address)s, %(delivery_ts)s, %(rate)s, %(tip_sum)s);
                """,
                {
                    "delivery_id": delivery.delivery_id,
                    "courier_uuid": delivery.courier_uuid,
                    "address": delivery.address,
                    "delivery_ts": delivery.delivery_ts,
                    "rate": delivery.rate,
                    "tip_sum": delivery.tip_sum
                },
            )

            cur.execute("""
                UPDATE tmp_deliveries tmp
                SET courier_id = c.id
                FROM dds.dm_couriers c
                WHERE tmp.courier_uuid = c.courier_id;
            """)

            cur.execute("""
                INSERT INTO dds.dm_deliveries (
                    delivery_id, courier_id, address, delivery_ts, rate, tip_sum
                )
                SELECT 
                    tmp.delivery_id, tmp.courier_id, tmp.address, 
                    tmp.delivery_ts, tmp.rate, tmp.tip_sum
                FROM tmp_deliveries tmp
                LEFT JOIN dds.dm_deliveries target 
                ON target.delivery_id = tmp.delivery_id
                AND target.courier_id = tmp.courier_id
                AND target.address = tmp.address
                AND target.delivery_ts = tmp.delivery_ts
                AND target.rate = tmp.rate
                AND target.tip_sum = tmp.tip_sum
                WHERE target.delivery_id IS NULL;
            """)

        conn.commit()


class DeliveryLoader:
    WF_KEY = "deliveries_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 50

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DeliveriesOriginRepository(pg_origin)
        self.stg = DeliveryDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_deliveries(self):
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
            load_queue = self.origin.list_deliveries(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} deliveries to load.")
            while load_queue:
                # Сохраняем объекты в базу dwh.
                for delivery in load_queue:
                    self.stg.insert_delivery(conn, delivery)

                # Сохраняем прогресс.
                # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
                # либо откатятся все изменения целиком.
                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
                wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
                self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

                self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
                last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
                load_queue = self.origin.list_deliveries(last_loaded, self.BATCH_LIMIT)
                self.log.info(f"Found {len(load_queue)} deliveries to load.")
            self.log.info("Quitting.")
            return