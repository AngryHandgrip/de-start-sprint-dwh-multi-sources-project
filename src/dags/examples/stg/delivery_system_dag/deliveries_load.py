from logging import Logger
from typing import List

from examples.stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from pydantic import BaseModel
import requests
from datetime import datetime, timedelta


class DeliveryObj(BaseModel):
    id: int
    order_id: str
    order_ts: datetime
    delivery_id: str
    courier_id: str
    address: str
    delivery_ts: datetime
    rate: int
    tip_sum: float


class DeliveriesOriginApi:
    def __init__(self, url: str, headers: dict) -> None:
        self._url = url
        self._headers = headers

    def list_deliveries(self, offset: int, limit: int, wf_setting_last_id: int, sort_field='id', sort_direction='asc') -> List[DeliveryObj]:
        params = {
            'sort_field': sort_field,
            'sort_direction': sort_direction,
            'limit': limit,
            'offset': offset,
            "from": (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
        }
        response = requests.get(self._url, params=params, headers=self._headers)
        data_batch = response.json()
        obj_list = []
        for i, data in enumerate(data_batch):
            obj_list.append(DeliveryObj(
                                        id=wf_setting_last_id + i + 1,
                                        order_id=data['order_id'],
                                        order_ts=datetime.fromisoformat(data['order_ts']),
                                        delivery_id=data['delivery_id'],
                                        courier_id=data['courier_id'],
                                        address=data['address'],
                                        delivery_ts=datetime.fromisoformat(data['delivery_ts']),
                                        rate = data['rate'],
                                        tip_sum=data['tip_sum']
                                    ))
        return obj_list


class DeliveryDestRepository:

    def insert_delivery(self, conn: Connection, delivery: dict) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.deliverysystem_deliveries(id, order_id, order_ts, delivery_id, courier_id, address, delivery_ts, rate, tip_sum)
                    VALUES (
                        %(id)s, 
                        %(order_id)s, 
                        %(order_ts)s, 
                        %(delivery_id)s, 
                        %(courier_id)s, 
                        %(address)s, 
                        %(delivery_ts)s, 
                        %(rate)s,
                        %(tip_sum)s
                    )
                    ON CONFLICT (id) DO UPDATE
                    SET
                        order_id = EXCLUDED.order_id,
                        order_ts = EXCLUDED.order_ts,
                        delivery_id = EXCLUDED.delivery_id,
                        courier_id = EXCLUDED.courier_id,
                        address = EXCLUDED.address,
                        delivery_ts = EXCLUDED.delivery_ts,
                        rate = EXCLUDED.rate,
                        tip_sum = EXCLUDED.tip_sum;
                """,
                {
                    "id": delivery.id,
                    "order_id": delivery.order_id,
                    "order_ts": delivery.order_ts,
                    "delivery_id": delivery.delivery_id,
                    "courier_id": delivery.courier_id,
                    "address": delivery.address,
                    "delivery_ts": delivery.delivery_ts,
                    "rate": delivery.rate,
                    "tip_sum": delivery.tip_sum,
                },
            )


class DeliveryLoader:
    WF_KEY = "deliveries_origin_to_stg_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 50

    def __init__(self, url: str, headers: dict, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DeliveriesOriginApi(url, headers)
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
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: 0})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_deliveries(last_loaded, self.BATCH_LIMIT, wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY])
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
                load_queue = self.origin.list_deliveries(last_loaded, self.BATCH_LIMIT, wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY])
                self.log.info(f"Found {len(load_queue)} deliveries to load.")
                
            self.log.info("Quitting.")
            return