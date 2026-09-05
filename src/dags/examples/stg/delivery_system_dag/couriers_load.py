from logging import Logger
from typing import List

from examples.stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from pydantic import BaseModel
import requests
from datetime import datetime, timedelta


class CourierObj(BaseModel):
    id: int
    courier_id: str
    courier_name: str


class CouriersOriginApi:
    def __init__(self, url: str, headers: dict) -> None:
        self._url = url
        self._headers = headers

    def list_couriers(self, offset: int, limit: int, wf_setting_last_id: int, sort_field='id', sort_direction='asc') -> List[CourierObj]:
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
            obj_list.append(CourierObj(
                                        id=wf_setting_last_id + i + 1,
                                        courier_id=data['_id'],
                                        courier_name=data['name']
                                    ))
        return obj_list


class CourierDestRepository:

    def insert_courier(self, conn: Connection, courier: dict) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.deliverysystem_couriers(id, courier_id, courier_name)
                    VALUES (%(id)s, %(courier_id)s, %(courier_name)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        courier_id = EXCLUDED.courier_id,
                        courier_name = EXCLUDED.courier_name;
                """,
                {
                    "id": courier.id,
                    "courier_id": courier.courier_id,
                    "courier_name": courier.courier_name
                },
            )


class CourierLoader:
    WF_KEY = "couriers_origin_to_stg_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 50

    def __init__(self, url: str, headers: dict, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = CouriersOriginApi(url, headers)
        self.stg = CourierDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_couriers(self):
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
            load_queue = self.origin.list_couriers(last_loaded, self.BATCH_LIMIT, wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY])
            self.log.info(f"Found {len(load_queue)} couriers to load.")
            while load_queue:
                # Сохраняем объекты в базу dwh.
                for courier in load_queue:
                    self.stg.insert_courier(conn, courier)

                # Сохраняем прогресс.
                # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
                # либо откатятся все изменения целиком.
                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
                wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
                self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

                self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")

                last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
                load_queue = self.origin.list_couriers(last_loaded, self.BATCH_LIMIT, wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY])
                self.log.info(f"Found {len(load_queue)} couriers to load.")

            self.log.info("Quitting.")
            return