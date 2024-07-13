"""
Доп функции и обертки над методами SDK
"""
import io
import csv
import pytz

from datetime import datetime
from typing import List, Iterator

from mpsiemlib.common import ModuleNames
from .content_helpers import *


def set_jsons_to_table(worker, table_name: str, jsons_list: Iterator[str]):
    tables_upload_batch_size = 1000

    sdk_module = worker.get_module(ModuleNames.TABLES)

    # API экспортирует время в timestamp, но для загрузки данных его надо перевести в нужный формат
    table_info = sdk_module.get_table_info(table_name)
    datetime_fields = []
    for i in table_info.get('fields'):
        if i.get('type') == 'datetime':
            datetime_fields.append(i.get('name'))

    # если мы пытаемся загрузить обратно то, что экспортировали через sdk
    def mp_prepare_data(dct):
        if dct.get('_id') is not None:  # при вставке не нужен _id
            del dct['_id']
        for k, v in dct.items():
            if k in datetime_fields and v is not None and type(v) == int:
                dct[k] = datetime.fromtimestamp(v, tz=pytz.timezone('UTC')).strftime('%d.%m.%Y %H:%M:%S')
            if v is None:  # все None значения меняются на строку "null"
                dct[k] = 'null'
        return dct

    def upload_table_batch(upload_batch: List[dict]):
        if len(upload_batch) == 0:
            return

        header = list(upload_batch[0].keys())

        stream = io.StringIO()
        csv_writer = csv.DictWriter(stream, fieldnames=header, delimiter=';', quoting=csv.QUOTE_NONNUMERIC)
        csv_writer.writeheader()
        csv_writer.writerows(upload_batch)

        sdk_module.set_table_data(table_name, stream.getvalue().encode('utf-8'))

        stream.close()

    batch = []
    for line in jsons_list:
        row = json.loads(line, object_hook=mp_prepare_data)
        batch.append(row)
        if (len(batch) % tables_upload_batch_size) == 0:
            upload_table_batch(batch)
            batch.clear()
    if len(batch) != 0:
        upload_table_batch(batch)
        batch.clear()



