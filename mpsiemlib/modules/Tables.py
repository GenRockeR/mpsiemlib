from datetime import datetime
from typing import Iterator, List, Optional

from mpsiemlib.common import ModuleInterface, MPSIEMAuth, LoggingHandler, MPComponents, Settings
from mpsiemlib.common import exec_request, get_metrics_start_time, get_metrics_took_time


class Tables(ModuleInterface, LoggingHandler):
    """Tables module."""
    __table_add_time_format = '%d.%m.%Y %H:%M:%S'

    __api_table_info = '/api/events/v2/table_lists/{}?siem_id={}'
    __api_table_search = '/api/events/v2/table_lists/{}/content/search?siem_id={}'
    __api_table_truncate = '/api/events/v2/table_lists/{}/content?siem_id={}'
    __api_table_list = '/api/events/v2/table_lists?siem_id={}'
    __api_table_import = '/api/events/v1/table_lists/{}/import?siem_id={}'
    __api_table_add_row = '/api/events/v2/table_lists/{}/content?siem_id={}'

    def __init__(self, auth: MPSIEMAuth, settings: Settings):
        ModuleInterface.__init__(self, auth, settings)
        LoggingHandler.__init__(self)
        self.__core_session = auth.connect(MPComponents.CORE)
        self.__core_hostname = auth.creds.core_hostname
        self.__core_version = auth.get_core_version()
        self.__tables_cache = {}
        self.log.debug('status=success, action=prepare, msg="Table Module init"')

    def get_tables_list(self, siem_id=None) -> dict:
        """Получить список всех установленных табличных списков :param siem_id:
        UUID конвейера.

        :return: {'id': 'name'}
        """
        self.log.debug('status=prepare, action=get_tables_list, msg="Try to get table list", '
                       'hostname="{}", conveyor_id="{}"'.format(self.__core_hostname, siem_id))

        api_url = self.__api_table_list.format(siem_id)
        url = f'https://{self.__core_hostname}{api_url}'
        rq = exec_request(self.__core_session, url, method='GET', timeout=self.settings.connection_timeout)
        self.__tables_cache.clear()
        response = rq.json()
        for i in response:
            self.__tables_cache[i['name']] = {'id': i.get('token'),
                                              'type': i.get('fillType').lower(),
                                              'editable': i.get('editable'),
                                              'ttl_enabled': i.get('ttlEnabled'),
                                              'notifications': i.get('notifications')}

        self.log.info('status=success, action=get_table_list, msg="Found {} tables", '
                      'hostname="{}", conveyor_id="{}"'.format(len(self.__tables_cache), self.__core_hostname,
                                                               siem_id))

        return self.__tables_cache

    def get_table_data(self, table_name: str, filters=None, siem_id=None) -> Iterator[dict]:
        """Итеративно загружаем содержимое табличного списка.

        Пример фильтра:     filters = {"select": ["_last_changed",
        "field2", "field3"],            "where": "_id>5",
        "orderBy": [{"field": "_last_changed",
        "sortOrder": "descending"}],            "timeZone": 0}

        :param table_name: Имя таблицы
        :param filters: Фильтр, опционально
        :param siem_id: UUID конвейера
        :return: Итератор по строкам таблицы
        """
        api_url = self.__api_table_search.format(self.get_table_id_by_name(table_name, siem_id), siem_id)
        url = f'https://{self.__core_hostname}{api_url}'
        params = {'filter': {'where': '',
                             'orderBy': [{'field': '_last_changed',
                                          'sortOrder': 'descending'}],
                             'timeZone': 0}
                  }

        if filters is not None:
            params['filter'] = filters

        # Пачками выгружаем содержимое таблички
        is_end = False
        offset = 0
        limit = self.settings.tables_batch_size
        line_counter = 0
        start_time = get_metrics_start_time()
        while not is_end:
            ret = self.__iterate_table(url, params, offset, limit)
            if len(ret) < limit:
                is_end = True
            offset += limit
            for i in ret:
                line_counter += 1
                yield i
        took_time = get_metrics_took_time(start_time)

        self.log.info('status=success, action=get_table_data, msg="Query executed, response have been read", '
                      'hostname="{}", conveyor_id="{}", lines={}'.format(self.__core_hostname,
                                                                         siem_id, line_counter))
        self.log.info(
            'hostname="{}", conveyor_id="{}", metric=get_table_data, took={}ms, objects={}'.
            format(self.__core_hostname, siem_id, took_time, line_counter))

    def __iterate_table(self, url, params, offset, limit):
        params['offset'] = offset
        params['limit'] = limit
        rq = exec_request(self.__core_session,
                          url,
                          method='POST',
                          timeout=self.settings.connection_timeout,
                          json=params)
        response = rq.json()
        if response is None or 'items' not in response:
            self.log.error('status=failed, action=table_iterate, msg="Table data request return None or '
                           'has wrong response structure", '
                           'hostname="{}"'.format(self.__core_hostname))
            raise Exception('Table data request return None or has wrong response structure')

        return response.get('items')

    def set_table_data(self, table_name: str, data: bytes, siem_id=None) -> None:
        """Импортировать бинарные данные в табличный список. Данные должны быть
        в формате CSV, понятном MP SIEM.

        Usage:
            with open("import.csv", "rb") as data:
                Tables.set_data("table_name", data)

        :param table_name: Имя таблицы
        :param data: Поток бинарных данных для вставки
        :param siem_id: UUID конвейера

        :return: None
        """
        self.log.debug('status=prepare, action=get_groups, msg="Try to import data to {}", '
                       'hostname="{}"'.format(table_name, self.__core_hostname))

        api_url = self.__api_table_import.format(table_name, siem_id)

        url = f'https://{self.__core_hostname}{api_url}'
        if int(self.__core_version.split('.')[0]) < 25:
            headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        else:
            headers = {'Content-Type': 'text/csv; charset=utf-8'}

        rq = exec_request(self.__core_session,
                          url,
                          method='POST',
                          timeout=self.settings.connection_timeout,
                          data=data,
                          headers=headers)
        response = rq.json()

        total_records = response.get('recordsNum')
        imported_records = response.get('importedNum')
        bad_records = response.get('badRecordsNum')
        skipped_records = response.get('skippedRecordsNum')
        if (imported_records == 0 or imported_records <= bad_records + skipped_records) and total_records != 0:
            self.log.error('status=failed, action=set_table_data, msg="Importing data to table {} ends with error", '
                           'hostname="{}", conveyor_id="{}", total={}, imported={}, bad={}, skipped={}'
                           .format(table_name,
                                   self.__core_hostname,
                                   siem_id,
                                   total_records,
                                   imported_records,
                                   bad_records,
                                   skipped_records))
            raise Exception(f'Importing data to table {table_name} ends with error')

        if bad_records != 0 or skipped_records != 0:
            self.log.error('status=warning, action=set_table_data, msg="Some data not imported to table {}", '
                           'hostname="{}", conveyor_id="{}", total={}, imported={}, bad={}, skipped={}'
                           .format(table_name,
                                   self.__core_hostname,
                                   siem_id,
                                   total_records,
                                   imported_records,
                                   bad_records,
                                   skipped_records))
        self.log.info('status=success, action=set_table_data, msg="Data imported to table {}", '
                      'hostname="{}", conveyor_id="{}", lines={}'.format(table_name, self.__core_hostname,
                                                                         siem_id,
                                                                         imported_records))

    def get_table_info(self, table_name, siem_id=None) -> dict:
        """Получить метаданные по табличке.

        :param table_name: Имя таблицы
        :param siem_id: UUID конвейера
        :return: {'property': 'value'}
        """
        self.log.debug('status=prepare, action=get_table_info, msg="Try to get table info for {}", '
                       'hostname="{}", conveyor_id="{}"'.format(table_name, self.__core_hostname, siem_id))

        table_id = self.get_table_id_by_name(table_name, siem_id)
        api_url = self.__api_table_info.format(table_id, siem_id)
        url = f'https://{self.__core_hostname}{api_url}'
        rq = exec_request(self.__core_session, url, method='GET', timeout=self.settings.connection_timeout)
        response = dict(rq.json())

        table_info = self.__tables_cache.get(table_name)
        table_info['size_max'] = response.get('maxSize')
        table_info['size_typical'] = response.get('typicalSize')
        table_info['ttl'] = response.get('ttl')
        table_info['description'] = response.get('description')
        table_info['created'] = response.get('created')
        table_info['updated'] = response.get('lastUpdated')
        table_info['size_current'] = response.get('currentSize')
        table_info['fields'] = response.get('fields')

        self.log.info('status=success, action=get_table_info, msg="Get {} properties for table {}", '
                      'hostname="{}", conveyor_id="{}"'.format(len(table_info), table_name, self.__core_hostname,
                                                               siem_id))

        return table_info

    def truncate_table(self, table_name: str, siem_id=None) -> bool:
        """Очистить табличный список.

        :param table_name: Имя таблицы
        :param siem_id: UUID конвейера
        :return: None
        """
        self.log.debug('status=prepare, action=truncate_table, msg="Try to truncate table {}", '
                       'hostname="{}", conveyor_id="{}"'.format(table_name, self.__core_hostname, siem_id))

        api_url = self.__api_table_truncate.format(self.get_table_id_by_name(table_name, siem_id), siem_id)
        url = f'https://{self.__core_hostname}{api_url}'

        rq = exec_request(self.__core_session, url, method='DELETE', timeout=self.settings.connection_timeout)
        response = rq.json()
        with open('debug.log', 'w') as debug_log:
            debug_log.write(str(response))

        if 'result' not in response or response.get('result') != 'success':
            self.log.error('status=failed, action=table_truncate, msg="Table {} have not been truncated", '
                           'hostname="{}"'.format(table_name, self.__core_hostname))
            raise Exception(f'Table {table_name} have not been truncated')

        self.log.info('status=success, action=truncate_table, msg="Table {} have been truncated", '
                      'hostname="{}", conveyor_id="{}"'.format(table_name, self.__core_hostname, siem_id))
        return True

    def get_table_id_by_name(self, table_name: str, siem_id=None) -> str:
        """Получение ID таблицы по её имени.

        :param table_name: Имя таблицы
        :param siem_id: UUID конвейера
        :return: UUID
        """

        if len(self.__tables_cache) == 0:
            self.get_tables_list(siem_id)
        table_id = self.__tables_cache.get(table_name)
        if table_id is None:
            raise Exception(f'Table list {table_name} not found in cache')
        return table_id.get('id')

    def set_table_row(self, table_name: str, add_rows: Optional[List[dict]] = None,
                      remove_rows: Optional[List[dict]] = None, siem_id=None):
        """Опасная (без остановки правил) работа со строками, установленных в
        SIEM таблиц.

        :param table_name: Имя таблицы
        :param add_rows: [{"field1": "value"}]
        :param remove_rows: [{"field1": "value"}]
        :param siem_id: UUID конвейера
        :return:
        """
        self.log.debug('status=prepare, action=set_table_row, msg="Try to add|remove table row {}", '
                       'hostname="{}", conveyor_id="{}"'.format(table_name, self.__core_hostname, siem_id))

        if add_rows is None and remove_rows is None:
            self.log.info('status=prepare, action=set_table_row, msg="Nothing to add/remove for table {}", '
                          'hostname="{}", conveyor_id="{}"'.format(table_name, self.__core_hostname, siem_id))
            return

        # в API добавление/удаление строк идет без явного маппинга на название полей.
        # маппинг определяется позицией значения в массиве, это неприемлемо
        table_info = self.get_table_info(table_name, siem_id)
        if table_info.get('type') not in ['correlationrule', 'enrichmentrule']:
            raise Exception('Unsupported table type to add/remove row')

        row_matrix = []  # шаблон для вставки нужного размера, заполненный None
        fields_types = {}
        attrs_position = {}  # в какой позиции во вставляемом списке должен находится каждый атрибут
        key_fields = set()  # перед вставкой надо убедиться, что присутствуют ключевые поля
        not_nullable_fields = set()  # перед вставкой надо убедиться, что заданы все поля где запрещен null
        counter = 0
        for i in table_info.get('fields'):  # определяем в каких позициях должны быть атрибуты
            name = i.get('name')
            attrs_position[name] = counter
            fields_types[name] = i.get('type')

            row_matrix.append(None)
            if i.get('primaryKey'):
                key_fields.add(name)
            if not i.get('nullable'):
                not_nullable_fields.add(name)
            counter += 1

        params = {'add': None, 'remove': None}
        if add_rows is not None:
            params['add'] = self.__prepare_rows(add_rows,
                                                row_matrix,
                                                attrs_position,
                                                key_fields,
                                                not_nullable_fields,
                                                fields_types)
        if remove_rows is not None:
            params['remove'] = self.__prepare_rows(remove_rows,
                                                   row_matrix,
                                                   attrs_position,
                                                   key_fields,
                                                   not_nullable_fields,
                                                   fields_types)

        table_id = table_info.get('id')
        api_url = self.__api_table_add_row.format(table_id, siem_id)
        url = f'https://{self.__core_hostname}{api_url}'
        rq = exec_request(self.__core_session,
                          url, method='PUT',
                          timeout=self.settings.connection_timeout,
                          json=params)
        response = rq.json()
        if response.get('result') is None or response.get('result') != 'success':
            self.log.error('status=failed, action=set_table_row, '
                           'msg="Got error while manipulate with table {} rows", '
                           'hostname="{}", conveyor_id="{}", error="{}"'.format(table_name,
                                                                                self.__core_hostname,
                                                                                siem_id,
                                                                                response.get('results')))
            raise Exception('Got error while manipulate with table rows')

        self.log.info('status=success, action=set_table_row, msg="Added {} rows Removed {} rows in table {}", '
                      'hostname="{}", conveyor_id="{}"'.format(len(add_rows) if add_rows is not None else 0,
                                                               len(remove_rows) if remove_rows is not None else 0,
                                                               table_name,
                                                               self.__core_hostname,
                                                               siem_id))

    def __prepare_rows(self, rows: List[dict], matrix: list, positions: dict, keys: set, not_nulls: set,
                       fields_types: dict):
        ret = list()
        for r in rows:
            tpl = matrix.copy()

            if len(keys.intersection(set(r.keys()))) != len(keys):
                raise Exception(f'Key fields {keys} not found in {r}')
            if len(not_nulls.intersection(set(r.keys()))) != len(not_nulls):
                raise Exception(f'Not nullable fields {not_nulls} not found in {r}')

            for k, v in r.items():
                pos = positions.get(k)
                if pos is None:
                    raise Exception(f'Key {k} not found in schema {positions.keys()}')

                # Конвертируем типы данных т.к. при построчной вставке есть особенности
                field_type = fields_types.get(k)
                # converted = None
                if field_type == 'number' and type(v) is not int:
                    converted = int(v)
                elif field_type == 'datetime' and type(v) is not int:
                    converted = round(datetime.strptime(v, self.__table_add_time_format).timestamp())
                else:
                    converted = v

                tpl[pos] = converted
            ret.append(tpl)
        return ret

    def close(self):
        if self.__core_session is not None:
            self.__core_session.close()
