from typing import Iterator

from mpsiemlib.common import ModuleInterface, MPSIEMAuth, LoggingHandler, Settings

from mpsiemlib.common import exec_request, get_metrics_start_time, get_metrics_took_time


class EventsAPI(ModuleInterface, LoggingHandler):
    """Модуль получения информации о событиях через API UI."""
    __api_events_metadata = '/api/events/v2/events_metadata'
    __api_event_details = '/api/events/v2/events/{}/normalized?time={}'
    __api_events_aggregation = '/api/events/v2/events/aggregation?offset=0'
    __api_events_aggregation_by_asset_group = '/api/events/v2/events/aggregation?offset=0&groupIds={}'
    __api_events_for_incident = '/api/incidents/events'
    __api_events = '/api/events/v2/events'
    __api_events_v3 = "/api/events/v3/events?limit={}&offset={}"

    def __init__(self, auth: MPSIEMAuth, settings: Settings):
        ModuleInterface.__init__(self, auth, settings)
        LoggingHandler.__init__(self)
        self.__core_session = auth.sessions['core']
        self.__core_hostname = auth.creds.core_hostname
        self.log.debug('status=success, action=prepare, msg="EventsAPI Module init"')

    def get_events_group_by_api_json(self, params: dict, group_ids=None) -> dict:
        """Получить группировки в формате JSON по фильтру запроса.

        :param: params: Параметры запроса
        :param: group_ids: Группа
        :return: Словарь с атрибутами активов
        """
        self.log.debug(
            'status=prepare, action=get_events_group_by_api_json, msg="Try to aggregate events by query {}", '
            'hostname="{}"'.format(params['filter']['groupBy'], self.__core_hostname))

        if group_ids:
            api_url = self.__api_events_aggregation_by_asset_group.format(group_ids)
            url = 'https://{}{}'.format(self.__core_hostname, api_url)
        else:
            url = 'https://{}{}'.format(self.__core_hostname, self.__api_events_aggregation)

        start_time = get_metrics_start_time()
        line_counter = 0
        rq = exec_request(self.__core_session,
                          url,
                          method='POST',
                          timeout=self.settings.connection_timeout,
                          json=params)
        response = rq.json()

        took_time = get_metrics_took_time(start_time)

        self.log.info(
            'status=success, action=get_events_group_by_api_json, msg="Query executed, response have been read", '
            'hostname="{}", lines={}'.format(self.__core_hostname, line_counter))
        self.log.info(
            'hostname="{}", metric=get_events_group_by_api_json, took={}ms, objects={}'
            .format(self.__core_hostname,
                    took_time,
                    line_counter))
        return response.get('rows')

    def get_event_details(self, event_id, event_date) -> dict:
        """Получить событие (все заполненные поля) по его идентификатору и
        дате.

        Args:
            event_id : идентификатор события
            event_date : дата
        Returns:
            [type]: событие
        """
        api_url = self.__api_event_details.format(event_id, event_date)

        url = 'https://{}{}'.format(self.__core_hostname, api_url)
        rq = exec_request(self.__core_session, url)
        response = rq.json()

        if response is None or 'event' not in response:
            self.log.error('status=failed, action=get_event_details, msg="Core data request return None or '
                           'has wrong response structure", '
                           'hostname="{}"'.format(self.__core_hostname))
            raise Exception("Core data request return None or has wrong response structure")
        return response.get('event')

    def get_events_metadata(self):
        """Получить список поддерживаемых полей таксономии событий."""

        url = 'https://{}{}'.format(self.__core_hostname, self.__api_events_metadata)

        rq = exec_request(self.__core_session, url)
        response = rq.json()

        if response is None or 'fields' not in response:
            self.log.error('status=failed, action=get_events_metadata, msg="Core data request return None or '
                           'has wrong response structure", '
                           'hostname="{}"'.format(self.__core_hostname))

            raise Exception('Core data request return None or has wrong response structure')
        return response.get('fields')

    def get_events_grouped_by_fields(self, query_filter, group_by_fields, time_from, time_to) -> dict[str, int]:
        """Получить события по фильтру, сгруппированные по заданным полям.

        Args:
            query_filter : фильтр на языке PDQL
            group_by_fields: список полей для группировки
            time_from : начало диапазона поиска (Unix timestamp в секундах)
            time_to : конец диапазона поиска (Unix timestamp в секундах)
        Returns:
            [type]: массив событий
        """
        null = None
        false = False
        true = True
        params = {
            "filter": {
                "select": ["time", "event_src.host", "text"],
                "where": query_filter,
                "orderBy": [{
                    "field": "time",
                    "sortOrder": "descending"
                }
                ],
                "groupBy": group_by_fields,
                "aggregateBy": [{
                    "function": "COUNT",
                    "field": "*",
                    "unique": false
                }
                ],
                "distributeBy": [],
                "top": 10000,
                "aliases": {
                    "groupBy": {},
                    "aggregateBy": {
                        "COUNT": "Cnt"
                    }
                },
                "searchType": null,
                "searchSources": null,
                "localSources": null,
                "groupByOrder": [{
                    "field": "count",
                    "sortOrder": "Descending"
                }
                ],
                "showNullGroups": true
            },
            "timeFrom": time_from,
            "timeTo": time_to
        }
        api_url = self.__api_events_aggregation
        url = f'https://{self.__core_hostname}{api_url}'

        rq = exec_request(self.__core_session, url, method='POST', json=params)
        response = rq.json()

        if response is None or 'rows' not in response:
            self.log.error('status=failed, action=get_events_grouped_by_fields, msg="Core data request return None or '
                           'has wrong response structure", '
                           'hostname="{}"'.format(self.__core_hostname))
            raise Exception('Core data request return None or has wrong response structure')

        return {' | '.join(str(s) for s in e['groups']): int(e['values'][0]) for e in response['rows']}

    def get_events_by_filter(self, pdql_filter, fields, time_from, time_to, limit, offset) -> dict:
        """Получить события по фильтру.

        Args:
            pdql_filter : фильтр на языке PDQL
            fields : список запрашиваемых полей событий
            time_from : начало диапазона поиска (Unix timestamp в секундах)
            time_to : конец диапазона поиска (Unix timestamp в секундах)
            limit: число запрашиваемых событий, соответствующих фильтру
            offset: позиция, начиная с которой возвращать требуемое число событий, соответствующих фильтру
        Returns:
            [type]: массив событий
        """
        null = None
        params = {
            'filter': {
                'select': fields,
                'where': f'{pdql_filter}',
                'orderBy': [
                    {
                        'field': 'time',
                        'sortOrder': 'ascending'
                    }
                ],
                'groupBy': [],
                'aggregateBy': [],
                'distributeBy': [],
                'top': null,
                'aliases': {
                    'groupBy': {}
                }
            },
            'groupValues': [],
            'timeFrom': time_from,
            'timeTo': time_to
        }
        api_url = self.__api_events.format(limit, offset)
        url = f'https://{self.__core_hostname}{api_url}'

        rq = exec_request(self.__core_session, url, method='POST', json=params)
        response = rq.json()

        if response is None or 'events' not in response:
            self.log.error('status=failed, action=get_events_by_filter, msg="Core data request return None or '
                           'has wrong response structure", '
                           'hostname="{}"'.format(self.__core_hostname))
            raise Exception('Core data request return None or has wrong response structure')
        return response.get('events')

    def get_events_for_incident(self, fields, incident_id, time_from, time_to, limit, offset):
        """Получить события, связанные с инцидентом.

        Args:
            fields : список запрашиваемых полей событий
            incident_id: идентификатор инцидента
            time_from : начало диапазона поиска (Unix timestamp в секундах)
            time_to : конец диапазона поиска (Unix timestamp в секундах)
            limit: число запрашиваемых событий, связанных с инцидентом
            offset: позиция, начиная с которой возвращать требуемое число событий, связанны с инцидентом
        Returns:
            [type]: массив событий
        """
        null = None
        params = {
            'filter': {
                'select': fields,
                'where': '',
                'orderBy': [
                    {
                        'field': 'time',
                        'sortOrder': 'descending'
                    }
                ],
                'groupBy': [],
                'aggregateBy': [],
                'distributeBy': [],
                'top': null,
                'aliases': {},
                'searchType': null,
                'searchSources': null
            },
            'timeFrom': time_from,
            'timeTo': time_to
        }

        api_url = self.__api_events_for_incident.format(incident_id, limit, offset)
        url = f'https://{self.__core_hostname}{api_url}'

        rq = exec_request(self.__core_session, url, method="POST", json=params)
        response = rq.json()
        if response is None or 'events' not in response:
            self.log.error('status=failed, action=get_events_for_incident, msg="Core data request return None or '
                           'has wrong response structure", '
                           'hostname="{}"'.format(self.__core_hostname))
            raise Exception('Core data request return None or has wrong response structure')
        return response.get('events')

    def __get_events_by_filter_v3(self, query_filter: str, time_from: int, time_to: int, offset: int, limit: int,
                                  token: str = None) -> dict:
        """
        Получить события по фильтру
        Args:
            query_filter (str): фильтр на языке PDQL в виде одной строки ""
            time_from (int): начало диапазона поиска (Unix timestamp в секундах)
            time_to (int): конец диапазона поиска (Unix timestamp в секундах)
            offset: (int) позиция, начиная с которой возвращать требуемое число событий, соответсвующих фильтру
            limit: (int) число запрашиваемых событий, соответсвующих фильтру
            token: (str) токен запроса (ускоряет ответ при запросе новой пачки событий по предыдущему фильтру)
        Returns:
            [dict]: массив событий
        """
        null = None
        params = {
            'filter': query_filter,
            'groupValues': ['1'],
            'timeFrom': time_from,
            'timeTo': time_to
        }
        api_url = self.__api_events_v3.format(limit, offset)
        url = 'https://{}{}'.format(self.__core_hostname, api_url)
        if token is not None:
            url += '&token={}'.format(token)

        rq = exec_request(self.__core_session, url, method='POST', json=params)
        response = rq.json()

        if response is None or 'events' not in response:
            self.log.error('status=failed, action=get_events_by_filter, msg="Core data request return None or '
                           'has wrong response structure", '
                           'hostname="{}"'.format(self.__core_hostname))
            raise Exception("Core data request return None or has wrong response structure")
        return response  # .get("events")

    def get_events_by_filter_v3(self, query_filter: str, time_from: int, time_to: int) -> Iterator[dict]:
        """Получить события по фильтру
        Args:
            query_filter (str): фильтр на языке PDQL в виде одной строки
            time_from (int): начало диапазона поиска (Unix timestamp в секундах)
            time_to (int): конец диапазона поиска (Unix timestamp в секундах)
        Yields:
            Iterator[dict]: Итератор
        """
        self.log.debug('status=prepare, action=get_events_by_filter_v3, msg="Try to get events", '
                       'hostname="{}", filter="{}", begin={}, end={}'.format(self.__core_hostname,
                                                                             query_filter,
                                                                             time_from,
                                                                             time_to))
        # Пачками выгружаем содержимое
        is_end = False
        offset = 0
        limit = self.settings.events_batch_size
        token = None
        line_counter = 0
        start_time = get_metrics_start_time()
        while not is_end:
            ret = self.__get_events_by_filter_v3(query_filter, time_from, time_to, offset, limit, token)
            events = ret.get('events')
            token = ret.get('token')  # последующие запросы с токеном должны быстрее, чем без него
            if len(events) < limit:
                is_end = True
            offset += limit
            for i in events:
                line_counter += 1
                yield i
        took_time = get_metrics_took_time(start_time)
        self.log.info(
            'hostname="{}", metric=get_events_by_filter_v3, took={}ms, objects={}'.format(self.__core_hostname,
                                                                                          took_time,
                                                                                          line_counter))