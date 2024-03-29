from mpsiemlib.common import ModuleInterface, MPSIEMAuth, MPComponents, LoggingHandler, Settings
from mpsiemlib.common import exec_request


class EventsAPI(ModuleInterface, LoggingHandler):
    """
    Модуль получения информации о событиях через API UI
    """
    __api_events_metadata = '/api/events/v2/events_metadata'
    __api_event_details = '/api/events/v2/events/{}/normalized?time={}'
    __api_events = '/api/events/v2/events?limit={}&offset={}'
    __api_events_for_incident = '/api/events/v2/events/?incidentId={}&limit={}&offset={}'
    __api_events_aggregation = '/api/events/v2/events/aggregation'

    def __init__(self, auth: MPSIEMAuth, settings: Settings):
        ModuleInterface.__init__(self, auth, settings)
        LoggingHandler.__init__(self)
        self.__core_session = auth.connect(MPComponents.CORE)
        self.__core_hostname = auth.creds.core_hostname
        self.log.debug('status=success, action=prepare, msg="EventsUI Module init"')

    def get_event_details(self, event_id, event_date) -> dict:
        """
        Получить событие (все заполненные поля) по его идентификатору и дате

        Args:
            event_id : идентификатор события
            event_date : дата
        Returns:
            [type]: событие
        """
        api_url = self.__api_event_details.format(event_id, event_date)
        url = f'https://{self.__core_hostname}{api_url}'
        rq = exec_request(self.__core_session, url)
        response = rq.json()

        if response is None or 'event' not in response:
            self.log.error('status=failed, action=get_event_details, msg="Core data request return None or '
                           'has wrong response structure", '
                           'hostname="{}"'.format(self.__core_hostname))
            raise Exception('Core data request return None or has wrong response structure')
        return response.get('event')

    def get_events_metadata(self):
        """
        Получить список поддерживаемых полей таксономии событий
        """
        url = f'https://{self.__core_hostname}{self.__api_events_metadata}'
        rq = exec_request(self.__core_session, url)
        response = rq.json()

        if response is None or "fields" not in response:
            self.log.error('status=failed, action=get_events_metadata, msg="Core data request return None or '
                           'has wrong response structure", '
                           'hostname="{}"'.format(self.__core_hostname))
            raise Exception('Core data request return None or has wrong response structure')
        return response.get('fields')

    def get_events_groupped_by_fields(self, filter, group_by_fields, time_from, time_to) -> list:
        """
        Получить события по фильтру, сгруппированные по заданым полям

        Args:
            filter : фильтр на языке PDQL
            fields : список запрашиваемых полей событий
            group_by_fields: список полей для группировки
            time_from : начало диапазона поиска (Unix timestamp в секундах)
            time_to : конец диапазона поиска (Unix timestamp в секундах)
            limit: число запрашиваемых событий, соответсвующих фильтру
            offset: позиция, начиная с которой возвращать требуемое число событий, соответсвующих фильтру 
        Returns:
            [type]: массив событий 
        """
        null = None
        false = False
        true = True
        params = {
            "filter": {
                "select": ["time", "event_src.host", "text"],
                "where": filter,
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
            self.log.error('status=failed, action=get_events_groupped_by_fields, msg="Core data request return None or '
                           'has wrong response structure", '
                           'hostname="{}"'.format(self.__core_hostname))
            raise Exception('Core data request return None or has wrong response structure')
        
        return {' | '.join(str(s) for s in e['groups']):int(e['values'][0]) for e in response['rows']}

    def get_events_by_filter(self, filter, fields, time_from, time_to, limit, offset) -> dict:
        """
        Получить события по фильру 

        Args:
            filter : фильтр на языке PDQL
            fields : список запрашиваемых полей событий
            time_from : начало диапазона поиска (Unix timestamp в секундах)
            time_to : конец диапазона поиска (Unix timestamp в секундах)
            limit: число запрашиваемых событий, соответсвующих фильтру
            offset: позиция, начиная с которой возвращать требуемое число событий, соответсвующих фильтру 
        Returns:
            [type]: массив событий 
        """
        null = None
        params = {
            'filter': {
                'select': fields,
                'where': f'{filter}',
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
        """
        Получить события, связанные с инцидентом 

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
