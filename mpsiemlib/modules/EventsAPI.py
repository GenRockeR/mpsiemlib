from mpsiemlib.common import ModuleInterface, MPSIEMAuth, MPComponents, LoggingHandler, Settings
from mpsiemlib.common import exec_request, get_metrics_start_time, get_metrics_took_time

class EventsAPI(ModuleInterface, LoggingHandler):
    """
    Модуль получения информации о событиях через API UI
    """
    __api_events_metadata = "/api/events/v2/events_metadata"
    __api_event_details = "/api/events/v2/events/{}/normalized?time={}"
    __api_events_group = "/api/events/v2/events/aggregation?offset=0"
    

    def __init__(self, auth: MPSIEMAuth, settings: Settings):
        ModuleInterface.__init__(self, auth, settings)
        LoggingHandler.__init__(self)
        self.__core_session = auth.connect(MPComponents.CORE)
        self.__core_hostname = auth.creds.core_hostname
        self.log.debug('status=succes, action=prepare, msg="EventsUI Module init"')

    def ger_default_groupby_api_params(self) -> dict:
        params = {"filter": {"select": ["time", "event_src.host", "text"],
                              "where": "correlation_name=\"Exchange_Spam_attack_from_one_mail_address\"",
                              "orderBy": [{"field": "time", "sortOrder": "descending"}],
                              "groupBy": ["subject.account.name"],
                              "aggregateBy": [{"function": "COUNT", "field": "*","unique": "false"}],
                              "distributeBy": [],
                              "top": 10000,
                              "aliases": {"groupBy": {}, "aggregateBy": {"COUNT": "Cnt"}},
                              "searchType": "local",
                              "searchSources": [],
                              "localSources": [],
                              "groupByOrder": [{"field": "count", "sortOrder": "Descending"}],
                              "showNullGroups": "true"},
                   "timeFrom": 1686690000}
        return params

    def get_events_groupby_api_json(self, params: dict) -> dict:
        """
        Получить группировки формате в JSON по фильту запроса

        :param token: Токен запроса
        :return: Словарь с атрибутами активов
        """
        self.log.debug('status=prepare, action=get_events_groupby_api_json, msg="Try to agregate events by query {}", '
                       'hostname="{}"'.format(params['filter']['groupBy'], self.__core_hostname))

        url = "https://{}{}".format(self.__core_hostname, self.__api_events_group)

        start_time = get_metrics_start_time()
        line_counter = 0
        rq = exec_request(self.__core_session,
                  url,
                  method="POST",
                  timeout=self.settings.connection_timeout,
                  json=params)
        response = rq.json()
        
        took_time = get_metrics_took_time(start_time)

        self.log.info('status=success, action=get_events_groupby_api_json, msg="Query executed, response have been read", '
                      'hostname="{}", lines={}'.format(self.__core_hostname, line_counter))
        self.log.info('hostname="{}", metric=get_events_groupby_api_json, took={}ms, objects={}'.format(self.__core_hostname,
                                                                                                 took_time,
                                                                                                 line_counter))
        return response.get("rows")

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
        url = "https://{}{}".format(self.__core_hostname, api_url)
        rq = exec_request(self.__core_session, url)
        response = rq.json()

        if response is None or "event" not in response:
            self.log.error('status=failed, action=get_event_details, msg="Core data request return None or '
                           'has wrong response structure", '
                           'hostname="{}"'.format(self.__core_hostname))
            raise Exception("Core data request return None or has wrong response structure")
        return response.get("event")
    
    def get_events_metadata(self):
        """
        Получить список поддерживаемых полей таксономии событий
        """
        url = "https://{}{}".format(self.__core_hostname, self.__api_events_metadata)
        rq = exec_request(self.__core_session, url)
        response = rq.json()

        if response is None or "fields" not in response:
            self.log.error('status=failed, action=get_events_metadata, msg="Core data request return None or '
                           'has wrong response structure", '
                           'hostname="{}"'.format(self.__core_hostname))
            raise Exception("Core data request return None or has wrong response structure")
        return response.get("fields")    
        