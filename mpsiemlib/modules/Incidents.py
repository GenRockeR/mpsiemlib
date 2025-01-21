from datetime import datetime
from typing import Optional, Iterator

import pytz

from mpsiemlib.common import ModuleInterface, MPSIEMAuth, LoggingHandler, Settings
from mpsiemlib.common import exec_request, get_metrics_start_time, get_metrics_took_time


class Incidents(ModuleInterface, LoggingHandler):
    """Incidents worker."""
    __time_format = "%Y-%m-%dT%H:%M:%S.%fZ"
    __time_format_r25 = "%Y-%m-%dT%H:%M:%S.000Z"

    __api_incidents_list = "/api/v2/incidents"
    __api_incidents_list_r25 = "/api/v2/incidents/"

    __api_incident_info = ''
    __api_incident_info_new = '/api/incidentsReadModel/incidents/{}'  # From R23
    __api_incident_info_old = '/api/incidents/{}'

    __api_incident_comments = ''
    __api_incident_comments_new = '/api/incidentsReadModel/incidents/{}/transitions'  # From R23
    __api_incident_comments_old = '/api/incidents/{}/transitions'

    __api_incident_events = "/api/incidents/{}/events?limit={}"
    __api_incident_events_count = "/api/incidents/{}/events/count"
    __api_incident_issue = "/api/incidents/{}/issues"
    __api_incident_netfor = "/api/incidents/{}/linkedObjects/netfor"
    __api_incident_netfor_sessions = "/api/incidents/{}/linkedObjects/netfor/sessions?offset=0&limit=2000"
    __api_incident_netfor_alerts = "/api/incidents/{}/linkedObjects/netfor/alerts?offset=0&limit=2000"

    class TimeFilterType:
        CREATED = 'creation'
        DETECTED = 'detection'
        MODIFIED = 'lastModification'
        APPROVED = 'approval'
        IN_PROGRESS = 'inProgress'
        RESOLVED = 'resolving'
        CLOSED = 'closing'

    def __init__(self, auth: MPSIEMAuth, settings: Settings):
        ModuleInterface.__init__(self, auth, settings)
        LoggingHandler.__init__(self)
        self.__core_session = auth.sessions['core']
        self.__core_hostname = auth.creds.core_hostname
        self.__core_version = auth.get_core_version()
        self.__incidents_mapping = {}

        if int(self.__core_version.split('.')[0]) > 23:
            self.__api_incident_comments = self.__api_incident_comments_new
            self.__api_incident_info = self.__api_incident_info_new
        else:
            self.__api_incident_comments = self.__api_incident_comments_old
            self.__api_incident_info = self.__api_incident_info_old

        self.log.debug('status=success, action=prepare, msg="Incidents Module init"')

    def get_incidents_list(self,
                           begin: int,
                           end: int,
                           time_type: str = TimeFilterType.CREATED,
                           filters: Optional[dict] = None) -> Iterator[dict]:
        """Получить список инцидентов за временной период.

        :param begin: Timestamp. Начало диапазона выгрузки
        :param end: Timestamp. окончание диапазона выгрузки
        :param time_type: Статус инцидента, к которому применяется
            временное окно. По умолчанию "созданные"
        :param filters: filters = { select": ["key", "name", "category",
            "type", "status", "created", "assigned"], "where": "",
            "orderby": [{"field": "created", "sortOrder": "descending"}
            ]},
        :return: Итератор
        """

        self.log.debug('status=prepare, action=get_incidents_list, msg="Try to get incidents list", '
                       'hostname="{}", filters="{}", begin={}, end={}'.format(self.__core_hostname,
                                                                              filters,
                                                                              begin,
                                                                              end))

        if int(self.__core_version.split('.')[0]) <= 24:
            url = "https://{}{}".format(self.__core_hostname, self.__api_incidents_list)
            time_from = datetime.fromtimestamp(begin, tz=pytz.timezone("UTC")).strftime(self.__time_format)
            time_to = datetime.fromtimestamp(end, tz=pytz.timezone("UTC")).strftime(self.__time_format)
        else:
            url = "https://{}{}".format(self.__core_hostname, self.__api_incidents_list_r25)
            time_from = datetime.fromtimestamp(begin, tz=pytz.timezone("UTC")).strftime(self.__time_format_r25)
            time_to = datetime.fromtimestamp(end, tz=pytz.timezone("UTC")).strftime(self.__time_format_r25)

        params = {
            'timeFrom': time_from,
            'timeTo': time_to,
            'groups': {
                'filterType': 'no_filter'
            },
            'filter': {
                'select': ['key', 'name', 'category', 'type', 'status', 'created', 'assigned'],
                'where': '',
                'orderby': [{'field': 'created',
                             'sortOrder': 'descending'}
                            ]},
            'filterTimeType': time_type,
            'queryIds': [
                'all_incidents'
            ]
        }
        if filters is not None:
            params['filter'].update(filters)

        # Пачками выгружаем содержимое
        is_end = False
        offset = 0
        limit = self.settings.incidents_batch_size
        line_counter = 0
        start_time = get_metrics_start_time()
        while not is_end:
            ret = self.__iterate_incidents(url, params, offset, limit)
            if len(ret) < limit:
                is_end = True
            offset += limit
            for i in ret:
                line_counter += 1
                inc_id = i.get('id')
                inc_key = i.get('key')
                self.__incidents_mapping[inc_key] = inc_key  # прогреваем кэш
                yield {'id': inc_id,
                       'key': inc_key,
                       'created': i.get('created'),
                       'name': i.get('name'),
                       'confirmed': i.get('isConfirmed'),
                       'status': i.get('status').lower(),
                       'category': i.get('category').lower(),
                       'type': i.get('type').lower(),
                       'assigned': i.get('assigned'),
                       'severity': i.get('severity', "").lower()}
        took_time = get_metrics_took_time(start_time)

        self.log.info('status=success, action=get_incidents_list, msg="Query executed, response have been read", '
                      'hostname="{}", filter="{}", lines={}'.format(self.__core_hostname,
                                                                    filters,
                                                                    line_counter))
        self.log.info('hostname="{}", metric=get_incidents_list, took={}ms, objects={}'.format(self.__core_hostname,
                                                                                               took_time,
                                                                                               line_counter))

    def __iterate_incidents(self, url: str, params: dict, offset: int, limit: int):
        params['offset'] = offset
        params['limit'] = limit
        rq = exec_request(self.__core_session,
                          url,
                          method='POST',
                          timeout=self.settings.connection_timeout,
                          json=params)
        response = rq.json()
        if response is None or 'incidents' not in response:
            self.log.error('status=failed, action=kb_objects_iterate, msg="Core data request return None or '
                           'has wrong response structure", '
                           'hostname="{}"'.format(self.__core_hostname))
            raise Exception('Core data request return None or has wrong response structure')

        return response.get('incidents')

    def get_incident_id_by_key(self, incident_key):
        """Получить ID инцидента по его Key (INC-<Number>). Поиск возможен
        только на глубину в 365 дней.

        :param incident_key: Key инцидента (INC-<Number>)
        :return: ID инцидента
        """

        incident_id = self.__incidents_mapping.get(incident_key)
        if incident_id is not None:
            return incident_id

        # Ищем по инцидентам на глубину в 365 дней
        end = round(datetime.now(tz=pytz.timezone(self.settings.local_timezone)).timestamp())
        begin = end - (365 * 86400)
        filters = {'where': f'key="{incident_key}"'}
        incident = next(self.get_incidents_list(begin, end, self.TimeFilterType.CREATED, filters))

        return incident.get('id') if incident is not None else None

    def get_incident_info(self, incident_id: str) -> dict:
        """Получить информацию по инциденту.

        :param incident_id: ID инцидента. Key (INC-<Number>) != ID
        :return: Перечень свойств инцидента
        """
        self.log.debug('status=prepare, action=get_groups, msg="Try to get incident info for {}", '
                       'hostname="{}"'.format(incident_id, self.__core_hostname))

        # получаем сам инцидент
        api_url = self.__api_incident_info.format(incident_id)
        url = f'https://{self.__core_hostname}{api_url}'
        rq = exec_request(self.__core_session, url, method='GET', timeout=self.settings.connection_timeout)
        inc = rq.json()

        comments = self.__load_comments(incident_id)
        events = self.__load_events(incident_id)
        issues = self.__load_issues(incident_id)

        ret = {'id': inc.get('id'),
               'key': inc.get('key'),
               'correlation_rule': inc.get('correlationRuleNames'),
               'created': inc.get('created'),
               'detected': inc.get('detected'),
               'modification_history': inc.get('modified'),
               'name': inc.get('name'),
               'description': inc.get('description'),
               'confirmed': inc.get('isConfirmed'),
               'status': inc.get('status').lower(),
               'category': inc.get('category').lower(),
               'type': inc.get('type').lower(),
               'assigned': inc.get('assigned'),
               'reporter': inc.get('reporter'),
               'severity': inc.get('severity', '').lower(),
               'targets': inc.get('targets'),
               'attackers': inc.get('attackers'),
               'comments': comments,
               'events': events,
               'issues': issues
               }

        if inc.get("source") == "netFor":
            netfor = self.__load_netfor(incident_id)
            ret["netfor"] = netfor

        self.log.info('status=success, action=get_table_info, msg="Get {} properties for incident {}", '
                      'hostname="{}"'.format(len(ret), incident_id, self.__core_hostname))

        return ret

    def __load_comments(self, incident_id):
        """Загрузка комментариев.

        :param incident_id:
        :return:
        """
        api_url = self.__api_incident_comments.format(incident_id)
        url = f'https://{self.__core_hostname}{api_url}'
        rq = exec_request(self.__core_session, url, method='GET', timeout=self.settings.connection_timeout)
        com = rq.json()

        comments = []
        for i in com:
            comments.append({
                'status_old': i.get('prevStatus').lower(),
                'status_new': i.get('nextStatus').lower(),
                'comment': i.get('comment'),
                'user_id': i.get('changedBy', {}).get('id')
            })

        return comments

    def __load_events(self, incident_id):
        """Загрузка событий.

        :param incident_id:
        :return:
        """
        api_url = self.__api_incident_events_count.format(incident_id)
        url = f'https://{self.__core_hostname}{api_url}'
        rq = exec_request(self.__core_session, url, method='GET', timeout=self.settings.connection_timeout)
        cnt = rq.json()

        events_count = cnt.get('count') if cnt.get('count') is not None else 0

        events = []
        if events_count != 0:
            api_url = self.__api_incident_events.format(incident_id, events_count)
            url = f'https://{self.__core_hostname}{api_url}'
            rq = exec_request(self.__core_session, url, method='GET', timeout=self.settings.connection_timeout)
            evts = rq.json()

            for i in evts:
                events.append({"id": i.get("id"), "description": i.get("description"), "date": i.get("date")})

        return events

    def __load_issues(self, incident_id):
        """Загрузить задачи.

        :param incident_id:
        :return:
        """
        api_url = self.__api_incident_issue.format(incident_id)
        url = f'https://{self.__core_hostname}{api_url}'
        rq = exec_request(self.__core_session, url, method='GET', timeout=self.settings.connection_timeout)
        response = rq.json()

        issues = []
        for i in response:
            issues.append({'type': i.get('type', '').lower(),
                           'description': i.get('description'),
                           'id': i.get('id'),
                           'status': i.get('status').lower(),
                           'name': i.get('name', ''),
                           'assigned': i.get('assigned', {}).get('id'),
                           'estimated': i.get('estimatedTime')
                           })

        return issues

    def __load_netfor(self, incident_id):
        """Загрузить информацию по сетевому трафику (для инцидентов, созданных
        нажатием кнопки в PT NAD)

        :param incident_id:
        :return:
        """
        api_url = self.__api_incident_netfor.format(incident_id)
        url = "https://{}{}".format(self.__core_hostname, api_url)
        rq = exec_request(self.__core_session, url, method="GET", timeout=self.settings.connection_timeout)

        response = rq.json()

        netfor = {}
        netfor = response
        netfor['sessions'] = self.__load_netfor_sessions(incident_id)
        netfor['alerts'] = self.__load_netfor_alerts(incident_id)

        return netfor

    def __load_netfor_sessions(self, incident_id):
        """Загрузить информацию по сессиям в сетевом трафике (для инцидентов,
        созданных нажатием кнопки в PT NAD)

        :param incident_id:
        :return:
        """
        api_url = self.__api_incident_netfor_sessions.format(incident_id)

        url = "https://{}{}".format(self.__core_hostname, api_url)
        rq = exec_request(self.__core_session, url, method="GET", timeout=self.settings.connection_timeout)

        response = rq.json()

        return response

    def __load_netfor_alerts(self, incident_id):
        """Загрузить информацию по алертам в сетевом трафике (для инцидентов,
        созданных нажатием кнопки в PT NAD)

        :param incident_id:
        :return:
        """
        api_url = self.__api_incident_netfor_alerts.format(incident_id)
        url = "https://{}{}".format(self.__core_hostname, api_url)

        rq = exec_request(self.__core_session, url, method="GET", timeout=self.settings.connection_timeout)
        response = rq.json()

        return response

    def close(self):
        if self.__core_session is not None:
            self.__core_session.close()
