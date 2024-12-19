from datetime import datetime
from typing import Optional, Iterator

import pytz

from mpsiemlib.common import ModuleInterface, MPSIEMAuth, LoggingHandler, Settings
from mpsiemlib.common import exec_request, get_metrics_start_time, get_metrics_took_time


class SourceMonitor(ModuleInterface, LoggingHandler):
    """Source monitor module."""

    __time_format = '%Y-%m-%dT%H:%M:%S.%fZ'
    __time_27_format = '%Y-%m-%dT%H:%M:%S.000Z'

    __api_sources_list = '/api/events_monitoring/v2/sources'
    __api_forwarders_list = '/api/events_monitoring/v2/forwarders'
    __api_sources_v3_list = '/api/events_monitoring/v3/assets'
    __api_forwarders_v3_list = '/api/events_monitoring/v3/forwarders'
    'https://mow03-mpsiem-dev.soc.bi.zone/api/events_monitoring/v3/assets?stateFilter=all&limit=50&offset=0'

    def __init__(self, auth: MPSIEMAuth, settings: Settings):
        ModuleInterface.__init__(self, auth, settings)
        LoggingHandler.__init__(self)
        self.__core_session = auth.sessions['core']
        self.__core_hostname = auth.creds.core_hostname
        self.__core_version = auth.get_core_version()
        self.log.debug('status=success, action=prepare, msg="SourceMonitor Module init"')

    def get_sources_list(self,
                         begin: int,
                         end: Optional[int] = None,
                         forwarder_id: Optional[str] = None) -> Iterator[dict]:
        """Получить список источников из мониторинга.

        :param begin: Timestamp начала диапазона
        :param end: timestamp. Если задан, то (end-begin)>=24h, иначе
            API вернет пусто результат
        :param forwarder_id: ID форвардера для которого надо вывести
            источники
        :return:
        """
        if int(self.__core_version.split('.')[0]) < 27:
            url = f'https://{self.__core_hostname}{self.__api_sources_list}'
        else:
            url = f'https://{self.__core_hostname}{self.__api_sources_v3_list}'
        params = self.__prepare_params(begin, end)

        if forwarder_id is not None:
            params['forwarderId'] = forwarder_id

        # Пачками выгружаем содержимое
        is_end = False
        offset = 0
        limit = self.settings.source_monitor_batch_size
        line_counter = 0
        start_time = get_metrics_start_time()
        while not is_end:
            if int(self.__core_version.split('.')[0]) < 27:
                payload = {}
                ret = self.__iterate_items(url, params, payload, offset, limit)
            else:
                payload = self.__prepare_payloads(begin, end)
                ret = self.__iterate_items(url, params, payload, offset, limit)
            if len(ret) < limit:
                is_end = True
            offset += limit
            if int(self.__core_version.split('.')[0]) < 27:
                for i in ret:
                    line_counter += 1
                    yield {'id': i.get('source').get('id'),
                           'control_status': i.get('source').get('controlStatus'),
                           'control_time_status': i.get('source').get('timeControlStatus'),
                           'control_delay_status': i.get('source').get('delayControlStatus'),
                           'control_eps_status': i.get('source').get('epsControlStatus'),
                           'asset_id': i.get('source').get('assetId'),
                           'name': i.get('source').get('name'),
                           'hostname': i.get('source').get('host'),
                           'ip': i.get('source').get('ip'),
                           'service_vendor': i.get('service').get('vendor'),
                           'service_title': i.get('service').get('title'),
                           'service_subsystem': i.get('service').get('subsystem'),
                           'discovered': i.get('discoveredTime'),
                           'seen': i.get('lastSeenTime'),
                           'eps': i.get('eps'),
                           'eps_diff': i.get('epsDiff'),
                           'time_shift': i.get('timeShift'),  # minutes (+/-)
                           'events_count': i.get('eventsCount')
                           }
            else:
                for i in ret.get('items'):
                    line_counter += 1
                    yield {'asset_id': i.get('asset').get('assetId'),
                           'asset_name': i.get('asset').get('name'),
                           'asset_type': i.get('asset').get('assetType'),
                           'asset_importance': i.get('asset').get('importance'),
                           'activity_control': i.get('activityControl'),
                           'delay_control': i.get('delayControl')}

        took_time = get_metrics_took_time(start_time)

        self.log.info('status=success, action=get_sources_list, msg="Query executed, response have been read", '
                      'hostname="{}", lines={}'.format(self.__core_hostname, line_counter))
        self.log.info('hostname="{}", metric=get_sources_list, took={}ms, objects={}'.format(self.__core_hostname,
                                                                                             took_time,
                                                                                             line_counter))

    def get_forwarders_list(self, begin: int, end: Optional[int] = None) -> Iterator[dict]:
        """Получить список форвардеров из мониторинга.

        :param begin: timestamp начала диапазона
        :param end: timestamp. если задан, то (end-begin)>=24h, иначе
            API вернет пусто результат
        :return:
        """

        if int(self.__core_version.split('.')[0]) < 27:
            url = f'https://{self.__core_hostname}{self.__api_forwarders_list}'
        else:
            url = f'https://{self.__core_hostname}{self.__api_forwarders_v3_list}'
        params = self.__prepare_params(begin, end)

        # Пачками выгружаем содержимое
        is_end = False
        offset = 0
        limit = self.settings.source_monitor_batch_size
        line_counter = 0
        start_time = get_metrics_start_time()
        while not is_end:
            if int(self.__core_version.split('.')[0]) < 27:
                payload = {}
                ret = self.__iterate_items(url, params, payload, offset, limit)
            else:
                payload = self.__prepare_payloads(begin, end)
                ret = self.__iterate_items(url, params, payload, offset, limit)
            if len(ret) < limit:
                is_end = True
            offset += limit
            if int(self.__core_version.split('.')[0]) < 27:
                for i in ret:
                    line_counter += 1
                    yield {'id': i.get('source').get('id'),
                           'control_status': i.get('source').get('controlStatus'),
                           'control_time_status': i.get('source').get('timeControlStatus'),
                           'control_delay_status': i.get('source').get('delayControlStatus'),
                           'control_eps_status': i.get('source').get('epsControlStatus'),
                           'asset_id': i.get('source').get('assetId'),
                           'name': i.get('source').get('name'),
                           'hostname': i.get('source').get('host'),
                           'ip': i.get('source').get('ip'),
                           'service_vendor': i.get('service').get('vendor'),
                           'service_title': i.get('service').get('title'),
                           'service_subsystem': i.get('service').get('subsystem'),
                           'discovered': i.get('discoveredTime'),
                           'seen': i.get('lastSeenTime'),
                           'eps': i.get('eps'),
                           'eps_diff': i.get('epsDiff'),
                           'time_shift': i.get('timeShift'),  # minutes (+/-)
                           'events_count': i.get('eventsCount')}
            else:
                for i in ret.get('items'):
                    yield {'asset_id': i.get('asset').get('assetId'),
                           'asset_type': i.get('asset').get('assetType'),
                           'asset_importance': i.get('asset').get('importance'),
                           'asset_name': i.get('asset').get('name'),
                           'activity_control': i.get('activityControl'),
                           'delay_control': i.get('delayControl'),
                           'eps': i.get('eps'),
                           'last_event_date_time': i.get('lastEventTime')}

        took_time = get_metrics_took_time(start_time)

        self.log.info('status=success, action=get_forwarders_list, msg="Query executed, response have been read", '
                      'hostname="{}", lines={}'.format(self.__core_hostname, line_counter))
        self.log.info('hostname="{}", metric=get_forwarders_list, took={}ms, objects={}'.
                      format(self.__core_hostname,
                             took_time,
                             line_counter))

    def get_sources_by_forwarder(self, forwarder_id: str, begin: int, end: Optional[int] = None) -> Iterator[dict]:
        """Получить все источники для форвардера Обертка над
        get_sources_list."""
        return self.get_sources_list(begin, end, forwarder_id)

    def __prepare_params(self, begin, end=None):

        if int(self.__core_version.split('.')[0]) < 27:
            start_time = datetime.fromtimestamp(begin, tz=pytz.timezone('UTC')).strftime(self.__time_format)
            params = {'timeFrom': start_time, 'controlState': 'all'}
        else:
            params = {'stateFilter': 'all'}

        if end is not None:
            if int(self.__core_version.split('.')[0]) < 27:
                end_time = datetime.fromtimestamp(end, tz=pytz.timezone('UTC')).strftime(self.__time_format)
                params['timeTo'] = end_time

        return params

    def __prepare_payloads(self, begin, end=None):
        if int(self.__core_version.split('.')[0]) >= 27:
            start_time = datetime.fromtimestamp(begin, tz=pytz.timezone('UTC')).strftime(self.__time_27_format)
            payload = {
                'fromDateTime': start_time,
                'groupIds': [
                    '00000000-0000-0000-0000-000000000002'
                ],
                'recursive': True
            }

            if end is not None:
                if int(self.__core_version.split('.')[0]) >= 27:
                    end_time = datetime.fromtimestamp(end, tz=pytz.timezone('UTC')).strftime(self.__time_27_format)
                    payload['toDateTime'] = end_time

            return payload

    def __iterate_items(self, url: str, params: dict, payload: dict, offset: int, limit: int):
        params['offset'] = offset
        params['limit'] = limit

        if int(self.__core_version.split('.')[0]) < 27:
            rq = exec_request(self.__core_session,
                              url,
                              method='GET',
                              timeout=self.settings.connection_timeout,
                              params=params)
        else:
            rq = exec_request(self.__core_session,
                              url,
                              method='POST',
                              timeout=self.settings.connection_timeout,
                              params=params, json=payload)
        response = rq.json()
        if response is None:
            self.log.error('status=failed, action=monitor_items_iterate, msg="Core data request return None or '
                           'has wrong response structure", '
                           'hostname="{}"'.format(self.__core_hostname))
            raise Exception('Core data request return None or has wrong response structure')

        return response

    def close(self):
        if self.__core_session is not None:
            self.__core_session.close()
