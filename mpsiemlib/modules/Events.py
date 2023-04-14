import json
import pytz
from typing import Iterator
from datetime import datetime, timedelta

from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import NotFoundError

from mpsiemlib.common import ModuleInterface, MPSIEMAuth, LoggingHandler, Settings, StorageVersion
from mpsiemlib.common import get_metrics_start_time, get_metrics_took_time


class Events(ModuleInterface, LoggingHandler):
    """
    Elasticsearch module
    """

    __storage_port = 9200
    
    def __init__(self, auth: MPSIEMAuth, settings: Settings):
        ModuleInterface.__init__(self, auth, settings)
        LoggingHandler.__init__(self)

        self.__storage_version = auth.get_storage_version()
        self.__storage_hostname = auth.get_creds().storage_hostname
        auth.disconnect()  # не будем пользоваться стандартной сессией, у нас есть модуль ElasticSearch-py
        self.__storage_session = Elasticsearch(hosts=self.__storage_hostname,
                                               port=self.__storage_port,
                                               timeout=self.settings.connection_timeout)

        self.QueryBuilder = ElasticQueryBuilder(self.__storage_version,
                                                self.settings.storage_events_timezone,
                                                self.settings.storage_bucket_size)

        self.log.debug('status=success, action=prepare, msg="Events Module init"')

    def get_events_groupby(self, filters: dict, begin: int, end: int) -> Iterator[dict]:
        """
        Отфильтровать события и сгруппировать за выбранный интервал по указанным полям

        :param filters: filter dict смотри в ElasticQueryBuilder
        :param begin: begin timestamp
        :param end: end timestamp
        :return: Iterator dicts {"field1_alias": "field1", "field2_alias": "field2", "count": 42}
        """
        self.log.debug('status=prepare, action=get_groups, msg="Try to exec query with filter", '
                       'hostname="{}", filter="{}" begin="{}", end="{}"'.format(self.__storage_hostname,
                                                                                filters,
                                                                                begin,
                                                                                end))
        fields = filters.get('fields')
        if filters is None or fields is None:
            raise Exception('Unsupported filters format "{}"'.format(filters))

        es_query = self.QueryBuilder.build_agg_query(filters, fields, begin, end)
        self.log.debug('status=prepare, action=build_query, msg="Generate ES query", '
                       'hostname="{}" query="{}"'.format(self.__storage_hostname, es_query))

        indexes = ','.join(self.__get_indexes_list(begin, end))
        timeout_report_gen = self.settings.connection_timeout * self.settings.connection_timeout_x

        start_time = get_metrics_start_time()
        es_response = self.__storage_session.search(index=indexes,
                                                    body=es_query,
                                                    size=0,
                                                    request_timeout=timeout_report_gen,
                                                    ignore_unavailable=True)
        took_time = get_metrics_took_time(start_time)

        # проверяем ответ на наличие ошибок исполнения запроса или его частичного исполнения
        self.__check_storage_response(es_response)

        if self.__is_empty_response(es_response):
            self.log.debug('status=success, action=get_groups, msg="Empty report", '
                           'hostname="{}", lines={}'.format(self.__storage_hostname, 0))
            yield {}

        # конвертируем ответ от ES
        converted_response = self.__convert_aggregation_response(es_response.get('aggregations', {}))

        base_schema = self.__make_return_schema(filters)
        for row in converted_response:
            schema = base_schema.copy()
            schema.update(row)
            yield schema

        line_counter = len(converted_response)
        self.log.info('status=success, action=get_groups, msg="Query executed, response have been read", '
                      'hostname="{}", lines={}'.format(self.__storage_hostname, line_counter))
        self.log.info('hostname="{}", metric=get_groups, took={}ms, objects={}'.format(self.__storage_hostname,
                                                                                       took_time,
                                                                                       line_counter))

    def get_events(self, filters: dict, begin: int, end: int) -> Iterator[dict]:
        """
        Итеративно получить все события по фильтру за указанный временной интервал

        :param filters: filter dict смотри в ElasticQueryBuilder
        :param begin: begin timestamp
        :param end: end timestamp
        :return: Iterator dicts {"field1_alias": "field1", "field2_alias": "field2", "count": 42}
        """
        self.log.debug('status=prepare, action=get_events, msg="Try to exec query with filter", '
                       'hostname="{}", filter="{}" begin="{}", end="{}"'.format(self.__storage_hostname,
                                                                                filters,
                                                                                begin,
                                                                                end))
        line_counter = 0
        es_query = self.QueryBuilder.build_filter_query(filters, begin, end)
        self.log.debug('status=prepare, action=build_query, msg="Generate ES query", '
                       'hostname="{}" query="{}"'.format(self.__storage_hostname, es_query))

        indexes = ','.join(self.__get_indexes_list(begin, end))
        timeout_report_gen = self.settings.connection_timeout * self.settings.connection_timeout_x

        start_time = get_metrics_start_time()
        try:
            for hit in helpers.scan(self.__storage_session,
                                    es_query,
                                    scroll='{}s'.format(round(self.settings.connection_timeout/2)),
                                    index=indexes,
                                    size=self.settings.storage_batch_size,
                                    raise_on_error=False,
                                    request_timeout=timeout_report_gen,
                                    preserve_order=True):
                if (line_counter % self.settings.storage_batch_size) == 0:
                    self.log.debug('status=failed, action=get_events, msg="Get rows from storage {}", '
                                   'hostname="{}",'.format(line_counter, self.__storage_hostname))
                line_counter += 1
                yield hit
        except NotFoundError as nf_ex:
            if nf_ex.error == 'index_not_found_exception':
                self.log.error('status=failed, action=get_events, msg="{}", '
                               'hostname="{}",'.format(nf_ex.error, self.__storage_hostname))
                yield {}
            else:
                raise Exception(nf_ex.error)

        took_time = get_metrics_took_time(start_time)
        self.log.info('status=success, action=get_events, msg="Query executed, response have been read", '
                      'hostname="{}", lines={}'.format(self.__storage_hostname, line_counter))
        self.log.info('hostname="{}", metric=get_events, took={}ms, objects={}'.format(self.__storage_hostname,
                                                                                       took_time,
                                                                                       line_counter))

    def __make_return_schema(self, filters):
        """
        При группировке ES не возвращает поля если они null, надо их явно восстановить в респонсе

        :param filters:
        :return: {'field1': '', 'field2': ''}
        """
        ret = {}
        field = filters.get('fields', '')
        field_list = field.split(',')
        for fld in field_list:
            fld_list = fld.strip().split(' as ')
            fld_name = fld.strip() if len(fld_list) == 1 else fld_list[1].strip()
            ret[fld_name] = ''
        return ret

    def __get_indexes_list(self, begin: int, end: int) -> list:
        """
        Расчет кол-ва дней между двумя timestamp и генерация имен индексов в Storage

        :param begin: Query start time
        :param end: Query end time
        :return: список затрагиваемых индексов
        """
        index_prefix = 'ptsiem_events_' if self.__storage_version == StorageVersion.ES17 else 'siem_events_'
        begin_date = datetime.fromtimestamp(begin, tz=pytz.timezone(self.settings.storage_events_timezone))
        end_date = datetime.fromtimestamp(end, tz=pytz.timezone(self.settings.storage_events_timezone))
        ret = []
        for n in range(int((end_date - begin_date).days) + 1):
            ret.append(index_prefix + (begin_date + timedelta(n)).strftime('%Y-%m-%d'))
        return ret

    def __convert_aggregation_response(self, aggs: dict) -> list:
        """
        Convert ES response to dict

        :param aggs: ES Aggregation response
        :return: [{'field1':'a', 'field2':'b', 'count':2}, {'field1':'c', 'field2':'d', 'count':1}]
        """
        ret = []
        for k, v in aggs.items():
            if v.get('buckets') is not None:
                for b in v.get('buckets'):
                    key = None
                    cnt = None
                    sub = []
                    for i, j in b.items():
                        if i == 'key':
                            key = j
                        if i == 'doc_count':
                            cnt = j
                        # рекурсивный обход, т.к. может быть группировка по нескольким полям,
                        # а это вложенная агрегация в ES
                        if isinstance(j, dict):
                            sub += self.__convert_aggregation_response({i: j})
                    if len(sub) == 0:
                        ret.append({k: key, "count": cnt})
                    for h in sub:
                        if h.get('count') is not None:
                            h.update({k: key})
                        else:
                            h.update({k: key, "count": cnt})
                    ret += sub
        return ret

    def __is_empty_response(self, storage_response: dict) -> bool:
        """
        Проверка ответов от Storage на пустой результат

        :param storage_response: JSON ответ от Storage
        :return: True - если результат не пустой
        """
        if storage_response is None or len(storage_response) == 0:
            self.log.error('status=failed, action=report_read, msg="Storage return empty response", '
                           'hostname="{}"'.format(self.__storage_hostname))
            return True

        if self.__storage_version == StorageVersion.ES7:
            if storage_response.get('hits').get('total').get('value') == 0:
                return True

        if self.__storage_version == StorageVersion.ES17:
            if storage_response.get('hits').get('total') == 0:
                return True

        return False

    def __check_storage_response(self, storage_response: dict) -> None:
        """
        Проверка ответа от Storage на наличие ошибок исполнения запроса.
        Если были ошибки на шардах, пишем в лог

        :param storage_response: Ответ от Elastic
        :return: None
        """
        if storage_response.get('error') is not None and storage_response.get('error').get('root_cause') is not None:
            error_msg = []
            for i in storage_response.get('error').get('root_cause'):
                error_msg.append(i.get('type'))
            self.log.error('hostname="{}", status=failed, action=exec_query, '
                           'msg="Storage return errors: {}"'.format(self.__storage_hostname, ','.join(error_msg)))
            storage_response.clear()  # надо остановить дальнейшую обработку, но чекер умеет только отписать ошибку
            return

        if storage_response.get('timed_out'):
            self.log.warning('hostname="{}", status=failed, action=exec_query, '
                             'msg="Storage return timed out for some shards. '
                             'Some data have been lost"'.format(self.__storage_hostname))
        elif storage_response.get('_shards').get('failed') != 0:
            self.log.warning('hostname="{}", status=failed, action=exec_query, msg="Storage return failed shards. '
                             'Some data have been lost"'.format(self.__storage_hostname))

        # При агрегации выводится сколько документов было пропущено в каждом шарде
        for k, v in storage_response.get('aggregations', {}).items():
            if v.get('doc_count_error_upper_bound') != 0 or v.get('sum_other_doc_count') != 0:
                self.log.warning('hostname="{}", status=failed, action=exec_query, '
                                 'msg="Elastic return doc count error. '
                                 'Some data have been lost"'.format(self.__storage_hostname))

    def close(self):
        if self.__storage_session is not None:
            self.__storage_session.close()


class ElasticQueryBuilder(LoggingHandler):
    """
    Построение запроса к Elastic по описанию вида
        es_filter: [
            '{"term": {"event_src/category": "DNS server"}}'
        ]
        es_filter_not: [
            '{"terms": {"event_src/category": ["Proxy server","Network device","Firewall","Web security"]}}'
            '{"range": {"dst/ip": {"gte": "127.0.0.0","lte": "127.255.255.255"}}}': '7'
            '{"range": {"dst/ip": {"gte": "169.254.0.0","lte": "169.254.255.255"}}}': '1.7'
            '{"range": {"dst/ip": {"gte": "10.0.0.0","lte": "10.255.255.255"}}}': 'ALL'
            '{"range": {"dst/ip": {"gte": "172.16.0.0","lte": "172.31.255.255"}}}
        ]
        fields: 'dst/ip as object,src/ip as subject'
    """

    def __init__(self, current_version: str, timezone: str, bucket_size: int):
        LoggingHandler.__init__(self)
        self.__es_current_version = current_version
        self.__timezone = timezone
        self.__es_bucket_size = bucket_size

    def build_filter_query(self, es_filter, begin, end):
        filter_expression = self.__build_es_filter_expression(es_filter, begin, end)
        query = {}
        if self.__es_current_version == StorageVersion.ES17:
            raise NotImplementedError()
        if self.__es_current_version == StorageVersion.ES7:
            query = {
                "query": {
                    "bool": filter_expression
                }
            }

        return query

    def build_agg_query(self, es_filter, agg_field, begin, end):
        """
        Build query for Elastic
        :param es_filter:
        :param agg_field:
        :param begin:
        :param end:
        :return:
        """
        filter_expression = self.__build_es_filter_expression(es_filter, begin, end)
        agg_expression = self.__build_es_agg_expression(agg_field)

        query = {}
        if self.__es_current_version == StorageVersion.ES17:
            raise NotImplementedError()

        if self.__es_current_version == StorageVersion.ES7:
            query = {
                "query": {
                    "bool": filter_expression
                },
                "aggs": agg_expression['aggs'],
                "size": 0
            }

        return query

    def __build_es_filter_expression(self, filters, begin, end):
        """
        Make filter part of query
        :param filters:
        :param begin:
        :param end:
        :return: dict
        """

        es_time_format = '%Y-%m-%dT%H:%M:%SZ'
        es_datetime_begin = datetime.fromtimestamp(begin, tz=pytz.timezone(self.__timezone)).strftime(es_time_format)
        es_datetime_end = datetime.fromtimestamp(end, tz=pytz.timezone(self.__timezone)).strftime(es_time_format)

        must_alias = 'filter' if self.__es_current_version == StorageVersion.ES7 else 'must'
        filter_dict = {must_alias: [{'range': {'time': {'gte': es_datetime_begin, "lte": es_datetime_end}}}]}

        # разбираем секцию es_filter
        es_filter = filters.get('es_filter')
        if es_filter is not None and len(es_filter) != 0:
            for flt in es_filter:
                if not self.__make_atomic_filter(filter_dict, flt, must_alias):
                    continue

        # разбираем секцию es_filter_not
        es_filter_not = filters.get('es_filter_not')
        if es_filter_not is not None and len(es_filter_not) != 0:
            filter_dict['must_not'] = []
            for flt in es_filter_not:
                if not self.__make_atomic_filter(filter_dict, flt, "must_not"):
                    continue

        return filter_dict

    def __make_atomic_filter(self, filter_dict, flt, filter_key):
        has_marker = type(flt) is dict  # Check if we have 'term': 'ALL|1.7|7' in filter
        if has_marker:
            k, v = dict(flt).popitem()
            if v not in [self.__es_current_version, StorageVersion.ALL]:
                self.log.debug('status=failed, action=build_query, msg="Drop unsupported expression", '
                               'expression="{}"'.format(k))
                return False
            flt = k
        if self.__es_current_version == StorageVersion.ES17:
            flt = flt.replace('/', '.')
        filter_dict[filter_key].append(json.loads(flt))

        return True

    def __build_es_agg_expression(self, field):
        agg_dict = {}
        field_list = field.split(',')
        current_node = agg_dict
        for fld in field_list:
            # могут быть поля A as ALIAS1, B as ALIAS2 или A as ALIAS1 или A,B или A
            fld_list = fld.strip().split(' as ')

            fld_name = None
            fld_alias = None
            if len(fld_list) == 1:
                fld_name = fld_alias = fld.strip()
            else:
                fld_name = fld_list[0].strip()
                fld_alias = fld_list[1].strip()

            if self.__es_current_version == StorageVersion.ES17:
                fld_name = fld_name.replace('/', '.')

            # собираем дерево группировки
            if current_node.get('aggs') is None:
                current_node['aggs'] = {}
            if fld_alias not in current_node['aggs']:
                current_node['aggs'][fld_alias] = {"terms": {"field": fld_name, "size": self.__es_bucket_size}}
            current_node = current_node['aggs'][fld_alias]

        return agg_dict
