"""Модуль работы с хранилищем событий (Elasticsearch).

Модуль - уровень SDK: ходит в Elasticsearch напрямую через `elasticsearch`
-клиент, минуя REST API ядра. Это extra-зависимость `mpsiemlib[events]`:
производитель переводит storage на Logspace, поэтому Elasticsearch-специфика
(включая определение версии хранилища) живёт только здесь, а не в ядре
библиотеки.

Поддерживаемое хранилище - Elasticsearch 7.17.x (MP SIEM 26.0 - 7.17, далее
7.17.14.1): события лежат в data stream `siem_events`, поэтому доступны point
in time и `search_after`. На ядрах с другим хранилищем (Logspace) модуль
бросает `StorageNotSupportedError` при создании.
"""

import json
from collections.abc import Iterator
from datetime import datetime, timedelta
from typing import Any

import pytz
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ElasticsearchException, NotFoundError

from mpsiemlib.common import (
    LoggingHandler,
    ModuleInterface,
    MPSIEMAuth,
    Settings,
    StorageNotSupportedError,
    StorageVersion,
    get_metrics_start_time,
    get_metrics_took_time,
)

JSONDict = dict[str, Any]

_TIME_FIELD = "time"
_ES_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
_BACKING_INDEX_DATE_FORMAT = "%Y%m%d"
_DATA_STREAM = "siem_events"
_DATA_STREAM_INDEX_PREFIX = ".ds-siem_events-"
_BACKING_INDEX_DATE_LENGTH = 10
# Поддерживаемая линейка версий ES: 7.17.x
_SUPPORTED_STORAGE_SERIES = "7.17"


class Events(ModuleInterface, LoggingHandler):
    """Выборка событий из Elasticsearch (агрегация и raw hits)."""

    _storage_port = 9200
    # TTL point in time: переоткрывается на каждой странице выборки
    _PIT_KEEP_ALIVE = "1m"
    # Внутренний сортировочный field ES (>=7.12): стабильный порядок без
    # сортировки по значению, tie-breaker для search_after
    _SHARD_DOC_FIELD = "_shard_doc"

    def __init__(self, auth: MPSIEMAuth, settings: Settings) -> None:
        ModuleInterface.__init__(self, auth, settings)
        LoggingHandler.__init__(self)

        storage_hostname = auth.get_creds().storage_hostname
        if not storage_hostname:
            raise RuntimeError("Storage hostname is not set in creds")
        self._storage_hostname = storage_hostname

        auth.disconnect()  # не будем пользоваться стандартной сессией, у нас есть ElasticSearch-py

        self._storage_session = Elasticsearch(
            hosts=self._storage_hostname,
            port=self._storage_port,
            timeout=settings.connection_timeout,
        )
        self._request_timeout = (
            settings.connection_timeout * settings.connection_timeout_x
        )

        self._storage_version = self.__request_storage_version()
        self.QueryBuilder = ElasticQueryBuilder(
            self._storage_version,
            settings.storage_events_timezone,
            settings.storage_bucket_size,
        )

        self.log.debug('status=success, action=prepare, msg="Events Module init"')

    def __request_storage_version(self) -> str:
        """Определить версию хранилища (ES) и проверить её поддержку.

        Ранее версия определялась ядром библиотеки (`MPSIEMAuth`) через
        `GET /_nodes`; после отказа производителя от Elasticsearch этот зонд
        перенесён сюда, чтобы ядро не знало про ES.

        :raise StorageNotSupportedError: хранилище недоступно либо это не
            Elasticsearch 7.17.x (например, Logspace)
        """
        try:
            info: JSONDict = self._storage_session.info()
            number = str(info.get("version", {}).get("number", ""))
        except ElasticsearchException as err:
            self.log.error(
                f"hostname={self._storage_hostname!r}, status=failed, "
                f"action=check_storage_version, msg={err!r}"
            )
            raise StorageNotSupportedError(
                f"hostname={self._storage_hostname!r}, storage is not reachable as "
                f"Elasticsearch: {err}"
            ) from err

        if not number.startswith(_SUPPORTED_STORAGE_SERIES):
            self.log.error(
                f"hostname={self._storage_hostname!r}, status=failed, "
                f"action=check_storage_version, version={number!r}, "
                f'msg="Storage is not Elasticsearch {_SUPPORTED_STORAGE_SERIES}x"'
            )
            raise StorageNotSupportedError(
                f"hostname={self._storage_hostname!r}: storage version {number!r} is "
                f"not Elasticsearch {_SUPPORTED_STORAGE_SERIES}.x "
                f"(supported since MP SIEM 26.0)"
            )

        self.log.info(
            f"hostname={self._storage_hostname!r}, status=success, "
            f"action=check_storage_version, version={number!r}"
        )
        return StorageVersion.ES7_17

    def get_events_group_by(
        self, filters: JSONDict, begin: int, end: int
    ) -> Iterator[JSONDict]:
        """Отфильтровать события и сгруппировать за выбранный интервал по
        указанным полям.

        Пустой результат (нет документов либо хранилище вернуло пустой ответ)
        отдаётся одной строкой-схемой с пустыми полями и `count=0`, чтобы
        вызывающий код не отличал «нет данных» от «ошибка запроса».

        :param filters: filter dict смотри в ElasticQueryBuilder; ключ `fields`
            обязателен
        :param begin: begin timestamp
        :param end: end timestamp
        :return: Iterator dicts {"field1_alias": "field1",
            "field2_alias": "field2", "count": 42}
        :raise RuntimeError: при отсутствии `fields` в filters
        """
        self.log.debug(
            f"status=prepare, action=get_groups, "
            f'msg="Try to exec query with filter", hostname={self._storage_hostname!r}, '
            f"filter={filters!r}, begin={begin!r}, end={end!r}"
        )
        fields = (filters or {}).get("fields")
        if not isinstance(fields, str) or not fields:
            raise RuntimeError(
                f"Unsupported filters format {filters!r}: 'fields' is required"
            )

        es_query = self.QueryBuilder.build_agg_query(filters, fields, begin, end)
        self.log.debug(
            f'status=prepare, action=build_query, msg="Generate ES query", '
            f"hostname={self._storage_hostname!r} query={es_query!r}"
        )

        start_time = get_metrics_start_time()
        es_response = self._search(
            begin,
            end,
            query=es_query.get("query"),
            aggs=es_query.get("aggs"),
            size=0,
            ignore_unavailable=True,
        )
        took_time = get_metrics_took_time(start_time)

        # проверяем ответ на наличие ошибок исполнения запроса или его частичного исполнения
        self._check_storage_response(es_response)

        base_schema = self._make_return_schema(filters)
        if self._is_empty_response(es_response):
            yield {**base_schema, "count": 0}
            return

        converted_response = self._convert_aggregation_response(
            es_response.get("aggregations", {})
        )
        for row in converted_response:
            yield {**base_schema, **row}

        line_counter = len(converted_response)
        self.log.info(
            f'status=success, action=get_groups, msg="Query executed, response have been read", '
            f"hostname={self._storage_hostname!r}, lines={line_counter!r}"
        )
        self.log.info(
            f"hostname={self._storage_hostname!r}, metric=get_groups, took={took_time:.4f} ms,"
            f"objects={line_counter!r}"
        )

    def get_events(self, filters: JSONDict, begin: int, end: int) -> Iterator[JSONDict]:
        """Получить все события по фильтру за указанный временной интервал.

        Выдача полная: выборка листается пачками по `storage_batch_size` через
        `search_after` + point in time (PIT). PIT фиксирует снапшот шардов на
        время чтения, tie-breaker - `_shard_doc` (стабильный порядок без
        сортировки по значению). Требует ES >= 7.12, поддерживаемые хранилища -
        7.17.x.

        :param filters: filter dict смотри в ElasticQueryBuilder
        :param begin: begin timestamp
        :param end: end timestamp
        :return: Iterator dicts - hits Elasticsearch
        """
        self.log.debug(
            'status=prepare, action=get_events, msg="Try to exec query with filter", '
            f"hostname={self._storage_hostname!r}, filter={filters!r} "
            f"begin={begin!r}, end={end!r}"
        )
        es_query = self.QueryBuilder.build_filter_query(filters, begin, end)
        self.log.debug(
            'status=prepare, action=build_query, msg="Generate ES query", '
            f"hostname={self._storage_hostname!r} query={es_query!r}"
        )

        indexes = ",".join(self._get_indexes_list(begin, end))
        line_counter = 0
        start_time = get_metrics_start_time()
        if not indexes:
            self.log.warning(
                f"hostname={self._storage_hostname!r}, status=failed, action=get_events, "
                'msg="No storage indexes for requested interval"'
            )
            return

        pit_id: str | None = None
        try:
            try:
                pit_id = self._storage_session.open_point_in_time(
                    index=indexes,
                    keep_alive=self._PIT_KEEP_ALIVE,
                    ignore_unavailable=True,
                    request_timeout=self._request_timeout,
                ).get("id")
            except NotFoundError as err:
                if err.error != "index_not_found_exception":
                    raise
                self.log.error(
                    f"status=failed, action=get_events, msg={err.error!r}, "
                    f"hostname={self._storage_hostname!r}"
                )
                yield {}
                return

            if not pit_id:
                raise RuntimeError("Storage return empty point in time id")

            search_after: list[Any] | None = None
            while True:
                body: JSONDict = {
                    "query": es_query.get("query"),
                    "size": self.settings.storage_batch_size,
                    "sort": [{self._SHARD_DOC_FIELD: "asc"}],
                    "pit": {"id": pit_id, "keep_alive": self._PIT_KEEP_ALIVE},
                }
                if search_after is not None:
                    body["search_after"] = search_after

                resp = self._storage_session.search(
                    body=body, request_timeout=self._request_timeout
                )
                self._check_storage_response(resp)
                # ES отдаёт обновлённый id PIT на каждой странице - держимся за
                # последний, чтобы в finally закрыть актуальный
                pit_id = resp.get("pit_id", pit_id)
                hits = resp.get("hits", {}).get("hits", [])
                for hit in hits:
                    line_counter += 1
                    yield hit

                if len(hits) < self.settings.storage_batch_size:
                    break
                search_after = hits[-1].get("sort")
                if not search_after:
                    self.log.warning(
                        f"hostname={self._storage_hostname!r}, status=failed, "
                        'action=get_events, msg="Storage return hit without sort values, '
                        'search_after stopped. Some data have been lost"'
                    )
                    break
        finally:
            if pit_id is not None:
                self._close_point_in_time(pit_id)

        took_time = get_metrics_took_time(start_time)
        self.log.info(
            f'status=success, action=get_events, msg="Query executed, response have been read", '
            f"hostname={self._storage_hostname!r}, lines={line_counter!r}"
        )
        self.log.info(
            f"hostname={self._storage_hostname!r}, metric=get_events, took={took_time:.4f} ms, "
            f"objects={line_counter!r}"
        )

    def _close_point_in_time(self, pit_id: str) -> None:
        """Освободить point in time.

        PIT держит ресурсы шардов, поэтому закрывается и при успехе, и при
        досрочном выходе вызывающего кода (generator.close()). Ошибки закрытия
        только логируются: они не должны перекрывать результат или исключение
        основной выборки.
        """
        try:
            self._storage_session.close_point_in_time(body={"pit_id": pit_id})
        except ElasticsearchException as err:
            self.log.warning(
                f"hostname={self._storage_hostname!r}, status=failed, "
                f"action=close_point_in_time, msg={err!r}"
            )

    def _search(self, begin: int, end: int, **kwargs: Any) -> JSONDict:
        """Выполнить search по backing-индексам data stream за интервал.

        При пустом списке индексов (data stream ещё не создан на этих датах)
        отдаётся пустой ответ в форме ES 7.x, не идя в ES: search по
        несуществующим индексам был бы отвергнут (`index_not_found_exception`).
        """
        indexes = ",".join(self._get_indexes_list(begin, end))
        if not indexes:
            return {
                "took": 0,
                "timed_out": False,
                "_shards": {"total": 0, "failed": 0},
                "hits": {"total": {"value": 0, "relation": "eq"}, "hits": []},
            }
        return self._storage_session.search(
            index=indexes, request_timeout=self._request_timeout, **kwargs
        )

    @staticmethod
    def _make_return_schema(filters: JSONDict) -> JSONDict:
        """При группировке ES не возвращает поля если они null, надо их явно
        восстановить в респонсе.

        :param filters: filters dict с ключом `fields`
        :return: Dict
        """
        schema: JSONDict = {}
        for field in (filters.get("fields") or "").split(","):
            parts = field.strip().split(" as ")
            alias = field.strip() if len(parts) == 1 else parts[1].strip()
            schema[alias] = ""
        return schema

    def _get_indexes_list(self, begin: int, end: int) -> list[str]:
        """Имена backing-индексов data stream `siem_events` за интервал.

        Storage 7.17 держит события в data stream, поэтому запрос идёт по
        backing-индексам, накрывающим [begin, end].

        :param begin: Query start time
        :param end: Query end time
        :return: список затрагиваемых индексов
        """
        begin_date = self._date(begin)
        end_date = self._date(end)
        interval = set(
            self._dates_range(begin_date, end_date, _BACKING_INDEX_DATE_FORMAT)
        )
        backing: list[str] = []

        for ds in self._storage_session.indices.get_data_stream(name="*").get(
            "data_streams", []
        ):
            if ds.get("name") != _DATA_STREAM:
                continue
            for ds_index in ds.get("indices", []):
                index_name = ds_index.get("index_name")
                if not index_name:
                    continue
                # backing индекс: .ds-siem_events-YYYY.MM.DD-<suffix>
                date_part = index_name.removeprefix(_DATA_STREAM_INDEX_PREFIX)
                date_part = date_part[:_BACKING_INDEX_DATE_LENGTH].replace(".", "")
                if date_part in interval and index_name not in backing:
                    backing.append(index_name)

        return backing

    def _date(self, timestamp: int) -> datetime:
        return datetime.fromtimestamp(
            timestamp, tz=pytz.timezone(self.settings.storage_events_timezone)
        )

    @staticmethod
    def _dates_range(
        begin_date: datetime, end_date: datetime, date_format: str
    ) -> list[str]:
        """Даты интервала [begin, end] включительно, в указанном формате."""
        days = int((end_date - begin_date).days) + 1
        return [
            (begin_date + timedelta(days=day)).strftime(date_format)
            for day in range(days)
        ]

    def _convert_aggregation_response(self, aggs: JSONDict) -> list[JSONDict]:
        """Convert ES response to dict.

        :param aggs: ES Aggregation response
        :return: [{'field1':'a', 'field2':'b', 'count':2},
            {'field1':'c', 'field2':'d', 'count':1}]
        """
        ret: list[JSONDict] = []
        for name, aggregation in aggs.items():
            for bucket in aggregation.get("buckets", []):
                sub = self._convert_aggregation_response(
                    {
                        key: value
                        for key, value in bucket.items()
                        if isinstance(value, dict)
                    }
                )
                key = bucket.get("key")
                doc_count = bucket.get("doc_count")
                for row in sub:
                    row.setdefault(name, key)
                    row.setdefault("count", doc_count)
                ret += sub if sub else [{name: key, "count": doc_count}]
        return ret

    def _is_empty_response(self, storage_response: JSONDict | None) -> bool:
        """Проверка ответов от Storage на пустой результат.

        :param storage_response: JSON ответ от Storage
        :return: True - если результат пустой
        """
        if not storage_response:
            self.log.error(
                'status=failed, action=report_read, msg="Storage return empty response", '
                f"hostname={self._storage_hostname!r}"
            )
            return True

        if storage_response.get("aggregations") is not None:
            aggregations: JSONDict = storage_response["aggregations"]
            return not any(agg.get("buckets") for agg in aggregations.values())

        # ES 7.x: hits.total - объект {"value": N, "relation": "eq"}
        return bool(storage_response.get("hits", {}).get("total", {}).get("value") == 0)

    def _check_storage_response(self, storage_response: JSONDict) -> None:
        """Проверка ответа от Storage на наличие ошибок исполнения запроса.
        Если были ошибки на шардах, пишем в лог.

        :param storage_response: Ответ от Elastic
        """
        error = storage_response.get("error")
        root_cause = error.get("root_cause") if isinstance(error, dict) else None
        if isinstance(root_cause, list):
            error_types = [str(item.get("type")) for item in root_cause]
            self.log.error(
                f"hostname={self._storage_hostname!r}, status=failed, action=exec_query, "
                f'msg="Storage return errors: {",".join(error_types)!r}"'
            )
            # надо остановить дальнейшую обработку, но чекер умеет только отписать ошибку
            storage_response.clear()
            return

        if storage_response.get("timed_out"):
            self.log.warning(
                f"hostname={self._storage_hostname!r}, status=failed, action=exec_query, "
                'msg="Storage return timed out for some shards. Some data have been lost"'
            )
        elif storage_response.get("_shards", {}).get("failed") not in (0, None):
            self.log.warning(
                f"hostname={self._storage_hostname!r}, status=failed, action=exec_query, "
                'msg="Storage return failed shards. Some data have been lost"'
            )

        # При агрегации выводится сколько документов было пропущено в каждом шарде
        for aggregation in storage_response.get("aggregations", {}).values():
            if aggregation.get("doc_count_error_upper_bound") not in (
                0,
                None,
            ) or aggregation.get("sum_other_doc_count") not in (0, None):
                self.log.warning(
                    f"hostname={self._storage_hostname!r}, status=failed, action=exec_query, "
                    'msg="Elastic return doc count error. Some data have been lost"'
                )

    def close(self) -> None:
        self._storage_session.close()


class ElasticQueryBuilder(LoggingHandler):
    """Построение запроса к Elastic по описанию вида
    es_filter: [
        '{"term": {"event_src/category": "DNS server"}}'
    ]
    es_filter_not: [
        '{"terms": {"event_src/category": ["Proxy server","Network device","Firewall","Web security"]}}'
        '{"range": {"dst/ip": {"gte": "127.0.0.0","lte": "127.255.255.255"}}}': '7'
        '{"range": {"dst/ip": {"gte": "10.0.0.0","lte": "10.255.255.255"}}}': 'ALL'
    ]
    fields: 'dst/ip as object,src/ip as subject'

    Поддерживаемое хранилище - ES 7.17 (`StorageVersion.ES7_17`); ES 1.7 и
    7.x ниже 7.17 из поддержанных ядер удалены.
    """

    def __init__(self, current_version: str, timezone: str, bucket_size: int) -> None:
        LoggingHandler.__init__(self)
        if current_version != StorageVersion.ES7_17:
            raise RuntimeError(
                f"Storage version {current_version!r} is not supported "
                f"(supported: {StorageVersion.ES7_17!r})"
            )
        self._es_current_version = current_version
        self._timezone = timezone
        self._es_bucket_size = bucket_size

    def build_filter_query(self, es_filter: JSONDict, begin: int, end: int) -> JSONDict:
        """Построить query выборки событий.

        :param es_filter: Описание фильтра (секции es_filter/es_filter_not)
        :param begin: Begin timestamp
        :param end: End timestamp
        :return: Тело запроса `{"query": ...}`
        """
        filter_expression = self._build_es_filter_expression(es_filter, begin, end)
        return {"query": {"bool": filter_expression}}

    def build_agg_query(
        self, es_filter: JSONDict, agg_field: str, begin: int, end: int
    ) -> JSONDict:
        """Построить query агрегации событий.

        :param es_filter: Описание фильтра (секции es_filter/es_filter_not)
        :param agg_field: Поля группировки через запятую, возможно с алиасами
        :param begin: Begin timestamp
        :param end: End timestamp
        :return: Тело запроса `{"query": ..., "aggs": ..., "size": 0}`
        """
        filter_expression = self._build_es_filter_expression(es_filter, begin, end)
        agg_expression = self._build_es_agg_expression(agg_field)
        return {
            "query": {"bool": filter_expression},
            "aggs": agg_expression["aggs"],
            "size": 0,
        }

    def _build_es_filter_expression(
        self, filters: JSONDict, begin: int, end: int
    ) -> JSONDict:
        """Собрать filter-часть запроса (временной интервал + секции фильтров).

        :param filters: Описание фильтра (секции es_filter/es_filter_not)
        :param begin: Begin timestamp
        :param end: End timestamp
        :return: dict для секции `bool`
        """
        es_datetime_begin = self._es_datetime(begin)
        es_datetime_end = self._es_datetime(end)

        # ES 7.x ожидает filter-клаузы в bool.filter, а не bool.must
        filter_dict: JSONDict = {
            "filter": [
                {
                    "range": {
                        _TIME_FIELD: {"gte": es_datetime_begin, "lte": es_datetime_end}
                    }
                }
            ]
        }

        for flt in (filters or {}).get("es_filter") or []:
            self._make_atomic_filter(filter_dict, flt, "filter")

        es_filter_not = (filters or {}).get("es_filter_not") or []
        if es_filter_not:
            filter_dict["must_not"] = []
            for flt in es_filter_not:
                self._make_atomic_filter(filter_dict, flt, "must_not")

        return filter_dict

    def _es_datetime(self, timestamp: int) -> str:
        return datetime.fromtimestamp(
            timestamp, tz=pytz.timezone(self._timezone)
        ).strftime(_ES_TIMESTAMP_FORMAT)

    def _make_atomic_filter(
        self, filter_dict: JSONDict, flt: Any, filter_key: str
    ) -> bool:
        """Добавить атомарный фильтр в секцию. Маркер версии у dict-формы
        (`{"<expr>": "7"}` / `"ALL"`) роняет выражение, если оно не под текущую
        версию storage.
        """
        if isinstance(flt, dict):
            expression, marker = dict(flt).popitem()
            if marker not in (self._es_current_version, StorageVersion.ALL):
                self.log.debug(
                    f'status=failed, action=build_query, msg="Drop unsupported expression", '
                    f"expression={expression!r}"
                )
                return False
            flt = expression

        filter_dict[filter_key].append(json.loads(flt))
        return True

    def _build_es_agg_expression(self, field: str) -> JSONDict:
        """Собрать дерево terms-агрегаций по полям группировки."""
        agg_dict: JSONDict = {}
        current_node = agg_dict
        for fld in field.split(","):
            # могут быть поля A as ALIAS1, B as ALIAS2 или A as ALIAS1 или A, B или A
            parts = fld.strip().split(" as ")
            if len(parts) == 1:
                fld_name = fld_alias = fld.strip()
            else:
                fld_name = parts[0].strip()
                fld_alias = parts[1].strip()

            if current_node.get("aggs") is None:
                current_node["aggs"] = {}
            if fld_alias not in current_node["aggs"]:
                current_node["aggs"][fld_alias] = {
                    "terms": {"field": fld_name, "size": self._es_bucket_size}
                }
            current_node = current_node["aggs"][fld_alias]

        return agg_dict
