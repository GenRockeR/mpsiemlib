"""Тесты модуля Events (Elasticsearch).

Storage на стендах - Logspace, порт ES :9200 закрыт, поэтому тесты построены
на моках `elasticsearch.Elasticsearch`: проверяется автодетект версии ES
(`info()`), тело запроса, расчёт backing-индексов data stream, обход выборки
через point in time + `search_after`, разбор ответов ES и поведение на
пустых/ошибочных ответах. Сетевых запросов тесты не делают и могут гоняться
без стенда.
"""

import unittest
from unittest import mock

from elasticsearch.exceptions import NotFoundError, TransportError

from mpsiemlib.common import (
    AuthType,
    Creds,
    Settings,
    StorageNotSupportedError,
    StorageVersion,
)
from mpsiemlib.modules.Events import ElasticQueryBuilder, Events

# 2025-01-01T00:00:00Z / 2025-01-02T00:00:00Z
BEGIN = 1735689600
END = 1735776000

# Backing-индексы data stream siem_events на даты тестового интервала
DATA_STREAMS = {
    "data_streams": [
        {
            "name": "siem_events",
            "indices": [
                {"index_name": ".ds-siem_events-2025.01.01-1"},
                {"index_name": ".ds-siem_events-2025.01.02-1"},
            ],
        }
    ]
}
INTERVAL_INDEXES = ".ds-siem_events-2025.01.01-1,.ds-siem_events-2025.01.02-1"
ES_INFO_7_17 = {"version": {"number": "7.17.14"}}


def fake_auth(storage_hostname: str | None = "es.invalid") -> mock.MagicMock:
    """MPSIEMAuth-заглушка: модулю нужны только креды (плюс disconnect())."""
    creds = Creds()
    creds.core_hostname = "core.invalid"
    creds.storage_hostname = storage_hostname
    creds.core_auth_type = AuthType.PAT_TOKEN
    creds.pat_token = "pat-unittest-fake"

    auth = mock.MagicMock()
    auth.get_creds.return_value = creds
    return auth


def batch_response(hits, pit_id=None, total=None):
    """Одна страница ответа search (PIT-форма: id в pit_id)."""
    resp = {
        "took": 1,
        "timed_out": False,
        "_shards": {"total": 1, "successful": 1, "failed": 0},
        "hits": {
            "total": {"value": total if total is not None else len(hits)},
            "hits": hits,
        },
    }
    if pit_id is not None:
        resp["pit_id"] = pit_id
    return resp


def make_events(
    settings: Settings | None = None,
    storage_hostname: str | None = "es.invalid",
    es_info=None,
) -> tuple[Events, mock.MagicMock]:
    """Собрать Events с мокнутым Elasticsearch-клиентом.

    :param settings: настройки модуля (по умолчанию - `Settings()`)
    :param storage_hostname: hostname хранилища в кредах (`None` - кейс
        незаполненных кредов)
    :param es_info: ответ `info()` (версия ES); по умолчанию - 7.17.14. Если
        передан Exception, `info()` его бросает (кейсы неподдерживаемого
        хранилища).
    :return: (модуль, мок сессии Elasticsearch)
    """
    auth = fake_auth(storage_hostname)
    with mock.patch("mpsiemlib.modules.Events.Elasticsearch") as es_cls:
        session = mock.MagicMock()
        session.indices.get_data_stream.return_value = DATA_STREAMS
        session.open_point_in_time.return_value = {"id": "pit-1"}
        if isinstance(es_info, Exception):
            session.info.side_effect = es_info
        else:
            session.info.return_value = ES_INFO_7_17 if es_info is None else es_info
        es_cls.return_value = session
        module = Events(auth, settings or Settings())
    module.auth.disconnect.assert_called_once()
    return module, session


class QueryBuilderTestCase(unittest.TestCase):
    """ElasticQueryBuilder строит тело запроса (ES 7.17)."""

    @staticmethod
    def builder(
        version: str = StorageVersion.ES7_17,
        bucket_size: int = 33000,
        timezone: str = "UTC",
    ) -> ElasticQueryBuilder:
        return ElasticQueryBuilder(version, timezone, bucket_size)

    def test_unsupported_storage_version(self):
        # StorageVersion хранит только поддерживаемую линейку ES7_17; сюда
        # подаём прежние значения (ES17/ES7/LS) литералами - они больше не
        # допускаются.
        for version in ("1.7", "7", "1", "8.11", ""):
            with self.subTest(version=version), self.assertRaises(RuntimeError):
                ElasticQueryBuilder(version, "UTC", 100)

    def test_time_range_in_filter(self):
        query = self.builder().build_filter_query({}, BEGIN, END)
        self.assertEqual(
            query["query"]["bool"]["filter"],
            [
                {
                    "range": {
                        "time": {
                            "gte": "2025-01-01T00:00:00Z",
                            "lte": "2025-01-02T00:00:00Z",
                        }
                    }
                }
            ],
        )

    def test_timezone_is_applied_to_timestamps(self):
        """Границы считаются в storage_events_timezone (UTC+3 -> сдвиг на час)."""
        query = self.builder(timezone="Europe/Moscow").build_filter_query(
            {}, BEGIN, END
        )
        time_range = query["query"]["bool"]["filter"][0]["range"]["time"]
        self.assertEqual(time_range["gte"], "2025-01-01T03:00:00Z")
        self.assertEqual(time_range["lte"], "2025-01-02T03:00:00Z")

    def test_es_filter_and_es_filter_not(self):
        filters = {
            "es_filter": ['{"term": {"normalized": "true"}}'],
            "es_filter_not": ['{"range": {"dst/ip": {"gte": "127.0.0.0"}}}'],
        }
        bool_query = self.builder().build_filter_query(filters, BEGIN, END)["query"][
            "bool"
        ]
        self.assertIn({"term": {"normalized": "true"}}, bool_query["filter"])
        self.assertEqual(
            bool_query["must_not"], [{"range": {"dst/ip": {"gte": "127.0.0.0"}}}]
        )

    def test_empty_sections_are_omitted(self):
        """Пустые/отсутствующие секции не добавляют must_not, `None` допустимы."""
        for filters in (
            {},
            {"es_filter": None, "es_filter_not": None},
            {"es_filter_not": []},
        ):
            with self.subTest(filters=filters):
                bool_query = self.builder().build_filter_query(filters, BEGIN, END)
                self.assertNotIn("must_not", bool_query["query"]["bool"])
                self.assertEqual(len(bool_query["query"]["bool"]["filter"]), 1)

    def test_version_marker_keeps_current_version(self):
        filters = {"es_filter": [{'{"term": {"a": "1"}}': StorageVersion.ES7_17}]}
        bool_query = self.builder().build_filter_query(filters, BEGIN, END)
        self.assertIn({"term": {"a": "1"}}, bool_query["query"]["bool"]["filter"])

    def test_version_marker_drops_other_version(self):
        """Маркер старой линейки ('7') не подходит для 7.17 - выражение отбрасывается."""
        filters = {"es_filter_not": [{'{"term": {"a": "1"}}': "7"}]}
        with self.assertLogs("ElasticQueryBuilder", level="DEBUG"):
            bool_query = self.builder().build_filter_query(filters, BEGIN, END)
        self.assertEqual(bool_query["query"]["bool"]["must_not"], [])

    def test_version_marker_all_kept(self):
        filters = {"es_filter": [{'{"term": {"a": "1"}}': StorageVersion.ALL}]}
        bool_query = self.builder().build_filter_query(filters, BEGIN, END)
        self.assertIn({"term": {"a": "1"}}, bool_query["query"]["bool"]["filter"])

    def test_invalid_json_filter_raises(self):
        filters = {"es_filter": ["{'term': {'a': 1}}"]}
        with self.assertRaises(ValueError):
            self.builder().build_filter_query(filters, BEGIN, END)

    def test_agg_query_nested_tree(self):
        query = self.builder(bucket_size=500).build_agg_query(
            {}, "src/ip as subject, event_src/host", BEGIN, END
        )
        self.assertEqual(query["size"], 0)
        self.assertIn("query", query)
        subject = query["aggs"]["subject"]
        self.assertEqual(subject["terms"], {"field": "src/ip", "size": 500})
        self.assertEqual(
            subject["aggs"]["event_src/host"]["terms"],
            {"field": "event_src/host", "size": 500},
        )

    def test_agg_query_single_field_without_alias(self):
        query = self.builder().build_agg_query({}, "normalized", BEGIN, END)
        self.assertEqual(list(query["aggs"]), ["normalized"])
        self.assertNotIn("aggs", query["aggs"]["normalized"])

    def test_agg_query_does_not_escape_slashes(self):
        """ES 1.7 (dot-номенклатура) из поддержки снят - `/` сохраняется как есть."""
        query = self.builder().build_agg_query({}, "dst/ip", BEGIN, END)
        self.assertEqual(query["aggs"]["dst/ip"]["terms"]["field"], "dst/ip")


class EventsInitTestCase(unittest.TestCase):
    """Инициализация модуля: автодетект версии ES, креды, отказ от REST-сессии."""

    def test_supported_storage_version(self):
        module, session = make_events()
        self.assertIsInstance(module.QueryBuilder, ElasticQueryBuilder)
        self.assertEqual(module._storage_version, StorageVersion.ES7_17)
        session.info.assert_called_once()
        self.assertFalse(session.search.called)
        module.close()
        session.close.assert_called_once()

    def test_unsupported_es_version_raises(self):
        """Хранилище отвечает версией вне 7.17.x (например, Logspace/ES8)."""
        for number in ("8.11.0", "7.10.2", "6.8.0", "", None):
            with self.subTest(number=number):
                info = {"version": {"number": number}} if number is not None else {}
                with self.assertRaises(StorageNotSupportedError):
                    make_events(es_info=info)

    def test_storage_not_reachable_raises(self):
        """ES недоступен (ConnectionError) - StorageNotSupportedError, не голое исключение."""
        with self.assertRaises(StorageNotSupportedError):
            make_events(es_info=TransportError(500, "ConnectionError", {}))

    def test_empty_storage_hostname_raises(self):
        auth = fake_auth(storage_hostname=None)
        with (
            mock.patch("mpsiemlib.modules.Events.Elasticsearch"),
            self.assertRaises(RuntimeError),
        ):
            Events(auth, Settings())

    def test_client_created_for_storage_host(self):
        with mock.patch("mpsiemlib.modules.Events.Elasticsearch") as es_cls:
            session = mock.MagicMock()
            session.info.return_value = ES_INFO_7_17
            es_cls.return_value = session
            Events(fake_auth(), Settings()).close()
        self.assertEqual(es_cls.call_args.kwargs["hosts"], "es.invalid")
        self.assertEqual(es_cls.call_args.kwargs["port"], 9200)


class IndexesListTestCase(unittest.TestCase):
    """Расчёт backing-индексов data stream за интервал."""

    def test_data_stream_backing_indexes(self):
        module, session = make_events()
        session.indices.get_data_stream.return_value = {
            "data_streams": [
                {
                    "name": "siem_events",
                    "indices": [
                        {"index_name": ".ds-siem_events-2025.01.01-000001"},
                        {"index_name": ".ds-siem_events-2025.01.02-000001"},
                        {"index_name": ".ds-siem_events-2025.01.05-000001"},
                    ],
                },
                {
                    "name": "siem_events_arch",
                    "indices": [{"index_name": ".ds-siem_events-2025.01.01-000009"}],
                },
            ]
        }
        self.assertEqual(
            module._get_indexes_list(BEGIN, END),
            [".ds-siem_events-2025.01.01-000001", ".ds-siem_events-2025.01.02-000001"],
        )
        session.indices.get_data_stream.assert_called_once_with(name="*")
        module.close()

    def test_data_stream_absent_returns_empty(self):
        module, session = make_events()
        session.indices.get_data_stream.return_value = {"data_streams": []}
        self.assertEqual(module._get_indexes_list(BEGIN, END), [])
        module.close()


class ResponseParsingTestCase(unittest.TestCase):
    """Разбор ответов ES: агрегации, пустые ответы, ошибки шардов."""

    def test_make_return_schema_uses_aliases(self):
        schema = Events._make_return_schema(
            {"fields": "dst/ip as object,src/ip as subject, normalized"}
        )
        self.assertEqual(schema, {"object": "", "subject": "", "normalized": ""})

    def test_convert_single_aggregation(self):
        module, _ = make_events()
        aggs = {"normalized": {"buckets": [{"key": "true", "doc_count": 7}]}}
        self.assertEqual(
            module._convert_aggregation_response(aggs),
            [{"normalized": "true", "count": 7}],
        )
        module.close()

    def test_convert_nested_aggregations(self):
        module, _ = make_events()
        aggs = {
            "host": {
                "buckets": [
                    {
                        "key": "host-a",
                        "doc_count": 3,
                        "generator": {"buckets": [{"key": "windows", "doc_count": 2}]},
                    }
                ]
            }
        }
        self.assertEqual(
            module._convert_aggregation_response(aggs),
            [{"generator": "windows", "count": 2, "host": "host-a"}],
        )
        module.close()

    def test_convert_empty_aggregations(self):
        module, _ = make_events()
        self.assertEqual(module._convert_aggregation_response({}), [])
        module.close()

    def test_is_empty_response(self):
        module, _ = make_events()
        cases = [
            ({}, True),
            (None, True),
            ({"hits": {"total": {"value": 0}, "hits": []}}, True),
            ({"hits": {"total": {"value": 5}, "hits": []}}, False),
            ({"aggregations": {}}, True),
            ({"aggregations": {"a": {"buckets": []}}}, True),
            (
                {"aggregations": {"a": {"buckets": [{"key": "x", "doc_count": 1}]}}},
                False,
            ),
        ]
        for response, expected in cases:
            with self.subTest(response=response):
                if not response:
                    with self.assertLogs("Events", level="ERROR"):
                        self.assertIs(module._is_empty_response(response), expected)
                else:
                    self.assertIs(module._is_empty_response(response), expected)
        module.close()

    def test_check_response_shard_errors_clears_response(self):
        module, _ = make_events()
        response = {"error": {"root_cause": [{"type": "index_not_found_exception"}]}}
        with self.assertLogs("Events", level="ERROR") as logs:
            module._check_storage_response(response)
        self.assertEqual(response, {})
        self.assertIn("index_not_found_exception", "\n".join(logs.output))
        module.close()

    def test_check_response_warns_about_partial_results(self):
        module, _ = make_events()
        responses = [
            {"timed_out": True, "_shards": {"failed": 0}},
            {"timed_out": False, "_shards": {"failed": 2}},
            {
                "_shards": {"failed": 0},
                "aggregations": {"a": {"doc_count_error_upper_bound": 5}},
            },
            {
                "_shards": {"failed": 0},
                "aggregations": {"a": {"sum_other_doc_count": 11}},
            },
        ]
        for response in responses:
            with (
                self.subTest(response=response),
                self.assertLogs("Events", level="WARNING"),
            ):
                module._check_storage_response(response)
        module.close()

    def test_check_response_clean_is_silent(self):
        module, _ = make_events()
        response = {
            "timed_out": False,
            "_shards": {"failed": 0},
            "aggregations": {
                "a": {"doc_count_error_upper_bound": 0, "sum_other_doc_count": 0}
            },
        }
        with self.assertNoLogs("Events", level="WARNING"):
            module._check_storage_response(response)
        module.close()


class GetEventsGroupByTestCase(unittest.TestCase):
    """get_events_group_by: запрос к ES и форма результата."""

    def test_requires_fields(self):
        module, session = make_events()
        for filters in (None, {}, {"fields": ""}, {"fields": None}, {"fields": 1}):
            with self.subTest(filters=filters), self.assertRaises(RuntimeError):
                list(module.get_events_group_by(filters, BEGIN, END))
        self.assertFalse(session.search.called)
        module.close()

    def test_yields_rows_with_schema(self):
        module, session = make_events()
        session.search.return_value = {
            "timed_out": False,
            "_shards": {"failed": 0},
            "hits": {"total": {"value": 3}, "hits": []},
            "aggregations": {
                "generator": {
                    "doc_count_error_upper_bound": 0,
                    "sum_other_doc_count": 0,
                    "buckets": [
                        {"key": "windows", "doc_count": 2},
                        {"key": "linux", "doc_count": 1},
                    ],
                }
            },
        }
        rows = list(
            module.get_events_group_by(
                {
                    "es_filter": ['{"term": {"normalized": "true"}}'],
                    "fields": "generator/type as generator",
                },
                BEGIN,
                END,
            )
        )
        self.assertEqual(
            rows,
            [{"generator": "windows", "count": 2}, {"generator": "linux", "count": 1}],
        )
        kwargs = session.search.call_args.kwargs
        self.assertEqual(kwargs["index"], INTERVAL_INDEXES)
        self.assertEqual(kwargs["size"], 0)
        self.assertTrue(kwargs["ignore_unavailable"])
        self.assertEqual(kwargs["request_timeout"], 360)
        self.assertEqual(
            kwargs["aggs"]["generator"]["terms"]["field"], "generator/type"
        )
        module.close()

    def test_null_fields_restored_from_schema(self):
        """ES не отдаёт поле в бакете - в строке оно восстанавливается из схемы."""
        module, session = make_events()
        session.search.return_value = {
            "timed_out": False,
            "_shards": {"failed": 0},
            "hits": {"total": {"value": 1}, "hits": []},
            "aggregations": {"host": {"buckets": [{"key": "h", "doc_count": 1}]}},
        }
        rows = list(
            module.get_events_group_by(
                {"fields": "event_src/host as host, generator/type as generator"},
                BEGIN,
                END,
            )
        )
        self.assertEqual(rows, [{"host": "h", "generator": "", "count": 1}])
        module.close()

    def test_empty_result_yields_single_empty_schema_row(self):
        module, session = make_events()
        session.search.return_value = {
            "timed_out": False,
            "_shards": {"failed": 0},
            "hits": {"total": {"value": 0}, "hits": []},
            "aggregations": {},
        }
        rows = list(module.get_events_group_by({"fields": "normalized"}, BEGIN, END))
        self.assertEqual(rows, [{"normalized": "", "count": 0}])
        module.close()

    def test_shard_errors_report_empty_result(self):
        """root_cause в ответе: данные теряются, наружу - пустая схема."""
        module, session = make_events()
        session.search.return_value = {
            "error": {"root_cause": [{"type": "search_phase_execution_exception"}]}
        }
        with self.assertLogs("Events", level="ERROR"):
            rows = list(
                module.get_events_group_by({"fields": "normalized"}, BEGIN, END)
            )
        self.assertEqual(rows, [{"normalized": "", "count": 0}])
        module.close()

    def test_no_indexes_skips_request(self):
        """Data stream на эти даты отсутствует - в ES не идём, ответ пустой."""
        module, session = make_events()
        session.indices.get_data_stream.return_value = {"data_streams": []}
        rows = list(module.get_events_group_by({"fields": "normalized"}, BEGIN, END))
        session.search.assert_not_called()
        self.assertEqual(rows, [{"normalized": "", "count": 0}])
        module.close()


class GetEventsTestCase(unittest.TestCase):
    """get_events: выборка через point in time + search_after."""

    @staticmethod
    def _settings(batch_size: int = 10000) -> Settings:
        settings = Settings()
        settings.storage_batch_size = batch_size
        return settings

    def test_single_page(self):
        """Пачка меньше batch_size - последней: одной страницы достаточно."""
        module, session = make_events()
        hits = [{"_id": "1", "sort": [0]}, {"_id": "2", "sort": [1]}]
        session.search.return_value = batch_response(hits)
        rows = list(
            module.get_events(
                {"es_filter": ['{"term": {"normalized": "true"}}']}, BEGIN, END
            )
        )
        self.assertEqual(rows, hits)

        session.open_point_in_time.assert_called_once_with(
            index=INTERVAL_INDEXES,
            keep_alive=module._PIT_KEEP_ALIVE,
            ignore_unavailable=True,
            request_timeout=360,
        )
        body = session.search.call_args.kwargs["body"]
        self.assertNotIn("search_after", body)
        self.assertEqual(body["size"], Settings().storage_batch_size)
        self.assertEqual(body["sort"], [{module._SHARD_DOC_FIELD: "asc"}])
        self.assertEqual(
            body["pit"], {"id": "pit-1", "keep_alive": module._PIT_KEEP_ALIVE}
        )
        session.close_point_in_time.assert_called_once_with(body={"pit_id": "pit-1"})
        module.close()

    def test_pagination_uses_search_after(self):
        """batch_size=2: полная пачка -> второй запрос с search_after и новым pit_id."""
        settings = self._settings(batch_size=2)
        module, session = make_events(settings=settings)
        page1 = [{"_id": "1", "sort": [0]}, {"_id": "2", "sort": [1]}]
        page2 = [{"_id": "3", "sort": [2]}]
        session.search.side_effect = [
            batch_response(page1, pit_id="pit-2"),
            batch_response(page2, pit_id="pit-3"),
        ]
        rows = list(module.get_events({}, BEGIN, END))
        self.assertEqual(rows, page1 + page2)
        self.assertEqual(session.search.call_count, 2)

        second_body = session.search.call_args_list[1].kwargs["body"]
        self.assertEqual(second_body["search_after"], [1])
        self.assertEqual(second_body["pit"]["id"], "pit-2")
        # закрывается последний полученный pit_id
        session.close_point_in_time.assert_called_once_with(body={"pit_id": "pit-3"})
        module.close()

    def test_close_point_in_time_on_early_consumer_exit(self):
        """Досрочный выход из генератора освобождает PIT (generator.close -> finally)."""
        settings = self._settings(batch_size=1)
        module, session = make_events(settings=settings)
        session.search.side_effect = [
            batch_response([{"_id": "1", "sort": [0]}], pit_id="pit-2"),
            batch_response([{"_id": "2", "sort": [1]}], pit_id="pit-3"),
        ]
        gen = module.get_events({}, BEGIN, END)
        first = next(gen)
        gen.close()
        self.assertEqual(first, {"_id": "1", "sort": [0]})
        session.close_point_in_time.assert_called_once()
        module.close()

    def test_hit_without_sort_stops_pagination(self):
        """Нет sort в последнем hit - дальше листать нельзя, предупреждаем и встаём."""
        settings = self._settings(batch_size=2)
        module, session = make_events(settings=settings)
        page1 = [{"_id": "1"}, {"_id": "2"}]  # без sort
        session.search.return_value = batch_response(page1)
        with self.assertLogs("Events", level="WARNING"):
            rows = list(module.get_events({}, BEGIN, END))
        self.assertEqual(rows, page1)
        self.assertEqual(session.search.call_count, 1)
        session.close_point_in_time.assert_called_once()
        module.close()

    def test_index_not_found_yields_empty_row(self):
        module, session = make_events()
        session.open_point_in_time.side_effect = NotFoundError(
            404,
            "index_not_found_exception",
            {"error": {"type": "index_not_found_exception"}},
        )
        with self.assertLogs("Events", level="ERROR"):
            rows = list(module.get_events({}, BEGIN, END))
        self.assertEqual(rows, [{}])
        session.close_point_in_time.assert_not_called()
        module.close()

    def test_other_not_found_error_propagates(self):
        module, session = make_events()
        session.open_point_in_time.side_effect = NotFoundError(
            404, "alias_not_found_exception", {}
        )
        with self.assertRaises(NotFoundError):
            list(module.get_events({}, BEGIN, END))
        module.close()

    def test_empty_pit_id_raises(self):
        module, session = make_events()
        session.open_point_in_time.return_value = {}
        with self.assertRaises(RuntimeError):
            list(module.get_events({}, BEGIN, END))
        module.close()

    def test_close_pit_error_is_swallowed(self):
        """Ошибка закрытия PIT не перекрывает успешный результат."""
        module, session = make_events()
        session.search.return_value = batch_response([{"_id": "1", "sort": [0]}])
        session.close_point_in_time.side_effect = TransportError(500, "boom", {})
        with self.assertLogs("Events", level="WARNING"):
            rows = list(module.get_events({}, BEGIN, END))
        self.assertEqual(rows, [{"_id": "1", "sort": [0]}])
        module.close()

    def test_no_indexes_skips_request(self):
        """Data stream на эти даты отсутствует - PIT не открываем, пусто."""
        module, session = make_events()
        session.indices.get_data_stream.return_value = {"data_streams": []}
        with self.assertLogs("Events", level="WARNING"):
            rows = list(module.get_events({}, BEGIN, END))
        session.open_point_in_time.assert_not_called()
        session.search.assert_not_called()
        self.assertEqual(rows, [])
        module.close()


if __name__ == "__main__":
    unittest.main()
