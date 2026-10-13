import unittest
from datetime import datetime

import pytz
import requests
from settings import creds_pat, settings

from mpsiemlib.common import *
from mpsiemlib.modules import MPSIEMWorker


class TestEventsAPITestCase(unittest.TestCase):
    """Тесты модуля EventsAPI.

    Каждый тест самодостаточен и опирается только на существующие на стенде
    события/инциденты (данные read-only, ничего не создаётся).
    """

    __mpsiemworker = None
    __module = None
    __incidents = None
    __creds = creds_pat
    __settings = settings
    __begin = 0
    __end = 0
    __v3_begin = 0

    @classmethod
    def setUpClass(cls) -> None:
        cls.__mpsiemworker = MPSIEMWorker(cls.__creds, cls.__settings)
        cls.__module = cls.__mpsiemworker.get_module(ModuleNames.EVENTSAPI)
        cls.__incidents = cls.__mpsiemworker.get_module(ModuleNames.INCIDENTS)
        cls.__end = round(
            datetime.now(tz=pytz.timezone(settings.local_timezone)).timestamp()
        )
        # Окно с запасом: на стенде с малым потоком событий за 60 секунд
        # выборка может оказаться пустой.
        cls.__begin = cls.__end - 3600
        # Для v3 окно уже: итератор выгружает выборку целиком, а стенд даёт
        # ~67k событий в час (~100 c на выгрузку). 5 минут достаточно, чтобы
        # выборка была непустой, и держит тест в пределах нескольких пачек.
        cls.__v3_begin = cls.__end - 300

    @classmethod
    def tearDownClass(cls) -> None:
        cls.__module.close()
        cls.__incidents.close()

    def _first_event(self) -> dict:
        """Первое событие v3 за окно теста."""
        return next(
            self.__module.get_events_by_filter_v3(
                query_filter="select(time, event_src.host, text)",
                time_from=self.__begin,
                time_to=self.__end,
            )
        )

    def _first_v3_page(self) -> dict:
        """Первая страница v3 (сырой ответ: events + token)."""
        return self.__module.get_events_by_filter_v3_page(
            query_filter="select(time, event_src.host, text)",
            time_from=self.__begin,
            time_to=self.__end,
            offset=0,
            limit=5,
        )

    def __skip_if_status(self, err: requests.HTTPError, code: int, action: str) -> None:
        if getattr(err.response, "status_code", None) == code:
            self.skipTest(f"{action}: HTTP {code} на текущих правах/данных стенда")
        raise err

    # ------------------------------------------------------------------ #
    # метаданные и группировки
    # ------------------------------------------------------------------ #
    def test_get_events_metadata(self):
        fields = self.__module.get_events_metadata()
        self.assertTrue(len(fields) > 0)
        self.assertTrue(all(isinstance(f, dict) and len(f) > 0 for f in fields))

    def test_get_events_grouped_by_fields(self):
        result = self.__module.get_events_grouped_by_fields(
            query_filter="normalized = true",
            group_by_fields=["id"],
            time_from=self.__begin,
            time_to=self.__end,
        )
        self.assertTrue(isinstance(result, dict))
        self.assertGreater(len(result), 0)
        self.assertTrue(all(isinstance(v, int) for v in result.values()))

    def test_get_events_group_by_api_json(self):
        params = {
            "filter": {
                "select": ["time", "event_src.host", "text"],
                "where": "normalized = true",
                "orderBy": [{"field": "time", "sortOrder": "descending"}],
                "groupBy": ["event_src.host"],
                "aggregateBy": [{"function": "COUNT", "field": "*", "unique": False}],
                "distributeBy": [],
                "top": 100,
                "aliases": {"groupBy": {}, "aggregateBy": {"COUNT": "Cnt"}},
            },
            "timeFrom": self.__begin,
            "timeTo": self.__end,
        }
        rows = self.__module.get_events_group_by_api_json(params=params)
        self.assertTrue(isinstance(rows, list) and len(rows) > 0)
        self.assertIn("groups", rows[0])
        self.assertIn("values", rows[0])

    # ------------------------------------------------------------------ #
    # выборки событий
    # ------------------------------------------------------------------ #
    def test_get_events_by_filter_v3(self):
        result = list(
            self.__module.get_events_by_filter_v3(
                query_filter="select(time, event_src.host, text) | sort(time desc)",
                time_from=self.__v3_begin,
                time_to=self.__end,
            )
        )
        self.assertGreater(len(result), 0)
        self.assertIn("_meta", result[0])
        self.assertIn("time", result[0])

    def test_get_events_by_filter_v2(self):
        events = self.__module.get_events_by_filter(
            pdql_filter="normalized = true",
            fields=["time", "event_src.host", "text"],
            time_from=self.__begin,
            time_to=self.__end,
            limit=5,
            offset=0,
        )
        self.assertEqual(len(events), 5)
        for event in events:
            self.assertIn("time", event)
            self.assertIn("_meta", event)

    def test_get_events_by_filter_v2_offset(self):
        """offset=0 и offset=1 (limit=1) возвращают разные события."""
        first = self.__module.get_events_by_filter(
            pdql_filter="normalized = true",
            fields=["time"],
            time_from=self.__begin,
            time_to=self.__end,
            limit=1,
            offset=0,
        )
        second = self.__module.get_events_by_filter(
            pdql_filter="normalized = true",
            fields=["time"],
            time_from=self.__begin,
            time_to=self.__end,
            limit=1,
            offset=1,
        )
        self.assertEqual(len(first), 1)
        self.assertEqual(len(second), 1)
        self.assertNotEqual(first[0]["_meta"]["id"], second[0]["_meta"]["id"])

    # ------------------------------------------------------------------ #
    # детали события
    # ------------------------------------------------------------------ #
    def test_get_event_details(self):
        event = self._first_event()
        details = self.__module.get_event_details(
            event_id=event["_meta"]["id"],
            event_date=event["_meta"]["time"],
        )
        self.assertTrue(isinstance(details, dict) and len(details) > 0)
        self.assertIn("normalized", details)

    # ------------------------------------------------------------------ #
    # события инцидента
    # ------------------------------------------------------------------ #
    def test_get_events_for_incident(self):
        incidents_begin = self.__end - 86400 * 30
        incident = next(
            self.__incidents.get_incidents_list(incidents_begin, self.__end),
            None,
        )
        if incident is None:
            self.skipTest("На стенде нет инцидентов за последние 30 дней")
        events = self.__module.get_events_for_incident(
            fields=None,
            incident_id=incident.get("id"),
            time_from=self.__begin,
            time_to=self.__end,
            limit=5,
            offset=0,
        )
        self.assertTrue(isinstance(events, list))
        if len(events) > 0:
            self.assertIn("id", events[0])
            self.assertIn("date", events[0])

    # ------------------------------------------------------------------ #
    # Агрегация v3 (конвейерный PDQL group(...), контракт GetAggregation v3)
    # ------------------------------------------------------------------ #
    def test_get_events_aggregation_v3(self):
        # Синтаксис v3-агрегации (отличается от v2 structured aggregateBy):
        # обязателен select(...) c временем и секция group(key:[...], agg: ...).
        query = (
            "select(time, event_src.host) | "
            "group(key: [event_src.host], agg: COUNT(*) as Cnt) | "
            "sort(Cnt desc)"
        )
        result = self.__module.get_events_aggregation_v3(
            query_filter=query,
            time_from=self.__begin,
            time_to=self.__end,
            limit=100,
        )
        for key in ("errors", "hasMoreResults", "columns", "rows"):
            self.assertIn(key, result)
        self.assertIsInstance(result["rows"], list)
        self.assertGreater(len(result["rows"]), 0)
        self.assertIn("groups", result["rows"][0])
        self.assertIn("values", result["rows"][0])

    def test_get_events_aggregation_v3_limit(self):
        # limit ограничивает число строк распределения. Поле text не groupable
        # ("Неверные поля в параметре group: text"), группируем двумя
        # groupable-полями.
        query = (
            "select(time, event_src.host, event_src.category) | "
            "group(key: [event_src.host, event_src.category], "
            "agg: COUNT(*) as Cnt) | sort(Cnt desc)"
        )
        result = self.__module.get_events_aggregation_v3(
            query_filter=query,
            time_from=self.__begin,
            time_to=self.__end,
            limit=1,
        )
        self.assertLessEqual(len(result["rows"]), 1)

    def test_get_events_aggregation_v3_invalid_pdql(self):
        # PDQL без секции group(...) - Core отвечает 400
        with self.assertRaises(requests.HTTPError):
            self.__module.get_events_aggregation_v3(
                query_filter="select(time)",
                time_from=self.__begin,
                time_to=self.__end,
            )

    # ------------------------------------------------------------------ #
    # Метаданные таксономии и настройки
    # ------------------------------------------------------------------ #
    def test_get_events_taxonomy(self):
        fields = self.__module.get_events_taxonomy()
        self.assertGreater(len(fields), 0)
        self.assertTrue(all(isinstance(f, dict) and "name" in f for f in fields))
        names = {f["name"] for f in fields}
        for required in ("time", "event_src.host", "normalized"):
            self.assertIn(required, names)

    def test_get_events_settings(self):
        settings_map = self.__module.get_events_settings()
        self.assertIsInstance(settings_map, dict)
        self.assertIn("showContentFromDescendants", settings_map)

    # ------------------------------------------------------------------ #
    # Постраничная выдача v3 и счётчик по токену
    # ------------------------------------------------------------------ #
    def test_get_events_by_filter_v3_page(self):
        page = self._first_v3_page()
        self.assertIn("events", page)
        self.assertIn("token", page)
        self.assertTrue(len(page["events"]) > 0)

    def test_get_events_count_by_token(self):
        page = self._first_v3_page()
        total = self.__module.get_events_count_by_token(page["token"])
        self.assertIsInstance(total, int)
        self.assertGreaterEqual(total, len(page["events"]))

    def test_get_events_count_by_token_not_found(self):
        # Контракт: неизвестный токен - 404 TokenNotFoundError
        with self.assertRaises(requests.HTTPError):
            self.__module.get_events_count_by_token(
                "ffffffff-0000-0000-0000-000000000000"
            )

    # ------------------------------------------------------------------ #
    # Подсчёт событий
    # ------------------------------------------------------------------ #
    def test_get_events_count(self):
        result = self.__module.get_events_count(
            query_filter="normalized = true",
            time_from=self.__begin,
            time_to=self.__end,
        )
        self.assertIn("totalCount", result)
        self.assertGreater(result["totalCount"], 0)

    def test_get_events_count_v3(self):
        # В v3 время в запросе не задаётся (см. docstring метода): считаем по
        # фильтру и сравниваем структуру ответа с v2-счётчиком.
        result = self.__module.get_events_count_v3(
            query_filter="select(time, event_src.host)"
        )
        self.assertIn("totalCount", result)
        self.assertIsInstance(result["totalCount"], int)
        self.assertGreater(result["totalCount"], 0)
        self.assertIn("periodStart", result)
        self.assertIn("periodEnd", result)

    # ------------------------------------------------------------------ #
    # Значения полей
    # ------------------------------------------------------------------ #
    def test_count_distinct_field_values(self):
        values = self.__module.count_distinct_field_values(
            query_filter="normalized = true",
            fields=["event_src.host"],
            time_from=self.__begin,
            time_to=self.__end,
            top=10,
        )
        self.assertIsInstance(values, list)
        self.assertGreater(len(values), 0)
        for item in values:
            self.assertIn("count", item)

    def test_get_group_values(self):
        result = self.__module.get_group_values(
            query_filter="normalized = true",
            fields=["event_src.host"],
            time_from=self.__begin,
            time_to=self.__end,
            top=10,
        )
        self.assertIn("groups", result)
        self.assertGreater(len(result["groups"]), 0)
        self.assertIn("fieldValues", result["groups"][0])

    # ------------------------------------------------------------------ #
    # Локализация события (требует прав выше read-only: под PAT - 403)
    # ------------------------------------------------------------------ #
    def test_get_event_localization(self):
        event = self._first_event()
        details = self.__module.get_event_details(
            event_id=event["_meta"]["id"], event_date=event["_meta"]["time"]
        )
        try:
            result = self.__module.get_event_localization(details, locale="RUS")
        except requests.HTTPError as err:
            self.__skip_if_status(err, 403, "get_event_localization")
        else:
            self.assertIn("text", result)

    # ------------------------------------------------------------------ #
    # Вложенные (sub)события: на стенде берём событие без subevents -
    # сервер отвечает 400 (поле-связка отсутствует). Негативный кейс
    # подтверждает, что эндпоинт жив и доходит до валидации Core.
    # ------------------------------------------------------------------ #
    def test_get_event_subevents_missing_field(self):
        event = self._first_event()
        with self.assertRaises(requests.HTTPError):
            self.__module.get_event_subevents(
                event_id=event["_meta"]["id"],
                event_date=event["_meta"]["time"],
                query_filter="normalized = true",
                time_from=self.__begin,
                time_to=self.__end,
            )

    def test_get_event_subevents_count_missing_field(self):
        event = self._first_event()
        with self.assertRaises(requests.HTTPError):
            self.__module.get_event_subevents_count(
                event_id=event["_meta"]["id"],
                event_date=event["_meta"]["time"],
                query_filter="normalized = true",
                time_from=self.__begin,
                time_to=self.__end,
            )

    # ------------------------------------------------------------------ #
    # Raw-событие: на ядрах <28.0 эндпоинт жив (возвращает dict),
    # на R28.0+ выведен из маршрутизации и закрыт version-gate
    # (RuntimeError до запроса). Проверяем обе ветки без завязки на версию.
    # ------------------------------------------------------------------ #
    def test_get_event_raw(self):
        event = self._first_event()
        try:
            raw = self.__module.get_event_raw(
                event_id=event["_meta"]["id"], recv_time=event["_meta"]["time"]
            )
        except RuntimeError as err:
            # version-gate на R28.0+
            self.assertIn("raw event API", str(err))
        else:
            self.assertIsInstance(raw, dict)


if __name__ == "__main__":
    unittest.main()
