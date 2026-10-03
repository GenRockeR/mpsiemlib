import unittest
from datetime import datetime

import pytz
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


if __name__ == "__main__":
    unittest.main()
