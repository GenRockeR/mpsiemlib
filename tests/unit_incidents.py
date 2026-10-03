import json
import unittest
from datetime import UTC, datetime

from settings import creds_pat, settings

from mpsiemlib.common import *
from mpsiemlib.modules import MPSIEMWorker


class IncidentsTestCase(unittest.TestCase):
    __mpsiemworker = None
    __module = None
    __creds = creds_pat
    __settings = settings
    __begin = 0
    __end = 0
    __incident = None
    __info = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.__mpsiemworker = MPSIEMWorker(cls.__creds, cls.__settings)
        cls.__module = cls.__mpsiemworker.get_module(ModuleNames.INCIDENTS)
        cls.__end = round(datetime.now(tz=UTC).timestamp())
        cls.__begin = cls.__end - 86400 * 30
        cls.__incident = next(
            cls.__module.get_incidents_list(cls.__begin, cls.__end), None
        )
        cls.__info = (
            cls.__module.get_incident_info(cls.__incident.get("id"))
            if cls.__incident is not None
            else None
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.__module.close()

    def __incident_key(self) -> str:
        incident = self.__incident
        if incident is None:
            self.skipTest("No incidents on the stand for the last 30 days")
        return str(incident.get("key"))

    def __incident_id(self) -> str:
        self.__incident_key()
        return str(self.__incident.get("id"))

    def __incident_info(self) -> dict:
        self.__incident_id()
        return self.__info

    def test_get_list(self):
        counter = 0
        for incident in self.__module.get_incidents_list(self.__begin, self.__end):
            counter += 1
            with self.subTest(incident=incident.get("key")):
                self.assertEqual(
                    sorted(incident),
                    sorted(
                        [
                            "id",
                            "key",
                            "created",
                            "name",
                            "confirmed",
                            "status",
                            "category",
                            "type",
                            "assigned",
                            "severity",
                        ]
                    ),
                )
                self.assertIsNotNone(incident.get("id"))
                self.assertIsNotNone(incident.get("key"))

        self.assertGreater(counter, 0)

    def test_get_list_filters(self):
        filters = {"where": f'key="{self.__incident_key()}"'}
        incidents = list(
            self.__module.get_incidents_list(self.__begin, self.__end, filters=filters)
        )

        print(json.dumps(incidents, indent=4, ensure_ascii=False))

        self.assertEqual(len(incidents), 1)
        self.assertEqual(incidents[0].get("key"), self.__incident_key())
        self.assertEqual(incidents[0].get("id"), self.__incident_id())

    def test_get_id_by_key(self):
        incident_id = self.__module.get_incident_id_by_key(self.__incident_key())

        print(json.dumps(incident_id, indent=4, ensure_ascii=False))

        self.assertEqual(incident_id, self.__incident_id())

    def test_get_id_by_key_unknown(self):
        self.assertIsNone(self.__module.get_incident_id_by_key("INC-000000000"))

    def test_get_info(self):
        incident = self.__incident_info()

        print(json.dumps(incident, indent=4, ensure_ascii=False))

        self.assertEqual(incident.get("id"), self.__incident_id())
        self.assertEqual(incident.get("key"), self.__incident_key())
        # enum-поля нормализованы в нижний регистр
        for field in ("status", "category", "type"):
            value = incident.get(field)
            self.assertIsInstance(value, str)
            self.assertEqual(value, value.lower())
        # имена коррелирующих правил (correlationRules, а не correlationRuleNames)
        self.assertIsInstance(incident.get("correlation_rule"), list)
        # связанные коллекции
        self.assertIsInstance(incident.get("comments"), list)
        self.assertIsInstance(incident.get("events"), list)
        self.assertIsInstance(incident.get("issues"), list)

    def test_get_info_comments(self):
        comments = self.__incident_info().get("comments")
        if not comments:
            self.skipTest("Incident has no status transitions on the stand")

        for comment in comments:
            with self.subTest(comment=comment.get("comment")):
                self.assertEqual(
                    sorted(comment),
                    sorted(
                        ["status_old", "status_new", "comment", "timestamp", "user_id"]
                    ),
                )

    def test_get_info_matches_dictionaries(self):
        incident = self.__incident_info()
        statuses = [s.lower() for s in self.__module.get_incident_statuses()]
        severities = [s.lower() for s in self.__module.get_incident_severities()]

        self.assertIn(incident.get("status"), statuses)
        self.assertIn(incident.get("severity"), severities)

    def test_get_short_info(self):
        short_info = self.__module.get_incident_short_info(self.__incident_id())
        incident = self.__incident_info()

        print(json.dumps(short_info, indent=4, ensure_ascii=False))

        self.assertEqual(
            sorted(short_info), sorted(["id", "key", "name", "source", "version"])
        )
        self.assertEqual(short_info.get("id"), self.__incident_id())
        self.assertEqual(short_info.get("key"), self.__incident_key())
        self.assertEqual(short_info.get("name"), incident.get("name"))
        self.assertIsInstance(short_info.get("version"), int)
        self.assertGreaterEqual(short_info.get("version"), 1)

    def test_get_events_count(self):
        events_count = self.__module.get_incident_events_count(self.__incident_id())
        events = self.__incident_info().get("events")

        print(json.dumps(events_count, indent=4, ensure_ascii=False))

        self.assertIsInstance(events_count, int)
        self.assertGreaterEqual(events_count, 0)
        # get_incident_info поднимает события той же пагинацией - счётчик сходится
        self.assertEqual(events_count, len(events))

    def test_get_events_severity_counts(self):
        severities = self.__module.get_incident_events_severity_counts(
            self.__incident_id()
        )
        events_count = self.__module.get_incident_events_count(self.__incident_id())

        print(json.dumps(severities, indent=4, ensure_ascii=False))

        self.assertIsInstance(severities, list)
        for item in severities:
            self.assertEqual(sorted(item), sorted(["severity", "count"]))
            self.assertEqual(item["severity"], item["severity"].lower())
            self.assertGreater(item["count"], 0)
        # сумма по критичностям равна общему числу событий
        self.assertEqual(sum(item["count"] for item in severities), events_count)

    def test_get_assets(self):
        for asset_type, info_key in (
            ("Targets", "targets"),
            ("attackers", "attackers"),
        ):
            with self.subTest(asset_type=asset_type):
                assets = self.__module.get_incident_assets(
                    self.__incident_id(), asset_type
                )

                print(json.dumps(assets, indent=4, ensure_ascii=False))

                self.assertIsInstance(assets, list)
                for asset in assets:
                    self.assertIsNotNone(asset.get("id"))
                    self.assertIsNotNone(asset.get("name"))
                # число активов из эндпоина согласуется с группой `others`
                # в развёрнутом объекте инцидента
                grouped = self.__incident_info().get(info_key) or {}
                self.assertEqual(
                    len(assets),
                    len(grouped.get("assets") or []) + len(grouped.get("others") or []),
                )

    def test_get_assets_wrong_type(self):
        with self.assertRaises(ValueError):
            self.__module.get_incident_assets(self.__incident_id(), "Undefined")

    def test_get_tasks_empty_ids(self):
        with self.assertRaises(ValueError):
            self.__module.get_incident_tasks([])

    def test_get_tasks(self):
        incident_id = self.__incident_id()
        tasks = self.__module.get_incident_tasks([incident_id])

        print(json.dumps(tasks, indent=4, ensure_ascii=False))

        self.assertIsInstance(tasks, list)
        for task in tasks:
            self.assertEqual(
                sorted(task),
                sorted(
                    [
                        "id",
                        "key",
                        "name",
                        "status",
                        "type",
                        "assigned",
                        "reporter",
                        "incident_id",
                        "created",
                        "updated",
                        "estimated",
                    ]
                ),
            )
            self.assertEqual(task["incident_id"], incident_id)

    def test_get_issues_count(self):
        issues_count = self.__module.get_incident_issues_count(self.__incident_id())
        issues = self.__incident_info().get("issues")

        print(json.dumps(issues_count, indent=4, ensure_ascii=False))

        self.assertIsInstance(issues_count, int)
        self.assertGreaterEqual(issues_count, 0)
        # счётчик задач сходится со списком задач в объекте инцидента
        self.assertEqual(issues_count, len(issues))

    def test_get_netfor_counts(self):
        alerts_count = self.__module.get_incident_netfor_alerts_count(
            self.__incident_id()
        )
        sessions_count = self.__module.get_incident_netfor_sessions_count(
            self.__incident_id()
        )

        print(
            json.dumps(
                {"alerts": alerts_count, "sessions": sessions_count},
                indent=4,
                ensure_ascii=False,
            )
        )

        self.assertIsInstance(alerts_count, int)
        self.assertIsInstance(sessions_count, int)
        self.assertGreaterEqual(alerts_count, 0)
        self.assertGreaterEqual(sessions_count, 0)

    def test_get_incident_queries(self):
        queries_by_category = {}
        for category in ("List", "Notifications"):
            with self.subTest(category=category):
                queries = self.__module.get_incident_queries(category)
                queries_by_category[category] = queries

                print(json.dumps(queries, indent=4, ensure_ascii=False))

                self.assertGreater(len(queries), 0)
                for query in queries:
                    self.assertEqual(sorted(query), sorted(["id", "name"]))
                self.assertIn("all_incidents", [q.get("id") for q in queries])

        # Notifications - подмножество List
        self.assertLessEqual(
            {q["id"] for q in queries_by_category["Notifications"]},
            {q["id"] for q in queries_by_category["List"]},
        )

    def test_get_incident_queries_case_insensitive(self):
        self.assertEqual(
            [q["id"] for q in self.__module.get_incident_queries("list")],
            [q["id"] for q in self.__module.get_incident_queries("List")],
        )

    def test_get_incident_queries_wrong_category(self):
        with self.assertRaises(ValueError):
            self.__module.get_incident_queries("SomethingElse")

    def test_get_incidents_settings(self):
        incidents_settings = self.__module.get_incidents_settings()

        print(json.dumps(incidents_settings, indent=4, ensure_ascii=False))

        self.assertEqual(sorted(incidents_settings), ["show_content_from_descendants"])
        self.assertIsInstance(incidents_settings["show_content_from_descendants"], bool)

    def test_get_dictionaries(self):
        values = {
            "statuses": self.__module.get_incident_statuses(),
            "severities": self.__module.get_incident_severities(),
            "task_statuses": self.__module.get_incident_task_statuses(),
            "task_types": self.__module.get_incident_task_types(),
        }

        print(json.dumps(values, indent=4, ensure_ascii=False))

        for name, items in values.items():
            with self.subTest(dictionary=name):
                self.assertGreater(len(items), 0)
                for item in items:
                    self.assertIsInstance(item, str)

    def test_get_dictionaries_localizations(self):
        localized = {
            "statuses": (
                self.__module.get_incident_statuses_localizations(),
                self.__module.get_incident_statuses(),
                "key",
            ),
            "severities": (
                self.__module.get_incident_severities_localizations(),
                self.__module.get_incident_severities(),
                "key",
            ),
            "task_statuses": (
                self.__module.get_incident_task_statuses_localizations(),
                self.__module.get_incident_task_statuses(),
                "id",
            ),
            "task_types": (
                self.__module.get_incident_task_types_localizations(),
                self.__module.get_incident_task_types(),
                "id",
            ),
        }

        for name, (localization, base, key_field) in localized.items():
            with self.subTest(dictionary=name):
                print(json.dumps(localization, indent=4, ensure_ascii=False))
                self.assertGreater(len(localization), 0)
                for item in localization:
                    self.assertIn(key_field, item)
                    self.assertIn("value" if key_field == "key" else "name", item)
                # ключи локализации соответствуют значениям базового словаря
                self.assertEqual({item[key_field] for item in localization}, set(base))


if __name__ == "__main__":
    unittest.main()
