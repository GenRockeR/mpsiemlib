import contextlib
import json
import time
import unittest
import uuid
from datetime import UTC, datetime
from typing import ClassVar

import requests
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
    __incidents: ClassVar[list[str]] = []
    __filters: ClassVar[list[str]] = []
    __folders: ClassVar[list[str]] = []
    __filter_folder = ""
    __filter_folder_name = ""
    __events_source: ClassVar[dict | None] = None
    __events_scanned = False

    # Тип инцидента из серверного справочника типов (контракт CreateIncident его
    # валидирует: неизвестный тип - 400 `Incorrect incident type`)
    TEST_INCIDENT_TYPE = "CriticalVulnerabilityDetection"
    # guuid корня «Пользовательские фильтры`: в системные папки писать нельзя
    # (403 forbidden.error), создавать объекты можно только здесь
    USER_FILTERS_FOLDER = "00000000-0000-0000-0000-000000000007"
    SYSTEM_FILTERS_FOLDER = "00000000-0000-0000-0000-000000000006"

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
        cls.__filter_folder_name = cls.__unique_name("folder")

    @classmethod
    def tearDownClass(cls) -> None:
        # Write-тесты обязаны убирать за собой объекты даже при падении кейса.
        # Фильтры удалять не нужно: удаление папки каскадно снимает её
        # содержимое (замерено на R28.0)
        for folder_id in cls.__folders:
            cls.__quietly(cls.__module.delete_incident_filter_folder, folder_id)
        for filter_id in cls.__filters:
            cls.__quietly(cls.__module.delete_incident_filter, filter_id)
        if cls.__filter_folder:
            cls.__quietly(
                cls.__module.delete_incident_filter_folder, cls.__filter_folder
            )
        if cls.__incidents:
            # Пакетное удаление на R28.0 теряет часть ID при длинном списке
            # (204 приходит, но инциденты остаются живыми), поэтому удаляем
            # по одному
            for incident_id in cls.__incidents:
                cls.__quietly(cls.__module.delete_incidents, [incident_id])
        cls.__module.close()

    @staticmethod
    def __quietly(func, *args) -> None:
        with contextlib.suppress(requests.RequestException, ValueError):
            func(*args)

    @classmethod
    def __unique_name(cls, prefix: str) -> str:
        return f"test_{prefix}_{uuid.uuid4().hex[:8]}"

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
        return dict(self.__info or {})

    # ------------------------------ helpers ------------------------------ #

    def __new_incident(self, **kwargs) -> dict:
        """Создать инцидент для write-кейсов и зарегистрировать в cleanup.

        Core отвечает только `{id, key}`, поэтому имя добавляем из запроса -
        оно нужно кейсам для выборки по фильтру.
        """
        params: dict = {
            "name": self.__unique_name("inc"),
            "incident_type": self.TEST_INCIDENT_TYPE,
            "severity": "Low",
            # Core требует, чтобы время обнаружения было в прошлом
            "detected": self.__end - 86400,
        }
        params.update(kwargs)
        incident = self.__module.create_incident(**params)
        self.__incidents.append(incident["id"])
        return {**incident, "name": str(params["name"])}

    def __test_filter_folder(self) -> str:
        """Создать пользовательскую папку фильтров (один раз на класс)."""
        if not self.__filter_folder:
            # Пишем в атрибут класса: иначе каждый экземпляр теста создавал бы
            # свою папку, а collide по имени (400 `В папке уже есть папки с
            # таким именем`)
            type(self).__filter_folder = self.__module.create_incident_filter_folder(
                name=self.__filter_folder_name,
                parent_id=self.USER_FILTERS_FOLDER,
            )["id"]
        return self.__filter_folder

    def __new_folder(self) -> str:
        """Создать отдельную пользовательскую папку и зарегистрировать её."""
        folder_id = self.__module.create_incident_filter_folder(
            name=self.__unique_name("folder"), parent_id=self.USER_FILTERS_FOLDER
        )["id"]
        self.__folders.append(folder_id)
        return folder_id

    def __new_filter(self, **kwargs) -> dict:
        """Создать фильтр в тестовой папке и зарегистрировать в cleanup."""
        params: dict = {
            "folder_id": self.__test_filter_folder(),
            "name": self.__unique_name("filter"),
            "select": ["key", "name", "status"],
            "where": 'status != "Closed"',
            "orderby": [{"field": "created", "sortOrder": "descending"}],
        }
        params.update(kwargs)
        created = self.__module.create_incident_filter(**params)
        self.__filters.append(created["id"])
        # сервер отвечает только `{id}` - имя добавляем, чтобы кейсы сверяли
        # чтение с запросом
        return {**created, "name": str(params["name"])}

    def __incident_with_events(self) -> dict | None:
        """Инцидент с привязанными событиями (для кейсов link/unlink).

        Первый инцидент выборки событий может не иметь, поэтому перебираем
        список (не больше 20 объектов) и кэшируем результат на весь прогон.
        """
        if not self.__events_scanned:
            type(self).__events_scanned = True
            for candidate in self.__module.get_incidents_list(self.__begin, self.__end):
                try:
                    events = self.__module.get_incident_events_by_id(candidate["id"])
                except requests.RequestException:
                    continue
                if events:
                    type(self).__events_source = candidate
                    break
        return self.__events_source

    def __assert_incident_deleted(self, incident_id: str) -> None:
        """Подтвердить удаление инцидента.

        `get_deleted_incident_ids` на R28.0 отвечает ложь (в «удалённые»
        попадают живые инциденты), поэтому факт удаления сверяем по экспорту:
        он стабильно отдаёт пустой список после удаления.
        """
        for _ in range(5):
            if not self.__module.export_incidents_by_ids([incident_id])["incidents"]:
                return
            time.sleep(2)
        self.fail(f"Incident {incident_id} is still exported after deletion")

    def __hierarchy_node_ids(
        self, notifications: bool = False, system_only: bool = True
    ) -> set[str]:
        tree = self.__module.get_incident_filters_hierarchy(notifications)
        ids = set()
        for node in self.__walk_hierarchy(tree.get("roots") or []):
            if system_only and (node.get("meta") or {}).get("source") != "system":
                continue
            ids.add(str(node["id"]))
        return ids

    @staticmethod
    def __walk_hierarchy(nodes):
        for node in nodes:
            yield node
            yield from IncidentsTestCase.__walk_hierarchy(node.get("children") or [])

    def __first_hierarchy_filter_id(self) -> str:
        """Первый filter-узел дерева фильтров (системный, read-only)."""
        tree = self.__module.get_incident_filters_hierarchy()
        node = next(
            (
                n
                for n in self.__walk_hierarchy(tree.get("roots") or [])
                if n.get("type") == "filter"
            ),
            None,
        )
        if node is None:
            self.skipTest("Incidents filters hierarchy has no filters")
        return str(node["id"])

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
        comments = self.__incident_info().get("comments") or []
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
        # версия меняется только от правок (замерено на R28.0): у инцидента из
        # корреляционных правил она равна 0
        self.assertGreaterEqual(short_info.get("version"), 0)

    def test_get_events_count(self):
        events_count = self.__module.get_incident_events_count(self.__incident_id())
        events = self.__incident_info().get("events") or []

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
                # эндпоинт отдаёт плоский список AssetDto, тогда как объект
                # инцидента группирует их по типам: сверяем с суммой
                # групп, активов и произвольных сущностей
                grouped = self.__incident_info().get(info_key) or {}
                self.assertEqual(
                    len(assets),
                    len(grouped.get("groups") or [])
                    + len(grouped.get("assets") or [])
                    + len(grouped.get("others") or []),
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
        issues = self.__incident_info().get("issues") or []

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

    def test_get_incident_dictionaries(self):
        dictionaries = {
            "categories": self.__module.get_incident_categories(),
            "influences": self.__module.get_incident_influences(),
            "types": self.__module.get_incident_types(),
            "parameters": self.__module.get_incident_parameters(),
        }

        for name, items in dictionaries.items():
            with self.subTest(dictionary=name):
                print(json.dumps(items, indent=4, ensure_ascii=False))
                self.assertIsInstance(items, list)
                for item in items:
                    self.assertIsInstance(item, dict)
                    self.assertIn("id", item)
                    self.assertIn("name", item)

        # типы из справочника типов согласуются с иерархией типов по категориям
        types = {item["id"] for item in dictionaries["types"]}
        hierarchy = self.__module.get_incident_types_hierarchy()
        print(json.dumps(hierarchy, indent=4, ensure_ascii=False))
        self.assertIsInstance(hierarchy, list)
        hierarchical = set()
        for category in hierarchy:
            self.assertIn("categoryId", category)
            self.assertIn("name", category)
            for incident_type in category.get("types") or []:
                # сервер отдаёт элемент иерархии как {"typeId", "name"}
                # (контракт IncidentTypesHierarchyType подписывает его "id")
                self.assertIn("typeId", incident_type)
                hierarchical.add(incident_type["typeId"])
        self.assertTrue({t.lower() for t in hierarchical} <= {t.lower() for t in types})

    def test_get_incident_parameters_match_types(self):
        types = {item["id"].lower() for item in self.__module.get_incident_types()}
        for parameter in self.__module.get_incident_parameters():
            self.assertIn("incidentTypeId", parameter)
            if parameter["incidentTypeId"]:
                self.assertIn(parameter["incidentTypeId"].lower(), types)

    # ------------------------- статистика ------------------------- #

    def test_statistics(self):
        """Все четыре эндпоинта статистики на `Day` (IncidentStatistics.yaml)."""
        fields = [
            "periodStart",
            "periodEnd",
            "criticalSeverity",
            "highSeverity",
            "mediumSeverity",
            "lowSeverity",
            "totalDuration",
        ]
        severities = {
            "created": self.__module.get_created_incidents_distribution(
                self.__begin, self.__end
            ),
            "closed": self.__module.get_closed_incidents_distribution(
                self.__begin, self.__end
            ),
            "open": self.__module.get_open_incidents_distribution(
                self.__begin, self.__end
            ),
        }

        statuses = self.__module.get_incidents_status_statistics(
            self.__begin, self.__end
        )
        print(json.dumps(statuses[:2], indent=4, ensure_ascii=False))

        self.assertGreater(len(statuses), 0)
        for period in statuses:
            self.assertEqual(
                sorted(period),
                sorted(
                    [
                        "periodStart",
                        "periodEnd",
                        "new",
                        "approved",
                        "inProgress",
                        "resolved",
                        "closed",
                    ]
                ),
            )

        for name, periods in severities.items():
            with self.subTest(statistic=name):
                print(json.dumps(periods[:2], indent=4, ensure_ascii=False))
                self.assertGreater(len(periods), 0)
                for period in periods:
                    self.assertEqual(sorted(period), sorted(fields))

    def test_statistics_granularities(self):
        """`granularity` меняет число периодов; неверное значение - ValueError."""
        periods = {}
        for granularity in ("Day", "Week", "Month", "Quarter"):
            with self.subTest(granularity=granularity):
                periods[granularity] = self.__module.get_created_incidents_distribution(
                    self.__begin, self.__end, granularity
                )
                self.assertGreater(len(periods[granularity]), 0)

        self.assertGreater(len(periods["Day"]), len(periods["Week"]))
        self.assertGreater(len(periods["Week"]), len(periods["Quarter"]))

    def test_statistics_wrong_granularity(self):
        with self.assertRaises(ValueError):
            self.__module.get_incidents_status_statistics(
                self.__begin, self.__end, "Year"
            )

    # ------------------------- read-model / sync / public ------------------------- #

    def test_get_incident_events_by_id_v2(self):
        events = self.__module.get_incident_events_by_id(self.__incident_id())

        print(json.dumps(events[:2], indent=4, ensure_ascii=False))

        self.assertIsInstance(events, list)
        for event in events:
            self.assertEqual(sorted(event), sorted(["id", "timestamp"]))
            self.assertIsNotNone(event.get("id"))

    def test_get_incident_events_by_id_v2_wrong_incident(self):
        with self.assertRaises(requests.RequestException):
            self.__module.get_incident_events_by_id(
                "11111111-1111-1111-1111-111111111111"
            )

    def test_get_deleted_incident_ids_empty(self):
        with self.assertRaises(ValueError):
            self.__module.get_deleted_incident_ids([])

    def test_get_deleted_incident_ids(self):
        deleted = self.__module.get_deleted_incident_ids([self.__incident_id()])

        print(json.dumps(deleted, indent=4, ensure_ascii=False))

        self.assertIsInstance(deleted, list)
        # На R28.0 read-model стенда отвечает «неизвестные» ID как удалённые,
        # поэтому проверяем только вложенность ответа
        self.assertTrue(set(deleted) <= {self.__incident_id()})

    def test_get_sync_incidents_short_info(self):
        sync_incidents = self.__module.get_sync_incidents_short_info()

        self.assertIsInstance(sync_incidents, list)
        for incident in sync_incidents:
            self.assertIsNotNone(incident.get("id"))

    def test_get_incident_version(self):
        version = self.__module.get_incident_version(self.__incident_id())

        print(json.dumps(version, indent=4, ensure_ascii=False))

        self.assertIsInstance(version, int)
        self.assertGreaterEqual(version, 0)

    def test_get_public_incident_id(self):
        public = self.__module.get_public_incident_id(self.__incident_id())

        print(json.dumps(public, indent=4, ensure_ascii=False))

        self.assertEqual(sorted(public), sorted(["id", "publicId"]))
        self.assertEqual(public.get("id"), self.__incident_id())
        self.assertEqual(public.get("publicId"), self.__incident_key())

    def test_get_public_incident_id_unknown(self):
        with self.assertRaises(requests.RequestException):
            self.__module.get_public_incident_id("11111111-1111-1111-1111-111111111111")

    # ------------------------- фильтры: read ------------------------- #

    def test_get_incident_filter(self):
        filter_id = self.__first_hierarchy_filter_id()
        incident_filter = self.__module.get_incident_filter(filter_id)

        print(json.dumps(incident_filter, indent=4, ensure_ascii=False))

        self.assertEqual(
            sorted(incident_filter),
            sorted(["id", "folderId", "name", "select", "where", "orderby"]),
        )
        self.assertEqual(incident_filter.get("id"), filter_id)
        self.assertIsInstance(incident_filter.get("select"), list)
        self.assertIsInstance(incident_filter.get("where"), str)
        for item in incident_filter.get("orderby"):
            self.assertEqual(sorted(item), sorted(["field", "sortOrder"]))
            self.assertIn(item["sortOrder"], ("ascending", "descending"))

    def test_get_incident_filter_public_matches(self):
        filter_id = self.__first_hierarchy_filter_id()

        self.assertEqual(
            self.__module.get_incident_filter_public(filter_id),
            self.__module.get_incident_filter(filter_id),
        )

    def test_get_incident_filters_by_ids(self):
        filter_id = self.__first_hierarchy_filter_id()
        infos = self.__module.get_incident_filters_by_ids(
            [filter_id, "11111111-1111-1111-1111-111111111111"]
        )

        print(json.dumps(infos, indent=4, ensure_ascii=False))

        self.assertEqual(len(infos), 1)  # неизвестный ID сервер пропускает
        self.assertEqual(sorted(infos[0]), sorted(["id", "name", "isRemoved"]))
        self.assertEqual(infos[0]["id"], filter_id)
        self.assertFalse(infos[0]["isRemoved"])

    def test_get_incident_filters_by_ids_empty(self):
        with self.assertRaises(ValueError):
            self.__module.get_incident_filters_by_ids([])

    def test_get_default_incident_filter_public(self):
        default_filter = self.__module.get_default_incident_filter_public()

        print(json.dumps(default_filter, indent=4, ensure_ascii=False))

        self.assertIn("id", default_filter)
        self.assertIn("where", default_filter)
        self.assertEqual(
            default_filter, self.__module.get_incident_filter(default_filter["id"])
        )

    def test_get_default_incident_filter(self):
        try:
            default_filter = self.__module.get_default_incident_filter()
        except requests.RequestException as err:
            # На R28.0 приватный маршрут нестабилен: 500 internal.error
            self.skipTest(f"Private default filter endpoint unavailable: {err}")

        print(json.dumps(default_filter, indent=4, ensure_ascii=False))

        self.assertIn("id", default_filter)
        self.assertIn("where", default_filter)

    def test_validate_incident_filter(self):
        self.assertTrue(
            self.__module.validate_incident_filter(
                {"select": ["key"], "where": 'status != "Closed"', "orderby": []}
            )
        )
        self.assertFalse(
            self.__module.validate_incident_filter(
                {"select": ["key"], "where": "!!!bad!!!", "orderby": []}
            )
        )

    def test_get_incident_filters_hierarchy(self):
        for notifications in (False, True):
            with self.subTest(notifications=notifications):
                tree = self.__module.get_incident_filters_hierarchy(notifications)

                self.assertIn("roots", tree)
                self.assertGreater(len(tree["roots"]), 0)
                nodes = list(self.__walk_hierarchy(tree["roots"]))
                self.assertGreater(len(nodes), 0)
                for node in nodes:
                    self.assertIn(node.get("type"), ("filter", "folder"))
                    self.assertIsNotNone(node.get("id"))
                    self.assertIsNotNone(node.get("name"))
                    self.assertIsInstance(node.get("permissions"), list)

        # Дерево для уведомлений совпадает с обычным в части системных узлов.
        # Полное сравнение невозможно: у пользовательских узлов уведомления
        # добавляют в meta владельца (`user`), а сами пользовательские папки
        # создают/удаляют другие кейсы этого же прогона
        self.assertEqual(
            self.__hierarchy_node_ids(notifications=True),
            self.__hierarchy_node_ids(notifications=False),
        )

    def test_get_incident_filter_folders(self):
        folder = self.__module.get_incident_filter_folder(self.SYSTEM_FILTERS_FOLDER)

        print(json.dumps(folder, indent=4, ensure_ascii=False))

        self.assertEqual(
            sorted(folder),
            sorted(
                [
                    "folderId",
                    "name",
                    "type",
                    "foldersCount",
                    "filtersCount",
                    "userDefinedId",
                    "orderInFolder",
                ]
            ),
        )
        self.assertEqual(folder["folderId"], self.SYSTEM_FILTERS_FOLDER)
        self.assertEqual(folder["type"], "System")
        self.assertGreater(folder["foldersCount"], 0)

        folders = self.__module.get_incident_filter_folders_in_folder(
            self.SYSTEM_FILTERS_FOLDER
        )
        filters = self.__module.get_incident_filters_in_folder(
            self.SYSTEM_FILTERS_FOLDER
        )

        print(json.dumps(folders[:2], indent=4, ensure_ascii=False))

        self.assertEqual(len(folders), folder["foldersCount"])
        self.assertEqual(len(filters), folder["filtersCount"])
        for item in folders:
            self.assertEqual(
                sorted(item),
                sorted(
                    [
                        "folderId",
                        "name",
                        "type",
                        "foldersCount",
                        "filtersCount",
                        "userDefinedId",
                        "orderInFolder",
                    ]
                ),
            )

        # во вложенной папке фильтры действительно лежат
        nested = next((f for f in folders if f["filtersCount"]), None)
        self.assertIsNotNone(nested)
        nested_filters = self.__module.get_incident_filters_in_folder(
            nested["folderId"]
        )
        self.assertEqual(len(nested_filters), nested["filtersCount"])
        for item in nested_filters:
            self.assertEqual(
                sorted(item),
                sorted(
                    [
                        "id",
                        "name",
                        "type",
                        "folderId",
                        "userDefinedId",
                        "isDefault",
                        "orderInFolder",
                    ]
                ),
            )

    def test_get_incident_filter_folder_unknown(self):
        # несуществующая папка на R28.0 - 500 internal.error, а не 404
        with self.assertRaises(requests.RequestException):
            self.__module.get_incident_filter_folder(
                "11111111-1111-1111-1111-111111111111"
            )

    # ------------------------- фильтры: write ------------------------- #

    def test_get_incident_filter_folder(self):
        folder_id = self.__test_filter_folder()
        folder = self.__module.get_incident_filter_folder(folder_id)

        self.assertEqual(folder["folderId"], folder_id)
        self.assertEqual(folder["type"], "User")
        self.assertEqual(folder["name"], self.__filter_folder_name)

    def test_create_incident_filter_folder_in_system_folder(self):
        # запись разрешена только в пользовательскую ветку
        with self.assertRaises(requests.RequestException):
            self.__module.create_incident_filter_folder(
                name=self.__unique_name("folder"),
                parent_id=self.SYSTEM_FILTERS_FOLDER,
            )

    def test_create_incident_filter_in_system_folder(self):
        with self.assertRaises(requests.RequestException):
            self.__module.create_incident_filter(
                folder_id=self.SYSTEM_FILTERS_FOLDER,
                name=self.__unique_name("filter"),
                select=["key"],
                where="",
            )

    def test_update_incident_filter_folder(self):
        folder_id = self.__new_folder()

        new_name = self.__unique_name("folder_renamed")
        self.assertTrue(
            self.__module.update_incident_filter_folder(
                folder_id, new_name, self.USER_FILTERS_FOLDER
            )
        )
        self.assertEqual(
            self.__module.get_incident_filter_folder(folder_id)["name"], new_name
        )

    def test_update_incident_filter_folder_system(self):
        with self.assertRaises(requests.RequestException):
            self.__module.update_incident_filter_folder(
                self.SYSTEM_FILTERS_FOLDER,
                self.__unique_name("folder"),
                self.USER_FILTERS_FOLDER,
            )

    def test_incident_filter_lifecycle(self):
        """Создание -> чтение -> обновление -> чтение (IncidentFilter.yaml)."""
        folder_id = self.__new_folder()
        created = self.__new_filter(folder_id=folder_id)
        filter_id = created["id"]

        read = self.__module.get_incident_filter(filter_id)
        self.assertEqual(read["name"], created["name"])
        self.assertEqual(read["folderId"], folder_id)
        self.assertEqual(read["select"], ["key", "name", "status"])
        self.assertEqual(read["where"], 'status != "Closed"')
        self.assertEqual(
            read["orderby"], [{"field": "created", "sortOrder": "descending"}]
        )

        new_name = self.__unique_name("filter_renamed")
        self.assertTrue(
            self.__module.update_incident_filter(
                filter_id,
                folder_id=folder_id,
                name=new_name,
                select=["key", "status"],
                where='status = "Closed"',
                orderby=[{"field": "created", "sortOrder": "ascending"}],
            )
        )

        updated = self.__module.get_incident_filter(filter_id)
        print(json.dumps(updated, indent=4, ensure_ascii=False))
        self.assertEqual(updated["name"], new_name)
        self.assertEqual(updated["select"], ["key", "status"])
        self.assertEqual(updated["where"], 'status = "Closed"')
        self.assertEqual(
            updated["orderby"], [{"field": "created", "sortOrder": "ascending"}]
        )

        # фильтр виден в папке и по списку ID
        in_folder = self.__module.get_incident_filters_in_folder(folder_id)
        self.assertEqual([item["id"] for item in in_folder], [filter_id])
        by_ids = self.__module.get_incident_filters_by_ids([filter_id])
        self.assertEqual([item["id"] for item in by_ids], [filter_id])

    def test_create_incident_filter_default_orderby(self):
        # без `orderby` сервер подставляет собственный порядок сортировки
        folder_id = self.__new_folder()
        created = self.__new_filter(folder_id=folder_id, orderby=None)

        read = self.__module.get_incident_filter(created["id"])
        self.assertEqual(read["name"], created["name"])
        self.assertTrue(read["orderby"])
        for item in read["orderby"]:
            self.assertIn(item["sortOrder"], ("ascending", "descending"))

    def test_create_incident_filter_bad_where(self):
        # сервер валидирует синтаксис условия
        with self.assertRaises(requests.RequestException):
            self.__module.create_incident_filter(
                folder_id=self.__test_filter_folder(),
                name=self.__unique_name("filter"),
                select=["key"],
                where="!!!bad!!!",
            )

    def test_update_incident_filter_unknown(self):
        with self.assertRaises(requests.RequestException):
            self.__module.update_incident_filter(
                "11111111-1111-1111-1111-111111111111",
                folder_id=self.__test_filter_folder(),
                name=self.__unique_name("filter"),
                select=["key"],
            )

    def test_update_incident_filter_system(self):
        filter_id = self.__first_hierarchy_filter_id()
        with self.assertRaises(requests.RequestException):
            self.__module.update_incident_filter(
                filter_id,
                folder_id="00000000-0000-0000-0000-000000000000",
                name=self.__unique_name("filter"),
                select=["key"],
            )

    def test_delete_incident_filter(self):
        folder_id = self.__new_folder()
        filter_id = self.__new_filter(folder_id=folder_id)["id"]

        self.assertTrue(self.__module.delete_incident_filter(filter_id))
        self.assertEqual(self.__module.get_incident_filters_in_folder(folder_id), [])
        self.__filters.remove(filter_id)

        # повторное удаление не идемпотентно (500 на R28.0)
        with self.assertRaises(requests.RequestException):
            self.__module.delete_incident_filter(filter_id)

    def test_delete_incident_filter_folder_cascades(self):
        """Удаление папки снимает и вложенные папки, и фильтры."""
        folder_id = self.__new_folder()
        subfolder_id = self.__module.create_incident_filter_folder(
            name=self.__unique_name("subfolder"), parent_id=folder_id
        )["id"]
        filter_id = self.__new_filter(folder_id=folder_id, select=["key"], where="")[
            "id"
        ]
        before = self.__hierarchy_node_ids(system_only=False)
        self.assertTrue({folder_id, subfolder_id, filter_id} <= before)

        self.assertTrue(self.__module.delete_incident_filter_folder(folder_id))
        self.__folders.remove(folder_id)
        self.__filters.remove(filter_id)

        # Иерархия - источник истины: каскадно сняты и вложенная папка, и её
        # фильтр. Отдельное чтение фильтра после каскада остаётся 200
        # (read-model устаревает), поэтому на него не опираемся
        after = self.__hierarchy_node_ids(system_only=False)
        self.assertFalse({folder_id, subfolder_id, filter_id} & after)

        with self.assertRaises(requests.RequestException):
            self.__module.get_incident_filter_folder(folder_id)

    def test_delete_incident_filter_folder_unknown(self):
        with self.assertRaises(requests.RequestException):
            self.__module.delete_incident_filter_folder(
                "11111111-1111-1111-1111-111111111111"
            )

    # ------------------------- инциденты: write ------------------------- #

    def test_create_and_delete_incident(self):
        """Полный цикл: создание, правка, меры, статусы, удаление."""
        created = self.__new_incident(description="created by mpsiemlib test")
        incident_id = created["id"]

        # сервер отвечает `{id, key}`, имя в словаре добавлено хелпером
        self.assertEqual(sorted(created), sorted(["id", "key", "name"]))
        self.assertTrue(created["key"].startswith("INC-"))

        short_info = self.__module.get_incident_short_info(incident_id)
        self.assertEqual(short_info["name"], created["name"])
        self.assertEqual(short_info["source"], "user")

        self.assertTrue(
            self.__module.update_incident(
                incident_id,
                name=f"{created['name']}_renamed",
                incident_type=self.TEST_INCIDENT_TYPE,
                severity="Medium",
                detected=self.__end - 86400,
            )
        )
        renamed = self.__module.get_incident_short_info(incident_id)
        self.assertEqual(renamed["name"], f"{created['name']}_renamed")
        self.assertGreater(renamed["version"], short_info["version"])

        self.assertTrue(
            self.__module.change_incident_measures(incident_id, "test measures")
        )
        # пустой список групп контракт допускает
        self.assertTrue(self.__module.update_incident_groups(incident_id, []))

        self.assertEqual(
            sorted(self.__module.export_incidents_by_ids([incident_id])),
            sorted(["formatVersion", "producer", "content", "incidents"]),
        )

        self.assertTrue(self.__module.delete_incidents([incident_id]))
        self.__assert_incident_deleted(incident_id)
        # ID остаётся в реестре cleanup: 204 не гарантирует фактического
        # удаления на R28.0, а повторное удаление сервер принимает молча

        # удаление неизвестного ID сервер принимает молча
        self.assertTrue(
            self.__module.delete_incidents(["11111111-1111-1111-1111-111111111111"])
        )

    def test_delete_incidents_empty(self):
        with self.assertRaises(ValueError):
            self.__module.delete_incidents([])

    def test_create_incident_wrong_type(self):
        with self.assertRaises(requests.RequestException):
            self.__module.create_incident(
                name=self.__unique_name("inc"),
                incident_type="NoSuchIncidentType",
                severity="Low",
                detected=self.__end - 86400,
            )

    def test_create_incident_future_detection(self):
        # Core требует время обнаружения в прошлом
        with self.assertRaises(requests.RequestException):
            self.__module.create_incident(
                name=self.__unique_name("inc"),
                incident_type=self.TEST_INCIDENT_TYPE,
                severity="Low",
                detected=self.__end + 86400,
            )

    def test_create_incident_wrong_source(self):
        with self.assertRaises(requests.RequestException):
            self.__module.create_incident(
                name=self.__unique_name("inc"),
                incident_type=self.TEST_INCIDENT_TYPE,
                severity="Low",
                detected=self.__end - 86400,
                source="NotAValue",
            )

    def test_create_incident_wrong_severity(self):
        with self.assertRaises(requests.RequestException):
            self.__module.create_incident(
                name=self.__unique_name("inc"),
                incident_type=self.TEST_INCIDENT_TYPE,
                severity="Bogus",
                detected=self.__end - 86400,
            )

    def test_update_incident_requires_full_body(self):
        # контракт допускает частичное обновление, но сервер отвечает 500 без
        # name/type/severity/detected, поэтому метод требует их все
        with self.assertRaises(TypeError):
            self.__module.update_incident(self.__new_incident()["id"], name="only_name")

    def test_change_incident_status(self):
        incident_id = self.__new_incident()["id"]

        # допустимая цепочка переходов (замерено на R28.0)
        for status in ("Approved", "InProgress", "Resolved", "Closed"):
            with self.subTest(status=status):
                self.assertTrue(
                    self.__module.change_incident_status(
                        incident_id, status, f"test -> {status}"
                    )
                )

        # обратный переход из Closed сервер не валидирует заранее - 500
        with self.assertRaises(requests.RequestException):
            self.__module.change_incident_status(incident_id, "InProgress", "back")

    def test_change_incident_status_unknown(self):
        incident_id = self.__new_incident()["id"]
        with self.assertRaises(requests.RequestException):
            self.__module.change_incident_status(incident_id, "NotAStatus", "x")

    def test_incident_tasks_lifecycle(self):
        """Задача инцидента: создание, чтение, read-modify-write правка."""
        incident_id = self.__new_incident()["id"]

        task = self.__module.add_incident_task(
            incident_id, name="test task", task_type="Investigation"
        )
        task_id = task["id"]

        print(json.dumps(task, indent=4, ensure_ascii=False))
        self.assertEqual(
            sorted(task), sorted(["id", "status", "name", "assigned", "estimatedTime"])
        )
        self.assertEqual(task["status"], "New")
        self.assertEqual(task["name"], "test task")

        read_task = self.__module.get_incident_task_by_id(task_id)
        self.assertEqual(read_task["taskId"], task_id)
        self.assertEqual(read_task["name"], "test task")
        self.assertEqual(read_task["type"], "Investigation")
        self.assertEqual(read_task["innerIncident"]["incidentId"], incident_id)

        # частичное обновление: сервер требует полное тело, поэтому модуль
        # читает задачу и перезаписывает только переданные поля
        self.assertTrue(
            self.__module.update_incident_task(task_id, name="renamed task")
        )
        after_rename = self.__module.get_incident_task_by_id(task_id)
        self.assertEqual(after_rename["name"], "renamed task")
        self.assertEqual(after_rename["type"], "Investigation")
        self.assertEqual(after_rename["status"], "New")

        self.assertTrue(
            self.__module.update_incident_task(task_id, status="InProgress")
        )
        after_status = self.__module.get_incident_task_by_id(task_id)
        self.assertEqual(after_status["status"], "InProgress")
        self.assertEqual(after_status["name"], "renamed task")

        self.assertTrue(
            self.__module.update_incident_task(
                task_id, description="written", estimated_time=self.__end + 3600
            )
        )
        after_all = self.__module.get_incident_task_by_id(task_id)
        self.assertEqual(after_all["description"], "written")
        self.assertIsNotNone(after_all["estimatedTime"])

        # задача видна в общем списке read-model
        issues = self.__module.get_incident_issues()
        self.assertIn(task_id, [issue["id"] for issue in issues])

        issue = self.__module.get_incident_issue_by_id(task_id)
        self.assertEqual(issue["id"], task_id)
        self.assertEqual(self.__module.get_incident_issues_count(incident_id), 1)

        tasks = self.__module.get_incident_tasks([incident_id])
        self.assertEqual([t["id"] for t in tasks], [task_id])
        self.assertEqual(tasks[0]["incident_id"], incident_id)

    def test_get_incident_task_by_id_unknown(self):
        # неизвестный ID - 200 с пустым телом, модуль поднимает ValueError
        with self.assertRaises(ValueError):
            self.__module.get_incident_task_by_id(
                "11111111-1111-1111-1111-111111111111"
            )

    def test_add_incident_task_missing_required(self):
        # контракт требует только incidentId, но без name/type сервер отвечает 500
        incident_id = self.__new_incident()["id"]
        with self.assertRaises(requests.RequestException):
            self.__module.add_incident_task(
                incident_id, name="", task_type="Investigation"
            )

    def test_link_and_unlink_incident_events(self):
        source = self.__incident_with_events()
        if source is None:
            self.skipTest("No incidents with events on the stand")
        source_events = self.__module.get_incident_events_by_id(source["id"])

        created = self.__new_incident()
        event = source_events[0]

        self.assertTrue(self.__module.link_incident_events(created["key"], [event]))
        linked = self.__module.get_incident_events_by_id(created["id"])
        self.assertEqual([e["id"] for e in linked], [event["id"]])

        self.assertTrue(self.__module.unlink_incident_event(created["id"], event["id"]))
        self.assertEqual(self.__module.get_incident_events_by_id(created["id"]), [])

        # отвязка несвязанного события не идемпотентна
        with self.assertRaises(requests.RequestException):
            self.__module.unlink_incident_event(created["id"], event["id"])

    def test_link_incident_events_wrong_key(self):
        with self.assertRaises(requests.RequestException):
            self.__module.link_incident_events(
                "INC-999999999", [{"id": self.__incident_id(), "timestamp": "now"}]
            )

    def test_unlink_incident_asset_wrong_type(self):
        incident_id = self.__incident_id()
        with self.assertRaises(ValueError):
            self.__module.unlink_incident_asset(incident_id, "Bogus", "Asset", "x")
        with self.assertRaises(ValueError):
            self.__module.unlink_incident_asset(incident_id, "Targets", "Bogus", "x")

    def test_unlink_incident_asset_unknown_element(self):
        # неизвестный элемент - 500 internal.error
        incident_id = self.__new_incident()["id"]
        with self.assertRaises(requests.RequestException):
            self.__module.unlink_incident_asset(
                incident_id, "Targets", "Other", "no-such-element"
            )

    # ------------------------- экспорт / удаление по фильтру ------------------------- #

    def test_export_incidents_by_filter(self):
        export = self.__module.export_incidents_by_filter(
            self.__begin,
            {
                "select": ["key", "name"],
                "where": f'key="{self.__incident_key()}"',
                "orderby": [],
            },
            time_to=self.__end,
        )

        print(json.dumps(export, indent=4, ensure_ascii=False)[:400])

        self.assertEqual(
            sorted(export),
            sorted(["formatVersion", "producer", "content", "incidents"]),
        )
        self.assertEqual(export["content"], ["incidents"])
        self.assertEqual(len(export["incidents"]), 1)
        self.assertEqual(
            export["incidents"][0]["identification"]["id"], self.__incident_key()
        )

    def test_export_incidents_by_ids_unknown(self):
        export = self.__module.export_incidents_by_ids(
            ["11111111-1111-1111-1111-111111111111"]
        )

        self.assertEqual(export["incidents"], [])

    def test_delete_incidents_by_filter_no_match(self):
        # пустая выборка на R28.0 - 400, а не «успешно удалено 0»
        with self.assertRaises(requests.RequestException):
            self.__module.delete_incidents_by_filter(
                self.__begin,
                {"select": ["key"], "where": 'key="INC-999999999"', "orderby": []},
                time_to=self.__end,
            )

    def test_delete_incidents_by_filter(self):
        incident = self.__new_incident()
        # окно обязано покрывать момент создания инцидента: self.__end - это
        # время старта прогона, инцидент создан позже него; если выборка пуста,
        # сервер отвечает 400, а инцидент остаётся живым
        self.assertTrue(
            self.__module.delete_incidents_by_filter(
                round(time.time()) - 600,
                {
                    "select": ["key", "name"],
                    "where": f'name="{incident["name"]}"',
                    "orderby": [{"field": "created", "sortOrder": "descending"}],
                },
                time_to=round(time.time()) + 60,
            )
        )

        self.__assert_incident_deleted(incident["id"])
        # ID остаётся в реестре cleanup: 204 не гарантирует фактического удаления


if __name__ == "__main__":
    unittest.main()
