import os
import re
import unittest
from functools import partial
from typing import ClassVar

import requests
from settings import creds_pat, settings

from mpsiemlib.common import *
from mpsiemlib.modules import MPSIEMWorker


class TablesTestCase(unittest.TestCase):
    """Тесты модуля Tables.

    Читающие тесты самодостаточны: список для чтения выбирается из
    установленных на стенде (первый непустой по алфавиту), а не tables[0]
    (первый по алфавиту список на стенде пуст - тест падал без причины).

    Пишущие тесты (import/set_table_row/truncate) работают со списком из
    MP_TEST_TABLE (по умолчанию test_tl_r272): такой список создаётся в KB
    вручную и устанавливается на конвейер - создание списков лежит в домене
    KB, не модуля Tables. Нет подходящего списка - тесты скипаются.

    Whitelist-тесты скипаются, если на стенде недоступен сервис
    /api/whitelists (на R28.0 отвечает 404 для всех token).
    """

    __mpsiemworker = None
    __module = None
    __tables: ClassVar["dict[str, dict]"] = {}
    __creds = creds_pat
    __settings = settings

    __test_table_name = os.getenv("MP_TEST_TABLE", "test_tl_r272")
    # Типы списков, строки которых module разрешает менять построчно
    __ROW_EDITABLE_TYPES = ("correlationrule", "enrichmentrule")
    # Имена whitelists-списков в контенте PTKB (для порядка перебора canEdit)
    __WL_NAME_RE = re.compile(r"(_WL$|_BL$|[Ww]hitelist|[Bb]lacklist)")
    __WL_PROBE_LIMIT = 40

    @classmethod
    def setUpClass(cls) -> None:
        cls.__mpsiemworker = MPSIEMWorker(cls.__creds, cls.__settings)
        cls.__module = cls.__mpsiemworker.get_module(ModuleNames.TABLES)
        cls.__tables = cls.__module.get_tables_list()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.__module.close()

    def __pick_data_table(self) -> str:
        """Первый установленный список с >3 строками (по алфавиту).

        Первый по алфавиту список на стенде пуст, поэтому выбираем список,
        в котором строки точно есть (`where _id>3` отдает хотя бы одну).
        """
        probe = {
            "select": ["_last_changed"],
            "where": "_id>3",
            "orderBy": [{"field": "_last_changed", "sortOrder": "descending"}],
            "timeZone": 0,
        }
        for name in sorted(self.__tables):
            if next(iter(self.__module.get_table_data(name, probe)), None) is not None:
                return name
        self.skipTest("No table list with data found on stand")

    def __require_test_table(self, types: tuple[str, ...] | None = None) -> str:
        """Имя тестового списка для пишущих тестов или skip."""
        entry = self.__tables.get(self.__test_table_name)
        if entry is None:
            self.skipTest(
                f"Table list {self.__test_table_name!r} not installed on stand; "
                "create it in KB (editable registry or rule-sourced) or set "
                "MP_TEST_TABLE"
            )
        if types is not None and entry.get("type") not in types:
            self.skipTest(
                f"Table list {self.__test_table_name!r} has type "
                f"{entry.get('type')!r}, expected one of {types}"
            )
        if not entry.get("editable"):
            self.skipTest(f"Table list {self.__test_table_name!r} is not editable")
        return self.__test_table_name

    def test_get_table_list(self):
        ret = self.__module.get_tables_list()
        self.assertGreater(len(ret), 0)
        entry = next(iter(ret.values()))
        for key in ("id", "type", "editable", "ttl_enabled", "notifications"):
            self.assertIn(key, entry)

    def test_get_table_id_by_name(self):
        name = min(self.__tables)
        table_id = self.__module.get_table_id_by_name(name)
        self.assertEqual(table_id, self.__tables[name]["id"])
        self.assertEqual(self.__module.get_table_name_by_id(table_id), name)

    def test_get_table_id_by_name_not_found(self):
        with self.assertRaises(ValueError):
            self.__module.get_table_id_by_name("no_such_table_list")
        with self.assertRaises(ValueError):
            self.__module.get_table_name_by_id("no_such_token")

    def test_get_table_data_simple(self):
        key = self.__pick_data_table()
        ret = list(self.__module.get_table_data(key))
        self.assertGreater(len(ret), 0)
        self.assertIn("_id", ret[0])

    def test_get_table_data_filtered(self):
        filters = {
            "select": ["_last_changed"],
            "where": "_id>2",
            "orderBy": [{"field": "_last_changed", "sortOrder": "descending"}],
            "timeZone": 0,
        }
        key = self.__pick_data_table()
        ret = list(self.__module.get_table_data(key, filters))
        self.assertGreater(len(ret), 0)
        for row in ret:
            self.assertGreater(int(row.get("_id")), 2)
            # только _id и _last_changed
            self.assertEqual(sorted(row), ["_id", "_last_changed"])

    def test_get_table_info(self):
        key = min(self.__tables)
        table_info = self.__module.get_table_info(key)

        lookup_fields = [
            "id",
            "type",
            "editable",
            "size_max",
            "size_typical",
            "size_current",
            "ttl",
            "ttl_enabled",
            "description",
            "created",
            "updated",
            "fields",
            "notifications",
        ]
        has_all_fields = len(set(table_info).intersection(lookup_fields)) == len(
            lookup_fields
        )

        is_valid_struct = True
        is_asset_table = False
        for k, v in table_info.items():
            if k == "type" and v in ["assetgrid", "registry"]:
                is_asset_table = True
            if (
                k in ["notifications", "size_max", "size_typical", "ttl", "ttl_enabled"]
                and is_asset_table
            ):
                continue
            if v is None and k != "notifications":
                is_valid_struct = False
                break

        self.assertTrue(has_all_fields)
        self.assertTrue(is_valid_struct)

    def test_get_table_rows_count(self):
        key = self.__pick_data_table()
        rows = list(self.__module.get_table_data(key))
        filters = {"select": ["_id"], "where": "", "timeZone": 0}
        self.assertEqual(self.__module.get_table_rows_count(key, filters), len(rows))
        # Без фильтра - то же число
        self.assertEqual(self.__module.get_table_rows_count(key), len(rows))
        # На R28.0 сервер count игнорирует where (замерено - всегда полный
        # размер списка, независимо от условия). Фиксируем фактическое
        # поведение, а не контракт
        for where in ("_id>0", "_id>1000000"):
            self.assertEqual(
                self.__module.get_table_rows_count(
                    key, {"select": ["_id"], "where": where, "timeZone": 0}
                ),
                len(rows),
            )

    def test_get_table_rows_count_wrong_name(self):
        with self.assertRaises(requests.HTTPError):
            self.__module.get_table_rows_count("no_such_table_list")

    def test_export_table(self):
        key = self.__pick_data_table()
        rows = list(self.__module.get_table_data(key))
        declared = [
            f.get("name") for f in (self.__module.get_table_info(key)["fields"] or [])
        ]

        # Экспорт всех полей: без select сервер отдаёт объявленные поля списка,
        # кроме системного `_id` (в заголовке его нет - замерено на R28.0)
        csv_text = self.__module.export_table(key)
        lines = [line for line in csv_text.lstrip("\ufeff").splitlines() if line]
        self.assertEqual(
            [column.strip('"') for column in lines[0].split(";")],
            [name for name in declared if name != "_id"],
        )
        self.assertEqual(len(lines) - 1, len(rows))

        # limit + выборка полей
        limited = self.__module.export_table(
            key,
            filters={"select": ["_id"], "where": "", "timeZone": 0},
            limit=3,
        )
        limited_lines = [line for line in limited.lstrip("\ufeff").splitlines() if line]
        self.assertEqual(limited_lines[0], '"_id"')
        self.assertLessEqual(len(limited_lines) - 1, 3)

        # where применяется к экспорту (в отличие от count)
        min_id = min(int(row["_id"]) for row in rows)
        where_one = self.__module.export_table(
            key,
            filters={"select": ["_id"], "where": f"_id>{min_id}", "timeZone": 0},
        )
        where_lines = [line for line in where_one.lstrip("\ufeff").splitlines() if line]
        self.assertEqual(len(where_lines) - 1, len(rows) - 1)

    def test_export_table_bad_field(self):
        key = min(self.__tables)
        with self.assertRaises(requests.HTTPError):
            self.__module.export_table(
                key, filters={"select": ["no_such_field"], "where": "", "timeZone": 0}
            )

    def test_export_table_by_ids(self):
        key = self.__pick_data_table()
        rows = list(self.__module.get_table_data(key))
        row_ids = [str(row["_id"]) for row in rows[:3]]

        csv_text = self.__module.export_table_by_ids(key, row_ids)
        lines = [line for line in csv_text.lstrip("\ufeff").splitlines() if line]
        self.assertGreater(len(lines), 1)
        self.assertEqual(len(lines) - 1, len(row_ids))

        # Несуществующие id молча пропускаются - остаётся только заголовок
        csv_empty = self.__module.export_table_by_ids(key, ["999999999"])
        lines_empty = [line for line in csv_empty.lstrip("\ufeff").splitlines() if line]
        self.assertEqual(len(lines_empty), 1)

    def test_table_session(self):
        key = min(self.__tables)
        # На стенде тесты идут параллельно с человеком: перед стартом
        # убеждаемся, что сессии нет (если открыта - закрываем)
        self.__ensure_session_closed(key)
        self.addCleanup(self.__ensure_session_closed, key)

        self.assertEqual(self.__module.get_table_session(key).get("result"), "none")

        self.__module.open_table_session(key, stop_activity=False)
        session = self.__module.get_table_session(key)
        self.assertEqual(session.get("result"), "active")
        self.assertIsNotNone(session.get("created"))
        self.assertFalse(session.get("stopActivity"))

        self.__module.close_table_session(key)
        self.assertEqual(self.__module.get_table_session(key).get("result"), "none")

    def __ensure_session_closed(self, table_name: str) -> None:
        """Закрыть сессию, если она активна (идемпотентно: повторный close
        на Core отвечает 400)."""
        if self.__module.get_table_session(table_name).get("result") == "active":
            self.__module.close_table_session(table_name)

    def test_set_table_row_unsupported_type(self):
        unsupported = [
            name
            for name, entry in self.__tables.items()
            if entry.get("type") not in self.__ROW_EDITABLE_TYPES
        ]
        if not unsupported:
            self.skipTest("No rule-sourced table lists installed on stand")
        with self.assertRaises(ValueError):
            self.__module.set_table_row(
                min(unsupported), add_rows=[{"whatever": "value"}]
            )

    def test_set_table_row_noop(self):
        key = self.__require_test_table()
        # add_rows=remove_rows=None - только лог, без запроса к Core
        self.__module.set_table_row(key)

    def test_set_table_data(self):
        key = self.__require_test_table()
        self.__module.truncate_table(key)

        example = '"_last_changed";"cust";"user";"session_stat"\r\n"07.02.2023 10:08:17";"customer1";"user1";"07.02.2023 10:08:07"'
        self.__module.set_table_data(key, example)

        ret = list(self.__module.get_table_data(key))
        self.assertGreater(len(ret), 0)
        self.assertIn("_id", ret[0])

    def test_set_table_row(self):
        key = self.__require_test_table(types=self.__ROW_EDITABLE_TYPES)
        add = [
            {
                "cust": "unittest_c1",
                "user": "unittest_u1",
                "session_stat": "12.12.2020 15:23:23",
            },
            {
                "cust": "unittest_c2",
                "user": "unittest_u2",
                "session_stat": "12.12.2020 15:23:23",
            },
        ]
        self.__module.set_table_row(key, add_rows=add, remove_rows=None)
        self.addCleanup(partial(self.__module.set_table_row, key, None, add))
        is_added = any(
            i.get("cust") == "unittest_c1" and i.get("user") == "unittest_u1"
            for i in self.__module.get_table_data(key)
        )
        self.assertTrue(is_added)

        self.__module.set_table_row(key, add_rows=None, remove_rows=add)
        is_removed = not any(
            i.get("cust") == "unittest_c1" and i.get("user") == "unittest_u1"
            for i in self.__module.get_table_data(key)
        )
        self.assertTrue(is_removed)

    def test_whitelist_rows_exists(self):
        table = self.__pick_whitelist_table()
        columns = [
            f.get("name")
            for f in (self.__module.get_table_info(table).get("fields") or [])
        ]
        rows = [["*"] * len(columns)]
        ret = self.__module.whitelist_rows_exists(table_name=table, rows=rows)
        self.assertEqual(len(ret), len(rows))

    def __pick_whitelist_table(self) -> str:
        """Первый список, который Core считает редактируемым белым списком.

        Перебираем `canEdit` по именам-кандидатам (_WL/_BL/Whitelist/...),
        затем прочие списки; не больше `__WL_PROBE_LIMIT` запросов.
        """
        candidates = sorted(
            self.__tables,
            key=lambda table_name: (
                0 if self.__WL_NAME_RE.search(table_name) else 1,
                table_name,
            ),
        )[: self.__WL_PROBE_LIMIT]
        for name in candidates:
            if self.__module.can_edit_whitelist(name):
                return name
        self.skipTest(
            "No editable whitelist found among "
            f"{len(candidates)} table lists probed: /api/whitelists/{{tokenId}} "
            "unresponsive (on R28.0 answers 404 for any token)"
        )


if __name__ == "__main__":
    unittest.main()
