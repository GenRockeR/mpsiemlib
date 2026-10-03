import contextlib
import unittest
from uuid import UUID

from helpers import gen_lowercase_string
from settings import creds, settings

from mpsiemlib.common import *
from mpsiemlib.modules import MPSIEMWorker


class MacrosTestCase(unittest.TestCase):
    """Тесты модуля Macros.

    Читающие операции проверяются на реальном контенте БД, CRUD - на
    временных объектах с префиксом ``unittest_`` (удаляются в finally).
    Поведение эндпоинтов сверено с измерениями на стенде R26+:
    GET коллекции контракта сломан, удаление мягкое (GET после DELETE -
    пустое тело), локаль удаляется по коду, PUT тела частичный.
    """

    __mpsiemworker = None
    __module = None
    __kb_module = None
    __creds = creds
    __settings = settings
    __db_name = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.__mpsiemworker = MPSIEMWorker(cls.__creds, cls.__settings)
        cls.__module = cls.__mpsiemworker.get_module(ModuleNames.MACROS)
        cls.__kb_module = cls.__mpsiemworker.get_module(ModuleNames.KB)
        cls.__db_name = cls.__pick_db_name()
        cls.__module.set_db_name(db_name=cls.__db_name)

    @classmethod
    def tearDownClass(cls) -> None:
        # Страховка: CRUD-тесты удаляют макросы/метки в finally, но падение до
        # захвата id оставляет мусор с префиксом unittest_ - подчищаем.
        try:
            for macro in cls.__module.get_macros_list(do_refresh=True):
                if str(macro.get("name") or "").startswith("unittest_"):
                    with contextlib.suppress(Exception):
                        cls.__module.delete_macro(macro["id"])
        except Exception as err:
            print(f"macros cleanup errors: {err!r}")
        cls.__module.close()
        cls.__kb_module.close()

    @classmethod
    def __pick_db_name(cls):
        for name, params in cls.__kb_module.get_databases_list().items():
            if params.get("deployable"):
                return name
        raise unittest.SkipTest("No deployable content database on stand")

    @property
    def __db(self):
        return self.__db_name

    # ---------------- helpers ---------------- #

    def __any_macro(self):
        macros = self.__module.get_macros_list(do_refresh=True)
        if not macros:
            self.skipTest("No macros in db")
        return macros[0]

    def __create_macro(self):
        name = gen_lowercase_string(12)
        macro_id = self.__module.create_macro(
            system_name=name,
            text="rule unittest: Event\nemit {\n\t$id = 'unittest'\n}",
            locales=[{"Locale": "RUS", "Name": name, "Description": "unittest"}],
        )
        self.__is_uuid(macro_id)
        return name, macro_id

    def __delete_macro_quietly(self, macro_id):
        # удаление - best-effort cleanup (объект мог быть уже удалён в тесте)
        with contextlib.suppress(Exception):
            self.__module.delete_macro(macro_id)

    def __delete_tag_quietly(self, tag_id):
        with contextlib.suppress(Exception):
            self.__module.delete_macro_tag(tag_id)

    def __is_uuid(self, value):
        try:
            UUID(str(value))
        except ValueError as err:
            self.fail(f"{value!r} is not a UUID: {err!r}")

    # ---------------- kb-ui: список и чтение ---------------- #

    def test_get_custom_macros_list(self):
        macros = self.__module.get_macros_list(do_refresh=True)
        self.assertGreater(len(macros), 0)
        for key in ("id", "name", "object_id"):
            self.assertIn(key, macros[0])

    def test_get_macros_list_cached(self):
        first = self.__module.get_macros_list()
        second = self.__module.get_macros_list()
        self.assertEqual(first, second)
        # копия, а не сам кэш
        first.append({"id": None})
        self.assertNotIn({"id": None}, self.__module.get_macros_list())

    def test_get_macros_info(self):
        macro = self.__any_macro()
        info = self.__module.get_macros_info(macro["id"])
        self.assertEqual(info["id"], macro["id"])
        self.assertEqual(info["name"], macro["name"])
        self.assertTrue(info["text"])

    def test_get_macros_by_name(self):
        macro = self.__any_macro()
        found = self.__module.get_macros_by_name(macro["name"])
        self.assertIsNotNone(found)
        self.assertEqual(found["id"], macro["id"])
        self.assertIsNone(self.__module.get_macros_by_name("unittest_no_such_macro"))

    def test_get_macros_by_object_id(self):
        macro = self.__any_macro()
        found = self.__module.get_macros_by_object_id(macro["object_id"])
        self.assertIsNotNone(found)
        self.assertEqual(found["id"], macro["id"])
        self.assertIsNone(self.__module.get_macros_by_object_id("unittest-no-such-id"))

    def test_get_macros_id_by_filter_name(self):
        macro = self.__any_macro()
        object_id = self.__module.get_macros_id_by_filter_name(macro["name"])
        self.assertEqual(object_id, macro["object_id"])
        self.assertIsNone(
            self.__module.get_macros_id_by_filter_name("unittest_no_such_macro")
        )

    def test_unpack_macros(self):
        loc_macro = None
        for macro in self.__module.get_macros_list():
            if str(macro.get("object_id", "")).startswith("LOC-RF"):
                loc_macro = macro
                break
        if loc_macro is None:
            self.skipTest("No LOC-RF macros in db")

        filters = self.__module.unpack_macros(loc_macro["object_id"])
        self.assertIsInstance(filters, list)
        self.assertEqual(len(filters), len(set(filters)))
        for name in filters:
            self.assertTrue(name)

    def test_unpack_macros_unknown_object_id(self):
        with self.assertRaises(ValueError):
            self.__module.unpack_macros("unittest-no-such-id")

    # ---------------- контракт: чтение ---------------- #

    def test_get_macro(self):
        macro = self.__any_macro()
        info = self.__module.get_macro(macro["id"])
        self.assertIsNotNone(info)
        self.assertEqual(info["id"], macro["id"])
        self.assertIsNotNone(info["system_name"])
        self.assertIsNotNone(info["text"])
        # kb-ui name (локаль RUS) совпадает с локализованным именем контракта
        rus = [loc for loc in info["locales"] if loc.get("Locale") == "RUS"]
        if rus:
            self.assertEqual(rus[0].get("Name"), macro["name"])

    def test_get_macros_tags_list(self):
        tags = self.__module.get_macros_tags_list()
        self.assertGreater(len(tags), 0)
        for key in ("id", "parent_id", "is_group", "locales"):
            self.assertIn(key, tags[0])

    # ---------------- контракт: CRUD макросов ---------------- #

    def test_create_update_delete_macro(self):
        name, macro_id = self.__create_macro()
        try:
            info = self.__module.get_macro(macro_id)
            self.assertEqual(info["system_name"], name)
            self.assertEqual(len(info["locales"]), 1)

            # макрос виден в kb-ui списке
            listed_names = [
                m["name"] for m in self.__module.get_macros_list(do_refresh=True)
            ]
            self.assertIn(name, listed_names)

            new_text = "rule unittest_updated: Event\nemit {\n\t$id = 'upd'\n}"
            self.__module.update_macro(macro_id, name, new_text)
            info = self.__module.get_macro(macro_id)
            self.assertEqual(info["text"], new_text)
            # PUT частичный: локали сохранились
            self.assertEqual(len(info["locales"]), 1)
        finally:
            self.__delete_macro_quietly(macro_id)

        # удаление мягкое: GET после DELETE - пустое тело
        self.assertIsNone(self.__module.get_macro(macro_id))
        self.assertNotIn(
            name, [m["name"] for m in self.__module.get_macros_list(do_refresh=True)]
        )

    def test_macro_params(self):
        _, macro_id = self.__create_macro()
        param_id = None
        try:
            param_id = self.__module.add_macro_param(
                macro_id,
                name="unittest_param",
                param_type="string",
                default_value="1",
                index=0,
                locales=[{"Locale": "RUS", "Description": "unittest"}],
            )
            self.__is_uuid(param_id)

            info = self.__module.get_macro(macro_id)
            self.assertEqual(len(info["params"]), 1)
            self.assertEqual(info["params"][0]["Name"], "unittest_param")

            self.__module.remove_macro_param(macro_id, param_id)
            param_id = None
            info = self.__module.get_macro(macro_id)
            self.assertEqual(info["params"], [])
        finally:
            if param_id is not None:
                with contextlib.suppress(Exception):
                    self.__module.remove_macro_param(macro_id, param_id)
            self.__delete_macro_quietly(macro_id)

    def test_macro_locales(self):
        name, macro_id = self.__create_macro()
        try:
            locale_id = self.__module.add_macro_locale(
                macro_id, "ENG", name, "unittest eng"
            )
            self.__is_uuid(locale_id)
            locales = self.__module.get_macro(macro_id)["locales"]
            self.assertEqual({loc["Locale"] for loc in locales}, {"RUS", "ENG"})

            self.__module.change_macro_locale(macro_id, "ENG", name + "_upd", "upd")
            eng = [
                loc
                for loc in self.__module.get_macro(macro_id)["locales"]
                if loc["Locale"] == "ENG"
            ]
            self.assertEqual(eng[0]["Name"], name + "_upd")

            # 26.0 RemoveLocaleFromSiemMacros дефектен: 204, но локаль
            # остаётся с обнулённым Name (измерено). С asserting факта удаления
            # воздерживаемся - проверяем только ответ.
            response = self.__module.remove_macro_locale(macro_id, "ENG")
            self.assertEqual(response.status_code, 204)
            eng = [
                loc
                for loc in self.__module.get_macro(macro_id)["locales"]
                if loc["Locale"] == "ENG"
            ]
            if eng:
                # дефект не исправлен: локаль осталась, обнулено имя
                self.assertIsNone(eng[0]["Name"])

        finally:
            self.__delete_macro_quietly(macro_id)

    def test_add_macro_locale_requires_description(self):
        name, macro_id = self.__create_macro()
        try:
            with self.assertRaises(ValueError):
                self.__module.add_macro_locale(macro_id, "ENG", name, "  ")
        finally:
            self.__delete_macro_quietly(macro_id)

    # ---------------- контракт: метки ---------------- #

    def test_macro_tag_lifecycle(self):
        tag_name = gen_lowercase_string(12)
        tag_id = self.__module.create_macro_tag(tag_name)
        self.__is_uuid(tag_id)
        macro_id = None
        try:
            tag = self.__module.get_macro_tag(tag_id)
            self.assertIsNotNone(tag)
            self.assertEqual(tag["id"], tag_id)
            self.assertFalse(tag["is_group"])

            loc_id = self.__module.add_macro_tag_locale(tag_id, "ENG", tag_name)
            self.__is_uuid(loc_id)
            self.__module.change_macro_tag_locale(tag_id, "ENG", tag_name + "_upd")
            eng = [
                loc
                for loc in self.__module.get_macro_tag(tag_id)["locales"]
                if loc["Locale"] == "ENG"
            ]
            self.assertEqual(eng[0]["Name"], tag_name + "_upd")
            self.__module.remove_macro_tag_locale(tag_id, "ENG")
            locales = self.__module.get_macro_tag(tag_id)["locales"]
            self.assertEqual({loc["Locale"] for loc in locales}, {"RUS"})

            # привязка/отвязка метки макросу
            _, macro_id = self.__create_macro()
            link_id = self.__module.add_macro_tag(macro_id, tag_id)
            self.__is_uuid(link_id)
            self.assertIn(tag_id, self.__module.get_macro(macro_id)["tag_ids"])

            self.__module.remove_macro_tag(macro_id, tag_id)
            self.assertNotIn(tag_id, self.__module.get_macro(macro_id)["tag_ids"])
        finally:
            if macro_id is not None:
                self.__delete_macro_quietly(macro_id)
            self.__delete_tag_quietly(tag_id)

        self.assertIsNone(self.__module.get_macro_tag(tag_id))

    def test_delete_macro_tag_unlinks_macros(self):
        # измерено на стенде: RemoveSiemMacrosTag снимает и привязки
        tag_id = self.__module.create_macro_tag(gen_lowercase_string(12))
        macro_id = None
        try:
            _, macro_id = self.__create_macro()
            self.__module.add_macro_tag(macro_id, tag_id)
            self.assertIn(tag_id, self.__module.get_macro(macro_id)["tag_ids"])

            self.__module.delete_macro_tag(tag_id)
            self.assertNotIn(tag_id, self.__module.get_macro(macro_id)["tag_ids"])
        finally:
            if macro_id is not None:
                self.__delete_macro_quietly(macro_id)
            self.__delete_tag_quietly(tag_id)


if __name__ == "__main__":
    unittest.main()
