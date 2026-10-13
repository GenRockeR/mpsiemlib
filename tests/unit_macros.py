import contextlib
import unittest
from uuid import UUID

import requests
from helpers import gen_lowercase_string
from settings import creds, settings

from mpsiemlib.common import *
from mpsiemlib.modules import MPSIEMWorker


class MacrosTestCase(unittest.TestCase):
    """Тесты модуля Macros.

    Читающие операции проверяются на реальном контенте БД, CRUD - на
    временных объектах с префиксом ``unittest_`` (удаляются в finally).

    RB-контракты ``CoreApi.SiemMacros*.yaml`` (``/api/SiemMacros*``) на стенде
    28.x не маршрутизируются (404), рабочий интерфейс - KB Studio API
    ``/api-studio/siem/macros*`` (порт 8091). Измеренное поведение, которое
    кодируют тесты:

    - изменяющие запросы защищены ``SiemMacros.HashMismatch`` - публичные
      методы сами перечитывают модель и перегенерируют хэш через ``validate``;
    - удаление физическое: ``GET`` после ``DELETE`` - ``404`` (``None``);
    - локалей у макроса должно оставаться минимум одна;
    - метка после ``DELETE`` отдаёт ``409`` (а не ``404``) - тоже ``None``.
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
        # Только по полному списку: серверный search в /list нестрогой, по
        # нему легко задеть посторонние объекты.
        macros: list[dict] = []
        try:
            macros = cls.__module.get_macros_list(do_refresh=True)
        except (requests.RequestException, ValueError) as err:
            print(f"macros cleanup skipped: {err!r}")
        for macro in macros:
            if str(macro.get("name") or "").startswith("unittest_"):
                with contextlib.suppress(Exception):
                    cls.__module.delete_macro(macro["id"])
        cls.__module.close()
        cls.__kb_module.close()

    @classmethod
    def __pick_db_name(cls):
        for name, params in cls.__kb_module.get_databases_list().items():
            if params.get("deployable"):
                return name
        raise unittest.SkipTest("No deployable content database on stand")

    # ---------------- helpers ---------------- #

    def __any_macro(self):
        macros = self.__module.get_macros_list(do_refresh=True)
        if not macros:
            self.skipTest("No macros in db")
        return macros[0]

    def __create_macro(self, with_arg: bool = False):
        name = gen_lowercase_string(12)
        signature = "(string $p)" if with_arg else "()"
        text = (
            f"filter {name}{signature} {{\n"
            f"\t{'action == $p' if with_arg else 'action == "access"'}\n}}"
        )
        macro_id = self.__module.create_macro(
            system_name=name,
            text=text,
            name=name,
            description="unittest",
        )
        self.__is_uuid(macro_id)
        return name, text, macro_id

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

    # ---------------- чтение: kb-ui список ---------------- #

    def test_get_macros_list(self):
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

    # ---------------- чтение: KB Studio API ---------------- #

    def test_get_macro(self):
        macro = self.__any_macro()
        info = self.__module.get_macro(macro["id"])
        self.assertIsNotNone(info)
        self.assertEqual(info["id"], macro["id"])
        self.assertIsNotNone(info["system_name"])
        self.assertIsNotNone(info["text"])
        # имя отдаётся в локали заголовка Content-Locale (RUS)
        self.assertEqual(info["name"], macro["name"])

    def test_get_macro_not_found(self):
        self.assertIsNone(
            self.__module.get_macro("00000000-0000-0000-0000-000000000000")
        )

    def test_get_macro_by_system_name(self):
        macro = self.__any_macro()
        system_name = self.__module.get_macro(macro["id"])["system_name"]
        info = self.__module.get_macro_by_system_name(system_name)
        self.assertIsNotNone(info)
        self.assertEqual(info["system_name"], system_name)

    def test_get_macro_by_system_name_not_found(self):
        self.assertIsNone(
            self.__module.get_macro_by_system_name("unittest_no_such_macro")
        )

    def test_get_macros_tags_list(self):
        tags = self.__module.get_macros_tags_list()
        self.assertGreater(len(tags), 0)
        for key in ("id", "parent_id", "name", "is_group"):
            self.assertIn(key, tags[0])

    # ---------------- валидация и проверка имени ---------------- #

    def test_validate_macro_text_ok(self):
        validation = self.__module.validate_macro_text(
            'filter unittest_valid() {\n\taction == "access"\n}'
        )
        self.assertEqual(validation["errors"], [])
        self.assertIsNotNone(validation["result_hash"])
        self.assertTrue(validation["sdk_version"])
        self.assertEqual(validation["system_name"], "unittest_valid")

    def test_validate_macro_text_syntax_error(self):
        validation = self.__module.validate_macro_text("filter unittest_bad( {")
        self.assertGreater(len(validation["errors"]), 0)

    def test_check_macro_system_name(self):
        name, _, macro_id = self.__create_macro()
        try:
            self.assertEqual(self.__module.check_macro_system_name(name), macro_id)
            self.assertIsNone(
                self.__module.check_macro_system_name("unittest_free_system_name_zz")
            )
        finally:
            self.__delete_macro_quietly(macro_id)

    # ---------------- CRUD макросов ---------------- #

    def test_create_update_delete_macro(self):
        name, text, macro_id = self.__create_macro()
        try:
            info = self.__module.get_macro(macro_id)
            self.assertEqual(info["system_name"], name)
            self.assertEqual(info["name"], name)

            # макрос виден в kb-ui списке
            listed_names = [
                m["name"] for m in self.__module.get_macros_list(do_refresh=True)
            ]
            self.assertIn(name, listed_names)

            new_text = text.replace("access", "block")
            self.__module.update_macro(macro_id, new_text)
            info = self.__module.get_macro(macro_id)
            self.assertIn("block", info["text"])
            # частичное изменение: имя и описание сохранились
            self.assertEqual(info["name"], name)
        finally:
            self.__delete_macro_quietly(macro_id)

        # удаление физическое: GET после DELETE - 404 -> None
        self.assertIsNone(self.__module.get_macro(macro_id))
        self.assertNotIn(
            name, [m["name"] for m in self.__module.get_macros_list(do_refresh=True)]
        )

    def test_create_macro_invalid_text_raises(self):
        with self.assertRaises(ValueError):
            self.__module.create_macro(
                system_name="unittest_broken_zz",
                text="filter unittest_broken( {",
            )

    def test_update_macro_invalid_text_raises(self):
        _, _, macro_id = self.__create_macro()
        try:
            with self.assertRaises(ValueError):
                self.__module.update_macro(macro_id, "filter broken( {")
            # объект не изменён
            self.assertIn("access", self.__module.get_macro(macro_id)["text"])
        finally:
            self.__delete_macro_quietly(macro_id)

    def test_delete_missing_macro_raises(self):
        with self.assertRaises(requests.HTTPError) as ctx:
            self.__module.delete_macro("00000000-0000-0000-0000-000000000000")
        self.assertEqual(ctx.exception.response.status_code, 404)

    def test_rename_macro(self):
        name, _, macro_id = self.__create_macro()
        new_name = name + "_renamed"
        try:
            new_text = self.__module.rename_macro(macro_id, new_name)
            info = self.__module.get_macro(macro_id)
            # rename переписывает имя и в поле, и в тексте фильтра
            self.assertEqual(info["system_name"], new_name)
            self.assertIn(new_name, info["text"])
            self.assertIn(new_name, new_text)
            self.assertEqual(
                self.__module.get_macro_by_system_name(new_name)["id"], macro_id
            )
        finally:
            self.__delete_macro_quietly(macro_id)

    def test_clone_macro(self):
        name, _, macro_id = self.__create_macro()
        clone_name = name + "_clone"
        clone_id = None
        try:
            clone_id = self.__module.clone_macro(macro_id, clone_name)
            self.__is_uuid(clone_id)
            self.assertNotEqual(clone_id, macro_id)
            clone = self.__module.get_macro(clone_id)
            self.assertEqual(clone["system_name"], clone_name)
            self.assertEqual(clone["text"], self.__module.get_macro(macro_id)["text"])
        finally:
            if clone_id is not None:
                self.__delete_macro_quietly(clone_id)
            self.__delete_macro_quietly(macro_id)

    # ---------------- параметры макроса ---------------- #

    def test_macro_params(self):
        _, _, macro_id = self.__create_macro(with_arg=True)
        try:
            self.__module.add_macro_param(
                macro_id, "p2", "string", default_value="x", description="описание p2"
            )
            params = self.__module.get_macro(macro_id)["params"]
            self.assertIn("p2", [p["Name"] for p in params])

            self.__module.remove_macro_param(macro_id, "p2")
            params = self.__module.get_macro(macro_id)["params"]
            self.assertNotIn("p2", [p["Name"] for p in params])
            # существующий параметр не задет
            self.assertIn("p", [p["Name"] for p in params])
        finally:
            self.__delete_macro_quietly(macro_id)

    def test_add_macro_param_duplicate_raises(self):
        _, _, macro_id = self.__create_macro(with_arg=True)
        try:
            with self.assertRaises(ValueError):
                self.__module.add_macro_param(macro_id, "p", "string")
        finally:
            self.__delete_macro_quietly(macro_id)

    def test_remove_macro_param_missing_raises(self):
        _, _, macro_id = self.__create_macro()
        try:
            with self.assertRaises(ValueError):
                self.__module.remove_macro_param(macro_id, "no_such_param")
        finally:
            self.__delete_macro_quietly(macro_id)

    # ---------------- локали макроса ---------------- #

    def test_macro_locales(self):
        name, _, macro_id = self.__create_macro()
        try:
            self.__module.add_macro_locale(macro_id, "ENG", name + "_en", "desc en")
            edit = self.__module.get_macro_edit_model(macro_id)
            self.assertEqual(set(edit["Name"]), {"RUS", "ENG"})

            self.__module.change_macro_locale(
                macro_id, "ENG", name + "_en2", "desc en2"
            )
            edit = self.__module.get_macro_edit_model(macro_id)
            self.assertEqual(edit["Name"]["ENG"], name + "_en2")
            self.assertEqual(edit["Description"]["ENG"], "desc en2")

            self.__module.remove_macro_locale(macro_id, "ENG")
            edit = self.__module.get_macro_edit_model(macro_id)
            self.assertEqual(set(edit["Name"]), {"RUS"})
        finally:
            self.__delete_macro_quietly(macro_id)

    def test_add_macro_locale_duplicate_raises(self):
        _, _, macro_id = self.__create_macro()
        try:
            with self.assertRaises(ValueError):
                self.__module.add_macro_locale(macro_id, "RUS", "dup", "dup")
        finally:
            self.__delete_macro_quietly(macro_id)

    def test_remove_last_macro_locale_raises(self):
        _, _, macro_id = self.__create_macro()
        try:
            with self.assertRaises(ValueError):
                self.__module.remove_macro_locale(macro_id, "RUS")
            # локаль не потеряна
            edit = self.__module.get_macro_edit_model(macro_id)
            self.assertEqual(set(edit["Name"]), {"RUS"})
        finally:
            self.__delete_macro_quietly(macro_id)

    # ---------------- метки ---------------- #

    def test_macro_tag_lifecycle(self):
        tag_name = gen_lowercase_string(12)
        tag_id = self.__module.create_macro_tag(tag_name)
        self.__is_uuid(tag_id)
        macro_id = None
        try:
            tag = self.__module.get_macro_tag(tag_id)
            self.assertEqual(tag["id"], tag_id)
            self.assertEqual(tag["name"], tag_name)
            self.assertFalse(tag["is_group"])

            _, _, macro_id = self.__create_macro()
            self.__module.add_macro_tag(macro_id, tag_id)
            self.assertIn(tag_id, self.__module.get_macro(macro_id)["tag_ids"])

            self.__module.remove_macro_tag(macro_id, tag_id)
            self.assertNotIn(tag_id, self.__module.get_macro(macro_id)["tag_ids"])

            self.__module.rename_macro_tag(tag_id, tag_name + "_upd")
            self.assertEqual(
                self.__module.get_macro_tag(tag_id)["name"], tag_name + "_upd"
            )
        finally:
            if macro_id is not None:
                self.__delete_macro_quietly(macro_id)
            self.__delete_tag_quietly(tag_id)

        # после удаления метка отдаёт 409 (не 404) - тоже None
        self.assertIsNone(self.__module.get_macro_tag(tag_id))

    def test_macro_tag_group_parent(self):
        group_id = self.__module.create_macro_tag(
            gen_lowercase_string(12), is_group=True
        )
        child_id = None
        try:
            child_id = self.__module.create_macro_tag(
                gen_lowercase_string(12), parent_id=group_id
            )
            child = self.__module.get_macro_tag(child_id)
            self.assertEqual(child["parent_id"], group_id)

            self.__module.rename_macro_tag(child_id, child["name"] + "_upd")
            self.assertEqual(
                self.__module.get_macro_tag(child_id)["parent_id"], group_id
            )
        finally:
            if child_id is not None:
                self.__delete_tag_quietly(child_id)
            self.__delete_tag_quietly(group_id)

    def test_get_macro_tag_not_found(self):
        self.assertIsNone(
            self.__module.get_macro_tag("00000000-0000-0000-0000-000000000000")
        )

    def test_delete_macro_tag_unlinks_macros(self):
        # измерено на стенде: удаление метки снимает и её привязки
        tag_id = self.__module.create_macro_tag(gen_lowercase_string(12))
        macro_id = None
        try:
            _, _, macro_id = self.__create_macro()
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
