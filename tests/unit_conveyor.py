import unittest

import requests
from settings import creds_pat, settings

from mpsiemlib.common import *
from mpsiemlib.modules import MPSIEMWorker


class ConveyorTestCase(unittest.TestCase):
    __mpsiemworker = None
    __module = None
    __creds = creds_pat
    __settings = settings

    @classmethod
    def setUpClass(cls) -> None:
        cls.__mpsiemworker = MPSIEMWorker(cls.__creds, cls.__settings)
        cls.__module = cls.__mpsiemworker.get_module(ModuleNames.CONVEYOR)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.__module.close()

    def test_get_conveyor_list(self):
        ret = self.__module.get_conveyor_list()
        self.assertGreater(len(ret), 0)
        for conveyor in ret:
            self.assertIsNotNone(conveyor.get("id"))
            self.assertIsNotNone(conveyor.get("alias"))
            self.assertIsInstance(conveyor.get("is_primary"), bool)

    def test_get_conveyor_list_includes_storage_fields(self):
        """Список отдаёт и поля подключения хранилища (SiemFullInfo).

        Вызывающему важны db_type/db_host/db_port: на стендах storage -
        Logspace, а не Elasticsearch, и выбирать запросы нужно по этому полю.
        """
        conveyor = self.__module.get_primary_conveyor()
        for field in (
            "uri_scheme",
            "port",
            "db_type",
            "db_host",
            "db_port",
            "db_cross_port",
            "seed_host",
            "taxonomy_version",
        ):
            self.assertIn(field, conveyor)

        siem_id = conveyor.get("id")
        full = self.__module.get_conveyor_info(siem_id)
        self.assertEqual(conveyor.get("db_type"), full.get("dbType"))
        self.assertEqual(conveyor.get("db_host"), full.get("dbHost"))
        self.assertEqual(conveyor.get("db_port"), full.get("dbPort"))
        self.assertEqual(conveyor.get("db_cross_port"), full.get("dbCrossPort"))
        self.assertEqual(conveyor.get("uri_scheme"), full.get("uriScheme"))
        self.assertEqual(conveyor.get("port"), full.get("port"))
        self.assertEqual(conveyor.get("seed_host"), full.get("seedHost"))

    def test_get_conveyor_count(self):
        self.assertEqual(
            self.__module.get_conveyor_count(),
            len(self.__module.get_conveyor_list()),
        )

    def test_get_primary_conveyor(self):
        primary = self.__module.get_primary_conveyor()
        self.assertTrue(primary.get("is_primary"))
        self.assertEqual(self.__module.get_primary_conveyor_id(), primary.get("id"))

    def test_get_conveyor_id_by_alias(self):
        primary = self.__module.get_primary_conveyor()
        alias = primary.get("alias")
        self.assertEqual(
            self.__module.get_conveyor_id_by_alias(alias), primary.get("id")
        )

    def test_get_conveyor_id_by_alias_not_found(self):
        with self.assertRaises(ValueError):
            self.__module.get_conveyor_id_by_alias("no_such_conveyor_alias")

    def test_get_conveyor_info(self):
        siem_id = self.__module.get_primary_conveyor_id()
        info = self.__module.get_conveyor_info(siem_id)
        self.assertEqual(info.get("id"), siem_id)
        self.assertIsNotNone(info.get("alias"))

    def test_set_default_conveyor(self):
        siem_id = self.__module.get_primary_conveyor_id()
        try:
            self.assertEqual(self.__module.set_default_conveyor(siem_id), siem_id)
            self.assertEqual(self.__module.get_default_conveyor_id(), siem_id)
            # явный siem_id имеет приоритет
            self.assertEqual(self.__module.resolve_siem_id(siem_id), siem_id)
            self.assertEqual(self.__module.resolve_siem_id(), siem_id)
        finally:
            self.__module.clear_default_conveyor()
        self.assertIsNone(self.__module.get_default_conveyor_id())

    def test_set_default_conveyor_by_id_not_found(self):
        with self.assertRaises(ValueError):
            self.__module.set_default_conveyor("00000000-0000-0000-0000-000000000000")

    def test_resolve_siem_id(self):
        # без default: None при одном конвейере, ValueError при нескольких
        resolved = None
        try:
            resolved = self.__module.resolve_siem_id()
        except ValueError as err:
            self.assertGreater(self.__module.get_conveyor_count(), 1)
            self.assertIn("siem_id is required", str(err))
        if resolved is None and self.__module.get_conveyor_count() == 1:
            pass  # единственный конвейер - параметр не нужен

    def test_get_conveyor_by_alias(self):
        primary = self.__module.get_primary_conveyor()
        found = self.__module.get_conveyor_by_alias(primary.get("alias"))
        self.assertEqual(found.get("id"), primary.get("id"))

    def test_get_conveyor_by_alias_not_found(self):
        with self.assertRaises(ValueError):
            self.__module.get_conveyor_by_alias("no_such_conveyor_alias")

    def test_get_conveyor_info_not_found(self):
        # Контракт: 400 SiemNotFoundError; Core SiemManager на R28.0 отвечает
        # 400 SM.SiemNotFound.Error - проверяем HTTPError в целом
        with self.assertRaises(requests.HTTPError):
            self.__module.get_conveyor_info("ffffffff-0000-0000-0000-000000000000")

    def test_get_conveyor_list_cache(self):
        first = self.__module.get_conveyor_list()
        again = self.__module.get_conveyor_list()
        self.assertEqual(first, again)
        # возвращаемые словари - копии: правка результата не должна
        # отравлять кэш
        if first:
            first[0]["alias"] = "unittest_poison"
            self.assertNotEqual(
                self.__module.get_conveyor_list(),
                first,
            )
        refreshed = self.__module.get_conveyor_list(refresh=True)
        self.assertEqual(len(refreshed), self.__module.get_conveyor_count())

    # ------------------------------------------------------------------ #
    # Write-эндпоинты (UpdateSiem / UpdateSiemInfo / UpdateSiemAgents /
    # MakeSiemPrimary). Операции идемпотентны: отправляем текущие значения
    # основного конвейера, стенд не меняется. UpdateSiem и UpdateSiemAgents
    # на R28.0 отвечают 403 и под PAT, и под Local Administrator - кейсы
    # скипаются
    # ------------------------------------------------------------------ #

    _SIEM_INFO_FIELDS = (
        "type",
        "address",
        "uriScheme",
        "port",
        "ipAddresses",
        "osType",
        "version",
        "taxonomyVersion",
        "agents",
        "dbHost",
        "dbPort",
        "dbCrossPort",
        "dbType",
    )

    def __siem_info_from_full(self, full_info: dict) -> dict:
        return {
            field: full_info[field]
            for field in self._SIEM_INFO_FIELDS
            if field in full_info
        }

    def __skip_if_forbidden(self, err: requests.HTTPError, action: str) -> None:
        if getattr(err.response, "status_code", None) == 403:
            self.skipTest(f"{action}: not permitted on stand (403)")
        raise err

    def test_register_conveyor(self):
        siem_id = self.__module.get_primary_conveyor_id()
        before = self.__module.get_conveyor_info(siem_id)
        siem_info = self.__siem_info_from_full(before)
        try:
            self.__module.register_conveyor(siem_id, siem_info)
        except requests.HTTPError as err:
            self.__skip_if_forbidden(err, "register_conveyor")
        after = self.__module.get_conveyor_info(siem_id)
        for field in self._SIEM_INFO_FIELDS:
            self.assertEqual(after.get(field), before.get(field), field)

    def test_update_conveyor_info(self):
        siem_id = self.__module.get_primary_conveyor_id()
        alias = self.__module.get_conveyor_info(siem_id).get("alias")
        try:
            self.__module.update_conveyor_info(siem_id, alias)
        except requests.HTTPError as err:
            self.__skip_if_forbidden(err, "update_conveyor_info")
        self.assertEqual(self.__module.get_conveyor_info(siem_id).get("alias"), alias)

    def test_update_conveyor_agents(self):
        siem_id = self.__module.get_primary_conveyor_id()
        agents = self.__module.get_conveyor_info(siem_id).get("agents") or []
        try:
            self.__module.update_conveyor_agents(siem_id, list(agents))
        except requests.HTTPError as err:
            self.__skip_if_forbidden(err, "update_conveyor_agents")
        self.assertEqual(
            sorted(self.__module.get_conveyor_info(siem_id).get("agents") or []),
            sorted(agents),
        )

    def test_make_conveyor_primary(self):
        # Назначаем основным уже основной конвейер - операция идемпотентна
        siem_id = self.__module.get_primary_conveyor_id()
        try:
            self.__module.make_conveyor_primary(siem_id)
        except requests.HTTPError as err:
            self.__skip_if_forbidden(err, "make_conveyor_primary")
        self.assertEqual(self.__module.get_primary_conveyor_id(), siem_id)
        self.__module.get_conveyor_list(refresh=True)

    def test_delete_conveyor_not_found(self):
        # Реальный конвейер не удаляем; проверяем, что эндпоинт отвечает
        # ошибкой на несуществующий id (контракт - 400 SiemNotFoundError,
        # Core SiemManager на R28.0 отвечает 400 SM.SiemNotFound.Error)
        with self.assertRaises(requests.HTTPError):
            self.__module.delete_conveyor("ffffffff-0000-0000-0000-000000000000")


if __name__ == "__main__":
    unittest.main()
