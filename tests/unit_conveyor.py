import unittest

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


if __name__ == "__main__":
    unittest.main()
