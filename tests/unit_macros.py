import unittest

from mpsiemlib.common import *
from mpsiemlib.modules import MPSIEMWorker
from tests.settings import creds, settings


class MacrosTestCase(unittest.TestCase):
    __mpsiemworker = None
    __module = None
    __creds = creds
    __settings = settings

    @classmethod
    def setUpClass(cls) -> None:
        cls.__mpsiemworker = MPSIEMWorker(cls.__creds, cls.__settings)
        cls.__module = cls.__mpsiemworker.get_module(ModuleNames.MACROS)
        cls.__module.set_db_name(db_name='DEV')

    def test_get_custom_macros_list(self):
        macros = self.__module.get_macros_list()
        self.assertTrue(len(macros) != 0)


if __name__ == '__main__':
    unittest.main()
