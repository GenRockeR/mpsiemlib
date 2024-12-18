import unittest

from mpsiemlib.common import *
from mpsiemlib.modules import MPSIEMWorker
from tests.settings import creds, settings


class FiltersTestCase(unittest.TestCase):
    __mpsiemworker = None
    __module = None
    __creds_ldap = creds
    __settings = settings

    @classmethod
    def setUpClass(cls) -> None:
        cls.__mpsiemworker = MPSIEMWorker(cls.__creds_ldap, cls.__settings)
        cls.__module = cls.__mpsiemworker.get_module(ModuleNames.FILTERS)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.__module.close()

    def test_get_folders_list(self):
        folders = self.__module.get_folders_list()

        self.assertGreater(len(folders), 0)

    def test_get_filters_list(self):
        filters = self.__module.get_filters_list()

        self.assertGreater(len(filters), 0)

    def test_get_filter_info(self):
        filter_id = next(iter(self.__module.get_filters_list()))
        filter_info = self.__module.get_filter_info(filter_id)

        self.assertGreater(len(filter_info), 0)


if __name__ == '__main__':
    unittest.main()
