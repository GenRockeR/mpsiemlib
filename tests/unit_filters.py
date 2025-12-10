import json
import unittest

from mpsiemlib.common import *
from mpsiemlib.modules import MPSIEMWorker
from settings import settings, creds_pat


class FiltersTestCase(unittest.TestCase):
    __mpsiemworker = None
    __module = None
    __creds = creds_pat
    __settings = settings

    @classmethod
    def setUpClass(cls) -> None:
        cls.__mpsiemworker = MPSIEMWorker(cls.__creds, cls.__settings)
        cls.__module = cls.__mpsiemworker.get_module(ModuleNames.FILTERS)   # noqa

    @classmethod
    def tearDownClass(cls) -> None:
        cls.__module.close()

    def test_get_folders_list(self):
        folders = self.__module.get_folders_list()

        print(json.dumps(folders, indent=4, ensure_ascii=False))

        self.assertGreater(len(folders), 0)

    def test_get_filters_list(self):
        filters = self.__module.get_filters_list()

        print(json.dumps(filters, indent=4, ensure_ascii=False))

        self.assertGreater(len(filters), 0)

    def test_get_filter_info(self):
        filter_id = next(iter(self.__module.get_filters_list()))
        filter_info = self.__module.get_filter_info(filter_id)

        print(json.dumps(filter_info, indent=4, ensure_ascii=False))

        self.assertGreater(len(filter_info), 0)


if __name__ == '__main__':
    unittest.main()
