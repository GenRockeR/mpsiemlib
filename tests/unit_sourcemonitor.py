import pytz
import unittest

from datetime import datetime

from mpsiemlib.common import *
from mpsiemlib.modules import MPSIEMWorker

from tests.settings import creds_ldap, settings


class SourceMonitorTestCase(unittest.TestCase):
    __mpsiemworker = None
    __module = None
    __creds_ldap = creds_ldap
    __settings = settings
    __begin = None
    __end = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.__mpsiemworker = MPSIEMWorker(cls.__creds_ldap, cls.__settings)
        cls.__module = cls.__mpsiemworker.get_module(ModuleNames.SOURCE_MONITOR)
        cls.__end = round(datetime.now(tz=pytz.timezone(settings.local_timezone)).timestamp())
        cls.__begin = cls.__end - 86400

    @classmethod
    def tearDownClass(cls) -> None:
        cls.__module.close()

    def test_get_sources_list(self):
        ret = []
        for i in self.__module.get_sources_list(self.__begin, self.__end):
            ret.append(i)
        self.assertTrue(len(ret) != 0)

    def test_get_forwarders_list(self):
        ret = []
        for i in self.__module.get_forwarders_list(self.__begin, self.__end):
            ret.append(i)
        self.assertTrue(len(ret) != 0)

    def test_get_sources_by_forwarder(self):
        forwarder = next(self.__module.get_forwarders_list(self.__begin, self.__end))
        ret = []
        for i in self.__module.get_sources_by_forwarder(forwarder.get("id"), self.__begin, self.__end):
            ret.append(i)
        self.assertTrue(len(ret) != 0)


if __name__ == '__main__':
    unittest.main()
