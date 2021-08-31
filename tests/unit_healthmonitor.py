import unittest

from mpsiemlib.common import *
from mpsiemlib.modules import MPSIEMWorker

from tests.settings import creds_ldap, settings


class KBTestCase(unittest.TestCase):
    __mpsiemworker = None
    __module = None
    __creds_ldap = creds_ldap
    __settings = settings

    @classmethod
    def setUpClass(cls) -> None:
        cls.__mpsiemworker = MPSIEMWorker(cls.__creds_ldap, cls.__settings)
        cls.__module = cls.__mpsiemworker.get_module(ModuleNames.HEALTH)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.__module.close()

    def test_get_global_status(self):
        ret = self.__module.get_health_status()
        self.assertTrue(type(ret) == str)

    def test_get_errors(self):
        ret = self.__module.get_health_errors()
        self.assertTrue(type(ret) == list)

    def test_get_license_status(self):
        ret = self.__module.get_health_license_status()
        self.assertTrue(len(ret) != 0)

    def test_get_agents_status(self):
        ret = self.__module.get_health_agents_status()
        self.assertTrue(len(ret) != 0)

    def test_get_kb_status(self):
        ret = self.__module.get_health_kb_status()
        self.assertTrue(len(ret) != 0)


if __name__ == '__main__':
    unittest.main()
