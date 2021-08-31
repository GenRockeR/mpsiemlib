import unittest

import requests
from mpsiemlib.common import *
from mpsiemlib.modules import MPSIEMWorker

from tests.settings import creds_ldap, creds_local, settings


class WorkerTestCase(unittest.TestCase):
    __mpsiemworker = None
    __creds_ldap = None
    __creds_local = None
    __settings = None
    
    def setUp(self) -> None:
        self.__creds_ldap = creds_ldap
        self.__creds_local = creds_local
        self.__settings = settings
        self.__mpsiemworker = MPSIEMWorker(self.__creds_ldap, self.__settings)

    def test_MPSIEMWorker_init(self):
        self.assertIsInstance(self.__mpsiemworker, WorkerInterface)

    def test_MPSIEMWorker_get_module_events(self):
        module = self.__mpsiemworker.get_module(ModuleNames.EVENTS)
        self.assertIsInstance(module, ModuleInterface)

    def test_MPSIEMWorker_get_module_table(self):
        module = self.__mpsiemworker.get_module(ModuleNames.TABLES)
        self.assertIsInstance(module, ModuleInterface)

    def test_MPSIEMWorker_get_module_auth(self):
        module = self.__mpsiemworker.get_module(ModuleNames.AUTH)
        self.assertIsInstance(module, AuthInterface)


class ModuleTestCase(unittest.TestCase):
    __creds_ldap = None
    __creds_local = None
    __settings = None

    def setUp(self) -> None:
        self.__creds_ldap = creds_ldap
        self.__creds_local = creds_local
        self.__settings = settings

    def test_MPSIEMAuth_connect_core_local(self):
        mpsiemworker = MPSIEMWorker(self.__creds_local, self.__settings)
        module = mpsiemworker.get_module(ModuleNames.AUTH)
        session = module.connect(MPComponents.CORE)
        self.assertIsInstance(session, requests.Session)
        session.close()

    def test_MPSIEMAuth_connect_core_ldap(self):
        mpsiemworker = MPSIEMWorker(self.__creds_ldap, self.__settings)
        module = mpsiemworker.get_module(ModuleNames.AUTH)
        session = module.connect(MPComponents.CORE)
        self.assertIsInstance(session, requests.Session)
        session.close()

    def test_MPSIEMAuth_get_core_version(self):
        mpsiemworker = MPSIEMWorker(self.__creds_ldap, self.__settings)
        module = mpsiemworker.get_module(ModuleNames.AUTH)
        version = module.get_core_version()
        self.assertTrue(version.startswith("2"))

    def test_MPSIEMAuth_get_storage_version(self):
        mpsiemworker = MPSIEMWorker(self.__creds_ldap, self.__settings)
        module = mpsiemworker.get_module(ModuleNames.AUTH)
        version = module.get_storage_version()
        self.assertTrue(version.startswith("7"))


if __name__ == '__main__':
    unittest.main()
