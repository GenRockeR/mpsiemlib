import unittest

import requests
from settings import creds_pat, settings

from mpsiemlib.common import *
from mpsiemlib.modules import MPSIEMWorker


class WorkerTestCase(unittest.TestCase):
    __creds = None
    __settings = None

    def setUp(self) -> None:
        self.__creds = creds_pat
        self.__settings = settings
        self.__mpsiemworker = MPSIEMWorker(self.__creds, self.__settings)

    def test_MPSIEMWorker_init(self):
        self.assertIsInstance(self.__mpsiemworker, WorkerInterface)

    @unittest.skip("Storage is Logspace, not Elasticsearch (no ES impl yet)")
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
    __creds = None
    __settings = None

    def setUp(self) -> None:
        self.__creds = creds_pat
        self.__settings = settings

    @unittest.skip("Skip test, when PAT auth")
    def test_MPSIEMAuth_connect_core_local(self):
        mpsiemworker = MPSIEMWorker(self.__creds, self.__settings)
        module = mpsiemworker.get_module(ModuleNames.AUTH)
        session = module.connect(MPComponents.CORE)
        self.assertIsInstance(session, requests.Session)
        session.close()

    @unittest.skip("Skip test, when Local auth")
    def test_MPSIEMAuth_connect_core_ldap(self):
        mpsiemworker = MPSIEMWorker(self.__creds, self.__settings)
        module = mpsiemworker.get_module(ModuleNames.AUTH)
        session = module.connect(MPComponents.CORE)
        self.assertIsInstance(session, requests.Session)
        session.close()

    def test_MPSIEMAuth_connect_core_pat(self):
        mpsiemworker = MPSIEMWorker(self.__creds, self.__settings)
        module = mpsiemworker.get_module(ModuleNames.AUTH)
        session = module.connect(MPComponents.CORE)
        self.assertIsInstance(session, requests.Session)
        session.close()

    def test_MPSIEMAuth_get_core_version(self):
        mpsiemworker = MPSIEMWorker(self.__creds, self.__settings)
        module = mpsiemworker.get_module(ModuleNames.AUTH)
        version = int(module.get_core_version().split(".")[0])
        self.assertGreaterEqual(version, 27)

    @unittest.skip("Storage is Logspace, not Elasticsearch (no ES impl yet)")
    def test_MPSIEMAuth_get_storage_version(self):
        mpsiemworker = MPSIEMWorker(self.__creds, self.__settings)
        module = mpsiemworker.get_module(ModuleNames.AUTH)
        version = int(module.get_storage_version().split(".")[0])
        self.assertGreaterEqual(version, 7)


if __name__ == "__main__":
    unittest.main()
