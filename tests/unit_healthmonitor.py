import json
import unittest

from mpsiemlib.common import *
from mpsiemlib.modules import MPSIEMWorker
from settings import settings, creds_pat


class KBTestCase(unittest.TestCase):
    __mpsiemworker = None
    __module = None
    __creds = creds_pat
    __settings = settings

    @classmethod
    def setUpClass(cls) -> None:
        cls.__mpsiemworker = MPSIEMWorker(cls.__creds, cls.__settings)
        cls.__module = cls.__mpsiemworker.get_module(ModuleNames.HEALTH)    # noqa

    @classmethod
    def tearDownClass(cls) -> None:
        cls.__module.close()

    def test_get_global_status(self):
        ret = self.__module.get_health_status()

        print(json.dumps(ret, indent=4, ensure_ascii=False))

        self.assertIsInstance(ret, str)

    def test_get_errors(self):
        ret = self.__module.get_health_errors()

        print(json.dumps(ret, indent=4, ensure_ascii=False))

        self.assertIsInstance(ret, list)

    def test_get_license_status(self):
        ret = self.__module.get_health_license_status()

        print(json.dumps(ret, indent=4, ensure_ascii=False))

        self.assertGreater(len(ret), 0)

    def test_get_agents_status(self):
        ret = self.__module.get_health_agents_status()

        print(json.dumps(ret, indent=4, ensure_ascii=False))

        self.assertGreater(len(ret), 0)

    @unittest.skip('Not implemented')
    def test_get_kb_status(self):
        ret = self.__module.get_health_kb_status()

        print(json.dumps(ret, indent=4, ensure_ascii=False))

        self.assertGreater(len(ret), 0)


if __name__ == '__main__':
    unittest.main()
