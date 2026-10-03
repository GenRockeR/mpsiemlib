import unittest
from datetime import datetime, timezone

from settings import creds_pat, settings

from mpsiemlib.common import *
from mpsiemlib.modules import MPSIEMWorker


class SourceMonitorTestCase(unittest.TestCase):
    __mpsiemworker = None
    __module = None
    __creds = creds_pat
    __settings = settings
    __begin = None
    __end = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.__mpsiemworker = MPSIEMWorker(cls.__creds, cls.__settings)
        cls.__module = cls.__mpsiemworker.get_module(ModuleNames.SOURCE_MONITOR)
        cls.__end = round(datetime.now(tz=timezone.utc).timestamp())
        cls.__begin = cls.__end - 86400

    @classmethod
    def tearDownClass(cls) -> None:
        cls.__module.close()

    def test_get_sources_list(self):
        ret = list(self.__module.get_sources_list(self.__begin, self.__end))
        self.assertNotEqual(len(ret), 0)

    @unittest.skip("Skip test, if no forwarders")
    def test_get_forwarders_list(self):
        ret = list(self.__module.get_forwarders_list(self.__begin, self.__end))
        self.assertNotEqual(len(ret), 0)

    @unittest.skip("Skip test, if no forwarders")
    def test_get_sources_by_forwarder(self):
        # 27.x: форвардер - актив, его id (asset_id) подаётся в forwarderIds
        forwarder = next(self.__module.get_forwarders_list(self.__begin, self.__end))
        forwarder_id = forwarder.get("asset_id") or forwarder.get("id")
        ret = list(
            self.__module.get_sources_by_forwarder(
                forwarder_id, self.__begin, self.__end
            )
        )
        self.assertNotEqual(len(ret), 0)

    def test_get_forwarders_list_invalid_state_filter(self):
        with self.assertRaises(ValueError):
            list(
                self.__module.get_forwarders_list(
                    self.__begin, self.__end, state_filter="no_such_filter"
                )
            )


if __name__ == "__main__":
    unittest.main()
