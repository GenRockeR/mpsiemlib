import unittest
from datetime import datetime

import pytz

from mpsiemlib.common import *
from mpsiemlib.modules import MPSIEMWorker
from tests.settings import creds_local, settings


class TestEventsAPITestCase(unittest.TestCase):
    __mpsiemworker = None
    __module = None
    __creds = creds_local
    __settings = settings
    __begin = 0
    __end = 0

    @classmethod
    def setUpClass(cls) -> None:
        cls.__mpsiemworker = MPSIEMWorker(cls.__creds, cls.__settings)
        cls.__module = cls.__mpsiemworker.get_module(ModuleNames.EVENTSAPI)
        cls.__end = round(datetime.now(tz=pytz.timezone(settings.local_timezone)).timestamp())
        cls.__begin = cls.__end - 86400

    @classmethod
    def tearDownClass(cls) -> None:
        cls.__module.close()
        
    def test_get_events_grouped_by_fields(self):
        grouping_fields = ['id']
        result = self.__module.get_events_grouped_by_fields(
            query_filter='normalized = true',
            group_by_fields=grouping_fields,
            time_from=self.__begin, 
            time_to=self.__end
        )
        self.assertGreater(len(result), 0)


if __name__ == '__main__':
    unittest.main()
