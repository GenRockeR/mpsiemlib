import json
import unittest
from datetime import datetime

import pytz

from mpsiemlib.common import *
from mpsiemlib.modules import MPSIEMWorker
from settings import settings, creds_pat


class TestEventsAPITestCase(unittest.TestCase):
    __mpsiemworker = None
    __module = None
    __creds = creds_pat
    __settings = settings
    __begin = 0
    __end = 0

    @classmethod
    def setUpClass(cls) -> None:
        cls.__mpsiemworker = MPSIEMWorker(cls.__creds, cls.__settings)
        cls.__module = cls.__mpsiemworker.get_module(ModuleNames.EVENTSAPI)  # noqa
        cls.__end = round(datetime.now(tz=pytz.timezone(settings.local_timezone)).timestamp())
        cls.__begin = cls.__end - 60

    @classmethod
    def tearDownClass(cls) -> None:
        cls.__module.close()

    def test_get_events_grouped_by_fields(self):
        grouping_fields = ['id']
        result = self.__module.get_events_grouped_by_fields(
            query_filter='normalized = true',
            group_by_fields=grouping_fields,
            time_from=self.__begin,
            time_to=self.__end)
        print(json.dumps(result, indent=4, ensure_ascii=False))
        self.assertGreater(len(result), 0)

    def test_get_events_metadata(self):
        result = self.__module.get_events_metadata()
        print(json.dumps(result, indent=4, ensure_ascii=False))
        self.assertGreater(len(result), 0)

    def test_get_events_by_filter_v3(self):
        query_filter = 'select(time, event_src.host, text) | sort(time desc)'
        time_from = self.__begin
        time_to = self.__end
        result = list(self.__module.get_events_by_filter_v3(query_filter=query_filter,
                                                            time_from=time_from, time_to=time_to))
        print(json.dumps(result, indent=4, ensure_ascii=False))
        self.assertGreater(len(result), 0)


if __name__ == '__main__':
    unittest.main()
