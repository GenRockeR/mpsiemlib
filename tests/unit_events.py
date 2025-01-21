import unittest
from datetime import datetime

import pytz

from mpsiemlib.common import *
from mpsiemlib.modules import MPSIEMWorker
from tests.settings import creds_local, settings


class EventsTestCase(unittest.TestCase):
    __mpsiemworker = None
    __module = None
    __creds_local = creds_local
    __settings = settings
    __begin = 0
    __end = 0

    @classmethod
    def setUpClass(cls) -> None:
        cls.__mpsiemworker = MPSIEMWorker(cls.__creds_local, cls.__settings)
        cls.__module = cls.__mpsiemworker.get_module(ModuleNames.EVENTS)
        cls.__end = round(datetime.now(tz=pytz.timezone(settings.local_timezone)).timestamp())
        cls.__begin = cls.__end - 86400

    @classmethod
    def tearDownClass(cls) -> None:
        cls.__module.close()

    def test_get_groups_simple(self):
        filters = {
            'es_filter': [
                '{"term": {"normalized": "true"}}'
            ],
            'es_filter_not': [
                '{"range": {"dst/ip": {"gte": "127.0.0.0","lte": "127.255.255.255"}}}',
                '{"range": {"dst/ip": {"gte": "169.254.0.0","lte": "169.254.255.255"}}}'
            ],
            "fields": "generator/type as generator"
        }
        counter = 0
        for _ in self.__module.get_events_group_by(filters, self.__begin, self.__end):
            counter += 1
        self.assertGreater(counter, 0)

    def test_get_groups_marks(self):
        filters = {
            'es_filter': [
                '{"term": {"normalized": "true"}}'
            ],
            'es_filter_not': [
                {'{"range": {"dst/ip": {"gte": "127.0.0.0","lte": "127.255.255.255"}}}': '7'},
                {'{"range": {"dst/ip": {"gte": "169.254.0.0","lte": "169.254.255.255"}}}': '7'},
                {'{"range": {"dst/ip": {"gte": "172.16.0.0","lte": "172.31.255.255"}}}': '1.7'},
                {'{"range": {"dst/ip": {"gte": "192.168.0.0","lte": "192.168.255.255"}}}': 'ALL'}
            ],
            'fields': 'dst/ip as object'
        }
        counter = 0

        for _ in self.__module.get_events_group_by(filters, self.__begin, self.__end):
            counter += 1
        self.assertGreater(counter, 0)

    def test_get_groups_multiple(self):
        filters = {
            'es_filter': [
                '{"term": {"normalized": "true"}}'
            ],
            'es_filter_not': [
                '{"range": {"dst/ip": {"gte": "127.0.0.0","lte": "127.255.255.255"}}}',
                '{"range": {"dst/ip": {"gte": "169.254.0.0","lte": "169.254.255.255"}}}',
                '{"range": {"dst/ip": {"gte": "172.16.0.0","lte": "172.31.255.255"}}}',
                '{"range": {"dst/ip": {"gte": "192.168.0.0","lte": "192.168.255.255"}}}'
            ],
            'fields': 'event_src/host as host, generator/type as generator'
        }
        ret = []
        for i in self.__module.get_events_group_by(filters, self.__begin, self.__end):
            ret.append(i)

        self.assertGreater(len(ret), 0) and ('source' in ret[0]) and ('destination' in ret[0]) and ('count' in ret[0])

    def test_get_events(self):
        begin = self.__end - 86400
        filters = {
            'es_filter': [
                '{"term": {"normalized": "true"}}'
            ],
            'es_filter_not': [
                '{"range": {"dst/ip": {"gte": "169.254.0.0","lte": "169.254.255.255"}}}'
            ],
        }
        ret = []
        for i in self.__module.get_events(filters, begin, self.__end):
            ret.append(i)

        self.assertGreater(len(ret), 0) and self.assertIsInstance(ret[0], dict)


if __name__ == '__main__':
    unittest.main()
