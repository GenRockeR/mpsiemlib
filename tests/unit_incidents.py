import json
import unittest
from datetime import datetime

import pytz

from mpsiemlib.common import *
from mpsiemlib.modules import MPSIEMWorker
from settings import settings, creds_pat


class EventsTestCase(unittest.TestCase):
    __mpsiemworker = None
    __module = None
    __creds = creds_pat
    __settings = settings
    __begin = 0
    __end = 0

    @classmethod
    def setUpClass(cls) -> None:
        cls.__mpsiemworker = MPSIEMWorker(cls.__creds, cls.__settings)
        cls.__module = cls.__mpsiemworker.get_module(ModuleNames.INCIDENTS)  # noqa
        cls.__end = round(datetime.now(tz=pytz.timezone(settings.local_timezone)).timestamp())
        cls.__begin = cls.__end - 86400 * 5

    @classmethod
    def tearDownClass(cls) -> None:
        cls.__module.close()

    def test_get_list(self):
        counter = 0
        for _ in self.__module.get_incidents_list(self.__begin, self.__end):
            counter += 1

        self.assertGreater(counter, 0)

    def test_get_id_by_key(self):
        incident = next(self.__module.get_incidents_list(self.__begin, self.__end))
        incident_id = self.__module.get_incident_id_by_key(incident.get("key"))

        print(json.dumps(incident, indent=4, ensure_ascii=False))

        self.assertTrue(incident.get("id"), incident_id)

    def test_get_info(self):
        incident_short = next(self.__module.get_incidents_list(self.__begin, self.__end))
        incident_id = incident_short.get("id")
        incident = self.__module.get_incident_info(incident_id)

        print(json.dumps(incident, indent=4, ensure_ascii=False))

        self.assertGreater(len(incident), 0)


if __name__ == '__main__':
    unittest.main()
