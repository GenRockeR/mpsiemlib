import unittest

import pytz
from datetime import datetime

from mpsiemlib.common import *
from mpsiemlib.modules import MPSIEMWorker

from tests.settings import creds_ldap, settings


class EventsTestCase(unittest.TestCase):
    __mpsiemworker = None
    __module = None
    __creds_ldap = creds_ldap
    __settings = settings
    __begin = 0
    __end = 0

    @classmethod
    def setUpClass(cls) -> None:
        cls.__mpsiemworker = MPSIEMWorker(cls.__creds_ldap, cls.__settings)
        cls.__module = cls.__mpsiemworker.get_module(ModuleNames.INCIDENTS)
        cls.__end = round(datetime.now(tz=pytz.timezone(settings.local_timezone)).timestamp())
        cls.__begin = cls.__end - 86400*5

    @classmethod
    def tearDownClass(cls) -> None:
        cls.__module.close()

    def test_get_list(self):
        counter = 0
        for i in self.__module.get_incidents_list(self.__begin, self.__end):
            counter += 1

        self.assertGreater(counter, 0)

    def test_get_id_by_key(self):
        incident = next(self.__module.get_incidents_list(self.__begin, self.__end))
        incident_id = self.__module.get_incident_id_by_key(incident.get("key"))

        self.assertTrue(incident.get("id"), incident_id)

    def test_get_info(self):
        incident_short = next(self.__module.get_incidents_list(self.__begin, self.__end))
        incident_id = incident_short.get("id")
        incident = self.__module.get_incident_info(incident_id)

        self.assertTrue(len(incident) != 0)


if __name__ == '__main__':
    unittest.main()
