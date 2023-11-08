import time
import unittest

from mpsiemlib.common import *
from mpsiemlib.modules import MPSIEMWorker
from tests.settings import creds, settings


class TasksTestCase(unittest.TestCase):
    __mpsiemworker = None
    __module = None
    __creds_ldap = creds
    __settings = settings

    @classmethod
    def setUpClass(cls) -> None:
        cls.__mpsiemworker = MPSIEMWorker(cls.__creds_ldap, cls.__settings)
        cls.__module = cls.__mpsiemworker.get_module(ModuleNames.TASKS)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.__module.close()

    def test_get_agents_list(self):
        ret = self.__module.get_agents_list()
        self.assertTrue(len(ret) != 0)

    def test_get_modules_list(self):
        ret = self.__module.get_modules_list()
        self.assertTrue(len(ret) != 0)

    def test_get_profiles_list(self):
        ret = self.__module.get_profiles_list()
        self.assertTrue(len(ret) != 0)

    @unittest.skip("Deprecated after R23")
    def test_get_transports_list(self):
        ret = self.__module.get_transports_list()
        self.assertTrue(len(ret) != 0)

    def test_get_credentials_list(self):
        ret = self.__module.get_credentials_list()
        self.assertTrue(len(ret) != 0)

    def test_get_tasks_list(self):
        ret = self.__module.get_tasks_list()
        self.assertTrue(len(ret) != 0)

    def test_get_tasks_info(self):
        task_id = next(iter(self.__module.get_tasks_list()))
        ret = self.__module.get_task_info(task_id)
        self.assertTrue(len(ret) != 0)

    def test_get_jobs_list(self):
        task_id = None
        for k, v in self.__module.get_tasks_list().items():
            if v.get("status") == "running":
                task_id = k
                break

        ret = self.__module.get_jobs_list(task_id)
        self.assertTrue(len(ret) != 0)

    @unittest.skip("Long test")
    def test_stop_start(self):

        task_id = None
        for k, v in self.__module.get_tasks_list().items():
            if v.get("status") == "running":
                task_id = k
                break

        self.__module.stop_task(task_id)

        success_stopped = False
        for i in range(60):
            time.sleep(10)
            status = self.__module.get_task_status(task_id)
            if status == "finished":
                success_stopped = True
                break

        self.__module.start_task(task_id)

        success_started = False
        for i in range(60):
            time.sleep(10)
            status = self.__module.get_task_status(task_id)
            if status == "running":
                success_started = True
                break

        self.assertTrue(success_stopped and success_started)

    # @unittest.skip("Long test")
    def test_create_delete(self):

        task_id = None

        for k, v in self.__module.get_agents_list().items():
            agent_uuid = k
            break

        for k, v in self.__module.get_profiles_list().items():
            if v['name'] == 'SysLog':
                profile_uuid = k
                break

        params = self.__module.get_default_syslog_task_params()
        params['name'] = 'test_creating_task'
        params['profile'] = profile_uuid
        params['agent'] = agent_uuid

        task_id = self.__module.create_task(params)

        success_created = False
        for _ in range(60):
            time.sleep(10)
            status = self.__module.get_task_status(task_id)
            if status == "new":
                success_created = True
                break

        ret = self.__module.delete_task(task_id)

        success_deleted = False
        if ret == 204:
            success_deleted = True

        self.assertTrue(success_created and success_deleted)


if __name__ == '__main__':
    unittest.main()
