import unittest

from mpsiemlib.common import *
from mpsiemlib.modules import MPSIEMWorker

from tests.settings import creds_ldap, settings


class URMTestCase(unittest.TestCase):
    __mpsiemworker = None
    __module = None
    __creds_ldap = creds_ldap
    __settings = settings

    @classmethod
    def setUpClass(cls) -> None:
        cls.__mpsiemworker = MPSIEMWorker(cls.__creds_ldap, cls.__settings)
        cls.__module = cls.__mpsiemworker.get_module(ModuleNames.URM)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.__module.close()

    def test_get_applications_list(self):
        ret = self.__module.get_applications_list()
        self.assertTrue(len(ret) != 0)

    def test_get_users_list_simple(self):
        ret = self.__module.get_users_list()
        key = next(iter(ret))
        user = ret.get(key)

        self.assertTrue((len(ret) != 0) and
                        (user.get("id") is not None) and
                        (user.get("status") is not None) and
                        (user.get("system") is not None))

    def test_get_users_list_filtered(self):
        filters = {"authTypes": [1, 0],
                   "statuses": ["active"],
                   "withoutRoles": False}

        ret = self.__module.get_users_list(filters)
        self.assertTrue(len(ret) != 0)

    def test_get_user_info(self):
        ret = self.__module.get_users_list()
        key = next(iter(ret))

        ret = self.__module.get_user_info(key)
        self.assertTrue(len(ret) != 0)

    def test_get_roles_list(self):
        ret = self.__module.get_roles_list()
        app_name = next(iter(ret))
        role_name = next(iter(ret.get(app_name)))
        role = ret[app_name][role_name]

        self.assertTrue((len(ret) != 0) and
                        (role.get("id") is not None) and
                        (role.get("privileges") is not None))

    def test_get_role_info(self):
        ret = self.__module.get_roles_list()
        app_name = next(iter(ret))
        role_name = next(iter(ret.get(app_name)))

        role = self.__module.get_role_info(role_name, MPComponents.MS)

        self.assertTrue((len(ret) != 0) and
                        (role.get("id") is not None) and
                        (role.get("privileges") is not None))

    def test_get_privileges_list(self):
        ret = self.__module.get_privileges_list()
        app_name = next(iter(ret))
        priv_name = next(iter(ret.get(app_name)))

        self.assertTrue((len(ret) != 0) and (priv_name is not None))

    @unittest.skip("NotImplemented")
    def test_create_user(self):
        pass

    @unittest.skip("NotImplemented")
    def test_delete_user(self):
        pass

    @unittest.skip("NotImplemented")
    def test_create_role(self):
        pass

    @unittest.skip("NotImplemented")
    def test_delete_role(self):
        pass


if __name__ == '__main__':
    unittest.main()
