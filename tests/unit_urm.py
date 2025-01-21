import unittest
from faker import Faker

from mpsiemlib.common import *
from mpsiemlib.modules import MPSIEMWorker

from tests.settings import creds, settings


class URMTestCase(unittest.TestCase):
    __mpsiemworker = None
    __module = None
    __creds = creds
    __settings = settings
    __user = Faker('ru_RU')
    __username = __user.profile(fields=['username']).get('username')
    __firstname = __user.first_name_male()
    __lastname = __user.last_name_male()
    __email = __user.email()

    @classmethod
    def setUpClass(cls) -> None:
        cls.__mpsiemworker = MPSIEMWorker(cls.__creds, cls.__settings)
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
        self.assertGreater(len(ret), 0)

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

        self.assertGreater(len(ret), 0) and role.get("id") is not None and role.get("privileges") is not None

    def test_get_privileges_list(self):
        ret = self.__module.get_privileges_list()
        app_name = next(iter(ret))
        priv_name = next(iter(ret.get(app_name)))

        self.assertTrue((len(ret) != 0) and (priv_name is not None))

    # @unittest.skip("NotImplemented")
    def test_create_user(self):
        payload = {'userName': self.__username,
                   'email': self.__email,
                   'authType': 0,
                   'ldapSyncEnabled': False,
                   'status': 'active',
                   'passwordChange': True,
                   'firstName': self.__firstname,
                   'lastName': self.__lastname}
        user = self.__module.create_user(data=payload, password_generation=True)

        self.assertGreater(len(user), 0)

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
