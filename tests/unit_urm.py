import unittest

from helpers import gen_uppercase_string
from settings import creds_pat, settings

from mpsiemlib.common import *
from mpsiemlib.modules import MPSIEMWorker


class URMTestCase(unittest.TestCase):
    """Тесты модуля UsersAndRoles (SSO/IAM).

    Читающие операции проверяются на реальном контенте стенда; CRUD - на
    временных объектах с префиксом ``unittest_``. Удаления пользователя в SSO
    API нет (только block/unblock), поэтому созданный пользователь в cleanup
    блокируется. Роли создаются только в приложении с
    ``roleManagementEnabled`` и удаляются в cleanup.
    """

    __mpsiemworker = None
    __module = None
    __creds = creds_pat
    __settings = settings

    @classmethod
    def setUpClass(cls) -> None:
        cls.__mpsiemworker = MPSIEMWorker(cls.__creds, cls.__settings)
        cls.__module = cls.__mpsiemworker.get_module(ModuleNames.URM)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.__module.close()

    # ---------------- helpers ---------------- #

    def _refresh_users(self) -> dict:
        return self.__module.get_users_list()

    def _block_quietly(self, user_name: str) -> None:
        # Удаления пользователей в SSO нет - best-effort блокируем, чтобы
        # тестовый аккаунт не мог аутентифицироваться.
        try:
            self.__module.get_users_list()
            self.__module.lock_user(user_name)
        except Exception:
            pass

    def _delete_role_quietly(self, role_name: str, component: str) -> None:
        try:
            self.__module.get_roles_list()
            self.__module.delete_role(role_name, component)
        except Exception:
            pass

    def _role_management_app(self) -> str | None:
        """Первый компонент (MS/KB/CORE) с разрешённым редактированием ролей."""
        apps = self.__module.get_applications_list()
        for component in (MPComponents.MS, MPComponents.KB, MPComponents.CORE):
            if apps.get(component, {}).get("role_management_enabled"):
                return component
        return None

    # ---------------- приложения ---------------- #

    def test_get_applications_list(self):
        apps = self.__module.get_applications_list()
        self.assertGreater(len(apps), 0)
        app = next(iter(apps.values()))
        self.assertIn("name", app)

    # ---------------- пользователи: чтение ---------------- #

    def test_get_users_list_simple(self):
        users = self.__module.get_users_list()
        self.assertGreater(len(users), 0)
        user = next(iter(users.values()))
        self.assertIsNotNone(user.get("id"))
        self.assertIsNotNone(user.get("status"))
        self.assertIsNotNone(user.get("system"))

    def test_get_users_list_filtered(self):
        filters = {
            "authTypes": [1, 0],
            "statuses": ["active"],
            "withoutRoles": False,
        }
        users = self.__module.get_users_list(filters)
        self.assertGreater(len(users), 0)

    def test_get_user_info(self):
        users = self.__module.get_users_list()
        name = next(iter(users))
        info = self.__module.get_user_info(name)
        self.assertIsNotNone(info)
        self.assertEqual(info.get("id"), users[name].get("id"))

    def test_get_user_info_unknown(self):
        self.__module.get_users_list()
        self.assertIsNone(self.__module.get_user_info("unittest_no_such_user"))

    # ---------------- роли и привилегии: чтение ---------------- #

    def test_get_roles_list(self):
        roles = self.__module.get_roles_list()
        self.assertGreater(len(roles), 0)
        component_roles = next(iter(roles.values()))
        role = next(iter(component_roles.values()))
        self.assertIsNotNone(role.get("id"))
        self.assertIsNotNone(role.get("privileges"))

    def test_get_role_info(self):
        roles = self.__module.get_roles_list()
        app = next(iter(roles))
        role_name = next(iter(roles[app]))
        role = self.__module.get_role_info(role_name, app)
        self.assertIsNotNone(role)
        self.assertEqual(role.get("id"), roles[app][role_name].get("id"))

    def test_get_role_info_unknown(self):
        self.__module.get_roles_list()
        self.assertIsNone(
            self.__module.get_role_info("unittest_no_such_role", MPComponents.MS)
        )

    def test_get_privileges_list(self):
        privileges = self.__module.get_privileges_list()
        self.assertGreater(len(privileges), 0)
        component_privs = next(iter(privileges.values()))
        self.assertGreater(len(component_privs), 0)
        # {code: name}
        code = next(iter(component_privs))
        self.assertTrue(component_privs[code])

    # ---------------- пользователи: валидация ---------------- #

    def test_create_user_requires_username(self):
        with self.assertRaises(ValueError):
            self.__module.create_user(data={"email": "a@b.c"}, password_generation=True)

    def test_update_user_requires_username(self):
        with self.assertRaises(ValueError):
            self.__module.update_user(data={"email": "a@b.c"})

    def test_lock_unknown_user_returns_false(self):
        self.__module.get_users_list()
        self.assertFalse(self.__module.lock_user("unittest_no_such_user"))

    def test_user_roles_update_unknown_user_returns_false(self):
        self.__module.get_users_list()
        self.assertFalse(
            self.__module.user_roles_update(
                "unittest_no_such_user", {MPComponents.MS: ["any"]}
            )
        )

    def test_user_roles_update_unknown_role_returns_false(self):
        users = self.__module.get_users_list()
        name = next(iter(users))
        # реальный пользователь, но несуществующая роль -> False (не бросает)
        self.assertFalse(
            self.__module.user_roles_update(
                name, {MPComponents.MS: ["unittest_no_such_role"]}
            )
        )

    # ---------------- пользователи: CRUD ---------------- #

    def test_create_user_duplicate(self):
        uname = gen_uppercase_string(12)
        self.addCleanup(self._block_quietly, uname)
        payload = {
            "userName": uname,
            "email": f"{uname.lower()}@unittest.local",
            "authType": 0,
            "ldapSyncEnabled": False,
            "status": "active",
            "passwordChange": True,
            "firstName": "unittest",
            "lastName": "user",
        }
        user_id = self.__module.create_user(payload, password_generation=True)
        self.assertTrue(user_id)

        # повторное создание того же логина -> None (пользователь уже есть)
        again = self.__module.create_user(dict(payload), password_generation=True)
        self.assertIsNone(again)

    def test_user_lifecycle(self):
        uname = gen_uppercase_string(12)
        self.addCleanup(self._block_quietly, uname)
        payload = {
            "userName": uname,
            "email": f"{uname.lower()}@unittest.local",
            "authType": 0,
            "ldapSyncEnabled": False,
            "status": "active",
            "passwordChange": True,
            "firstName": "unittest",
            "lastName": "user",
        }
        user_id = self.__module.create_user(payload, password_generation=True)
        self.assertTrue(user_id)

        # пользователь появился в списке
        self._refresh_users()
        info = self.__module.get_user_info(uname)
        self.assertIsNotNone(info)
        self.assertEqual(info.get("id"), user_id)
        self.assertEqual(info.get("status"), "active")

        # update профиля (email)
        new_email = f"{uname.lower()}-upd@unittest.local"
        self.__module.update_user({"userName": uname, "email": new_email})
        self._refresh_users()
        self.assertEqual(self.__module.get_user_info(uname).get("email"), new_email)

        # block -> blocked, повторный block -> False
        self.assertTrue(self.__module.lock_user(uname))
        self._refresh_users()
        self.assertEqual(self.__module.get_user_info(uname).get("status"), "blocked")
        self.assertFalse(self.__module.lock_user(uname))

        # unblock -> active
        self.assertTrue(self.__module.unlock_user(uname))
        self._refresh_users()
        self.assertEqual(self.__module.get_user_info(uname).get("status"), "active")

    # ---------------- роли: CRUD ---------------- #

    def test_role_lifecycle(self):
        component = self._role_management_app()
        if component is None:
            self.skipTest("No application with roleManagementEnabled on stand")

        privileges = self.__module.get_privileges_list()
        if not privileges.get(component):
            self.skipTest(f"No privileges for component {component!r}")
        priv_name = next(iter(privileges[component].values()))

        role_name = gen_uppercase_string(12)
        self.addCleanup(self._delete_role_quietly, role_name, component)

        role_id = self.__module.create_role(
            role_name, "unittest role", component, [priv_name]
        )
        self.assertTrue(role_id)

        self.__module.get_roles_list()
        self.assertIsNotNone(self.__module.get_role_info(role_name, component))

        # invalid privilege -> None
        self.assertIsNone(
            self.__module.create_role(
                gen_uppercase_string(12), "bad", component, ["unittest_no_such_priv"]
            )
        )

        # update: переименование
        new_name = gen_uppercase_string(12)
        self.assertTrue(
            self.__module.update_role(
                role_name, new_name, "unittest role upd", component, [priv_name]
            )
        )
        self.__module.get_roles_list()
        self.assertIsNotNone(self.__module.get_role_info(new_name, component))

        # delete
        self.assertTrue(self.__module.delete_role(new_name, component))
        self.__module.get_roles_list()
        self.assertIsNone(self.__module.get_role_info(new_name, component))

    def test_create_role_unknown_component(self):
        self.__module.get_roles_list()
        self.assertIsNone(
            self.__module.create_role(
                gen_uppercase_string(12), "x", "unittest_no_such_app", ["whatever"]
            )
        )

    def test_delete_role_unknown_returns_false(self):
        self.__module.get_roles_list()
        self.assertFalse(
            self.__module.delete_role("unittest_no_such_role", MPComponents.MS)
        )


if __name__ == "__main__":
    unittest.main()
