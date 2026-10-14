import unittest

import requests
from settings import creds_local, creds_pat, settings

from mpsiemlib.common import *
from mpsiemlib.modules import MPSIEMWorker


class WorkerTestCase(unittest.TestCase):
    __creds = None
    __settings = None

    def setUp(self) -> None:
        self.__creds = creds_pat
        self.__settings = settings
        self.__mpsiemworker = MPSIEMWorker(self.__creds, self.__settings)

    def test_MPSIEMWorker_init(self):
        self.assertIsInstance(self.__mpsiemworker, WorkerInterface)

    @unittest.skip(
        "Storage is Logspace: Events (extra mpsiemlib[events]) needs Elasticsearch"
    )
    def test_MPSIEMWorker_get_module_events(self):
        module = self.__mpsiemworker.get_module(ModuleNames.EVENTS)
        self.assertIsInstance(module, ModuleInterface)

    def test_MPSIEMWorker_get_module_table(self):
        module = self.__mpsiemworker.get_module(ModuleNames.TABLES)
        self.assertIsInstance(module, ModuleInterface)

    def test_MPSIEMWorker_get_module_auth(self):
        module = self.__mpsiemworker.get_module(ModuleNames.AUTH)
        self.assertIsInstance(module, AuthInterface)


class ModuleTestCase(unittest.TestCase):
    __creds = None
    __settings = None

    def setUp(self) -> None:
        self.__creds = creds_pat
        self.__settings = settings

    @unittest.skip("Skip test, when PAT auth")
    def test_MPSIEMAuth_connect_core_local(self):
        mpsiemworker = MPSIEMWorker(self.__creds, self.__settings)
        module = mpsiemworker.get_module(ModuleNames.AUTH)
        session = module.connect(MPComponents.CORE)
        self.assertIsInstance(session, requests.Session)
        session.close()

    @unittest.skip("Skip test, when Local auth")
    def test_MPSIEMAuth_connect_core_ldap(self):
        mpsiemworker = MPSIEMWorker(self.__creds, self.__settings)
        module = mpsiemworker.get_module(ModuleNames.AUTH)
        session = module.connect(MPComponents.CORE)
        self.assertIsInstance(session, requests.Session)
        session.close()

    def test_MPSIEMAuth_connect_core_pat(self):
        mpsiemworker = MPSIEMWorker(self.__creds, self.__settings)
        module = mpsiemworker.get_module(ModuleNames.AUTH)
        session = module.connect(MPComponents.CORE)
        self.assertIsInstance(session, requests.Session)
        session.close()

    def test_MPSIEMAuth_get_core_version(self):
        mpsiemworker = MPSIEMWorker(self.__creds, self.__settings)
        module = mpsiemworker.get_module(ModuleNames.AUTH)
        version = int(module.get_core_version().split(".")[0])
        self.assertGreaterEqual(version, 27)


class AuthMethodsTestCase(unittest.TestCase):
    """Прямые проверки MPSIEMAuth (вне MPSIEMWorker).

    Замерено на R28.0: `system_info` (MC Portal) отвечает 500, версия
    добирается fallback'ом licensing v1; SECRET ходит на :3334/connect/token.
    Креды не мутируются: общие объекты settings.py читают другие классы
    тестов, поэтому для каждого кейса строится копия.
    """

    @staticmethod
    def _copy(creds: Creds, auth_type: int | None = None) -> Creds:
        copy = Creds()
        copy.core_hostname = creds.core_hostname
        copy.storage_hostname = creds.storage_hostname
        copy.siem_hostname = creds.siem_hostname
        copy.core_login = creds.core_login
        copy.core_pass = creds.core_pass
        copy.client_secret = creds.client_secret
        copy.pat_token = creds.pat_token
        copy.core_auth_type = auth_type if auth_type is not None else AuthType.LOCAL
        return copy

    @staticmethod
    def _auth(creds: Creds) -> MPSIEMAuth:
        return MPSIEMAuth(creds, settings)

    def test_pat_token_is_creds_token(self):
        auth = self._auth(creds_pat)
        self.assertEqual(auth.get_token(), creds_pat.pat_token)

    def test_core_version_and_release(self):
        auth = self._auth(creds_pat)
        auth.connect(MPComponents.CORE)
        version = auth.get_core_version()
        self.assertRegex(version, r"^\d+\.\d+")
        release = auth.get_core_release()
        self.assertIsInstance(release, tuple)
        self.assertGreaterEqual(release, (26, 0))
        self.assertTrue(auth.is_core_release_at_least(release))
        self.assertFalse(auth.is_core_release_at_least((release[0] + 1, 0)))

    def test_secret_get_token_via_ms(self):
        """SECRET: get_token() ходит на :3334/connect/token и отдаёт токен."""
        creds = self._copy(creds_local, AuthType.SECRET)
        if creds.client_secret is None:
            self.skipTest("Нужен CLIENT_SECRET в окружении (tests/settings.py)")
        auth = self._auth(creds)
        token = auth.get_token()
        self.assertIsNotNone(token)
        self.assertGreater(len(token or ""), 0)

    def test_local_connect_core(self):
        """LOCAL: cookie-вход через UI-form, версия определяется fallback'ом."""
        creds = self._copy(creds_local, AuthType.LOCAL)
        auth = self._auth(creds)
        try:
            session = auth.connect(MPComponents.CORE)
            self.assertIsInstance(session, requests.Session)
            # токен LOCAL берётся только при заданном client_secret
            token = auth.get_token()
            if creds.client_secret is None:
                self.assertIsNone(token)
            else:
                self.assertTrue(token)
            self.assertGreaterEqual(auth.get_core_release(), (26, 0))
        except AuthError as err:
            self.skipTest(f"LOCAL login недоступен на стенде: {err}")
        finally:
            auth.disconnect()

    def test_local_disconnect_logs_out_server_session(self):
        """disconnect() LOCAL гасит серверную cookie-сессию (замерено на R28.0).

        Логин изолированного MPSIEMAuth: logout завершает вход IAM только этой
        сессии - основные PAT-сессии других классов тестов не затрагиваются
        (проверено: повторный connect() аутентифицируется заново).
        """
        creds = self._copy(creds_local, AuthType.LOCAL)
        auth = self._auth(creds)
        try:
            session = auth.connect(MPComponents.CORE)
        except AuthError as err:
            self.skipTest(f"LOCAL login недоступен на стенде: {err}")

        probe_url = f"https://{creds.core_hostname}/api/v3/events/filters/default"
        self.assertEqual(exec_request(session, probe_url, timeout=30).status_code, 200)

        auth.disconnect()

        response = session.get(
            probe_url, timeout=30, verify=False, allow_redirects=False
        )
        self.assertIn(response.status_code, (401, 403))

    def test_local_bad_credentials_raise_auth_error(self):
        """Неверные креды -> AuthError (сообщение IAM), а не requests.HTTPError."""
        creds = self._copy(creds_local, AuthType.LOCAL)
        creds.core_login = "unittest_no_such_user"
        creds.core_pass = "Xx123456"
        auth = self._auth(creds)
        with self.assertRaises(AuthError):
            auth.connect(MPComponents.CORE)

    def test_storage_component_not_supported(self):
        """connect(STORAGE) больше не ES-зонд: storage вне ядра библиотеки.

        Elasticsearch вынесен в extra `mpsiemlib[events]` (производитель
        переводит storage на Logspace), поэтому MPSIEMAuth отвечает
        StorageNotSupportedError, а не лезет в `GET :9200/_nodes`. Метод
        `get_storage_version()` из MPSIEMAuth удалён вместе с ES-номенклатурой
        версий.
        """
        auth = self._auth(creds_pat)
        with self.assertRaises(StorageNotSupportedError):
            auth.connect(MPComponents.STORAGE)
        self.assertFalse(hasattr(auth, "get_storage_version"))

    def test_check_missing_envs(self):
        # пустой pat_token
        creds = Creds()
        creds.core_hostname = "core.invalid"
        creds.core_auth_type = AuthType.PAT_TOKEN
        with self.assertRaises(AuthError):
            self._auth(creds).check_missing_envs()

        # неизвестный auth_type (setter Creds его не пропустит - проверяем
        # сам MPSIEMAuth на creds, собранных без валидации сеттером)
        bad = Creds({"core": {"hostname": "core.invalid", "auth_type": 99}})
        with self.assertRaises(AuthError):
            self._auth(bad).check_missing_envs()

    def test_kb_connect_pat(self):
        """KB под PAT: Bearer, db list + aboutSystem (версия KB)."""
        auth = self._auth(creds_pat)
        session = auth.connect(MPComponents.KB)
        self.assertIsInstance(session, requests.Session)
        self.assertRegex(auth.get_kb_version(), r"^\d+\.\d+")
        session.close()


if __name__ == "__main__":
    unittest.main()
