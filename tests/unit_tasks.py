import contextlib
import datetime
import time
import unittest
import uuid
from typing import ClassVar

import requests
from settings import creds_pat, settings

from mpsiemlib.common import *
from mpsiemlib.modules import MPSIEMWorker


class TasksTestCase(unittest.TestCase):
    __mpsiemworker = None
    __module = None
    __creds = creds_pat
    __settings = settings
    __copies: ClassVar[list[str]] = []
    __created_credentials: ClassVar[list[str]] = []
    __created_dictionaries: ClassVar[list[str]] = []
    __created_profiles: ClassVar[list[str]] = []
    __selections: ClassVar[list[str]] = []

    # base64 публичной части тестового EC-сертификата (сервер требует base64,
    # PEM «как есть» отбивается invalid.certificate.error)
    TEST_CERTIFICATE_BASE64 = (
        "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUJWakNCL0tBREFnRUNBaFJlaGp3"
        "YldEeUZJVW9tN1RlMkhxVVdkNVhQeFRBS0JnZ3Foa2pPUFFRREFqQVIKTVE4d0RRWURW"
        "UVFEREFaNmNISnZZbVV3SGhjTk1qWXhNREUwTVRReE16SXhXaGNOTXpZeE1ERXhNVFF4"
        "TXpJeApXakFSTVE4d0RRWURWUVFEREFaNmNISnZZbVV3V1RBVEJnY3Foa2pPUFFJQkJn"
        "Z3Foa2pPUFFNQkJ3TkNBQVRHCkEwR29wc2liNVlQSnc2a21ERXFXZ09Fanc1RXhjZVR3"
        "aS9mb3AwVUM4TUpvUWVMWFlVU2w4RFlOVXJkWFRSblYKdUJyOWRQOW5HMkE2OEZtWUp1"
        "NUdvekl3TURBZEJnTlZIUTRFRmdRVW1xRWFNRmcrZFB0b3RuRVdTbUhlTjJjRApwQzh3"
        "RHdZRFZSMFRBUUgvQkFVd0F3RUIvekFLQmdncWhrak9QUVFEQWdOSkFEQkdBaUVBcUJz"
        "a1Z1T1BTSDlKCkliVFlPak5ibGlrWFduQURBS3B4ejBQUWU5T1llREVDSVFEQ29KQ09q"
        "MDB6Sk1VM1p5empadURxK3VNWVp3N24KaTNHUlVwWFVwSUpIWWc9PQotLS0tLUVORCBD"
        "RVJUSUZJQ0FURS0tLS0tCg=="
    )

    @classmethod
    def setUpClass(cls) -> None:
        cls.__mpsiemworker = MPSIEMWorker(cls.__creds, cls.__settings)
        cls.__module = cls.__mpsiemworker.get_module(ModuleNames.TASKS)

    @classmethod
    def tearDownClass(cls) -> None:
        # Write-тесты обязаны убирать за собой объекты даже при падении кейса
        for task_id in cls.__copies:
            cls.__quietly(cls.__module.delete_task, task_id)
        for credential_id in cls.__created_credentials:
            cls.__quietly(cls.__module.delete_credential, credential_id)
        for dictionary_id in cls.__created_dictionaries:
            cls.__quietly(cls.__module.delete_dictionary, dictionary_id)
        for profile_id in cls.__created_profiles:
            cls.__quietly(cls.__module.delete_profile, profile_id)
        for selection_id in cls.__selections:
            cls.__quietly(cls.__module.delete_task_selection, selection_id)
        cls.__module.close()

    @staticmethod
    def __quietly(func, *args) -> None:
        with contextlib.suppress(requests.RequestException, ValueError):
            func(*args)

    @classmethod
    def __unique_name(cls, prefix: str) -> str:
        return f"test_{prefix}_{uuid.uuid4().hex[:8]}"

    # ------------------------- справочники сканера -------------------------

    def test_get_agents_list(self):
        ret = self.__module.get_agents_list()
        self.assertGreater(len(ret), 0)
        agent = next(iter(ret.values()))
        self.assertIn("hostname", agent)
        self.assertIn("status", agent)

    def test_get_agents_list_cache(self):
        first = self.__module.get_agents_list()
        again = self.__module.get_agents_list()
        self.assertEqual(first, again)
        self.assertIsNot(first, again)
        self.assertGreater(len(self.__module.get_agents_list(refresh=True)), 0)

    def test_get_available_agents_list(self):
        available = self.__module.get_available_agents_list()
        self.assertGreater(len(available), 0)
        self.assertLessEqual(
            set(available), set(self.__module.get_agents_list(refresh=True))
        )

    def test_get_agents_by_ids(self):
        agent_id = next(iter(self.__module.get_agents_list()))
        ret = self.__module.get_agents_by_ids(
            [agent_id, "00000000-0000-0000-0000-000000000000"]
        )
        # Неизвестные id сервер молча пропускает
        self.assertEqual(set(ret), {agent_id})

    def test_get_agents_by_ids_empty(self):
        self.assertEqual(self.__module.get_agents_by_ids([]), {})

    def test_get_agent_module_capacity(self):
        agent_id = next(iter(self.__module.get_agents_list()))
        ret = self.__module.get_agent_module_capacity(agent_id, "audit")
        self.assertIn("system_defined_capacity", ret)
        self.assertIn("user_defined_capacity", ret)

    def test_get_agent_module_capacity_unknown_agent(self):
        # Контракт обещает 404, сервер на неизвестного агента отвечает 500
        # (замерено на R28.0); неизвестный модуль агента - 404
        with self.assertRaises(requests.exceptions.HTTPError) as ctx:
            self.__module.get_agent_module_capacity(
                "00000000-0000-0000-0000-000000000000", "audit"
            )
        self.assertEqual(ctx.exception.response.status_code, 500)

    def test_get_agent_module_capacity_unknown_module(self):
        agent_id = next(iter(self.__module.get_agents_list()))
        with self.assertRaises(requests.exceptions.HTTPError) as ctx:
            self.__module.get_agent_module_capacity(agent_id, "no_such_module")
        self.assertEqual(ctx.exception.response.status_code, 404)

    def test_set_and_delete_agent_module_capacity(self):
        agent_id = next(iter(self.__module.get_agents_list()))
        modules = self.__module.get_agents_list()[agent_id]["modules"] or []
        if not modules:
            self.skipTest("Agent has no modules on the stand")
        module_id = modules[0]

        before = self.__module.get_agent_module_capacity(agent_id, module_id)
        self.assertTrue(self.__module.set_agent_module_capacity(agent_id, module_id, 5))
        try:
            after_set = self.__module.get_agent_module_capacity(agent_id, module_id)
            self.assertEqual(after_set["user_defined_capacity"], 5)
            self.assertEqual(
                after_set["system_defined_capacity"],
                before["system_defined_capacity"],
            )
            # 0 - допустимое значение, а не «сброс»
            self.__module.set_agent_module_capacity(agent_id, module_id, 0)
            self.assertEqual(
                self.__module.get_agent_module_capacity(agent_id, module_id)[
                    "user_defined_capacity"
                ],
                0,
            )
        finally:
            self.assertTrue(
                self.__module.delete_agent_module_capacity(agent_id, module_id)
            )
        self.assertIsNone(
            self.__module.get_agent_module_capacity(agent_id, module_id)[
                "user_defined_capacity"
            ]
        )

    def test_set_agent_module_capacity_unknown_module(self):
        # PUT ёмкости на R28.0 модуль не валидирует: неизвестный module_id
        # принимает (204), хотя GET/DELETE того же пути отвечают 404
        agent_id = next(iter(self.__module.get_agents_list()))
        self.assertTrue(
            self.__module.set_agent_module_capacity(agent_id, "no_such_module", 1)
        )
        with self.assertRaises(requests.exceptions.HTTPError) as ctx:
            self.__module.get_agent_module_capacity(agent_id, "no_such_module")
        self.assertEqual(ctx.exception.response.status_code, 404)

    def test_validate_agents_delete(self):
        # Операция read-only: считает подзадачи на агентах
        agent_id = next(iter(self.__module.get_agents_list()))
        self.assertGreaterEqual(self.__module.validate_agents_delete([agent_id]), 0)

    def test_delete_agents_unknown_id_and_status(self):
        # Реальных агентов не удаляем: проверяем только приём запроса по
        # несуществующему id и последующую проверку статуса удаления
        token = self.__module.delete_agents(["00000000-0000-0000-0000-000000000000"])
        self.assertIsNotNone(token)
        # На R28.0 токен из startDelete сервером не принимается
        # (core.agents.token.invalid / validation.error: view)
        with self.assertRaises(requests.exceptions.HTTPError) as ctx:
            self.__module.get_agents_delete_status(str(token))
        self.assertEqual(ctx.exception.response.status_code, 400)

    def test_get_modules_list(self):
        ret = self.__module.get_modules_list()
        self.assertGreater(len(ret), 0)
        self.assertIn("type", next(iter(ret.values())))

    def test_get_modules_list_work_type_filter(self):
        all_modules = self.__module.get_modules_list(refresh=True)
        active = self.__module.get_modules_list(work_type="active", refresh=True)
        self.assertGreater(len(active), 0)
        self.assertLess(len(active), len(all_modules))
        # Отфильтрованная выборка не должна подменять полный кэш
        self.assertEqual(
            len(self.__module.get_modules_list()),
            len(all_modules),
        )

    def test_get_modules_list_invalid_work_type(self):
        with self.assertRaises(ValueError):
            self.__module.get_modules_list(work_type="no_such_type")

    def test_get_profiles_list(self):
        ret = self.__module.get_profiles_list()
        self.assertGreater(len(ret), 0)
        profile = next(iter(ret.values()))
        self.assertIn("name", profile)
        self.assertIn("system", profile)

    def test_get_profiles_list_by_module(self):
        profiles = self.__module.get_profiles_list(refresh=True)
        module_id = next(p["module_id"] for p in profiles.values() if p["module_id"])
        filtered = self.__module.get_profiles_list(module_id=module_id)
        self.assertGreater(len(filtered), 0)
        self.assertLessEqual(len(filtered), len(profiles))
        self.assertTrue(all(p["module_id"] == module_id for p in filtered.values()))

    def test_get_profile_info(self):
        profile_id = next(iter(self.__module.get_profiles_list()))
        ret = self.__module.get_profile_info(profile_id)
        self.assertEqual(ret["id"], profile_id)
        self.assertIn("output", ret)

    def test_get_profile_info_unknown(self):
        with self.assertRaises(requests.exceptions.HTTPError) as ctx:
            self.__module.get_profile_info("00000000-0000-0000-0000-000000000000")
        self.assertEqual(ctx.exception.response.status_code, 400)

    def test_get_profile_schemas(self):
        all_schemas = self.__module.get_profile_schemas(refresh=True)
        self.assertGreater(len(all_schemas), 0)
        self.assertIn("dataSchema", all_schemas[0])

    def test_get_profile_schemas_by_module(self):
        module_id = next(iter(self.__module.get_modules_list()))
        ret = self.__module.get_profile_schemas(module_id=module_id)
        self.assertEqual(len(ret), 1)
        self.assertEqual(ret[0]["moduleId"], module_id)

    def test_get_profile_schemas_localization(self):
        ret = self.__module.get_profile_schemas_localization()
        self.assertGreater(len(ret), 0)

    def test_create_update_delete_profile(self):
        profiles = self.__module.get_profiles_list(refresh=True)
        base_id = next(
            (pid for pid, p in profiles.items() if p["name"] == "Unix SSH Audit"), None
        )
        if base_id is None:
            self.skipTest("No 'Unix SSH Audit' profile on the stand")

        name = self.__unique_name("profile")
        profile_id = self.__module.create_profile(name, base_id, description="probe")
        self.assertIsNotNone(profile_id)
        self.__created_profiles.append(str(profile_id))

        created = self.__module.get_profile_info(str(profile_id))
        self.assertEqual(created["name"], name)
        self.assertFalse(created["isSystem"])
        self.assertEqual(created["baseProfileId"], base_id)

        renamed = name + "_r"
        self.assertTrue(
            self.__module.update_profile(
                str(profile_id),
                renamed,
                description="probe2",
                overrides=created.get("overrides") or {},
            )
        )
        self.assertEqual(
            self.__module.get_profile_info(str(profile_id))["name"], renamed
        )
        # Обновление кэшированного списка после write-операции
        self.assertIn(str(profile_id), self.__module.get_profiles_list())

        self.assertTrue(self.__module.clear_profile_migration_errors(str(profile_id)))
        self.assertTrue(self.__module.delete_profile(str(profile_id)))
        with self.assertRaises(requests.exceptions.HTTPError) as ctx:
            self.__module.get_profile_info(str(profile_id))
        self.assertEqual(ctx.exception.response.status_code, 400)

    def test_create_profile_unknown_base(self):
        with self.assertRaises(requests.exceptions.HTTPError) as ctx:
            self.__module.create_profile(
                self.__unique_name("profile"), "00000000-0000-0000-0000-000000000000"
            )
        self.assertEqual(ctx.exception.response.status_code, 400)

    def test_update_profile_unknown(self):
        with self.assertRaises(requests.exceptions.HTTPError) as ctx:
            self.__module.update_profile(
                "00000000-0000-0000-0000-000000000000", self.__unique_name("profile")
            )
        self.assertEqual(ctx.exception.response.status_code, 400)

    @unittest.skip("PenTest profile API is broken on R28.0 stand (500 internal.error)")
    def test_generate_pentest_profile(self):
        ret = self.__module.generate_pentest_profile(["CVE-2017-0144"])
        self.assertIn("profile", ret)

    @unittest.skip("PenTest profile API is broken on R28.0 stand (405 GET / 500 POST)")
    def test_validate_vulnerability_for_pentest(self):
        self.__module.validate_vulnerability_for_pentest("CVE-2017-0144")

    def test_get_credentials_list(self):
        ret = self.__module.get_credentials_list()
        self.assertGreater(len(ret), 0)
        self.assertIn("type", next(iter(ret.values())))

    def test_get_credentials_list_by_tag(self):
        all_creds = self.__module.get_credentials_list(refresh=True)
        tag = next(iter(self.__module.get_credentials_tags()))
        filtered = self.__module.get_credentials_list(credential_tags=[tag])
        # Сервер возвращает безтеговые учётки + учётки с запрошенным тэгом,
        # поэтому проверяем вложенность, а не точное совпадение
        tagged = {
            cid for cid, c in all_creds.items() if tag in (c["credential_tags"] or [])
        }
        no_tag = {cid for cid, c in all_creds.items() if not c["credential_tags"]}
        self.assertLessEqual(len(filtered), len(all_creds))
        self.assertLessEqual(tagged, set(filtered))
        self.assertLessEqual(no_tag, set(filtered))

    def test_get_credentials_tags(self):
        ret = self.__module.get_credentials_tags()
        self.assertGreater(len(ret), 0)
        self.assertIsInstance(ret[0], str)

    def test_get_credential(self):
        credentials = self.__module.get_credentials_list()
        credential_id, credential = next(iter(credentials.items()))
        # Детальный GET привязан к типу: LoginPassword -> login_passwords
        credential_type = {"LoginPassword": "login_passwords"}.get(credential["type"])
        if credential_type is None:
            self.skipTest(f"No GET mapping for credential type {credential['type']!r}")
        ret = self.__module.get_credential(credential_id, credential_type)
        self.assertEqual(ret["id"], credential_id)
        self.assertEqual(ret["type"], credential["type"])

    def test_get_credential_wrong_type(self):
        # GET не своего типа сервер отбивает 500 (замерено на R28.0)
        credential_id = next(iter(self.__module.get_credentials_list()))
        with self.assertRaises(requests.exceptions.HTTPError) as ctx:
            self.__module.get_credential(credential_id, "passwords_only")
        self.assertEqual(ctx.exception.response.status_code, 500)

    def test_get_credential_invalid_type(self):
        with self.assertRaises(ValueError):
            self.__module.get_credential("00000000-0000-0000-0000-000000000000", "nope")

    def test_get_credential_type(self):
        credentials = self.__module.get_credentials_list()
        credential_id, credential = next(iter(credentials.items()))
        expected = self.__module.CREDENTIAL_TYPE_ALIASES.get(credential["type"])
        self.assertEqual(self.__module.get_credential_type(credential_id), expected)

    def test_get_credential_type_unknown(self):
        with self.assertRaises(ValueError):
            self.__module.get_credential_type("00000000-0000-0000-0000-000000000000")

    def test_create_update_delete_credential_login_passwords(self):
        name = self.__unique_name("cred")
        credential_id = self.__module.create_credential(
            "login_passwords",
            {
                "name": name,
                "description": "probe",
                "credentialTags": [],
                "domain": "",
                "login": "probe_user",
                "password": "Probe!2026",
            },
        )
        self.assertIsNotNone(credential_id)
        self.__created_credentials.append(str(credential_id))

        created = self.__module.get_credential(str(credential_id), "login_passwords")
        self.assertEqual(created["name"], name)
        self.assertEqual(created["login"], "probe_user")
        self.assertEqual(
            self.__module.get_credential_type(str(credential_id)), "login_passwords"
        )
        self.assertIn(str(credential_id), self.__module.get_credentials_list())

        self.assertTrue(
            self.__module.update_credential(
                str(credential_id),
                "login_passwords",
                {
                    "name": name + "_r",
                    "description": "probe2",
                    "credentialTags": [],
                    "domain": "",
                    "login": "probe_user2",
                    "shouldResetPassword": True,
                    "password": "Probe!2027",
                },
            )
        )
        updated = self.__module.get_credential(str(credential_id), "login_passwords")
        self.assertEqual(updated["name"], name + "_r")
        self.assertEqual(updated["login"], "probe_user2")

        self.assertTrue(self.__module.delete_credential(str(credential_id)))
        self.__created_credentials.remove(str(credential_id))
        with self.assertRaises(requests.exceptions.HTTPError) as ctx:
            self.__module.get_credential(str(credential_id), "login_passwords")
        self.assertEqual(ctx.exception.response.status_code, 404)

    def test_create_credential_duplicate_name(self):
        name = self.__unique_name("cred")
        body = {
            "name": name,
            "credentialTags": [],
            "domain": "",
            "login": "probe_user",
            "password": "Probe!2026",
        }
        credential_id = self.__module.create_credential("login_passwords", body)
        self.__created_credentials.append(str(credential_id))
        with self.assertRaises(requests.exceptions.HTTPError) as ctx:
            self.__module.create_credential("login_passwords", body)
        self.assertEqual(ctx.exception.response.status_code, 400)

    def test_create_credential_unknown_tag(self):
        with self.assertRaises(requests.exceptions.HTTPError) as ctx:
            self.__module.create_credential(
                "passwords_only",
                {
                    "name": self.__unique_name("cred"),
                    "credentialTags": ["NoSuchCredentialTag"],
                    "password": "Probe!2026",
                },
            )
        self.assertEqual(ctx.exception.response.status_code, 400)

    def test_create_credential_invalid_type(self):
        with self.assertRaises(ValueError):
            self.__module.create_credential("no_such_type", {"name": "x"})

    def test_create_update_delete_credential_passwords_only(self):
        name = self.__unique_name("cred")
        credential_id = self.__module.create_credential(
            "passwords_only",
            {"name": name, "credentialTags": [], "password": "Probe!2026"},
        )
        self.__created_credentials.append(str(credential_id))
        self.assertEqual(
            self.__module.get_credential_type(str(credential_id)), "passwords_only"
        )
        self.assertTrue(
            self.__module.update_credential(
                str(credential_id),
                "passwords_only",
                {
                    "name": name + "_r",
                    "credentialTags": [],
                    "shouldResetPassword": False,
                },
            )
        )
        self.assertEqual(
            self.__module.get_credential(str(credential_id), "passwords_only")["name"],
            name + "_r",
        )

    def test_create_update_delete_credential_certificate(self):
        # Сервер принимает base64 публичного сертификата; PEM «как есть»
        # отбивается core.scanning.credential.invalid.certificate.error
        name = self.__unique_name("cred")
        credential_id = self.__module.create_credential(
            "certificates",
            {
                "name": name,
                "credentialTags": [],
                "login": "probe_user",
                "certificate": self.TEST_CERTIFICATE_BASE64,
            },
        )
        self.assertIsNotNone(credential_id)
        self.__created_credentials.append(str(credential_id))

        self.assertTrue(
            self.__module.update_credential(
                str(credential_id),
                "certificates",
                {
                    "name": name + "_r",
                    "credentialTags": [],
                    "shouldResetCertificatePassword": False,
                },
            )
        )
        self.assertTrue(
            self.__module.update_credential_v4(
                str(credential_id),
                {
                    "name": name + "_v4",
                    "credentialTags": [],
                    "shouldResetCertificate": False,
                    "shouldResetCertificatePassword": False,
                },
            )
        )
        self.assertEqual(
            self.__module.get_credential(str(credential_id), "certificates")["name"],
            name + "_v4",
        )
        # v4 требует оба shouldReset*-флага
        with self.assertRaises(requests.exceptions.HTTPError) as ctx:
            self.__module.update_credential_v4(
                str(credential_id), {"name": name, "credentialTags": []}
            )
        self.assertEqual(ctx.exception.response.status_code, 400)

    def test_check_laps_provider_connection(self):
        credential_id = next(iter(self.__module.get_credentials_list()))
        # AD на стенде недоступен - ожидается connectionSuccess=False, не исключение
        ret = self.__module.check_laps_provider_connection(
            domain="lab.local",
            port=389,
            ssl=False,
            active_directory_credentials_id=credential_id,
            search_bases=["DC=lab,DC=local"],
        )
        self.assertIn("connectionSuccess", ret)
        self.assertFalse(ret["connectionSuccess"])

    def test_delete_credential_unknown(self):
        with self.assertRaises(requests.exceptions.HTTPError) as ctx:
            self.__module.delete_credential("00000000-0000-0000-0000-000000000000")
        self.assertEqual(ctx.exception.response.status_code, 400)

    def test_get_transports_list_not_implemented(self):
        with self.assertRaises(NotImplementedError):
            self.__module.get_transports_list()

    # ------------------------- справочники (dictionaries) -------------------------

    def test_get_dictionaries_list(self):
        ret = self.__module.get_dictionaries_list()
        self.assertGreater(len(ret), 0)
        self.assertIn("is_system", next(iter(ret.values())))

    def test_get_dictionary(self):
        dictionary_id = next(iter(self.__module.get_dictionaries_list()))
        ret = self.__module.get_dictionary(dictionary_id)
        self.assertEqual(ret["id"], dictionary_id)
        self.assertIn("content", ret)

    def test_get_dictionary_unknown(self):
        # Сервер на неизвестный id отвечает пустым телом (200)
        self.assertEqual(
            self.__module.get_dictionary("00000000-0000-0000-0000-000000000000"), {}
        )

    def test_create_update_delete_dictionary(self):
        name = self.__unique_name("dict")
        created = self.__module.create_dictionary(name, "a,b,c\n1,2,3\n")
        dictionary_id = created["id"]
        self.__created_dictionaries.append(dictionary_id)
        self.assertEqual(created["name"], name)
        self.assertFalse(created["isSystem"])

        ret = self.__module.get_dictionary(dictionary_id)
        self.assertEqual(ret["content"], "a,b,c\n1,2,3\n")
        self.assertIn(dictionary_id, self.__module.get_dictionaries_list())

        self.assertTrue(
            self.__module.update_dictionary(dictionary_id, name + "_r", "x,y\n1,2\n")
        )
        updated = self.__module.get_dictionary(dictionary_id)
        self.assertEqual(updated["name"], name + "_r")
        self.assertEqual(updated["content"], "x,y\n1,2\n")

        self.assertTrue(self.__module.delete_dictionary(dictionary_id))
        self.__created_dictionaries.remove(dictionary_id)
        # После удаления сервер отдаёт пустое тело (200)
        self.assertEqual(self.__module.get_dictionary(dictionary_id), {})

    def test_create_dictionary_duplicate_name(self):
        name = self.__unique_name("dict")
        created = self.__module.create_dictionary(name, "a\n")
        self.__created_dictionaries.append(created["id"])
        with self.assertRaises(requests.exceptions.HTTPError) as ctx:
            self.__module.create_dictionary(name, "b\n")
        self.assertEqual(ctx.exception.response.status_code, 400)

    def test_update_dictionary_unknown(self):
        with self.assertRaises(requests.exceptions.HTTPError) as ctx:
            self.__module.update_dictionary(
                "00000000-0000-0000-0000-000000000000",
                self.__unique_name("dict"),
                "x\n",
            )
        self.assertEqual(ctx.exception.response.status_code, 400)

    # ------------------------- задачи -------------------------

    def test_get_tasks_list(self):
        ret = self.__module.get_tasks_list()
        self.assertGreater(len(ret), 0)
        self.assertIn("status", next(iter(ret.values())))

    def test_get_tasks_list_filters_are_not_confused(self):
        all_tasks = self.__module.get_tasks_list(refresh=True)
        running = self.__module.get_tasks_list(main_filter="running", refresh=True)
        self.assertLessEqual(len(running), len(all_tasks))
        self.assertTrue(
            all(t["status"] in self.__module.RUNNING_STATUSES for t in running.values())
        )
        # Повторный вызов без фильтра обязан вернуть полный набор, а не кэш
        # выборки main_filter='running'
        self.assertEqual(len(self.__module.get_tasks_list()), len(all_tasks))

    def test_get_tasks_list_invalid_filters(self):
        with self.assertRaises(ValueError):
            self.__module.get_tasks_list(main_filter="no_such_filter")
        with self.assertRaises(ValueError):
            self.__module.get_tasks_list(additional_filter="no_such_filter")

    def test_get_tasks_count(self):
        self.assertEqual(
            self.__module.get_tasks_count(),
            len(self.__module.get_tasks_list(refresh=True)),
        )
        self.assertLessEqual(
            self.__module.get_tasks_count(main_filter="running"),
            self.__module.get_tasks_count(),
        )

    def test_get_tasks_count_invalid_filter(self):
        with self.assertRaises(ValueError):
            self.__module.get_tasks_count(main_filter="no_such_filter")

    def test_get_scope_tasks_count(self):
        scope_id = next(
            iter(
                t["scope"]["id"]
                for t in self.__module.get_tasks_list().values()
                if t.get("scope")
            )
        )
        self.assertGreater(self.__module.get_scope_tasks_count(scope_id), 0)

    def test_get_scope_tasks_count_unknown_scope(self):
        self.assertEqual(
            self.__module.get_scope_tasks_count("00000000-0000-0000-0000-000000000000"),
            0,
        )

    def test_get_tasks_bookmarks(self):
        ret = self.__module.get_tasks_bookmarks(offset=0, limit=2)
        self.assertIn("items", ret)
        self.assertIn("totalItems", ret)
        self.assertLessEqual(len(ret["items"]), 2)
        if ret["items"]:
            self.assertIn("savepoint", ret["items"][0])

    def test_get_tasks_bookmarks_invalid_limit(self):
        for limit in (0, 1001):
            with self.subTest(limit=limit), self.assertRaises(ValueError):
                self.__module.get_tasks_bookmarks(limit=limit)

    def test_validate_task(self):
        task_id = next(iter(self.__module.get_tasks_list()))
        self.assertTrue(self.__module.validate_task(task_id))

    def test_validate_task_unknown(self):
        # Валидация несуществующей задачи - 400 validation.error
        with self.assertRaises(requests.exceptions.HTTPError) as ctx:
            self.__module.validate_task("00000000-0000-0000-0000-000000000000")
        self.assertEqual(ctx.exception.response.status_code, 400)

    def test_get_task_info(self):
        task_id = next(iter(self.__module.get_tasks_list()))
        ret = self.__module.get_task_info(task_id)
        self.assertEqual(ret["id"], task_id)
        # Карточка v3 не содержит parameters (в контракте его нет)
        self.assertNotIn("parameters", ret)
        self.assertIn("triggerParameters", ret)

    def test_get_task_info_unknown(self):
        with self.assertRaises(requests.exceptions.HTTPError) as ctx:
            self.__module.get_task_info("00000000-0000-0000-0000-000000000000")
        self.assertEqual(ctx.exception.response.status_code, 404)

    def test_get_task_info_v4(self):
        task_id = next(iter(self.__module.get_tasks_list()))
        ret = self.__module.get_task_info_v4(task_id)
        self.assertEqual(ret["id"], task_id)
        # v4 отличается от v3 составом полей
        self.assertIn("agents", ret)
        self.assertIn("agentGeneration", ret)

    def test_get_task_info_v4_unknown(self):
        with self.assertRaises(requests.exceptions.HTTPError) as ctx:
            self.__module.get_task_info_v4("00000000-0000-0000-0000-000000000000")
        self.assertEqual(ctx.exception.response.status_code, 404)

    def test_calc_next_run_time(self):
        task_id = next(iter(self.__module.get_tasks_list()))
        trigger = self.__module.get_task_info(task_id)["triggerParameters"]
        if not trigger:
            self.skipTest("Task has no triggerParameters on the stand")
        ret = self.__module.calc_next_run_time(trigger)
        self.assertIsNotNone(ret)

    def test_calc_next_run_time_invalid_params(self):
        with self.assertRaises(requests.exceptions.HTTPError) as ctx:
            self.__module.calc_next_run_time({"type": "NoSuch", "isEnabled": True})
        self.assertEqual(ctx.exception.response.status_code, 400)

    def test_get_task_status_unknown(self):
        with self.assertRaises(ValueError):
            self.__module.get_task_status("00000000-0000-0000-0000-000000000000")

    # ------------------------- копии задач / v4 CRUD -------------------------

    __base_task_id: str | None = None

    def __base_task(self) -> str:
        """Задача-источник для копирования: завершённая, чтобы копия не
        наследовала активный запуск."""
        if self.__base_task_id is None:
            tasks = self.__module.get_tasks_list(refresh=True)
            self.__base_task_id = next(
                (
                    tid
                    for tid, t in tasks.items()
                    if t["status"] == "finished" and t.get("agent")
                ),
                None,
            )
        if self.__base_task_id is None:
            self.skipTest("No finished task with agent on the stand")
        return self.__base_task_id

    def __make_copy(
        self, use_v4: bool = False, source_task_id: str | None = None
    ) -> str:
        copy_id = self.__module.copy_task(
            source_task_id or self.__base_task(),
            self.__unique_name("copy"),
            use_v4=use_v4,
        )
        self.assertIsNotNone(copy_id)
        self.__copies.append(str(copy_id))
        return str(copy_id)

    def test_get_task_params_for_copy(self):
        params = self.__module.get_task_params_for_copy(self.__base_task())
        self.assertIsInstance(params["scope"], str)
        self.assertIsInstance(params["profile"], str)
        self.assertIsInstance(params["agent"], str)
        self.assertNotIn("agents", params)

        params_v4 = self.__module.get_task_params_for_copy(
            self.__base_task(), use_v4=True
        )
        # v4-карточка вместо agent отдаёт agents.agentIds
        self.assertNotIn("agent", params_v4)
        self.assertTrue(params_v4["agents"]["agentIds"])

    def test_copy_task_v3_and_edit(self):
        copy_id = self.__make_copy()
        # copy_task ждёт статус new сам
        self.assertEqual(self.__module.get_task_status(copy_id, refresh=True), "new")

        name = self.__unique_name("copy")
        params = self.__module.get_task_params_for_copy(copy_id)
        params["name"] = name
        self.assertEqual(self.__module.edit_task(copy_id, params), copy_id)
        self.assertEqual(self.__module.get_task_info(copy_id)["name"], name)

    def test_copy_task_v4_and_edit_v4(self):
        copy_id = self.__make_copy(use_v4=True)
        card = self.__module.get_task_info_v4(copy_id)
        self.assertIn("agents", card)

        name = self.__unique_name("copy")
        params = self.__module.get_task_params_for_copy(copy_id, use_v4=True)
        params["name"] = name
        self.assertEqual(self.__module.edit_task_v4(copy_id, params), copy_id)
        self.assertEqual(self.__module.get_task_info_v4(copy_id)["name"], name)

    def test_create_task_v4_requires_valid_params(self):
        with self.assertRaises(requests.exceptions.HTTPError) as ctx:
            self.__module.create_task_v4({"name": self.__unique_name("copy")})
        self.assertEqual(ctx.exception.response.status_code, 400)

    def test_suspend_copy_not_suspendable(self):
        # Задача new сервер suspend не принимает (400 "Cannot suspend"),
        # метод должен вернуть False, не доводя до запроса
        copy_id = self.__make_copy()
        self.assertFalse(self.__module.suspend_task(copy_id))

    def test_clean_copy_bookmarks_and_migration_errors(self):
        copy_id = self.__make_copy()
        self.assertTrue(self.__module.clean_task_bookmarks(copy_id))
        self.assertTrue(self.__module.clear_task_migration_errors(copy_id))

    def test_start_copy_connection_check(self):
        # Копия берётся от задачи, у которой connectionCheckSupported=true
        # (флаг есть только в v4-карточке). На R28.0 стенда запуск проверки
        # подключения отбивается лицензией
        # (core.scanning.task.license.access.denied, 400)
        tasks = self.__module.get_tasks_list(refresh=True)
        supported = next(
            (
                tid
                for tid, task in tasks.items()
                if task["status"] == "finished"
                and self.__module.get_task_info_v4(tid).get("connectionCheckSupported")
            ),
            None,
        )
        if supported is None:
            self.skipTest("No finished task with connectionCheckSupported on the stand")
        copy_id = self.__make_copy(use_v4=True, source_task_id=supported)
        self.assertTrue(
            self.__module.get_task_info_v4(copy_id).get("connectionCheckSupported")
        )
        with self.assertRaises(requests.exceptions.HTTPError) as ctx:
            self.__module.start_task_connection_check(copy_id)
        self.assertEqual(ctx.exception.response.status_code, 400)

    def test_start_copy_connection_check_invalid_mode(self):
        with self.assertRaises(ValueError):
            self.__module.start_task_connection_check(
                "00000000-0000-0000-0000-000000000000", mode="no_such_mode"
            )
        with self.assertRaises(ValueError):
            self.__module.start_task_connection_check(
                "00000000-0000-0000-0000-000000000000",
                agent_selection_mode="no_such_mode",
            )

    def test_get_tasks_list_v4(self):
        # На R28.0 dev-стенде v4-выборка отдаёт totalCount=0 (задачи другого
        # поколения агентов), но контракт отрабатывает: 200 + items/token
        ret = self.__module.get_tasks_list_v4(filter_body={"text": "no_such_task"})
        self.assertIn("items", ret)
        self.assertIn("token", ret)
        self.assertEqual(ret["items"], [])

    def test_get_tasks_list_v4_invalid_params(self):
        with self.assertRaises(ValueError):
            self.__module.get_tasks_list_v4(limit=0)
        with self.assertRaises(ValueError):
            self.__module.get_tasks_list_v4(order_by="no_such_order")
        with self.assertRaises(ValueError):
            self.__module.get_tasks_list_v4(order_direction="nope")

    # ------------------------- выделения задач -------------------------

    def test_task_selection_lifecycle(self):
        copies = [self.__make_copy() for _ in range(2)]
        ret = self.__module.create_task_selection(copies)
        selection_id = str(ret["selectionId"])
        self.__selections.append(selection_id)
        self.assertEqual(ret["selectedTasksCount"], 2)
        self.assertIn("start", ret["availableOperations"])

        fetched = self.__module.get_task_selection(selection_id)
        self.assertEqual(fetched["selectionId"], selection_id)
        self.assertEqual(fetched["selectedTasksCount"], 2)

        # PUT перечислением тех же id: count не уменьшается (замер R28.0)
        edited = self.__module.edit_task_selection(selection_id, copies)
        self.assertEqual(edited["selectedTasksCount"], 2)

        # Пустой список server отбивает как пустую выборку
        with self.assertRaises(requests.exceptions.HTTPError) as ctx:
            self.__module.edit_task_selection(selection_id, [])
        self.assertEqual(ctx.exception.response.status_code, 400)

        self.assertTrue(self.__module.delete_task_selection(selection_id))
        # Удалённое выделение больше не читается
        with self.assertRaises(requests.exceptions.HTTPError) as ctx:
            self.__module.get_task_selection(selection_id)
        self.assertEqual(ctx.exception.response.status_code, 400)

    def test_create_task_selection_all_except(self):
        known = list(self.__module.get_tasks_list())[:1]
        ret = self.__module.create_task_selection(unselected_task_ids=known)
        selection_id = str(ret["selectionId"])
        self.__selections.append(selection_id)
        try:
            # «Выбрано всё, кроме перечисленного»
            self.assertGreaterEqual(ret["selectedTasksCount"], 1)
        finally:
            self.__module.delete_task_selection(selection_id)
            self.__selections.remove(selection_id)

    def test_create_task_selection_invalid_args(self):
        with self.assertRaises(ValueError):
            self.__module.create_task_selection()
        with self.assertRaises(ValueError):
            self.__module.create_task_selection(["a"], unselected_task_ids=["b"])
        with self.assertRaises(ValueError):
            self.__module.create_task_selection([], main_filter="no_such_filter")

    def test_get_task_selection_unknown(self):
        with self.assertRaises(requests.exceptions.HTTPError) as ctx:
            self.__module.get_task_selection("00000000-0000-0000-0000-000000000000")
        self.assertEqual(ctx.exception.response.status_code, 400)

    # ------------------------- массовые операции -------------------------

    def __selection_of_copies(self, count: int = 2) -> str:
        copies = [self.__make_copy() for _ in range(count)]
        ret = self.__module.create_task_selection(copies)
        self.assertEqual(ret["selectedTasksCount"], count)
        selection_id = str(ret["selectionId"])
        self.__selections.append(selection_id)
        return selection_id

    def test_compare_tasks_schedule(self):
        selection_id = self.__selection_of_copies(2)
        ret = self.__module.compare_tasks_schedule(selection_id)
        self.assertIn("triggersAreSame", ret)
        self.assertIn(ret["schedulersState"], self.__module.SCHEDULERS_STATES)

    def test_update_tasks_schedule_and_wait(self):
        selection_id = self.__selection_of_copies(2)
        trigger = {
            "type": "Daily",
            "isEnabled": False,
            "fromDate": datetime.datetime.now(datetime.UTC).strftime(
                "%Y-%m-%dT%H:%M:%S.000Z"
            ),
            "timeZone": "+03:00",
            "atTime": "03:30:00",
            "daysOfWeek": [
                "monday",
                "tuesday",
                "wednesday",
                "thursday",
                "friday",
                "saturday",
                "sunday",
            ],
        }
        operation_id = self.__module.update_tasks_schedule(selection_id, trigger)
        self.assertIsNotNone(operation_id)
        self.assertEqual(
            self.__module.wait_bulk_operation(str(operation_id), timeout=180),
            "finished",
        )

    def test_bulk_operations_on_stopped_tasks(self):
        # Задачи new: сервер принимает операции, но задачи остаются new
        selection_id = self.__selection_of_copies(2)
        for operation in (
            self.__module.clean_tasks_bookmarks,
            self.__module.suspend_tasks,
            self.__module.stop_tasks,
        ):
            with self.subTest(operation=operation.__name__):
                operation_id = operation(selection_id)
                self.assertIsNotNone(operation_id)
                self.assertEqual(
                    self.__module.wait_bulk_operation(str(operation_id), timeout=120),
                    "finished",
                )
                # Завершённую операцию можно удалить
                self.assertTrue(self.__module.delete_bulk_operation(str(operation_id)))

    def test_start_tasks_connection_check_bulk(self):
        selection_id = self.__selection_of_copies(1)
        operation_id = self.__module.start_tasks_connection_check(selection_id)
        self.assertIsNotNone(operation_id)
        # На R28.0 стенда лицензия отклоняет запуск проверки подключения, и
        # операция висит inProgress - wait_bulk_operation обязан отвалиться
        # по таймауту, а не по ошибке
        status = self.__module.wait_bulk_operation(str(operation_id), timeout=20)
        self.assertIn(status, self.__module.BULK_OPERATION_STATUSES)

    def test_tasks_groups_round_trip(self):
        selection_id = self.__selection_of_copies(1)
        groups = self.__module.get_tasks_groups(selection_id)
        self.assertIsInstance(groups, list)
        # Групп на стенде нет ([]), поэтому проверяем только «пустой» вызов:
        # 204 без изменения состава
        self.assertTrue(self.__module.update_tasks_groups(selection_id, [], []))

    def test_bulk_operation_unknown(self):
        with self.assertRaises(requests.exceptions.HTTPError) as ctx:
            self.__module.get_bulk_operation_status(
                "00000000-0000-0000-0000-000000000000"
            )
        self.assertEqual(ctx.exception.response.status_code, 400)
        with self.assertRaises(requests.exceptions.HTTPError) as ctx:
            self.__module.delete_bulk_operation("00000000-0000-0000-0000-000000000000")
        self.assertEqual(ctx.exception.response.status_code, 400)

    def test_delete_tasks_bulk(self):
        # Удаление проверяется на двух созданных копиях - именно для них
        # bulk-delete и предназначен
        copies = [self.__make_copy() for _ in range(2)]
        selection_id = str(self.__module.create_task_selection(copies)["selectionId"])
        self.__selections.append(selection_id)
        operation_id = self.__module.delete_tasks(selection_id)
        self.assertIsNotNone(operation_id)
        self.assertEqual(
            self.__module.wait_bulk_operation(str(operation_id), timeout=180),
            "finished",
        )
        remaining = self.__module.get_tasks_list(refresh=True)
        for copy_id in copies:
            self.assertNotIn(copy_id, remaining)
            self.__copies.remove(copy_id)

    def test_get_task_report(self):
        _, run_id = self.__pick_run()
        if run_id is None:
            self.skipTest("No task runs on the stand")
        content, filename = self.__module.get_task_report(run_id)
        # xlsx = zip-архив
        self.assertTrue(content.startswith(b"PK"))
        if filename:
            self.assertTrue(filename.endswith(".xlsx"))

    def test_get_task_report_with_filters(self):
        task_id, run_id = self.__pick_run()
        if run_id is None:
            self.skipTest("No task runs on the stand")
        jobs = self.__module.get_jobs_list(task_id, run_id=run_id)
        if not jobs:
            self.skipTest("Run has no jobs on the stand")
        agent = next(iter(jobs.values()))["agent"]
        agent_id = agent["id"] if isinstance(agent, dict) else agent
        content, _ = self.__module.get_task_report(
            run_id, agent_id=agent_id, target_pattern="10."
        )
        self.assertTrue(content.startswith(b"PK"))

    def test_get_task_report_unknown_run(self):
        with self.assertRaises(requests.exceptions.HTTPError) as ctx:
            self.__module.get_task_report("00000000-0000-0000-0000-000000000000")
        self.assertEqual(ctx.exception.response.status_code, 400)

    # ------------------------- снимок результатов скана -------------------------

    def test_build_scan_snapshot(self):
        xml = self.__module.build_scan_snapshot(["198.51.100.1", "198.51.100.2"])
        self.assertTrue(xml.startswith("<Snapshot "))
        self.assertEqual(xml.count("<ns0:Host "), 2)
        self.assertIn("<Address>198.51.100.2</Address>", xml)

    def test_add_scan(self):
        scope_id = next(
            iter(
                t["scope"]["id"]
                for t in self.__module.get_tasks_list().values()
                if t.get("scope")
            )
        )
        scan_id = str(uuid.uuid4())
        self.assertTrue(
            self.__module.add_scan(
                scan_id,
                source="PenTest",
                scope_id=scope_id,
                scan_time=datetime.datetime.now(datetime.UTC).strftime(
                    "%Y-%m-%dT%H:%M:%S.0000000Z"
                ),
                content=self.__module.build_scan_snapshot(["198.51.100.77"]),
                no_ttl=False,
                create_only=True,
            )
        )

    def test_add_scan_invalid_source(self):
        with self.assertRaises(ValueError):
            self.__module.add_scan(
                str(uuid.uuid4()),
                source="no_such_source",
                scope_id="00000000-0000-0000-0000-000000000005",
                scan_time="2026-01-01T00:00:00.0000000Z",
                content=self.__module.build_scan_snapshot(["198.51.100.1"]),
            )

    def test_add_scan_corrupted_content(self):
        # scope сервер не валидирует (чужой UUID принимает), а битый XML - нет
        with self.assertRaises(requests.exceptions.HTTPError) as ctx:
            self.__module.add_scan(
                str(uuid.uuid4()),
                source="PenTest",
                scope_id="00000000-0000-0000-0000-000000000005",
                scan_time="2026-01-01T00:00:00.0000000Z",
                content="<Snapshot>not-a-snapshot",
            )
        self.assertEqual(ctx.exception.response.status_code, 400)

    # ------------------------- запуски и подзадачи -------------------------

    def __pick_run(self):
        """Первый попавшийся запуск любой задачи (или None)."""
        for task_id in self.__module.get_tasks_list():
            runs = self.__module.get_runs_list(task_id, limit=1)
            if runs["items"]:
                return task_id, runs["items"][0]["id"]
        return None, None

    def test_get_runs_list(self):
        task_id = next(iter(self.__module.get_tasks_list()))
        ret = self.__module.get_runs_list(task_id, limit=3)
        self.assertIn("items", ret)
        self.assertIn("totalItems", ret)
        self.assertLessEqual(len(ret["items"]), 3)

    def test_get_runs_list_unknown_task(self):
        self.assertEqual(
            self.__module.get_runs_list("00000000-0000-0000-0000-000000000000")[
                "items"
            ],
            [],
        )

    def test_get_runs_list_by_run_mode(self):
        task_id = next(iter(self.__module.get_tasks_list()))
        all_runs = self.__module.get_runs_list(task_id)
        if not all_runs["items"]:
            self.skipTest("No task runs on the stand")
        ret = self.__module.get_runs_list(task_id, run_mode="default")
        self.assertTrue(all(r["runMode"] == "default" for r in ret["items"]))
        # connectionCheck - отдельный режим, на стенде запусков такого режима нет
        self.assertEqual(
            self.__module.get_runs_list(task_id, run_mode="connectionCheck")["items"],
            [],
        )

    def test_get_runs_list_invalid_params(self):
        task_id = next(iter(self.__module.get_tasks_list()))
        with self.assertRaises(ValueError):
            self.__module.get_runs_list(task_id, limit=0)
        with self.assertRaises(ValueError):
            self.__module.get_runs_list(task_id, run_mode="no_such_mode")

    def test_get_jobs_list(self):
        task_id = None
        for k, v in self.__module.get_tasks_list().items():
            if v.get("status") == "running":
                task_id = k
                break
        if task_id is None:
            self.skipTest("No running task on the stand")

        ret = self.__module.get_jobs_list(task_id)
        self.assertGreater(len(ret), 0)

    def test_get_jobs_list_by_run(self):
        task_id, run_id = self.__pick_run()
        if run_id is None:
            self.skipTest("No task runs on the stand")
        ret = self.__module.get_jobs_list(task_id, run_id=run_id)
        # Завершённый запуск обязан иметь хотя бы одну подзадачу
        self.assertGreater(len(ret), 0)

    def test_get_job_info(self):
        task_id, run_id = self.__pick_run()
        if run_id is None:
            self.skipTest("No task runs on the stand")
        jobs = self.__module.get_jobs_list(task_id, run_id=run_id)
        if not jobs:
            self.skipTest("Run has no jobs on the stand")
        job_id = next(iter(jobs))
        ret = self.__module.get_job_info(job_id)
        self.assertEqual(ret["id"], job_id)
        self.assertIn("status", ret)

    def test_get_job_info_unknown(self):
        with self.assertRaises(requests.exceptions.HTTPError) as ctx:
            self.__module.get_job_info("00000000-0000-0000-0000-000000000000")
        self.assertEqual(ctx.exception.response.status_code, 400)

    def test_get_job_errors(self):
        task_id, run_id = self.__pick_run()
        if run_id is None:
            self.skipTest("No task runs on the stand")
        jobs = self.__module.get_jobs_list(task_id, run_id=run_id)
        if not jobs:
            self.skipTest("Run has no jobs on the stand")
        ret = self.__module.get_job_errors(next(iter(jobs)))
        self.assertIn("items", ret)
        self.assertIn("totalItems", ret)

    def test_get_run_agents(self):
        _, run_id = self.__pick_run()
        if run_id is None:
            self.skipTest("No task runs on the stand")
        ret = self.__module.get_run_agents(run_id)
        self.assertGreater(len(ret), 0)
        self.assertIn("name", ret[0])

    def test_get_run_agents_unknown_run(self):
        # Несуществующий запуск отвечает пустым списком (замерено на R28.0)
        self.assertEqual(
            self.__module.get_run_agents("00000000-0000-0000-0000-000000000000"), []
        )

    # ------------------------- CRUD / управление -------------------------

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

    def test_create_delete(self):
        agents = self.__module.get_agents_list(refresh=True)
        if not agents:
            self.skipTest("No scanner agents on the stand")
        agent_uuid = next(iter(agents))

        profiles = self.__module.get_profiles_list(refresh=True)
        profile_uuid = next(
            (pid for pid, p in profiles.items() if p["name"] == "SysLog"), None
        )
        if profile_uuid is None:
            self.skipTest("No SysLog profile on the stand")

        params = self.__module.get_default_syslog_task_params()
        params["name"] = "test_creating_task"
        params["profile"] = profile_uuid
        params["agent"] = agent_uuid

        task_id = self.__module.create_task(params)
        self.assertIsNotNone(task_id)
        # Задача осядет на стенде при падении ниже (цикл ожидания/assert)
        self.addCleanup(self.__delete_quietly, task_id)

        success_created = False
        for _ in range(60):
            time.sleep(10)
            status = self.__module.get_task_status(task_id)
            if status == "new":
                success_created = True
                break

        deleted = self.__module.delete_task(task_id)
        self.assertTrue(success_created and deleted)

    def __delete_quietly(self, task_id):
        with contextlib.suppress(requests.RequestException):
            self.__module.delete_task(task_id)


if __name__ == "__main__":
    unittest.main()
