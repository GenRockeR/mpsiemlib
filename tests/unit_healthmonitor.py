import json
import os
import tempfile
import unittest

import requests
from settings import creds_pat, settings

from mpsiemlib.common import *
from mpsiemlib.modules import MPSIEMWorker


class KBTestCase(unittest.TestCase):
    __mpsiemworker = None
    __module = None
    __creds = creds_pat
    __settings = settings

    @classmethod
    def setUpClass(cls) -> None:
        cls.__mpsiemworker = MPSIEMWorker(cls.__creds, cls.__settings)
        cls.__module = cls.__mpsiemworker.get_module(ModuleNames.HEALTH)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.__module.close()

    def test_get_global_status(self):
        ret = self.__module.get_health_status()

        print(json.dumps(ret, indent=4, ensure_ascii=False))

        self.assertIsInstance(ret, str)

    def test_get_errors(self):
        ret = self.__module.get_health_errors()

        print(json.dumps(ret, indent=4, ensure_ascii=False))

        self.assertIsInstance(ret, list)

    def test_get_license_status(self):
        ret = self.__module.get_health_license_status()

        print(json.dumps(ret, indent=4, ensure_ascii=False))

        self.assertGreater(len(ret), 0)

    def test_get_agents_status(self):
        ret = self.__module.get_health_agents_status()

        print(json.dumps(ret, indent=4, ensure_ascii=False))

        self.assertGreater(len(ret), 0)

    def test_get_licenses(self):
        ret = self.__module.get_health_licenses()

        print(json.dumps(ret, indent=4, ensure_ascii=False))

        self.assertIsInstance(ret, list)
        for lic in ret:
            self.assertIn("key", lic)
            self.assertIsInstance(lic.get("workloads"), dict)

    def test_get_archive_licenses(self):
        ret = self.__module.get_health_archive_licenses()

        print(json.dumps(ret, indent=4, ensure_ascii=False))

        self.assertIsInstance(ret, list)
        for lic in ret:
            self.assertIn("id", lic)

    def test_get_license_by_product(self):
        licenses = self.__module.get_health_licenses()
        product_id = next(
            (lic.get("product_id") for lic in licenses if lic.get("product_id")),
            None,
        )
        if product_id is None:
            self.skipTest("No license with bound product on the stand")

        ret = self.__module.get_health_license_by_product(product_id)

        print(json.dumps(ret, indent=4, ensure_ascii=False))

        self.assertIsInstance(ret, dict)
        self.assertIn("use_license", ret)

    def test_get_platform_activation_status(self):
        ret = self.__module.get_health_platform_activation_status()

        print(json.dumps(ret, indent=4, ensure_ascii=False))

        self.assertIsInstance(ret, dict)
        self.assertIsNotNone(ret.get("status"))

    def test_get_available_agents(self):
        ret = self.__module.get_health_available_agents()

        print(json.dumps(ret, indent=4, ensure_ascii=False))

        self.assertIsInstance(ret, list)

    def test_get_agents_by_ids(self):
        agents = self.__module.get_health_agents_status()
        if len(agents) == 0:
            self.skipTest("No agents on the stand")
        agent_id = agents[0].get("id")

        ret = self.__module.get_health_agents_by_ids([agent_id])

        print(json.dumps(ret, indent=4, ensure_ascii=False))

        self.assertEqual(len(ret), 1)
        self.assertEqual(ret[0].get("id"), agent_id)

    def test_get_agents_by_ids_empty(self):
        with self.assertRaises(ValueError):
            self.__module.get_health_agents_by_ids([])

    def test_get_license_files(self):
        license_ids = [
            lic["id"] for lic in self.__module.get_health_licenses() if lic.get("id")
        ]
        if not license_ids:
            self.skipTest("No licenses with id on the stand")

        with tempfile.TemporaryDirectory() as tmp_dir:
            filepath = os.path.join(tmp_dir, "license_keys.zip")
            archive = self.__module.get_health_license_files(
                license_ids, local_filepath=filepath
            )

            print(
                json.dumps(
                    {"license_ids": license_ids, "size": len(archive)},
                    indent=4,
                    ensure_ascii=False,
                )
            )

            self.assertGreater(len(archive), 0)
            self.assertEqual(archive[:2], b"PK")
            self.assertTrue(os.path.exists(filepath))
            self.assertEqual(os.path.getsize(filepath), len(archive))

    def test_get_license_files_empty(self):
        with self.assertRaises(ValueError):
            self.__module.get_health_license_files([])

    def test_get_licensing_archive(self):
        archive = self.__module.get_health_licensing_archive()

        print(json.dumps({"size": len(archive)}, indent=4, ensure_ascii=False))

        self.assertGreater(len(archive), 0)
        self.assertEqual(archive[:2], b"PK")

    def test_get_installation_key(self):
        try:
            installation_key = self.__module.get_health_installation_key()
        except requests.HTTPError as err:
            # 403 forbidden - ключ доступен только привилегированной учётной
            # записи, на стенде это ожидаемо и не ошибка библиотеки
            if err.response is not None and err.response.status_code == 403:
                self.skipTest("installation_key is forbidden for test credentials")
            raise

        print(
            json.dumps({"length": len(installation_key)}, indent=4, ensure_ascii=False)
        )

        self.assertIsInstance(installation_key, str)
        self.assertGreater(len(installation_key), 0)

    def test_validate_agents_delete(self):
        agents = self.__module.get_health_agents_status()
        if len(agents) == 0:
            self.skipTest("No agents on the stand")
        agent_ids = [agents[0].get("id")]

        total_jobs = self.__module.validate_health_agents_delete(agent_ids)

        print(json.dumps({"total_jobs": total_jobs}, indent=4, ensure_ascii=False))

        self.assertIsInstance(total_jobs, int)
        self.assertGreaterEqual(total_jobs, 0)

    def test_validate_agents_delete_empty(self):
        with self.assertRaises(ValueError):
            self.__module.validate_health_agents_delete([])

    @unittest.skip("Not implemented")
    def test_get_kb_status(self):
        ret = self.__module.get_health_kb_status()

        print(json.dumps(ret, indent=4, ensure_ascii=False))

        self.assertGreater(len(ret), 0)


if __name__ == "__main__":
    unittest.main()
