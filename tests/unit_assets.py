﻿import unittest
from random import choice
from string import ascii_uppercase

from mpsiemlib.common import *
from mpsiemlib.modules import MPSIEMWorker
from tests.settings import creds, settings


class AssetsTestCase(unittest.TestCase):
    __mpsiemworker = None
    __module = None
    __creds_ldap = creds
    __settings = settings

    @classmethod
    def setUpClass(cls) -> None:
        cls.__mpsiemworker = MPSIEMWorker(cls.__creds_ldap, cls.__settings)
        cls.__module = cls.__mpsiemworker.get_module(ModuleNames.ASSETS)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.__module.close()

    def test_get_scopes(self):
        scopes = self.__module.get_scopes_list()

        self.assertTrue(len(scopes) > 0)

    def test_get_scope_id_by_name(self):
        scope = self.__module.get_scope_id_by_name('Инфраструктура по умолчанию')

        self.assertEqual(scope, '00000000-0000-0000-0000-000000000005')

    def test_import_assets_get_groups(self):
        groups = self.__module.import_assets_get_groups()

        self.assertTrue(len(groups) > 0)

    def test_get_group_id_by_name(self):
        group_id = self.__module.get_group_id_by_name('Root')

        self.assertTrue(group_id is not None)

    def test_import_assets_from_csv_2(self):
        group_id = self.__module.get_group_id_by_name('Root')
        scope_id = '00000000-0000-0000-0000-000000000005'

        import io
        csv = '''
        "typealias";"fqdn";"hostname";"ip";"mac";"isvirtual"
        "windows";"xxxxxxxxxx.test.local";"xxxxxxxxxx";"::1 | 10.254.129.129 | 127.0.0.1";"00:11:22:33:44:55";"true"
        "windows";"";"xxxxxxxxxx1";"::1 | 10.254.129.130 | 127.0.0.1";"00:11:22:33:44:56";"true"
        '''
        content = io.StringIO(csv)

        status, count, log = self.__module.import_assets_from_csv(content=content, scope_id=scope_id, group_id=group_id)

        self.assertTrue(status and (count == 1) and len(log) > 0)

    def test_get_assets_list_json(self):
        group_id = self.__module.get_group_id_by_name("Unmanaged hosts")
        token = self.__module.create_assets_request(pdql='select(@Host as host)',
                                                    group_ids=[group_id],
                                                    include_nested=False)
        request_size = self.__module.get_assets_request_size(token)

        counter = 0
        for i in self.__module.get_assets_list_json(token):
            if len(i) != 0:
                counter += 1

        self.assertTrue(counter == request_size)

    def test_get_assets_list_csv(self):
        group_id = self.__module.get_group_id_by_name("Unmanaged hosts")
        token = self.__module.create_assets_request(pdql='select(@Host as host)',
                                                    group_ids=[group_id],
                                                    include_nested=False)

        request_size = self.__module.get_assets_request_size(token)

        counter = 0
        for i in self.__module.get_assets_list_csv(token):
            if len(i) != 0:
                counter += 1

        self.assertTrue(counter == (request_size + 1))

    def test_static_group(self):
        group_name = 'sdk-test-' + (''.join(choice(ascii_uppercase) for i in range(12)))  # случайное имя
        parent_id = self.__module.get_group_id_by_name("Root")

        new_group_id = self.__module.create_group_static(parent_id=parent_id, group_name=group_name)
        group_id = self.__module.get_group_id_by_name(group_name, do_refresh=True)

        status = self.__module.delete_group(new_group_id)

        self.assertTrue((new_group_id == group_id) and status)

    def test_dynamic_group(self):
        group_name = 'sdk-test-' + (''.join(choice(ascii_uppercase) for i in range(12)))  # случайное имя
        parent_id = self.__module.get_group_id_by_name("Root")

        new_group_id = self.__module.create_group_dynamic(parent_id=parent_id, group_name=group_name, predicate='Host')
        group_id = self.__module.get_group_id_by_name(group_name, do_refresh=True)

        status = self.__module.delete_group(new_group_id)

        self.assertTrue((new_group_id == group_id) and status)

    @unittest.skip("Version less than 25.1")
    def test_delete_assets_by_id(self):
        group_id = self.__module.get_group_id_by_name("Root")
        token = self.__module. \
            create_assets_request(pdql='filter(Host.fqdn="xxxxxxxxxx.test.local")|select(Host.@id as id)',
                                  group_ids=[group_id],
                                  include_nested=True)

        hosts = [x.strip('"') for x in self.__module.get_assets_list_stream(token=token)]
        hosts.pop(0)

        status = self.__module.delete_assets_by_ids(asset_ids=hosts)

        self.assertGreater(len(hosts), 0) and (status is not None) and (status['succeedCount'] == len(hosts))

    def test_delete_assets_by_id_v27_2(self):
        group_id = self.__module.get_group_id_by_name("Root")
        token = self.__module. \
            create_assets_request(pdql='qsearch("xxxxxxxxxx.test.local") | select(Host. @ id as id)',
                                  group_ids=[group_id],
                                  include_nested=True)

        hosts = [x.strip('"') for x in self.__module.get_assets_list_stream(token=token)]
        hosts.pop(0)

        status = self.__module.delete_assets_by_ids(asset_ids=hosts)

        self.assertGreater(len(hosts), 0) and (status is not None) and (status['succeedCount'] == len(hosts))


if __name__ == '__main__':
    unittest.main()
