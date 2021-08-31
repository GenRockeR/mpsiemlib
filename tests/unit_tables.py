import unittest

from mpsiemlib.common import *
from mpsiemlib.modules import MPSIEMWorker

from tests.settings import creds_ldap, settings


class TablesTestCase(unittest.TestCase):
    __mpsiemworker = None
    __module = None
    __creds_ldap = creds_ldap
    __settings = settings

    @classmethod
    def setUpClass(cls) -> None:
        cls.__mpsiemworker = MPSIEMWorker(cls.__creds_ldap, cls.__settings)
        cls.__module = cls.__mpsiemworker.get_module(ModuleNames.TABLES)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.__module.close()

    def test_get_table_list(self):
        ret = self.__module.get_tables_list()
        self.assertTrue(len(ret) != 0)

    def test_get_table_data_simple(self):
        tables = list(self.__module.get_tables_list())
        key = tables[0]
        ret = []
        for i in self.__module.get_table_data(key):
            ret.append(i)
        self.assertTrue((len(ret) != 0) and ("_id" in ret[0]))

    def test_get_table_data_filtered(self):
        filters = {"select": ["_last_changed"],
                   "where": "_id>5",
                   "orderBy": [{"field": "_last_changed",
                                "sortOrder": "descending"}],
                   "timeZone": 0}
        is_id_less = True
        is_valid_struct = True
        tables = list(self.__module.get_tables_list())
        key = tables[0]
        ret = []
        for i in self.__module.get_table_data(key, filters):
            ret.append(i)
            if int(i.get("_id")) <= 5:
                is_id_less = False
                break
            if len(i) != 2 or "_last_changed" not in i:  # должно быть только поле _id и _last_changed
                is_valid_struct = False
                break

        self.assertTrue((len(ret) != 0) and is_valid_struct and is_id_less)

    def test_get_table_info(self):
        tables = list(self.__module.get_tables_list())
        key = tables[0]
        table_info = self.__module.get_table_info(key)

        lookup_fields = ["id", "type", "editable", "size_max", "size_typical", "size_current",
                         "ttl", "ttl_enabled", "description", "created", "updated", "fields",
                         "notifications"]
        has_all_fields = len(set(table_info).intersection(lookup_fields)) == len(lookup_fields)

        is_valid_struct = True
        is_asset_table = False
        for k, v in table_info.items():
            if k == "type" and v in ["assetgrid", "registry"]:
                is_asset_table = True
            if k in ["notifications", "size_max", "size_typical", "ttl", "ttl_enabled"] and is_asset_table:
                continue
            if v is None and k != "notifications":
                is_valid_struct = False
                break

        self.assertTrue(has_all_fields and is_valid_struct)

    def test_set_table_data(self):
        self.__module.truncate_table("test_tl_2_r23")

        import io
        example = '''"_last_changed";"cust";"user";"session_stat"\r\n"25.11.2020 19:35:20";"customer1";"user1";"2020-11-22 00:00:00"'''
        self.__module.set_table_data("test_tl_2_r23", io.StringIO(example))

        ret = []
        for i in self.__module.get_table_data("test_tl_2_r23"):
            ret.append(i)
        self.assertTrue((len(ret) != 0) and ("_id" in ret[0]))

    @unittest.skip("Dangerous")
    def test_truncate(self):
        self.assertTrue(self.__module.truncate_table("test_tl_2_r23"))

    def test_set_table_row(self):
        add = [{"cust": "test1",
                "user": "user1",
                "session_stat": "12.12.2020 15:23:23"},
               {"cust": "test2",
                "user": "user2",
                "session_stat": "12.12.2020 15:23:23"}
               ]
        self.__module.set_table_row("test_tl_2_r23", add_rows=add, remove_rows=None)
        is_added = False
        for i in self.__module.get_table_data("test_tl_2_r23"):
            if i.get("cust") == "test1" and i.get("user") == "user1":
                is_added = True

        self.__module.set_table_row("test_tl_2_r23", add_rows=None, remove_rows=add)
        is_removed = True
        for i in self.__module.get_table_data("test_tl_2_r23"):
            if i.get("cust") == "test1" and i.get("user") == "user1":
                is_removed = False

        self.assertTrue(is_added and is_removed)


if __name__ == '__main__':
    unittest.main()
