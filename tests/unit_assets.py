import io
import unittest
import uuid
from functools import partial
from random import choice
from string import ascii_lowercase

import requests
from settings import creds_pat, settings

from mpsiemlib.common import *
from mpsiemlib.modules import MPSIEMWorker


class AssetsTestCase(unittest.TestCase):
    """Тесты модуля Assets.

    Каждый тест самодостаточен: создаёт собственные объекты (уникальные
    активы/группы/запросы) и удаляет их через addCleanup. Тесты не зависят
    друг от друга и от порядка запуска (unittest запускает их по алфавиту).
    """

    __mpsiemworker = None
    __module = None
    __creds = creds_pat
    __settings = settings

    _QUERY_FOLDERS_API = (
        "/api/assets_temporal_readmodel/v1/stored_queries/folders/queries"
    )
    _QUERIES_API = "/api/assets_temporal_readmodel/v1/stored_queries/queries"

    @classmethod
    def setUpClass(cls) -> None:
        cls.__mpsiemworker = MPSIEMWorker(cls.__creds, cls.__settings)
        cls.__module = cls.__mpsiemworker.get_module(ModuleNames.ASSETS)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.__module.close()

    # ------------------------------------------------------------------ #
    # helpers
    # ------------------------------------------------------------------ #
    @staticmethod
    def _unique(prefix: str) -> str:
        return prefix + "".join(choice(ascii_lowercase) for _ in range(12))

    def _session(self):
        return self.__module._Assets__core_session

    def _api_url(self, path: str) -> str:
        return f"https://{self.__creds.core_hostname}{path}"

    def _raw_delete(self, path: str, obj_id: str) -> None:
        # SDK не предоставляет удаления папок/запросов, чистим напрямую
        url = self._api_url(f"{path}/{obj_id}")
        try:
            exec_request(
                self._session(),
                url,
                method="DELETE",
                timeout=self.__settings.connection_timeout,
            )
        except requests.RequestException:
            pass

    def _import_probe_asset(self) -> str:
        """Импортировать актив с уникальным fqdn и вернуть его id."""
        uniq = self._unique("sdk")
        fqdn = f"{uniq}.test.local"
        group_id = self.__module.get_group_id_by_name("Root")
        scope_id = self.__module.get_scope_id_by_name("Инфраструктура по умолчанию")
        csv = (
            '"typealias";"fqdn";"hostname";"ip";"mac";"isvirtual"\n'
            f'"windows";"{fqdn}";"{uniq}";'
            '"::1 | 10.254.199.199 | 127.0.0.1";"00:11:22:33:44:55";"true"\n'
        )
        status, count, _ = self.__module.import_assets_from_csv(
            content=io.StringIO(csv), scope_id=scope_id, group_id=group_id
        )
        self.assertTrue(status and count == 1)
        token = self.__module.create_assets_request(
            pdql=f'qsearch("{uniq}") | select(Host.@id as id)',
            group_ids=[group_id],
            include_nested=True,
        )
        ids = [x.strip('"') for x in self.__module.get_assets_list_stream(token=token)][
            1:
        ]
        self.assertTrue(ids, f"импортированный актив {fqdn} не найден")
        return ids[0]

    def _delete_asset(self, asset_id: str) -> None:
        # Актив мог быть удалён самим тестом - чистка не должна падать
        try:
            self.__module.delete_assets_by_ids(asset_ids=[asset_id])
        except (requests.RequestException, RuntimeError):
            pass

    def _create_probe_group(self) -> tuple[str, str]:
        parent_id = self.__module.get_group_id_by_name("Root")
        group_name = self._unique("sdk-grp-")
        group_id = self.__module.create_group_static(
            parent_id=parent_id, group_name=group_name
        )
        self.addCleanup(partial(self.__module.delete_group, group_id))
        return group_name, group_id

    def _create_probe_dynamic_group(self, predicate: str) -> tuple[str, str]:
        parent_id = self.__module.get_group_id_by_name("Root")
        group_name = self._unique("sdk-dyn-")
        group_id = self.__module.create_group_dynamic(
            parent_id=parent_id, group_name=group_name, predicate=predicate
        )
        self.addCleanup(partial(self.__module.delete_group, group_id))
        return group_name, group_id

    @staticmethod
    def _asset_config_payload(config: dict) -> dict:
        """Собрать AssetChangeViewModel из AssetViewModel.

        PUT принимает только os{id, properties, ports}, key{properties} и
        softs{added, changed, removed}; поля only-read (`displayName`, `type`,
        `objectId`, `canEditPorts`, `isReadOnly` и т.п.) сервер отклоняет (500),
        а отсутствующий `softs` тоже даёт 500.
        """
        os_view = config["os"]
        return {
            "os": {
                "id": os_view["id"],
                "properties": [
                    {"name": prop["name"], "value": prop["value"]}
                    for prop in os_view["properties"]
                ],
                "ports": os_view.get("ports") or [],
            },
            "key": {
                "properties": [
                    {"name": key["name"], "values": key["values"]}
                    for key in config["key"]
                ]
            },
            "softs": {"added": [], "changed": [], "removed": []},
        }

    def _user_root_id(self) -> str:
        tree = self.__module.get_queries()
        return next(n["id"] for n in tree if n.get("type") == "user")

    # ------------------------------------------------------------------ #
    # справочные запросы
    # ------------------------------------------------------------------ #
    def test_get_scopes(self):
        scopes = self.__module.get_scopes_list()
        self.assertTrue(len(scopes) > 0)

    def test_get_scope_id_by_name(self):
        scope = self.__module.get_scope_id_by_name("Инфраструктура по умолчанию")
        self.assertEqual(scope, "00000000-0000-0000-0000-000000000005")

    def test_import_assets_get_groups(self):
        groups = self.__module.import_assets_get_groups()
        self.assertTrue(len(groups) > 0)

    def test_get_import_csv_metadata(self):
        metadata = self.__module.get_import_csv_metadata()
        self.assertIsInstance(metadata, list)
        for item in metadata:
            self.assertIn("propertyName", item)
            self.assertIn("propertyDeclarations", item)

    def test_get_import_csv_example(self):
        # BOM сервер отдаёт обязательно, BOM в ответе быть не должно
        bom = "\N{BYTE ORDER MARK}"
        example = self.__module.get_import_csv_example()
        self.assertFalse(example.startswith(bom))
        self.assertIn('"typealias"', example)
        self.assertIn("Fqdn", example)

    def test_get_group_input_permissions(self):
        # Служебная группа Root для ручного ввода не предназначена,
        # созданная пользователем статическая группа - предназначена.
        root_id = self.__module.get_group_id_by_name("Root")
        self.assertFalse(self.__module.get_group_input_permissions(root_id))
        _, group_id = self._create_probe_group()
        self.assertTrue(self.__module.get_group_input_permissions(group_id))

    def test_get_groups_hierarchy(self):
        hierarchy = self.__module.get_groups_hierarchy()
        self.assertTrue(isinstance(hierarchy, list) and len(hierarchy) > 0)

    def test_get_group_id_by_name(self):
        group_id = self.__module.get_group_id_by_name("Root")
        self.assertTrue(group_id is not None)

    # ------------------------------------------------------------------ #
    # импорт / выгрузка активов
    # ------------------------------------------------------------------ #
    def test_import_assets_from_csv(self):
        group_id = self.__module.get_group_id_by_name("Root")
        scope_id = "00000000-0000-0000-0000-000000000005"
        uniq = self._unique("sdk")
        csv = (
            '"typealias";"fqdn";"hostname";"ip";"mac";"isvirtual"\n'
            f'"windows";"{uniq}.test.local";"{uniq}";'
            '"::1 | 10.254.129.129 | 127.0.0.1";"00:11:22:33:44:55";"true"\n'
            '"windows";"";"xxxxxxxxxx1";'
            '"::1 | 10.254.129.130 | 127.0.0.1";"00:11:22:33:44:56";"true"\n'
        )
        status, count, log = self.__module.import_assets_from_csv(
            content=io.StringIO(csv), scope_id=scope_id, group_id=group_id
        )
        self.assertTrue(status)
        self.assertEqual(count, 1)
        self.assertTrue(len(log) > 0)
        # Импортированный актив оседает на стенде - найдём его id и уберём.
        # (Строка-дубль без fqdn не импортируется, мусором не становится.)
        token = self.__module.create_assets_request(
            pdql=f'qsearch("{uniq}") | select(Host.@id as id)',
            group_ids=[group_id],
            include_nested=True,
        )
        ids = [x.strip('"') for x in self.__module.get_assets_list_stream(token=token)][
            1:
        ]
        for asset_id in ids:
            self.addCleanup(self._delete_asset, asset_id)

    def _make_selection_token(self, pdql: str) -> tuple[str, int]:
        group_id = self.__module.get_group_id_by_name("Root")
        self.assertIsNotNone(group_id, "группа Root не найдена на стенде")
        token = self.__module.create_assets_request(
            pdql=pdql, group_ids=[group_id], include_nested=True
        )
        return token, self.__module.get_assets_request_size(token)

    def test_get_assets_list_json(self):
        token, request_size = self._make_selection_token("select(@Host as host)")
        counter = sum(
            1 for row in self.__module.get_assets_list_json(token) if len(row) != 0
        )
        self.assertEqual(counter, request_size)

    def test_get_assets_list_csv(self):
        token, request_size = self._make_selection_token("select(@Host as host)")
        counter = sum(
            1 for row in self.__module.get_assets_list_csv(token) if len(row) != 0
        )
        self.assertEqual(counter, request_size + 1)

    def test_get_assets_list_stream(self):
        token, request_size = self._make_selection_token("select(@Host as host)")
        lines = [line for line in self.__module.get_assets_list_stream(token) if line]
        # Поток = заголовок CSV + строки выборки
        self.assertEqual(len(lines), request_size + 1)
        self.assertIn("host", lines[0])

    def test_get_assets_list_stream_invalid_token(self):
        # Токен живёт на стороне Core: неизвестный -> 404, пустой -> 400
        cases = {
            "неизвестный токен": ("sdk-unknown-token", 404),
            "пустой токен": ("", 400),
        }
        for label, (token, status_code) in cases.items():
            with self.subTest(label):
                with self.assertRaises(requests.exceptions.HTTPError) as ctx:
                    list(self.__module.get_assets_list_stream(token=token))
                self.assertEqual(ctx.exception.response.status_code, status_code)

    def test_get_assets_list_pagination(self):
        # Меньший batch заставляет читать выборку несколькими страницами
        token, request_size = self._make_selection_token("select(@Host as host)")
        self.assertGreater(
            request_size, 10, "выборка слишком мала для проверки пагинации"
        )
        batch_size = self.__module.settings.assets_batch_size
        self.__module.settings.assets_batch_size = 10
        try:
            counter = sum(
                1 for row in self.__module.get_assets_list_json(token) if len(row) != 0
            )
        finally:
            self.__module.settings.assets_batch_size = batch_size
        self.assertEqual(counter, request_size)

    # ------------------------------------------------------------------ #
    # PDQL: доступные поля и типы
    # ------------------------------------------------------------------ #
    def test_get_pdql_available_fields(self):
        fields = self.__module.get_pdql_available_fields("select(@Host as host)")
        names = [field.get("name") for field in fields]
        self.assertIn("host", names)
        self.assertIn("host.Name", names)

    def test_get_pdql_available_fields_invalid_pdql(self):
        with self.assertRaises(requests.exceptions.HTTPError):
            self.__module.get_pdql_available_fields("select(@NoSuchField)")

    def test_get_pdql_available_types(self):
        roots = self.__module.get_pdql_available_types()
        nested = self.__module.get_pdql_available_types(
            type_name="Host.", selected_fields=["Host.Name"]
        )
        self.assertTrue(roots)
        self.assertTrue(nested)
        for node in roots + nested:
            self.assertIn("name", node)
            self.assertIn("type", node)
            self.assertIn("isPrimitive", node)
        self.assertNotEqual(
            {node.get("name") for node in roots},
            {node.get("name") for node in nested},
            "typeName не повлиял на выборку типов",
        )

    # ------------------------------------------------------------------ #
    # конфигурация актива
    # ------------------------------------------------------------------ #
    def test_get_asset_configuration_by_id(self):
        asset_id = self._import_probe_asset()
        self.addCleanup(self._delete_asset, asset_id)
        config = self.__module.get_asset_configuration_by_id(asset_id)
        self.assertEqual(config.get("id"), asset_id)
        self.assertIn("os", config)

    def test_get_asset_configuration_by_id_unknown(self):
        # Сервер отвечает 404/500, exec_request поднимает HTTPError
        with self.assertRaises(requests.exceptions.HTTPError):
            self.__module.get_asset_configuration_by_id(str(uuid.uuid4()))

    def test_change_asset_configuration_by_id(self):
        asset_id = self._import_probe_asset()
        self.addCleanup(self._delete_asset, asset_id)
        config = self.__module.get_asset_configuration_by_id(asset_id)
        payload = self._asset_config_payload(config)
        for prop in payload["os"]["properties"]:
            if prop["name"].endswith(".OsName"):
                prop["value"] = "sdk-probe-os"
        status = self.__module.change_asset_configuration_by_id(asset_id, payload)
        self.assertIsNotNone(status)
        self.assertTrue(status.get("isSuccessful"))
        self.assertEqual(status.get("assetId"), asset_id)
        changed = self.__module.get_asset_configuration_by_id(asset_id)
        values = [
            prop.get("value")
            for prop in changed["os"]["properties"]
            if prop["name"].endswith(".OsName")
        ]
        self.assertEqual(values, ["sdk-probe-os"])

    # ------------------------------------------------------------------ #
    # группы
    # ------------------------------------------------------------------ #
    def test_static_group(self):
        group_name, group_id = self._create_probe_group()
        resolved = self.__module.get_group_id_by_name(group_name, do_refresh=True)
        self.assertEqual(resolved, group_id)
        self.assertTrue(self.__module.delete_group(group_id))

    def test_dynamic_group(self):
        parent_id = self.__module.get_group_id_by_name("Root")
        group_name = self._unique("sdk-dyn-")
        new_group_id = self.__module.create_group_dynamic(
            parent_id=parent_id, group_name=group_name, predicate="Host"
        )
        group_id = self.__module.get_group_id_by_name(group_name, do_refresh=True)
        status = self.__module.delete_group(new_group_id)
        self.assertEqual(new_group_id, group_id)
        self.assertTrue(status)

    def test_edit_group_dynamic(self):
        _, group_id = self._create_probe_dynamic_group("Host")
        # Предикат проходит валидацию, но отличается от исходного
        edited = self.__module.edit_group_dynamic(
            group_id=group_id, predicate="Not Host"
        )
        self.assertEqual(edited, group_id)
        entry = self.__module.get_groups_list(do_refresh=True).get(group_id)
        self.assertIsNotNone(entry)
        self.assertEqual(entry.get("type"), "dynamic")
        self.assertFalse(entry.get("is_invalid_predicate"))

    def test_edit_group_dynamic_invalid_predicate(self):
        _, group_id = self._create_probe_dynamic_group("Host")
        with self.assertRaises(requests.exceptions.HTTPError):
            self.__module.edit_group_dynamic(group_id=group_id, predicate="!!!bad!!!")

    def test_get_groups_list(self):
        root_id = self.__module.get_group_id_by_name("Root")
        groups = self.__module.get_groups_list(do_refresh=True)
        self.assertIn(root_id, groups)
        root = groups[root_id]
        self.assertEqual(root.get("name"), "Root")
        self.assertIsNone(root.get("parent_id"))
        self.assertEqual(root.get("type"), "static")
        self.assertTrue(root.get("is_readonly"))

        # Результат - копия кэша: мутация не должна влиять на модуль
        groups["sdk-fake-group"] = {}
        self.assertNotIn("sdk-fake-group", self.__module.get_groups_list())

        # do_refresh=True обязан видеть изменения дерева
        group_name, group_id = self._create_probe_group()
        self.assertIn(group_id, self.__module.get_groups_list(do_refresh=True))
        self.assertEqual(self.__module.get_group_id_by_name(group_name), group_id)

    def test_get_group_id_by_name_unknown(self):
        self.assertIsNone(self.__module.get_group_id_by_name("sdk-no-such-group"))

    def test_delete_group_readonly(self):
        # Read-only служебные группы удалять нельзя - False без исключения
        self.assertFalse(
            self.__module.delete_group(self.__module.get_group_id_by_name("Root"))
        )
        self.assertIsNotNone(
            self.__module.get_group_id_by_name("Root", do_refresh=True)
        )

    def test_delete_group_unknown(self):
        with self.assertRaises(requests.exceptions.HTTPError):
            self.__module.delete_group(str(uuid.uuid4()))

    def test_validate_group_predicate(self):
        result = self.__module.validate_group_predicate("Host")
        self.assertTrue(result.get("isValid"))
        self.assertIn("isPotentiallySlow", result)

    def test_validate_group_predicate_invalid(self):
        # Невалидный предикат сервер отвечает 400, а не isValid=false
        with self.assertRaises(requests.exceptions.HTTPError):
            self.__module.validate_group_predicate("!!!bad!!!")

    # ------------------------------------------------------------------ #
    # операции над активами (самодостаточные)
    # ------------------------------------------------------------------ #
    def test_delete_assets_by_ids(self):
        asset_id = self._import_probe_asset()
        status = self.__module.delete_assets_by_ids(asset_ids=[asset_id])
        self.assertIsNotNone(status)
        self.assertEqual(status.get("succeedCount"), 1)

    def test_update_group_entries_add_remove(self):
        _, group_id = self._create_probe_group()
        asset_id = self._import_probe_asset()
        self.addCleanup(self._delete_asset, asset_id)

        add = self.__module.update_group_entries_by_ids(
            asset_ids=[asset_id], include=[group_id]
        )
        self.assertIsNotNone(add)
        self.assertEqual(add.get("succeedCount"), 1)

        remove = self.__module.update_group_entries_by_ids(
            asset_ids=[asset_id], exclude=[group_id]
        )
        self.assertIsNotNone(remove)
        self.assertEqual(remove.get("succeedCount"), 1)

    def test_get_group_assets_ids(self):
        _, group_id = self._create_probe_group()
        asset_id = self._import_probe_asset()
        self.addCleanup(self._delete_asset, asset_id)

        self.assertEqual(self.__module.get_group_assets_ids(group_id), [])

        add = self.__module.update_group_entries_by_ids(
            asset_ids=[asset_id], include=[group_id]
        )
        self.assertEqual(add.get("succeedCount"), 1)
        self.assertEqual(self.__module.get_group_assets_ids(group_id), [asset_id])

        root_id = self.__module.get_group_id_by_name("Root")
        self.assertIn(asset_id, self.__module.get_group_assets_ids(root_id, True))

    # ------------------------------------------------------------------ #
    # сохранённые запросы
    # ------------------------------------------------------------------ #
    def test_get_queries(self):
        tree = self.__module.get_queries()
        self.assertTrue(isinstance(tree, list) and len(tree) > 0)
        user_roots = [n for n in tree if n.get("type") == "user"]
        self.assertTrue(len(user_roots) > 0)

    def test_get_query_by_id(self):
        # берём стандартный запрос из дерева (без создания и очистки)
        tree = self.__module.get_queries()

        def first_leaf(nodes):
            for n in nodes:
                if not n.get("isFolder"):
                    return n["id"]
                if n.get("children"):
                    found = first_leaf(n["children"])
                    if found:
                        return found
            return None

        query_id = first_leaf(tree)
        self.assertTrue(query_id is not None)
        query = self.__module.get_query_by_id(query_id)
        self.assertEqual(query.get("id"), query_id)

    def test_query_crud(self):
        root_id = self._user_root_id()

        folder = self.__module.create_query_folder(
            parent_id=root_id, folder_name=self._unique("sdk-folder-")
        )
        folder_id = folder["id"]
        self.addCleanup(self._raw_delete, self._QUERY_FOLDERS_API, folder_id)

        query = self.__module.create_query(
            folder_id=folder_id,
            query_name=self._unique("sdk-query-"),
            pdql_filter='qsearch("sdk-probe")',
            pdql_selection="select(@Host)",
        )
        query_id = query["id"]
        # query удаляется раньше папки (addCleanup - LIFO)
        self.addCleanup(self._raw_delete, self._QUERIES_API, query_id)

        got = self.__module.get_query_by_id(query_id)
        self.assertEqual(got["id"], query_id)
        self.assertEqual(got["folderId"], folder_id)

        new_name = self._unique("sdk-query-")
        self.__module.update_query(
            query_id=query_id,
            folder_id=folder_id,
            query_name=new_name,
            pdql_filter='qsearch("sdk-probe2")',
            pdql_selection="select(@Host)",
        )
        updated = self.__module.get_query_by_id(query_id)
        self.assertEqual(updated["displayName"], new_name)
        self.assertEqual(updated["filterPdql"], 'qsearch("sdk-probe2")')


if __name__ == "__main__":
    unittest.main()
