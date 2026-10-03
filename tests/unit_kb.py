import itertools
import os
import time
import unittest
from tempfile import TemporaryDirectory
from uuid import UUID

from helpers import clean_kb_trash, gen_lowercase_string, gen_uppercase_string
from settings import creds, settings

from mpsiemlib.common import *
from mpsiemlib.modules import MPSIEMWorker


class KBTestCase(unittest.TestCase):
    __mpsiemworker = None
    __module = None
    __creds = creds
    __settings = settings

    __test_co_rule = (
        'event Event:\n\tkey:\n\t\tsrc.ip\n\tfilter {\n        msgid == "4688"\n\t}\n\nrule TestRule: '
        "Event\nemit {\n\t$id = 'TestRule'\n}"
    )

    def __choose_any_db(self):
        return self.__deployable_db_name

    def __choose_deployable_db(self):
        return self.__deployable_db_name

    @classmethod
    def __pick_deployable_db_name(cls):
        for name, params in cls.__module.get_databases_list().items():
            if params.get("deployable"):
                return name
        raise unittest.SkipTest(
            "No deployable content database (deployable=true) on stand"
        )

    @classmethod
    def setUpClass(cls) -> None:
        cls.__mpsiemworker = MPSIEMWorker(cls.__creds, cls.__settings)
        cls.__module = cls.__mpsiemworker.get_module(ModuleNames.KB)
        cls.__deployable_db_name = cls.__pick_deployable_db_name()

    @classmethod
    def tearDownClass(cls) -> None:
        # Подчищаем тестовый мусор (папки/наборы/правила unittest_*), который
        # создают читающие/CRUD-тесты, не удаляющие свои объекты.
        errors = clean_kb_trash(cls.__module, cls.__deployable_db_name)
        if errors:
            print(f"clean_kb_trash errors: {errors}")
        cls.__module.close()

    def test_get_databases_list(self):
        ret = self.__module.get_databases_list()
        self.assertGreater(len(ret), 0)

    def test_get_groups_list(self):
        db_name = self.__choose_any_db()
        ret = self.__module.get_groups_list(db_name)
        self.assertGreater(len(ret), 0)

    def test_get_folders_list(self):
        db_name = self.__choose_deployable_db()
        ret = self.__module.get_folders_list(db_name)
        self.assertGreater(len(ret), 0)

    def test_get_packs_list(self):
        db_name = self.__choose_deployable_db()
        ret = self.__module.get_packs_list(db_name)
        self.assertTrue(ret is not None)

    def test_get_all_objects(self):
        db_name = self.__choose_any_db()
        norm = []
        for i in self.__module.get_normalizations_list(db_name):
            norm.append(i)
        corr = []
        for i in self.__module.get_correlations_list(db_name):
            corr.append(i)
        agg = []
        for i in self.__module.get_aggregations_list(db_name):
            agg.append(i)
        enrich = []
        for i in self.__module.get_enrichments_list(db_name):
            enrich.append(i)
        tbls = []
        for i in self.__module.get_tables_list(db_name):
            tbls.append(i)

        self.assertTrue(
            (len(norm) != 0)
            and (len(corr) != 0)
            and (len(agg) != 0)
            and (len(enrich) != 0)
            and (len(tbls) != 0)
        )

    def test_get_object_id_by_name(self):
        db_name = self.__choose_any_db()

        norm = next(self.__module.get_normalizations_list(db_name))
        object_name = norm.get("name")
        object_id = norm.get("id")

        calc_ids = self.__module.get_id_by_name(
            db_name, MPContentTypes.NORMALIZATION, object_name
        )

        found = False
        for i in calc_ids:
            if i.get("id") == object_id:
                found = True

        self.assertTrue(found)

    def test_get_rule(self):
        db_name = self.__choose_any_db()

        rule_info = next(self.__module.get_normalizations_list(db_name))
        rule_id = rule_info.get("id")
        norm_rule = self.__module.get_rule(
            db_name, MPContentTypes.NORMALIZATION, rule_id
        )

        rule_info = next(self.__module.get_correlations_list(db_name))
        rule_id = rule_info.get("id")
        corr_rule = self.__module.get_rule(db_name, MPContentTypes.CORRELATION, rule_id)

        rule_info = next(self.__module.get_aggregations_list(db_name))
        rule_id = rule_info.get("id")
        agg_rule = self.__module.get_rule(db_name, MPContentTypes.AGGREGATION, rule_id)

        rule_info = next(self.__module.get_enrichments_list(db_name))
        rule_id = rule_info.get("id")
        enrich_rule = self.__module.get_rule(
            db_name, MPContentTypes.ENRICHMENT, rule_id
        )

        self.assertTrue(
            (len(norm_rule) != 0)
            and (len(corr_rule) != 0)
            and (len(agg_rule) != 0)
            and (len(enrich_rule) != 0)
        )

    def test_get_table_info(self):
        db_name = self.__choose_any_db()

        tbl = next(self.__module.get_tables_list(db_name))
        tbl_id = tbl.get("id")

        ret = self.__module.get_table_info(db_name, tbl_id)

        self.assertGreater(len(ret), 0)

    def test_get_table_data(self):
        db_name = self.__choose_any_db()

        tbl = next(self.__module.get_tables_list(db_name))
        tbl_id = tbl.get("id")

        ret = self.__module.get_table_data(db_name, tbl_id)

        self.assertTrue(ret is not None)

    @unittest.skip(
        "Skip for development testing: deploy in 26.0 installs all "
        "pending content and takes tens of minutes"
    )
    def test_deploy(self):
        db_name = self.__choose_deployable_db()

        norm_rule = None
        for i in self.__module.get_normalizations_list(
            db_name, filters={"filters": {"DeploymentStatus": ["1"]}}
        ):
            if i.get("deployment_status") == "notinstalled":
                norm_rule = i
                break
        if norm_rule is None:
            self.skipTest("No notinstalled normalization rule in db")

        # MP SIEM 26.0: установка полная (compile + deploy всего pending-контента),
        # per-object install/uninstall эндпоинтов нет
        self.__module.compile_objects_sync(
            db_name, MPContentTypes.NORMALIZATION, [norm_rule.get("id")]
        )
        deploy_ids = self.__module.deploy(db_name)
        self.assertGreater(len(deploy_ids), 0)

        success_install = False
        for i in range(30):
            time.sleep(10)
            deploy_status = self.__module.get_deploy_status(db_name, deploy_ids[0])
            if deploy_status.get("deployment_status") == "succeeded":
                success_install = True
                break

        content_item = self.__module.get_content_item(
            db_name, norm_rule.get("id"), "NormalizationRule"
        )
        self.assertTrue(success_install)
        self.assertEqual(
            "installed", str(content_item.get("GeneralDeploymentStatus", "")).lower()
        )

    @unittest.skip(
        "Skip for development testing: 26.0 install is a full "
        "pending redeploy of the whole DB and takes tens of minutes; "
        "leaves the pipeline reflecting removal only after next deploy"
    )
    def test_install_sync(self):
        db_name = self.__choose_deployable_db()

        folder_name = gen_uppercase_string(12)  # случайное имя
        new_folder_id_str = self.__module.create_folder(db_name, folder_name, None)

        rule_name = gen_lowercase_string(20)  # случайное имя
        code = self.__test_co_rule

        group_name = gen_uppercase_string(12)  # случайное имя
        new_group_id_str = self.__module.create_group(db_name, group_name)

        groups = [
            new_group_id_str,
        ]

        new_rule_id_str = self.__module.create_co_rule(
            db_name, rule_name, code, "Descr", new_folder_id_str, group_ids=groups
        )

        install_set_id = self.__module.get_install_deployment_set_id(db_name)
        try:
            # MP SIEM 26.0: install_content = link to install set -> compile -> deploy
            self.__module.install_content(
                db_name,
                MPContentTypes.CORRELATION,
                [
                    new_rule_id_str,
                ],
            )

            content_item = self.__module.get_content_item(
                db_name, new_rule_id_str, "CorrelationRule"
            )
            self.assertEqual(
                "installed",
                str(content_item.get("GeneralDeploymentStatus", "")).lower(),
            )
        finally:
            # отвязать от install-набора и удалить правило: конвейер отразит
            # удаление на следующем deploy
            self.__module.link_content_to_groups(
                db_name, [new_rule_id_str], [], remove_group_ids=[install_set_id]
            )
            self.__module.delete_content_rule(
                db_name, MPContentTypes.CORRELATION, new_rule_id_str
            )

    @unittest.skip(
        "Not Implemented: KB 26.0 не имеет per-group установки, "
        "deploy ставит весь pending-контент БД"
    )
    def test_deploy_group(self):
        self.assertTrue(False)

    # @unittest.skip("Skip for development testing")
    def test_start_stop_rule(self):
        db_name = self.__choose_deployable_db()
        rule = next(
            self.__module.get_correlations_list(
                db_name, filters={"filters": {"DeploymentStatus": ["1"]}}
            )
        )
        rule_id = rule.get("id")
        self.__module.stop_rule(db_name, MPContentTypes.CORRELATION, [rule_id])
        is_stopped = (
            self.__module.get_rule_running_state(
                db_name, MPContentTypes.CORRELATION, rule_id
            ).get("state")
            == "stopped"
        )
        self.__module.start_rule(db_name, MPContentTypes.CORRELATION, [rule_id])
        is_running = (
            self.__module.get_rule_running_state(
                db_name, MPContentTypes.CORRELATION, rule_id
            ).get("state")
            == "running"
        )

        self.assertTrue(is_stopped and is_running)

    def test_create_root_folder(self):
        db_name = self.__choose_deployable_db()
        folder_name = gen_uppercase_string(12)  # случайное имя
        new_folder_id_str = self.__module.create_folder(db_name, folder_name, None)
        try:
            folder_id = UUID(new_folder_id_str)
        except ValueError:
            folder_id = "Bad value"

        self.assertEqual(new_folder_id_str, str(folder_id))

    def test_delete_folder(self):
        db_name = self.__choose_deployable_db()
        folder_name = gen_uppercase_string(12)  # случайное имя
        new_folder_id_str = self.__module.create_folder(db_name, folder_name, None)

        retval = self.__module.delete_folder(db_name, new_folder_id_str)
        self.assertEqual(204, retval.status_code)

    def test_create_co_rule(self):
        db_name = self.__choose_deployable_db()
        folder_name = gen_uppercase_string(12)  # случайное имя
        new_folder_id_str = self.__module.create_folder(db_name, folder_name, None)

        rule_name = gen_lowercase_string(20)  # случайное имя
        code = self.__test_co_rule

        new_rule_id_str = self.__module.create_co_rule(
            db_name, rule_name, code, "Descr", new_folder_id_str
        )

        try:
            rule_id = UUID(new_rule_id_str)
        except ValueError:
            rule_id = "Bad value"

        self.assertEqual(new_rule_id_str, str(rule_id))

    def test_create_co_rule_with_group(self):
        db_name = self.__choose_deployable_db()
        folder_name = gen_uppercase_string(12)  # случайное имя
        new_folder_id_str = self.__module.create_folder(db_name, folder_name, None)

        rule_name = gen_lowercase_string(20)  # случайное имя
        code = self.__test_co_rule

        group_name = gen_uppercase_string(12)  # случайное имя
        new_group_id_str = self.__module.create_group(db_name, group_name)

        groups = [
            new_group_id_str,
        ]

        new_rule_id_str = self.__module.create_co_rule(
            db_name, rule_name, code, "Descr", new_folder_id_str, group_ids=groups
        )

        try:
            rule_id = UUID(new_rule_id_str)
        except ValueError:
            rule_id = "Bad value"

        self.assertEqual(new_rule_id_str, str(rule_id))

    def test_delete_co_rule(self):
        db_name = self.__choose_deployable_db()
        folder_name = gen_uppercase_string(12)  # случайное имя
        new_folder_id_str = self.__module.create_folder(db_name, folder_name, None)

        rule_name = gen_lowercase_string(20)  # случайное имя
        code = self.__test_co_rule

        new_rule_id_str = self.__module.create_co_rule(
            db_name, rule_name, code, "Descr", new_folder_id_str
        )

        retval = self.__module.delete_content_item(
            db_name, new_rule_id_str, "CorrelationRule"
        )

        self.assertEqual(204, retval.status_code)

    def test_create_root_group(self):
        db_name = self.__choose_deployable_db()
        group_name = gen_uppercase_string(12)  # случайное имя
        new_group_id_str = self.__module.create_group(db_name, group_name)

        try:
            group_id = UUID(new_group_id_str)
        except ValueError:
            group_id = "Bad value"

        self.assertEqual(new_group_id_str, str(group_id))

    def test_delete_group(self):
        db_name = self.__choose_deployable_db()
        group_name = gen_uppercase_string(12)  # случайное имя
        new_group_id_str = self.__module.create_group(db_name, group_name)

        retval = self.__module.delete_group(db_name, new_group_id_str)

        self.assertEqual(204, retval.status_code)

    def test_is_group_empty(self):
        db_name = self.__choose_deployable_db()

        group_name = gen_uppercase_string(12)  # случайное имя
        new_group_id_str = self.__module.create_group(db_name, group_name)

        self.assertTrue(self.__module.is_group_empty(db_name, new_group_id_str))

    def test_export_group_kb_format(self):
        db_name = self.__choose_deployable_db()
        folder_name = gen_uppercase_string(12)  # случайное имя
        new_folder_id_str = self.__module.create_folder(db_name, folder_name, None)

        rule_name = gen_lowercase_string(20)  # случайное имя
        code = self.__test_co_rule

        group_name = gen_uppercase_string(12)  # случайное имя
        new_group_id_str = self.__module.create_group(db_name, group_name)

        groups = [
            new_group_id_str,
        ]

        new_rule_id_str = self.__module.create_co_rule(
            db_name, rule_name, code, "Descr", new_folder_id_str, group_ids=groups
        )
        self.assertIsNotNone(new_rule_id_str)

        filename = gen_lowercase_string(20) + ".kb"
        with TemporaryDirectory() as tmp_dir_name:
            filepath = os.path.join(tmp_dir_name, filename)

            exported_bytes = self.__module.export_group(
                db_name, new_group_id_str, filepath
            )
            self.assertGreater(exported_bytes, 0)

    def test_export_group_siem_format(self):
        db_name = self.__choose_deployable_db()
        folder_name = gen_uppercase_string(12)  # случайное имя
        new_folder_id_str = self.__module.create_folder(db_name, folder_name, None)

        rule_name = gen_lowercase_string(20)  # случайное имя
        code = self.__test_co_rule

        group_name = gen_uppercase_string(12)  # случайное имя
        new_group_id_str = self.__module.create_group(db_name, group_name)

        groups = [
            new_group_id_str,
        ]

        new_rule_id_str = self.__module.create_co_rule(
            db_name, rule_name, code, "Descr", new_folder_id_str, group_ids=groups
        )
        self.assertIsNotNone(new_rule_id_str)

        filename = gen_lowercase_string(20) + ".zip"
        with TemporaryDirectory() as tmp_dir_name:
            filepath = os.path.join(tmp_dir_name, filename)
            exported_bytes = self.__module.export_group(
                db_name,
                new_group_id_str,
                filepath,
                export_format=self.__module.EXPORT_FORMAT_SIEM_LITE,
            )
            self.assertGreater(exported_bytes, 0)

    def test_create_group_path(self):
        db_name = self.__choose_deployable_db()

        root_group_name = gen_uppercase_string(12)
        child_group_name = gen_uppercase_string(12)
        grandchild_group_name = gen_uppercase_string(12)

        path = "/".join((root_group_name, child_group_name, grandchild_group_name))

        self.__module.create_group_path(db_name, path)
        group_id = self.__module.get_group_id_by_path(db_name, path)

        self.assertNotEqual(group_id, "")

    def test_import_group_add_and_update(self):
        db_name = self.__choose_deployable_db()
        status_code = self.__module.import_group(db_name, "test.kb")
        self.assertEqual(status_code, 201)

    def test_get_group_path_by_id(self):
        db_name = self.__choose_deployable_db()

        root_group_name = gen_uppercase_string(12)
        root_group_id_str = self.__module.create_group(db_name, root_group_name)

        child_group_name = gen_uppercase_string(12)
        child_group_id_str = self.__module.create_group(
            db_name, child_group_name, root_group_id_str
        )

        self.assertEqual(
            self.__module.get_group_path_by_id(db_name, child_group_id_str),
            "/".join((root_group_name, child_group_name)),
        )

    def test_get_group_id_by_path(self):
        db_name = self.__choose_deployable_db()
        folder_name = gen_uppercase_string(12)  # случайное имя
        new_folder_id_str = self.__module.create_folder(db_name, folder_name, None)

        rule_name = gen_lowercase_string(20)  # случайное имя
        code = self.__test_co_rule

        group_name = gen_uppercase_string(12)  # случайное имя
        group_id_str = self.__module.create_group(db_name, group_name)

        nested_group_name = gen_uppercase_string(12)
        nested_group_id_str = self.__module.create_group(
            db_name, nested_group_name, group_id_str
        )

        groups = [
            nested_group_id_str,
        ]

        new_rule_id_str = self.__module.create_co_rule(
            db_name, rule_name, code, "Descr", new_folder_id_str, group_ids=groups
        )
        self.assertIsNotNone(new_rule_id_str)

        search_str = "/".join((group_name, nested_group_name))
        self.assertEqual(
            self.__module.get_group_id_by_path(db_name, search_str), nested_group_id_str
        )

    def test_get_nested_group_ids(self):
        db_name = self.__choose_deployable_db()

        root_group_name = gen_uppercase_string(12)
        root_group_id_str = self.__module.create_group(db_name, root_group_name)

        child_group_name = gen_uppercase_string(12)
        child_group_id_str = self.__module.create_group(
            db_name, child_group_name, root_group_id_str
        )

        grandchild_group_name = gen_uppercase_string(12)
        grandchild_group_id_str = self.__module.create_group(
            db_name, grandchild_group_name, child_group_id_str
        )

        self.assertListEqual(
            self.__module.get_nested_group_ids(db_name, root_group_id_str),
            [child_group_id_str, grandchild_group_id_str],
        )

    def test_link_content_to_groups(self):
        db_name = self.__choose_deployable_db()

        root_group_name = gen_uppercase_string(12)
        root_group_id_str = self.__module.create_group(db_name, root_group_name)

        child_group_name = gen_uppercase_string(12)
        child_group_id_str = self.__module.create_group(
            db_name, child_group_name, root_group_id_str
        )

        second_group_name = gen_uppercase_string(12)
        second_group_id_str = self.__module.create_group(db_name, second_group_name)

        folder_name = gen_uppercase_string(12)  # случайное имя
        new_folder_id_str = self.__module.create_folder(db_name, folder_name, None)

        rule_name = gen_lowercase_string(20)  # случайное имя
        code = self.__test_co_rule
        new_rule_id_str = self.__module.create_co_rule(
            db_name, rule_name, code, "Descr", new_folder_id_str
        )

        self.__module.link_content_to_groups(
            db_name,
            [
                new_rule_id_str,
            ],
            [child_group_id_str, second_group_id_str],
        )

        group_ids = self.__module.get_linked_groups(db_name, new_rule_id_str)

        self.assertCountEqual(group_ids, [child_group_id_str, second_group_id_str])

    def test_get_folder_path_by_id(self):
        db_name = self.__choose_deployable_db()
        root_folder_name = gen_uppercase_string(12)  # случайное имя
        parent_folder_id = self.__module.create_folder(db_name, root_folder_name, None)
        nested_folder_name = gen_uppercase_string(12)  # случайное имя
        nested_folder_id = self.__module.create_folder(
            db_name, nested_folder_name, parent_folder_id
        )
        self.assertEqual(
            "/".join((root_folder_name, nested_folder_name)),
            self.__module.get_folder_path_by_id(db_name, nested_folder_id),
        )

    def test_get_folder_id_by_path(self):
        db_name = self.__choose_deployable_db()
        root_folder_name = gen_uppercase_string(12)  # случайное имя
        parent_folder_id = self.__module.create_folder(db_name, root_folder_name, None)
        nested_folder_name = gen_uppercase_string(12)  # случайное имя
        nested_folder_id = self.__module.create_folder(
            db_name, nested_folder_name, parent_folder_id
        )
        self.assertEqual(
            nested_folder_id,
            self.__module.get_folder_id_by_path(
                db_name, "/".join((root_folder_name, nested_folder_name))
            ),
        )

    def test_get_nested_folder_ids_by_folder_id(self):
        db_name = self.__choose_deployable_db()

        root_folder_name = gen_uppercase_string(12)  # случайное имя
        parent_folder_id = self.__module.create_folder(db_name, root_folder_name, None)
        nested_folder_name = gen_uppercase_string(12)  # случайное имя
        nested_folder_id = self.__module.create_folder(
            db_name, nested_folder_name, parent_folder_id
        )
        nested_folder_name2 = gen_uppercase_string(12)  # случайное имя
        nested_folder_id2 = self.__module.create_folder(
            db_name, nested_folder_name2, parent_folder_id
        )

        nested_ids = self.__module.get_nested_folder_ids_by_folder_id(
            db_name, parent_folder_id
        )
        self.assertCountEqual([nested_folder_id, nested_folder_id2], nested_ids)

    def test_get_content_data_by_folder_id(self):
        db_name = self.__choose_deployable_db()

        root_folder_name = gen_uppercase_string(12)  # случайное имя
        parent_folder_id = self.__module.create_folder(db_name, root_folder_name, None)
        nested_folder_name = gen_uppercase_string(12)  # случайное имя
        nested_folder_id = self.__module.create_folder(
            db_name, nested_folder_name, parent_folder_id
        )

        rule_name1 = gen_lowercase_string(20)  # случайное имя
        code1 = self.__test_co_rule
        rule_id_str1 = self.__module.create_co_rule(
            db_name, rule_name1, code1, "Descr", parent_folder_id
        )

        rule_name2 = gen_lowercase_string(20)  # случайное имя
        code2 = self.__test_co_rule
        rule_id_str2 = self.__module.create_co_rule(
            db_name, rule_name2, code2, "Descr", nested_folder_id
        )
        self.assertIsNotNone(rule_id_str2)

        nested_ids = self.__module.get_content_data_by_folder_id(
            db_name, parent_folder_id
        )
        self.assertDictEqual({rule_id_str1: "CorrelationRule"}, nested_ids)

    def test_move_folder(self):
        db_name = self.__choose_deployable_db()

        src_folder_name = gen_uppercase_string(12)  # случайное имя
        src_folder_id = self.__module.create_folder(db_name, src_folder_name, None)
        nested_folder_name = gen_uppercase_string(12)  # случайное имя
        nested_folder_id = self.__module.create_folder(
            db_name, nested_folder_name, src_folder_id
        )
        dst_folder_name = gen_uppercase_string(12)  # случайное имя
        dst_folder_id = self.__module.create_folder(db_name, dst_folder_name, None)

        self.__module.move_folder(db_name, nested_folder_id, dst_folder_id)
        self.__module.get_folders_list(db_name, do_refresh=True)
        self.assertEqual(
            "/".join((dst_folder_name, nested_folder_name)),
            self.__module.get_folder_path_by_id(db_name, nested_folder_id),
        )

    def test_get_content_item(self):
        db_name = self.__choose_deployable_db()
        rule_name = gen_lowercase_string(20)  # случайное имя
        code = self.__test_co_rule
        new_rule_id_str = self.__module.create_co_rule(
            db_name, rule_name, code, "Descr", None
        )
        rule_data = self.__module.get_content_item(
            db_name, new_rule_id_str, "CorrelationRule"
        )
        self.assertEqual(code, rule_data.get("Formula"))

    def test_move_co_rule(self):
        db_name = self.__choose_deployable_db()

        src_folder_name = gen_uppercase_string(12)  # случайное имя
        src_folder_id = self.__module.create_folder(db_name, src_folder_name, None)

        dst_folder_name = gen_uppercase_string(12)  # случайное имя
        dst_folder_id = self.__module.create_folder(db_name, dst_folder_name, None)

        rule_name = gen_lowercase_string(20)  # случайное имя
        code = self.__test_co_rule
        new_rule_id_str = self.__module.create_co_rule(
            db_name, rule_name, code, "Descr", src_folder_id
        )

        self.__module.move_content_item(
            db_name, new_rule_id_str, "CorrelationRule", dst_folder_id
        )
        rule_data = self.__module.get_content_item(
            db_name, new_rule_id_str, "CorrelationRule"
        )

        self.assertEqual(dst_folder_id, rule_data.get("Folder").get("Id"))

    def test_move_folder_content(self):
        db_name = self.__choose_deployable_db()

        src_folder_name = gen_uppercase_string(12)  # случайное имя
        src_folder_id = self.__module.create_folder(db_name, src_folder_name, None)

        dst_folder_name = gen_uppercase_string(12)  # случайное имя
        dst_folder_id = self.__module.create_folder(db_name, dst_folder_name, None)

        child_folder_name = gen_uppercase_string(12)  # случайное имя
        child_folder_id = self.__module.create_folder(
            db_name, child_folder_name, src_folder_id
        )

        rule_name = gen_lowercase_string(20)  # случайное имя
        code = self.__test_co_rule
        new_rule_id_str = self.__module.create_co_rule(
            db_name, rule_name, code, "Descr", src_folder_id
        )

        self.__module.move_folder_content(db_name, src_folder_name, dst_folder_name)
        rule_data = self.__module.get_content_item(
            db_name, new_rule_id_str, "CorrelationRule"
        )

        self.assertEqual(dst_folder_id, rule_data.get("Folder").get("Id"))
        self.assertEqual(
            "/".join((dst_folder_name, child_folder_name)),
            self.__module.get_folder_path_by_id(db_name, child_folder_id),
        )

    def test_get_content_items_by_group_id(self):
        db_name = self.__choose_deployable_db()

        root_group_name = gen_uppercase_string(12)
        root_group_id = self.__module.create_group(db_name, root_group_name)

        # KB 26.0 не отдаёт состав набора установки (см. докстринг метода)
        with self.assertRaises(NotImplementedError):
            self.__module.get_content_items_by_group_id(
                db_name, root_group_id, recursive=True
            )

    def test_get_pipelines_list(self):
        db_name = self.__choose_deployable_db()

        pipelines = self.__module.get_pipelines_list(db_name)

        self.assertGreater(len(pipelines), 0)
        for pipeline in pipelines:
            self.assertIsNotNone(pipeline.get("id"))
            self.assertIsNotNone(pipeline.get("url"))

    def test_get_origins_list_and_origin(self):
        db_name = self.__choose_deployable_db()

        origins = self.__module.get_origins_list(db_name)
        self.assertGreater(len(origins), 0)

        origin = origins[0]
        by_id = self.__module.get_origin(db_name, origin_id=origin.get("id"))
        self.assertEqual(origin.get("id"), by_id.get("id"))

        by_name = self.__module.get_origin(
            db_name, system_name=origin.get("system_name")
        )
        self.assertEqual(origin.get("id"), by_name.get("id"))

        with self.assertRaises(ValueError):
            self.__module.get_origin(db_name)

    def test_get_siem_settings_info(self):
        db_name = self.__choose_deployable_db()

        info = self.__module.get_siem_settings_info(db_name)

        self.assertTrue(info.get("sdk_version"))

    def test_get_rule_localizations_and_localization(self):
        db_name = self.__choose_deployable_db()

        localizations = self.__module.get_rule_localizations(db_name)
        self.assertGreater(len(localizations), 0)

        localization = self.__module.get_rule_localization(
            db_name, localizations[0].get("id")
        )
        self.assertEqual(localizations[0].get("id"), localization.get("id"))

    def test_get_rule_classes_and_rule_class(self):
        db_name = self.__choose_deployable_db()

        classes = self.__module.get_rule_classes(db_name)
        self.assertGreater(len(classes), 0)

        rule_class = self.__module.get_rule_class(db_name, classes[0].get("id"))
        self.assertEqual(classes[0].get("id"), rule_class.get("id"))
        self.assertIsNotNone(rule_class.get("system_name"))

    def test_update_rule_class(self):
        db_name = self.__choose_deployable_db()

        group_name = gen_uppercase_string(12)
        group_id = self.__module.create_group(db_name, group_name)

        new_name = gen_uppercase_string(12)
        self.__module.update_rule_class(
            db_name, group_id, {"SystemName": new_name, "ParentRuleClassId": None}
        )

        rule_class = self.__module.get_rule_class(db_name, group_id)
        self.assertEqual(new_name, rule_class.get("system_name"))

    def test_list_content_rules(self):
        db_name = self.__choose_deployable_db()

        # Aggregation - небольшой тип (десятки правил), полный список отдаётся
        # быстро. Без фильтра на Normalization (~10к правил) сервер отвечает
        # минуты - там нужен увеличенный timeout (см. list_content_rules).
        rules = self.__module.list_content_rules(db_name, MPContentTypes.AGGREGATION)
        self.assertGreater(len(rules), 0)

        studio_rule = next(self.__module.get_aggregations_list(db_name))
        contract_rule = next(
            (r for r in rules if r.get("id") == studio_rule.get("id")), {}
        )
        self.assertEqual(studio_rule.get("id"), contract_rule.get("id"))
        self.assertEqual(studio_rule.get("name"), contract_rule.get("system_name"))

        # серверный фильтр - быстрый даже на большом типе
        norm = next(self.__module.get_normalizations_list(db_name))
        filtered = self.__module.list_content_rules(
            db_name,
            MPContentTypes.NORMALIZATION,
            system_name_equals=norm.get("name"),
        )
        self.assertGreaterEqual(len(filtered), 1)
        self.assertIn(norm.get("id"), [r.get("id") for r in filtered])

        missing = self.__module.list_content_rules(
            db_name,
            MPContentTypes.NORMALIZATION,
            system_name_equals=gen_lowercase_string(30),
        )
        self.assertEqual(0, len(missing))

    def test_get_content_rule(self):
        db_name = self.__choose_deployable_db()

        studio_rule = next(self.__module.get_normalizations_list(db_name))
        rule = self.__module.get_content_rule(
            db_name, MPContentTypes.NORMALIZATION, studio_rule.get("id")
        )

        self.assertEqual(studio_rule.get("id"), rule.get("id"))
        self.assertIsNotNone(rule.get("formula"))

    def test_create_update_delete_content_rule(self):
        db_name = self.__choose_deployable_db()

        folder_name = gen_uppercase_string(12)
        folder_id = self.__module.create_folder(db_name, folder_name, None)

        rule_name = gen_lowercase_string(20)
        create_body = {
            "SystemName": rule_name,
            "Formula": 'parser test_parser { set event.category = "other"; }',
            "FolderId": folder_id,
            "IsSuffixRule": False,
            "Locales": [{"Locale": "RUS", "Name": rule_name, "Description": "test"}],
            "RuleClassIds": [],
        }

        rule_id = self.__module.create_content_rule(
            db_name, MPContentTypes.NORMALIZATION, create_body
        )
        self.assertEqual(str(UUID(rule_id)), rule_id)

        rule = self.__module.get_content_rule(
            db_name, MPContentTypes.NORMALIZATION, rule_id
        )
        self.assertEqual(rule_name, rule.get("system_name"))

        new_name = gen_lowercase_string(20)
        self.__module.update_content_rule(
            db_name,
            MPContentTypes.NORMALIZATION,
            rule_id,
            {"SystemName": new_name, "Formula": create_body["Formula"]},
        )
        rule = self.__module.get_content_rule(
            db_name, MPContentTypes.NORMALIZATION, rule_id
        )
        self.assertEqual(new_name, rule.get("system_name"))

        self.__module.delete_content_rule(
            db_name, MPContentTypes.NORMALIZATION, rule_id
        )

        remaining = self.__module.list_content_rules(
            db_name, MPContentTypes.NORMALIZATION, system_name_equals=new_name
        )
        self.assertEqual(0, len(remaining))

    def test_bulk_add_rule_class(self):
        db_name = self.__choose_deployable_db()

        folder_name = gen_uppercase_string(12)
        folder_id = self.__module.create_folder(db_name, folder_name, None)

        add_group_id = self.__module.create_group(db_name, gen_uppercase_string(12))
        remove_group_id = self.__module.create_group(db_name, gen_uppercase_string(12))

        # create_co_rule с group_ids проверяет точку #1: привязка через
        # mass-operations после создания (groupsToSave в 26.0 игнорируется)
        rule_name = gen_lowercase_string(20)
        rule_id = self.__module.create_co_rule(
            db_name,
            rule_name,
            self.__test_co_rule,
            "Descr",
            folder_id,
            group_ids=[remove_group_id],
        )
        try:
            self.assertIn(
                remove_group_id, self.__module.get_linked_groups(db_name, rule_id)
            )

            # bulk-add на 26.0 - no-op (204 без привязки): правило не должно
            # попасть в add_group_id. Если сервер починят - тест упадёт и
            # вернётся к прямому использованию bulk-add.
            self.__module.bulk_add_rule_class(db_name, add_group_id, [rule_id])
            self.assertNotIn(
                add_group_id, self.__module.get_linked_groups(db_name, rule_id)
            )

            # рабочий механизм привязки/отвязки (link + remove_group_ids)
            self.__module.link_content_to_groups(db_name, [rule_id], [add_group_id])
            self.assertIn(
                add_group_id, self.__module.get_linked_groups(db_name, rule_id)
            )

            self.__module.link_content_to_groups(
                db_name, [rule_id], [], remove_group_ids=[remove_group_id]
            )
            linked = self.__module.get_linked_groups(db_name, rule_id)
            self.assertNotIn(remove_group_id, linked)
            self.assertIn(add_group_id, linked)
        finally:
            # отвязать от оставшегося набора и удалить правило
            self.__module.link_content_to_groups(
                db_name, [rule_id], [], remove_group_ids=[add_group_id]
            )
            self.__module.delete_content_rule(
                db_name, MPContentTypes.CORRELATION, rule_id
            )

    def test_process_kb_metadata(self):
        db_name = self.__choose_deployable_db()

        folder_name = gen_uppercase_string(12)
        folder_id = self.__module.create_folder(db_name, folder_name, None)

        rule_name = gen_lowercase_string(20)
        rule_id = self.__module.create_co_rule(
            db_name, rule_name, self.__test_co_rule, "Descr", folder_id
        )

        group_path = "/".join((gen_uppercase_string(12), gen_uppercase_string(12)))
        kb_meta = {
            "group_path": group_path,
            "kb_tree": {MPContentTypes.CORRELATION: [f"{folder_name}/{rule_name}"]},
        }
        obj_map = {(MPContentTypes.CORRELATION, f"{folder_name}/{rule_name}"): rule_id}

        self.__module.process_kb_metadata(db_name, obj_map, kb_meta)

        group_id = self.__module.get_group_id_by_path(db_name, group_path)
        self.assertEqual(str(UUID(group_id)), group_id)

    # ------------------------------------------------------------------
    # Группа 1. История просмотра пакетов и справочники фильтров объектов
    # ------------------------------------------------------------------
    def test_get_object_filters_and_values(self):
        db_name = self.__choose_deployable_db()

        filters = self.__module.get_object_filters(db_name)
        self.assertGreater(len(filters), 0)
        filter_ids = {f.get("id") for f in filters}
        self.assertIn("SiemObjectType", filter_ids)

        values = self.__module.get_object_filter_values(db_name, "SiemObjectType")
        self.assertGreater(len(values), 0)
        self.assertIn("Correlation", {v.get("id") for v in values})

    def test_get_knowledge_packs_view_history(self):
        db_name = self.__choose_deployable_db()

        history = self.__module.get_knowledge_packs_view_history(db_name)
        self.assertIsInstance(history, list)
        if not history:
            self.skipTest("No knowledge-pack view history on stand")

        entry = history[0]
        self.assertIsNotNone(entry.get("object_id"))
        # Идемпотентное PUT на уже просмотренный пакет - не меняет количество
        self.__module.mark_knowledge_pack_seen(
            db_name, entry.get("object_id"), entry.get("last_seen_version_hash")
        )
        after = self.__module.get_knowledge_packs_view_history(db_name)
        self.assertEqual(len(history), len(after))

    # ------------------------------------------------------------------
    # Группа 2. Наборы установки и установка в SIEM
    # ------------------------------------------------------------------
    def test_get_deployment_sets_and_content_exists(self):
        db_name = self.__choose_deployable_db()

        sets = self.__module.get_deployment_sets(db_name)
        self.assertGreater(len(sets), 0)
        self.assertIsNotNone(sets[0].get("id"))
        self.assertIn("compilation_status", sets[0])

        install_id = self.__module.get_install_deployment_set_id(db_name)
        self.assertTrue(
            self.__module.is_deployment_set_content_exists(db_name, install_id)
        )

    def test_get_deployment_statistics(self):
        db_name = self.__choose_deployable_db()

        stats = self.__module.get_deployment_statistics(db_name)
        self.assertIsInstance(stats.get("installed"), int)
        self.assertIsInstance(stats.get("outdated"), int)
        self.assertIsInstance(stats.get("has_successful_deployments"), bool)

    def test_get_pipeline_deployment_status(self):
        db_name = self.__choose_deployable_db()

        pipelines = self.__module.get_pipelines_list(db_name)
        self.assertGreater(len(pipelines), 0)

        status = self.__module.get_pipeline_deployment_status(
            db_name, pipelines[0].get("id")
        )
        self.assertIsInstance(status.get("is_siem_online"), bool)
        self.assertIn("percentage", status)

    def test_get_rules_compilation_statuses(self):
        db_name = self.__choose_deployable_db()

        ids = [
            obj["id"]
            for obj in itertools.islice(self.__module.get_correlations_list(db_name), 3)
        ]
        self.assertGreater(len(ids), 0)

        statuses = self.__module.get_rules_compilation_statuses(db_name, ids)
        self.assertEqual(set(ids), set(statuses.keys()))
        for value in statuses.values():
            self.assertIsInstance(value, int)

        # Очередь компиляции - список (пустой, если компиляция не идёт)
        self.assertIsInstance(self.__module.get_compilation_queue(db_name), list)

    # ------------------------------------------------------------------
    # Группа 3. Формы редактирования / клонирование / переименование правил
    # ------------------------------------------------------------------
    def test_get_content_rule_edit_form(self):
        db_name = self.__choose_deployable_db()

        rule = next(self.__module.get_correlations_list(db_name))
        form = self.__module.get_content_rule_edit_form(
            db_name, MPContentTypes.CORRELATION, rule.get("id")
        )
        self.assertEqual(rule.get("id"), form.get("Id"))
        self.assertEqual(rule.get("name"), form.get("SystemName"))
        self.assertIn("SuggestedCloneName", form)

    def test_clone_content_rule(self):
        db_name = self.__choose_deployable_db()

        folder_id = self.__module.create_folder(db_name, gen_uppercase_string(12), None)
        rule_name = gen_lowercase_string(20)
        rule_id = self.__module.create_co_rule(
            db_name, rule_name, self.__test_co_rule, "Descr", folder_id
        )
        clone_id = None
        try:
            clone_id = self.__module.clone_content_rule(
                db_name, MPContentTypes.CORRELATION, rule_id
            )
            self.assertEqual(str(UUID(clone_id)), clone_id)

            clone = self.__module.get_content_item(db_name, clone_id, "CorrelationRule")
            self.assertEqual(f"{rule_name}_copy", clone.get("SystemName"))
        finally:
            if clone_id:
                self.__module.delete_content_item(db_name, clone_id, "CorrelationRule")
            self.__module.delete_content_item(db_name, rule_id, "CorrelationRule")
            self.__module.delete_folder(db_name, folder_id)

    def test_rename_content_rule(self):
        db_name = self.__choose_deployable_db()

        folder_id = self.__module.create_folder(db_name, gen_uppercase_string(12), None)
        rule_name = gen_lowercase_string(20)
        rule_id = self.__module.create_co_rule(
            db_name, rule_name, self.__test_co_rule, "Descr", folder_id
        )
        try:
            new_name = gen_lowercase_string(20)
            # preview ничего не сохраняет (rename - это превью пересборки формулы)
            preview = self.__module.preview_rename_content_rule(
                db_name,
                MPContentTypes.CORRELATION,
                rule_id,
                new_system_name=new_name,
                old_system_name=rule_name,
                formula=self.__test_co_rule,
            )
            self.assertEqual(new_name, preview.get("new_system_name"))
            self.assertEqual(
                rule_name,
                self.__module.get_content_item(db_name, rule_id, "CorrelationRule").get(
                    "SystemName"
                ),
            )

            # реальное переименование (edit-form + save)
            self.__module.rename_content_rule(
                db_name, MPContentTypes.CORRELATION, rule_id, new_name
            )
            renamed = self.__module.get_content_item(
                db_name, rule_id, "CorrelationRule"
            )
            self.assertEqual(new_name, renamed.get("SystemName"))

            content = self.__module.get_content_rule(
                db_name, MPContentTypes.CORRELATION, rule_id
            )
            self.assertEqual(new_name, content.get("system_name"))
        finally:
            self.__module.delete_content_item(db_name, rule_id, "CorrelationRule")
            self.__module.delete_folder(db_name, folder_id)

    # ------------------------------------------------------------------
    # Группа 4. Управление базами контента (read-only часть)
    # ------------------------------------------------------------------
    def test_get_content_databases_details_and_revisions(self):
        db_name = self.__choose_deployable_db()

        details = self.__module.get_content_databases_details(db_name)
        self.assertGreater(len(details), 0)
        self.assertIsNotNone(details[0].get("uid"))
        self.assertIsNotNone(details[0].get("name"))

        tree = self.__module.get_content_database_tree_details(db_name)
        self.assertIn("BranchedFromRevision", tree)

        merge = self.__module.get_content_database_merge_details(db_name)
        self.assertIn("ContentDatabase", merge)

        actions = self.__module.get_content_database_tree_actions(db_name)
        self.assertIn("CanViewDatabase", actions)

        top = self.__module.get_content_database_parent_top_revision(db_name)
        self.assertIsInstance(top, int)

        revisions = self.__module.get_content_database_revisions(db_name, 1, 5)
        self.assertGreater(len(revisions), 0)
        self.assertIn("revision", revisions[0])

    def test_get_revisions_diff(self):
        db_name = self.__choose_deployable_db()

        old_revisions = self.__module.get_content_database_revisions(db_name, 1, 50)
        if not old_revisions:
            self.skipTest("No revisions in db")
        early = max(r.get("revision") for r in old_revisions)

        # Создаём папку и правило - это порождает новые ревизии
        folder_name = gen_uppercase_string(12)
        rule_name = gen_lowercase_string(20)
        folder_id = self.__module.create_folder(db_name, folder_name, None)
        rule_id = self.__module.create_co_rule(
            db_name, rule_name, self.__test_co_rule, "Descr", folder_id
        )

        try:
            new_revisions = self.__module.get_content_database_revisions(
                db_name, early + 1, 50
            )
            late = max((r.get("revision") for r in new_revisions), default=None)
            if late is None or late <= early:
                self.skipTest("New revision did not appear after content change")

            # Порядок ревизий намеренно обратный: метод сам расставляет early/late
            revisions_diff = self.__module.get_revisions_diff(
                db_name, late, early
            )
            self.assertIsInstance(revisions_diff, dict)
            self.assertGreater(len(revisions_diff), 0)

            dumped = str(revisions_diff)
            self.assertIn(rule_name, dumped)
            self.assertIn(folder_name, dumped)

            for group_diff in revisions_diff.values():
                self.assertIsInstance(group_diff, list)
                for resource in group_diff:
                    self.assertEqual(1, len(resource))
                    for changes in resource.values():
                        self.assertIsInstance(changes, list)
                        for change in changes:
                            self.assertLessEqual(
                                {"name", "action", "old_value", "new_value"},
                                set(change),
                            )
        finally:
            self.__module.delete_content_item(db_name, rule_id, "CorrelationRule")
            self.__module.delete_folder(db_name, folder_id)

    # ------------------------------------------------------------------
    # Группа 5. Массовые операции над объектами
    # ------------------------------------------------------------------
    def test_mass_operations(self):
        db_name = self.__choose_deployable_db()

        src_folder_id = self.__module.create_folder(
            db_name, gen_uppercase_string(12), None
        )
        dst_folder_id = self.__module.create_folder(
            db_name, gen_uppercase_string(12), None
        )
        rule_id = self.__module.create_co_rule(
            db_name,
            gen_lowercase_string(20),
            self.__test_co_rule,
            "Descr",
            src_folder_id,
        )
        try:
            # check-* считают объекты, только если filter.folderId их покрывает
            deleted = self.__module.check_mass_delete(
                db_name, [rule_id], src_folder_id=src_folder_id
            )
            self.assertEqual(1, deleted.get("success"))
            self.assertEqual({"success", "user", "user_installed"}, set(deleted))

            moved = self.__module.check_mass_move(
                db_name,
                dst_folder_id,
                [rule_id],
                src_folder_id=src_folder_id,
            )
            self.assertEqual(1, moved.get("success"))

            self.__module.move_objects(
                db_name, dst_folder_id, [rule_id], src_folder_id=src_folder_id
            )
            moved_item = self.__module.get_content_item(
                db_name, rule_id, "CorrelationRule"
            )
            self.assertEqual(dst_folder_id, moved_item.get("Folder", {}).get("Id"))

            # массовое удаление (объект теперь в dst_folder_id)
            self.__module.delete_objects(
                db_name, [rule_id], src_folder_id=dst_folder_id
            )
            remaining = self.__module.list_content_rules(
                db_name,
                MPContentTypes.CORRELATION,
                system_name_equals=moved_item.get("SystemName"),
            )
            self.assertEqual(0, len(remaining))
            rule_id = None  # удалено масс-операцией
        finally:
            if rule_id:
                self.__module.delete_content_item(db_name, rule_id, "CorrelationRule")
            for folder_id in (dst_folder_id, src_folder_id):
                self.__module.delete_folder(db_name, folder_id)

    # ------------------------------------------------------------------
    # Группа 6. Прочее (настройки SIEM / поиск по короткому id / миграция)
    # ------------------------------------------------------------------
    def test_get_siem_settings_current(self):
        db_name = self.__choose_deployable_db()

        settings_ = self.__module.get_siem_settings_current(db_name)
        self.assertTrue(settings_.get("system_sdk_version"))
        self.assertIn("taxonomy_version", settings_)

    def test_get_migration_info_and_applications(self):
        db_name = self.__choose_deployable_db()

        info = self.__module.get_migration_info(db_name)
        self.assertIn("can_show", info)

        apps = self.__module.get_content_distribution_applications(db_name)
        self.assertIsInstance(apps, list)

    def test_get_object_by_short_id(self):
        db_name = self.__choose_deployable_db()

        rule = next(self.__module.get_correlations_list(db_name))
        item = self.__module.get_content_item(
            db_name, rule.get("id"), "CorrelationRule"
        )
        short_id = item.get("ObjectId")

        found = self.__module.get_object_by_short_id(db_name, short_id)
        self.assertEqual(rule.get("id"), found.get("id"))
        self.assertEqual("CorrelationRule", found.get("object_type"))


if __name__ == "__main__":
    unittest.main()
