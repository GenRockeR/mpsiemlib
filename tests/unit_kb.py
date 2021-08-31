import time
import unittest
import os

from mpsiemlib.common import *
from mpsiemlib.modules import MPSIEMWorker

from tests.settings import creds, settings

from uuid import UUID
from tempfile import TemporaryDirectory
from tests.helpers import gen_lowercase_string, gen_uppercase_string


class KBTestCase(unittest.TestCase):
    __mpsiemworker = None
    __module = None
    __creds = creds
    __settings = settings

    __test_co_rule = "event Event:\n\tkey:\n\t\tsrc.ip\n\tfilter {\n        msgid == \"4688\"\n\t}\n\nrule TestRule: Event\nemit {\n\t$id = 'TestRule'\n}"

    def __choose_any_db(self):
        dbs = self.__module.get_databases_list()
        db_name = None
        if "Editable" in dbs:
            db_name = "Editable"
        elif "dev" in dbs:
            db_name = "dev"
        else:
            db_name = next(iter(dbs))

        return db_name

    def __choose_deployable_db(self):
        db_name = None
        dbs = self.__module.get_databases_list()
        for k, v in dbs.items():
            if v.get("deployable"):
                db_name = k
                break

        return db_name

    @classmethod
    def setUpClass(cls) -> None:
        cls.__mpsiemworker = MPSIEMWorker(cls.__creds, cls.__settings)
        cls.__module = cls.__mpsiemworker.get_module(ModuleNames.KB)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.__module.close()

    def test_get_databases_list(self):
        ret = self.__module.get_databases_list()
        self.assertTrue(len(ret) != 0)

    def test_get_groups_list(self):
        db_name = self.__choose_any_db()
        ret = self.__module.get_groups_list(db_name)
        self.assertTrue(len(ret) != 0)

    def test_get_folders_list(self):
        db_name = self.__choose_deployable_db()
        ret = self.__module.get_folders_list(db_name)
        self.assertTrue(len(ret) != 0)

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
        enrch = []
        for i in self.__module.get_enrichments_list(db_name):
            enrch.append(i)
        tbls = []
        for i in self.__module.get_tables_list(db_name):
            tbls.append(i)

        self.assertTrue((len(norm) != 0) and
                        (len(corr) != 0) and
                        (len(agg) != 0) and
                        (len(enrch) != 0) and
                        (len(tbls) != 0))

    def test_get_object_id_by_name(self):
        db_name = self.__choose_any_db()

        norm = next(self.__module.get_normalizations_list(db_name))
        object_name = norm.get("name")
        object_id = norm.get("id")

        calc_ids = self.__module.get_id_by_name(db_name, MPContentTypes.NORMALIZATION, object_name)

        found = False
        for i in calc_ids:
            if i.get("id") == object_id:
                found = True

        self.assertTrue(found)

    def test_get_rule(self):
        db_name = self.__choose_any_db()

        rule_info = next(self.__module.get_normalizations_list(db_name))
        rule_id = rule_info.get("id")
        norm_rule = self.__module.get_rule(db_name, MPContentTypes.NORMALIZATION, rule_id)

        rule_info = next(self.__module.get_correlations_list(db_name))
        rule_id = rule_info.get("id")
        corr_rule = self.__module.get_rule(db_name, MPContentTypes.CORRELATION, rule_id)

        rule_info = next(self.__module.get_aggregations_list(db_name))
        rule_id = rule_info.get("id")
        agg_rule = self.__module.get_rule(db_name, MPContentTypes.AGGREGATION, rule_id)

        rule_info = next(self.__module.get_enrichments_list(db_name))
        rule_id = rule_info.get("id")
        enrch_rule = self.__module.get_rule(db_name, MPContentTypes.ENRICHMENT, rule_id)

        self.assertTrue((len(norm_rule) != 0) and
                        (len(corr_rule) != 0) and
                        (len(agg_rule) != 0) and
                        (len(enrch_rule) != 0))

    def test_get_table_info(self):
        db_name = self.__choose_any_db()

        tbl = next(self.__module.get_tables_list(db_name))
        tbl_id = tbl.get("id")

        ret = self.__module.get_table_info(db_name, tbl_id)

        self.assertTrue(len(ret) != 0)

    def test_get_table_data(self):
        db_name = self.__choose_any_db()

        tbl = next(self.__module.get_tables_list(db_name))
        tbl_id = tbl.get("id")

        ret = self.__module.get_table_data(db_name, tbl_id)

        self.assertTrue(ret is not None)

    # @unittest.skip("Skip for development testing")
    def test_deploy(self):
        db_name = self.__choose_deployable_db()

        norm_rule = None
        for i in self.__module.get_normalizations_list(db_name, filters={"filters": {"DeploymentStatus": ["1"]}}):
            if i.get("deployment_status") == "notinstalled":
                norm_rule = i
                break
        deploy_id = self.__module.install_objects(db_name, [norm_rule.get('id')])

        success_install = False
        for i in range(30):
            time.sleep(10)
            deploy_status = self.__module.get_deploy_status(db_name, deploy_id)
            if deploy_status.get("deployment_status") == "succeeded":
                success_install = True
                break

        deploy_id = self.__module.uninstall_objects(db_name, [norm_rule.get('id')])

        success_uninstall = False
        for i in range(30):
            time.sleep(10)
            deploy_status = self.__module.get_deploy_status(db_name, deploy_id)
            if deploy_status.get("deployment_status") == "succeeded":
                success_uninstall = True
                break

        self.assertTrue(success_install and success_uninstall)


    def test_install_sync(self):
        db_name = self.__choose_deployable_db()

        folder_name = gen_uppercase_string(12)  # случайное имя
        new_folder_id_str = self.__module.create_folder(db_name, folder_name, None)

        rule_name = gen_lowercase_string(20)  # случайное имя
        code = self.__test_co_rule

        group_name = gen_uppercase_string(12)  # случайное имя
        new_group_id_str = self.__module.create_group(db_name, group_name)

        groups = [new_group_id_str, ]

        new_rule_id_str = self.__module.create_co_rule(db_name, rule_name, code, 'Descr', new_folder_id_str,
                                                       group_ids=groups)


        content_items = self.__module.get_content_items_by_group_id(db_name, new_group_id_str, False)
        content_items_ids = [item['id'] for item in content_items]
        self.__module.install_objects_sync(db_name, content_items_ids)

        content_item = self.__module.get_content_items_by_group_id(db_name, new_group_id_str, False)[0]
        self.assertEqual('installed', content_item.get('deployment_status', ''))


    @unittest.skip("Not Implemented")
    def test_deploy_group(self):
        db_name = self.__choose_deployable_db()

        deploy_id = self.__module.install_objects_by_group_id(db_name, "0")

        success_install = False
        for i in range(30):
            time.sleep(10)
            deploy_status = self.__module.get_deploy_status(db_name, deploy_id)
            if deploy_status.get("deployment_status") == "succeeded":
                success_install = True
                break

        self.assertTrue(False)

    # @unittest.skip("Skip for development testing")
    def test_start_stop_rule(self):
        db_name = self.__choose_deployable_db()
        rule = next(self.__module.get_correlations_list(db_name, filters={"filters": {"DeploymentStatus": ["1"]}}))
        rule_id = rule.get("id")
        self.__module.stop_rule(db_name, MPContentTypes.CORRELATION, [rule_id])
        is_stopped = self.__module.get_rule_running_state(db_name,
                                                          MPContentTypes.CORRELATION,
                                                          rule_id).get("state") == "stopped"
        self.__module.start_rule(db_name, MPContentTypes.CORRELATION, [rule_id])
        is_running = self.__module.get_rule_running_state(db_name,
                                                          MPContentTypes.CORRELATION,
                                                          rule_id).get("state") == "running"

        self.assertTrue(is_stopped and is_running)

    def test_create_root_folder(self):
        db_name = self.__choose_deployable_db()
        folder_name = gen_uppercase_string(12)  # случайное имя
        new_folder_id_str = self.__module.create_folder(db_name, folder_name, None)
        try:
            folder_id = UUID(new_folder_id_str)
        except ValueError:
            folder_id = 'Bad value'

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

        new_rule_id_str = self.__module.create_co_rule(db_name, rule_name, code, 'Descr', new_folder_id_str)

        try:
            rule_id = UUID(new_rule_id_str)
        except ValueError:
            rule_id = 'Bad value'

        self.assertEqual(new_rule_id_str, str(rule_id))

    def test_create_co_rule_with_group(self):
        db_name = self.__choose_deployable_db()
        folder_name = gen_uppercase_string(12)  # случайное имя
        new_folder_id_str = self.__module.create_folder(db_name, folder_name, None)

        rule_name = gen_lowercase_string(20)  # случайное имя
        code = self.__test_co_rule

        group_name = gen_uppercase_string(12)  # случайное имя
        new_group_id_str = self.__module.create_group(db_name, group_name)

        groups = [new_group_id_str, ]

        new_rule_id_str = self.__module.create_co_rule(db_name, rule_name, code, 'Descr', new_folder_id_str,
                                                       group_ids=groups)

        try:
            rule_id = UUID(new_rule_id_str)
        except ValueError:
            rule_id = 'Bad value'

        self.assertEqual(new_rule_id_str, str(rule_id))

    def test_delete_co_rule(self):
        db_name = self.__choose_deployable_db()
        folder_name = gen_uppercase_string(12)  # случайное имя
        new_folder_id_str = self.__module.create_folder(db_name, folder_name, None)

        rule_name = gen_lowercase_string(20)  # случайное имя
        code = self.__test_co_rule

        new_rule_id_str = self.__module.create_co_rule(db_name, rule_name, code, 'Descr', new_folder_id_str)

        retval = self.__module.delete_content_item(db_name, new_rule_id_str, 'CorrelationRule')

        self.assertEqual(204, retval.status_code)

    def test_create_root_group(self):
        db_name = self.__choose_deployable_db()
        group_name = gen_uppercase_string(12)  # случайное имя
        new_group_id_str = self.__module.create_group(db_name, group_name)

        try:
            group_id = UUID(new_group_id_str)
        except ValueError:
            group_id = 'Bad value'

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

        self.assertTrue(
            self.__module.is_group_empty(db_name, new_group_id_str)
        )

    def test_export_group_kb_format(self):
        db_name = self.__choose_deployable_db()
        folder_name = gen_uppercase_string(12)  # случайное имя
        new_folder_id_str = self.__module.create_folder(db_name, folder_name, None)

        rule_name = gen_lowercase_string(20)  # случайное имя
        code = self.__test_co_rule

        group_name = gen_uppercase_string(12)  # случайное имя
        new_group_id_str = self.__module.create_group(db_name, group_name)

        groups = [new_group_id_str, ]

        new_rule_id_str = self.__module.create_co_rule(db_name, rule_name, code, 'Descr', new_folder_id_str,
                                                       group_ids=groups)

        filename = gen_lowercase_string(20) + '.kb'
        with TemporaryDirectory() as tmpdirname:
            filepath = os.path.join(tmpdirname, filename)

            bytes = self.__module.export_group(db_name, new_group_id_str, filepath)
            self.assertGreater(bytes, 0)

    def test_export_group_siem_format(self):
        db_name = self.__choose_deployable_db()
        folder_name = gen_uppercase_string(12)  # случайное имя
        new_folder_id_str = self.__module.create_folder(db_name, folder_name, None)

        rule_name = gen_lowercase_string(20)  # случайное имя
        code = self.__test_co_rule

        group_name = gen_uppercase_string(12)  # случайное имя
        new_group_id_str = self.__module.create_group(db_name, group_name)

        groups = [new_group_id_str, ]

        new_rule_id_str = self.__module.create_co_rule(db_name, rule_name, code, 'Descr', new_folder_id_str,
                                                       group_ids=groups)

        filename = gen_lowercase_string(20) + '.zip'
        with TemporaryDirectory() as tmpdirname:
            filepath = os.path.join(tmpdirname, filename)
            bytes = self.__module.export_group(db_name, new_group_id_str, filepath,
                                               export_format=self.__module.EXPORT_FORMAT_SIEM_LITE)
            self.assertGreater(bytes, 0)

    def test_create_group_path(self):
        db_name = self.__choose_deployable_db()

        root_group_name = gen_uppercase_string(12)
        child_group_name = gen_uppercase_string(12)
        grandchild_group_name = gen_uppercase_string(12)

        path = '/'.join((root_group_name, child_group_name, grandchild_group_name))

        self.__module.create_group_path(db_name, path)
        group_id = self.__module.get_group_id_by_path(db_name, path)

        self.assertNotEqual(group_id, '')

    def test_import_group_add_and_update(self):
        db_name = self.__choose_deployable_db()
        status_code = self.__module.import_group(db_name, 'test.kb')
        self.assertEqual(status_code, 201)

    def test_get_group_path_by_id(self):
        db_name = self.__choose_deployable_db()

        root_group_name = gen_uppercase_string(12)
        root_group_id_str = self.__module.create_group(db_name, root_group_name)

        child_group_name = gen_uppercase_string(12)
        child_group_id_str = self.__module.create_group(db_name, child_group_name, root_group_id_str)

        self.assertEqual(
            self.__module.get_group_path_by_id(db_name, child_group_id_str),
            '/'.join((root_group_name, child_group_name))
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
        nested_group_id_str = self.__module.create_group(db_name, nested_group_name, group_id_str)

        groups = [nested_group_id_str, ]

        new_rule_id_str = self.__module.create_co_rule(db_name, rule_name, code, 'Descr', new_folder_id_str,
                                                       group_ids=groups)

        search_str = '/'.join((group_name, nested_group_name))
        self.assertEqual(self.__module.get_group_id_by_path(db_name, search_str), nested_group_id_str)

    def test_get_nested_group_ids(self):
        db_name = self.__choose_deployable_db()

        root_group_name = gen_uppercase_string(12)
        root_group_id_str = self.__module.create_group(db_name, root_group_name)

        child_group_name = gen_uppercase_string(12)
        child_group_id_str = self.__module.create_group(db_name, child_group_name, root_group_id_str)

        grandchild_group_name = gen_uppercase_string(12)
        grandchild_group_id_str = self.__module.create_group(db_name, grandchild_group_name, child_group_id_str)

        self.assertListEqual(self.__module.get_nested_group_ids(db_name, root_group_id_str),
                             [child_group_id_str, grandchild_group_id_str])

    def test_link_content_to_groups(self):
        db_name = self.__choose_deployable_db()

        root_group_name = gen_uppercase_string(12)
        root_group_id_str = self.__module.create_group(db_name, root_group_name)

        child_group_name = gen_uppercase_string(12)
        child_group_id_str = self.__module.create_group(db_name, child_group_name, root_group_id_str)

        second_group_name = gen_uppercase_string(12)
        second_group_id_str = self.__module.create_group(db_name, second_group_name)

        folder_name = gen_uppercase_string(12)  # случайное имя
        new_folder_id_str = self.__module.create_folder(db_name, folder_name, None)

        rule_name = gen_lowercase_string(20)  # случайное имя
        code = self.__test_co_rule
        new_rule_id_str = self.__module.create_co_rule(db_name, rule_name, code, 'Descr', new_folder_id_str)

        self.__module.link_content_to_groups(db_name,
                                             [new_rule_id_str, ],
                                             [child_group_id_str, second_group_id_str]
                                             )

        group_ids = self.__module.get_linked_groups(db_name, new_rule_id_str)

        self.assertCountEqual(group_ids, [child_group_id_str, second_group_id_str])

    def test_get_folder_path_by_id(self):
        db_name = self.__choose_deployable_db()
        root_folder_name = gen_uppercase_string(12)  # случайное имя
        parent_folder_id = self.__module.create_folder(db_name, root_folder_name, None)
        nested_folder_name = gen_uppercase_string(12)  # случайное имя
        nested_folder_id = self.__module.create_folder(db_name, nested_folder_name, parent_folder_id)
        self.assertEqual('/'.join((root_folder_name, nested_folder_name)),
                         self.__module.get_folder_path_by_id(db_name, nested_folder_id)
                         )

    def test_get_folder_id_by_path(self):
        db_name = self.__choose_deployable_db()
        root_folder_name = gen_uppercase_string(12)  # случайное имя
        parent_folder_id = self.__module.create_folder(db_name, root_folder_name, None)
        nested_folder_name = gen_uppercase_string(12)  # случайное имя
        nested_folder_id = self.__module.create_folder(db_name, nested_folder_name, parent_folder_id)
        self.assertEqual(nested_folder_id, self.__module.get_folder_id_by_path(db_name,
                                                                               '/'.join((
                                                                                   root_folder_name,
                                                                                   nested_folder_name
                                                                               ))))

    def test_get_nested_folder_ids_by_folder_id(self):
        db_name = self.__choose_deployable_db()

        root_folder_name = gen_uppercase_string(12)  # случайное имя
        parent_folder_id = self.__module.create_folder(db_name, root_folder_name, None)
        nested_folder_name = gen_uppercase_string(12)  # случайное имя
        nested_folder_id = self.__module.create_folder(db_name, nested_folder_name, parent_folder_id)
        nested_folder_name2 = gen_uppercase_string(12)  # случайное имя
        nested_folder_id2 = self.__module.create_folder(db_name, nested_folder_name2, parent_folder_id)

        nested_ids = self.__module.get_nested_folder_ids_by_folder_id(db_name, parent_folder_id)
        self.assertCountEqual([nested_folder_id, nested_folder_id2], nested_ids)

    def test_get_content_data_by_folder_id(self):
        db_name = self.__choose_deployable_db()

        root_folder_name = gen_uppercase_string(12)  # случайное имя
        parent_folder_id = self.__module.create_folder(db_name, root_folder_name, None)
        nested_folder_name = gen_uppercase_string(12)  # случайное имя
        nested_folder_id = self.__module.create_folder(db_name, nested_folder_name, parent_folder_id)

        rule_name1 = gen_lowercase_string(20)  # случайное имя
        code1 = self.__test_co_rule
        rule_id_str1 = self.__module.create_co_rule(db_name, rule_name1, code1, 'Descr', parent_folder_id)

        rule_name2 = gen_lowercase_string(20)  # случайное имя
        code2 = self.__test_co_rule
        rule_id_str2 = self.__module.create_co_rule(db_name, rule_name2, code2, 'Descr', nested_folder_id)

        nested_ids = self.__module.get_content_data_by_folder_id(db_name, parent_folder_id)
        self.assertDictEqual({rule_id_str1: "CorrelationRule"}, nested_ids)

    def test_move_folder(self):
        db_name = self.__choose_deployable_db()

        src_folder_name = gen_uppercase_string(12)  # случайное имя
        src_folder_id = self.__module.create_folder(db_name, src_folder_name, None)
        nested_folder_name = gen_uppercase_string(12)  # случайное имя
        nested_folder_id = self.__module.create_folder(db_name, nested_folder_name, src_folder_id)
        dst_folder_name = gen_uppercase_string(12)  # случайное имя
        dst_folder_id = self.__module.create_folder(db_name, dst_folder_name, None)

        self.__module.move_folder(db_name, nested_folder_id, dst_folder_id)
        self.__module.get_folders_list(db_name, do_refresh=True)
        self.assertEqual('/'.join((dst_folder_name, nested_folder_name)),
                         self.__module.get_folder_path_by_id(db_name, nested_folder_id))

    def test_get_content_item(self):
        db_name = self.__choose_deployable_db()
        rule_name = gen_lowercase_string(20)  # случайное имя
        code = self.__test_co_rule
        new_rule_id_str = self.__module.create_co_rule(db_name, rule_name, code, 'Descr', None)
        rule_data = self.__module.get_content_item(db_name, new_rule_id_str, "CorrelationRule")
        self.assertEqual(code, rule_data.get('Formula'))

    def test_move_co_rule(self):
        db_name = self.__choose_deployable_db()

        src_folder_name = gen_uppercase_string(12)  # случайное имя
        src_folder_id = self.__module.create_folder(db_name, src_folder_name, None)

        dst_folder_name = gen_uppercase_string(12)  # случайное имя
        dst_folder_id = self.__module.create_folder(db_name, dst_folder_name, None)

        rule_name = gen_lowercase_string(20)  # случайное имя
        code = self.__test_co_rule
        new_rule_id_str = self.__module.create_co_rule(db_name, rule_name, code, 'Descr', src_folder_id)

        self.__module.move_content_item(db_name, new_rule_id_str, 'CorrelationRule', dst_folder_id)
        rule_data = self.__module.get_content_item(db_name, new_rule_id_str, 'CorrelationRule')

        self.assertEqual(dst_folder_id, rule_data.get('Folder').get('Id'))

    def test_move_folder_content(self):
        db_name = self.__choose_deployable_db()

        src_folder_name = gen_uppercase_string(12)  # случайное имя
        src_folder_id = self.__module.create_folder(db_name, src_folder_name, None)

        dst_folder_name = gen_uppercase_string(12)  # случайное имя
        dst_folder_id = self.__module.create_folder(db_name, dst_folder_name, None)

        child_folder_name = gen_uppercase_string(12)  # случайное имя
        child_folder_id = self.__module.create_folder(db_name, child_folder_name, src_folder_id)

        rule_name = gen_lowercase_string(20)  # случайное имя
        code = self.__test_co_rule
        new_rule_id_str = self.__module.create_co_rule(db_name, rule_name, code, 'Descr', src_folder_id)

        self.__module.move_folder_content(db_name, src_folder_name, dst_folder_name)
        rule_data = self.__module.get_content_item(db_name, new_rule_id_str, 'CorrelationRule')

        self.assertEqual(dst_folder_id, rule_data.get('Folder').get('Id'))
        self.assertEqual('/'.join((dst_folder_name, child_folder_name)),
                         self.__module.get_folder_path_by_id(db_name, child_folder_id))


    def test_get_content_items_by_group_id(self):
        db_name = self.__choose_deployable_db()

        root_group_name = gen_uppercase_string(12)
        child_group_name = gen_uppercase_string(12)
        root_group_id = self.__module.create_group(db_name, root_group_name)
        child_group_id = self.__module.create_group(db_name, child_group_name, root_group_id)

        root_rule_name = gen_lowercase_string(20)  # случайное имя
        code = self.__test_co_rule
        root_rule_id_str = self.__module.create_co_rule(db_name, root_rule_name, code, 'Descr',
                                                                        folder_id=None, group_ids=[root_group_id,])

        child_rule_name = gen_lowercase_string(20)  # случайное имя
        code = self.__test_co_rule
        child_rule_id_str = self.__module.create_co_rule(db_name, child_rule_name, code, 'Descr',
                                                        folder_id=None, group_ids=[child_group_id, ])

        rule_data = self.__module.get_content_items_by_group_id(db_name, root_group_id, recursive=True)
        rule_ids = [obj['id'] for obj in rule_data]
        self.assertCountEqual([root_rule_id_str, child_rule_id_str], rule_ids)

if __name__ == '__main__':
    unittest.main()
