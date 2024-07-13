# coding: utf-8

import os
import re
import sys
import json
import yaml
import uuid
import time
import shutil

from zipfile import ZipFile
from tempfile import TemporaryDirectory


class AggregationRule:
    """
    Правило агрегации
    """

    def parse_tokens(self):
        code_without_comments = re.sub('#.*', '', self.code)

        # Find rule name
        res = re.search(r'^aggregate\s+(.+?)[\r\n\t]', code_without_comments, re.MULTILINE)
        if res and res.group(1):
            self.name = res.group(1)

    def __init__(self, meta, code, i18n_ru=None):
        self.meta = meta
        self.ObjectId = meta['ObjectId']
        self.code = code
        self.name = 'Unknown'
        self.i18n_ru = i18n_ru

        self.parse_tokens()

    def __str__(self):
        return f'AR RULE [{self.ObjectId}] {self.name}'

    def __repr__(self):
        return self.__str__()


class EnrichmentRule:
    """
    Правило обогащения
    """

    def parse_tokens(self):
        code_without_comments = re.sub('#.*', '', self.code)

        # Find rule name
        res = re.search(r'^enrichment\s+(.+?)[\r\n\t]', code_without_comments, re.MULTILINE)
        if res and res.group(1):
            self.name = res.group(1)

        # Find remove_from statements
        for res in re.finditer('remove_from\s(\w+)', code_without_comments, re.MULTILINE):
            self.remove_from_tables.add(res.group(1))

        # Find insert_into statements
        for res in re.finditer('insert_into\s(\w+)', code_without_comments, re.MULTILINE):
            self.insert_into_tables.add(res.group(1))

        # Find query statements
        for res in re.finditer('query\s+.+?from\s+(\w+)', code_without_comments, re.MULTILINE):
            self.query_tlists.add(res.group(1))

    def __init__(self, meta, code, i18n_ru=None):
        self.meta = meta
        self.ObjectId = meta['ObjectId']
        self.code = code
        self.name = 'Unknown'
        self.i18n_ru = i18n_ru
        self.query_tlists = set()
        self.insert_into_tables = set()
        self.remove_from_tables = set()

        self.parse_tokens()

    def __str__(self):
        return f'ER RULE [{self.ObjectId}] {self.name}'

    def __repr__(self):
        return f'ER RULE [{self.ObjectId}] {self.name}'

    def print_all(self):
        print(self.__str__())
        print('\tquery:', self.query_tlists)
        print('\tinsert_into:', self.insert_into_tables)
        print('\tremove_from:', self.remove_from_tables)
        print()


class CorrelationRule:
    """
    Правило корреляции
    """

    def parse_tokens(self):
        code_without_comments = re.sub('#.*', '', self.code)

        # Find rule name
        res = re.search(r'rule\s+(.+):', code_without_comments, re.MULTILINE)
        if res and res.group(1):
            self.name = res.group(1)

        # Find query statements
        for res in re.finditer('query\s+.+?from\s+(\w+)', code_without_comments, re.MULTILINE):
            self.query_tlists.add(res.group(1))

    def __init__(self, meta, code, i18n_ru):
        self.meta = meta
        self.ObjectId = meta['ObjectId']
        self.code = code
        self.name = 'Unknown'
        self.query_tlists = set()
        self.i18n_ru = i18n_ru

        self.parse_tokens()

    def __str__(self):
        return f'CR RULE [{self.ObjectId}] {self.name}'

    def __repr__(self):
        return f'CR RULE [{self.ObjectId}] {self.name}'

    def print_all(self):
        print(self.__str__())
        print('\tquery:', self.query_tlists)
        print()


class TablularList:
    """
    Табличный список
    """

    def __init__(self, meta, code, i18n_ru=None):
        self.meta = meta
        self.ObjectId = meta['ObjectId']
        self.code = code
        self.i18n_ru = i18n_ru
        self.description = i18n_ru['Description'] if i18n_ru and 'Description' in i18n_ru else ''
        self.name = code['name']
        self.hasDefaults = True if 'defaults' in code else False

    def __str__(self):
        return f'TABULAR LIST [{self.ObjectId}] {self.name}'

    def __repr__(self):
        return f'TABULAR LIST [{self.ObjectId}] {self.name}'

    def print_defaults(self):
        defaults = self.code['defaults']['LOC']
        for row in defaults:
            print('\t{}'.format(row))


class NormalizationFormula:
    """
    Формула нормализации
    """

    def parse_tokens(self):
        code_without_comments = re.sub('#.*', '', self.code)

        # Find rule name
        res = re.search('^id\s*=\s*(\'|\"|)([a-zA-Z0-9_]+)', code_without_comments, re.MULTILINE)
        if res and res.group(2):
            self.name = res.group(2)

    def __init__(self, meta, code, i18n_ru=None):
        self.meta = meta
        self.ObjectId = meta['ObjectId']
        self.code = code
        self.name = 'Unknown'
        self.i18n_ru = i18n_ru

        self.parse_tokens()


class ContentPack:
    """
    Набор установки
    """

    # Правила агрегации
    AR_PATH = 'aggregation'
    AR_RULE_FILENAME = 'rule.agr'
    AR_METADATA_FILENAME = 'metainfo.yaml'
    AR_DESCR_PATH = 'i18n'
    AR_DESCR_RU_FILENAME = 'i18n_ru.yaml'

    # Правила обогащения
    ER_PATH = 'enrichments'
    ER_RULE_FILENAME = 'rule.en'
    ER_METADATA_FILENAME = 'metainfo.yaml'

    # Правила корреляции
    CR_PATH = 'correlations'
    CR_RULE_FILENAME = 'rule.co'
    CR_METADATA_FILENAME = 'metainfo.yaml'
    CR_DESCR_PATH = 'i18n'
    CR_DESCR_RU_FILENAME = 'i18n_ru.yaml'

    # Табличные списки
    TL_PATH = 'table_lists'
    TL_SCHEMA_FILENAME = 'table.tl'
    TL_METADATA_FILENAME = 'metainfo.yaml'
    TL_DESCR_PATH = 'i18n'
    TL_DESCR_RU_FILENAME = 'i18n_ru.yaml'

    # Категории событий
    EC_PATH = 'event_categories'
    EC_FILENAME = 'event_categories.yaml'

    # Формулы нормализации
    NF_PATH = 'normalizations'
    NF_FORMULA_FILENAME = 'formula.xp'
    NF_METADATA_FILENAME = 'metainfo.yaml'
    NF_DESCR_PATH = 'i18n'
    NF_DESCR_RU_FILENAME = 'i18n_ru.yaml'

    # Origins
    ORIGINS_PATH = 'origins'
    ORIGINS_FILENAME = 'origins.json'

    # Rules filters tag
    TAGS_PATH = 'rules_filters_tag'
    TAGS_FILENAME = 'tags.yaml'

    # Taxonomy
    TAXONOMY_PATH = 'taxonomy'
    TAXONOMY_FILENAME = 'taxonomy.json'

    # Knowledgebase.tree
    KB_TREE_FILENAME = 'knowledgebase.tree'

    # Properties
    PROPS_FILENAME = 'properties.txt'

    # Инициализируется каталогом или паком
    def __init__(self, input_object):
        # Правила агрегации
        self.ar_rules = {}
        # Правила обогащения
        self.er_rules = {}
        # Правила нормализации
        self.cr_rules = {}
        # Табличные списки
        self.tlists = {}
        # Категории событий
        self.event_categories = {}
        # Формулы нормализации
        self.nf_formulas = {}
        # Origins
        self.origins = {}
        # Rules Filters Tag
        self.tags = {}
        # Таксономия событий
        self.taxonomy = {}
        # Дерево каталогов
        self.kb_tree = {}
        # properties
        self.properties = ''

        # Служебные названия для типа контента
        self.NAMES = {
            'NormalizationRule': {
                'PATH': 'normalizations',
                'FILENAME': 'formula.xp',
                'RULE_FILE_TYPE': 'text',
                'CONSTRUCTOR': NormalizationFormula,
                'LIST': self.nf_formulas,
                'KIND': 'Normalization',
            },
            'CorrelationRule': {
                'PATH': 'correlations',
                'FILENAME': 'rule.co',
                'RULE_FILE_TYPE': 'text',
                'CONSTRUCTOR': CorrelationRule,
                'LIST': self.cr_rules,
                'KIND': 'Correlation',
            },
            'EnrichmentRule': {
                'PATH': 'enrichments',
                'FILENAME': 'rule.en',
                'RULE_FILE_TYPE': 'text',
                'CONSTRUCTOR': EnrichmentRule,
                'LIST': self.er_rules,
                'KIND': 'Enrichment',
            },
            'AggregationRule': {
                'PATH': 'aggregation',
                'FILENAME': 'rule.agr',
                'RULE_FILE_TYPE': 'text',
                'CONSTRUCTOR': AggregationRule,
                'LIST': self.ar_rules,
                'KIND': 'Aggregation',
            },
            'TabularList': {
                'PATH': 'table_lists',
                'FILENAME': 'table.tl',
                'RULE_FILE_TYPE': 'yaml',
                'CONSTRUCTOR': TablularList,
                'LIST': self.tlists,
                'KIND': 'TableList',
            },
        }

        self.TREE_TO_NAME = {
            'Normalization': 'NormalizationRule',
            'Correlation': 'CorrelationRule',
            'Enrichment': 'EnrichmentRule',
            'TableList': 'TabularList',
            'Aggregation': 'AggregationRule',
        }

        self.METADATA_FILENAME = 'metainfo.yaml'
        self.DESCR_PATH = 'i18n'
        self.DESCR_RU_FILENAME = 'i18n_ru.yaml'

        if os.path.isfile(input_object):
            # Извлечение начинки пака во временный каталог
            with TemporaryDirectory() as tempdir:
                with ZipFile(input_object) as content_pack:
                    content_pack.extractall(path=tempdir)
                self.load_pack_from_dir(tempdir)
        else:
            self.load_pack_from_tree_folder(input_object)

    # ------------------------------- Loaders (PT structure) ----------------------------------------------------

    def load_pack_from_dir(self, base_path):
        """
        Загрузка структуры набора установки из PT-структуры

        :param base_path: каталог с начинкой набора установки
        :return:
        """
        self.__load_event_categories(base_path)
        self.__load_origins(base_path)
        self.__load_tags(base_path)
        self.__load_taxonomy(base_path)
        self.__load_kb_tree(base_path)
        self.__load_props(base_path)

        for obj_type in self.NAMES:
            self.__load_rules(base_path, obj_type)

    def __load_rules(self, base_path, obj_type):
        """
        Загрузка правил и табличных списков

        :param base_path: каталог с начинкой набора установки
        :param obj_type:
        :return:
        """
        data_path = os.path.join(base_path, self.NAMES[obj_type]['PATH'])
        if os.path.exists(data_path):
            base_dirs = os.listdir(data_path)
            for base_dir in base_dirs:
                current_path = os.path.join(data_path, base_dir)
                rule_path = os.path.join(current_path, self.NAMES[obj_type]['FILENAME'])

                if self.NAMES[obj_type]['RULE_FILE_TYPE'] == 'text':
                    with open(rule_path, 'rt', encoding='utf-8-sig', newline='\n') as rulefile:
                        code = rulefile.read()
                else:
                    with open(rule_path, 'rt', encoding='utf-8-sig') as rulefile:
                        code = yaml.full_load(rulefile)

                meta_path = os.path.join(current_path, self.METADATA_FILENAME)
                with open(meta_path, 'rt', encoding='utf-8-sig') as metafile:
                    meta = yaml.full_load(metafile)

                descr_path = os.path.join(current_path,
                                          self.DESCR_PATH,
                                          self.DESCR_RU_FILENAME)
                i18n_ru = None
                if os.path.isfile(descr_path):
                    with open(descr_path, 'rt', encoding='utf-8-sig') as desc_file:
                        i18n_ru = yaml.full_load(desc_file)

                rule = self.NAMES[obj_type]['CONSTRUCTOR'](meta, code, i18n_ru)
                self.NAMES[obj_type]['LIST'][base_dir] = rule

    def __load_event_categories(self, base_path):
        """
        Загрузка категорий

        :param base_path: каталог с начинкой набора установки
        :return:
        """
        ec_path = os.path.join(base_path, ContentPack.EC_PATH, ContentPack.EC_FILENAME)
        if os.path.exists(ec_path):
            with open(ec_path, 'rt', encoding='utf-8-sig') as ec_file:
                self.event_categories = yaml.full_load(ec_file)

    def __load_origins(self, base_path):
        """
        Загрузка Origins

        :param base_path: каталог с начинкой набора установки
        :return:
        """
        origins_path = os.path.join(base_path, ContentPack.ORIGINS_PATH, ContentPack.ORIGINS_FILENAME)
        if os.path.exists(origins_path):
            with open(origins_path, 'rt', encoding='utf-8-sig') as origins_file:
                self.origins = json.load(origins_file)

    def __load_tags(self, base_path):
        """
        Загрузка tags

        :param base_path: каталог с начинкой набора установки
        :return:
        """
        tags_path = os.path.join(base_path, ContentPack.TAGS_PATH, ContentPack.TAGS_FILENAME)
        if os.path.exists(tags_path):
            with open(tags_path, 'rt', encoding='utf-8-sig') as tags_file:
                self.tags = yaml.full_load(tags_file)

    def __load_taxonomy(self, base_path):
        """
        Загрузка таксономии событий

        :param base_path: каталог с начинкой набора установки
        :return:
        """
        taxonomy_path = os.path.join(base_path, ContentPack.TAXONOMY_PATH, ContentPack.TAXONOMY_FILENAME)
        if os.path.exists(taxonomy_path):
            with open(taxonomy_path, 'rt', encoding='utf-8') as taxonomy_file:
                self.taxonomy = json.load(taxonomy_file)

    def __load_kb_tree(self, base_path):
        """
        Загрузка структуры набора установки

        :param base_path:
        :return:
        """
        kb_tree_path = os.path.join(base_path, ContentPack.KB_TREE_FILENAME)
        if os.path.exists(kb_tree_path):
            with open(kb_tree_path, 'rt', encoding='utf-8-sig') as kb_tree_file:
                self.kb_tree = json.load(kb_tree_file)

    def __load_props(self, base_path):
        """
        Загрузка props

        :param base_path:
        :return:
        """
        props_path = os.path.join(base_path, ContentPack.PROPS_FILENAME)
        if os.path.exists(props_path):
            with open(props_path, 'rt', encoding='utf-8') as props_file:
                self.properties = props_file.read()

    # ------------------------------- Dumpers (PT structure) ----------------------------------------------------
    def dump_to_kb_file(self, kb_file_path):
        """
        Упаковка каталога с контентом в файл набора установки

        :param kb_file_path: каталог с контентом
        :return:
        """
        with TemporaryDirectory() as tempdir:
            arch_dir = os.path.join(tempdir, 'arch')
            self.dump_pack_to_dir(arch_dir)
            arch_filename = str(uuid.uuid4())
            shutil.make_archive(os.path.join(tempdir, arch_filename), 'zip', root_dir=arch_dir)
            arch_filename += '.zip'
            shutil.copyfile(os.path.join(tempdir, arch_filename), kb_file_path)
            shutil.rmtree(tempdir, ignore_errors=True)
            time.sleep(2)

    def dump_pack_to_dir(self, base_path):
        """
        Дамп набора установки в каталог

        :param base_path: каталог для дампа
        :return:
        """
        if not os.path.isdir(base_path):
            os.mkdir(base_path)

        for obj_type in self.NAMES:
            self.__dump_rule(base_path, obj_type)

        self.__dump_event_categories(base_path)
        self.__dump_origins(base_path)
        self.__dump_tags(base_path)
        self.__dump_taxonomy(base_path)
        self.__dump_kb_tree(base_path)
        self.__dump_props(base_path)

    def __dump_rule(self, base_path, obj_type):
        """
        Дамп правил

        :param base_path: каталог для дампа
        :param obj_type:
        :return:
        """
        data_path = os.path.join(base_path, self.NAMES[obj_type]['PATH'])
        if os.path.exists(data_path):
            shutil.rmtree(data_path)
        if self.NAMES[obj_type]['LIST']:
            os.mkdir(data_path)
            for rule in self.NAMES[obj_type]['LIST'].values():
                base_dir = os.path.join(data_path, rule.ObjectId)
                os.mkdir(base_dir)
                code_filename = os.path.join(base_dir, self.NAMES[obj_type]['FILENAME'])

                if self.NAMES[obj_type]['RULE_FILE_TYPE'] == 'text':
                    with open(code_filename, 'wt', encoding='utf-8-sig', newline='\n') as code_file:
                        code_file.write(rule.code)
                else:
                    with open(code_filename, 'wt', encoding='utf-8-sig') as code_file:
                        yaml.safe_dump(rule.code, code_file, allow_unicode=True)

                meta_filename = os.path.join(base_dir, self.METADATA_FILENAME)
                with open(meta_filename, 'wt', encoding='utf-8-sig') as meta_file:
                    yaml.safe_dump(rule.meta, meta_file, allow_unicode=True)

                if rule.i18n_ru:
                    i18n_dir = os.path.join(base_dir, self.DESCR_PATH)
                    os.mkdir(i18n_dir)
                    descr_path = os.path.join(i18n_dir, self.DESCR_RU_FILENAME)
                    with open(descr_path, 'wt', encoding='utf-8-sig') as desc_file:
                        yaml.safe_dump(rule.i18n_ru, desc_file, allow_unicode=True)

    def __dump_event_categories(self, base_path):
        """
        Дамп категорий

        :param base_path: каталог для дампа
        :return:
        """
        ec_path = os.path.join(base_path, ContentPack.EC_PATH)
        os.mkdir(ec_path)
        ec_filename = os.path.join(ec_path, ContentPack.EC_FILENAME)
        with open(ec_filename, 'wt', encoding='utf-8-sig') as ec_file:
            yaml.safe_dump(self.event_categories, ec_file, allow_unicode=True)

    def __dump_origins(self, base_path):
        """
        Дамп Origins

        :param base_path: каталог для дампа
        :return:
        """
        origins_path = os.path.join(base_path, ContentPack.ORIGINS_PATH)
        os.mkdir(origins_path)
        origins_filename = os.path.join(origins_path, ContentPack.ORIGINS_FILENAME)
        with open(origins_filename, 'wt', encoding='utf-8-sig') as origins_file:
            json.dump(self.origins, origins_file, ensure_ascii=False)

    def __dump_tags(self, base_path):
        """
        Дамп tags

        :param base_path: каталог для дампа
        :return:
        """
        tags_path = os.path.join(base_path, ContentPack.TAGS_PATH)
        os.mkdir(tags_path)
        tags_filename = os.path.join(tags_path, ContentPack.TAGS_FILENAME)
        with open(tags_filename, 'wt', encoding='utf-8-sig') as tags_file:
            yaml.safe_dump(self.tags, tags_file, allow_unicode=True)

    def __dump_taxonomy(self, base_path):
        """
        Дамп таксономии

        :param base_path: каталог для дампа
        :return:
        """
        taxonomy_path = os.path.join(base_path, ContentPack.TAXONOMY_PATH)
        os.mkdir(taxonomy_path)
        taxonomy_filename = os.path.join(taxonomy_path, ContentPack.TAXONOMY_FILENAME)
        with open(taxonomy_filename, 'wt', encoding='utf-8') as taxonomy_file:
            json.dump(self.taxonomy, taxonomy_file)

    def __dump_kb_tree(self, base_path):
        """
        Дамп структуры набора установки

        :param base_path: каталог для дампа
        :return:
        """
        kb_tree_path = os.path.join(base_path, ContentPack.KB_TREE_FILENAME)
        with open(kb_tree_path, 'wt', encoding='utf-8-sig') as kb_tree_file:
            json.dump(self.kb_tree, kb_tree_file, indent=2, ensure_ascii=False)

    def __dump_props(self, base_path):
        """
        Дамп props

        :param base_path: каталог для дампа
        :return:
        """
        props_path = os.path.join(base_path, ContentPack.PROPS_FILENAME)
        with open(props_path, 'wt', encoding='utf-8') as props_file:
            props_file.write(self.properties)

    # ------------------------------- Dumpers (Иерархическая структура) ------------------------------------------

    def __dump_taxonomy_tree(self, taxonomy_path):
        """
        Дамп таксономии в иерархическую структуру

        :param taxonomy_path: каталог для дампа
        :return:
        """
        taxonomy_filename = os.path.join(taxonomy_path, ContentPack.TAXONOMY_FILENAME)
        with open(taxonomy_filename, 'wt', encoding='utf-8') as taxonomy_file:
            json.dump(self.taxonomy, taxonomy_file)

    def __dump_origins_tree(self, origins_path):
        """
        Дамп Origins в иерархическую структуру

        :param origins_path: каталог для дампа
        :return:
        """
        origins_filename = os.path.join(origins_path, ContentPack.ORIGINS_FILENAME)
        with open(origins_filename, 'wt', encoding='utf-8-sig') as origins_file:
            json.dump(self.origins, origins_file, ensure_ascii=False)

    def __dump_event_categories_tree(self, ec_path):
        """
        Дамп категорий в иерархическую структуру

        :param ec_path: каталог для дампа
        :return:
        """
        ec_filename = os.path.join(ec_path, ContentPack.EC_FILENAME)
        with open(ec_filename, 'wt', encoding='utf-8') as ec_file:
            yaml.safe_dump(self.event_categories, ec_file, allow_unicode=True)

    def __dump_tags_tree(self, tags_path):
        """
        Дамп tags в иерархическую структуру

        :param tags_path: каталог для дампа
        :return:
        """
        tags_filename = os.path.join(tags_path, ContentPack.TAGS_FILENAME)
        with open(tags_filename, 'wt', encoding='utf-8') as tags_file:
            yaml.safe_dump(self.tags, tags_file, allow_unicode=True)

    def __dump_rule_tree(self, id, name, path, obj_type):
        """
        Дамп правил в иерархическую структуру

        :param id: ID
        :param name: имя
        :param path: путь
        :param obj_type: тип правила
        :return:
        """
        rule = self.NAMES[obj_type]['LIST'][id]
        base_dir = os.path.join(path, name)

        if not os.path.isdir(base_dir):
            os.mkdir(base_dir)

        with open(os.path.join(base_dir, 'id.yaml'), 'wt') as idfile:
            yaml.safe_dump({'id': rule.ObjectId}, idfile, allow_unicode=True)

        rule_path = os.path.join(base_dir, self.NAMES[obj_type]['FILENAME'])
        if self.NAMES[obj_type]['RULE_FILE_TYPE'] == 'text':
            with open(rule_path, 'wt', encoding='utf-8', newline='\n') as rule_file:
                rule_file.write(rule.code)
        else:
            with open(rule_path, 'wt', encoding='utf-8') as rule_file:
                yaml.safe_dump(rule.code, rule_file, allow_unicode=True)

        meta_path = os.path.join(base_dir, self.METADATA_FILENAME)
        with open(meta_path, 'wt', encoding='utf-8') as meta_file:
            yaml.safe_dump(rule.meta, meta_file, allow_unicode=True)

        if rule.i18n_ru:
            i18n_dir = os.path.join(base_dir, self.DESCR_PATH)
            if not os.path.isdir(i18n_dir):
                os.mkdir(i18n_dir)
            desc_path = os.path.join(i18n_dir, self.DESCR_RU_FILENAME)
            with open(desc_path, 'wt', encoding='utf-8') as desc_file:
                yaml.safe_dump(rule.i18n_ru, desc_file, allow_unicode=True)

    def __dump_tree_level(self, level, path):
        """
        Дамп уровня в дереве

        :param level: уровень
        :param path: путь
        :return:
        """
        for element in level:
            if 'Kind' in element:
                kind = element['Kind']
                if kind == 'Taxonomy':
                    self.__dump_taxonomy_tree(path)
                elif kind == 'Origins':
                    self.__dump_origins_tree(path)
                elif kind == 'EventCategories':
                    self.__dump_event_categories_tree(path)
                elif kind == 'RulesFiltersTag':
                    self.__dump_tags_tree(path)

                elif kind in ('Correlation', 'Enrichment', 'TableList', 'Normalization', 'Aggregation'):
                    rule_type = self.TREE_TO_NAME[kind]
                    name = element['Name']
                    object_id = element['Id']
                    self.__dump_rule_tree(object_id, name, path, rule_type)

            elif 'Items' in element:
                name = element['Name']
                items = element['Items']
                items_path = os.path.join(path, name)
                if not os.path.isdir(items_path):
                    os.mkdir(items_path)
                self.__dump_tree_level(items, items_path)

    def dump_pack_to_tree_folder(self, base_path):
        """
        Дамп набора установки в иерархическую структуру

        :param base_path: путь для дампа
        :return:
        """
        if not os.path.isdir(base_path):
            os.mkdir(base_path)

        self.__dump_tree_level(self.kb_tree, base_path)
        self.__dump_props(base_path)

    # ------------------------------- Loaders (Иерархическая структура) --------------------------------------------
    def load_pack_from_tree_folder(self, base_path):
        """
        Загрузка набора установки из иерархической структуры

        :param base_path: каталог для загрузки
        :return:
        """
        tree = [
            {
                'Kind': 'Taxonomy',
                'Name': 'Taxonomy'
            },
            {
                'Kind': 'Origins',
                'Name': 'Origins'
            },
            {
                'Kind': 'EventCategories',
                'Name': 'EventCategories'
            },
            {
                'Kind': 'RulesFiltersTag',
                'Name': 'rules_filters_tag'
            },
        ]

        self.__load_tree_taxonomy(base_path)
        self.__load_tree_origins(base_path)
        self.__load_tree_event_categories(base_path)
        self.__load_tree_tags(base_path)
        tree.extend(self.__load_tree_level(base_path)['Items'])

        self.kb_tree = tree

    def __load_tree_taxonomy(self, taxonomy_path):
        """
        Загрузка таксономии из иерархической структуры

        :param taxonomy_path: каталог для загрузки
        :return:
        """
        taxonomy_filename = os.path.join(taxonomy_path, ContentPack.TAXONOMY_FILENAME)
        with open(taxonomy_filename, 'rt', encoding='utf-8') as taxonomy_file:
            self.taxonomy = json.load(taxonomy_file)

    def __load_tree_origins(self, origins_path):
        """
        Загрузка Origins

        :param origins_path: каталог для загрузки
        :return:
        """
        origins_filename = os.path.join(origins_path, ContentPack.ORIGINS_FILENAME)
        with open(origins_filename, 'rt', encoding='utf-8-sig') as origins_file:
            self.origins = json.load(origins_file)

    def __load_tree_event_categories(self, ec_path):
        """
        Загрузка категорий

        :param ec_path: каталог для загрузки
        :return:
        """
        ec_filename = os.path.join(ec_path, ContentPack.EC_FILENAME)
        with open(ec_filename, 'rt', encoding='utf-8') as ec_file:
            self.event_categories = yaml.full_load(ec_file)

    def __load_tree_tags(self, tags_path):
        """
        Загрузка tags

        :param tags_path: каталог для загрузки
        :return:
        """
        tags_filename = os.path.join(tags_path, ContentPack.TAGS_FILENAME)
        with open(tags_filename, 'rt', encoding='utf-8') as tags_file:
            self.tags = yaml.full_load(tags_file)

    def __load_tree_rule(self, current_path, obj_id, obj_type):
        """
        Загрузка правила из иерархической структуры

        :param current_path: текущий путь
        :param obj_id: ID объекта
        :param obj_type: тип правила
        :return:
        """
        rule_path = os.path.join(current_path, self.NAMES[obj_type]['FILENAME'])
        if self.NAMES[obj_type]['RULE_FILE_TYPE'] == 'text':
            with open(rule_path, 'rt', encoding='utf-8-sig', newline='\n') as rule_file:
                code = rule_file.read()
        else:
            with open(rule_path, 'rt', encoding='utf-8-sig') as rule_file:
                code = yaml.full_load(rule_file)

        meta_path = os.path.join(current_path, self.METADATA_FILENAME)
        with open(meta_path, 'rt', encoding='utf-8-sig') as meta_file:
            meta = yaml.full_load(meta_file)

        descr_path = os.path.join(current_path,
                                  self.DESCR_PATH,
                                  self.DESCR_RU_FILENAME)
        i18n_ru = None
        if os.path.isfile(descr_path):
            with open(descr_path, 'rt', encoding='utf-8-sig') as desc_file:
                i18n_ru = yaml.full_load(desc_file)

        rule = self.NAMES[obj_type]['CONSTRUCTOR'](meta, code, i18n_ru)

        self.NAMES[obj_type]['LIST'][obj_id] = rule

        return {
            'Kind': self.NAMES[obj_type]['KIND'],
            'Id': obj_id,
            'Name': os.path.basename(current_path)
        }

    def __load_tree_level(self, current_path):
        """
        Загрузка уровня дерева

        :param current_path: путь
        :return:
        """

        items = os.listdir(current_path)
        if 'id.yaml' in items:
            # load object
            with open(os.path.join(current_path, 'id.yaml'), 'rt') as idfile:
                obj_id = yaml.full_load(idfile)['id']
            if '-CR-' in obj_id:
                return self.__load_tree_rule(current_path, obj_id, 'CorrelationRule')
            elif '-ER-' in obj_id:
                return self.__load_tree_rule(current_path, obj_id, 'EnrichmentRule')
            elif '-AR-' in obj_id:
                return self.__load_tree_rule(current_path, obj_id, 'AggregationRule')
            elif '-NF-' in obj_id:
                return self.__load_tree_rule(current_path, obj_id, 'NormalizationRule')
            elif '-TL-' in obj_id:
                return self.__load_tree_rule(current_path, obj_id, 'TabularList')
        else:
            # iterate over folders
            base = os.path.basename(current_path)
            objs = []
            for item in items:
                item_path = os.path.join(current_path, item)
                if os.path.isdir(item_path):
                    nested_obj = self.__load_tree_level(item_path)
                    objs.append(nested_obj)

            return {
                'Name': base,
                'Items': objs
            }

    def __get_folder_paths(self, level, path):
        ar_list = []
        cr_list = []
        er_list = []
        tl_list = []
        nf_list = []
        for element in level:
            name = element['Name']
            element_path = '/'.join((path, name)) if path else name

            if 'Kind' in element:
                kind = element['Kind']
                if kind in ('Taxonomy', 'Origins', 'EventCategories', 'RulesFiltersTag'):
                    continue

                elif kind == 'Normalization':
                    nf_list.append(element_path)

                elif kind == 'Correlation':
                    cr_list.append(element_path)

                elif kind == 'Enrichment':
                    er_list.append(element_path)

                elif kind == 'Aggregation':
                    ar_list.append(element_path)

                elif kind == 'TableList':
                    tl_list.append(element_path)

            elif 'Items' in element:
                items = element['Items']
                child_nf, child_cr, child_er, child_tl, child_ar = self.__get_folder_paths(items, element_path)
                nf_list.extend(child_nf)
                cr_list.extend(child_cr)
                er_list.extend(child_er)
                tl_list.extend(child_tl)
                ar_list.extend(child_ar)

        return nf_list, cr_list, er_list, tl_list, ar_list

    def get_content_links(self):
        nf_list, cr_list, er_list, tl_list, ar_list = self.__get_folder_paths(self.kb_tree, '')
        return {
            'NormalizationRule': nf_list,
            'CorrelationRule': cr_list,
            'EnrichmentRule': er_list,
            'AggregationRule': ar_list,
            'TabularList': tl_list
        }


# --------------- Вспомогательные функции для работы с рабочей копией ----------------

def content_folder_to_work_copy(contend_folder: str, work_copy):
    """
    Преобразование папки с выгруженным контентом в рабочую копию для Git

    :param contend_folder:
    :param work_copy:
    :return:
    """
    if os.path.isdir(contend_folder):
        # Создать контентную папку для контента
        if not os.path.isdir(work_copy):
            os.mkdir(work_copy)

        work_copy_content = os.path.join(work_copy, 'Content')
        if not os.path.isdir(work_copy_content):
            os.mkdir(work_copy_content)

        work_copy_groups = os.path.join(work_copy, 'Groups')
        if not os.path.isdir(work_copy_groups):
            os.mkdir(work_copy_groups)

        for filename in os.listdir(contend_folder):
            if filename.endswith('.kb'):
                sys.stdout.write('Dumping {}\n'.format(filename))
                pack = ContentPack(os.path.join(contend_folder, filename))
                pack.dump_pack_to_tree_folder(work_copy_content)
            elif filename.endswith('.yaml'):
                shutil.copyfile(
                    os.path.join(contend_folder, filename),
                    os.path.join(work_copy_groups, filename)
                )


def work_copy_to_content_folder(work_copy, content_folder, filters=[], optimize=False):
    """
    Преобразование рабочей копиии в структуру для загрузки в SIEM

    :param work_copy: каталог рабочей копии
    :param content_folder: каталог со структурой для загрузки в SIEM
    :param filters: перечень наборов установки для загрузки в SIEM
    :param optimize: оптимизация kb-файлов
    :return:
    """
    if os.path.isdir(work_copy):
        STATIC_FILES = [
            'event_categories.yaml',
            'origins.json',
            'properties.txt',
            'tags.yaml',
            'taxonomy.json'
        ]
        global_path_list = []

        work_copy_content = os.path.join(work_copy, 'Content')
        work_copy_groups = os.path.join(work_copy, 'Groups')

        if not os.path.isdir(content_folder):
            os.mkdir(content_folder)

        for group in os.listdir(work_copy_groups):

            if filters and group not in filters:
                continue

            group_filepath = os.path.join(work_copy_groups, group)
            with open(group_filepath, 'rt', encoding='utf-8') as kb_meta_file:
                kb_meta = yaml.full_load(kb_meta_file)
                shutil.copyfile(group_filepath,
                                os.path.join(content_folder, group)
                                )
                if 'kb_tree' in kb_meta:
                    if optimize:
                        for content_type in kb_meta['kb_tree']:
                            global_path_list.extend(kb_meta['kb_tree'][content_type])

                    else:
                        # По файлу kb на каждый набор установки
                        with TemporaryDirectory() as tmp_dir:
                            path_list = []
                            for content_type in kb_meta['kb_tree']:
                                path_list.extend(kb_meta['kb_tree'][content_type])

                            for path_item in path_list:
                                src_path = os.path.normpath(os.path.join(work_copy_content, path_item))
                                dst_path = os.path.normpath(os.path.join(tmp_dir, path_item))
                                shutil.copytree(src_path, dst_path)

                            for static_file in STATIC_FILES:
                                src_file = os.path.join(work_copy_content, static_file)
                                dst_file = os.path.join(tmp_dir, static_file)
                                shutil.copyfile(src_file, dst_file)

                            pack = ContentPack(tmp_dir)
                            pack.dump_to_kb_file(
                                os.path.join(content_folder, group.replace('.yaml', '.kb'))
                            )

        if optimize:
            # Оптимизация контента. Весь контент загружается в единый файл kb
            with TemporaryDirectory() as tmp_dir:
                for path_item in global_path_list:
                    src_path = os.path.normpath(os.path.join(work_copy_content, path_item))
                    dst_path = os.path.normpath(os.path.join(tmp_dir, path_item))
                    shutil.copytree(src_path, dst_path)

                for static_file in STATIC_FILES:
                    src_file = os.path.join(work_copy_content, static_file)
                    dst_file = os.path.join(tmp_dir, static_file)
                    shutil.copyfile(src_file, dst_file)

                pack = ContentPack(tmp_dir)
                pack.dump_to_kb_file(
                    os.path.join(content_folder, 'ContentPack.kb')
                )
