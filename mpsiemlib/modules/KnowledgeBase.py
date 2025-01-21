import os
import time

from hashlib import sha256
from typing import Iterator, Optional

from mpsiemlib.common import ModuleInterface, MPSIEMAuth, LoggingHandler, MPComponents, Settings, MPContentTypes
from mpsiemlib.common import exec_request, get_metrics_start_time, get_metrics_took_time


class KnowledgeBase(ModuleInterface, LoggingHandler):
    """PT KB module."""

    __kb_port = 8091

    #  обрабатывается в KB
    __api_root = '/api-studio'
    __api_kb_db_list = f'{__api_root}/content-database-selector/content-databases'
    __api_temp_file_storage_upload = f'{__api_root}/tempFileStorage/upload'

    __api_siem = f'{__api_root}/siem'
    __api_deploy_object = f'{__api_siem}/deploy'
    __api_deploy_log = f'{__api_siem}/deploy/log'
    __api_list_objects = f'{__api_siem}/objects/list'
    __api_rule_code = f'{__api_siem}' + '/{}-rules/{}'
    __api_table_info = f'{__api_siem}' + '/tabular-lists/{}'
    __api_table_rows = f'{__api_siem}' + '/tabular-lists/{}/rows'
    __api_groups = f'{__api_siem}/groups'
    __api_folders_packs_list = f'{__api_siem}/folders/tree?includeObjects=false'
    __api_folders = f'{__api_siem}/folders'
    __api_folders_children = f'{__api_folders}' + '/{}/children?includeObjects=false'
    __api_co_rules = f'{__api_siem}/correlation-rules'
    __api_export = f'{__api_siem}/export'
    __api_import = f'{__api_siem}/import'
    __api_mass_operations = f'{__api_siem}/mass-operations'
    __api_siem_objgroups_values = f'{__api_mass_operations}/SiemObjectGroup/values'

    # обрабатывается в Core
    __api_rule_running_info = '/api/siem/v2/rules/{}/{}'
    __api_rule_stop = '/api/siem/v2/rules/{}/commands/stop'
    __api_rule_start = '/api/siem/v2/rules/{}/commands/start'

    # Форматы экспорта
    EXPORT_FORMAT_KB = 'kb'
    EXPORT_FORMAT_SIEM_LITE = 'siem'

    # Режимы импорта

    # Добавить и обновить объекты из файла
    #
    # Все объекты из файла добавятся как пользовательские.
    # Существующие в системе объекты будут заменены, в том числе
    # записи табличных списков.
    IMPORT_ADD_AND_UPDATE = 'upsert'

    # Добавить объекты Локальная система как системные
    #
    # Будут импортированы только объекты Локальная система.
    # Новые объекты добавятся, существующие будут заменены.
    IMPORT_LOCAL_SYSTEM_AS_SYSTEM = 'upsert_origin'

    # Синхронизировать объекты Локальная система с содержимым файла
    #
    # Будут импортированы только объекты Локальная система.
    # Существующие объекты будут заменены на объекты из файла,
    # а объекты, которых нет в файле, будут удалены из системы.
    IMPORT_SYNC_SYSTEM = 'replace_origin'

    # Маппинг типа контента в часть url для запросов
    ITEM_TYPE_MAP = {
        'CorrelationRule': 'correlation-rules',
        'AggregationRule': 'aggregation-rules',
        'EnrichmentRule': 'enrichment-rules',
        'NormalizationRule': 'normalization-rules',
        'TabularList': 'tabular-lists',
    }

    # Статусы установки в SIEM для контента
    DEPLOYMENT_STATUS_INSTALLED = 'Installed'
    DEPLOYMENT_STATUS_NOT_INSTALLED = 'NotInstalled'

    # Таймаут операций установки/удаления контента из SIEM
    DEPLOYMENT_TIMEOUT = 10
    # Попытки проверки статуса при неизменном проценте
    DEPLOYMENT_RETRIES = 10

    def __init__(self, auth: MPSIEMAuth, settings: Settings):
        ModuleInterface.__init__(self, auth, settings)
        LoggingHandler.__init__(self)
        self.__kb_session = auth.sessions['kb']
        self.__kb_hostname = auth.creds.core_hostname
        self.__rules_mapping = {}
        self.__groups = {}
        self.__folders = {}
        self.__packs = {}
        self.log.debug('status=success, action=prepare, msg="KB Module init"')

    def install_objects(self, db_name: str, guids_list: list, do_remove=False) -> str:
        """Установить объекты из KB в SIEM.

        :param db_name: Имя БД
        :param guids_list: Список объектов для установки
        :param do_remove:
        :return: deploy ID
        """
        self.log.info('status=prepare, action=install_objects, msg="Try to {} objects {}", '
                      'hostname="{}", db="{}"'.format('install' if not do_remove else 'uninstall',
                                                      guids_list,
                                                      self.__kb_hostname,
                                                      db_name))
        headers = {'Content-Database': db_name,
                   'Content-Locale': 'RUS'}

        core_major_ver = int(self.auth.get_core_version().split('.')[0])
        if core_major_ver >= 24:
            # Изменения с версии 24
            params = {'mode': 'selection' if not do_remove else 'uninstall',
                      'selectedObjects': {
                          'ids': guids_list,
                          'selectionMode': 'Selected'
                      }
                      }
        else:
            params = {'mode': 'selection' if not do_remove else 'uninstall',
                      'include': guids_list}

        url = f'https://{self.__kb_hostname}:{self.__kb_port}{self.__api_deploy_object}'
        r = exec_request(self.__kb_session,
                         url,
                         method='POST',
                         timeout=self.settings.connection_timeout,
                         headers=headers,
                         json=params)
        response = r.json()

        self.log.info('status=success, action=install_objects, msg="{} objects {}", '
                      'hostname="{}", db="{}"'.format("Install" if not do_remove else "Uninstall",
                                                      guids_list,
                                                      self.__kb_hostname,
                                                      db_name))

        return response.get('Id')

    def install_objects_by_group_id(self, db_name: str, group_id: str) -> str:
        """Установить объекты из KB в SIEM.

        :param db_name: Имя БД
        :param group_id: ID набора для установки, None для установки
            всего контента
        :return: deploy ID
        """
        self.log.info('status=prepare, action=install_objects_by_group_id, msg="Try to install group {}", '
                      'hostname="{}", db="{}"'.format(group_id, self.__kb_hostname, db_name))

        headers = {'Content-Database': db_name,
                   'Content-Locale': 'RUS'}
        if group_id is None:
            params = {'mode': 'all'}
        else:
            params = {'mode': 'group', 'groupId': group_id}
        url = f'https://{self.__kb_hostname}:{self.__kb_port}{self.__api_deploy_object}'
        r = exec_request(self.__kb_session,
                         url,
                         method='POST',
                         timeout=self.settings.connection_timeout,
                         headers=headers,
                         json=params)
        response = r.json()

        self.log.info('status=success, action=install_objects_by_group_id, msg="Install group {}", '
                      'hostname="{}", db="{}"'.format(group_id, self.__kb_hostname, db_name))

        return response.get('Id')

    def uninstall_objects(self, db_name: str, guids_list: list) -> str:
        """Удалить объекты из SIEM.

        :param db_name: Имя БД
        :param guids_list: Список обЪектов для удаления из SIEM
        :return: deploy ID
        """

        return self.install_objects(db_name, guids_list, do_remove=True)

    def install_objects_sync(self, db_name: str, guids_list: list,
                             do_remove=False,
                             timeout: int = DEPLOYMENT_TIMEOUT,
                             max_retries: int = DEPLOYMENT_RETRIES):
        """Синхронный вариант инсталляции/деинсталляции контента из SIEM.

        :param db_name: имя БД
        :param guids_list: список ID элементов контента
        :param do_remove: False - инсталляция контента, True - деинсталляция контента
        :param timeout: таймаут между попытками проверки статуса установки
        :param max_retries: максимальное количество попыток проверки статуса установки.
                            Если процент установки меняется - счетчик сбрасывается
        :return:
        """

        operation = 'Uninstall' if do_remove else 'Install'

        deploy_id = self.install_objects(db_name, guids_list, do_remove)
        retries = max_retries
        last_percentage = 0
        while retries > 0:
            time.sleep(timeout)
            status = self.get_deploy_status(db_name, deploy_id)
            deployment_status = status.get('deployment_status')
            if deployment_status == 'succeeded':
                self.log.info(
                    'status=success, action=install_objects_sync, msg="{} succeed", '
                    'hostname="{}", db="{}"'.format(operation, self.__kb_hostname, db_name))
                break
            elif deployment_status == 'running':
                percentage = int(status.get('percentage'))
                if percentage > last_percentage:
                    retries = max_retries
                else:
                    retries -= 1
            else:
                errors = status.get('errors')
                self.log.error(
                    'status=failure, action=install_objects_sync, msg="{} failed. Errors: {}", '
                    'hostname="{}", db="{}"'.format(operation, errors, self.__kb_hostname, db_name))
                break

    def get_deploy_status(self, db_name: str, deploy_id: str) -> dict:
        """Получить общий статус установки контента.

        :param db_name: Имя БД
        :param deploy_id: Идентификатор процесса установки/удаления
        :return: {"start_date": "date_string", "deployment_status":
            "succeeded|running"}
        """
        headers = {'Content-Database': db_name,
                   'Content-Locale': 'RUS'}

        url = f'https://{self.__kb_hostname}:{self.__kb_port}{self.__api_deploy_log}/{deploy_id}'
        r = exec_request(self.__kb_session,
                         url,
                         method='GET',
                         timeout=self.settings.connection_timeout,
                         headers=headers,
                         )
        state = r.json()

        deployment_status = state.get('DeployStatusId', '')
        percentage = str(state.get('Percentage', ''))
        errors = str(state.get('Errors', ''))

        self.log.info('status=success, action=get_deploy_status, msg="Deploy status: {}, percentage: {}%, errors: {}", '
                      'hostname="{}", db="{}"'.format(deployment_status, percentage, errors, self.__kb_hostname,
                                                      db_name))
        return {
            'deployment_status': deployment_status,
            'percentage': percentage,
            'errors': errors
        }

    def start_rule(self, db_name: str, content_type: str, guids_list: list):
        """Запустить правила, установленные в SIEM Server Используются ID
        правил из KB.

        :param db_name: Имя БД в KB
        :param content_type: MPContentType
        :param guids_list: Список ID правил для установки
        :return:
        """
        self.__manipulate_rule(db_name, content_type, guids_list, 'start')

    def stop_rule(self, db_name: str, content_type: str, guids_list: list):
        """Остановить правило, установленное в SIEM Server Используются ID
        правил из KB.

        :param db_name: Имя БД в KB
        :param content_type: MPContentType
        :param guids_list: Список ID правил для установки
        :return:
        """
        self.__manipulate_rule(db_name, content_type, guids_list, 'stop')

    def __manipulate_rule(self, db_name: str, content_type: str, guids_list: list, control='stop'):
        # Нет гарантий, что объекты в PT KB и SIEM будут называться одинаково.
        # Сейчас в классе прописаны названия из PT KB. Название табличек уже разное.
        object_type = None
        if content_type == MPContentTypes.CORRELATION:
            object_type = 'correlation'
        elif content_type == MPContentTypes.ENRICHMENT:
            object_type = 'enrichment'
        else:
            raise Exception(f'Unsupported content type to stop {content_type}')

        if len(self.__rules_mapping) == 0:
            self.__update_rules_mapping(db_name, content_type)

        rules_names = []
        for i in guids_list:
            name = self.__rules_mapping.get(db_name, {}).get(content_type, {}).get(i, {}).get("name")
            if name is None:
                self.log.error('status=failed, action=manipulate_rule, msg="Rule id not found", '
                               'hostname="{}", db="{}", rule_id="{}"'.format(self.__kb_hostname, db_name, i))
            rules_names.append(name)

        data = {'names': rules_names}
        api_url = self.__api_rule_start.format(object_type) if control == 'start' \
            else self.__api_rule_stop.format(object_type)
        url = f'https://{self.__kb_hostname}{api_url}'
        r = exec_request(self.__kb_session,
                         url,
                         method='POST',
                         timeout=self.settings.connection_timeout,
                         json=data)
        response = r.json()

        if len(response.get('error')) != 0:
            self.log.error('status=failed, action=manipulate_rule, '
                           'msg="Got error while manipulate rule", hostname="{}", rules_ids="{}", '
                           'rules_names="{}", db="{}", error="{}"'.format(self.__kb_hostname,
                                                                          guids_list,
                                                                          rules_names,
                                                                          db_name,
                                                                          response.get('error')))
            raise Exception('Got error while manipulate rule')

        self.log.info('status=success, action=manipulate_rule, msg="{} {} rules", '
                      'hostname="{}", rules_names="{}", db="{}"'.format(control,
                                                                        object_type,
                                                                        self.__kb_hostname,
                                                                        rules_names,
                                                                        db_name))

    def get_rule_running_state(self, db_name: str, content_type: str, guid: str):
        """Получить статус правила, работающего в SIEM Server. Используются ID
        правил из KB.

        :param db_name: Имя БД в KB
        :param content_type: MPContentType
        :param guid: ID правила
        :return:
        """
        object_type = None
        if content_type == MPContentTypes.CORRELATION:
            object_type = 'correlation'
        elif content_type == MPContentTypes.ENRICHMENT:
            object_type = 'enrichment'
        else:
            raise Exception(f'Unsupported content type to stop {content_type}')

        if len(self.__rules_mapping) == 0:
            self.__update_rules_mapping(db_name, content_type)

        name = self.__rules_mapping.get(db_name, {}).get(content_type, {}).get(guid, {}).get("name")

        api_url = self.__api_rule_running_info.format(object_type, name)
        url = f'https://{self.__kb_hostname}{api_url}'
        r = exec_request(self.__kb_session,
                         url,
                         method='GET',
                         timeout=self.settings.connection_timeout)
        response = r.json()

        state = response.get('state', {})

        return {'state': state.get('name'), 'reason': state.get('reason'), 'context': state.get('context')}

    def get_databases_list(self) -> dict:
        """Получить список БД.

        :return: {'db_name': {'param1': 'value1'}}
        """
        # TODO Не учитывается что БД с разными родительскими БД могут иметь одинаковое имя
        url = f'https://{self.__kb_hostname}:{self.__kb_port}{self.__api_kb_db_list}'
        r = exec_request(self.__kb_session,
                         url,
                         method='GET',
                         timeout=self.settings.connection_timeout)
        db_names = r.json()
        ret = {}
        for i in db_names:
            name = i.get('Name')
            ret[name] = {'id': i.get('Uid'),
                         'status': i.get('Status').lower(),
                         'updatable': i.get('IsUpdatable'),
                         'deployable': i.get('IsDeployable'),
                         'parent': i.get('ParentName'),
                         'revisions': i.get('RevisionsCount')}

        self.log.info('status=success, action=get_databases_list, msg="Got {} databases", '
                      'hostname="{}"'.format(len(ret), self.__kb_hostname))

        return ret

    def get_groups_list(self, db_name: str, do_refresh=False) -> dict:
        """Получить список групп.

        :param db_name: Имя БД
        :param do_refresh: Обновить кэш
        :return: {'group_id': {'parent_id': 'value', 'name': 'value'}}
        """
        if not do_refresh and len(self.__groups) != 0:
            return self.__groups

        headers = {'Content-Database': db_name,
                   'Content-Locale': 'RUS'}
        url = f'https://{self.__kb_hostname}:{self.__kb_port}{self.__api_groups}'

        r = exec_request(self.__kb_session,
                         url,
                         method='GET',
                         timeout=self.settings.connection_timeout,
                         headers=headers)
        groups = r.json()
        self.__groups.clear()

        for i in groups:
            self.__groups[i.get('Id')] = {'parent_id': i.get('ParentGroupId'),
                                          'name': i.get('SystemName')}

        self.log.info('status=success, action=get_groups_list, msg="Got {} groups", '
                      'hostname="{}", db="{}"'.format(len(self.__groups), self.__kb_hostname, db_name))

        return self.__groups

    def get_folders_list(self, db_name: str, do_refresh=False) -> dict:
        """Получить список папок.

        :param db_name: Имя БД
        :param do_refresh: Обновить кэш
        :return: {'group_id': {'parent_id': 'value', 'name': 'value'}}
        """
        if do_refresh or len(self.__folders) == 0:
            self.__folders.clear()
            self.__packs.clear()
            self.__get_folder_pack_root_level(db_name)

            self.log.info('status=success, action=get_folders_list, msg="Got {} folders", '
                          'hostname="{}", db="{}"'.format(len(self.__folders), self.__kb_hostname, db_name))

        return self.__folders

    def get_packs_list(self, db_name: str, do_refresh=False) -> dict:
        """Получить список паков.

        :param db_name: Имя БД
        :param do_refresh: Обновить кэш
        :return: {'group_id': {'parent_id': 'value', 'name': 'value'}}
        """
        if do_refresh or len(self.__packs) == 0:
            self.__get_folder_pack_root_level(db_name)

            self.log.info('status=success, action=get_packs_list, msg="Got {} packs", '
                          'hostname="{}", db="{}"'.format(len(self.__packs), self.__kb_hostname, db_name))

        return self.__packs

    def __iterate_folders_tree(self, db_name: str, folder_id: str):
        headers = {'Content-Database': db_name,
                   'Content-Locale': 'RUS'}

        folder_url = self.__api_folders_children.format(folder_id)

        url = f'https://{self.__kb_hostname}:{self.__kb_port}{folder_url}'

        r = exec_request(self.__kb_session,
                         url,
                         method='GET',
                         timeout=self.settings.connection_timeout,
                         headers=headers)

        folders_packs = r.json()
        for i in folders_packs:
            node_type = i.get('NodeKind')
            current = None
            if node_type == 'Folder':
                current = self.__folders
            elif node_type == 'KnowledgePack':
                current = self.__packs
            else:
                continue

            obj_id = i.get('Id')
            obj_name = i.get('Name')
            parent_obj_id = i.get('ParentId')
            current[obj_id] = {'parent_id': parent_obj_id,
                               'name': obj_name}

            if node_type == 'Folder' and i.get('HasChildren'):
                self.__iterate_folders_tree(db_name, obj_id)

    def __get_folder_pack_root_level(self, db_name: str):
        # if expand_nodes is None:
        #     expand_nodes = []
        params = {'expandNodes': []}
        headers = {'Content-Database': db_name,
                   'Content-Locale': 'RUS'}
        url = f'https://{self.__kb_hostname}:{self.__kb_port}{self.__api_folders_packs_list}'

        r = exec_request(self.__kb_session,
                         url,
                         method='POST',
                         timeout=self.settings.connection_timeout,
                         headers=headers,
                         json=params)
        folders_packs = r.json()

        for i in folders_packs:
            node_type = i.get('NodeKind')
            current = None
            if node_type == 'Folder':
                current = self.__folders
            elif node_type == 'KnowledgePack':
                current = self.__packs
            else:
                continue

            obj_id = i.get('Id')
            obj_name = i.get('Name')
            parent_obj_id = i.get('ParentId')
            current[obj_id] = {'parent_id': parent_obj_id,
                               'name': obj_name}

            if node_type == 'Folder' and i.get('HasChildren'):
                # Iterate nested
                self.__iterate_folders_tree(db_name, obj_id)

    def get_normalizations_list(self, db_name: str, filters: Optional[dict] = None) -> Iterator[dict]:
        """Получить список правил нормализации.

        :param db_name: Имя БД
        :param filters: см get_all_objects
        :return: Iterator
        """
        params = {'filters': {'SiemObjectType': ['Normalization']}}
        if filters is not None:
            filters.update(params)
        else:
            filters = params

        return self.get_all_objects(db_name, filters)

    def get_correlations_list(self, db_name: str, filters: Optional[dict] = None) -> Iterator[dict]:
        """Получить список правил корреляции.

        :param db_name: Имя БД
        :param filters: см get_all_objects
        :return: Iterator
        """
        params = {'filters': {'SiemObjectType': ['Correlation']}}
        if filters is not None:
            filters.update(params)
        else:
            filters = params

        return self.get_all_objects(db_name, filters)

    def get_enrichments_list(self, db_name: str, filters: Optional[dict] = None) -> Iterator[dict]:
        """Получить список правил обогащения.

        :param db_name: Имя БД
        :param filters: см get_all_objects
        :return: Iterator
        """
        params = {'filters': {'SiemObjectType': ['Enrichment']}}
        if filters is not None:
            filters.update(params)
        else:
            filters = params

        return self.get_all_objects(db_name, filters)

    def get_aggregations_list(self, db_name: str, filters: Optional[dict] = None) -> Iterator[dict]:
        """Получить список правил агрегации.

        :param db_name: Имя БД
        :param filters: см get_all_objects
        :return: Iterator
        """
        params = {'filters': {'SiemObjectType': ['Aggregation']}}
        if filters is not None:
            filters.update(params)
        else:
            filters = params

        return self.get_all_objects(db_name, filters)

    def get_tables_list(self, db_name: str, filters: Optional[dict] = None) -> Iterator[dict]:
        """Получить список табличек.

        :param db_name: Имя БД
        :param filters: см get_all_objects
        :return: Iterator
        """
        params = {'filters': {'SiemObjectType': ['TabularList']}}
        if filters is not None:
            filters.update(params)
        else:
            filters = params

        return self.get_all_objects(db_name, filters)

    def get_all_objects(self, db_name: str, filters: Optional[dict] = None, group_id: str = None) -> Iterator[dict]:
        """Выгрузка всех объектов, кроме макросов.

        :param db_name: Имя БД из которой идет выгрузка
        :param filters: {"folderId": null, "filters": {
            "SiemObjectType": ["Normalization"], "ContentType":
            ["System"], "DeploymentStatus": ["0"], "CompilationStatus":
            ["2"], "SiemObjectRegex": [".*_test_name"] }, "search": "",
            "sort": [{"name": "objectId", "order": 0, "type": 1}],
            "groupId": null, }
        :param group_id: Идентификатор набора установки
        :return: {"param1": "value1", "param2": "value2"}
        """
        self.log.info('status=prepare, action=get_all_objects, msg="Try to get objects list", '
                      'hostname="{}", db="{}", filters="{}"'.format(self.__kb_hostname, db_name, filters))

        url = f'https://{self.__kb_hostname}:{self.__kb_port}{self.__api_list_objects}'
        headers = {'Content-Database': db_name,
                   'Content-Locale': 'RUS'}
        params = {
            'sort': [{'name': 'objectId', 'order': 0, 'type': 1}],
        }
        if filters is not None:
            params.update(filters)

        if group_id:
            params.update(
                {'groupId': group_id}
            )

        # Пачками выгружаем содержимое
        is_end = False
        offset = 0
        limit = self.settings.kb_objects_batch_size
        line_counter = 0
        start_time = get_metrics_start_time()
        while not is_end:
            ret = self.__iterate_objects(url, params, headers, offset, limit)
            if len(ret) < limit:
                is_end = True
            offset += limit
            for i in ret:
                line_counter += 1
                yield {'id': i.get('Id'),
                       'guid': i.get('ObjectId'),
                       'name': i.get('SystemName'),
                       'folder_id': i.get('FolderId'),
                       'object_kind': i.get('ObjectKind'),
                       'folder_path': i.get('FolderPath').replace('\\', '/') if i.get('FolderPath') else '',
                       'origin_id': i.get('OriginId'),
                       'compilation_sdk': i.get('CompilationStatus', {}).get('SdkVersion'),
                       'compilation_status': i.get('CompilationStatus', {}).get('CompilationStatusId'),
                       'deployment_status': i.get('DeploymentStatus', '').lower()}
        took_time = get_metrics_took_time(start_time)

        self.log.info('status=success, action=get_all_objects, msg="Query executed, response have been read", '
                      'hostname="{}", filter="{}", lines={}, db="{}"'.format(self.__kb_hostname,
                                                                             filters,
                                                                             line_counter,
                                                                             db_name))
        self.log.info(
            f'hostname="{self.__kb_hostname}", metric=get_all_objects, took={took_time}ms, objects={line_counter}')

    def __iterate_objects(self, url: str, params: dict, headers: dict, offset: int, limit: int):
        params['withoutGroups'] = False
        params['recursive'] = True
        params['skip'] = offset
        params['take'] = limit
        rq = exec_request(self.__kb_session,
                          url,
                          method='POST',
                          timeout=self.settings.connection_timeout,
                          headers=headers,
                          json=params)
        response = rq.json()
        if response is None or 'Rows' not in response:
            self.log.error('status=failed, action=kb_objects_iterate, msg="KB data request return None or '
                           'has wrong response structure", '
                           'hostname="{}"'.format(self.__kb_hostname))
            raise Exception('KB data request return None or has wrong response structure')

        return response.get('Rows')

    def get_id_by_name(self, db_name: str, content_type: str, object_name: str) -> list:
        """Узнать ID объекта по его имени. KB позволяет создавать объекты с
        неуникальным именем.

        :param db_name: Имя БД
        :param content_type: Тип объекта MPContentType
        :param object_name: Имя искомого объекта
        :return: [{'id': value, 'folder_id': value}]
        """
        if len(self.__rules_mapping) == 0:
            self.__update_rules_mapping(db_name, content_type)

        ret = []
        for k, v in self.__rules_mapping[db_name][content_type].items():
            if v.get('name') == object_name:
                ret.append({'id': k, 'folder_id': v.get('folder_id'), 'guid': v.get('guid')})
        return ret

    def __update_rules_mapping(self, db_name: str, content_type: str):
        if self.__rules_mapping.get(db_name) is None:
            self.__rules_mapping[db_name] = {}
        if self.__rules_mapping.get(db_name).get(content_type) is None:
            self.__rules_mapping[db_name][content_type] = {}
        params = {'filters': {'SiemObjectType': [content_type]}}
        for i in self.get_all_objects(db_name, params):
            self.__rules_mapping[db_name][content_type][i.get('id')] = {'name': i.get('name'),
                                                                        'folder_id': i.get('folder_id'),
                                                                        'guid': i.get('guid')}

    def get_rule(self, db_name: str, content_type: str, rule_id: str) -> dict:
        """Получить полное описание и тело правила.

        :param db_name: Имя БД
        :param content_type: Тип объекта MPContentType
        :param rule_id: KB ID правила
        :return: {'param1': value, 'param2': value}
        """
        if content_type == MPContentTypes.TABLE:
            raise Exception(f'Method get_rule not supported {MPContentTypes.TABLE}')

        self.log.info('status=success, action=get_rule, msg="Try to get rule {}", '
                      'hostname="{}", db="{}"'.format(rule_id, self.__kb_hostname, db_name))

        headers = {'Content-Database': db_name,
                   'Content-Locale': 'RUS'}
        api_url = self.__api_rule_code.format(content_type.lower(), rule_id)
        url = f'https://{self.__kb_hostname}:{self.__kb_port}{api_url}'

        r = exec_request(self.__kb_session,
                         url,
                         method='GET',
                         timeout=self.settings.connection_timeout,
                         headers=headers)
        rule = r.json()

        rule_groups = []
        for i in rule.get('Groups'):
            rule_groups.append({'id': i.get('Id'), 'name': i.get('SystemName')})

        ret = {'id': rule.get('Id'),
               'guid': rule.get('ObjectId'),
               'folder_id': rule.get('Folder', {}).get('Id'),
               'origin_id': rule.get('OriginId'),
               'name': rule.get('SystemName'),
               'formula': rule.get('Formula'),
               'groups': rule_groups,
               'localization_rules': rule.get('LocalizationRules'),
               'compilation_sdk': rule.get('CompilationStatus', {}).get('SdkVersion'),
               'compilation_status': rule.get('CompilationStatus', {}).get('CompilationStatusId'),
               'deployment_status': rule.get('DeploymentStatus', '').lower()}
        ret["hash"] = sha256(str(ret.get('formula', '')).encode('utf-8')).hexdigest()

        self.log.info('status=success, action=get_rule, msg="Got rule {}", '
                      'hostname="{}", db="{}"'.format(rule_id, self.__kb_hostname, db_name))

        return ret

    def get_table_info(self, db_name: str, table_id: str) -> dict:
        """Получить описание табличного списка.

        :param db_name: Имя БД
        :param table_id: KB ID табличного списка
        :return: {'param1': value, 'param2': value}
        """

        headers = {'Content-Database': db_name,
                   'Content-Locale': 'RUS'}
        api_url = self.__api_table_info.format(table_id)
        url = f'https://{self.__kb_hostname}:{self.__kb_port}{api_url}'

        r = exec_request(self.__kb_session,
                         url,
                         method='GET',
                         timeout=self.settings.connection_timeout,
                         headers=headers)
        table = r.json()

        table_groups = []
        for i in table.get('Groups'):
            table_groups.append({'id': i.get('Id'), 'name': i.get('SystemName')})

        table_fields = []
        for i in table.get('Fields'):
            table_fields.append({'name': i.get('Name'),
                                 'mapping': i.get('Mapping'),
                                 'type_id': i.get('TypeId'),
                                 'primary_key': i.get('IsPrimaryKey'),
                                 'indexed': i.get('IsIndex'),
                                 'nullable': i.get('IsNullable')})

        ret = {'id': table.get('Id'),
               'guid': table.get('ObjectId'),
               'folder_id': table.get('Folder').get('Id') if table.get('Folder') is not None else None,
               'origin_id': table.get('OriginId'),
               'name': table.get('SystemName'),
               'size_max': table.get('MaxSize'),
               'size_typical': table.get('TypicalSize'),
               'ttl': table.get('Ttl'),
               'fields': table_fields,
               'description': table.get('Description'),
               'groups': table_groups,
               'fill_type': table.get('FillType').lower(),
               'pdql': table.get('PdqlQuery'),
               'asset_groups': table.get('AssetGroups'),
               'deployment_status': table.get('DeploymentStatus').lower()}

        self.log.info('status=success, action=get_table_info, msg="Got table {}", '
                      'hostname="{}", db="{}"'.format(table_id, self.__kb_hostname, db_name))

        return ret

    def get_table_data(self, db_name: str, table_id: str, filters: Optional[dict] = None) -> Iterator[dict]:
        """Получить содержимое табличного из KB. В KB только справочники могут
        содержать записи. Для доступа к данным иных типов таблиц необходимо
        использовать class Table.

        :param db_name: Имя БД
        :param table_id: KB ID табличного списка
        :param filters: KB фильтр записей в таблице. Спецификацию можно
            найти путем реверса WEB API
        :return: Iterator
        """
        api_url = self.__api_table_rows.format(table_id)

        url = f'https://{self.__kb_hostname}:{self.__kb_port}{api_url}'
        headers = {'Content-Database': db_name,
                   'Content-Locale': 'RUS'}
        params = {'sort': None}

        if filters is not None:
            params.update(filters)

        # Пачками выгружаем содержимое
        is_end = False
        offset = 0
        limit = self.settings.kb_objects_batch_size
        line_counter = 0
        start_time = get_metrics_start_time()
        while not is_end:
            ret = self.__iterate_table_rows(url, params, headers, offset, limit)
            if len(ret) < limit:
                is_end = True
            offset += limit
            for i in ret:
                line_counter += 1
                i.pop('Id')
                yield i
        took_time = get_metrics_took_time(start_time)

        self.log.info('status=success, action=get_table_data, msg="Query executed, response have been read", '
                      'hostname="{}", lines={}, db="{}"'.format(self.__kb_hostname, line_counter, db_name))
        self.log.info('hostname="{}", metric=get_table_data, took={}ms, lines={}'.format(self.__kb_hostname,
                                                                                         took_time,
                                                                                         line_counter))

    def __iterate_table_rows(self, url: str, params: dict, headers: dict, offset: int, limit: int):
        params['skip'] = offset
        params['take'] = limit
        rq = exec_request(self.__kb_session,
                          url,
                          method='POST',
                          timeout=self.settings.connection_timeout,
                          headers=headers,
                          json=params)
        response = rq.json()
        if response is None or 'Rows' not in response:
            self.log.error('status=failed, action=kb_objects_iterate, msg="KB data request return None or '
                           'has wrong response structure", '
                           'hostname="{}"'.format(self.__kb_hostname))
            raise Exception('KB data request return None or has wrong response structure')

        return response.get('Rows')

    def create_folder(self, db_name: str, name: str, parent_id: Optional[str] = None) -> str:
        """Создать папку для контента.

        :param db_name: Имя БД
        :param name: Имя создаваемой папки
        :param parent_id: Идентификатор родительской папки
        :return: ID созданной папки
        """
        params = {
            'name': name,
            'parentId': parent_id
        }
        headers = {'Content-Database': db_name,
                   'Content-Locale': 'RUS'}
        url = f'https://{self.__kb_hostname}:{self.__kb_port}{self.__api_folders}'

        r = exec_request(self.__kb_session,
                         url,
                         method='POST',
                         timeout=self.settings.connection_timeout,
                         headers=headers,
                         json=params)

        if r.status_code == 201:
            folder_id = r.json()
            self.get_folders_list(db_name, do_refresh=True)
            folder_path = self.get_folder_path_by_id(db_name, folder_id)
            self.log.info('status=success, action=create_folder, msg="created folder {} with id {}", '
                          'hostname="{}", db="{}"'.format(folder_path, folder_id, self.__kb_hostname, db_name))

        else:
            self.log.error('status=failed, action=create_folder, msg="failed to create folder {}", '
                           'hostname="{}", db="{}"'.format(name, self.__kb_hostname, db_name))

        return r.json()

    def delete_folder(self, db_name: str, folder_id: str):
        """Удалить папку.

        :param db_name: Имя БД
        :param folder_id: ID удаляемой папки
        :return: Объект Response
        """
        folder_path = self.get_folder_path_by_id(db_name, folder_id)

        headers = {'Content-Database': db_name,
                   'Content-Locale': 'RUS'}
        url = f'https://{self.__kb_hostname}:{self.__kb_port}{self.__api_folders}/{folder_id}'

        r = exec_request(self.__kb_session,
                         url,
                         method='DELETE',
                         timeout=self.settings.connection_timeout,
                         headers=headers)

        if r.status_code == 204:
            self.log.info('status=success, action=delete_folder, msg="deleted folder {} with id {}", '
                          'hostname="{}", db="{}"'.format(folder_path, folder_id, self.__kb_hostname, db_name))
            self.get_folders_list(db_name, do_refresh=True)
        else:
            self.log.error('status=failed, action=delete_folder, msg="failed to delete folder {} with id {}", '
                           'hostname="{}", db="{}"'.format(folder_path, folder_id, self.__kb_hostname, db_name))

        return r

    def create_co_rule(self, db_name: str, name: str, code: str, ru_desc: Optional[str],
                       folder_id: str, group_ids: Optional[list] = []) -> str:
        """Создать правило корреляции.

        :param db_name: Имя БД
        :param name: имя создаваемого правила корреляции
        :param code: код правила
        :param ru_desc: описание в русской локали
        :param folder_id: ID каталога, в который разместить правило
        :param group_ids: ID наборов установки, в которые включить
            правило
        :return: ID созданного правила
        """
        params = {
            'systemName': name,
            'formula': code,
            'description': {},
            'folderId': folder_id,
            'groupsToSave': group_ids,
            'localizationRulesToAdd': [],
            'mappingConflictAction': 'exception'
        }

        if ru_desc:
            params.update({
                'description': {
                    'RUS': ru_desc
                }
            })

        headers = {'Content-Database': db_name,
                   'Content-Locale': 'RUS'}
        url = f'https://{self.__kb_hostname}:{self.__kb_port}{self.__api_co_rules}'

        r = exec_request(self.__kb_session,
                         url,
                         method='POST',
                         timeout=self.settings.connection_timeout,
                         headers=headers,
                         json=params)

        if r.status_code == 201:
            self.log.info('status=success, action=create_co_rule, msg="created co rule {} with id {}", '
                          'hostname="{}", db="{}"'.format(name, r.json(), self.__kb_hostname, db_name))
        else:
            self.log.error('status=failed, action=create_co_rule, msg="failed to create co rule {}", '
                           'hostname="{}", db="{}"'.format(name, self.__kb_hostname, db_name))

        return r.json()

    def create_group(self, db_name: str, name: str, parent_id: Optional[str] = None) -> str:
        """Создать набор установки.

        :param db_name: Имя БД
        :param name: Имя набора установки
        :param parent_id: ID родительского набора установки
        :return: ID созданного набора установки
        """
        params = {
            'systemName': name,
            'parentGroupId': parent_id,
            'locales': []
        }

        headers = {'Content-Database': db_name,
                   'Content-Locale': 'RUS'}
        url = f'https://{self.__kb_hostname}:{self.__kb_port}{self.__api_groups}'

        r = exec_request(self.__kb_session,
                         url,
                         method='POST',
                         timeout=self.settings.connection_timeout,
                         headers=headers,
                         json=params)

        if r.status_code == 201:
            group_id = r.json()
            self.get_groups_list(db_name, do_refresh=True)
            group_path = self.get_group_path_by_id(db_name, group_id)
            self.log.info('status=success, action=create_group, msg="created group {} with id {}", '
                          'hostname="{}", db="{}"'.format(group_path, group_id, self.__kb_hostname, db_name))

        else:
            self.log.error('status=failed, action=create_group, msg="failed to create group {}", '
                           'hostname="{}", db="{}"'.format(name, self.__kb_hostname, db_name))

        return r.json()

    def delete_group(self, db_name: str, group_id: str):
        """Удалить набор установки.

        :param db_name: Имя БД
        :param group_id: ID удаляемого набора установки
        :return: Объект Response
        """
        group_name = self.get_group_path_by_id(db_name, group_id)

        headers = {'Content-Database': db_name,
                   'Content-Locale': 'RUS'}

        params = {'MappingConflictAction': 'exception'}

        url = f'https://{self.__kb_hostname}:{self.__kb_port}{self.__api_groups}/{group_id}'

        r = exec_request(self.__kb_session,
                         url,
                         method='DELETE',
                         timeout=self.settings.connection_timeout,
                         headers=headers,
                         json=params)

        if r.status_code == 204:
            self.log.info('status=success, action=delete_group, msg="deleted group {} with id {}", '
                          'hostname="{}", db="{}"'.format(group_name, group_id, self.__kb_hostname, db_name))
            self.get_groups_list(db_name, do_refresh=True)
        else:
            self.log.error('status=failed, action=delete_group, msg="failed to delete group {} with id", '
                           'hostname="{}", db="{}"'.format(group_name, group_id, self.__kb_hostname, db_name))

        return r

    def is_group_empty(self, db_name: str, group_id: str) -> bool:
        """Проверить есть ли данные в наборе установки.

        :param db_name: имя БД
        :param group_id: идентификатор набора установки
        :return: True - если в наборе установки нет контента
        """
        headers = {'Content-Database': db_name,
                   'Content-Locale': 'RUS'}

        params = {'skip': 0,
                  'folderId': None,
                  'filters': None,
                  'search': "",
                  'sort': [
                      {'name': 'objectId', 'order': 0, 'type': 0}
                  ],
                  'recursive': True,
                  'groupId': group_id,
                  'withoutGroups': False,
                  'take': 50
                  }

        url = f'https://{self.__kb_hostname}:{self.__kb_port}{self.__api_list_objects}'

        r = exec_request(self.__kb_session,
                         url,
                         method='POST',
                         timeout=self.settings.connection_timeout,
                         headers=headers,
                         json=params)

        if r.status_code == 201:

            num_rows = r.json().get('Count', 0)

            self.log.info('status=success, action=list_group, msg="group {} has {} rows", '
                          'hostname="{}", db="{}"'.format(group_id, num_rows, self.__kb_hostname, db_name))

            return True if num_rows == 0 else False

        else:
            self.log.error('status=failed, action=list_group, msg="failed to list group {}", '
                           'hostname="{}", db="{}"'.format(group_id, self.__kb_hostname, db_name))

    def get_group_path_by_id(self, db_name: str, folder_id: str) -> str:
        """Получить путь в дереве наборов установки по идентификатору набора
        установки.

        :param db_name: Имя БД
        :param folder_id: идентификатор набора установки
        :return: путь в дереве наборов установки вида
            root/child/grandchild
        """
        groups = self.get_groups_list(db_name)

        parent = groups[folder_id]['parent_id']
        name = groups[folder_id]['name']
        ret_path = self.get_group_path_by_id(db_name, parent) if parent else ''
        return '/'.join((ret_path, name)) if ret_path else name

    def get_group_id_by_path(self, db_name: str, search_path: str) -> str:
        """Получить идентификатор набора установки по пути в дереве.

        :param db_name: Имя БД
        :param search_path: Путь в формате root/child/grandchild
        :return: идентификатор набора установки
        """
        groups = self.get_groups_list(db_name)
        path_index = {}
        for current_id, group_data in groups.items():
            path = self.get_group_path_by_id(db_name, current_id)
            path_index[path] = current_id

        return path_index.get(search_path, '')

    def __get_group_children_tree(self, groups, current_id) -> list:
        children = groups[current_id]['children_ids'] if 'children_ids' in groups[current_id] else []
        retval = list(children)
        for child in children:
            grand_children = self.__get_group_children_tree(groups, child)
            retval.extend(grand_children)

        return retval

    def get_nested_group_ids(self, db_name: str, group_id: str) -> list:
        """Получить идентификаторы дочерних наборов установки.

        :param db_name: Имя БД
        :param group_id: идентификатор группы
        :return: список идентификаторов дочерних наборов установки
        """
        groups = dict(self.get_groups_list(db_name))
        for current_id, group_data in groups.items():
            parent_id = group_data['parent_id']
            if parent_id:
                if 'children_ids' not in groups[parent_id]:
                    groups[parent_id]['children_ids'] = []

                groups[parent_id]['children_ids'].append(current_id)

        return self.__get_group_children_tree(groups, group_id)

    def export_group(self, db_name: str, group_id: str, local_filepath: str,
                     export_format: Optional[str] = EXPORT_FORMAT_KB
                     ) -> int:
        """Экспортировать набор установки.

        :param db_name: имя БД
        :param group_id: ID набора установки
        :param local_filepath: файл в который сохранить набор установки
        :param export_format: формат экспорта (KB / SIEM Lite)
        :return: размер созданного файла
        """
        group_path = self.get_group_path_by_id(db_name, group_id)

        headers = {'Content-Locale': 'RUS'}

        params = {
            'format': export_format,
            'groupId': group_id,
            'mode': "group"
        }

        url = f'https://{self.__kb_hostname}:{self.__kb_port}{self.__api_export}/?contentDatabase={db_name}'

        r = exec_request(self.__kb_session,
                         url,
                         method='POST',
                         timeout=self.settings.connection_timeout,
                         headers=headers,
                         json=params)

        retval = 0
        if r.status_code == 201:

            # Не экспортировать пустой пак (в него попадают все ПТшные макросы)
            is_group_empty = self.is_group_empty(db_name, group_id)
            if not is_group_empty:
                with open(local_filepath, 'wb') as kbfile:
                    retval = kbfile.write(r.content)

                self.log.info('status=success, action=export_group, msg="group {} with id {} exported to {}", '
                              'hostname="{}", db="{}"'.format(group_path, group_id, local_filepath, self.__kb_hostname,
                                                              db_name))
            else:
                self.log.info('status=success, action=export_group, msg="group {} with id {} is empty", '
                              'hostname="{}", db="{}"'.format(group_path, group_id, self.__kb_hostname, db_name))


        else:
            self.log.error('status=failed, action=export_group, msg="failed to export group {}", '
                           'hostname="{}", db="{}"'.format(group_id, self.__kb_hostname, db_name))

        return retval

    def import_group(self, db_name: str, filepath: str, mode: Optional[str] = IMPORT_ADD_AND_UPDATE) -> int:
        """Импортировать набор установки.

        :param db_name: имя БД
        :param filepath: имя файла набора установки
        :param mode: режим импорта
        :return: response_code
        """
        headers = {'Content-Database': db_name,
                   'Content-Locale': 'RUS',
                   'Content-Type': 'application/octet-stream'}

        filename = os.path.basename(filepath)

        url = (f'https://{self.__kb_hostname}:{self.__kb_port}{self.__api_temp_file_storage_upload}?'
               f'fileName={filename}&storageType=Temp')

        uploaded_id = ""
        with open(filepath, 'rb') as kbfile:

            r = exec_request(self.__kb_session,
                             url,
                             method='POST',
                             timeout=self.settings.connection_timeout,
                             headers=headers,
                             data=kbfile,
                             )

            if r.status_code == 201:
                # Upload successful
                uploaded_id = r.json().get('UploadId')
                self.log.info('status=success, action=upload_file, msg="file {} uploaded", '
                              'hostname="{}", db="{}"'.format(filepath, self.__kb_hostname, db_name))
            else:
                self.log.error('status=failed, action=upload_file, msg="failed to upload file {}", '
                               'hostname="{}", db="{}"'.format(filepath, self.__kb_hostname, db_name))

        if uploaded_id:
            # make import

            headers = {'Content-Database': db_name,
                       'Content-Locale': 'RUS'}

            params = {
                'importMacros': False,
                'mode': mode,
                'uploadId': uploaded_id
            }

            url = f'https://{self.__kb_hostname}:{self.__kb_port}{self.__api_import}'

            r = exec_request(self.__kb_session,
                             url,
                             method='POST',
                             timeout=self.settings.connection_timeout,
                             headers=headers,
                             json=params,
                             )

            if r.status_code == 201:
                # Upload successful
                self.log.info('status=success, action=import_file, msg="file {} imported", '
                              'hostname="{}", db="{}"'.format(filepath, self.__kb_hostname, db_name))
            else:
                self.log.error('status=failed, action=import_file, msg="failed to import file {}", '
                               'hostname="{}", db="{}"'.format(filepath, self.__kb_hostname, db_name))

            return r.status_code
        else:
            return -1

    def create_group_path(self, db_name: str, group_path: str) -> str:
        """Последовательное создание пути в дереве наборов установки.

        :param db_name: Имя БД
        :param group_path: путь в дереве наборов установки
        :return: идентификатор листьевого набора установки
        """
        path_parts = group_path.split('/')
        for i in range(1, len(path_parts) + 1):
            parent_path = '/'.join(path_parts[0:i - 1])
            path = '/'.join(path_parts[0:i])
            group_id = self.get_group_id_by_path(db_name, path)
            if not group_id:
                parent_group_id = self.get_group_id_by_path(db_name, parent_path) or None
                self.create_group(db_name, path_parts[i - 1], parent_group_id)

        return self.get_group_id_by_path(db_name, group_path)

    def __get_linked_ids(self, objects):
        """Разбор ответа на запрос привязанных наборов установки.

        :param objects: ответ API MPSIEM
        :return:
        """
        linked = []
        for item in objects:
            if 'AssignedTo' in item and item['AssignedTo'] == 'All':
                if 'Id' in item:
                    linked.append(item['Id'])
            if 'Children' in item and item['Children']:
                linked.extend(self.__get_linked_ids(item['Children']))

        return linked

    def get_linked_groups(self, db_name: str, content_item_id: str) -> list:
        """Получить список идентификаторов связанных наборов установки для
        элемента контента.

        :param db_name: Имя БД
        :param content_item_id: идентификатор контента
        :return: список идентификаторов связанных наборов установки
        """
        headers = {'Content-Database': db_name,
                   'Content-Locale': 'RUS'}

        core_major_ver = int(self.auth.get_core_version().split('.')[0])

        if core_major_ver >= 24:
            params = {
                'selectedObjects': {
                    'ids': [content_item_id, ],
                    'selectionMode': 'Selected'
                },
                'filter': None
            }
        else:
            params = {
                'include': [content_item_id, ],
                'filter': None
            }

        url = f'https://{self.__kb_hostname}:{self.__kb_port}{self.__api_siem_objgroups_values}'

        r = exec_request(self.__kb_session,
                         url,
                         method='POST',
                         timeout=self.settings.connection_timeout,
                         headers=headers,
                         json=params,
                         )

        if r.status_code == 201:

            group_ids = self.__get_linked_ids(r.json())

            self.log.info('status=success, action=get_linked_groups, msg="Item {} linked to groups {}", '
                          'hostname="{}", db="{}"'.format(content_item_id, group_ids, self.__kb_hostname, db_name))

            return group_ids
        else:
            self.log.error('status=failed, action=get_linked_groups, msg="can not get group links for {}", '
                           'hostname="{}", db="{}"'.format(content_item_id, self.__kb_hostname, db_name))

    def link_content_to_groups(self, db_name: str, content_items_ids: list, group_ids: list):
        """Связать идентификаторы контента с идентификаторами наборов
        установки.

        :param db_name: Имя БД
        :param content_items_ids: идентификаторы контента
        :param group_ids: идентификаторы наборов установки
        :return:
        """

        # получить перечень обновляемых объектов
        content_item_names = [obj['name'] for obj in self.get_all_objects(db_name) if obj['id'] in content_items_ids]

        # получить имя набора установки с которым осуществляется связывание
        group_names = [group_data.get('name', '') for group_id, group_data in self.get_groups_list(db_name).items() if
                       group_id in group_ids]

        headers = {'Content-Database': db_name,
                   'Content-Locale': 'RUS'}

        core_major_ver = int(self.auth.get_core_version().split('.')[0])
        if core_major_ver >= 24:
            params = {
                'Operations': [
                    {
                        'Id': 'SiemObjectGroup',
                        'ValuesToSave': group_ids,
                        'ValuesToRemove': []
                    }
                ],
                'Entities': {
                    'selectedObjects': {
                        'ids': content_items_ids,
                        'selectionMode': 'Selected'
                    },
                    'filter': {}
                }
            }
        else:
            params = {
                'Operations': [
                    {
                        'Id': 'SiemObjectGroup',
                        'ValuesToSave': group_ids,
                        'ValuesToRemove': []
                    }
                ],
                'Entities': {
                    'include': content_items_ids,
                    'filter': {}
                }
            }

        url = f'https://{self.__kb_hostname}:{self.__kb_port}{self.__api_mass_operations}'

        r = exec_request(self.__kb_session,
                         url,
                         method='PUT',
                         timeout=self.settings.connection_timeout,
                         headers=headers,
                         json=params,
                         )

        if r.status_code == 200:
            self.log.info('status=success, action=link_content_to_groups, msg="{} linked to {}", '
                          'hostname="{}", db="{}"'.format(str(content_item_names), str(group_names), self.__kb_hostname,
                                                          db_name))
        else:
            self.log.error('status=failed, action=import_file, msg="can not link {} to {}", '
                           'hostname="{}", db="{}"'.format(str(content_item_names), str(group_names),
                                                           self.__kb_hostname, db_name))

    def process_kb_metadata(self, db_name, obj_map, kb_meta):
        if 'group_path' in kb_meta and kb_meta['group_path']:
            # Создать путь в дереве наборов установки
            group_id = self.create_group_path(db_name, kb_meta['group_path'])

            # Есть связанные элементы контента
            if 'kb_tree' in kb_meta and kb_meta['kb_tree']:
                contend_guid_strs = []
                for content_type in kb_meta['kb_tree']:
                    for content_path in kb_meta['kb_tree'][content_type]:
                        key = (content_type, content_path)

                        # Пробуем найти маппинг (Type, Path)->GUID
                        if key in obj_map:
                            contend_guid_strs.append(obj_map[key])
                        else:
                            self.log.error('status=failed, action=map_id_to_guid, msg="can not find object {}", '
                                           'hostname="{}", db="{}"'.format(key, self.__kb_hostname,
                                                                           db_name))

                if contend_guid_strs:
                    # Связать контент с набором установки
                    self.link_content_to_groups(db_name, contend_guid_strs, [group_id, ])

    def get_folder_path_by_id(self, db_name: str, folder_id: str) -> str:
        """Получить путь в дереве папок по ID папки.

        :param db_name: имя БД
        :param folder_id: ID папки
        :return: путь в дереве папок
        """
        folders = self.get_folders_list(db_name)
        parent = folders[folder_id]['parent_id']
        name = folders[folder_id]['name']
        ret_path = self.get_folder_path_by_id(db_name, parent) if parent else ''
        return '/'.join((ret_path, name)) if ret_path else name

    def get_folder_id_by_path(self, db_name: str, path: str) -> str:
        """Получить ID папки по пути в дереве папок.

        :param db_name: имя БД
        :param path: путь в дереве папок
        :return: ID папки
        """
        folders = self.get_folders_list(db_name)
        path_to_id_map = {self.get_folder_path_by_id(db_name, folder_id): folder_id for folder_id in folders}
        return path_to_id_map.get(path)

    def get_content_data_by_folder_id(self, db_name: str, folder_id) -> dict:
        """Получить данные по контенту лежащему в папке с заданным ID.

        :param db_name: имя БД
        :param folder_id: ID папки
        :return: словарь вида {'ID объекта' : 'Тип объекта'}
        """
        filters = {
            'folderId': folder_id,
            'filters': None,
            'search': "",
            'sort': [
                {'name': 'objectId', 'order': 0, 'type': 0}
            ],
            'recursive': False,
            'groupId': None,
            'withoutGroups': False
        }
        content = list(self.get_all_objects(db_name, filters))
        nested_data = {item['id']: item['object_kind'] for item in content if item['folder_id'] == folder_id}
        return nested_data

    def get_nested_folder_ids_by_folder_id(self, db_name: str, folder_id: str) -> list:
        """Получить идентификаторы дочерних папок по ID папки.

        :param db_name: имя БД
        :param folder_id: ID папки
        :return: идентификаторы вложенных папок
        """
        folders = self.get_folders_list(db_name)
        return [fold_id for fold_id, fold_data in folders.items() if fold_data['parent_id'] == folder_id]

    def move_folder(self, db_name: str, folder_id: str, dst_folder_id: str):
        """Переместить папку под другого родителя.

        :param db_name:
        :param folder_id:
        :param dst_folder_id:
        :return:
        """

        folder_path = self.get_folder_path_by_id(db_name, folder_id)
        dst_folder_path = self.get_folder_path_by_id(db_name, dst_folder_id)
        folder_name = folder_path.split('/')[-1]

        headers = {'Content-Database': db_name,
                   'Content-Locale': 'RUS'}
        url = f'https://{self.__kb_hostname}:{self.__kb_port}{self.__api_folders}/{folder_id}'

        params = {
            'id': folder_id,
            'name': folder_name,
            'parentId': dst_folder_id
        }

        r = exec_request(self.__kb_session,
                         url,
                         method='PUT',
                         timeout=self.settings.connection_timeout,
                         headers=headers,
                         json=params)

        if r.status_code == 200:
            self.log.info('status=success, action=move_folder, msg="folder {} moved to {}", '
                          'hostname="{}", db="{}"'.format(folder_path, dst_folder_path, self.__kb_hostname, db_name))
            self.get_folders_list(db_name, do_refresh=True)
        else:
            self.log.error('status=failed, action=move_folder, msg="failed to move folder {} to {}", '
                           'hostname="{}", db="{}"'.format(folder_path, dst_folder_path, self.__kb_hostname, db_name))

        return r

    def get_content_item(self, db_name: str, item_id: str, item_type: str) -> dict:
        """Получить элемент контента по типу и ID.

        :param db_name: имя БД
        :param item_id: ID элемента контента
        :param item_type: тип (CorrelationRule/AggregationRule/EnrichmentRule/NormalizationRule/TabularList)
        :return: dict с полями для элемента контента
        """

        headers = {'Content-Database': db_name,
                   'Content-Locale': 'RUS'}
        url = ('https://{hostname}:{port}{endpoint}/{item_type}/{item_id}'.
               format(hostname=self.__kb_hostname,
                      port=self.__kb_port,
                      endpoint=self.__api_siem,
                      item_type=KnowledgeBase.ITEM_TYPE_MAP.get(item_type),
                      item_id=item_id))

        r = exec_request(self.__kb_session,
                         url,
                         method='GET',
                         timeout=self.settings.connection_timeout,
                         headers=headers)

        if r.status_code == 200:
            self.log.info('status=success, action=get_content_item, msg="Get item type {} item id {}", '
                          'hostname="{}", db="{}"'.format(item_type, item_id, self.__kb_hostname, db_name))
        else:
            self.log.error('status=failed, action=get_content_item, msg="Failed to get item type {} item id {}", '
                           'hostname="{}", db="{}"'.format(item_type, item_id, self.__kb_hostname, db_name))

        return r.json()

    def move_content_item(self, db_name: str, item_id: str, item_type: str, dst_folder_id: str):
        """Переместить элемент контента в другую папку.

        :param db_name: имя БД
        :param item_id: ID элемента контента
        :param item_type: тип контента
        :param dst_folder_id: (CorrelationRule/AggregationRule/EnrichmentRule/NormalizationRule/TabularList)
        :return:
        """

        dst_folder_path = self.get_folder_path_by_id(db_name, dst_folder_id)

        item_data = self.get_content_item(db_name, item_id, item_type)
        item_name = item_data.get('SystemName', 'Unknown')

        put_content = {
            'folderId': dst_folder_id,
            'description': {'RUS': item_data.get('Description')} if item_data.get('Description') else {}
        }

        if item_type == 'TabularList':
            put_content.update({
                'userCanEditContent': item_data.get('UserCanEditContent'),
                'fields': [{
                    "id": d['Id'],
                    "name": d['Name'],
                    "typeId": d['TypeId'],
                    "isPrimaryKey": d['IsPrimaryKey'],
                    "isIndex": d['IsIndex'],
                    "isNullable": d['IsNullable'],
                    "mapping": d['Mapping']
                } for d in item_data.get('Fields')],
            })
        else:
            put_content.update({
                'systemName': item_data.get('SystemName'),
                'formula': item_data.get('Formula'),
            })

        headers = {'Content-Database': db_name,
                   'Content-Locale': 'RUS'}
        url = ('https://{hostname}:{port}{endpoint}/{item_type}/{item_id}'.
               format(hostname=self.__kb_hostname,
                      port=self.__kb_port,
                      endpoint=self.__api_siem,
                      item_type=KnowledgeBase.ITEM_TYPE_MAP.get(item_type),
                      item_id=item_id))

        r = exec_request(self.__kb_session,
                         url,
                         method='PUT',
                         timeout=self.settings.connection_timeout,
                         headers=headers,
                         json=put_content)

        if r.status_code == 200:
            self.log.info('status=success, action=move_content_item, msg={} {} moved to {}", '
                          'hostname="{}", db="{}"'.format(item_type, item_name, dst_folder_path,
                                                          self.__kb_hostname, db_name))
        else:
            self.log.error('status=failed, action=move_content_item, msg="Failed to move {} {} to {}", '
                           'hostname="{}", db="{}"'.format(item_type, item_name, dst_folder_path,
                                                           self.__kb_hostname, db_name))

    def delete_content_item(self, db_name: str, item_id: str, item_type: str):
        """Удаление элемента контента.

        :param db_name:  Имя БД
        :param item_id: ID элемента контента
        :param item_type: тип (CorrelationRule/AggregationRule/EnrichmentRule/NormalizationRule/TabularList)
        :return:
        """

        item_data = self.get_content_item(db_name, item_id, item_type)
        item_name = item_data.get('SystemName', 'Unknown')

        # Деинсталлировать установленный контент
        if item_data.get('DeploymentStatus') == KnowledgeBase.DEPLOYMENT_STATUS_INSTALLED:
            self.install_objects_sync(db_name, [item_id, ], do_remove=True)

        headers = {'Content-Database': db_name,
                   'Content-Locale': 'RUS'}
        url = ('https://{hostname}:{port}{endpoint}/{item_type}/{item_id}'.
               format(hostname=self.__kb_hostname,
                      port=self.__kb_port,
                      endpoint=self.__api_siem,
                      item_type=KnowledgeBase.ITEM_TYPE_MAP.get(item_type),
                      item_id=item_id))

        r = exec_request(self.__kb_session,
                         url,
                         method='DELETE',
                         timeout=self.settings.connection_timeout,
                         headers=headers)

        if r.status_code == 204:
            self.log.info('status=success, action=delete_content_item, msg="deleted {} {} with id {}", '
                          'hostname="{}", db="{}"'.format(item_type, item_name, item_id, self.__kb_hostname, db_name))
        else:
            self.log.error('status=failed, action=delete_content_item, msg="failed to delete {} {} with id {}", '
                           'hostname="{}", db="{}"'.format(item_type, item_name, item_id, self.__kb_hostname, db_name))

        return r

    def move_folder_content(self, db_name: str, src_folder_path: str, dst_folder_path: str):
        """Переместить всю начинку папки (дочерние папки и контент) в другую
        папку.

        :param db_name: имя БД
        :param src_folder_path: путь до исходной папки
        :param dst_folder_path: путь до папки назанчения
        :return:
        """
        src_folder_id = self.get_folder_id_by_path(db_name, src_folder_path)
        dst_folder_id = self.get_folder_id_by_path(db_name, dst_folder_path)

        child_folders = self.get_nested_folder_ids_by_folder_id(db_name, src_folder_id)
        child_content_items = self.get_content_data_by_folder_id(db_name, src_folder_id)

        # Move folders
        for child_folder_id in child_folders:
            self.move_folder(db_name, child_folder_id, dst_folder_id)

        # Move content items
        for child_content_id, child_content_type in child_content_items.items():
            self.move_content_item(db_name, child_content_id, child_content_type, dst_folder_id)

    def get_content_items_by_group_id(self, db_name: str, group_id: str, recursive: bool = True) -> list:
        """Получить элементы контента по ID набора установки.

        :param db_name: Имя БД
        :param group_id: ID набора установки
        :param recursive: получить элементы контента из дочерних наборов
            установки
        :return: элементы контента в виде списка dict
        """
        content_items = []
        group_ids = [group_id, ]

        if recursive:
            group_ids.extend(self.get_nested_group_ids(db_name, group_id))

        for gid in group_ids:
            content_objects = self.get_all_objects(db_name, filters=None, group_id=gid)
            content_items.extend(content_objects)

        return content_items

    def close(self):
        if self.__kb_session is not None:
            self.__kb_session.close()
