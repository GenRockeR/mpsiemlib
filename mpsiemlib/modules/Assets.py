import time
from datetime import datetime
from typing import List, Tuple, Optional, Iterator, Union

import pytz

from mpsiemlib.common import ModuleInterface, MPSIEMAuth, LoggingHandler, Settings
from mpsiemlib.common import exec_request, get_metrics_start_time, get_metrics_took_time


class Assets(ModuleInterface, LoggingHandler):
    """Assets module."""

    __api_scopes = '/api/scopes/v2/scopes'
    __api_assets_processing_input_groups = "/api/assets_processing/v2/assets_input/groups"
    __api_assets_processing_v2_groups = "/api/assets_processing/v2/groups"
    __api_assets_trm_groups_hierarchy = "/api/assets_temporal_readmodel/v2/groups/hierarchy"
    __api_assets_processing_v2_configuration = "/api/assets_processing/v2/assets_input/assets"
    __api_assets_processing_v2_check_created = "/api/assets_processing/v2/assets_input/assets/checkCreated"
    __api_assets_trm_grid = '/api/assets_temporal_readmodel/v1/assets_grid'
    __api_assets_trm_row_count = '/api/assets_temporal_readmodel/v1/assets_grid/row_count'
    __api_assets_trm_selection = '/api/assets_temporal_readmodel/v1/assets_grid/data'
    __api_assets_trm_export_csv = '/api/assets_temporal_readmodel/v1/assets_grid/export'
    __api_assets_trm_stored_queries_folders = '/api/assets_temporal_readmodel/v1/stored_queries/folders/queries'
    __api_assets_trm_stored_queries_query = '/api/assets_temporal_readmodel/v1/stored_queries/queries'
    __api_assets_v2_import_operation = '/api/assets_processing/v2/csv/import_operation'
    __api_assets_v1_removeassets = '/api/assets_processing/v1/asset_operations/removeAssets'

    def __init__(self, auth: MPSIEMAuth, settings: Settings):
        ModuleInterface.__init__(self, auth, settings)
        LoggingHandler.__init__(self)
        self.__core_session = auth.sessions['core']
        self.__core_hostname = auth.creds.core_hostname
        self.__core_version = auth.get_core_version()
        siem_tz = datetime.now(pytz.timezone(settings.local_timezone)).strftime('%z')
        self.__default_utc_offset = '{0}:{1}'.format(siem_tz[:-2], siem_tz[-2:])  # convert to +HH:MM
        self.__scopes = {}
        self.__groups = {}
        self.log.debug('status=success, action=prepare, msg="Assets Module init"')

    def get_scopes_list(self, do_refresh=False) -> dict:
        """Получить все инфраструктуры.

        :return: {'id': {'name': 'Инфраструктура по умолчанию',
            'tenant_id': '97267c62-1455-4db0-8c84-497faf9a679e'}}
        """
        self.log.debug('status=prepare, action=get_scopes_list, msg="Try to get scopes list", '
                       'hostname="{}"'.format(self.__core_hostname))

        if len(self.__scopes) != 0 and not do_refresh:
            return self.__scopes

        self.__scopes.clear()

        url = f"https://{self.__core_hostname}{self.__api_scopes}"
        r = exec_request(self.__core_session,
                         url,
                         method='GET',
                         timeout=self.settings.connection_timeout)
        response = r.json()

        for i in response:
            self.__scopes[i.get('id')] = {'name': i.get('name'),
                                          'tenant_id': i.get('tenantId')}

        self.log.info('status=success, action=get_scopes_list, msg="Got scopes {}", '
                      'hostname="{}"'.format(len(self.__scopes), self.__core_hostname))

        return self.__scopes

    def get_scope_id_by_name(self, scope_name: str, do_refresh=False) -> str:
        """Получить id инфраструктуры по имени.

        :param scope_name: Название инфраструктуры
        :param do_refresh: Обновить кэш
        :return: '00000000-0000-0000-0000-000000000005'
        """
        self.log.debug('status=prepare, action=get_scope_id_by_name, msg="Try to get id for scope {}", '
                       'hostname="{}"'.format(scope_name, self.__core_hostname))

        if do_refresh or len(self.__scopes) == 0:
            self.get_scopes_list(do_refresh=True)

        scope = [k for k, v in self.__scopes.items() if v['name'] == scope_name]

        return scope[0] if len(scope) != 0 else None

    def create_assets_request(self,
                              pdql: str,
                              group_ids: List[str] = None,
                              include_nested: bool = True,
                              utc_offset: str = None) -> str:
        """Создать поисковый pdql-запрос и получить токен для доступа к
        результатам.

        :param pdql: PDQL-запрос
        :param group_ids: Список ID групп в которых надо искать активы
        :param include_nested: Искать ли во вложенных группах
        :param utc_offset: Часовой пояс смещение +00:00, по умолчанию
            +03:00
        :return: Токен запроса
        """
        self.log.debug('status=prepare, action=create_assets_request, msg="Try to select assets with pdql {}", '
                       'hostname="{}"'.format(pdql, self.__core_hostname))

        url = f"https://{self.__core_hostname}{self.__api_assets_trm_grid}"

        params = {'pdql': pdql,
                  'selectedGroupIds': group_ids if group_ids is not None else [],
                  'additionalFilterParameters': {'groupIds': [], 'assetIds': []},
                  'includeNestedGroups': include_nested,
                  'utcOffset': utc_offset if utc_offset is not None else self.__default_utc_offset}

        r = exec_request(self.__core_session,
                         url,
                         method='POST',
                         timeout=self.settings.connection_timeout,
                         json=params)

        resp = r.json()
        # {'token': 'xxxxxx', 'isPotentiallySlow': True, 'hasTimepointPipe': False,
        # 'hasTimeseriesPipe': False,
        # 'fields': [{'name': '@Host', 'localizedName': 'Узел', 'type': 'assetInfo', 'origin': 'dataField'}]}

        token = resp.get('token')

        self.log.info('status=success, action=create_assets_request, msg="Got selection token {}", '
                      'hostname="{}"'.format(token, self.__core_hostname))

        return token

    def get_assets_request_size(self, token: str) -> int:
        """Получить количество записей по токену.

        :param token: Токен запроса
        :return: Кол-во записей
        """
        self.log.debug('status=prepare, action=get_assets_request_size, '
                       'msg="Try to get assets count from request with token {}", '
                       'hostname="{}"'.format(token, self.__core_hostname))

        url = f"https://{self.__core_hostname}{self.__api_assets_trm_row_count}"
        params = {'pdqlToken': token}
        r = exec_request(self.__core_session,
                         url,
                         method='GET',
                         timeout=self.settings.connection_timeout,
                         params=params)
        resp = r.json()
        count = int(resp.get('rowCount'))

        self.log.info('status=success, action=get_row_count, msg="Got row count for filter {}", '
                      'hostname="{}"'.format(count, self.__core_hostname))

        return count

    def get_assets_list_json(self, token: str) -> Iterator[dict]:
        """Получить список активов в JSON по токену запроса.

        :param token: Токен запроса
        :return: Словарь с атрибутами активов
        """
        self.log.debug('status=prepare, action=get_assets_list_json, msg="Try to iterate assets by token {}", '
                       'hostname="{}"'.format(token, self.__core_hostname))

        url = f"https://{self.__core_hostname}{self.__api_assets_trm_selection}"
        params = {'pdqlToken': token}

        is_end = False
        offset = 0
        limit = self.settings.assets_batch_size
        line_counter = 0
        start_time = get_metrics_start_time()
        while not is_end:
            ret = self.__iterate_assets(url, params, offset, limit)
            if len(ret) < limit:
                is_end = True
            offset += limit
            for i in ret:
                line_counter += 1
                yield i

        took_time = get_metrics_took_time(start_time)

        self.log.info('status=success, action=get_assets_list_json, msg="Query executed, response have been read", '
                      'hostname="{}", lines={}'.format(self.__core_hostname, line_counter))
        self.log.info('hostname="{}", metric=get_assets_list_json, took={}ms, objects={}'.format(self.__core_hostname,
                                                                                                 took_time,
                                                                                                 line_counter))

    def __iterate_assets(self, url, params, offset, limit):
        params['offset'] = offset
        params['limit'] = limit
        rq = exec_request(self.__core_session,
                          url,
                          method='GET',
                          timeout=self.settings.connection_timeout,
                          params=params)
        response = rq.json()
        if response is None or 'records' not in response:
            self.log.error('status=failed, action=iterate_assets, msg="Assets data request return None or '
                           'has wrong response structure", '
                           'hostname="{}"'.format(self.__core_hostname))
            raise Exception('Assets data request return None or has wrong response structure')

        self.log.debug(f'Iterate assets, count={len(response.get("records"))}, offset={offset}, limit={limit}')

        return response.get('records')

    def get_assets_list_csv(self, token: str) -> Iterator[str]:
        """Получить список активов в CSV по токену запроса.

        :param token: Токен запроса
        :return: Строка CSV
        """
        self.log.debug('status=prepare, action=get_assets_csv, msg="Try to iterate assets csv by token {}", '
                       'hostname="{}"'.format(token, self.__core_hostname))

        iterator = self.get_assets_list_stream(token)

        line_counter = 0
        start_time = get_metrics_start_time()
        for line in iterator:
            if line:
                line_counter += 1
                yield line
        took_time = get_metrics_took_time(start_time)

        self.log.info('status=success, action=get_assets_list_csv, msg="Query executed, response have been read", '
                      'hostname="{}", lines={}'.format(self.__core_hostname, line_counter))
        self.log.info('hostname="{}", metric=get_assets_csv, took={}ms, objects={}'.format(self.__core_hostname,
                                                                                           took_time,
                                                                                           line_counter))

    def get_assets_list_stream(self, token: str) -> Iterator:
        """Получить список активов в виде бинарного потока CSV.

        :param token: Токен запроса
        :return: Поток
        """
        url = 'https://{}{}'.format(self.__core_hostname, self.__api_assets_trm_export_csv)
        params = {'pdqlToken': token}

        r = exec_request(self.__core_session,
                         url,
                         method='GET',
                         timeout=self.settings.connection_timeout,
                         params=params,
                         stream=True)

        if r.encoding is None:
            r.encoding = 'utf-8-sig'

        return r.iter_lines(decode_unicode=True)

    def import_assets_from_csv(self,
                               content: bytes,
                               scope_id: str,
                               group_id: str, timeout: int = 360) -> Tuple[bool, int, Optional[str]]:
        """Импорт активов в MP из CSV.

        :param content: Тело CSV для загрузки
        :param scope_id: ID инфраструктуры, куда импортируются активы
        :param group_id: ID группы, куда импортируются активы
        :param timeout: Время ожидания окончания загрузки (сек)
        :return: status, count, errors_log
        """
        self.log.debug('status=prepare, action=import_assets_from_csv, '
                       'msg="Try to import new assets from CSV", '
                       'hostname="{}"'.format(self.__core_hostname))

        success_install = False
        error_log = None
        import_status = None

        resp = self.__import_assets_from_csv_prepare(content, scope_id)

        if resp['id'] is not None:
            if resp['isLogFileCreated']:
                error_log = self.__import_assets_get_logfile(resp['id'])
            resp2 = self.__import_assets_from_csv_start(resp['id'], group_id)
            if resp2 == 200:
                for i in range(int(round(timeout / 10))):
                    time.sleep(10)
                    self.log.debug('Try to check operation status')
                    import_status = self.__import_assets_get_status(resp['id'])
                    if import_status.get('state') == 'completed':
                        success_install = True
                        break

        if not success_install:
            self.log.error('status=failed, action=import_assets_from_csv, msg="Can not import assets", '
                           'error="{}", hostname="{}"'.format(error_log, self.__core_hostname))

        counter_imported = import_status.get('succeedCount')
        self.log.info('status=success, action=import_assets_from_csv, '
                      'msg="Assets have been imported", imported_assets={}, updated_groups="{}", '
                      'hostname="{}"'.format(counter_imported,
                                             len(import_status.get('updatedGroups', [])),
                                             self.__core_hostname))

        return success_install, counter_imported, error_log

    def __import_assets_from_csv_prepare(self, content, scope_id: str) -> dict:
        """Подготовить операцию импорта.

        :param content: Тело CSV для загрузки
        :param scope_id: ID инфраструктуры, куда импортируются активы
        :return: {'id': '328cbed6-6625-41c1-aea6-d33eb034c260',
            'isLogFileCreated': False, 'rowsCountExceeded': False,
            'validRowsCount': 1, 'totalRowsCount': 1}
        """
        self.log.debug('status=prepare, action=import_assets_from_csv_prepare, '
                       'msg="Try to upload CSV with new assets", '
                       'hostname="{}"'.format(self.__core_hostname))

        url = f'https://{self.__core_hostname}{self.__api_assets_v2_import_operation}?scopeId={scope_id}'
        files = {'upfile': ('body', content, 'application/octet-stream')}

        r = exec_request(self.__core_session,
                         url,
                         method='POST',
                         timeout=self.settings.connection_timeout,
                         files=files)

        resp = r.json()

        self.log.info('status=success, action=import_assets_from_csv_prepare, '
                      'msg="Selected assets uploaded", valid_rows={}, total_rows={}, '
                      'hostname="{}"'.format(resp['validRowsCount'], resp['totalRowsCount'], self.__core_hostname))

        return resp

    def __import_assets_from_csv_start(self, operation_id: str, group_id: str) -> int:
        """Запуск импорта ранее загруженных данных.

        :param operation_id: ID операции загрузки
        :param group_id: ID группы, куда импортируются активы
        :return: status_code
        """
        self.log.debug('status=prepare, action=import_assets_from_csv_start, '
                       'msg="Try to insert new assets into MP", '
                       'hostname="{}"'.format(self.__core_hostname))

        url = f'https://{self.__core_hostname}{self.__api_assets_v2_import_operation}/{operation_id}/start'
        params = {'groupsId': [group_id]}

        r = exec_request(self.__core_session,
                         url,
                         method='POST',
                         timeout=self.settings.connection_timeout,
                         json=params)

        self.log.info('status=success, action=import_assets_from_csv_start, msg="Import operation started", '
                      'hostname="{}"'.format(self.__core_hostname))

        return r.status_code

    def __import_assets_get_status(self, operation_id: str) -> dict:
        """Получить статус операции импорта.

        :return: {"state":"inprogress","succeedCount":null,"updatedGroups":null,"errorModel":null}
        """

        url = f'https://{self.__core_hostname}{self.__api_assets_v2_import_operation}/{operation_id}/state'

        r = exec_request(self.__core_session,
                         url,
                         method='GET',
                         timeout=self.settings.connection_timeout)
        resp = r.json()

        self.log.info('status=success, action=get_import_status, msg="Check import operation status: {}", '
                      'hostname="{}"'.format(resp['state'], self.__core_hostname))

        return resp

    def __import_assets_get_logfile(self, operation_id: str) -> str:
        """Получить журнал ошибок :return: csv-formatted list of problems."""

        url = f'https://{self.__core_hostname}{self.__api_assets_v2_import_operation}/{operation_id}/logfile'

        r = exec_request(self.__core_session,
                         url,
                         method='GET',
                         timeout=self.settings.connection_timeout)

        self.log.info('status=success, action=get_import_logfile, msg="Got import logfile.", '
                      'hostname="{}"'.format(self.__core_hostname))

        return r.content.decode("utf-8")

    def import_assets_get_groups(self) -> list:
        """Получить список групп, куда можно проводить импорт.

        :return: ['12f04fc3-3e00-0001-0000-000000000006',
            '12e9a858-b700-0001-0000-000000000002']
        """

        url = f'https://{self.__core_hostname}{self.__api_assets_processing_input_groups}'
        r = exec_request(self.__core_session,
                         url,
                         method='GET',
                         timeout=self.settings.connection_timeout)
        resp = r.json()

        self.log.info('status=success, action=get_import_groups, msg="Got input groups list: {}", '
                      'hostname="{}"'.format(resp, self.__core_hostname))

        return resp

    def create_group_dynamic(self, parent_id: str, group_name: str, predicate: str) -> str:
        """Создать динамическую группу.

        :param parent_id: ID родительской группы
        :param group_name: Название группы
        :param predicate: Фильтр динамической группы
        :return: '12f04fc3-3e00-0001-0000-000000000006'
        """
        self.log.debug('status=prepare, action=create_group_dynamic, '
                       'msg="Try to create dynamic group", '
                       'hostname="{}"'.format(self.__core_hostname))

        url = f'https://{self.__core_hostname}{self.__api_assets_processing_v2_groups}'
        if int(self.__core_version.split('.')[0]) < 27:
            params = {'name': group_name, 'parentId': parent_id, 'groupType': 'dynamic',
                      'predicate': predicate,
                      'metrics': {'td': 'ND', 'cdp': 'ND', 'cr': 'ND', 'ir': 'ND', 'ar': 'ND'},
                      'organizationInformation': {},
                      'organizationInfrastructure': {}}
        else:
            params = {
                'name': group_name, 'parentId': parent_id, 'groupType': 'dynamic', 'predicate': predicate,
                'metadata': [],
                'organizationInformation': {},
                'organizationInfrastructure': {}
            }

        r = exec_request(self.__core_session,
                         url,
                         method='POST',
                         timeout=self.settings.connection_timeout,
                         json=params)
        resp = r.json()

        if 'operationId' not in resp:
            raise Exception(f'operationId not found in response "{resp}"')

        operation_id = resp['operationId']
        group_id = self.__group_operation_status(operation_id)

        self.log.info('status=success, action=create_dynamic_group, '
                      'msg="Dynamic group have been created.", operation_id="{}", group_id="{}", '
                      'hostname="{}"'.format(operation_id, group_id, self.__core_hostname))

        return group_id

    def edit_group_dynamic(self, group_id: str, predicate: str) -> str:
        """Редактировать динамическую группу.

        :param group_id: ID редактируемой группы
        :param predicate: Фильтр динамической группы
        :return: '12f04fc3-3e00-0001-0000-000000000006'
        """
        self.log.debug('status=prepare, action=edit_group_dynamic, '
                       'msg="Try to edit dynamic group", '
                       'hostname="{}"'.format(self.__core_hostname))

        url = "https://{}{}".format(self.__core_hostname,
                                    '{}/{}'.format(self.__api_assets_processing_v2_groups, group_id))
        params = [{"type": "SetPredicateGroupCommand",
                   "value": predicate}]

        r = exec_request(self.__core_session,
                         url,
                         method='PUT',
                         timeout=self.settings.connection_timeout,
                         json=params)
        resp = r.json()

        if 'operationId' not in resp:
            raise Exception('operationId not found in response "{}"'.format(resp))

        operation_id = resp['operationId']
        group_id = self.__group_operation_status(operation_id)

        self.log.info('status=success, action=edit_group_dynamic, '
                      'msg="Dynamic group has been edited.", operation_id="{}", group_id="{}", '
                      'hostname="{}"'.format(operation_id, group_id, self.__core_hostname))

        return group_id

    def create_group_static(self, parent_id: str, group_name: str) -> str:
        """Создать статическую группу.

        :param parent_id: ID родительской группы
        :param group_name: Название группы
        :return: '12f04fc3-3e00-0001-0000-000000000006'
        """
        self.log.debug('status=prepare, action=create_group_static, '
                       'msg="Try to create static group", '
                       'hostname="{}"'.format(self.__core_hostname))

        url = f'https://{self.__core_hostname}{self.__api_assets_processing_v2_groups}'
        if int(self.__core_version.split('.')[0]) < 27:
            params = {'name': group_name, 'parentId': parent_id, 'groupType': 'static',
                      'metrics': {'td': 'ND', 'cdp': 'ND', 'cr': 'ND', 'ir': 'ND', 'ar': 'ND'},
                      'organizationInformation': {},
                      'organizationInfrastructure': {}}
        else:
            params = {'name': group_name, 'parentId': parent_id, 'groupType': 'static',
                      'metadata': [], 'organizationInformation': {}, 'organizationInfrastructure': {}}

        r = exec_request(self.__core_session,
                         url,
                         method='POST',
                         timeout=self.settings.connection_timeout,
                         json=params)
        resp = r.json()

        if 'operationId' not in resp:
            raise Exception(f'operationId not found in response "{resp}"')

        operation_id = resp['operationId']
        group_id = self.__group_operation_status(operation_id)

        self.log.info('status=success, action=create_dynamic_group, '
                      'msg="Static group have been created.", operation_id="{}", group_id="{}", '
                      'hostname="{}"'.format(operation_id, group_id, self.__core_hostname))

        return group_id

    def delete_group(self, group_id: str) -> bool:
        """Удалить группы.

        :param group_id: ID групп для удаления
        :return: status_code
        """
        self.log.debug('status=prepare, action=delete_group, '
                       'msg="Try to delete group", '
                       'hostname="{}"'.format(self.__core_hostname))

        url = f'https://{self.__core_hostname}{self.__api_assets_processing_v2_groups}/removeOperation'
        params = {'groupIds': [group_id]}

        r = exec_request(self.__core_session,
                         url,
                         method='POST',
                         timeout=self.settings.connection_timeout,
                         json=params)
        resp = r.json()

        if 'operationId' not in resp:
            raise Exception(f'operationId not found in response "{resp}"')

        operation_id = resp['operationId']
        status = self.__group_operation_status(operation_id, 'remove')
        operation_status = (status['succeedCount'] == 1)

        self.log.info('status=success, action=delete_group, msg="Group deleted", status={}, report="{}", '
                      'hostname="{}"'.format(operation_status, status, self.__core_hostname))

        return operation_status

    def __group_operation_status(self,
                                 operation_id: str,
                                 operation_type: str = 'create',
                                 timeout: int = 360) -> Union[str, dict]:

        operation = 'operations' if operation_type == 'create' else 'removeOperation'

        url = f"https://{self.__core_hostname}{self.__api_assets_processing_v2_groups}/{operation}/{operation_id}"
        r = None
        for i in range(int(round(timeout / 10))):
            time.sleep(10)
            self.log.debug('Try to check operation status')
            r = exec_request(self.__core_session,
                             url,
                             method='GET',
                             timeout=self.settings.connection_timeout)
            if r.status_code == 200:
                break

        resp = r.text.strip('"') if operation_type == 'create' else r.json()

        self.log.debug('status=success, action=group_operation_status, msg="Got operation {} status: {}", '
                       'hostname="{}"'.format(operation_id, resp, self.__core_hostname))

        return resp

    def get_groups_hierarchy(self) -> List[dict]:
        """Получить иерархию групп.

        :return: [{'id': '00000000-0000-0000-0000-000000000002', 'name':
            'Root', 'groupType': 'static', 'isReadOnly': True,
            'isRemovable': False, 'isRoot': True, 'isInvalidPredicate':
            False, 'isSlow': False, 'treePath': 'Root', 'children':
            [{'id': '12e9a858-b700-0001-0000-000000000002', 'name':
            'AD', 'groupType': 'dynamic', 'isReadOnly': False,
            'isRemovable': True, 'isRoot': False, 'isInvalidPredicate':
            False, 'isSlow': False, 'treePath': 'AD', 'children': []}]
        """

        self.log.debug('status=prepare, action=get_groups_hierarchy, msg="Try to get groups tree", '
                       'hostname="{}"'.format(self.__core_hostname))

        url = f'https://{self.__core_hostname}{self.__api_assets_trm_groups_hierarchy}'
        r = exec_request(self.__core_session,
                         url,
                         method='GET',
                         timeout=self.settings.connection_timeout)
        resp = r.json()

        self.log.info('status=success, action=get_groups_hierarchy, msg="Got input groups", '
                      'hostname="{}"'.format(self.__core_hostname))

        return resp

    def get_groups_list(self, do_refresh=False) -> dict:
        """Получить список всех групп активов :param do_refresh: bool.

        :return: {"id": {"parent_id":
            "00000000-0000-0000-0000-000000000002", "name":
            "value","type": "static","is_readonly": True,
            "is_removable": False, "is_invalid_predicate": False,
            "is_slow": False}}
        """
        self.log.debug('status=prepare, action=get_groups_hierarchy, msg="Try to get groups list", '
                       'hostname="{}"'.format(self.__core_hostname))

        if len(self.__groups) != 0 and not do_refresh:
            return self.__groups

        tree = self.get_groups_hierarchy()
        self.__iterate_groups_tree(tree)

        self.log.info('status=success, action=get_groups_list, msg="Got {} groups", '
                      'hostname="{}"'.format(len(self.__groups), self.__core_hostname))

        return self.__groups

    def __iterate_groups_tree(self, root_node, parent_id=None):
        for i in root_node:
            node_id = i.get('id')
            self.__groups[node_id] = {'parent_id': parent_id,
                                      'name': i.get('name'),
                                      'type': i.get('groupType'),
                                      'is_readonly': i.get('isReadOnly', False),
                                      'is_removable': i.get('isRemovable', False),
                                      'is_invalid_predicate': i.get('isInvalidPredicate', False),
                                      'is_slow': i.get('isSlow', False)}
            node_children = i.get('children')
            if node_children is not None and len(node_children) != 0:
                self.__iterate_groups_tree(node_children, node_id)

    def get_group_id_by_name(self, group_name: str, do_refresh=False) -> str:
        """Получить id группы по имени. Регистр важен.

        :param group_name: Имя группы
        :param do_refresh: Обновить кэш
        :return: '00000000-0000-0000-0000-000000000005'
        """
        self.log.debug('status=prepare, action=get_groups_hierarchy, msg="Try to get groups id by name {}", '
                       'hostname="{}"'.format(group_name, self.__core_hostname))

        if do_refresh or len(self.__groups) == 0:
            self.get_groups_list(do_refresh=True)

        group_id = None
        for k, v in self.__groups.items():
            if v.get('name') == group_name:
                group_id = k
                break

        return group_id

    def __delete_assets_by_ids(self, asset_ids: list) -> str:
        """Удаление активов по id.

        :param asset_ids: Список ID активов, которые необходимо удалить
        :return: {"operationId":"16b577e1-3900-a001-0000-000000000e79"}
        """
        self.log.debug('status=prepare, action=__delete_assets_by_ids, '
                       'msg="Try to delete {} asset(s)", '
                       'hostname="{}"'.format(len(asset_ids), self.__core_hostname))

        url = f'https://{self.__core_hostname}{self.__api_assets_v1_removeassets}'
        params = {'assetsIds': asset_ids}

        r = exec_request(self.__core_session,
                         url,
                         method='POST',
                         timeout=self.settings.connection_timeout,
                         json=params)
        resp = r.json()

        self.log.info('status=success, action=__delete_assets_by_ids, '
                      'msg="Deleting started", operation_id={}, '
                      'hostname="{}"'.format(resp['operationId'], self.__core_hostname))

        return resp.get('operationId')

    def __remove_assets_get_status(self, operation_id: str) -> dict:
        """Получить статус операции удаления.

        :return: {"type":"AssetsOperationResult","totalCount":2,"succeedCount":2,"failedCount":0}
        """

        url = f"https://{self.__core_hostname}{self.__api_assets_v1_removeassets}?operationId={operation_id}"

        r = exec_request(self.__core_session,
                         url,
                         method='GET',
                         timeout=self.settings.connection_timeout)

        if r.status_code == 200:
            resp = r.json()
            self.log.info('status=success, action=__remove_assets_get_status, msg="Check remove operation status: {}", '
                          'hostname="{}"'.format(resp, self.__core_hostname))
            return resp
        else:
            self.log.info(
                'status=success, action=__remove_assets_get_status, msg="Check remove operation status: HTTP:{}", '
                'hostname="{}"'.format(r.status_code, self.__core_hostname))

            return None

    def delete_assets_by_ids(self, asset_ids: list) -> dict:
        """Удаление активов по id.

        :param asset_ids: Список ID активов, которые необходимо удалить
        :return: {"type":"AssetsOperationResult","totalCount":2,"succeedCount":2,"failedCount":0}
        """

        operation_id = self.__delete_assets_by_ids(asset_ids)

        if operation_id is not None:
            for i in range(30):
                time.sleep(1)
                status = self.__remove_assets_get_status(operation_id=operation_id)
                self.log.warning(f'status: {status}')
                if status is not None:
                    break

        self.log.info('status=success, action=delete_assets_by_ids, '
                      'msg="Deleting finished", status={}, '
                      'hostname="{}"'.format(status, self.__core_hostname))
        return status

    def __change_asset_configuration_by_id(self, asset_id: str, params: dict) -> dict:
        """Получить статус операции изменения актива.

        :return: {"type":"AssetsOperationResult","totalCount":2,"succeedCount":2,"failedCount":0}
        """

        url = "https://{}{}/{}".format(self.__core_hostname, self.__api_assets_processing_v2_configuration, asset_id)

        r = exec_request(self.__core_session,
                         url,
                         method='PUT',
                         timeout=self.settings.connection_timeout,
                         json=params)

        if r.status_code == 200:
            resp = r.json()
            self.log.info(
                'status=success, action=__change_asset_configuration_by_id, msg="Check change operation status: {}", '
                'hostname="{}"'.format(resp, self.__core_hostname))
            return resp.get('ticketId')
        else:
            self.log.info(
                'status=success, action=__change_asset_configuration_by_id, msg="Check change operation status: HTTP:{}", '
                'hostname="{}"'.format(r.status_code, self.__core_hostname))

            return None

    def __change_assets_get_status(self, ticket_id: str) -> dict:
        """Получить статус операции удаления.

        :return: {"type":"AssetsOperationResult","totalCount":2,"succeedCount":2,"failedCount":0}
        """

        url = "https://{}{}?ticketId={}".format(self.__core_hostname, self.__api_assets_processing_v2_check_created,
                                                ticket_id)

        r = exec_request(self.__core_session,
                         url,
                         method='GET',
                         timeout=self.settings.connection_timeout)

        if r.status_code == 200:
            resp = r.json()
            self.log.info('status=success, action=__change_assets_get_status, msg="Check edit operation status: {}", '
                          'hostname="{}"'.format(resp, self.__core_hostname))
            return resp
        else:
            self.log.debug(
                'status=success, action=__change_assets_get_status, msg="Check edit operation status: HTTP:{}", '
                'hostname="{}"'.format(r.status_code, self.__core_hostname))

            return None

    def change_asset_configuration_by_id(self, asset_id: str, config: dict) -> dict:
        """Изменение активов по id.

        :param asset_ids: ID актива, который надо сконфигурировать
        :return: {"isSuccessful":true,"assetId":"17c930dc-0340-0001-0000-000000000002"}
        """

        ticket_id = self.__change_asset_configuration_by_id(asset_id, config)

        if ticket_id is not None:
            for i in range(15):
                time.sleep(2)
                status = self.__change_assets_get_status(ticket_id=ticket_id)
                self.log.warning("status: {}".format(status))
                if status is not None:
                    break

        self.log.info('status=success, action=change_asset_configuration_by_id, '
                      'msg="Editing finished", status={}, '
                      'hostname="{}"'.format(status, self.__core_hostname))
        return status

    def get_asset_configuration_by_id(self, asset_id: str) -> dict:
        """Получение информации об активе по id.

        :param asset_id: ID интересующего актива
        :return: { dict with asset config }
        """

        resp = {}

        url = "https://{}{}/{}".format(self.__core_hostname, self.__api_assets_processing_v2_configuration, asset_id)
        r = exec_request(self.__core_session,
                         url,
                         method='GET',
                         timeout=self.settings.connection_timeout)
        if r.status_code == 200:
            resp = r.json()
            self.log.info('status=success, action=get_asset_configuration_by_id, msg="Got asset configuration", '
                          'hostname="{}"'.format(self.__core_hostname))
            return resp
        else:
            self.log.info(
                'status=success, action=__remove_assets_get_status, msg="Check remove operation status: HTTP:{}", '
                'hostname="{}"'.format(r.status_code, self.__core_hostname))
        return resp

    def get_queries(self) -> dict:
        """Получить все запросы.

        :return: {'id': {'name': 'Инфраструктура по умолчанию',
            'tenant_id': '97267c62-1455-4db0-8c84-497faf9a679e'}}
        """
        url = f'https://{self.__core_hostname}{self.__api_assets_trm_stored_queries_folders}'
        r = exec_request(self.__core_session,
                         url,
                         method='GET',
                         timeout=self.settings.connection_timeout)
        response = r.json()

        self.log.info('status=success, action=get_queries, msg="Got queries {}", '
                      'hostname="{}"'.format(len(response), self.__core_hostname))

        return response['nodes']

    def get_query_by_id(self, queryid: str) -> dict:
        """Получить запрос по id.

        :return: {"id":"23d2ffe845be453b885b97ff69bf7604","displayName":"Test__Tests.29","folderId":"994b5d4f0b394fc4842e4cdeeb560d69",
                  "filterId":null,"filterPdql":"qsearch(\"70f5ef43-196f-49d7-b5cb-46ae008376f7.com\")","selectionId":null,
                  "selectionPdql":"select(@Host, Host.OsName, Host.Softs.Name, Host.@UpdateTime) | sort(@Host ASC) | group(Host.OsName, COUNT(*))",
                  "isInvalid":false,"isDeleted":false,"type":"user"}
        """
        url = f'https://{self.__core_hostname}{self.__api_assets_trm_stored_queries_query + "/" + queryid}'
        r = exec_request(self.__core_session,
                         url,
                         method='GET',
                         timeout=self.settings.connection_timeout)
        response = r.json()

        self.log.info('status=success, action=get_query_by_id, msg="Got query {}", '
                      'hostname="{}"'.format(response['displayName'], self.__core_hostname))

        return response

    def create_query_folder(self, parent_id: str, folder_name: str) -> dict:
        """Создать папку для запросов.

        :param parent_id: ID родительской папки
        :param folder_name: Название папки
        :return: {"id":"6224c6bca5b64e6c98825cc336be19e8","displayName":"a / b","parentId":"c51f8e849598459cb0f352280a1ccf8c","type":"common"}
        """
        #        self.log.debug('status=prepare, action=create_query_folder, '
        #                       'msg="Try to create static group", '
        #                       'hostname="{}"'.format(self.__core_hostname))

        url = f'https://{self.__core_hostname}{self.__api_assets_trm_stored_queries_folders}'
        params = {'displayName': folder_name, 'parentId': parent_id}
        r = exec_request(self.__core_session,
                         url,
                         method='POST',
                         timeout=self.settings.connection_timeout,
                         json=params)

        resp = r.json()

        self.log.info('status=success, action=create_query_folder, '
                      'msg="Query folder created", Name="{}", type="{}", '
                      'hostname="{}"'.format(resp['displayName'], resp['type'], self.__core_hostname))

        return resp

    def create_query(self, folder_id: str, query_name: str, filterPdql: str, selectionPdql: str) -> dict:
        """Создать запрос в папке.

        :param folder_id: ID родительской папки
        :paraq query_name: Название запроса
        :paraq filterPdql: фильтр
        :paraq selectionPdql: выборка
        :return: {"id":"5279a28a6bdb464d9c5fcbaba08284ff","displayName":"xxxx","folderId":"a50a65f8a5c04c2785b0515d84135007","filterId":null,"filterPdql":null,"selectionId":null,"selectionPdql":"zzzz","isInvalid":false,"isDeleted":false,"type":"user"}
        """

        url = f'https://{self.__core_hostname}{self.__api_assets_trm_stored_queries_query}'

        params = {'displayName': query_name, 'folderId': folder_id, 'selectionPdql': selectionPdql,
                  'filterPdql': filterPdql}

        r = exec_request(self.__core_session,
                         url,
                         method='POST',
                         timeout=self.settings.connection_timeout,
                         json=params)
        resp = r.json()

        self.log.info('status=success, action=create_query, '
                      'msg="Query created", Name="{}", type="{}", '
                      'hostname="{}"'.format(resp['displayName'], resp['type'], self.__core_hostname))

        return resp

    def update_query(self, query_id: str, folder_id: str, query_name: str, filterPdql: str, selectionPdql: str) -> dict:
        """Обновить запрос в папке.

        :param query_id: query id
        :param folder_id: ID родительской папки :paraq query_name:
            Название запроса :paraq filterPdql: фильтр :paraq
            selectionPdql: выборка
        :return: request response
        """

        url = f'https://{self.__core_hostname}{self.__api_assets_trm_stored_queries_query + "/" + query_id}'

        params = {'id': query_id, 'displayName': query_name, 'folderId': folder_id, 'selectionPdql': selectionPdql,
                  'filterPdql': filterPdql}

        r = exec_request(self.__core_session,
                         url,
                         method='PUT',
                         timeout=self.settings.connection_timeout,
                         json=params)

        if r.status_code == 204:
            self.log.info('status=success, action=update_query, '
                          'msg="Query update", name="{}", id="{}", '
                          'hostname="{}"'.format(query_name, query_id, self.__core_hostname))
        else:
            self.log.error('status=failed, action=update_query, '
                           'msg="Query update", name="{}", id="{}", '
                           'hostname="{}"'.format(query_name, query_id, self.__core_hostname))

        return r

    def close(self):
        if self.__core_session is not None:
            self.__core_session.close()
