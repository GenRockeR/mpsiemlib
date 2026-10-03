import time
from collections.abc import Iterator
from datetime import datetime
from io import IOBase
from typing import IO, Any, cast

import pytz
import requests

from mpsiemlib.common import (
    AuthError,
    LoggingHandler,
    ModuleInterface,
    MPSIEMAuth,
    Settings,
    exec_request,
    get_metrics_start_time,
    get_metrics_took_time,
)


class Assets(ModuleInterface, LoggingHandler):
    """Assets module."""

    __api_scopes = "/api/scopes/v2/scopes"
    __api_assets_processing_input_groups = (
        "/api/assets_processing/v2/assets_input/groups"
    )
    __api_assets_processing_v2_groups = "/api/assets_processing/v2/groups"
    __api_assets_trm_groups_hierarchy = (
        "/api/assets_temporal_readmodel/v2/groups/hierarchy"
    )
    __api_assets_processing_v2_configuration = (
        "/api/assets_processing/v2/assets_input/assets"
    )
    __api_assets_processing_v2_check_created = (
        "/api/assets_processing/v2/assets_input/assets/checkCreated"
    )
    __api_assets_trm_grid = "/api/assets_temporal_readmodel/v1/assets_grid"
    __api_assets_trm_row_count = (
        "/api/assets_temporal_readmodel/v1/assets_grid/row_count"
    )
    __api_assets_trm_selection = "/api/assets_temporal_readmodel/v1/assets_grid/data"
    __api_assets_trm_export_csv = "/api/assets_temporal_readmodel/v1/assets_grid/export"
    __api_assets_trm_stored_queries_folders = (
        "/api/assets_temporal_readmodel/v1/stored_queries/folders/queries"
    )
    __api_assets_trm_stored_queries_query = (
        "/api/assets_temporal_readmodel/v1/stored_queries/queries"
    )
    __api_assets_v2_import_operation = "/api/assets_processing/v2/csv/import_operation"
    __api_assets_v1_remove_assets = (
        "/api/assets_processing/v1/asset_operations/removeAssets"
    )
    __api_assets_v1_update_group_entries = (
        "/api/assets_processing/v1/asset_operations/updateGroupEntries"
    )

    def __init__(self, auth: MPSIEMAuth, settings: Settings) -> None:
        ModuleInterface.__init__(self, auth, settings)
        LoggingHandler.__init__(self)
        if auth.sessions is None or "core" not in auth.sessions:
            raise AuthError("Core session is not initialized")
        creds = auth.get_creds()
        if creds is None or creds.core_hostname is None:
            raise AuthError("Core hostname is not set")
        self.__core_session = auth.sessions["core"]
        self.__core_hostname = creds.core_hostname
        self.__core_version = auth.get_core_version()
        siem_tz = datetime.now(pytz.timezone(settings.local_timezone)).strftime("%z")
        self.__default_utc_offset = (
            f"{siem_tz[:-2]}:{siem_tz[-2:]}"  # convert to +HH:MM
        )
        self.__scopes: dict[str, dict[str, Any]] = {}
        self.__groups: dict[str, dict[str, Any]] = {}
        self.log.debug('status=success, action=prepare, msg="Assets Module init"')

    def get_scopes_list(self, do_refresh: bool = False) -> dict:
        """Получить все инфраструктуры.

        :return: {'id': {'name': 'Инфраструктура по умолчанию',
            'tenant_id': '97267c62-1455-4db0-8c84-497faf9a679e'}}
        """
        self.log.debug(
            f'status=prepare, action=get_scopes_list, msg="Try to get scopes list", '
            f"hostname={self.__core_hostname!r}"
        )

        if len(self.__scopes) != 0 and not do_refresh:
            return self.__scopes

        self.__scopes.clear()

        url = f"https://{self.__core_hostname}{self.__api_scopes}"
        response = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
        ).json()

        for i in response:
            self.__scopes[i.get("id")] = {
                "name": i.get("name"),
                "tenant_id": i.get("tenantId"),
            }

        self.log.info(
            f'status=success, action=get_scopes_list, msg="Got scopes {len(self.__scopes)!r}", '
            f"hostname={self.__core_hostname!r}"
        )

        return self.__scopes

    def get_scope_id_by_name(
        self, scope_name: str, do_refresh: bool = False
    ) -> str | None:
        """Получить id инфраструктуры по имени.

        :param scope_name: Название инфраструктуры
        :param do_refresh: Обновить кэш
        :return: '00000000-0000-0000-0000-000000000005' или None
        """
        self.log.debug(
            f'status=prepare, action=get_scope_id_by_name, msg="Try to get id for scope {scope_name!r}",'
            f"hostname={self.__core_hostname!r}"
        )

        if do_refresh or len(self.__scopes) == 0:
            self.get_scopes_list(do_refresh=True)

        scope = [k for k, v in self.__scopes.items() if v["name"] == scope_name]

        return scope[0] if len(scope) != 0 else None

    def create_assets_request(
        self,
        pdql: str,
        group_ids: list[str] | None = None,
        include_nested: bool = True,
        utc_offset: str | None = None,
    ) -> str:
        """Создать поисковый pdql-запрос и получить токен для доступа к
        результатам.

        :param pdql: PDQL-запрос
        :param group_ids: Список ID групп в которых надо искать активы
        :param include_nested: Искать ли во вложенных группах
        :param utc_offset: Часовой пояс смещение +00:00, по умолчанию смещение MP
        :return: Токен запроса
        """
        self.log.debug(
            f'status=prepare, action=create_assets_request, msg="Try to select assets with pdql {pdql!r}", '
            f"hostname={self.__core_hostname!r}"
        )

        url = f"https://{self.__core_hostname}{self.__api_assets_trm_grid}"

        params = {
            "pdql": pdql,
            "selectedGroupIds": group_ids if group_ids is not None else [],
            "additionalFilterParameters": {"groupIds": [], "assetIds": []},
            "includeNestedGroups": include_nested,
            "utcOffset": utc_offset
            if utc_offset is not None
            else self.__default_utc_offset,
        }

        response = exec_request(
            self.__core_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
            json=params,
        ).json()

        token = response.get("token")

        self.log.info(
            "status=success, action=create_assets_request, "
            f'msg="Got selection token {token!r}", hostname={self.__core_hostname!r}'
        )
        return cast("str", token)

    def get_assets_request_size(self, token: str) -> int:
        """Получить количество записей по токену.

        :param token: Токен запроса
        :return: Кол-во записей
        """
        self.log.debug(
            "status=prepare, action=get_assets_request_size, "
            f'msg="Try to get assets count from request with token {token!r}", '
            f"hostname={self.__core_hostname!r}"
        )

        url = f"https://{self.__core_hostname}{self.__api_assets_trm_row_count}"
        params = {"pdqlToken": token}
        response = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
            params=params,
        ).json()
        count = int(response.get("rowCount"))

        self.log.info(
            f'status=success, action=get_row_count, msg="Got row count for filter {count!r}", '
            f"hostname={self.__core_hostname!r}"
        )

        return count

    def get_assets_list_json(self, token: str) -> Iterator[dict]:
        """Получить список активов в JSON по токену запроса.

        :param token: Токен запроса
        :return: Словарь с атрибутами активов
        """
        self.log.debug(
            f'status=prepare, action=get_assets_list_json, msg="Try to iterate assets by token {token!r}", '
            f"hostname={self.__core_hostname!r}"
        )

        url = f"https://{self.__core_hostname}{self.__api_assets_trm_selection}"
        params = {"pdqlToken": token}

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

        self.log.info(
            'status=success, action=get_assets_list_json, msg="Query executed, response have been read", '
            f"hostname={self.__core_hostname!r}, lines={line_counter}"
        )
        self.log.info(
            f"hostname={self.__core_hostname!r}, metric=get_assets_list_json, "
            f"took={took_time:.4f} ms, objects={line_counter!r}"
        )

    def __iterate_assets(self, url: str, params: dict, offset: int, limit: int) -> list:
        params["offset"] = offset
        params["limit"] = limit
        response = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
            params=params,
        ).json()

        if response is None or "records" not in response:
            self.log.error(
                'status=failed, action=iterate_assets, msg="Assets data request return None or '
                'has wrong response structure", '
                f"hostname={self.__core_hostname!r}"
            )
            raise Exception(
                "Assets data request return None or has wrong response structure"
            )

        self.log.debug(
            f"Iterate assets, count={len(response.get('records'))!r}, offset={offset!r}, limit={limit!r}"
        )

        return cast("list", response.get("records"))

    def get_assets_list_csv(self, token: str) -> Iterator[str]:
        """Получить список активов в CSV по токену запроса.

        :param token: Токен запроса
        :return: Строка CSV
        """
        self.log.debug(
            f'status=prepare, action=get_assets_csv, msg="Try to iterate assets csv by token {token!r}", '
            f"hostname={self.__core_hostname!r}"
        )

        iterator = self.get_assets_list_stream(token)

        line_counter = 0
        start_time = get_metrics_start_time()
        for line in iterator:
            if line:
                line_counter += 1
                yield line
        took_time = get_metrics_took_time(start_time)

        self.log.info(
            'status=success, action=get_assets_list_csv, msg="Query executed, response have been read", '
            f"hostname={self.__core_hostname!r}, lines={line_counter!r}"
        )
        self.log.info(
            f"hostname={self.__core_hostname!r}, metric=get_assets_csv, "
            f"took={took_time:.4f} ms, objects={line_counter!r}"
        )

    def get_assets_list_stream(self, token: str) -> Iterator[str]:
        """Получить список активов в виде потока строк CSV.

        :param token: Токен запроса
        :return: Поток
        """
        url = f"https://{self.__core_hostname}{self.__api_assets_trm_export_csv}"
        params = {"pdqlToken": token}

        response = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
            params=params,
            stream=True,
        )

        # noinspection PyUnreachableCode
        if response.encoding is None:
            # requests может вернуть encoding=None, если charset не указан в Content-Type
            response.encoding = "utf-8-sig"

        # decode_unicode=True + выставленный encoding -> str; стаб этого не сужает
        return cast(Iterator[str], response.iter_lines(decode_unicode=True))

    def import_assets_from_csv(
        self,
        content: bytes | str | IO[Any] | IOBase,
        scope_id: str,
        group_id: str,
        timeout: int = 360,
    ) -> tuple[bool, int, str | None]:
        """Импорт активов в MP из CSV.

        :param content: Тело CSV для загрузки
        :param scope_id: ID инфраструктуры, куда импортируются активы
        :param group_id: ID группы, куда импортируются активы
        :param timeout: Время ожидания окончания загрузки (сек)
        :return: status, count, errors_log
        """
        self.log.debug(
            "status=prepare, action=import_assets_from_csv, "
            'msg="Try to import new assets from CSV", '
            f"hostname={self.__core_hostname!r}"
        )

        success_install = False
        error_log: str | None = None
        import_status: dict = {}

        resp = self.__import_assets_from_csv_prepare(content, scope_id)
        operation_id = resp.get("id")

        if operation_id is not None:
            # operation_id приходит из JSON как Any|None, сужаем до str
            operation_id = cast("str", operation_id)
            if resp.get("isLogFileCreated"):
                error_log = self.__import_assets_get_logfile(operation_id)
            resp2 = self.__import_assets_from_csv_start(operation_id, group_id)
            if resp2 == 200:
                for _ in range(round(timeout / 10)):
                    time.sleep(10)
                    self.log.debug("Try to check operation status")
                    import_status = self.__import_assets_get_status(operation_id)
                    if import_status.get("state") == "completed":
                        success_install = True
                        break

        if not success_install:
            self.log.error(
                f'status=failed, action=import_assets_from_csv, msg="Can not import assets", '
                f"error={error_log!r}, hostname={self.__core_hostname!r}"
            )

        counter_imported = import_status.get("succeedCount") or 0
        self.log.info(
            "status=success, action=import_assets_from_csv, "
            'msg="Assets have been imported", '
            f"imported_assets={counter_imported!r}, "
            f"updated_groups={len(import_status.get('updatedGroups', []))!r}, "
            f"hostname={self.__core_hostname!r}"
        )

        return success_install, counter_imported, error_log

    def __import_assets_from_csv_prepare(
        self, content: bytes | str | IO[Any] | IOBase, scope_id: str
    ) -> dict:
        """Подготовить операцию импорта.

        :param content: Тело CSV для загрузки
        :param scope_id: ID инфраструктуры, куда импортируются активы
        :return: {'id': '328cbed6-6625-41c1-aea6-d33eb034c260',
            'isLogFileCreated': False, 'rowsCountExceeded': False,
            'validRowsCount': 1, 'totalRowsCount': 1}
        """
        self.log.debug(
            "status=prepare, action=import_assets_from_csv_prepare, "
            'msg="Try to upload CSV with new assets", '
            f"hostname={self.__core_hostname!r}"
        )

        url = (
            f"https://{self.__core_hostname}"
            f"{self.__api_assets_v2_import_operation}?scopeId={scope_id}"
        )
        files = {"upfile": ("body", content, "application/octet-stream")}

        response = exec_request(
            self.__core_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
            files=files,
        ).json()

        self.log.info(
            "status=success, action=import_assets_from_csv_prepare, "
            'msg="Selected assets uploaded", '
            f"valid_rows={response.get('validRowsCount')}, "
            f"total_rows={response.get('totalRowsCount')}, "
            f"hostname={self.__core_hostname!r}"
        )

        return cast("dict", response)

    def __import_assets_from_csv_start(self, operation_id: str, group_id: str) -> int:
        """Запуск импорта ранее загруженных данных.

        :param operation_id: ID операции загрузки
        :param group_id: ID группы, куда импортируются активы
        :return: status_code
        """
        self.log.debug(
            "status=prepare, action=import_assets_from_csv_start, "
            'msg="Try to insert new assets into MP", '
            f"hostname={self.__core_hostname!r}"
        )

        url = (
            f"https://{self.__core_hostname}"
            f"{self.__api_assets_v2_import_operation}/{operation_id}/start"
        )
        params = {"groupsId": [group_id]}

        response = exec_request(
            self.__core_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
            json=params,
        )

        self.log.info(
            'status=success, action=import_assets_from_csv_start, msg="Import operation started", '
            f"hostname={self.__core_hostname!r}"
        )

        return response.status_code

    def __import_assets_get_status(self, operation_id: str) -> dict:
        """Получить статус операции импорта.

        :return: {"state":"inprogress","succeedCount":null,"updatedGroups":null,"errorModel":null}
        """

        url = (
            f"https://{self.__core_hostname}"
            f"{self.__api_assets_v2_import_operation}/{operation_id}/state"
        )

        response = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
        ).json()

        state = response.get("state")
        self.log.info(
            "status=success, action=get_import_status, "
            f'msg="Check import operation status: {state!r}", '
            f"hostname={self.__core_hostname!r}"
        )

        return cast("dict", response)

    def __import_assets_get_logfile(self, operation_id: str) -> str:
        """Получить журнал ошибок.

        :return: csv-formatted list of problems.
        """

        url = (
            f"https://{self.__core_hostname}"
            f"{self.__api_assets_v2_import_operation}/{operation_id}/logfile"
        )

        response = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
        )

        self.log.info(
            'status=success, action=get_import_logfile, msg="Got import logfile.", '
            f"hostname={self.__core_hostname!r}"
        )

        return response.content.decode("utf-8")

    def import_assets_get_groups(self) -> list:
        """Получить список групп, куда можно проводить импорт.

        :return: ['12f04fc3-3e00-0001-0000-000000000006',
            '12e9a858-b700-0001-0000-000000000002']
        """

        url = (
            f"https://{self.__core_hostname}{self.__api_assets_processing_input_groups}"
        )
        response = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
        ).json()

        self.log.info(
            f'status=success, action=get_import_groups, msg="Got input groups list: {response!r}", '
            f"hostname={self.__core_hostname!r}"
        )

        return cast("list", response)

    def create_group_dynamic(
        self, parent_id: str, group_name: str, predicate: str
    ) -> str:
        """Создать динамическую группу.

        :param parent_id: ID родительской группы
        :param group_name: Название группы
        :param predicate: Фильтр динамической группы
        :return: '12f04fc3-3e00-0001-0000-000000000006'
        """
        self.log.debug(
            "status=prepare, action=create_group_dynamic, "
            'msg="Try to create dynamic group", '
            f"hostname={self.__core_hostname!r}"
        )

        url = f"https://{self.__core_hostname}{self.__api_assets_processing_v2_groups}"
        if int(self.__core_version.split(".")[0]) < 27:
            params = {
                "name": group_name,
                "parentId": parent_id,
                "groupType": "dynamic",
                "predicate": predicate,
                "metrics": {
                    "td": "ND",
                    "cdp": "ND",
                    "cr": "ND",
                    "ir": "ND",
                    "ar": "ND",
                },
                "organizationInformation": {},
                "organizationInfrastructure": {},
            }
        else:
            params = {
                "name": group_name,
                "parentId": parent_id,
                "groupType": "dynamic",
                "predicate": predicate,
                "metadata": [],
                "organizationInformation": {},
                "organizationInfrastructure": {},
            }

        response = exec_request(
            self.__core_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
            json=params,
        ).json()

        if "operationId" not in response:
            raise Exception(f"operationId not found in response {response!r}")

        operation_id = response["operationId"]
        group_id = str(self.__group_operation_status(operation_id))

        self.log.info(
            "status=success, action=create_dynamic_group, "
            'msg="Dynamic group have been created.", '
            f"operation_id={operation_id!r}, "
            f"group_id={group_id!r}, "
            f"hostname={self.__core_hostname!r}"
        )

        return group_id

    def edit_group_dynamic(self, group_id: str, predicate: str) -> str:
        """Редактировать динамическую группу.

        :param group_id: ID редактируемой группы
        :param predicate: Фильтр динамической группы
        :return: '12f04fc3-3e00-0001-0000-000000000006'
        """
        self.log.debug(
            "status=prepare, action=edit_group_dynamic, "
            'msg="Try to edit dynamic group", '
            f"hostname={self.__core_hostname!r}"
        )

        url = (
            f"https://{self.__core_hostname}"
            f"{self.__api_assets_processing_v2_groups}/{group_id}"
        )
        params = [{"type": "SetPredicateGroupCommand", "value": predicate}]

        response = exec_request(
            self.__core_session,
            url,
            method="PUT",
            timeout=self.settings.connection_timeout,
            json=params,
        ).json()

        if "operationId" not in response:
            raise Exception(f"operationId not found in response {response!r}")

        operation_id = response["operationId"]
        result_group_id = str(self.__group_operation_status(operation_id))

        self.log.info(
            "status=success, action=edit_group_dynamic, "
            'msg="Dynamic group has been edited.", '
            f"operation_id={operation_id!r}, "
            f"group_id={result_group_id!r}, hostname={self.__core_hostname!r}"
        )

        return result_group_id

    def create_group_static(self, parent_id: str, group_name: str) -> str:
        """Создать статическую группу.

        :param parent_id: ID родительской группы
        :param group_name: Название группы
        :return: '12f04fc3-3e00-0001-0000-000000000006'
        """
        self.log.debug(
            "status=prepare, action=create_group_static, "
            'msg="Try to create static group", '
            f"hostname={self.__core_hostname!r}"
        )

        url = f"https://{self.__core_hostname}{self.__api_assets_processing_v2_groups}"
        if int(self.__core_version.split(".")[0]) < 27:
            params = {
                "name": group_name,
                "parentId": parent_id,
                "groupType": "static",
                "metrics": {
                    "td": "ND",
                    "cdp": "ND",
                    "cr": "ND",
                    "ir": "ND",
                    "ar": "ND",
                },
                "organizationInformation": {},
                "organizationInfrastructure": {},
            }
        else:
            params = {
                "name": group_name,
                "parentId": parent_id,
                "groupType": "static",
                "metadata": [],
                "organizationInformation": {},
                "organizationInfrastructure": {},
            }

        response = exec_request(
            self.__core_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
            json=params,
        ).json()

        if "operationId" not in response:
            raise Exception(f"operationId not found in response {response!r}")

        operation_id = response["operationId"]
        group_id = str(self.__group_operation_status(operation_id))

        self.log.info(
            "status=success, action=create_dynamic_group, "
            'msg="Static group have been created.", '
            f"operation_id={operation_id!r}, "
            f"group_id={group_id!r}, hostname={self.__core_hostname!r}"
        )

        return group_id

    def delete_group(self, group_id: str) -> bool:
        """Удалить группы.

        :param group_id: ID групп для удаления
        :return: status_code
        """
        self.log.debug(
            "status=prepare, action=delete_group, "
            'msg="Try to delete group", '
            f"hostname={self.__core_hostname!r}"
        )

        url = (
            f"https://{self.__core_hostname}"
            f"{self.__api_assets_processing_v2_groups}/removeOperation"
        )
        params = {"groupIds": [group_id]}

        response = exec_request(
            self.__core_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
            json=params,
        ).json()

        if "operationId" not in response:
            raise Exception(f"operationId not found in response {response!r}")

        operation_id = response["operationId"]
        status = self.__group_operation_status(operation_id, "remove")
        operation_status = isinstance(status, dict) and status.get("succeedCount") == 1

        self.log.info(
            f'status=success, action=delete_group, msg="Group deleted", status={operation_status!r},'
            f" report={status!r}, hostname={self.__core_hostname!r}"
        )

        return operation_status

    def __poll_operation(self, url: str, timeout: int) -> requests.Response:
        """Опрос статуса асинхронной операции до 200 или тайм-аута."""
        last_response: requests.Response | None = None
        for _ in range(max(1, round(timeout / 10))):
            time.sleep(10)
            self.log.debug("Try to check operation status")
            response = exec_request(
                self.__core_session,
                url,
                method="GET",
                timeout=self.settings.connection_timeout,
            )
            last_response = response
            if response.status_code == 200:
                return response

        if last_response is None:
            raise Exception(f"Operation status check timed out: {url!r}")
        return last_response

    def __group_operation_status(
        self, operation_id: str, operation_type: str = "create", timeout: int = 360
    ) -> str | dict:
        operation = "operations" if operation_type == "create" else "removeOperation"

        url = (
            f"https://{self.__core_hostname}"
            f"{self.__api_assets_processing_v2_groups}/{operation}/{operation_id}"
        )
        response = self.__poll_operation(url, timeout)

        result: str | dict = (
            response.text.strip('"') if operation_type == "create" else response.json()
        )

        self.log.debug(
            f'status=success, action=group_operation_status, msg="Got operation {operation_id!r} '
            f"status: {result!r}, hostname={self.__core_hostname!r}"
        )

        return result

    def get_groups_hierarchy(self) -> list:
        """Получить иерархию групп.

        :return: Список узлов дерева, каждый узел содержит id, name,
            groupType, isReadOnly, isRemovable, isRoot, isInvalidPredicate,
            isSlow, treePath и children (вложенные узлы). Пример поля id:
            '00000000-0000-0000-0000-000000000002' (корневой узел 'Root').
        """

        self.log.debug(
            f'status=prepare, action=get_groups_hierarchy, msg="Try to get groups tree", '
            f"hostname={self.__core_hostname!r}"
        )

        url = f"https://{self.__core_hostname}{self.__api_assets_trm_groups_hierarchy}"
        response = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
        ).json()

        self.log.info(
            'status=success, action=get_groups_hierarchy, msg="Got input groups", '
            f"hostname={self.__core_hostname!r}"
        )

        return cast("list", response)

    def get_groups_list(self, do_refresh: bool = False) -> dict:
        """Получить список всех групп активов.

        :param do_refresh: Обновить кэш
        :return: {"id": {"parent_id":
            "00000000-0000-0000-0000-000000000002", "name":
            "value","type": "static","is_readonly": True,
            "is_removable": False, "is_invalid_predicate": False,
            "is_slow": False}}
        """
        self.log.debug(
            f'status=prepare, action=get_groups_hierarchy, msg="Try to get groups list", '
            f"hostname={self.__core_hostname!r}"
        )

        if len(self.__groups) != 0 and not do_refresh:
            return self.__groups

        tree = self.get_groups_hierarchy()
        self.__iterate_groups_tree(tree)

        self.log.info(
            f'status=success, action=get_groups_list, msg="Got {len(self.__groups)!r} groups", '
            f"hostname={self.__core_hostname!r}"
        )

        return self.__groups

    def __iterate_groups_tree(
        self, root_node: list, parent_id: str | None = None
    ) -> None:
        for i in root_node:
            node_id = i.get("id")
            self.__groups[node_id] = {
                "parent_id": parent_id,
                "name": i.get("name"),
                "type": i.get("groupType"),
                "is_readonly": i.get("isReadOnly", False),
                "is_removable": i.get("isRemovable", False),
                "is_invalid_predicate": i.get("isInvalidPredicate", False),
                "is_slow": i.get("isSlow", False),
            }
            node_children = i.get("children")
            if node_children is not None and len(node_children) != 0:
                self.__iterate_groups_tree(node_children, node_id)

    def get_group_id_by_name(
        self, group_name: str, do_refresh: bool = False
    ) -> str | None:
        """Получить id группы по имени. Регистр важен.

        :param group_name: Имя группы
        :param do_refresh: Обновить кэш
        :return: '00000000-0000-0000-0000-000000000005' или None
        """
        self.log.debug(
            f"status=prepare, action=get_groups_hierarchy, "
            f'msg="Try to get groups id by name {group_name!r}", hostname={self.__core_hostname!r}'
        )

        if do_refresh or len(self.__groups) == 0:
            self.get_groups_list(do_refresh=True)

        group_id: str | None = None
        for k, v in self.__groups.items():
            if v.get("name") == group_name:
                group_id = k
                break

        return group_id

    def __delete_assets_by_ids(self, asset_ids: list) -> str:
        """Удаление активов по id.

        :param asset_ids: Список ID активов, которые необходимо удалить
        :return: {"operationId":"16b577e1-3900-a001-0000-000000000e79"}
        """
        self.log.debug(
            "status=prepare, action=__delete_assets_by_ids, "
            f'msg="Try to delete {len(asset_ids)!r} asset(s)", '
            f"hostname={self.__core_hostname!r}"
        )

        url = f"https://{self.__core_hostname}{self.__api_assets_v1_remove_assets}"
        params = {"assetsIds": asset_ids}

        response = exec_request(
            self.__core_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
            json=params,
        ).json()

        self.log.info(
            "status=success, action=__delete_assets_by_ids, "
            'msg="Deleting started", '
            f"operation_id={response['operationId']!r}, "
            f"hostname={self.__core_hostname!r}"
        )

        return cast("str", response.get("operationId"))

    def __remove_assets_get_status(self, operation_id: str) -> dict | None:
        """Получить статус операции удаления.

        :return: {"type":"AssetsOperationResult","totalCount":2,"succeedCount":2,"failedCount":0}
        """

        url = (
            f"https://{self.__core_hostname}"
            f"{self.__api_assets_v1_remove_assets}?operationId={operation_id}"
        )

        response = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
        )
        status_code = response.status_code

        if status_code == 200:
            resp = response.json()
            self.log.info(
                "status=success, action=__remove_assets_get_status, "
                f'msg="Check remove operation status: {resp!r}", '
                f"hostname={self.__core_hostname!r}"
            )
            return cast("dict", resp)
        else:
            self.log.info(
                "status=success, action=__remove_assets_get_status, "
                f'msg="Check remove operation status: HTTP:{status_code!r}", '
                f"hostname={self.__core_hostname!r}"
            )

            return None

    def delete_assets_by_ids(self, asset_ids: list) -> dict | None:
        """Удаление активов по id.

        :param asset_ids: Список ID активов, которые необходимо удалить
        :return: {"type":"AssetsOperationResult","totalCount":2,"succeedCount":2,"failedCount":0}
            или None, если статус операции не получен за время ожидания
        """
        status: dict | None = None
        operation_id = self.__delete_assets_by_ids(asset_ids)

        if operation_id is not None:
            for _ in range(30):
                time.sleep(1)
                status = self.__remove_assets_get_status(operation_id=operation_id)
                self.log.warning(f"status: {status}")
                if status is not None:
                    break

        self.log.info(
            "status=success, action=delete_assets_by_ids, "
            'msg="Deleting finished", '
            f"status={status!r}, hostname={self.__core_hostname!r}"
        )
        return status

    def __change_asset_configuration_by_id(
        self, asset_id: str, params: dict
    ) -> str | None:
        """Отправить изменение конфигурации актива.

        :return: TicketId операции или None
        """

        url = (
            f"https://{self.__core_hostname}"
            f"{self.__api_assets_processing_v2_configuration}/{asset_id}"
        )

        response = exec_request(
            self.__core_session,
            url,
            method="PUT",
            timeout=self.settings.connection_timeout,
            json=params,
        )

        status_code = response.status_code

        if status_code == 200:
            resp = response.json()
            self.log.info(
                "status=success, action=__change_asset_configuration_by_id, "
                f'msg="Check change operation status: {resp!r}", '
                f"hostname={self.__core_hostname!r}"
            )
            return cast("str | None", resp.get("ticketId"))
        else:
            self.log.info(
                "status=success, action=__change_asset_configuration_by_id, "
                f'msg="Check change operation status: HTTP:{status_code!r}, '
                f"hostname={self.__core_hostname!r}"
            )

            return None

    def __change_assets_get_status(self, ticket_id: str) -> dict | None:
        """Получить статус операции изменения актива.

        :return: {"type":"AssetsOperationResult","totalCount":2,"succeedCount":2,"failedCount":0}
        """

        url = (
            f"https://{self.__core_hostname}"
            f"{self.__api_assets_processing_v2_check_created}?ticketId={ticket_id}"
        )

        r = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
        )

        if r.status_code == 200:
            resp = r.json()
            self.log.info(
                "status=success, action=__change_assets_get_status, "
                f'msg="Check edit operation status: {resp!r}", '
                f"hostname={self.__core_hostname!r}"
            )
            return cast("dict", resp)
        else:
            self.log.debug(
                "status=success, action=__change_assets_get_status, "
                f'msg="Check edit operation status: HTTP:{r.status_code}", '
                f"hostname={self.__core_hostname!r}"
            )

            return None

    def change_asset_configuration_by_id(
        self, asset_id: str, config: dict
    ) -> dict | None:
        """Изменение активов по id.

        :param asset_id: ID актива, который надо сконфигурировать
        :param config: конфигурация актива
        :return: {"isSuccessful":true,"assetId":"17c930dc-0340-0001-0000-000000000002"}
        """

        ticket_id = self.__change_asset_configuration_by_id(asset_id, config)

        status: dict | None = None
        if ticket_id is not None:
            for _ in range(15):
                time.sleep(2)
                status = self.__change_assets_get_status(ticket_id=ticket_id)
                self.log.warning(f"status: {status}")
                if status is not None:
                    break

        self.log.info(
            "status=success, action=change_asset_configuration_by_id, "
            'msg="Editing finished", '
            f"status={status!r}, hostname={self.__core_hostname!r}"
        )
        return status

    def get_asset_configuration_by_id(self, asset_id: str) -> dict:
        """Получение информации об активе по id.

        :param asset_id: ID интересующего актива
        :return: { dict with asset config }
        """

        resp: dict = {}

        url = (
            f"https://{self.__core_hostname}"
            f"{self.__api_assets_processing_v2_configuration}/{asset_id}"
        )
        r = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
        )
        if r.status_code == 200:
            resp = r.json()
            self.log.info(
                "status=success, action=get_asset_configuration_by_id, "
                'msg="Got asset configuration", '
                f"hostname={self.__core_hostname!r}"
            )
            return resp
        else:
            self.log.info(
                "status=success, action=get_asset_configuration_by_id, "
                f'msg="Got asset configuration: HTTP:{r.status_code}", '
                f"hostname={self.__core_hostname!r}"
            )
        return resp

    def get_queries(self) -> dict:
        """Получить все запросы.

        :return: Узлы дерева сохранённых запросов
        """
        url = (
            f"https://{self.__core_hostname}"
            f"{self.__api_assets_trm_stored_queries_folders}"
        )
        r = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
        )
        response = r.json()

        self.log.info(
            "status=success, action=get_queries, "
            f'msg="Got queries {len(response)}", '
            f"hostname={self.__core_hostname!r}"
        )

        return cast("dict", response["nodes"])

    def get_query_by_id(self, queryid: str) -> dict:
        """Получить запрос по id.

        :return: Объект запроса с полями id, displayName, folderId,
            filterId, filterPdql, selectionId, selectionPdql, isInvalid,
            isDeleted, type. Пример id: "23d2ffe845be453b885b97ff69bf7604",
            filterPdql: qsearch("70f5ef43.com").
        """
        url = (
            f"https://{self.__core_hostname}"
            f"{self.__api_assets_trm_stored_queries_query}/{queryid}"
        )
        r = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
        )
        response = r.json()

        self.log.info(
            "status=success, action=get_query_by_id, "
            f'msg="Got query {response["displayName"]!r}", '
            f"hostname={self.__core_hostname!r}"
        )

        return cast("dict", response)

    def create_query_folder(self, parent_id: str, folder_name: str) -> dict:
        """Создать папку для запросов.

        :param parent_id: ID родительской папки
        :param folder_name: Название папки
        :return: {"id":"6224c6bca5b64e6c98825cc336be19e8",
            "displayName":"a / b",
            "parentId":"c51f8e849598459cb0f352280a1ccf8c","type":"common"}
        """

        url = (
            f"https://{self.__core_hostname}"
            f"{self.__api_assets_trm_stored_queries_folders}"
        )
        params = {"displayName": folder_name, "parentId": parent_id}
        response = exec_request(
            self.__core_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
            json=params,
        ).json()

        self.log.info(
            "status=success, action=create_query_folder, "
            f'msg="Query folder created", Name={response["displayName"]!r}, '
            f"type={response['type']!r}, "
            f"hostname={self.__core_hostname!r}"
        )

        return cast("dict", response)

    def create_query(
        self, folder_id: str, query_name: str, pdql_filter: str, pdql_selection: str
    ) -> dict:
        """Создать запрос в папке.

        :param folder_id: ID родительской папки
        :param query_name: Название запроса
        :param pdql_filter: фильтр
        :param pdql_selection: выборка
        :return: {"id":"5279a28a6bdb464d9c5fcbaba08284ff",
        "displayName":"xxxx",
        "folderId":"a50a65f8a5c04c2785b0515d84135007",
        "filterId":null,"filterPdql":null,"selectionId":null,
        "selectionPdql":"zzzz","isInvalid":false,"isDeleted":false,"type":"user"}
        """

        url = (
            f"https://{self.__core_hostname}"
            f"{self.__api_assets_trm_stored_queries_query}"
        )

        params = {
            "displayName": query_name,
            "folderId": folder_id,
            "selectionPdql": pdql_selection,
            "filterPdql": pdql_filter,
        }

        response = exec_request(
            self.__core_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
            json=params,
        ).json()

        self.log.info(
            "status=success, action=create_query, "
            f'msg="Query created", Name={response["displayName"]!r}, '
            f"type={response['type']!r}, "
            f"hostname={self.__core_hostname!r}"
        )

        return cast("dict", response)

    def update_query(
        self,
        query_id: str,
        folder_id: str,
        query_name: str,
        pdql_filter: str,
        pdql_selection: str,
    ) -> dict:
        """Обновить запрос в папке.

        Имя и папка меняются отдельным эндпоинтом: контракт `UpdateQueryCommand`
        (`PUT /stored_queries/queries/{queryId}`) принимает только
        `id`, `filterId`, `filterPdql`, `selectionId`, `selectionPdql` и молча
        игнорирует `displayName`/`folderId` (измерено на стенде: 204, PDQL
        применился, имя осталось). Переименование/перенос — `PUT
        /stored_queries/queries/{queryId}/path` (`PathInfo`).

        :param query_id: Query ID
        :param folder_id: ID родительской папки
        :param query_name: Название запроса
        :param pdql_filter: фильтр
        :param pdql_selection: выборка
        :return: dict
        """

        url = (
            f"https://{self.__core_hostname}"
            f"{self.__api_assets_trm_stored_queries_query}/{query_id}"
        )

        params = {
            "id": query_id,
            "selectionPdql": pdql_selection,
            "filterPdql": pdql_filter,
        }

        response = exec_request(
            self.__core_session,
            url,
            method="PUT",
            timeout=self.settings.connection_timeout,
            json=params,
        )

        status_code = response.status_code

        if status_code == 204:
            self.log.info(
                "status=success, action=update_query, "
                f'msg="Query update", name={query_name!r}, id={query_id!r}, '
                f"hostname={self.__core_hostname!r}"
            )
        else:
            self.log.error(
                "status=failed, action=update_query, "
                f'msg="Query update", name={query_name!r}, id={query_id!r}, '
                f"hostname={self.__core_hostname!r}"
            )

        current = self.get_query_by_id(query_id)

        if (
            current.get("displayName") != query_name
            or current.get("folderId") != folder_id
        ):
            # PathInfo требует оба поля: передаём текущую связку целиком, чтобы
            # изменение имени не сбрасывало папку (и наоборот).
            path_response = exec_request(
                self.__core_session,
                f"{url}/path",
                method="PUT",
                timeout=self.settings.connection_timeout,
                json={"displayName": query_name, "folderId": folder_id},
            )
            self.log.info(
                "status=success, action=update_query_path, "
                f'msg="Query path update", name={query_name!r}, id={query_id!r}, '
                f"status_code={path_response.status_code!r}, "
                f"hostname={self.__core_hostname!r}"
            )

        return response.json() if response.text else {}

    def __update_group_entries_by_ids(
        self,
        asset_ids: list,
        include: list | None = None,
        exclude: list | None = None,
    ) -> str:
        """Изменение групп активов по id.

        :param asset_ids: Список ID активов, для которых нужно изменить группы
        :param include: Список ID групп, в которые необходимо добавить актив
        :param exclude: Список ID групп, из которых необходимо удалить актив
        :return: {"operationId":"16b577e1-3900-a001-0000-000000000e79"}
        """
        if exclude is None:
            exclude = []
        if include is None:
            include = []

        self.log.debug(
            "status=prepare, action=__update_group_entries_by_ids, "
            f'msg="Try to update groups for {len(asset_ids)!r} asset(s)", '
            f"hostname={self.__core_hostname!r}"
        )

        url = (
            f"https://{self.__core_hostname}{self.__api_assets_v1_update_group_entries}"
        )
        params = {
            "assetsIds": asset_ids,
            "includeInGroups": include,
            "excludeFromGroups": exclude,
        }

        response = exec_request(
            self.__core_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
            json=params,
        ).json()

        self.log.info(
            "status=success, action=__update_group_entries_by_ids, "
            'msg="Update started", '
            f"operation_id={response['operationId']!r}, "
            f"hostname={self.__core_hostname!r}"
        )

        return cast("str", response.get("operationId"))

    def __update_group_entries_get_status(self, operation_id: str) -> dict:
        """Получить статус операции изменения групп.

        :return: {"type":"AssetsOperationResult","totalCount":2,"succeedCount":2,"failedCount":0}
        """

        url = (
            f"https://{self.__core_hostname}"
            f"{self.__api_assets_v1_update_group_entries}?operationId={operation_id}"
        )

        response = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
        )

        status_code = response.status_code

        if status_code == 200:
            resp = response.json()
            self.log.info(
                "status=success, action=__update_group_entries_get_status, "
                f'msg="Check update operation status: {resp!r}", '
                f"hostname={self.__core_hostname!r}"
            )

            return cast("dict", resp)
        else:
            self.log.info(
                "status=success, action=__update_group_entries_get_status, "
                f'msg="Check update operation status: HTTP:{status_code}", '
                f"hostname={self.__core_hostname!r}"
            )

            return {}

    def update_group_entries_by_ids(
        self,
        asset_ids: list,
        include: list | None = None,
        exclude: list | None = None,
    ) -> dict:
        """Изменение групп активов по id.

        :param asset_ids: Список ID активов, для которых нужно изменить группы
        :param include: Список ID групп, в которые необходимо добавить актив
        :param exclude: Список ID групп, из которых необходимо удалить актив
        :return: {"type":"AssetsOperationResult","totalCount":2,"succeedCount":2,"failedCount":0}
        """

        if exclude is None:
            exclude = []
        if include is None:
            include = []
        status: dict = {}
        operation_id = self.__update_group_entries_by_ids(asset_ids, include, exclude)

        if operation_id is not None:
            for _ in range(30):
                time.sleep(1)
                status = self.__update_group_entries_get_status(
                    operation_id=operation_id
                )
                self.log.debug(f"status: {status}")
                if len(status) != 0:
                    break

        self.log.info(
            "status=success, action=update_group_entries_by_ids, "
            f'msg="Update finished", status={status}, '
            f'hostname="{self.__core_hostname}"'
        )

        return status

    def close(self) -> None:
        if self.__core_session is not None:
            self.__core_session.close()
