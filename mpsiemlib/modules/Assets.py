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
    __api_assets_trm_query_fields = (
        "/api/assets_temporal_readmodel/v1/assets_grid/query/fields"
    )
    __api_assets_trm_query_types = (
        "/api/assets_temporal_readmodel/v1/assets_grid/query/types"
    )
    __api_assets_trm_groups = "/api/assets_temporal_readmodel/v2/groups"
    __api_assets_v2_csv = "/api/assets_processing/v2/csv"
    __api_assets_v2_predicate_validate = (
        "/api/assets_processing/v2/groups/predicate/validate"
    )
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
        # Релиз ядра (MAJOR, MINOR): "27.6.40521" -> (27, 6). Сравнение
        # кортежем, а не float: "26.10" превращается в 26.1.
        version_parts = self.__core_version.split(".")
        self.__core_release: tuple[int, int] = (
            int(version_parts[0]),
            int(version_parts[1]),
        )
        siem_tz = datetime.now(pytz.timezone(settings.local_timezone)).strftime("%z")
        self.__default_utc_offset = (
            f"{siem_tz[:-2]}:{siem_tz[-2:]}"  # convert to +HH:MM
        )
        self.__scopes: dict[str, dict[str, Any]] = {}
        self.__groups: dict[str, dict[str, Any]] = {}
        self.log.debug('status=success, action=prepare, msg="Assets Module init"')

    def get_scopes_list(self, do_refresh: bool = False) -> dict[str, dict[str, Any]]:
        """Получить все инфраструктуры.

        :return: {'id': {'name': 'Инфраструктура по умолчанию',
            'tenant_id': '97267c62-1455-4db0-8c84-497faf9a679e'}}
        """
        self.log.debug(
            f'status=prepare, action=get_scopes_list, msg="Try to get scopes list", '
            f"hostname={self.__core_hostname!r}"
        )

        if len(self.__scopes) != 0 and not do_refresh:
            return dict(self.__scopes)

        self.__scopes.clear()

        url = f"https://{self.__core_hostname}{self.__api_scopes}"
        response: list[dict[str, Any]] = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
        ).json()

        for item in response:
            scope_id = item.get("id")
            if scope_id is None:
                continue
            self.__scopes[str(scope_id)] = {
                "name": item.get("name"),
                "tenant_id": item.get("tenantId"),
            }

        self.log.info(
            f'status=success, action=get_scopes_list, msg="Got scopes {len(self.__scopes)!r}", '
            f"hostname={self.__core_hostname!r}"
        )

        return dict(self.__scopes)

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

        response: dict[str, Any] = exec_request(
            self.__core_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
            json=params,
        ).json()

        token = response.get("token")
        if token is None:
            self.log.error(
                'status=failed, action=create_assets_request, msg="PDQL parse result '
                'has no token", '
                f"hostname={self.__core_hostname!r}"
            )
            raise RuntimeError(f"Assets grid PDQL parse result has no token: {pdql!r}")

        self.log.info(
            "status=success, action=create_assets_request, "
            f'msg="Got selection token {token!r}", hostname={self.__core_hostname!r}'
        )
        return str(token)

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
        response: dict[str, Any] = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
            params=params,
        ).json()

        row_count = response.get("rowCount")
        # PdqlRowCount.rowCount (int64) обязателен по контракту AssetsGrid
        if row_count is None:
            self.log.error(
                'status=failed, action=get_assets_request_size, msg="Row count is '
                'missing in response", '
                f"hostname={self.__core_hostname!r}"
            )
            raise RuntimeError("Assets row count is missing in response")

        count = int(cast("int", row_count))

        self.log.info(
            f'status=success, action=get_row_count, msg="Got row count for filter {count!r}", '
            f"hostname={self.__core_hostname!r}"
        )

        return count

    def get_pdql_available_fields(self, pdql: str) -> list[dict[str, Any]]:
        """Получить поля, доступные по итогу выполнения PDQL-запроса.

        Поля возвращаются после применения всех блоков запроса (фильтр,
        проекции, агрегации), поэтому результат можно использовать для
        валидации и автодополнения следующего блока PDQL.

        :param pdql: PDQL-запрос целиком
        :return: [{"name": "host", "localizedName": "Узел",
            "type": "assetInfo", "isArray": False, "origin": "dataField"}]
        :raises requests.exceptions.HTTPError: если PDQL невалиден (HTTP 400)
        """
        self.log.debug(
            "status=prepare, action=get_pdql_available_fields, "
            f'msg="Try to get fields available for pdql {pdql!r}", '
            f"hostname={self.__core_hostname!r}"
        )

        url = f"https://{self.__core_hostname}{self.__api_assets_trm_query_fields}"
        response: list[dict[str, Any]] = exec_request(
            self.__core_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
            json={"pdqlQuery": pdql},
        ).json()

        self.log.info(
            "status=success, action=get_pdql_available_fields, "
            f'msg="Got {len(response)!r} fields", '
            f"hostname={self.__core_hostname!r}"
        )

        return list(response)

    def get_pdql_available_types(
        self,
        type_name: str | None = None,
        selected_fields: list[str] | None = None,
        has_time_series_pipe: bool = False,
        has_time_point_pipe: bool = False,
    ) -> list[dict[str, Any]]:
        """Получить типы сущностей, доступные в модельном (начальном) блоке PDQL.

        :param type_name: Начальная часть имени типа для автодополнения,
            должна заканчиваться точкой (например ``"Host."``). Без него
            возвращаются корневые типы.
        :param selected_fields: Поля, уже выбранные в проекциях
        :param has_time_series_pipe: Присутствует ли пайп ``timeseries``
        :param has_time_point_pipe: Присутствует ли пайп ``timepoint``
        :return: [{"name": "AccessLists", "localizedName": "Списки контроля
            доступа", "isPrimitive": False, "type": "string"}]
        """
        self.log.debug(
            "status=prepare, action=get_pdql_available_types, "
            f'msg="Try to get types available for pdql, type_name={type_name!r}", '
            f"hostname={self.__core_hostname!r}"
        )

        params: dict[str, Any] = {
            "hasTimeSeriesPipe": has_time_series_pipe,
            "hasTimePointPipe": has_time_point_pipe,
        }
        if type_name is not None:
            params["typeName"] = type_name
        if selected_fields is not None:
            params["selectedFields"] = selected_fields

        url = f"https://{self.__core_hostname}{self.__api_assets_trm_query_types}"
        response: dict[str, Any] = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
            params=params,
        ).json()

        types = [dict(item) for item in response.get("types") or []]

        self.log.info(
            "status=success, action=get_pdql_available_types, "
            f'msg="Got {len(types)!r} types", '
            f"hostname={self.__core_hostname!r}"
        )

        return types

    def get_assets_list_json(self, token: str) -> Iterator[dict[str, Any]]:
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

    def __iterate_assets(
        self, url: str, params: dict[str, Any], offset: int, limit: int
    ) -> list[dict[str, Any]]:
        page_params = dict(params, offset=offset, limit=limit)
        response: dict[str, Any] = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
            params=page_params,
        ).json()

        if response is None or "records" not in response:
            self.log.error(
                'status=failed, action=iterate_assets, msg="Assets data request return None or '
                'has wrong response structure", '
                f"hostname={self.__core_hostname!r}"
            )
            raise RuntimeError(
                "Assets data request return None or has wrong response structure"
            )

        records = list(response["records"])
        self.log.debug(
            f"Iterate assets, count={len(records)!r}, offset={offset!r}, limit={limit!r}"
        )

        return records

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

    def get_import_csv_metadata(self) -> list[dict[str, Any]]:
        """Получить метадату пользовательских свойств для CSV-импорта.

        Пустой список означает, что пользовательских свойств в модели актива
        нет; обязательные колонки импорта (`typealias`, `fqdn`, `hostname`,
        `ip`, `mac` и т.п.) в ответе не отдаются.

        :return: [{"propertyName": "Custom.property",
            "propertyDeclarations": [{"modelTypeName": "Host",
            "propertyType": "string"}]}]
        """
        self.log.debug(
            "status=prepare, action=get_import_csv_metadata, "
            'msg="Try to get CSV import metadata", '
            f"hostname={self.__core_hostname!r}"
        )

        url = f"https://{self.__core_hostname}{self.__api_assets_v2_csv}/metadata"
        response: list[dict[str, Any]] = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
        ).json()

        self.log.info(
            "status=success, action=get_import_csv_metadata, "
            f'msg="Got {len(response)!r} custom properties", '
            f"hostname={self.__core_hostname!r}"
        )

        return list(response)

    def get_import_csv_example(self) -> str:
        """Получить пример CSV-файла для импорта активов.

        Сервер отдаёт файл с BOM и `Content-Type: application/json`, поэтому
        тело читается как текст, а не как JSON.

        :return: Содержимое CSV-примера
        """
        self.log.debug(
            "status=prepare, action=get_import_csv_example, "
            'msg="Try to get CSV import example", '
            f"hostname={self.__core_hostname!r}"
        )

        url = f"https://{self.__core_hostname}{self.__api_assets_v2_csv}/example"
        response = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
        )
        example = response.content.decode("utf-8-sig")

        self.log.info(
            "status=success, action=get_import_csv_example, "
            f'msg="Got CSV example, {len(example)!r} chars", '
            f"hostname={self.__core_hostname!r}"
        )

        return example

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
        import_status: dict[str, Any] = {}

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
                    state = import_status.get("state")
                    # state: validated | inprogress | completed | failed
                    if state == "completed":
                        success_install = True
                        break
                    if state == "failed":
                        if error_log is None:
                            error_log = self.__import_assets_get_logfile(operation_id)
                        break

        counter_imported = import_status.get("succeedCount") or 0
        if not success_install:
            self.log.error(
                f'status=failed, action=import_assets_from_csv, msg="Can not import assets", '
                f"state={import_status.get('state')!r}, error={error_log!r}, "
                f"hostname={self.__core_hostname!r}"
            )
        else:
            self.log.info(
                "status=success, action=import_assets_from_csv, "
                'msg="Assets have been imported", '
                f"imported_assets={counter_imported!r}, "
                f"updated_groups={len(import_status.get('updatedGroups') or [])}, "
                f"hostname={self.__core_hostname!r}"
            )

        return success_install, counter_imported, error_log

    def __import_assets_from_csv_prepare(
        self, content: bytes | str | IO[Any] | IOBase, scope_id: str
    ) -> dict[str, Any]:
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

        response: dict[str, Any] = exec_request(
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

        return response

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

    def __import_assets_get_status(self, operation_id: str) -> dict[str, Any]:
        """Получить статус операции импорта.

        :return: {"state":"inprogress","succeedCount":null,"updatedGroups":null,"errorModel":null}
        """

        url = (
            f"https://{self.__core_hostname}"
            f"{self.__api_assets_v2_import_operation}/{operation_id}/state"
        )

        response: dict[str, Any] = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
        ).json()

        state = response.get("state")
        self.log.debug(
            "status=success, action=get_import_status, "
            f'msg="Check import operation status: {state!r}", '
            f"hostname={self.__core_hostname!r}"
        )

        return response

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

    def import_assets_get_groups(self) -> list[str]:
        """Получить список групп, куда можно проводить импорт.

        :return: ['12f04fc3-3e00-0001-0000-000000000006',
            '12e9a858-b700-0001-0000-000000000002']
        """

        url = (
            f"https://{self.__core_hostname}{self.__api_assets_processing_input_groups}"
        )
        response: list[str] = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
        ).json()

        self.log.info(
            f"status=success, action=get_import_groups, "
            f'msg="Got {len(response)!r} input groups", '
            f"hostname={self.__core_hostname!r}"
        )

        return list(response)

    def get_group_input_permissions(self, group_id: str) -> bool:
        """Проверить, разрешён ли в группе ручной ввод активов.

        :param group_id: ID группы
        :return: True, если ручной ввод разрешён
        """
        self.log.debug(
            "status=prepare, action=get_group_input_permissions, "
            f'msg="Try to check manual input permission for group {group_id!r}", '
            f"hostname={self.__core_hostname!r}"
        )

        url = (
            f"https://{self.__core_hostname}"
            f"{self.__api_assets_processing_input_groups}/{group_id}/permissions"
        )
        response: dict[str, Any] = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
        ).json()

        enabled = bool(response.get("enabled"))

        self.log.info(
            "status=success, action=get_group_input_permissions, "
            f'msg="Manual input allowed: {enabled!r}", '
            f"group_id={group_id!r}, hostname={self.__core_hostname!r}"
        )

        return enabled

    def validate_group_predicate(self, predicate: str) -> dict[str, Any]:
        """Проверить предикат динамической группы, не создавая группу.

        Невалидный предикат сервер считает ошибкой запроса, поэтому возвращается
        только успешный результат.

        :param predicate: Предикат динамической группы (фрагмент PDQL)
        :return: {"isValid": True, "isPotentiallySlow": False}
        :raises requests.exceptions.HTTPError: если предикат невалиден (HTTP 400)
        """
        self.log.debug(
            "status=prepare, action=validate_group_predicate, "
            f'msg="Try to validate group predicate {predicate!r}", '
            f"hostname={self.__core_hostname!r}"
        )

        url = f"https://{self.__core_hostname}{self.__api_assets_v2_predicate_validate}"
        response: dict[str, Any] = exec_request(
            self.__core_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
            json={"predicate": predicate},
        ).json()

        self.log.info(
            "status=success, action=validate_group_predicate, "
            f'msg="Predicate checked: {response!r}", '
            f"hostname={self.__core_hostname!r}"
        )

        return response

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
        if self.__core_release < (27, 0):
            # MP SIEM 26.x: тело создания группы требовало блок `metrics`
            params: dict[str, Any] = {
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

        response: dict[str, Any] = exec_request(
            self.__core_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
            json=params,
        ).json()

        operation_id = self.__operation_id_from_response(
            response, "create_group_dynamic"
        )
        group_id = str(self.__group_operation_status(operation_id))

        self.log.info(
            "status=success, action=create_dynamic_group, "
            'msg="Dynamic group have been created.", '
            f"operation_id={operation_id!r}, "
            f"group_id={group_id!r}, "
            f"hostname={self.__core_hostname!r}"
        )

        # Дерево групп изменилось
        self.__reset_groups_cache()
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

        response: dict[str, Any] = exec_request(
            self.__core_session,
            url,
            method="PUT",
            timeout=self.settings.connection_timeout,
            json=params,
        ).json()

        operation_id = self.__operation_id_from_response(response, "edit_group_dynamic")
        result_group_id = str(self.__group_operation_status(operation_id))

        self.log.info(
            "status=success, action=edit_group_dynamic, "
            'msg="Dynamic group has been edited.", '
            f"operation_id={operation_id!r}, "
            f"group_id={result_group_id!r}, hostname={self.__core_hostname!r}"
        )

        # Дерево групп изменилось
        self.__reset_groups_cache()
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
        if self.__core_release < (27, 0):
            # MP SIEM 26.x: тело создания группы требовало блок `metrics`
            params: dict[str, Any] = {
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

        response: dict[str, Any] = exec_request(
            self.__core_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
            json=params,
        ).json()

        operation_id = self.__operation_id_from_response(
            response, "create_group_static"
        )
        group_id = str(self.__group_operation_status(operation_id))

        self.log.info(
            "status=success, action=create_static_group, "
            'msg="Static group have been created.", '
            f"operation_id={operation_id!r}, "
            f"group_id={group_id!r}, hostname={self.__core_hostname!r}"
        )

        # Дерево групп изменилось
        self.__reset_groups_cache()
        return group_id

    def delete_group(self, group_id: str) -> bool:
        """Удалить группу.

        :param group_id: ID группы для удаления
        :return: True при успехе
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

        response: dict[str, Any] = exec_request(
            self.__core_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
            json=params,
        ).json()

        operation_id = self.__operation_id_from_response(response, "delete_group")
        status = self.__group_operation_status(operation_id, "remove")
        operation_status = isinstance(status, dict) and status.get("succeedCount") == 1

        if operation_status:
            self.log.info(
                f'status=success, action=delete_group, msg="Group deleted", '
                f"operation_id={operation_id!r}, report={status!r}, "
                f"hostname={self.__core_hostname!r}"
            )
        else:
            self.log.error(
                f'status=failed, action=delete_group, msg="Group have not been deleted", '
                f"operation_id={operation_id!r}, report={status!r}, "
                f"hostname={self.__core_hostname!r}"
            )

        # Дерево групп изменилось (успех или нет - на всякий случай)
        self.__reset_groups_cache()
        return operation_status

    @staticmethod
    def __operation_id_from_response(response: dict[str, Any], action: str) -> str:
        """Достать `operationId` из ответа асинхронной операции.

        :raises RuntimeError: Если идентификатора операции в ответе нет
        """
        operation_id = response.get("operationId")
        if operation_id is None:
            raise RuntimeError(
                f"{action}: operationId not found in response: {response!r}"
            )
        return str(operation_id)

    def __poll_operation(self, url: str, timeout: int) -> requests.Response:
        """Опрос статуса асинхронной операции до 200 или тайм-аута.

        :raises RuntimeError: Если статус так и не получен за время ожидания
        """
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

        code = last_response.status_code if last_response is not None else "n/a"
        raise RuntimeError(
            f"Operation status check timed out (last status: {code!r}): {url!r}"
        )

    def __group_operation_status(
        self, operation_id: str, operation_type: str = "create", timeout: int = 360
    ) -> str | dict[str, Any]:
        operation = "operations" if operation_type == "create" else "removeOperation"

        url = (
            f"https://{self.__core_hostname}"
            f"{self.__api_assets_processing_v2_groups}/{operation}/{operation_id}"
        )
        response = self.__poll_operation(url, timeout)

        result: str | dict[str, Any] = (
            response.text.strip('"') if operation_type == "create" else response.json()
        )

        self.log.debug(
            f'status=success, action=group_operation_status, msg="Got operation {operation_id!r} '
            f"status: {result!r}, hostname={self.__core_hostname!r}"
        )

        return result

    def get_groups_hierarchy(self) -> list[dict[str, Any]]:
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
        response: list[dict[str, Any]] = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
        ).json()

        self.log.info(
            'status=success, action=get_groups_hierarchy, msg="Got input groups", '
            f"hostname={self.__core_hostname!r}"
        )

        return list(response)

    def get_group_assets_ids(
        self, group_id: str, is_recursive: bool = True
    ) -> list[str]:
        """Получить ID активов, входящих в группу.

        :param group_id: ID группы
        :param is_recursive: Включая активы вложенных групп
        :return: ['1ebc98c2-7400-0001-0000-0000000000bc', ...]
        """
        self.log.debug(
            "status=prepare, action=get_group_assets_ids, "
            f'msg="Try to get assets of group {group_id!r}", '
            f"hostname={self.__core_hostname!r}"
        )

        url = (
            f"https://{self.__core_hostname}"
            f"{self.__api_assets_trm_groups}/{group_id}/assets"
        )
        # Сервер понимает только "True"/"False": "true"/"false" дают пустой
        # список, поэтому передаём bool, а не строку.
        response: list[str] = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
            params={"isRecursive": is_recursive},
        ).json()

        self.log.info(
            "status=success, action=get_group_assets_ids, "
            f'msg="Got {len(response)!r} assets of group {group_id!r}", '
            f"hostname={self.__core_hostname!r}"
        )

        return list(response)

    def get_groups_list(self, do_refresh: bool = False) -> dict[str, dict[str, Any]]:
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
            return dict(self.__groups)

        self.__groups.clear()
        tree = self.get_groups_hierarchy()
        self.__iterate_groups_tree(tree)

        self.log.info(
            f'status=success, action=get_groups_list, msg="Got {len(self.__groups)!r} groups", '
            f"hostname={self.__core_hostname!r}"
        )

        return dict(self.__groups)

    def __reset_groups_cache(self) -> None:
        """Сбросить кэш групп: он устаревает при любом изменении дерева групп."""
        self.__groups = {}

    def __iterate_groups_tree(
        self, root_node: list[dict[str, Any]], parent_id: str | None = None
    ) -> None:
        for node in root_node:
            node_id = node.get("id")
            if node_id is None:
                continue
            self.__groups[str(node_id)] = {
                "parent_id": parent_id,
                "name": node.get("name"),
                "type": node.get("groupType"),
                "is_readonly": node.get("isReadOnly", False),
                "is_removable": node.get("isRemovable", False),
                "is_invalid_predicate": node.get("isInvalidPredicate", False),
                "is_slow": node.get("isSlow", False),
            }
            node_children = node.get("children")
            if isinstance(node_children, list) and len(node_children) != 0:
                self.__iterate_groups_tree(node_children, str(node_id))

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

    def __delete_assets_by_ids(self, asset_ids: list[str]) -> str:
        """Удаление активов по id.

        :param asset_ids: Список ID активов, которые необходимо удалить
        :return: ID операции удаления
        """
        self.log.debug(
            "status=prepare, action=delete_assets_by_ids, "
            f'msg="Try to delete {len(asset_ids)!r} asset(s)", '
            f"hostname={self.__core_hostname!r}"
        )

        url = f"https://{self.__core_hostname}{self.__api_assets_v1_remove_assets}"
        params = {"assetsIds": asset_ids}

        response: dict[str, Any] = exec_request(
            self.__core_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
            json=params,
        ).json()

        operation_id = self.__operation_id_from_response(
            response, "delete_assets_by_ids"
        )
        self.log.info(
            "status=success, action=delete_assets_by_ids, "
            'msg="Deleting started", '
            f"operation_id={operation_id!r}, "
            f"hostname={self.__core_hostname!r}"
        )

        return operation_id

    def __remove_assets_get_status(self, operation_id: str) -> dict[str, Any] | None:
        """Получить статус операции удаления.

        :return: {"type":"AssetsOperationResult","totalCount":2,"succeedCount":2,"failedCount":0}
            или None, если операция ещё не завершена
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

        if status_code != 200:
            self.log.debug(
                "status=prepare, action=__remove_assets_get_status, "
                f'msg="Remove operation is not ready: HTTP:{status_code!r}", '
                f"hostname={self.__core_hostname!r}"
            )
            return None

        result = cast("dict[str, Any]", response.json())
        self.log.debug(
            "status=success, action=__remove_assets_get_status, "
            f'msg="Check remove operation status: {result!r}", '
            f"hostname={self.__core_hostname!r}"
        )
        return result

    def delete_assets_by_ids(self, asset_ids: list[str]) -> dict[str, Any] | None:
        """Удаление активов по id.

        :param asset_ids: Список ID активов, которые необходимо удалить
        :return: {"type":"AssetsOperationResult","totalCount":2,"succeedCount":2,"failedCount":0}
            или None, если статус операции не получен за время ожидания
        """
        status: dict[str, Any] | None = None
        operation_id = self.__delete_assets_by_ids(asset_ids)

        for _ in range(30):
            time.sleep(1)
            status = self.__remove_assets_get_status(operation_id=operation_id)
            if status is not None:
                break

        if status is None:
            self.log.error(
                "status=failed, action=delete_assets_by_ids, "
                'msg="Deleting status is not ready", '
                f"operation_id={operation_id!r}, hostname={self.__core_hostname!r}"
            )
        else:
            self.log.info(
                "status=success, action=delete_assets_by_ids, "
                'msg="Deleting finished", '
                f"status={status!r}, hostname={self.__core_hostname!r}"
            )
        return status

    def __change_asset_configuration_by_id(
        self, asset_id: str, params: dict[str, Any]
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

        if status_code != 200:
            self.log.debug(
                "status=failed, action=__change_asset_configuration_by_id, "
                f'msg="Asset change request not accepted: HTTP:{status_code!r}", '
                f"asset_id={asset_id!r}, hostname={self.__core_hostname!r}"
            )
            return None

        resp: dict[str, Any] = response.json()
        ticket_id = resp.get("ticketId")
        self.log.info(
            "status=success, action=__change_asset_configuration_by_id, "
            f'msg="Asset change request accepted", ticket_id={ticket_id!r}, '
            f"asset_id={asset_id!r}, hostname={self.__core_hostname!r}"
        )
        return str(ticket_id) if ticket_id is not None else None

    def __change_assets_get_status(self, ticket_id: str) -> dict[str, Any] | None:
        """Получить статус операции изменения актива.

        :return: {"type":"AssetsOperationResult","totalCount":2,"succeedCount":2,"failedCount":0}
            или None, если операция ещё не завершена
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

        if r.status_code != 200:
            self.log.debug(
                "status=prepare, action=__change_assets_get_status, "
                f'msg="Edit operation is not ready: HTTP:{r.status_code}", '
                f"hostname={self.__core_hostname!r}"
            )
            return None

        result = cast("dict[str, Any]", r.json())
        self.log.debug(
            "status=success, action=__change_assets_get_status, "
            f'msg="Check edit operation status: {result!r}", '
            f"hostname={self.__core_hostname!r}"
        )
        return result

    def change_asset_configuration_by_id(
        self, asset_id: str, config: dict[str, Any]
    ) -> dict[str, Any] | None:
        """Изменение активов по id.

        :param asset_id: ID актива, который надо сконфигурировать
        :param config: конфигурация актива
        :return: {"isSuccessful":true,"assetId":"17c930dc-0340-0001-0000-000000000002"}
            или None, если статус операции не получен за время ожидания
        """

        ticket_id = self.__change_asset_configuration_by_id(asset_id, config)

        status: dict[str, Any] | None = None
        if ticket_id is not None:
            for _ in range(15):
                time.sleep(2)
                status = self.__change_assets_get_status(ticket_id=ticket_id)
                if status is not None:
                    break

        if status is None:
            self.log.error(
                "status=failed, action=change_asset_configuration_by_id, "
                'msg="Editing status is not ready", '
                f"asset_id={asset_id!r}, hostname={self.__core_hostname!r}"
            )
        else:
            self.log.info(
                "status=success, action=change_asset_configuration_by_id, "
                'msg="Editing finished", '
                f"status={status!r}, hostname={self.__core_hostname!r}"
            )
        return status

    def get_asset_configuration_by_id(self, asset_id: str) -> dict[str, Any]:
        """Получение информации об активе по id.

        :param asset_id: ID интересующего актива
        :return: { dict with asset config }
        """

        resp: dict[str, Any] = {}

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

        self.log.warning(
            "status=failed, action=get_asset_configuration_by_id, "
            f'msg="Asset configuration is not available: HTTP:{r.status_code}", '
            f"asset_id={asset_id!r}, hostname={self.__core_hostname!r}"
        )
        return resp

    def get_queries(self) -> list[dict[str, Any]]:
        """Получить дерево сохранённых запросов.

        :return: Узлы дерева сохранённых запросов (см. FoldersHierarchy -
            `GET /stored_queries/folders/{hierarchyType}`, `hierarchyType=queries`)
        """
        url = (
            f"https://{self.__core_hostname}"
            f"{self.__api_assets_trm_stored_queries_folders}"
        )
        response: dict[str, Any] = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
        ).json()

        nodes = [dict(node) for node in response.get("nodes") or []]

        self.log.info(
            "status=success, action=get_queries, "
            f'msg="Got {len(nodes)!r} query tree roots", '
            f"hostname={self.__core_hostname!r}"
        )

        return nodes

    def get_query_by_id(self, queryid: str) -> dict[str, Any]:
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
        response: dict[str, Any] = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
        ).json()

        self.log.info(
            "status=success, action=get_query_by_id, "
            f'msg="Got query {response.get("displayName")!r}", '
            f"hostname={self.__core_hostname!r}"
        )

        return response

    def create_query_folder(self, parent_id: str, folder_name: str) -> dict[str, Any]:
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
        response: dict[str, Any] = exec_request(
            self.__core_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
            json=params,
        ).json()

        self.log.info(
            "status=success, action=create_query_folder, "
            f'msg="Query folder created", name={response.get("displayName")!r}, '
            f"type={response.get('type')!r}, "
            f"hostname={self.__core_hostname!r}"
        )

        return response

    def create_query(
        self, folder_id: str, query_name: str, pdql_filter: str, pdql_selection: str
    ) -> dict[str, Any]:
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

        response: dict[str, Any] = exec_request(
            self.__core_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
            json=params,
        ).json()

        self.log.info(
            "status=success, action=create_query, "
            f'msg="Query created", name={response.get("displayName")!r}, '
            f"type={response.get('type')!r}, "
            f"hostname={self.__core_hostname!r}"
        )

        return response

    def update_query(
        self,
        query_id: str,
        folder_id: str,
        query_name: str,
        pdql_filter: str,
        pdql_selection: str,
    ) -> dict[str, Any]:
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
        :return: Обновлённый объект запроса (см. :meth:`get_query_by_id`)
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

        # 204 No Content, тело ответа отсутствует
        exec_request(
            self.__core_session,
            url,
            method="PUT",
            timeout=self.settings.connection_timeout,
            json=params,
        )

        self.log.info(
            "status=success, action=update_query, "
            f'msg="Query updated", name={query_name!r}, id={query_id!r}, '
            f"hostname={self.__core_hostname!r}"
        )

        current = self.get_query_by_id(query_id)

        if (
            current.get("displayName") != query_name
            or current.get("folderId") != folder_id
        ):
            # PathInfo требует оба поля: передаём текущую связку целиком, чтобы
            # изменение имени не сбрасывало папку (и наоборот).
            exec_request(
                self.__core_session,
                f"{url}/path",
                method="PUT",
                timeout=self.settings.connection_timeout,
                json={"displayName": query_name, "folderId": folder_id},
            )
            self.log.info(
                "status=success, action=update_query_path, "
                f'msg="Query path updated", name={query_name!r}, id={query_id!r}, '
                f"hostname={self.__core_hostname!r}"
            )
            current = self.get_query_by_id(query_id)

        return current

    def __update_group_entries_by_ids(
        self,
        asset_ids: list[str],
        include: list[str] | None = None,
        exclude: list[str] | None = None,
    ) -> str:
        """Изменение групп активов по id.

        :param asset_ids: Список ID активов, для которых нужно изменить группы
        :param include: Список ID групп, в которые необходимо добавить актив
        :param exclude: Список ID групп, из которых необходимо удалить актив
        :return: ID операции изменения
        """
        self.log.debug(
            "status=prepare, action=update_group_entries_by_ids, "
            f'msg="Try to update groups for {len(asset_ids)!r} asset(s)", '
            f"hostname={self.__core_hostname!r}"
        )

        url = (
            f"https://{self.__core_hostname}{self.__api_assets_v1_update_group_entries}"
        )
        params = {
            "assetsIds": asset_ids,
            "includeInGroups": include or [],
            "excludeFromGroups": exclude or [],
        }

        response: dict[str, Any] = exec_request(
            self.__core_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
            json=params,
        ).json()

        operation_id = self.__operation_id_from_response(
            response, "update_group_entries_by_ids"
        )
        self.log.info(
            "status=success, action=update_group_entries_by_ids, "
            'msg="Update started", '
            f"operation_id={operation_id!r}, "
            f"hostname={self.__core_hostname!r}"
        )

        return operation_id

    def __update_group_entries_get_status(
        self, operation_id: str
    ) -> dict[str, Any] | None:
        """Получить статус операции изменения групп.

        :return: {"type":"AssetsOperationResult","totalCount":2,"succeedCount":2,"failedCount":0}
            или None, если операция ещё не завершена
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

        if status_code != 200:
            self.log.debug(
                "status=prepare, action=__update_group_entries_get_status, "
                f'msg="Update operation is not ready: HTTP:{status_code}", '
                f"hostname={self.__core_hostname!r}"
            )
            return None

        result = cast("dict[str, Any]", response.json())
        self.log.debug(
            "status=success, action=__update_group_entries_get_status, "
            f'msg="Check update operation status: {result!r}", '
            f"hostname={self.__core_hostname!r}"
        )
        return result

    def update_group_entries_by_ids(
        self,
        asset_ids: list[str],
        include: list[str] | None = None,
        exclude: list[str] | None = None,
    ) -> dict[str, Any] | None:
        """Изменение групп активов по id.

        :param asset_ids: Список ID активов, для которых нужно изменить группы
        :param include: Список ID групп, в которые необходимо добавить актив
        :param exclude: Список ID групп, из которых необходимо удалить актив
        :return: {"type":"AssetsOperationResult","totalCount":2,"succeedCount":2,"failedCount":0}
            или None, если статус операции не получен за время ожидания
        """

        status: dict[str, Any] | None = None
        operation_id = self.__update_group_entries_by_ids(asset_ids, include, exclude)

        for _ in range(30):
            time.sleep(1)
            status = self.__update_group_entries_get_status(operation_id=operation_id)
            if status is not None:
                break

        if status is None:
            self.log.error(
                "status=failed, action=update_group_entries_by_ids, "
                'msg="Update status is not ready", '
                f"operation_id={operation_id!r}, hostname={self.__core_hostname!r}"
            )
        else:
            self.log.info(
                "status=success, action=update_group_entries_by_ids, "
                f'msg="Update finished", status={status!r}, '
                f"hostname={self.__core_hostname!r}"
            )

        return status

    def close(self) -> None:
        if self.__core_session is not None:
            self.__core_session.close()
