from collections.abc import Iterator
from datetime import datetime
from typing import Any, cast

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

from .Conveyor import Conveyor


class Tables(ModuleInterface, LoggingHandler):
    """Tables module.

    Работа с табличными списками, установленными в SIEM (сторона Core,
    ``/api/events/v*/table_lists``), и с их белыми списками
    (``/api/whitelists/{tokenId}``, доступны с R27.0).

    MP SIEM 26.0+ : идентификатор списка для этих эндпоинтов - ``token`` из
    ``GET /api/events/v2/table_lists`` (не uuid объекта KB). Создание и
    правление самими списками - домен KB (``CoreApi.TabularLists.yaml``),
    здесь только содержимое установленных списков.
    """

    __table_add_time_format = "%d.%m.%Y %H:%M:%S"

    __api_table_list = "/api/events/v2/table_lists"
    __api_table_list_v1 = "/api/events/v1/table_lists"
    __api_whitelist = "/api/whitelists"

    # Типы списков, строки которых можно менять построчно через content PUT
    __ROW_EDITABLE_TYPES = ("correlationrule", "enrichmentrule")

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
        # {siem_key: {table_name: entry}}; siem_key - разрешённый siem_id
        # либо "" для единственного конвейера (параметр не передаётся)
        self.__tables_cache: dict[str, dict[str, dict[str, Any]]] = {}
        # Разрешение siem_id (конвейеры): при одном конвейере параметр не
        # передаётся, при нескольких - обязателен (см. Conveyor.resolve_siem_id)
        self.__conveyor = Conveyor(auth, settings)
        self.log.debug('status=success, action=prepare, msg="Table Module init"')

    # ------------------------------------------------------------------ #
    # siem_id (конвейеры)
    # ------------------------------------------------------------------ #

    def set_default_conveyor(self, siem_id: str | None = None) -> str:
        """Задать конвейер по умолчанию для всех операций с таблицами.

        Нужно, когда в Core зарегистрировано несколько конвейеров: без
        выбора Core не понимает, к какому SIEM обращаться.

        :param siem_id: id конвейера (GUID); None — основной конвейер
        :return: Установленный siem_id
        """
        return self.__conveyor.set_default_conveyor(siem_id)

    def add_params(self, siem_id: str | None = None) -> dict[str, str]:
        """Query-параметры конвейера (siem_id).

        Пусто, если конвейер в Core один (сервер выбирает его сам).
        Явный ``siem_id`` имеет приоритет над установленным по умолчанию;
        при нескольких конвейерах без выбора — ``ValueError``.
        """
        resolved = self.__conveyor.resolve_siem_id(siem_id)
        return {} if resolved is None else {"siem_id": resolved}

    def __siem_key(self, siem_id: str | None) -> str:
        resolved = self.__conveyor.resolve_siem_id(siem_id)
        return resolved if resolved is not None else ""

    def get_tables_list(self, siem_id: str | None = None) -> dict[str, dict[str, Any]]:
        """Получить список всех установленных табличных списков.

        :param siem_id: id конвейера (GUID); обязателен при нескольких
            конвейерах в Core (см. ``set_default_conveyor``)
        :return: {table_name: {id, type, editable, ttl_enabled, notifications}}
        """
        url = f"https://{self.__core_hostname}{self.__api_table_list}"
        params = self.add_params(siem_id)
        response: list[dict[str, Any]] = exec_request(
            self.__core_session,
            url,
            method="GET",
            params=params,
            timeout=self.settings.connection_timeout,
        ).json()

        tables: dict[str, dict[str, Any]] = {}
        for item in response:
            tables[item["name"]] = {
                "id": item.get("token"),
                "type": str(item.get("fillType")).lower(),
                "editable": item.get("editable"),
                "ttl_enabled": item.get("ttlEnabled"),
                "notifications": item.get("notifications"),
            }
        self.__tables_cache[params.get("siem_id", "")] = tables

        self.log.info(
            f"status=success, action=get_tables_list, "
            f'msg="Found {len(tables)} tables", '
            f"hostname={self.__core_hostname!r}, "
            f"siem_id={params.get('siem_id')!r}"
        )
        return dict(tables)

    def get_table_data(
        self,
        table_name: str,
        filters: dict[str, Any] | None = None,
        siem_id: str | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Итеративно выгрузить содержимое табличного списка.

        Пример фильтра::

            filters = {
                "select": ["_last_changed", "field2"],
                "where": "_id>5",
                "orderBy": [{"field": "_last_changed", "sortOrder": "descending"}],
                "timeZone": 0,
            }

        :param table_name: Имя таблицы
        :param filters: Фильтр (контракт content/search), опционально
        :param siem_id: id конвейера (GUID); обязателен при нескольких
            конвейерах в Core (см. ``set_default_conveyor``)
        :return: Итератор по строкам таблицы
        """
        table_id = self.get_table_id_by_name(table_name, siem_id)
        api_url = f"{self.__api_table_list}/{table_id}/content/search"
        url = f"https://{self.__core_hostname}{api_url}"

        body: dict[str, Any] = {
            "filter": {
                "where": "",
                "orderBy": [{"field": "_last_changed", "sortOrder": "descending"}],
                "timeZone": 0,
            }
        }
        if filters is not None:
            body["filter"] = filters

        is_end = False
        offset = 0
        limit = self.settings.tables_batch_size
        line_counter = 0
        start_time = get_metrics_start_time()
        while not is_end:
            ret = self.__iterate_table(url, body, offset, limit, siem_id)
            if len(ret) < limit:
                is_end = True
            offset += limit
            for row in ret:
                line_counter += 1
                yield row

        took_time = get_metrics_took_time(start_time)
        self.log.info(
            f"status=success, action=get_table_data, "
            f'msg="Query executed, response have been read", '
            f"hostname={self.__core_hostname!r}, siem_id={siem_id!r}, "
            f"lines={line_counter}, metric=get_table_data, took={took_time}ms"
        )

    def __iterate_table(
        self,
        url: str,
        body: dict[str, Any],
        offset: int,
        limit: int,
        siem_id: str | None,
    ) -> list[dict[str, Any]]:
        params = dict(body, offset=offset, limit=limit)
        response: dict[str, Any] = exec_request(
            self.__core_session,
            url,
            method="POST",
            params=self.add_params(siem_id),
            timeout=self.settings.connection_timeout,
            json=params,
        ).json()
        if "items" not in response:
            self.log.error(
                f"status=failed, action=table_iterate, "
                f'msg="Table data request has wrong response structure", '
                f"hostname={self.__core_hostname!r}"
            )
            raise RuntimeError(
                "Table data request return None or has wrong response structure"
            )
        return list(response["items"])

    def set_table_data(
        self, table_name: str, data: bytes | str, siem_id: str | None = None
    ) -> None:
        """Импортировать бинарные данные в табличный список.

        Данные должны быть в формате CSV, понятном MP SIEM.

        Usage::

            with open("import.csv", "rb") as data:
                tables.set_table_data("table_name", data)

        :param table_name: Имя таблицы
        :param data: Бинарные (или текстовые) данные CSV для вставки
        :param siem_id: id конвейера (GUID); обязателен при нескольких
            конвейерах в Core (см. ``set_default_conveyor``)
        """
        api_url = f"{self.__api_table_list_v1}/{table_name}/import"
        url = f"https://{self.__core_hostname}{api_url}"
        headers = {"Content-Type": "text/csv; charset=utf-8"}

        response: dict[str, Any] = exec_request(
            self.__core_session,
            url,
            method="POST",
            params=self.add_params(siem_id),
            timeout=self.settings.connection_timeout,
            data=data,
            headers=headers,
        ).json()

        total_records = response.get("recordsNum") or 0
        imported_records = response.get("importedNum") or 0
        bad_records = response.get("badRecordsNum") or 0
        skipped_records = response.get("skippedRecordsNum") or 0

        if (
            imported_records == 0 or imported_records <= bad_records + skipped_records
        ) and total_records != 0:
            self.log.error(
                f"status=failed, action=set_table_data, "
                f'msg="Importing data to table {table_name!r} ends with error", '
                f"hostname={self.__core_hostname!r}, siem_id={siem_id!r}, "
                f"total={total_records}, imported={imported_records}, "
                f"bad={bad_records}, skipped={skipped_records}"
            )
            raise RuntimeError(f"Importing data to table {table_name} ends with error")

        if bad_records != 0 or skipped_records != 0:
            self.log.warning(
                f"status=warning, action=set_table_data, "
                f'msg="Some data not imported to table {table_name!r}", '
                f"hostname={self.__core_hostname!r}, siem_id={siem_id!r}, "
                f"total={total_records}, imported={imported_records}, "
                f"bad={bad_records}, skipped={skipped_records}"
            )
        self.log.info(
            f"status=success, action=set_table_data, "
            f'msg="Data imported to table {table_name!r}", '
            f"hostname={self.__core_hostname!r}, siem_id={siem_id!r}, "
            f"lines={imported_records}"
        )

    def get_table_info(
        self, table_name: str, siem_id: str | None = None
    ) -> dict[str, Any]:
        """Получить метаданные табличного списка.

        :param table_name: Имя таблицы
        :param siem_id: id конвейера (GUID); обязателен при нескольких
            конвейерах в Core (см. ``set_default_conveyor``)
        :return: {property: value}
        """
        table_id = self.get_table_id_by_name(table_name, siem_id)
        api_url = f"{self.__api_table_list}/{table_id}"
        url = f"https://{self.__core_hostname}{api_url}"
        response: dict[str, Any] = exec_request(
            self.__core_session,
            url,
            method="GET",
            params=self.add_params(siem_id),
            timeout=self.settings.connection_timeout,
        ).json()

        # Кэш мог устареть (список удалён/переименован) - обновляем целиком
        table_info = dict(
            self.__tables_cache.get(self.__siem_key(siem_id), {}).get(table_name, {})
        )
        table_info["id"] = table_id
        table_info["name"] = table_name
        table_info["type"] = str(response.get("type") or "").lower()
        table_info["size_max"] = response.get("maxSize")
        table_info["size_typical"] = response.get("typicalSize")
        table_info["size_current"] = response.get("currentSize")
        table_info["ttl"] = response.get("ttl")
        table_info["description"] = response.get("description")
        table_info["created"] = response.get("created")
        table_info["updated"] = response.get("lastUpdated")
        table_info["fields"] = response.get("fields")

        self.log.info(
            f"status=success, action=get_table_info, "
            f'msg="Got {len(table_info)} properties for table {table_name!r}", '
            f"hostname={self.__core_hostname!r}, siem_id={siem_id!r}"
        )
        return table_info

    def truncate_table(self, table_name: str, siem_id: str | None = None) -> bool:
        """Очистить табличный список.

        :param table_name: Имя таблицы
        :param siem_id: id конвейера (GUID); обязателен при нескольких
            конвейерах в Core (см. ``set_default_conveyor``)
        :return: True при успехе
        :raises RuntimeError: если список не очищен
        """
        api_url = (
            f"{self.__api_table_list}/"
            f"{self.get_table_id_by_name(table_name, siem_id)}/content"
        )
        url = f"https://{self.__core_hostname}{api_url}"
        response: dict[str, Any] = exec_request(
            self.__core_session,
            url,
            method="DELETE",
            params=self.add_params(siem_id),
            timeout=self.settings.connection_timeout,
        ).json()

        if response.get("result") != "success":
            self.log.error(
                f"status=failed, action=truncate_table, "
                f'msg="Table {table_name!r} have not been truncated", '
                f"hostname={self.__core_hostname!r}"
            )
            raise RuntimeError(f"Table {table_name} have not been truncated")

        self.log.info(
            f"status=success, action=truncate_table, "
            f'msg="Table {table_name!r} have been truncated", '
            f"hostname={self.__core_hostname!r}, siem_id={siem_id!r}"
        )
        return True

    def get_table_id_by_name(self, table_name: str, siem_id: str | None = None) -> str:
        """Получение token табличного списка по его имени.

        :param table_name: Имя таблицы
        :param siem_id: id конвейера (GUID); обязателен при нескольких
            конвейерах в Core (см. ``set_default_conveyor``)
        :return: token
        :raises ValueError: если список не найден
        """
        key = self.__siem_key(siem_id)
        if key not in self.__tables_cache:
            self.get_tables_list(siem_id)
        entry = self.__tables_cache[key].get(table_name)
        if entry is None:
            raise ValueError(f"Table list {table_name!r} not found")
        return cast("str", entry.get("id"))

    def get_table_name_by_id(self, table_id: str, siem_id: str | None = None) -> str:
        """Получение имени табличного списка по его token.

        :param table_id: Token таблицы
        :param siem_id: id конвейера (GUID); обязателен при нескольких
            конвейерах в Core (см. ``set_default_conveyor``)
        :return: имя таблицы
        :raises ValueError: если список с таким token не найден
        """
        key = self.__siem_key(siem_id)
        if key not in self.__tables_cache:
            self.get_tables_list(siem_id)
        for name, entry in self.__tables_cache[key].items():
            if str(entry.get("id")) == table_id:
                return name
        raise ValueError(f"Table with ID={table_id!r} not found")

    def set_table_row(
        self,
        table_name: str,
        add_rows: list[dict[str, Any]] | None = None,
        remove_rows: list[dict[str, Any]] | None = None,
        siem_id: str | None = None,
    ) -> None:
        """Добавить/удалить строки установленного табличного списка.

        Операция опасна (без остановки правил). Маппинга по именам полей в API
        нет - позиция значения в массиве определяет поле, поэтому порядок
        значений выстраивается по ``fields`` из ``get_table_info``.

        :param table_name: Имя таблицы
        :param add_rows: [{"field1": "value"}, ...]
        :param remove_rows: [{"field1": "value"}, ...]
        :param siem_id: id конвейера (GUID); обязателен при нескольких
            конвейерах в Core (см. ``set_default_conveyor``)
        """
        if add_rows is None and remove_rows is None:
            self.log.info(
                f"status=prepare, action=set_table_row, "
                f'msg="Nothing to add/remove for table {table_name!r}", '
                f"hostname={self.__core_hostname!r}, siem_id={siem_id!r}"
            )
            return

        table_info = self.get_table_info(table_name, siem_id)
        if table_info.get("type") not in self.__ROW_EDITABLE_TYPES:
            raise ValueError(
                f"Unsupported table type {table_info.get('type')!r} to add/remove row"
            )

        row_matrix: list[Any] = []
        fields_types: dict[str, Any] = {}
        attrs_position: dict[str, int] = {}
        key_fields: set[str] = set()
        not_nullable_fields: set[str] = set()
        for counter, field in enumerate(table_info.get("fields") or []):
            name = field.get("name")
            attrs_position[name] = counter
            fields_types[name] = field.get("type")
            row_matrix.append(None)
            if field.get("primaryKey"):
                key_fields.add(name)
            if not field.get("nullable"):
                not_nullable_fields.add(name)

        params: dict[str, Any] = {
            "add": (
                self.__prepare_rows(
                    add_rows,
                    row_matrix,
                    attrs_position,
                    key_fields,
                    not_nullable_fields,
                    fields_types,
                )
                if add_rows is not None
                else None
            ),
            "remove": (
                self.__prepare_rows(
                    remove_rows,
                    row_matrix,
                    attrs_position,
                    key_fields,
                    not_nullable_fields,
                    fields_types,
                )
                if remove_rows is not None
                else None
            ),
        }

        api_url = f"{self.__api_table_list}/{table_info.get('id')}/content"
        url = f"https://{self.__core_hostname}{api_url}"
        response: dict[str, Any] = exec_request(
            self.__core_session,
            url,
            method="PUT",
            params=self.add_params(siem_id),
            timeout=self.settings.connection_timeout,
            json=params,
        ).json()

        if response.get("result") != "success":
            self.log.error(
                f"status=failed, action=set_table_row, "
                f'msg="Got error while manipulate with table {table_name!r} rows", '
                f"hostname={self.__core_hostname!r}, siem_id={siem_id!r}, "
                f'error="{response.get("results")}"'
            )
            raise RuntimeError("Got error while manipulate with table rows")

        self.log.info(
            f"status=success, action=set_table_row, "
            f'msg="Added {len(add_rows) if add_rows else 0} rows '
            f"Removed {len(remove_rows) if remove_rows else 0} rows "
            f'in table {table_name!r}", '
            f"hostname={self.__core_hostname!r}, siem_id={siem_id!r}"
        )

    def __prepare_rows(
        self,
        rows: list[dict[str, Any]],
        matrix: list[Any],
        positions: dict[str, int],
        keys: set[str],
        not_nulls: set[str],
        fields_types: dict[str, Any],
    ) -> list[list[Any]]:
        ret: list[list[Any]] = []
        for row in rows:
            tpl = matrix.copy()

            if not keys.issubset(row.keys()):
                raise ValueError(f"Key fields {keys} not found in {row}")
            if not not_nulls.issubset(row.keys()):
                raise ValueError(f"Not nullable fields {not_nulls} not found in {row}")

            for key, value in row.items():
                pos = positions.get(key)
                if pos is None:
                    raise ValueError(
                        f"Key {key} not found in schema {positions.keys()}"
                    )

                field_type = fields_types.get(key)
                if field_type == "number" and not isinstance(value, int):
                    converted = int(value)
                elif field_type == "datetime" and not isinstance(value, int):
                    converted = round(
                        datetime.strptime(str(value), self.__table_add_time_format)
                        .astimezone()
                        .timestamp()
                    )
                else:
                    converted = value
                tpl[pos] = converted
            ret.append(tpl)
        return ret

    # ------------------------------------------------------------------ #
    # Whitelists API (CoreApi.Whitelists.yaml), доступен с R27.0
    # ------------------------------------------------------------------ #

    def __check_whitelist_supported(self, action: str) -> None:
        if self.__core_release < (27, 0):
            raise RuntimeError(
                f"SIEM version {self.__core_version} does not support whitelist API "
                f"({action}); R27.0+ required"
            )

    def can_edit_whitelist(self, table_name: str, siem_id: str | None = None) -> bool:
        """Можно ли редактировать белый список (контракт CanEdit).

        Core отвечает ``204/200`` для списков-белых списков и ``404`` для
        прочих табличных списков.

        :param table_name: Имя таблицы
        :param siem_id: id конвейера (GUID) для резолвинга token списка;
            обязателен при нескольких конвейерах (см.
            ``set_default_conveyor``)
        :return: True если список доступен для редактирования
        """
        self.__check_whitelist_supported("can_edit_whitelist")
        table_id = self.get_table_id_by_name(table_name, siem_id)
        api_url = f"{self.__api_whitelist}/{table_id}/canEdit"
        url = f"https://{self.__core_hostname}{api_url}"
        try:
            exec_request(
                self.__core_session,
                url,
                method="GET",
                timeout=self.settings.connection_timeout,
            )
        except requests.HTTPError:
            # 404/4xx => список не является редактируемым белым списком
            # (сетевые ошибки не глушим: они не про права/тип списка)
            self.log.info(
                f"status=success, action=can_edit_whitelist, "
                f'msg="Whitelist {table_name!r} is not editable", '
                f"hostname={self.__core_hostname!r}"
            )
            return False

        self.log.info(
            f"status=success, action=can_edit_whitelist, "
            f'msg="Whitelist {table_name!r} is editable", '
            f"hostname={self.__core_hostname!r}"
        )
        return True

    def whitelist_rows_exists(
        self,
        table_name: str,
        rows: list[list[str]],
        siem_id: str | None = None,
    ) -> list[bool]:
        """Проверить нахождение строк в белом списке (контракт IsRowExists).

        :param table_name: Имя таблицы
        :param rows: [["Subrule_Unix_PortForwarding", "445", "alert_context"], ...]
        :param siem_id: id конвейера (GUID) для резолвинга token списка;
            обязателен при нескольких конвейерах (см.
            ``set_default_conveyor``)
        :return: список булев по каждой строке
        """
        self.__check_whitelist_supported("whitelist_rows_exists")
        table_id = self.get_table_id_by_name(table_name, siem_id)
        api_url = f"{self.__api_whitelist}/{table_id}/exists"
        url = f"https://{self.__core_hostname}{api_url}"
        response: list[bool] = exec_request(
            self.__core_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
            json=rows,
        ).json()
        self.log.info(
            f"status=success, action=whitelist_rows_exists, "
            f'msg="Checked {len(rows)} rows in {table_name!r}", '
            f"hostname={self.__core_hostname!r}"
        )
        return response

    def insert_whitelist_rows(
        self, table_name: str, row: list[str], siem_id: str | None = None
    ) -> str:
        """Вставить одну строку в белый список (контракт InsertRow).

        Контракт ожидает плоский массив значений строки
        (``["col1", "col2", ...]``), а не список строк.

        :param table_name: Имя таблицы
        :param row: Значения строки по порядку её полей
        :param siem_id: id конвейера (GUID) для резолвинга token списка;
            обязателен при нескольких конвейерах (см.
            ``set_default_conveyor``)
        :return: Текст ответа
        """
        self.__check_whitelist_supported("insert_whitelist_rows")
        table_id = self.get_table_id_by_name(table_name, siem_id)
        api_url = f"{self.__api_whitelist}/{table_id}/insert"
        url = f"https://{self.__core_hostname}{api_url}"
        response = exec_request(
            self.__core_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
            json=row,
        )
        self.log.info(
            f"status=success, action=insert_whitelist_rows, "
            f'msg="Inserted row to {table_name!r}", '
            f"hostname={self.__core_hostname!r}"
        )
        return response.text

    def remove_whitelist_rows(
        self, table_name: str, row: list[str], siem_id: str | None = None
    ) -> str:
        """Удалить одну строку из белого списка (контракт RemoveRow).

        :param table_name: Имя таблицы
        :param row: Значения удаляемой строки по порядку её полей
        :param siem_id: id конвейера (GUID) для резолвинга token списка;
            обязателен при нескольких конвейерах (см.
            ``set_default_conveyor``)
        :return: Текст ответа
        """
        self.__check_whitelist_supported("remove_whitelist_rows")
        table_id = self.get_table_id_by_name(table_name, siem_id)
        api_url = f"{self.__api_whitelist}/{table_id}/remove"
        url = f"https://{self.__core_hostname}{api_url}"
        response = exec_request(
            self.__core_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
            json=row,
        )
        self.log.info(
            f"status=success, action=remove_whitelist_rows, "
            f'msg="Removed row from {table_name!r}", '
            f"hostname={self.__core_hostname!r}"
        )
        return response.text

    def close(self) -> None:
        if self.__core_session is not None:
            self.__core_session.close()
