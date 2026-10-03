from typing import Any, cast

from mpsiemlib.common import (
    AuthError,
    LoggingHandler,
    ModuleInterface,
    MPSIEMAuth,
    Settings,
    exec_request,
)


class Filters(ModuleInterface, LoggingHandler):
    """Filters module."""

    __api_filters_list = "/api/v2/events/filters_hierarchy"
    __api_filter_v3 = "/api/v3/events/filters"
    __api_filter_v2 = "/api/v2/events/filters"
    __api_filters_by_tag = "/api/v3/events/filters_by_tags"
    __api_folder = "/api/v2/events/folders"

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
        # Релиз ядра (MAJOR, MINOR) из версии "27.6.40521". Сравнение кортежем,
        # а не float: "26.10" превращается в 26.1 и неотличим от 26.1.
        version_parts = self.__core_version.split(".")
        self.__core_release: tuple[int, int] = (
            int(version_parts[0]),
            int(version_parts[1]),
        )
        self.__folders: dict[str, dict[str, Any]] = {}
        self.__filters: dict[str, dict[str, Any]] = {}
        self.log.debug('status=success, action=prepare, msg="Filters Module init"')

    def __reset_hierarchy_cache(self) -> None:
        """Сбросить кэш иерархии. Он устаревает при любом изменении папок или фильтров."""
        self.__folders = {}
        self.__filters = {}

    def get_folders_list(self) -> dict:
        """Получить список всех папок с фильтрами.

        :return: {"id": {"parent_id": "value", "name": "value",
            "source": "value"}}
        """
        if len(self.__folders) == 0:
            url = f"https://{self.__core_hostname}{self.__api_filters_list}"

            response: dict[str, Any] = exec_request(
                self.__core_session,
                url,
                method="GET",
                timeout=self.settings.connection_timeout,
            ).json()

            self.__iterate_folders_tree(response.get("roots", []))

            self.log.info(
                f"status=success, action=get_folders_list, "
                f'msg="Got {len(self.__folders)!r} folders", '
                f"hostname={self.__core_hostname!r}"
            )

        return dict(self.__folders)

    def create_event_filter_folder(self, folder_name: str, parent_id: str) -> str:
        """Создать директорию для фильтров.

        :param folder_name: Имя создаваемой директории
        :param parent_id: ID родительской директории
        :return: folder_id: ID созданной директории
        """
        url = f"https://{self.__core_hostname}{self.__api_folder}"
        params = {"name": folder_name, "parentId": parent_id}
        response: dict[str, Any] = exec_request(
            self.__core_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
            json=params,
        ).json()

        folder_id = cast("str", response["folderId"])
        self.log.info(
            f"status=success, action=create_event_filter_folder, "
            f'msg="Created folder {folder_name!r}", folder_id={folder_id!r}, '
            f"hostname={self.__core_hostname!r}"
        )
        # Кэш иерархии устарел
        self.__reset_hierarchy_cache()
        return folder_id

    def get_event_filter_folder_info(self, folder_id: str) -> dict:
        """Получить информацию о папке фильтров (краткая форма).

        :param folder_id: ID папки
        :return: {"folderId": "...", "name": "...", "type": "user",
            "foldersCount": 0, "filtersCount": 1, "userDefinedId": None,
            "orderInFolder": 0}
        """
        api_url = f"{self.__api_folder}/{folder_id}"
        url = f"https://{self.__core_hostname}{api_url}"

        response: dict[str, Any] = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
        ).json()

        self.log.info(
            f"status=success, action=get_event_filter_folder_info, "
            f'msg="Got info for folder {folder_id!r}", '
            f"hostname={self.__core_hostname!r}"
        )
        return response

    def update_event_filter_folder(
        self, folder_id: str, folder_name: str, parent_id: str
    ) -> None:
        """Изменить папку фильтров: имя и/или местоположение.

        Контракт UpdateFolder требует оба поля FiltersFolder (`name` и
        `parentId`), поэтому родитель передаётся всегда. `parentId` не является
        декоративным: PUT с другим родителем переносит папку (измерено на
        стенде - в иерархии меняется parent_id). Для переименования на месте
        следует передавать текущего родителя.

        :param folder_id: ID папки
        :param folder_name: Новое имя папки
        :param parent_id: ID родительской папки (текущий, либо новый при
            переносе)
        """
        api_url = f"{self.__api_folder}/{folder_id}"
        url = f"https://{self.__core_hostname}{api_url}"
        params = {"name": folder_name, "parentId": parent_id}

        exec_request(
            self.__core_session,
            url,
            method="PUT",
            timeout=self.settings.connection_timeout,
            json=params,
        )

        self.log.info(
            f"status=success, action=update_event_filter_folder, "
            f'msg="Folder {folder_name!r} updated", folder_id={folder_id!r}, '
            f"hostname={self.__core_hostname!r}"
        )
        self.__reset_hierarchy_cache()

    def delete_event_filter_folder(self, folder_id: str) -> None:
        """Удалить папку фильтров.

        Вложенные фильтры удаляются вместе с папкой (измерено на стенде:
        DELETE непустой папки - 200, дочерний фильтр после этого недоступен).

        :param folder_id: ID папки
        """
        api_url = f"{self.__api_folder}/{folder_id}"
        url = f"https://{self.__core_hostname}{api_url}"

        exec_request(
            self.__core_session,
            url,
            method="DELETE",
            timeout=self.settings.connection_timeout,
        )

        self.log.info(
            f"status=success, action=delete_event_filter_folder, "
            f'msg="Folder deleted", folder_id={folder_id!r}, '
            f"hostname={self.__core_hostname!r}"
        )
        self.__reset_hierarchy_cache()

    def get_event_filter_folders_in_folder(self, folder_id: str) -> list[dict]:
        """Получить вложенные папки указанной папки.

        :param folder_id: ID родительской папки
        :return: [{"folderId": "...", "name": "...", "type": "user",
            "foldersCount": 0, "filtersCount": 0, "orderInFolder": 0}, ...]
        """
        api_url = f"{self.__api_folder}/{folder_id}/folders"
        url = f"https://{self.__core_hostname}{api_url}"

        response: list[dict[str, Any]] = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
        ).json()

        self.log.info(
            f"status=success, action=get_event_filter_folders_in_folder, "
            f'msg="Got {len(response)!r} folders in {folder_id!r}", '
            f"hostname={self.__core_hostname!r}"
        )
        return response

    def get_event_filters_in_folder(self, folder_id: str) -> list[dict]:
        """Получить фильтры, лежащие в указанной папке.

        :param folder_id: ID папки
        :return: [{"id": "...", "name": "...", "type": "user",
            "folderId": "...", "isDefault": false, "orderInFolder": 0}, ...]
        """
        api_url = f"{self.__api_folder}/{folder_id}/filters"
        url = f"https://{self.__core_hostname}{api_url}"

        response: list[dict[str, Any]] = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
        ).json()

        self.log.info(
            f"status=success, action=get_event_filters_in_folder, "
            f'msg="Got {len(response)!r} filters in {folder_id!r}", '
            f"hostname={self.__core_hostname!r}"
        )
        return response

    def create_event_filter_v2(
        self, filter_name: str, folder_id: str, params: dict
    ) -> str:
        """Создать фильтр по контракту v2 (FilterDto).

        :param filter_name: Имя создаваемого фильтра
        :param folder_id: ID директории, в которой создается фильтр
        :param params: Части фильтра по контракту FilterDto
            (select/where/orderBy и так далее), а не PDQL-строка
        :return: filter_id: ID созданного фильтра
        """
        url = f"https://{self.__core_hostname}{self.__api_filter_v2}"
        # Не мутируем словарь вызывающего
        body = dict(params, folderId=folder_id, name=filter_name)
        response: dict[str, Any] = exec_request(
            self.__core_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
            json=body,
        ).json()

        filter_id = cast("str", response["id"])
        self.log.info(
            f"status=success, action=create_event_filter_v2, "
            f'msg="Created filter {filter_id!r}", '
            f"hostname={self.__core_hostname!r}"
        )
        self.__reset_hierarchy_cache()
        return filter_id

    def create_event_filter_v3(
        self, filter_name: str, folder_id: str, pdql_query: str
    ) -> str:
        """Создать фильтр по контракту v3 (PDQL-строка).

        :param filter_name: Имя создаваемого фильтра
        :param folder_id: ID директории, в которой создается фильтр
        :param pdql_query: Поисковый запрос PDQL одной строкой
        :return: filter_id: ID созданного фильтра
        """
        url = f"https://{self.__core_hostname}{self.__api_filter_v3}"
        params = {"folderId": folder_id, "name": filter_name, "pdqlQuery": pdql_query}
        response: dict[str, Any] = exec_request(
            self.__core_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
            json=params,
        ).json()

        filter_id = cast("str", response["id"])
        self.log.info(
            f"status=success, action=create_event_filter_v3, "
            f'msg="Created filter {filter_id!r}", '
            f"hostname={self.__core_hostname!r}"
        )
        self.__reset_hierarchy_cache()
        return filter_id

    def create_event_filter(
        self, filter_name: str, folder_id: str, pdql_query: str | dict
    ) -> str:
        """Создать фильтр, выбирая контракт по типу запроса.

        Имя параметра сохранено ради совместимости вызовов, но контракт у
        запросов разный: PDQL-строка понимает только v3 (R26.1 и новее),
        а v2 принимает словарь с частями фильтра (FilterDto). Разбор по типу
        вместо проверки версии ядра означает, что вызывающий сам выбирает
        контракт, а не получает TypeError на строке в create_event_filter_v2.

        :param filter_name: Имя создаваемого фильтра
        :param folder_id: ID директории, в которой создается фильтр
        :param pdql_query: PDQL-строка (контракт v3) либо словарь частей
            фильтра (контракт v2)
        :return: filter_id: ID созданного фильтра
        """
        if isinstance(pdql_query, str):
            if self.__core_release < (26, 1):
                raise ValueError(
                    f"Core {self.__core_version}: создание фильтра по PDQL-строке "
                    "требует R26.1+, используйте create_event_filter_v2 "
                    "со словарём частей фильтра"
                )
            self.log.debug("Using create_event_filter_v3")
            return self.create_event_filter_v3(filter_name, folder_id, pdql_query)

        self.log.debug("Using create_event_filter_v2")
        return self.create_event_filter_v2(filter_name, folder_id, pdql_query)

    def get_filters_list(self) -> dict:
        """Получить список всех фильтров.

        :return: {"id": {"folder_id": "value", "name": "value",
            "source": "value"}}
        """
        if len(self.__filters) == 0:
            # папки и фильтры лежат в одной структуре и парсятся совместно
            self.get_folders_list()

            self.log.info(
                f"status=success, action=get_filters_list, "
                f'msg="Got {len(self.__filters)!r} filters", '
                f"hostname={self.__core_hostname!r}"
            )

        return dict(self.__filters)

    def __iterate_folders_tree(
        self, root_node: list[dict[str, Any]], parent_id: str | None = None
    ) -> None:
        for node in root_node:
            # id и type обязательны по контракту HierarchyNode
            node_id = node["id"]
            node_name = node.get("name")
            node_source = node.get("meta", {}).get("source")
            if node["type"] == "filter_node":
                self.__filters[node_id] = {
                    "folder_id": parent_id,
                    "name": node_name,
                    "source": node_source,
                }
                continue
            if node["type"] == "folder_node":
                self.__folders[node_id] = {
                    "parent_id": parent_id,
                    "name": node_name,
                    "source": node_source,
                }
                node_children = node.get("children") or []
                if len(node_children) != 0:
                    self.__iterate_folders_tree(node_children, node_id)

    def get_filter_info(self, filter_id: str) -> dict:
        """Получить информацию по фильтру (контракт по версии ядра).

        :param filter_id: ID фильтра
        :return: {"param1": "value", "param2": "value"}
        """
        self.log.debug(f"Current core version: {self.__core_version!r}")
        if self.__core_release < (26, 1):
            self.log.debug("Using get_filter_info_v2")
            return self.get_filter_info_v2(filter_id)
        else:
            self.log.debug("Using get_filter_info_v3")
            return self.get_filter_info_v3(filter_id)

    def get_filter_info_v2(self, filter_id: str) -> dict:
        """Получить информацию по фильтру (контракт v2).

        :param filter_id: ID фильтра
        :return: {"param1": "value", "param2": "value"}
        """
        api_url = f"{self.__api_filter_v2}/{filter_id}"
        url = f"https://{self.__core_hostname}{api_url}"

        response: dict[str, Any] = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
        ).json()

        self.log.info(
            f"status=success, action=get_filter_info_v2, "
            f'msg="Got info for filter {filter_id!r}", '
            f"hostname={self.__core_hostname!r}"
        )

        return {
            "name": response.get("name"),
            "folder_id": response.get("folderId"),
            "removed": response.get("isRemoved"),
            "source": response.get("source"),
            "query": {
                "select": response.get("select"),
                "where": response.get("where"),
                "group": response.get("groupBy"),
                "order": response.get("orderBy"),
                "aggregate": response.get("aggregateBy"),
                "distribute": response.get("distributeBy"),
                "top": response.get("top"),
                "aliases": response.get("aliases"),
            },
        }

    def get_filter_info_v3(self, filter_id: str) -> dict:
        """Получить информацию по фильтру (контракт v3).

        :param filter_id: ID фильтра
        :return: {"param1": "value", "param2": "value"}
        """
        api_url = f"{self.__api_filter_v3}/{filter_id}"
        url = f"https://{self.__core_hostname}{api_url}"

        response: dict[str, Any] = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
        ).json()

        self.log.info(
            f"status=success, action=get_filter_info_v3, "
            f'msg="Got info for filter {filter_id!r}", '
            f"hostname={self.__core_hostname!r}"
        )

        return {
            "name": response.get("name"),
            "folder_id": response.get("folderId"),
            "removed": response.get("isRemoved"),
            "source": response.get("source"),
            "pdqlQuery": response.get("pdqlQuery"),
        }

    def get_filters_by_ids(
        self, filter_ids: list[str], with_removed: bool = False
    ) -> list[dict]:
        """Получить фильтры по списку идентификаторов (контракт v3).

        Эндпоинт v2 (`GET /api/v2/events/filters?filterId=`) на стенде R27.6
        отвечает 500 Internal Server Error, поэтому маршрут выбран v3.

        :param filter_ids: Идентификаторы фильтров
        :param with_removed: Нужно ли вернуть удалённые фильтры
        :return: [{"id": "...", "name": "...", "isRemoved": false}, ...]
        """
        params: dict[str, Any] = {
            "filterId": filter_ids,
            "withRemoved": "true" if with_removed else "false",
        }
        url = f"https://{self.__core_hostname}{self.__api_filter_v3}"

        response: list[dict[str, Any]] = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
            params=params,
        ).json()

        self.log.info(
            f"status=success, action=get_filters_by_ids, "
            f'msg="Got {len(response)!r} filters for '
            f'{len(filter_ids)!r} id(s)", '
            f"hostname={self.__core_hostname!r}"
        )
        return response

    def get_filters_by_tag(self, tag: str, with_removed: bool = False) -> list[dict]:
        """Получить фильтры по тегу (контракт GetFiltersByTag, v3).

        Теги фильтрам присваивает не Events API: модель фильтра (v2 и v3) поля
        для тегов не имеет, а сервер молча игнорирует `tags` в теле POST/PUT
        (измерено на R27.6). Источник тегов - внешняя относительно Core Events
        подсистема, поэтому метод возвращает [] для тегов, проставленных не
        через неё, и это не ошибка.

        Эндпоинт v2 (`GET /api/v2/events/filters_by_tags`) отвечает так же
        (измерено), маршрут выбран v3 за единообразие с get_filters_by_ids и
        get_default_filter.

        :param tag: Тег
        :param with_removed: Нужно ли вернуть удалённые фильтры
        :return: [{"id": "...", "name": "...", "folderId": "...",
            "isRemoved": false, "source": "user", ...}, ...]
        """
        # Сервер требует непустой tag и отвечает на него 400 BadRequest
        # (измерено: пустая строка и пробелы тоже 400), local-check даёт
        # внятную ошибку вместо round-trip
        if not tag.strip():
            raise ValueError("Тег не может быть пустым")

        params: dict[str, Any] = {
            "tag": tag,
            "withRemoved": "true" if with_removed else "false",
        }
        url = f"https://{self.__core_hostname}{self.__api_filters_by_tag}"

        response: list[dict[str, Any]] = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
            params=params,
        ).json()

        self.log.info(
            f"status=success, action=get_filters_by_tag, "
            f'msg="Got {len(response)!r} filters for tag {tag!r}", '
            f"hostname={self.__core_hostname!r}"
        )
        return response

    def get_default_filter(self) -> dict:
        """Получить фильтр по умолчанию (контракт v3).

        :return: {"id": "...", "name": "...", "folderId": "...",
            "isRemoved": false, "source": "system", "pdqlQuery": "..."}
        """
        url = f"https://{self.__core_hostname}{self.__api_filter_v3}/default"

        response: dict[str, Any] = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
        ).json()

        self.log.info(
            f"status=success, action=get_default_filter, "
            f'msg="Got default filter {response.get("name")!r}", '
            f"hostname={self.__core_hostname!r}"
        )
        return response

    def update_event_filter_v3(
        self, filter_id: str, filter_name: str, folder_id: str, pdql_query: str
    ) -> None:
        """Изменить фильтр (контракт v3).

        В отличие от stored_queries, здесь `name` и `folderId` применяются:
        переименование и PDQL проходят одним PUT (измерено на стенде).

        :param filter_id: ID фильтра
        :param filter_name: Новое имя фильтра
        :param folder_id: ID папки-контейнера
        :param pdql_query: Новый PDQL-запрос
        """
        api_url = f"{self.__api_filter_v3}/{filter_id}"
        url = f"https://{self.__core_hostname}{api_url}"
        params = {"name": filter_name, "folderId": folder_id, "pdqlQuery": pdql_query}

        exec_request(
            self.__core_session,
            url,
            method="PUT",
            timeout=self.settings.connection_timeout,
            json=params,
        )

        self.log.info(
            f"status=success, action=update_event_filter_v3, "
            f'msg="Filter {filter_name!r} updated", filter_id={filter_id!r}, '
            f"hostname={self.__core_hostname!r}"
        )
        self.__reset_hierarchy_cache()

    def update_event_filter_v2(
        self, filter_id: str, filter_name: str, folder_id: str, params: dict
    ) -> None:
        """Изменить фильтр (контракт v2, FilterDto).

        :param filter_id: ID фильтра
        :param filter_name: Новое имя фильтра
        :param folder_id: ID папки-контейнера
        :param params: Части фильтра (select/where/orderBy и так далее)
        """
        api_url = f"{self.__api_filter_v2}/{filter_id}"
        url = f"https://{self.__core_hostname}{api_url}"
        body = dict(params, folderId=folder_id, name=filter_name)

        exec_request(
            self.__core_session,
            url,
            method="PUT",
            timeout=self.settings.connection_timeout,
            json=body,
        )

        self.log.info(
            f"status=success, action=update_event_filter_v2, "
            f'msg="Filter {filter_name!r} updated", filter_id={filter_id!r}, '
            f"hostname={self.__core_hostname!r}"
        )
        self.__reset_hierarchy_cache()

    def delete_event_filter(self, filter_id: str) -> None:
        """Удалить фильтр.

        Удаление помечает фильтр (`isRemoved=true`): он исчезает из иерархии и
        из выборки без `withRemoved=true`, но остаётся доступным по id.

        :param filter_id: ID фильтра
        """
        api_url = f"{self.__api_filter_v2}/{filter_id}"
        url = f"https://{self.__core_hostname}{api_url}"

        exec_request(
            self.__core_session,
            url,
            method="DELETE",
            timeout=self.settings.connection_timeout,
        )

        self.log.info(
            f"status=success, action=delete_event_filter, "
            f'msg="Filter deleted", filter_id={filter_id!r}, '
            f"hostname={self.__core_hostname!r}"
        )
        self.__reset_hierarchy_cache()

    def close(self) -> None:
        if self.__core_session is not None:
            self.__core_session.close()
