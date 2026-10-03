import re
from typing import Any

import requests

from mpsiemlib.common import (
    AuthError,
    LoggingHandler,
    ModuleInterface,
    MPComponents,
    MPSIEMAuth,
    Settings,
    exec_request,
)


class Macros(ModuleInterface, LoggingHandler):
    """Macros module.

    MP SIEM 26.0: макросы - контент KB. Список отдаёт kb-ui API на KB-стороне
    (порт 8091) с обязательным заголовком ``Content-Database``: контрактный
    ``GET /api/SiemMacros/`` на 26.0 неработоспособен (415 без Content-Type,
    с ним - 400 "A non-empty request body is required", измерено на стенде).
    Остальные операции (CRUD макросов, параметров, локалей, меток) -
    контрактный Content API ``CoreApi.SiemMacros.yaml``
    (``/api/SiemMacros``) и ``CoreApi.SiemMacrosTag.yaml``
    (``/api/SiemMacrosTag``) с обязательным query-параметром
    ``contentDatabase``.
    """

    __kb_port = 8091
    __api_macros_list = "/api-studio/siem/macros/list"
    __api_macros_info = "/api-studio/siem/macros/"
    __api_kb_db_list = "/api-studio/content-database-selector/content-databases"

    # Контрактный Content API (CoreApi.SiemMacros.yaml / .SiemMacrosTag.yaml),
    # обслуживается на KB-стороне
    __api_content_siem_macros = "/api/SiemMacros"
    __api_content_siem_macros_tags = "/api/SiemMacrosTag"

    def __init__(self, auth: MPSIEMAuth, settings: Settings) -> None:
        ModuleInterface.__init__(self, auth, settings)
        LoggingHandler.__init__(self)
        sessions = auth.sessions or {}
        kb_session = sessions.get("kb")
        if kb_session is None:
            # MPSIEMWorker подключает только core/ms, поэтому KB-сессию
            # поднимаем лениво при первом обращении к модулю
            kb_session = auth.connect(MPComponents.KB)
        self.__kb_session = kb_session
        creds = auth.get_creds()
        if creds is None or creds.core_hostname is None:
            raise AuthError("Core hostname is not set")
        self.__kb_hostname = creds.core_hostname
        self.__macros: list[dict[str, Any]] = []
        self.__db_name: str | None = None
        self.log.debug('status=success, action=prepare, msg="Macros Module init"')

    def set_db_name(self, db_name: str) -> None:
        """Установить БД по умолчанию для работы с макросами.

        :param db_name: Имя БД в KB
        """
        self.__db_name = db_name

    def __resolve_db_name(self, db_name: str | None) -> str:
        """Использовать БД из параметра, из ``set_db_name``, либо первую
        доступную на стенде."""
        if db_name is not None:
            return db_name
        if self.__db_name is not None:
            return self.__db_name

        url = f"https://{self.__kb_hostname}:{self.__kb_port}{self.__api_kb_db_list}"
        databases: list[dict[str, Any]] = exec_request(
            self.__kb_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
        ).json()
        for db in databases:
            name = db.get("Name")
            if name:
                self.log.info(
                    f'status=success, action=resolve_db_name, msg="Selected first '
                    f'available db {name!r}", hostname={self.__kb_hostname!r}'
                )
                return str(name)

        raise ValueError("No content databases available on KB")

    @staticmethod
    def __kb_headers(db_name: str) -> dict[str, str]:
        return {"Content-Database": db_name, "Content-Locale": "RUS"}

    def __request_macros_page(
        self, db_name: str, search: str, skip: int, take: int
    ) -> list[dict[str, Any]]:
        """Страница макросов из kb-ui ``POST /api-studio/siem/macros/list``."""
        url = f"https://{self.__kb_hostname}:{self.__kb_port}{self.__api_macros_list}"
        body: dict[str, Any] = {
            "tagId": None,
            "sort": [{"name": "objectId", "order": 0, "type": 0}],
            "filters": None,
            "search": search,
            "skip": skip,
            "take": take,
        }

        response: dict[str, Any] = exec_request(
            self.__kb_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
            json=body,
            headers=self.__kb_headers(db_name),
        ).json()

        return list(response.get("Rows") or [])

    def get_macros_list(
        self, db_name: str | None = None, do_refresh: bool = False
    ) -> list[dict[str, Any]]:
        """Получить список всех макросов.

        :param db_name: Имя БД (None - из ``set_db_name``, иначе первая
            доступная)
        :param do_refresh: Обновить кэш
        :return: [{"id": ..., "name": ..., "object_id": ...}]
        """
        db_name = self.__resolve_db_name(db_name)
        if not do_refresh and len(self.__macros) != 0:
            return list(self.__macros)

        limit = self.settings.kb_objects_batch_size
        self.__macros.clear()
        offset = 0
        while True:
            rows = self.__request_macros_page(db_name, "", offset, limit)
            for macro in rows:
                self.__macros.append(
                    {
                        "id": macro.get("Id"),
                        "name": macro.get("Name"),
                        "object_id": macro.get("ObjectId"),
                    }
                )
            if len(rows) < limit:
                break
            offset += limit

        self.log.info(
            f'status=success, action=get_macros_list, msg="Got {len(self.__macros)} '
            f'macros", hostname={self.__kb_hostname!r}, db={db_name!r}'
        )

        return list(self.__macros)

    def get_macros_info(
        self, macro_id: str, db_name: str | None = None
    ) -> dict[str, Any]:
        """Получить описание макроса по id.

        :param macro_id: ID макроса из ``get_macros_list``
        :param db_name: Имя БД (None - из ``set_db_name``, иначе первая
            доступная)
        :return: {"id": ..., "name": ..., "text": ...}
        """
        db_name = self.__resolve_db_name(db_name)
        url = f"https://{self.__kb_hostname}:{self.__kb_port}{self.__api_macros_info}{macro_id}"

        response: dict[str, Any] = exec_request(
            self.__kb_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
            headers=self.__kb_headers(db_name),
        ).json()

        self.log.info(
            f'status=success, action=get_macros_info, msg="Got info for macro '
            f'{macro_id!r}", hostname={self.__kb_hostname!r}, db={db_name!r}'
        )

        return {
            "id": macro_id,
            "name": response.get("Name"),
            "text": response.get("Text"),
        }

    def get_macros_by_name(
        self, macro_name: str, db_name: str | None = None
    ) -> dict[str, Any] | None:
        """Получение макроса по имени.

        :param macro_name: Имя макроса
        :param db_name: Имя БД (None - из ``set_db_name``, иначе первая
            доступная)
        :return: Макрос либо ``None``, если имени нет в списке
        """
        for macro in self.get_macros_list(db_name):
            if macro.get("name") == macro_name:
                return macro
        return None

    def get_macros_by_object_id(
        self, object_id: str, db_name: str | None = None
    ) -> dict[str, Any] | None:
        """Получение макроса по ObjectId.

        :param object_id: ObjectId макроса
        :param db_name: Имя БД (None - из ``set_db_name``, иначе первая
            доступная)
        :return: Макрос либо ``None``, если ObjectId нет в списке
        """
        for macro in self.get_macros_list(db_name):
            if macro.get("object_id") == object_id:
                return macro
        return None

    def get_macros_id_by_filter_name(
        self, filter_name: str, db_name: str | None = None
    ) -> str | None:
        """Получить ObjectId макроса по имени.

        ``search`` применяется на сервере нестрого, поэтому среди
        кандидатов отбирается точное совпадение имени.

        :param filter_name: Имя макроса
        :param db_name: Имя БД (None - из ``set_db_name``, иначе первая
            доступная)
        :return: ObjectId макроса либо ``None`` при точном отсутствии
        """
        db_name = self.__resolve_db_name(db_name)
        rows = self.__request_macros_page(
            db_name, filter_name, 0, self.settings.kb_objects_batch_size
        )

        for macro in rows:
            if macro.get("Name") == filter_name:
                self.log.info(
                    f"status=success, action=get_macros_id_by_filter_name, "
                    f'msg="Found macro {filter_name!r}", '
                    f"hostname={self.__kb_hostname!r}, db={db_name!r}"
                )
                return str(macro.get("ObjectId"))

        self.log.warning(
            f"status=failed, action=get_macros_id_by_filter_name, "
            f'msg="Macro {filter_name!r} not found", '
            f"hostname={self.__kb_hostname!r}, db={db_name!r}"
        )
        return None

    @staticmethod
    def __normalize_text(text: str) -> str:
        """Тело макроса в одну строку (для разбора ссылок вида
        ``filter::Name()``)."""
        return text.replace("\n", "").replace("'", "").replace("\t", " ").strip()

    def unpack_macros(self, object_id: str, db_name: str | None = None) -> list[str]:
        """Раскрытие макроса: список фильтров (``filter::Name()``), на которые
        он ссылается.

        :param object_id: ObjectId раскрываемого макроса
            (например ``LOC-RF-34`` - макрос локали РФ)
        :param db_name: Имя БД (None - из ``set_db_name``, иначе первая
            доступная)
        :return: Имена фильтров без дублей, в порядке встречи
        :raises ValueError: если макрос с таким ObjectId не найден
        """
        macro = self.get_macros_by_object_id(object_id, db_name)
        if macro is None:
            raise ValueError(f"Macro with object_id {object_id!r} not found")

        info = self.get_macros_info(macro["id"], db_name)
        text = self.__normalize_text(str(info.get("text") or ""))
        return list(dict.fromkeys(re.findall(r"filter::(\S+)\(\)", text)))

    # ------------------------------------------------------------------ #
    # Контрактный Content API макросов (CoreApi.SiemMacros*.yaml)
    # ------------------------------------------------------------------ #

    def __content_api_request(
        self,
        db_name: str,
        api_path: str,
        action: str,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        body: Any = None,
    ) -> requests.Response:
        """Запрос к контрактному Content API макросов на KB-стороне.

        Контракт требует обязательный query-параметр ``contentDatabase``,
        поэтому он передаётся и как query, и как заголовок
        ``Content-Database``.

        :param db_name: Имя БД
        :param api_path: Путь эндпоинта
        :param action: Имя действия для логирования
        :param method: HTTP-метод запроса
        :param params: Дополнительные query-параметры
        :param body: Тело запроса (для POST/PUT)
        :return: Объект Response
        """
        query: dict[str, Any] = {"contentDatabase": db_name}
        if params:
            query.update(params)

        url = f"https://{self.__kb_hostname}:{self.__kb_port}{api_path}"
        kwargs: dict[str, Any] = {"params": query}
        if body is not None:
            kwargs["json"] = body

        response = exec_request(
            self.__kb_session,
            url,
            method=method,
            timeout=self.settings.connection_timeout,
            headers=self.__kb_headers(db_name),
            **kwargs,
        )

        self.log.info(
            f"status=success, action={action}, method={method}, "
            f"code={response.status_code}, "
            f"hostname={self.__kb_hostname!r}, db={db_name!r}"
        )
        return response

    @staticmethod
    def __normalize_macro(macro: dict[str, Any]) -> dict[str, Any]:
        """Привести SiemMacrosCoreDto к snake_case стилю модуля."""
        return {
            "id": macro.get("Id"),
            "system_name": macro.get("SystemName"),
            "text": macro.get("Text"),
            "locales": macro.get("Locales") or [],
            "tag_ids": macro.get("TagIds") or [],
            "set_as_event_name": macro.get("SetAsEventName"),
            "params": macro.get("Params") or [],
            "origin_id": macro.get("OriginId"),
            "parent_uid": macro.get("ParentUid"),
        }

    @staticmethod
    def __normalize_tag(tag: dict[str, Any]) -> dict[str, Any]:
        """Привести SiemMacrosTagCoreDto к snake_case стилю модуля."""
        return {
            "id": tag.get("Id"),
            "parent_id": tag.get("ParentId"),
            "is_group": tag.get("IsGroup"),
            "origin_id": tag.get("OriginId"),
            "locales": tag.get("Locales") or [],
        }

    def get_macro(
        self, macro_id: str, db_name: str | None = None
    ) -> dict[str, Any] | None:
        """Полное описание макроса (контракт GetSiemMacros).

        На 26.0 эндпоинт мягкого удаления: после DELETE возвращает пустое
        тело, поэтому отсутствующий макрос отдаётся как ``None``.

        :param macro_id: ID макроса
        :param db_name: Имя БД (None - из ``set_db_name``, иначе первая
            доступная)
        :return: Описание макроса либо ``None``, если макрос отсутствует
        """
        db_name = self.__resolve_db_name(db_name)
        api_path = f"{self.__api_content_siem_macros}/{macro_id}"
        response = self.__content_api_request(db_name, api_path, "get_macro")

        if not response.content:
            self.log.warning(
                f'status=failed, action=get_macro, msg="Macro {macro_id!r} not found", '
                f"hostname={self.__kb_hostname!r}, db={db_name!r}"
            )
            return None

        return self.__normalize_macro(response.json())

    def create_macro(
        self,
        system_name: str,
        text: str,
        locales: list[dict[str, str]] | None = None,
        set_as_event_name: bool = False,
        db_name: str | None = None,
    ) -> str:
        """Создать макрос (контракт CreateSiemMacros).

        :param system_name: Системное имя макроса
        :param text: Текст (фильтр/правило) макроса
        :param locales: Локали вида
            ``[{"Locale": "RUS", "Name": ..., "Description": ...}]``
        :param set_as_event_name: Помечать ли как имя события
        :param db_name: Имя БД (None - из ``set_db_name``, иначе первая
            доступная)
        :return: ID созданного макроса
        """
        db_name = self.__resolve_db_name(db_name)
        body: dict[str, Any] = {
            "SystemName": system_name,
            "Text": text,
            "SetAsEventName": set_as_event_name,
            "Locales": locales or [],
        }
        response = self.__content_api_request(
            db_name,
            self.__api_content_siem_macros,
            "create_macro",
            method="POST",
            body=body,
        )
        macro_id = str(response.json())
        self.__macros.clear()
        self.log.info(
            f'status=success, action=create_macro, msg="Created macro {system_name!r}", '
            f"macro_id={macro_id!r}, hostname={self.__kb_hostname!r}, db={db_name!r}"
        )
        return macro_id

    def update_macro(
        self,
        macro_id: str,
        system_name: str,
        text: str,
        set_as_event_name: bool = False,
        db_name: str | None = None,
    ) -> None:
        """Изменить макрос (контракт UpdateSiemMacros).

        Тело частичное: локали и параметры макроса сохраняются, меняются
        только ``SystemName``/``Text``/``SetAsEventName`` (измерено на стенде).

        :param macro_id: ID макроса
        :param system_name: Новое системное имя
        :param text: Новый текст макроса
        :param set_as_event_name: Помечать ли как имя события
        :param db_name: Имя БД (None - из ``set_db_name``, иначе первая
            доступная)
        """
        db_name = self.__resolve_db_name(db_name)
        api_path = f"{self.__api_content_siem_macros}/{macro_id}"
        body: dict[str, Any] = {
            "SystemName": system_name,
            "Text": text,
            "SetAsEventName": set_as_event_name,
        }
        self.__content_api_request(
            db_name, api_path, "update_macro", method="PUT", body=body
        )
        self.__macros.clear()
        self.log.info(
            f'status=success, action=update_macro, msg="Updated macro {system_name!r}", '
            f"macro_id={macro_id!r}, hostname={self.__kb_hostname!r}, db={db_name!r}"
        )

    def delete_macro(
        self, macro_id: str, db_name: str | None = None
    ) -> requests.Response:
        """Удалить макрос (контракт RemoveSiemMacros).

        :param macro_id: ID удаляемого макроса
        :param db_name: Имя БД (None - из ``set_db_name``, иначе первая
            доступная)
        :return: Объект Response
        """
        db_name = self.__resolve_db_name(db_name)
        api_path = f"{self.__api_content_siem_macros}/{macro_id}"
        response = self.__content_api_request(
            db_name, api_path, "delete_macro", method="DELETE"
        )
        self.__macros.clear()
        self.log.info(
            f'status=success, action=delete_macro, msg="Deleted macro", '
            f"macro_id={macro_id!r}, hostname={self.__kb_hostname!r}, db={db_name!r}"
        )
        return response

    def add_macro_param(
        self,
        macro_id: str,
        name: str,
        param_type: str,
        default_value: str = "",
        index: int = 0,
        locales: list[dict[str, str]] | None = None,
        db_name: str | None = None,
    ) -> str:
        """Добавить параметр макроса (контракт AddParamToSiemMacros).

        Сервер 26.0 отвечает 500 на неполное тело, поэтому все поля DTO
        (``DefaultValue``/``Index``/``Locales``) заполняются значениями по
        умолчанию.

        :param macro_id: ID макроса
        :param name: Имя параметра
        :param param_type: Тип параметра: ``string`` | ``number`` | ``bool``
        :param default_value: Значение по умолчанию
        :param index: Порядок параметра
        :param locales: Локали параметра ``[{"Locale": ..., "Description": ...}]``
        :param db_name: Имя БД (None - из ``set_db_name``, иначе первая
            доступная)
        :return: ID созданного параметра
        """
        db_name = self.__resolve_db_name(db_name)
        api_path = f"{self.__api_content_siem_macros}/{macro_id}/params"
        body: dict[str, Any] = {
            "Name": name,
            "Type": param_type,
            "DefaultValue": default_value,
            "Index": index,
            "Locales": locales or [],
        }
        response = self.__content_api_request(
            db_name, api_path, "add_macro_param", method="POST", body=body
        )
        param_id = str(response.json())
        self.log.info(
            f'status=success, action=add_macro_param, msg="Added param {name!r}", '
            f"macro_id={macro_id!r}, param_id={param_id!r}, "
            f"hostname={self.__kb_hostname!r}, db={db_name!r}"
        )
        return param_id

    def remove_macro_param(
        self, macro_id: str, param_id: str, db_name: str | None = None
    ) -> requests.Response:
        """Удалить параметр макроса (контракт RemoveParamFromSiemMacros).

        :param macro_id: ID макроса
        :param param_id: ID параметра
        :param db_name: Имя БД (None - из ``set_db_name``, иначе первая
            доступная)
        :return: Объект Response
        """
        db_name = self.__resolve_db_name(db_name)
        api_path = f"{self.__api_content_siem_macros}/{macro_id}/params/{param_id}"
        return self.__content_api_request(
            db_name, api_path, "remove_macro_param", method="DELETE"
        )

    def add_macro_locale(
        self,
        macro_id: str,
        locale: str,
        name: str,
        description: str,
        db_name: str | None = None,
    ) -> str:
        """Добавить локаль макросу (контракт AddLocaleToSiemMacros).

        Сервер 26.0 требует непустой ``Description`` (без него - ``400``),
        поэтому параметр обязателен.

        :param macro_id: ID макроса
        :param locale: Код локали (``RUS``/``ENG`` ...)
        :param name: Имя в этой локали
        :param description: Описание в этой локали (не пустое)
        :param db_name: Имя БД (None - из ``set_db_name``, иначе первая
            доступная)
        :return: ID созданной локали
        :raises ValueError: если ``description`` пустой
        """
        if not description.strip():
            raise ValueError("Локаль макроса требует непустой description")

        db_name = self.__resolve_db_name(db_name)
        api_path = f"{self.__api_content_siem_macros}/{macro_id}/locales"
        body: dict[str, Any] = {
            "Locale": locale,
            "Name": name,
            "Description": description,
        }
        response = self.__content_api_request(
            db_name, api_path, "add_macro_locale", method="POST", body=body
        )
        return str(response.json())

    def change_macro_locale(
        self,
        macro_id: str,
        locale: str,
        name: str,
        description: str,
        db_name: str | None = None,
    ) -> None:
        """Изменить локаль макроса (контракт ChangeSiemMacrosLocale).

        Сервер 26.0 требует непустой ``Description`` (без него - ``400``).

        :param macro_id: ID макроса
        :param locale: Код изменяемой локали
        :param name: Новое имя в этой локали
        :param description: Новое описание в этой локали (не пустое)
        :param db_name: Имя БД (None - из ``set_db_name``, иначе первая
            доступная)
        :raises ValueError: если ``description`` пустой
        """
        if not description.strip():
            raise ValueError("Локаль макроса требует непустой description")

        db_name = self.__resolve_db_name(db_name)
        api_path = f"{self.__api_content_siem_macros}/{macro_id}/locales"
        body: dict[str, Any] = {
            "Locale": locale,
            "Name": name,
            "Description": description,
        }
        self.__content_api_request(
            db_name, api_path, "change_macro_locale", method="PUT", body=body
        )

    def remove_macro_locale(
        self, macro_id: str, locale: str, db_name: str | None = None
    ) -> requests.Response:
        """Удалить локаль макроса (контракт RemoveLocaleFromSiemMacros).

        Сервер 26.0 работает с дефектом: отвечает ``204`` и в пути ожидает код
        локали (``ENG``; по uuid - ``404``), но локаль из объекта не исчезает -
        обнуляется только её ``Name`` (измерено на стенде). Для меток
        (``remove_macro_tag_locale``) удаление работает корректно.

        :param macro_id: ID макроса
        :param locale: Код локали для удаления
        :param db_name: Имя БД (None - из ``set_db_name``, иначе первая
            доступная)
        :return: Объект Response
        """
        db_name = self.__resolve_db_name(db_name)
        api_path = f"{self.__api_content_siem_macros}/{macro_id}/locales/{locale}"
        return self.__content_api_request(
            db_name, api_path, "remove_macro_locale", method="DELETE"
        )

    def add_macro_tag(
        self, macro_id: str, tag_id: str, db_name: str | None = None
    ) -> str:
        """Привязать метку к макросу (контракт AddTagToSiemMacros).

        :param macro_id: ID макроса
        :param tag_id: ID метки (из ``get_macros_tags_list``)
        :param db_name: Имя БД (None - из ``set_db_name``, иначе первая
            доступная)
        :return: ID связи макрос-метка
        """
        db_name = self.__resolve_db_name(db_name)
        api_path = f"{self.__api_content_siem_macros}/{macro_id}/tags/{tag_id}"
        response = self.__content_api_request(
            db_name, api_path, "add_macro_tag", method="POST"
        )
        return str(response.json())

    def remove_macro_tag(
        self, macro_id: str, tag_id: str, db_name: str | None = None
    ) -> requests.Response:
        """Отвязать метку от макроса (контракт RemoveTagFromSiemMacros).

        :param macro_id: ID макроса
        :param tag_id: ID метки
        :param db_name: Имя БД (None - из ``set_db_name``, иначе первая
            доступная)
        :return: Объект Response
        """
        db_name = self.__resolve_db_name(db_name)
        api_path = f"{self.__api_content_siem_macros}/{macro_id}/tags/{tag_id}"
        return self.__content_api_request(
            db_name, api_path, "remove_macro_tag", method="DELETE"
        )

    def get_macros_tags_list(self, db_name: str | None = None) -> list[dict[str, Any]]:
        """Список меток макросов (контракт GetSiemMacrosTagList).

        :param db_name: Имя БД (None - из ``set_db_name``, иначе первая
            доступная)
        :return: [{"id": ..., "parent_id": ..., "is_group": ..., "origin_id":
            ..., "locales": [...]}]
        """
        db_name = self.__resolve_db_name(db_name)
        response = self.__content_api_request(
            db_name, self.__api_content_siem_macros_tags, "get_macros_tags_list"
        )
        tags = [self.__normalize_tag(t) for t in response.json()]
        self.log.info(
            f'status=success, action=get_macros_tags_list, msg="Got {len(tags)} tags", '
            f"hostname={self.__kb_hostname!r}, db={db_name!r}"
        )
        return tags

    def get_macro_tag(
        self, tag_id: str, db_name: str | None = None
    ) -> dict[str, Any] | None:
        """Метка макросов по ID (контракт GetSiemMacrosTag).

        :param tag_id: ID метки
        :param db_name: Имя БД (None - из ``set_db_name``, иначе первая
            доступная)
        :return: Метка либо ``None``, если она отсутствует
        """
        db_name = self.__resolve_db_name(db_name)
        api_path = f"{self.__api_content_siem_macros_tags}/{tag_id}"
        response = self.__content_api_request(db_name, api_path, "get_macro_tag")

        if not response.content:
            self.log.warning(
                f'status=failed, action=get_macro_tag, msg="Tag {tag_id!r} not found", '
                f"hostname={self.__kb_hostname!r}, db={db_name!r}"
            )
            return None

        return self.__normalize_tag(response.json())

    def create_macro_tag(
        self,
        name: str,
        locales: list[dict[str, str]] | None = None,
        is_group: bool = False,
        parent_id: str | None = None,
        db_name: str | None = None,
    ) -> str:
        """Создать метку макросов (контракт CreateSiemMacrosTag).

        :param name: Имя метки (если ``locales`` не заданы - создаётся локаль
            ``RUS`` с этим именем)
        :param locales: Локали ``[{"Locale": ..., "Name": ...}]``
        :param is_group: Является ли метка группой
        :param parent_id: ID родительской метки-группы
        :param db_name: Имя БД (None - из ``set_db_name``, иначе первая
            доступная)
        :return: ID созданной метки
        """
        db_name = self.__resolve_db_name(db_name)
        body: dict[str, Any] = {
            "Locales": locales or [{"Locale": "RUS", "Name": name}],
            "IsGroup": is_group,
            "ParentId": parent_id,
        }
        response = self.__content_api_request(
            db_name,
            self.__api_content_siem_macros_tags,
            "create_macro_tag",
            method="POST",
            body=body,
        )
        tag_id = str(response.json())
        self.log.info(
            f'status=success, action=create_macro_tag, msg="Created tag {name!r}", '
            f"tag_id={tag_id!r}, hostname={self.__kb_hostname!r}, db={db_name!r}"
        )
        return tag_id

    def add_macro_tag_locale(
        self, tag_id: str, locale: str, name: str, db_name: str | None = None
    ) -> str:
        """Добавить локаль метке (контракт AddLocaleToSiemMacrosTag).

        :param tag_id: ID метки
        :param locale: Код локали
        :param name: Имя в этой локали
        :param db_name: Имя БД (None - из ``set_db_name``, иначе первая
            доступная)
        :return: ID созданной локали
        """
        db_name = self.__resolve_db_name(db_name)
        api_path = f"{self.__api_content_siem_macros_tags}/{tag_id}/locales"
        body: dict[str, Any] = {"Locale": locale, "Name": name}
        response = self.__content_api_request(
            db_name, api_path, "add_macro_tag_locale", method="POST", body=body
        )
        return str(response.json())

    def change_macro_tag_locale(
        self, tag_id: str, locale: str, name: str, db_name: str | None = None
    ) -> None:
        """Изменить локаль метки (контракт ChangeSiemMacrosTagLocale).

        :param tag_id: ID метки
        :param locale: Код изменяемой локали
        :param name: Новое имя в этой локали
        :param db_name: Имя БД (None - из ``set_db_name``, иначе первая
            доступная)
        """
        db_name = self.__resolve_db_name(db_name)
        api_path = f"{self.__api_content_siem_macros_tags}/{tag_id}/locales"
        body: dict[str, Any] = {"Locale": locale, "Name": name}
        self.__content_api_request(
            db_name, api_path, "change_macro_tag_locale", method="PUT", body=body
        )

    def remove_macro_tag_locale(
        self, tag_id: str, locale: str, db_name: str | None = None
    ) -> requests.Response:
        """Удалить локаль метки (контракт RemoveLocaleFromSiemMacrosTag).

        :param tag_id: ID метки
        :param locale: Код локали для удаления
        :param db_name: Имя БД (None - из ``set_db_name``, иначе первая
            доступная)
        :return: Объект Response
        """
        db_name = self.__resolve_db_name(db_name)
        api_path = f"{self.__api_content_siem_macros_tags}/{tag_id}/locales/{locale}"
        return self.__content_api_request(
            db_name, api_path, "remove_macro_tag_locale", method="DELETE"
        )

    def delete_macro_tag(
        self, tag_id: str, db_name: str | None = None
    ) -> requests.Response:
        """Удалить метку макросов (контракт RemoveSiemMacrosTag).

        Эндпоинт снимает и все привязки метки к макросам (измерено на стенде).

        :param tag_id: ID удаляемой метки
        :param db_name: Имя БД (None - из ``set_db_name``, иначе первая
            доступная)
        :return: Объект Response
        """
        db_name = self.__resolve_db_name(db_name)
        api_path = f"{self.__api_content_siem_macros_tags}/{tag_id}"
        return self.__content_api_request(
            db_name, api_path, "delete_macro_tag", method="DELETE"
        )

    def close(self) -> None:
        if self.__kb_session is not None:
            self.__kb_session.close()
