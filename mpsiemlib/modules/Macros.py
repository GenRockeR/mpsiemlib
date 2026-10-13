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

    Начиная с MP SIEM 26.0 макросы - контент KB, а контрактный Content API
    ``CoreApi.SiemMacros.yaml`` (``/api/SiemMacros``) и
    ``CoreApi.SiemMacrosTag.yaml`` (``/api/SiemMacrosTag``) на стендах 28.x
    не маршрутизируется (``404`` на 443 и 8091, измерено). Рабочий интерфейс -
    KB Studio API (``/api-studio/siem/macros``, порт 8091) с обязательным
    заголовком ``Content-Database``: его использует веб-редактор KB Studio
    (legacy-приложение ``/o/``).

    Все изменяющие операции (``POST``/``PUT``) защищены оптимистической
    блокировкой по хэшу скомпилированного тела: сервер отвечает ``409``
    ``SiemMacros.HashMismatch``. Хэш выдаёт ``POST /siem/macros/validate``,
    поэтому перед изменением объект перечитывается через ``.../edit``, а его
    тело прогоняется через ``validate`` (``sdkVersion``/``hash``/
    ``compilationStatus`` добавляются в тело запроса).

    Методы ``add_macro_param``/``remove_macro_param``, ``add_macro_locale``,
    ``change_macro_locale``, ``remove_macro_locale``, ``add_macro_tag`` и
    ``remove_macro_tag`` исторически соответствовали эндпоинтам контрактного
    Content API. В KB Studio API отдельных эндпоинтов для них нет -
    параметры/локали/метки являются частью тела макроса, поэтому они
    реализованы как чтение-модификация-запись (``.../edit`` + ``validate`` +
    ``PUT``). Локалей у метки ровно одна (имя - словарь ``{locale: text}``),
    поэтому отдельные операции с локалью метки контрактного API свёрнуты в
    ``rename_macro_tag``.
    """

    __kb_port = 8091
    __api_kb_db_list = "/api-studio/content-database-selector/content-databases"
    __api_macros = "/api-studio/siem/macros"
    __api_macros_tags = "/api-studio/siem/macros/tags"

    #: compilationStatus из SiemMacroDataService.getValidationResultDto:
    #: 1 - ошибок нет, 2 - есть ошибки, 3 - результат валидации отсутствует
    __compilation_ok = 1
    __compilation_failed = 2
    __compilation_not_validated = 3
    __default_locale = "RUS"

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

    def __macros_url(self, suffix: str = "") -> str:
        return (
            f"https://{self.__kb_hostname}:{self.__kb_port}{self.__api_macros}{suffix}"
        )

    def __tags_url(self, suffix: str = "") -> str:
        return (
            f"https://{self.__kb_hostname}:{self.__kb_port}"
            f"{self.__api_macros_tags}{suffix}"
        )

    def __request(
        self,
        db_name: str,
        url: str,
        action: str,
        method: str = "GET",
        body: Any = None,
    ) -> requests.Response:
        """Запрос к KB Studio API макросов (порт 8091).

        :param db_name: Имя БД
        :param url: Полный URL эндпоинта
        :param action: Имя действия для логирования
        :param method: HTTP-метод запроса
        :param body: Тело запроса (для POST/PUT)
        :return: Объект Response
        """
        kwargs: dict[str, Any] = {}
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

    # ------------------------------------------------------------------ #
    # KB Studio API: список и чтение
    # ------------------------------------------------------------------ #

    def __request_macros_page(
        self, db_name: str, search: str, skip: int, take: int
    ) -> list[dict[str, Any]]:
        """Страница макросов из ``POST /api-studio/siem/macros/list``."""
        body: dict[str, Any] = {
            "tagId": None,
            "sort": [{"name": "objectId", "order": 0, "type": 0}],
            "filters": None,
            "search": search,
            "skip": skip,
            "take": take,
        }

        response: dict[str, Any] = self.__request(
            db_name,
            self.__macros_url("/list"),
            "request_macros_page",
            method="POST",
            body=body,
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

    @staticmethod
    def __normalize_macro(macro: dict[str, Any]) -> dict[str, Any]:
        """Привести объект макроса KB Studio к snake_case стилю модуля."""
        return {
            "id": macro.get("Id"),
            "system_name": macro.get("SystemName"),
            "name": macro.get("Name"),
            "description": macro.get("Description"),
            "text": macro.get("Text"),
            "set_as_event_name": macro.get("SetAsEventName"),
            "params": macro.get("Args") or [],
            "tags": macro.get("Tags") or [],
            "tag_ids": [tag.get("Id") for tag in macro.get("Tags") or []],
            "origin": macro.get("Origin") or {},
            "copy_of": macro.get("CopyOf"),
        }

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
        response: dict[str, Any] = self.__request(
            db_name, self.__macros_url(f"/{macro_id}"), "get_macros_info"
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

    def get_macro(
        self, macro_id: str, db_name: str | None = None
    ) -> dict[str, Any] | None:
        """Полное описание макроса (``GET /siem/macros/{id}``).

        Удаление в KB Studio физическое: отсутствующий макрос отдаётся ``404``,
        он возвращается как ``None``.

        :param macro_id: ID макроса
        :param db_name: Имя БД (None - из ``set_db_name``, иначе первая
            доступная)
        :return: Описание макроса либо ``None``, если макрос отсутствует
        """
        db_name = self.__resolve_db_name(db_name)
        try:
            response = self.__request(
                db_name, self.__macros_url(f"/{macro_id}"), "get_macro"
            )
        except requests.HTTPError as err:
            code = err.response.status_code if err.response is not None else None
            if code == 404:
                self.log.warning(
                    f'status=failed, action=get_macro, msg="Macro {macro_id!r} not '
                    f'found", hostname={self.__kb_hostname!r}, db={db_name!r}'
                )
                return None
            raise

        return self.__normalize_macro(response.json())

    def get_macro_edit_model(
        self, macro_id: str, db_name: str | None = None
    ) -> dict[str, Any]:
        """Модель редактирования макроса (``GET /siem/macros/{id}/edit``).

        Отличие от ``get_macro``: локализованные поля (``name``,
        ``description``, описания параметров) возвращаются словарями
        ``{locale: text}`` как их отдаёт сервер, а не приведёнными к локали
        заголовка.

        :param macro_id: ID макроса
        :param db_name: Имя БД (None - из ``set_db_name``, иначе первая
            доступная)
        :return: {"Id", "Text", "Name", "SystemName", "Description",
            "SetAsEventName", "TagIds", "Args"}
        """
        db_name = self.__resolve_db_name(db_name)
        return self.__get_edit_model(macro_id, db_name, "get_macro_edit_model")

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

    def get_macro_by_system_name(
        self, system_name: str, db_name: str | None = None
    ) -> dict[str, Any] | None:
        """Полное описание макроса по системному имени
        (``GET /siem/macros/byName/{systemName}``).

        :param system_name: Системное имя макроса
        :param db_name: Имя БД (None - из ``set_db_name``, иначе первая
            доступная)
        :return: Описание макроса либо ``None``, если макрос отсутствует
        """
        db_name = self.__resolve_db_name(db_name)
        try:
            response = self.__request(
                db_name,
                self.__macros_url(f"/byName/{system_name}"),
                "get_macro_by_system_name",
            )
        except requests.HTTPError as err:
            code = err.response.status_code if err.response is not None else None
            if code == 404:
                self.log.warning(
                    f"status=failed, action=get_macro_by_system_name, "
                    f'msg="Macro {system_name!r} not found", '
                    f"hostname={self.__kb_hostname!r}, db={db_name!r}"
                )
                return None
            raise

        return self.__normalize_macro(response.json())

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
    # KB Studio API: валидация и изменение
    # ------------------------------------------------------------------ #

    def validate_macro_text(
        self, text: str, db_name: str | None = None
    ) -> dict[str, Any]:
        """Проверить тело макроса (``POST /siem/macros/validate``).

        Сервер компилирует ``text`` и возвращает хэш результата
        (``ResultHash``) и версию SDK - без них изменяющие запросы отклоняются
        с ``409 SiemMacros.HashMismatch``.

        :param text: Текст (фильтр/правило) макроса
        :param db_name: Имя БД (None - из ``set_db_name``, иначе первая
            доступная)
        :return: {"errors": [...], "warnings": [...], "sdk_version": ...,
            "result_hash": ..., "system_name": ..., "args": [...]}
        """
        db_name = self.__resolve_db_name(db_name)
        response: dict[str, Any] = self.__request(
            db_name,
            self.__macros_url("/validate"),
            "validate_macro_text",
            method="POST",
            body={"text": text},
        ).json()

        return {
            "errors": response.get("Errors") or [],
            "warnings": response.get("Warnings") or [],
            "sdk_version": response.get("SdkVersion"),
            "result_hash": response.get("ResultHash"),
            "system_name": response.get("SystemName"),
            "args": response.get("Args") or [],
        }

    def check_macro_system_name(
        self, system_name: str, db_name: str | None = None
    ) -> str | None:
        """Занято ли системное имя (``POST /siem/macros/exists``).

        :param system_name: Системное имя макроса
        :param db_name: Имя БД (None - из ``set_db_name``, иначе первая
            доступная)
        :return: ID занявшего имя макроса либо ``None``, если имя свободно
        """
        db_name = self.__resolve_db_name(db_name)
        response: dict[str, Any] = self.__request(
            db_name,
            self.__macros_url("/exists"),
            "check_macro_system_name",
            method="POST",
            body={"systemName": system_name},
        ).json()
        exist_id = response.get("ExistId")
        return str(exist_id) if exist_id else None

    def __get_edit_model(
        self, macro_id: str, db_name: str, action: str
    ) -> dict[str, Any]:
        """Модель редактирования макроса (``GET /siem/macros/{id}/edit``)."""
        edit_model: dict[str, Any] = self.__request(
            db_name, self.__macros_url(f"/{macro_id}/edit"), action
        ).json()
        return edit_model

    @staticmethod
    def __as_dict(value: Any) -> dict[str, Any]:
        """Локализованное поле DTO как словарь (пустой - если не словарь)."""
        return dict(value) if isinstance(value, dict) else {}

    def __validation_fields(self, validation: dict[str, Any]) -> dict[str, Any]:
        """Поля результата валидации в DTO сохраняющего запроса (схема
        ``SiemMacroDataService.getValidationResultDto``)."""
        if validation.get("errors"):
            compilation_status = self.__compilation_failed
        elif validation.get("result_hash") is None:
            compilation_status = self.__compilation_not_validated
        else:
            compilation_status = self.__compilation_ok

        return {
            "sdkVersion": validation.get("sdk_version"),
            "hash": validation.get("result_hash"),
            "compilationStatus": compilation_status,
        }

    def __build_macro_body(
        self,
        text: str,
        system_name: str,
        name: Any,
        description: Any,
        set_as_event_name: bool,
        tag_ids: list[str],
        args: list[dict[str, Any]],
        validation: dict[str, Any],
    ) -> dict[str, Any]:
        """Тело сохраняющего запроса: модель макроса + поля валидации."""
        body: dict[str, Any] = {
            "name": name,
            "description": description,
            "text": text,
            "systemName": system_name,
            "setAsEventName": set_as_event_name,
            "tagIds": tag_ids,
            "args": self.__renumber_args(args),
        }
        body.update(self.__validation_fields(validation))
        return body

    @staticmethod
    def __localize(value: Any, text: str, locale: str) -> dict[str, str]:
        """Локализованное поле: словарь ``{locale: text}``. Существующие
        локали сохраняются, если передан словарь."""
        if isinstance(value, dict):
            merged = dict(value)
            merged[locale] = text
            return {k: str(v) for k, v in merged.items() if v is not None}
        return {locale: text}

    @staticmethod
    def __default_locale_name(edit_model: dict[str, Any], fallback: str) -> str:
        """Имя из модели редактирования в локали по умолчанию (или в первой
        доступной)."""
        name = edit_model.get("Name")
        if isinstance(name, dict):
            if name.get(Macros.__default_locale):
                return str(name[Macros.__default_locale])
            for value in name.values():
                if value:
                    return str(value)
        elif name:
            return str(name)
        return fallback

    def create_macro(
        self,
        system_name: str,
        text: str,
        name: str | None = None,
        description: str = "",
        set_as_event_name: bool = False,
        tag_ids: list[str] | None = None,
        db_name: str | None = None,
    ) -> str:
        """Создать макрос (``POST /api-studio/siem/macros``).

        Текст предварительно прогоняется через ``validate``: без
        ``hash``/``sdkVersion`` сервер отвечает ``409 SiemMacros.HashMismatch``.

        :param system_name: Системное имя макроса
        :param text: Текст (фильтр/правило) макроса
        :param name: Отображаемое имя (по умолчанию - ``system_name``)
        :param description: Описание
        :param set_as_event_name: Помечать ли как имя события
        :param tag_ids: ID меток, привязываемых при создании
        :param db_name: Имя БД (None - из ``set_db_name``, иначе первая
            доступная)
        :return: ID созданного макроса
        :raises ValueError: если текст не скомпилировался
        """
        db_name = self.__resolve_db_name(db_name)
        validation = self.validate_macro_text(text, db_name)
        if validation["errors"]:
            raise ValueError(
                f"Текст макроса не скомпилировался: {validation['errors']}"
            )

        display_name = name if name else system_name
        # Параметры макроса существуют только в тексте: без args из результата
        # валидации объект сохранился бы без них (измерено на стенде).
        body = self.__build_macro_body(
            text=text,
            system_name=system_name,
            name={self.__default_locale: display_name},
            description={self.__default_locale: description},
            set_as_event_name=set_as_event_name,
            tag_ids=list(tag_ids or []),
            args=list(validation["args"]),
            validation=validation,
        )

        response: dict[str, Any] = self.__request(
            db_name,
            self.__macros_url(),
            "create_macro",
            method="POST",
            body=body,
        ).json()
        macro_id = str(response["Id"])

        self.__macros.clear()
        self.log.info(
            f'status=success, action=create_macro, msg="Created macro '
            f'{system_name!r}", macro_id={macro_id!r}, '
            f"hostname={self.__kb_hostname!r}, db={db_name!r}"
        )
        return macro_id

    def update_macro(
        self,
        macro_id: str,
        text: str,
        system_name: str | None = None,
        name: str | None = None,
        description: str | None = None,
        set_as_event_name: bool | None = None,
        tag_ids: list[str] | None = None,
        args: list[dict[str, Any]] | None = None,
        db_name: str | None = None,
    ) -> None:
        """Изменить макрос (``PUT /api-studio/siem/macros/{id}``).

        KB Studio API не принимает частичное тело: модель читается через
        ``.../edit``, заменяются только переданные аргументы. Новый ``text``
        прогоняется через ``validate`` - прислать старый хэш нельзя.

        :param macro_id: ID макроса
        :param text: Новый текст макроса
        :param system_name: Новое системное имя (None - оставить). Меняется
            только поле ``SystemName``: имя в строке ``filter ...()`` текста не
            переписывается, для согласованного переименования используйте
            ``rename_macro``
        :param name: Новое отображаемое имя в локали ``RUS`` (None - оставить)
        :param description: Новое описание в локали ``RUS`` (None - оставить)
        :param set_as_event_name: Помечать ли как имя события (None -
            оставить)
        :param tag_ids: Новые ID меток (None - оставить привязанные)
        :param args: Новые параметры макроса (None - оставить существующие)
        :param db_name: Имя БД (None - из ``set_db_name``, иначе первая
            доступная)
        :raises ValueError: если текст не скомпилировался
        """
        db_name = self.__resolve_db_name(db_name)
        edit = self.__get_edit_model(macro_id, db_name, "update_macro")
        validation = self.validate_macro_text(text, db_name)
        if validation["errors"]:
            raise ValueError(
                f"Текст макроса не скомпилировался: {validation['errors']}"
            )

        new_name = edit.get("Name")
        if name is not None:
            new_name = self.__localize(new_name, name, self.__default_locale)
        new_description = edit.get("Description")
        if description is not None:
            new_description = self.__localize(
                new_description, description, self.__default_locale
            )

        body = self.__build_macro_body(
            text=text,
            system_name=system_name or str(edit.get("SystemName")),
            name=new_name,
            description=new_description,
            set_as_event_name=(
                bool(edit.get("SetAsEventName"))
                if set_as_event_name is None
                else set_as_event_name
            ),
            tag_ids=(
                list(tag_ids) if tag_ids is not None else list(edit.get("TagIds") or [])
            ),
            args=list(args) if args is not None else list(edit.get("Args") or []),
            validation=validation,
        )

        self.__request(
            db_name,
            self.__macros_url(f"/{macro_id}"),
            "update_macro",
            method="PUT",
            body=body,
        )

        self.__macros.clear()
        self.log.info(
            f'status=success, action=update_macro, msg="Updated macro", '
            f"macro_id={macro_id!r}, hostname={self.__kb_hostname!r}, db={db_name!r}"
        )

    def delete_macro(
        self, macro_id: str, db_name: str | None = None
    ) -> requests.Response:
        """Удалить макрос (``DELETE /api-studio/siem/macros/{id}``).

        Удаление физическое: повторный ``DELETE``/``GET`` по этому ID - ``404``.

        :param macro_id: ID удаляемого макроса
        :param db_name: Имя БД (None - из ``set_db_name``, иначе первая
            доступная)
        :return: Объект Response
        """
        db_name = self.__resolve_db_name(db_name)
        response = self.__request(
            db_name,
            self.__macros_url(f"/{macro_id}"),
            "delete_macro",
            method="DELETE",
        )
        self.__macros.clear()
        self.log.info(
            f'status=success, action=delete_macro, msg="Deleted macro", '
            f"macro_id={macro_id!r}, hostname={self.__kb_hostname!r}, db={db_name!r}"
        )
        return response

    def clone_macro(
        self, macro_id: str, system_name: str, db_name: str | None = None
    ) -> str:
        """Клонировать макрос (``PUT /api-studio/siem/macros/{id}/clone``).

        Текст остаётся прежним, поэтому хэш берётся из валидации текущего тела.

        :param macro_id: ID исходного макроса
        :param system_name: Системное имя клона
        :param db_name: Имя БД (None - из ``set_db_name``, иначе первая
            доступная)
        :return: ID созданного клона
        :raises ValueError: если текст исходного макроса не компилируется
        """
        db_name = self.__resolve_db_name(db_name)
        edit = self.__get_edit_model(macro_id, db_name, "clone_macro")
        text = str(edit.get("Text") or "")
        validation = self.validate_macro_text(text, db_name)
        if validation["errors"]:
            raise ValueError(
                f"Текст макроса не скомпилировался: {validation['errors']}"
            )

        body = self.__build_macro_body(
            text=text,
            system_name=system_name,
            name=self.__localize(
                edit.get("Name"),
                self.__default_locale_name(edit, system_name),
                self.__default_locale,
            ),
            description=edit.get("Description"),
            set_as_event_name=bool(edit.get("SetAsEventName")),
            tag_ids=list(edit.get("TagIds") or []),
            args=list(edit.get("Args") or []),
            validation=validation,
        )

        response: dict[str, Any] = self.__request(
            db_name,
            self.__macros_url(f"/{macro_id}/clone"),
            "clone_macro",
            method="PUT",
            body=body,
        ).json()
        clone_id = str(response["Id"])

        self.__macros.clear()
        self.log.info(
            f'status=success, action=clone_macro, msg="Cloned macro to '
            f'{system_name!r}", macro_id={clone_id!r}, '
            f"hostname={self.__kb_hostname!r}, db={db_name!r}"
        )
        return clone_id

    def rename_macro(
        self, macro_id: str, new_system_name: str, db_name: str | None = None
    ) -> str:
        """Переименовать макрос (``POST /api-studio/siem/macros/rename`` + save).

        Эндпоинт ``rename`` сам объект не меняет: он переписывает системное имя
        в переданном ``formula`` и возвращает исправленное тело (с ``null`` в
        ``formula`` - ``500``, измерено). Поэтому берётся текущий текст,
        переписывается через ``rename`` и сохраняется вместе с новым
        ``systemName`` - иначе в тексте остаётся старое имя.

        :param macro_id: ID макроса
        :param new_system_name: Новое системное имя
        :param db_name: Имя БД (None - из ``set_db_name``, иначе первая
            доступная)
        :return: Текст макроса с новым системным именем
        """
        db_name = self.__resolve_db_name(db_name)
        edit = self.__get_edit_model(macro_id, db_name, "rename_macro")
        text = str(edit.get("Text") or "")

        response: dict[str, Any] = self.__request(
            db_name,
            self.__macros_url("/rename"),
            "rename_macro",
            method="POST",
            body={
                "oldSystemName": edit.get("SystemName"),
                "newSystemName": new_system_name,
                "formula": text,
            },
        ).json()
        new_text = str(response.get("Formula") or "")

        self.update_macro(
            macro_id,
            new_text,
            system_name=new_system_name,
            set_as_event_name=bool(edit.get("SetAsEventName")),
            db_name=db_name,
        )

        self.log.info(
            f'status=success, action=rename_macro, msg="Renamed '
            f'{edit.get("SystemName")!r} to {new_system_name!r}", '
            f"macro_id={macro_id!r}, hostname={self.__kb_hostname!r}, db={db_name!r}"
        )
        return new_text

    # ------------------------------------------------------------------ #
    # Параметры, локали и метки макроса (модификация через edit + save)
    # ------------------------------------------------------------------ #

    def __save_macro_model(
        self,
        edit: dict[str, Any],
        macro_id: str,
        db_name: str,
        action: str,
        text: str | None = None,
        system_name: str | None = None,
        name: Any = None,
        description: Any = None,
        tag_ids: list[str] | None = None,
        args: list[dict[str, Any]] | None = None,
    ) -> None:
        """Сохранить изменённую модель макроса с обязательной валидацией."""
        new_text = text if text is not None else str(edit.get("Text") or "")
        validation = self.validate_macro_text(new_text, db_name)
        if validation["errors"]:
            raise ValueError(
                f"Текст макроса не скомпилировался: {validation['errors']}"
            )

        body = self.__build_macro_body(
            text=new_text,
            system_name=system_name or str(edit.get("SystemName")),
            name=edit.get("Name") if name is None else name,
            description=(
                edit.get("Description") if description is None else description
            ),
            set_as_event_name=bool(edit.get("SetAsEventName")),
            tag_ids=(
                list(edit.get("TagIds") or []) if tag_ids is None else list(tag_ids)
            ),
            args=list(edit.get("Args") or []) if args is None else list(args),
            validation=validation,
        )

        self.__request(
            db_name,
            self.__macros_url(f"/{macro_id}"),
            action,
            method="PUT",
            body=body,
        )
        self.__macros.clear()
        self.log.info(
            f"status=success, action={action}, method=PUT, "
            f"macro_id={macro_id!r}, hostname={self.__kb_hostname!r}, db={db_name!r}"
        )

    @staticmethod
    def __renumber_args(args: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Пронумеровать ``Index`` параметров по порядку.

        Сервер отклоняет запрос с повторяющимся ``Index``
        (``409 SiemMacros.IndexConflict``), а сохранённые параметры нумерует сам.
        """
        return [
            {**{k: v for k, v in arg.items() if k != "Index"}, "Index": index}
            for index, arg in enumerate(args)
        ]

    def add_macro_param(
        self,
        macro_id: str,
        name: str,
        param_type: str,
        default_value: str = "",
        index: int | None = None,
        description: str = "",
        locale: str | None = None,
        db_name: str | None = None,
    ) -> None:
        """Добавить параметр макроса.

        Отдельного эндпоинта в KB Studio API нет: параметр добавляется в
        ``Args`` модели редактирования, тело перечитывается через ``validate``
        и сохраняется целиком. ``Args`` - самостоятельные метаданные: сервер
        сохраняет и параметр, объявленный только здесь (в ``text`` его может не
        быть).

        :param macro_id: ID макроса
        :param name: Имя параметра (без ``$``)
        :param param_type: Тип параметра: ``string`` | ``number`` | ``bool``
        :param default_value: Значение по умолчанию
        :param index: Позиция параметра (None - в конец списка)
        :param description: Описание параметра
        :param locale: Локаль описания (по умолчанию ``RUS``)
        :param db_name: Имя БД (None - из ``set_db_name``, иначе первая
            доступная)
        :raises ValueError: если параметр с таким именем уже есть
        """
        db_name = self.__resolve_db_name(db_name)
        locale = locale or self.__default_locale
        edit = self.__get_edit_model(macro_id, db_name, "add_macro_param")
        args = [dict(arg) for arg in edit.get("Args") or []]
        if any(arg.get("Name") == name for arg in args):
            raise ValueError(f"Параметр {name!r} уже есть у макроса {macro_id!r}")

        new_arg = {
            "Name": name,
            "Type": param_type,
            "DefaultValue": default_value,
            "Description": {locale: description},
        }
        if index is None:
            args.append(new_arg)
        else:
            args.insert(min(max(index, 0), len(args)), new_arg)

        self.__save_macro_model(edit, macro_id, db_name, "add_macro_param", args=args)

    def remove_macro_param(
        self, macro_id: str, name: str, db_name: str | None = None
    ) -> None:
        """Удалить параметр макроса по имени.

        :param macro_id: ID макроса
        :param name: Имя параметра
        :param db_name: Имя БД (None - из ``set_db_name``, иначе первая
            доступная)
        :raises ValueError: если параметр не найден
        """
        db_name = self.__resolve_db_name(db_name)
        edit = self.__get_edit_model(macro_id, db_name, "remove_macro_param")
        args = [dict(arg) for arg in edit.get("Args") or []]
        remaining = [arg for arg in args if arg.get("Name") != name]
        if len(remaining) == len(args):
            raise ValueError(f"Параметр {name!r} не найден у макроса {macro_id!r}")

        self.__save_macro_model(
            edit, macro_id, db_name, "remove_macro_param", args=remaining
        )

    def add_macro_locale(
        self,
        macro_id: str,
        locale: str,
        name: str,
        description: str,
        db_name: str | None = None,
    ) -> None:
        """Добавить локаль макроса.

        :param macro_id: ID макроса
        :param locale: Код локали (``RUS``/``ENG`` ...)
        :param name: Имя в этой локали
        :param description: Описание в этой локали
        :param db_name: Имя БД (None - из ``set_db_name``, иначе первая
            доступная)
        :raises ValueError: если локаль уже есть
        """
        db_name = self.__resolve_db_name(db_name)
        edit = self.__get_edit_model(macro_id, db_name, "add_macro_locale")
        current_name = self.__as_dict(edit.get("Name"))
        if locale in current_name:
            raise ValueError(f"Локаль {locale!r} уже есть у макроса {macro_id!r}")

        self.__save_macro_model(
            edit,
            macro_id,
            db_name,
            "add_macro_locale",
            name={**current_name, locale: name},
            description={
                **self.__as_dict(edit.get("Description")),
                locale: description,
            },
        )

    def change_macro_locale(
        self,
        macro_id: str,
        locale: str,
        name: str,
        description: str,
        db_name: str | None = None,
    ) -> None:
        """Изменить локаль макроса.

        :param macro_id: ID макроса
        :param locale: Код изменяемой локали
        :param name: Новое имя в этой локали
        :param description: Новое описание в этой локали
        :param db_name: Имя БД (None - из ``set_db_name``, иначе первая
            доступная)
        """
        db_name = self.__resolve_db_name(db_name)
        edit = self.__get_edit_model(macro_id, db_name, "change_macro_locale")
        current_name = self.__as_dict(edit.get("Name"))
        if locale not in current_name:
            raise ValueError(f"Локаль {locale!r} не найдена у макроса {macro_id!r}")

        self.__save_macro_model(
            edit,
            macro_id,
            db_name,
            "change_macro_locale",
            name={**current_name, locale: name},
            description={
                **self.__as_dict(edit.get("Description")),
                locale: description,
            },
        )

    def remove_macro_locale(
        self, macro_id: str, locale: str, db_name: str | None = None
    ) -> None:
        """Удалить локаль макроса.

        :param macro_id: ID макроса
        :param locale: Код локали для удаления
        :param db_name: Имя БД (None - из ``set_db_name``, иначе первая
            доступная)
        :raises ValueError: если локаль не найдена или это последняя локаль
        """
        db_name = self.__resolve_db_name(db_name)
        edit = self.__get_edit_model(macro_id, db_name, "remove_macro_locale")
        current_name = self.__as_dict(edit.get("Name"))
        if locale not in current_name:
            raise ValueError(f"Локаль {locale!r} не найдена у макроса {macro_id!r}")
        if len(current_name) < 2:
            raise ValueError("Нельзя удалить последнюю локаль макроса")

        description = self.__as_dict(edit.get("Description"))
        self.__save_macro_model(
            edit,
            macro_id,
            db_name,
            "remove_macro_locale",
            name={k: v for k, v in current_name.items() if k != locale},
            description={k: v for k, v in description.items() if k != locale},
        )

    def __set_macro_tags(
        self, macro_id: str, tag_ids: list[str], action: str, db_name: str
    ) -> None:
        """Перезаписать метки макроса (``TagIds`` модели редактирования)."""
        edit = self.__get_edit_model(macro_id, db_name, action)
        self.__save_macro_model(edit, macro_id, db_name, action, tag_ids=tag_ids)

    def add_macro_tag(
        self, macro_id: str, tag_id: str, db_name: str | None = None
    ) -> None:
        """Привязать метку к макросу.

        :param macro_id: ID макроса
        :param tag_id: ID метки (из ``get_macros_tags_list``)
        :param db_name: Имя БД (None - из ``set_db_name``, иначе первая
            доступная)
        """
        db_name = self.__resolve_db_name(db_name)
        macro = self.get_macro(macro_id, db_name)
        if macro is None:
            raise ValueError(f"Макрос {macro_id!r} не найден")

        tag_ids = list(macro["tag_ids"])
        if tag_id not in tag_ids:
            tag_ids.append(tag_id)
        self.__set_macro_tags(macro_id, tag_ids, "add_macro_tag", db_name)

    def remove_macro_tag(
        self, macro_id: str, tag_id: str, db_name: str | None = None
    ) -> None:
        """Отвязать метку от макроса.

        :param macro_id: ID макроса
        :param tag_id: ID метки
        :param db_name: Имя БД (None - из ``set_db_name``, иначе первая
            доступная)
        """
        db_name = self.__resolve_db_name(db_name)
        macro = self.get_macro(macro_id, db_name)
        if macro is None:
            raise ValueError(f"Макрос {macro_id!r} не найден")

        tag_ids = [t for t in macro["tag_ids"] if t != tag_id]
        self.__set_macro_tags(macro_id, tag_ids, "remove_macro_tag", db_name)

    # ------------------------------------------------------------------ #
    # Метки макросов (api-studio/siem/macros/tags)
    # ------------------------------------------------------------------ #

    @staticmethod
    def __normalize_tag(tag: dict[str, Any]) -> dict[str, Any]:
        """Привести метку KB Studio к snake_case стилю модуля."""
        return {
            "id": tag.get("Id"),
            "parent_id": tag.get("ParentId"),
            "name": tag.get("Name"),
            "is_group": tag.get("IsGroup"),
            "origin": tag.get("Origin") or {},
        }

    def get_macros_tags_list(self, db_name: str | None = None) -> list[dict[str, Any]]:
        """Список меток макросов (``GET /api-studio/siem/macros/tags``).

        :param db_name: Имя БД (None - из ``set_db_name``, иначе первая
            доступная)
        :return: [{"id": ..., "parent_id": ..., "name": ..., "is_group": ...,
            "origin": {...}}]
        """
        db_name = self.__resolve_db_name(db_name)
        tags_raw: list[dict[str, Any]] = self.__request(
            db_name, self.__tags_url(), "get_macros_tags_list"
        ).json()
        tags = [self.__normalize_tag(tag) for tag in tags_raw]

        self.log.info(
            f'status=success, action=get_macros_tags_list, msg="Got {len(tags)} tags", '
            f"hostname={self.__kb_hostname!r}, db={db_name!r}"
        )
        return tags

    def get_macro_tag(
        self, tag_id: str, db_name: str | None = None
    ) -> dict[str, Any] | None:
        """Метка макросов по ID (``GET /api-studio/siem/macros/tags/{id}``).

        :param tag_id: ID метки
        :param db_name: Имя БД (None - из ``set_db_name``, иначе первая
            доступная)
        :return: Метка либо ``None``, если она отсутствует
        """
        db_name = self.__resolve_db_name(db_name)
        try:
            response = self.__request(
                db_name, self.__tags_url(f"/{tag_id}"), "get_macro_tag"
            )
        except requests.HTTPError as err:
            code = err.response.status_code if err.response is not None else None
            if code in (404, 409):
                self.log.warning(
                    f'status=failed, action=get_macro_tag, msg="Tag {tag_id!r} not '
                    f'found", code={code}, hostname={self.__kb_hostname!r}, '
                    f"db={db_name!r}"
                )
                return None
            raise

        return self.__normalize_tag(response.json())

    def get_macro_tag_edit_model(
        self, tag_id: str, db_name: str | None = None
    ) -> dict[str, Any]:
        """Модель редактирования метки
        (``GET /api-studio/siem/macros/tags/{id}/edit``).

        :param tag_id: ID метки
        :param db_name: Имя БД (None - из ``set_db_name``, иначе первая
            доступная)
        :return: {"Id", "ParentId", "Name", "IsGroup"} c ``Name`` словарём
            локалей
        """
        db_name = self.__resolve_db_name(db_name)
        edit_model: dict[str, Any] = self.__request(
            db_name, self.__tags_url(f"/{tag_id}/edit"), "get_macro_tag_edit_model"
        ).json()
        return edit_model

    def create_macro_tag(
        self,
        name: str,
        is_group: bool = False,
        parent_id: str | None = None,
        db_name: str | None = None,
    ) -> str:
        """Создать метку макросов (``POST /api-studio/siem/macros/tags``).

        :param name: Имя метки
        :param is_group: Является ли метка группой
        :param parent_id: ID родительской метки-группы
        :param db_name: Имя БД (None - из ``set_db_name``, иначе первая
            доступная)
        :return: ID созданной метки
        """
        db_name = self.__resolve_db_name(db_name)
        response: dict[str, Any] = self.__request(
            db_name,
            self.__tags_url(),
            "create_macro_tag",
            method="POST",
            body={
                "name": {self.__default_locale: name},
                "isGroup": is_group,
                "parentId": parent_id,
            },
        ).json()
        tag_id = str(response["Id"])

        self.log.info(
            f'status=success, action=create_macro_tag, msg="Created tag {name!r}", '
            f"tag_id={tag_id!r}, hostname={self.__kb_hostname!r}, db={db_name!r}"
        )
        return tag_id

    def rename_macro_tag(
        self,
        tag_id: str,
        name: str,
        is_group: bool | None = None,
        parent_id: str | None = None,
        db_name: str | None = None,
    ) -> None:
        """Изменить метку (``PUT /api-studio/siem/macros/tags/{id}``).

        Тело полное: берётся из ``.../edit``, заменяются только переданные
        поля.

        :param tag_id: ID метки
        :param name: Новое имя в локали ``RUS``
        :param is_group: Новое значение признака группы (None - оставить)
        :param parent_id: Новый ID родительской группы (None - оставить)
        :param db_name: Имя БД (None - из ``set_db_name``, иначе первая
            доступная)
        """
        db_name = self.__resolve_db_name(db_name)
        edit: dict[str, Any] = self.__request(
            db_name, self.__tags_url(f"/{tag_id}/edit"), "rename_macro_tag"
        ).json()

        self.__request(
            db_name,
            self.__tags_url(f"/{tag_id}"),
            "rename_macro_tag",
            method="PUT",
            body={
                "name": self.__localize(edit.get("Name"), name, self.__default_locale),
                "isGroup": (
                    bool(edit.get("IsGroup")) if is_group is None else is_group
                ),
                "parentId": (edit.get("ParentId") if parent_id is None else parent_id),
            },
        )

        self.log.info(
            f'status=success, action=rename_macro_tag, msg="Renamed tag to '
            f'{name!r}", tag_id={tag_id!r}, hostname={self.__kb_hostname!r}, '
            f"db={db_name!r}"
        )

    def delete_macro_tag(
        self, tag_id: str, db_name: str | None = None
    ) -> requests.Response:
        """Удалить метку макросов (``DELETE /api-studio/siem/macros/tags/{id}``).

        Привязки метки к макросам снимаются на стороне сервера.

        :param tag_id: ID удаляемой метки
        :param db_name: Имя БД (None - из ``set_db_name``, иначе первая
            доступная)
        :return: Объект Response
        """
        db_name = self.__resolve_db_name(db_name)
        response = self.__request(
            db_name,
            self.__tags_url(f"/{tag_id}"),
            "delete_macro_tag",
            method="DELETE",
        )
        self.log.info(
            f'status=success, action=delete_macro_tag, msg="Deleted tag", '
            f"tag_id={tag_id!r}, hostname={self.__kb_hostname!r}, db={db_name!r}"
        )
        return response

    def close(self) -> None:
        if self.__kb_session is not None:
            self.__kb_session.close()
