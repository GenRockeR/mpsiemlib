from typing import Any, cast

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


class Conveyor(ModuleInterface, LoggingHandler):
    """Conveyor module.

    Конвейеры (SIEM-инстансы), зарегистрированные в Core: контракт
    ``SiemManager.yaml`` (``/api/siem_manager/v1/siems``).

    ``siem_id`` (он же ``id`` конвейера из ``GET /siems``) - query-параметр
    многих Core-эндпоинтов, включая ``/api/events/*/table_lists``. Контракт
    помечает его как необязательный: при единственном конвейере сервер
    выбирает его сам, но когда конвейеров несколько - параметр становится
    обязательным. Правило инкапсулировано в :meth:`resolve_siem_id` для
    использования другими модулями (например, Tables).
    """

    __api_conveyor_list = "/api/siem_manager/v1/siems"

    def __init__(self, auth: MPSIEMAuth, settings: Settings) -> None:
        ModuleInterface.__init__(self, auth, settings)
        LoggingHandler.__init__(self)
        sessions = auth.sessions or {}
        core_session = sessions.get("core")
        if core_session is None:
            # MPSIEMWorker поднимает core-сессию сам; для standalone
            # экземпляра поднимаем лениво (как в Macros для KB)
            core_session = auth.connect(MPComponents.CORE)
        creds = auth.get_creds()
        if creds is None or creds.core_hostname is None:
            raise AuthError("Core hostname is not set")
        self.__core_session = core_session
        self.__core_hostname = creds.core_hostname
        self.__conveyor: list[dict[str, Any]] = []
        self.__default_conveyor_id: str | None = None
        self.log.debug('status=success, action=prepare, msg="Conveyor Module init"')

    # ------------------------------------------------------------------ #
    # Чтение (GetSiems / GetSiem)
    # ------------------------------------------------------------------ #

    def get_conveyor_list(self, refresh: bool = False) -> list[dict[str, Any]]:
        """Получить список конвейеров, зарегистрированных в Core
        (контракт GetSiems).

        :param refresh: принудительно обновить кэш (по умолчанию — из кэша)
        :return: [{id, alias, is_primary, status, type, address, version}]
        """
        if self.__conveyor and not refresh:
            return [dict(conveyor) for conveyor in self.__conveyor]

        url = f"https://{self.__core_hostname}{self.__api_conveyor_list}"
        response: list[dict[str, Any]] = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
        ).json()

        self.__conveyor = [
            {
                "id": item.get("id"),
                "alias": item.get("alias"),
                "is_primary": bool(item.get("isPrimary")),
                "status": item.get("status"),
                "type": item.get("type"),
                "address": item.get("address"),
                "version": item.get("version"),
            }
            for item in response
        ]

        self.log.info(
            f"status=success, action=get_conveyor_list, "
            f'msg="Found {len(self.__conveyor)} conveyors", '
            f"hostname={self.__core_hostname!r}"
        )
        return [dict(conveyor) for conveyor in self.__conveyor]

    def get_conveyor_info(self, siem_id: str) -> dict[str, Any]:
        """Полная информация о конвейере (контракт GetSiem, SiemFullInfo).

        :param siem_id: id конвейера (GUID)
        :return: SiemFullInfo как есть из ответа Core
        """
        api_url = f"{self.__api_conveyor_list}/{siem_id}"
        url = f"https://{self.__core_hostname}{api_url}"
        response: dict[str, Any] = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
        ).json()

        self.log.info(
            f"status=success, action=get_conveyor_info, "
            f'msg="Got conveyor {siem_id!r} info", '
            f"hostname={self.__core_hostname!r}"
        )
        return response

    def get_conveyor_by_alias(self, alias: str) -> dict[str, Any]:
        """Найти конвейер по псевдониму.

        :param alias: Псевдоним конвейера
        :return: Элемент списка конвейеров
        :raises ValueError: если конвейер с таким псевдонимом не найден
        """
        for conveyor in self.get_conveyor_list():
            if conveyor.get("alias") == alias:
                return conveyor
        raise ValueError(f"Conveyor with alias {alias!r} not found")

    def get_conveyor_id_by_alias(self, alias: str) -> str:
        """Получить id конвейера по псевдониму.

        :param alias: Псевдоним конвейера
        :return: id конвейера (GUID)
        :raises ValueError: если конвейер с таким псевдонимом не найден
        """
        return cast("str", self.get_conveyor_by_alias(alias).get("id"))

    def get_primary_conveyor(self) -> dict[str, Any]:
        """Получить основной конвейер.

        :return: Элемент списка конвейеров с is_primary=True
        :raises RuntimeError: если основной конвейер не помечен ни у одного
        """
        for conveyor in self.get_conveyor_list():
            if conveyor.get("is_primary"):
                return conveyor
        raise RuntimeError("Primary conveyor is not set among registered SIEMs")

    def get_primary_conveyor_id(self) -> str:
        """Получить id основного конвейера.

        :return: id конвейера (GUID)
        """
        return cast("str", self.get_primary_conveyor().get("id"))

    def get_conveyor_count(self) -> int:
        """Количество зарегистрированных конвейеров."""
        return len(self.get_conveyor_list())

    # ------------------------------------------------------------------ #
    # Разрешение siem_id (для query-параметров других модулей)
    # ------------------------------------------------------------------ #

    def set_default_conveyor(self, siem_id: str | None = None) -> str:
        """Запомнить конвейер по умолчанию для :meth:`resolve_siem_id`.

        :param siem_id: id конвейера (GUID); None — основной конвейер
        :return: Установленный siem_id
        :raises ValueError: если siem_id не зарегистрирован в Core
        """
        if siem_id is None:
            resolved = self.get_primary_conveyor_id()
        else:
            self.__check_conveyor_exists(siem_id)
            resolved = siem_id
        self.__default_conveyor_id = resolved
        self.log.info(
            f"status=success, action=set_default_conveyor, "
            f'msg="Default conveyor set to {resolved!r}", '
            f"hostname={self.__core_hostname!r}"
        )
        return resolved

    def get_default_conveyor_id(self) -> str | None:
        """Id конвейера по умолчанию (None — не задан)."""
        return self.__default_conveyor_id

    def clear_default_conveyor(self) -> None:
        """Сбросить конвейер по умолчанию."""
        self.__default_conveyor_id = None

    def resolve_siem_id(self, siem_id: str | None = None) -> str | None:
        """Определить значение siem_id для query-параметра Core-эндпоинтов.

        Правила:

        - явный ``siem_id`` — возвращается как есть (без сверки с реестром,
          чтобы не требовать прав на SiemManager API на каждый запрос);
        - иначе — конвейер по умолчанию из :meth:`set_default_conveyor`;
        - иначе, если конвейер ровно один — ``None``: параметр не
          требуется, Core подставит единственный конвейер сам;
        - иначе (конвейеров несколько, выбор не задан) — ``ValueError``:
          siem_id обязателен, нужно передать его явно или задать
          конвейер по умолчанию.

        :param siem_id: Явно заданный id конвейера (GUID), опционально
        :return: siem_id или None (параметр не нужен)
        :raises ValueError: конвейеров несколько и выбор не задан
        """
        if siem_id is not None:
            return siem_id
        if self.__default_conveyor_id is not None:
            return self.__default_conveyor_id

        try:
            conveyors = self.get_conveyor_list()
        except requests.RequestException as err:
            # Реестр недоступен (например, нет прав у учётной записи):
            # оставляем siem_id не переданным - сервер выберет сам.
            self.log.warning(
                f"status=warning, action=resolve_siem_id, "
                f'msg="Cannot enumerate conveyors, siem_id is omitted", '
                f"hostname={self.__core_hostname!r}, error={err!r}"
            )
            return None

        if len(conveyors) == 0:
            self.log.warning(
                f"status=warning, action=resolve_siem_id, "
                f'msg="No conveyors registered in Core, siem_id is omitted", '
                f"hostname={self.__core_hostname!r}"
            )
            return None
        if len(conveyors) == 1:
            return None
        raise ValueError(
            f"Core has {len(conveyors)} conveyors, siem_id is required: "
            f"pass siem_id explicitly or call set_default_conveyor(); "
            f"known aliases: {[c.get('alias') for c in conveyors]!r}"
        )

    def __check_conveyor_exists(self, siem_id: str) -> None:
        known = {str(conveyor.get("id")) for conveyor in self.get_conveyor_list()}
        if siem_id not in known:
            raise ValueError(f"Conveyor {siem_id!r} is not registered in Core")

    # ------------------------------------------------------------------ #
    # Управление регистрациями (UpdateSiem / DeleteSiem / ...)
    # ------------------------------------------------------------------ #

    def register_conveyor(self, siem_id: str, siem_info: dict[str, Any]) -> None:
        """Создать или обновить регистрацию конвейера (контракт UpdateSiem).

        Тело запроса — SiemInfo (type, address, uriScheme, port,
        ipAddresses, osType, version, agents, dbHost, dbPort, dbCrossPort,
        dbType).

        :param siem_id: id конвейера (GUID)
        :param siem_info: SiemInfo
        """
        api_url = f"{self.__api_conveyor_list}/{siem_id}"
        url = f"https://{self.__core_hostname}{api_url}"
        exec_request(
            self.__core_session,
            url,
            method="PUT",
            timeout=self.settings.connection_timeout,
            json=siem_info,
        )
        self.__invalidate_cache()
        self.log.info(
            f"status=success, action=register_conveyor, "
            f'msg="Conveyor {siem_id!r} registration updated", '
            f"hostname={self.__core_hostname!r}"
        )

    def delete_conveyor(self, siem_id: str) -> None:
        """Удалить конвейер (контракт DeleteSiem).

        :param siem_id: id конвейера (GUID)
        """
        api_url = f"{self.__api_conveyor_list}/{siem_id}"
        url = f"https://{self.__core_hostname}{api_url}"
        exec_request(
            self.__core_session,
            url,
            method="DELETE",
            timeout=self.settings.connection_timeout,
        )
        self.__invalidate_cache()
        self.log.info(
            f"status=success, action=delete_conveyor, "
            f'msg="Conveyor {siem_id!r} deleted", '
            f"hostname={self.__core_hostname!r}"
        )

    def update_conveyor_info(self, siem_id: str, alias: str) -> None:
        """Изменить псевдоним конвейера (контракт UpdateSiemInfo,
        SiemShortInfo).

        :param siem_id: id конвейера (GUID)
        :param alias: Новый псевдоним
        """
        api_url = f"{self.__api_conveyor_list}/{siem_id}/info"
        url = f"https://{self.__core_hostname}{api_url}"
        exec_request(
            self.__core_session,
            url,
            method="PUT",
            timeout=self.settings.connection_timeout,
            json={"alias": alias},
        )
        self.__invalidate_cache()
        self.log.info(
            f"status=success, action=update_conveyor_info, "
            f'msg="Conveyor {siem_id!r} alias set to {alias!r}", '
            f"hostname={self.__core_hostname!r}"
        )

    def update_conveyor_agents(self, siem_id: str, agents: list[str]) -> None:
        """Обновить состав агентов конвейера (контракт UpdateSiemAgents).

        :param siem_id: id конвейера (GUID)
        :param agents: Список id агентов (GUID)
        """
        api_url = f"{self.__api_conveyor_list}/{siem_id}/agents"
        url = f"https://{self.__core_hostname}{api_url}"
        exec_request(
            self.__core_session,
            url,
            method="PUT",
            timeout=self.settings.connection_timeout,
            json={"agents": agents},
        )
        self.__invalidate_cache()
        self.log.info(
            f"status=success, action=update_conveyor_agents, "
            f'msg="Conveyor {siem_id!r} agents updated '
            f'({len(agents)} agents)", '
            f"hostname={self.__core_hostname!r}"
        )

    def make_conveyor_primary(self, siem_id: str) -> None:
        """Назначить конвейер основным (контракт MakeSiemPrimary).

        :param siem_id: id конвейера (GUID)
        """
        api_url = f"{self.__api_conveyor_list}/{siem_id}/make_primary"
        url = f"https://{self.__core_hostname}{api_url}"
        exec_request(
            self.__core_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
        )
        self.__invalidate_cache()
        self.log.info(
            f"status=success, action=make_conveyor_primary, "
            f'msg="Conveyor {siem_id!r} made primary", '
            f"hostname={self.__core_hostname!r}"
        )

    def __invalidate_cache(self) -> None:
        self.__conveyor = []
        self.__default_conveyor_id = None

    def close(self) -> None:
        if self.__core_session is not None:
            self.__core_session.close()
