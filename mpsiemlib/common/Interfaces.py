"""Базовые интерфейсы, настройки и примитивы библиотеки.

Модуль не должен импортировать реализацию (MPSIEMAuth) во избежание
циклических импортов: тип MPSIEMAuth подключается только для mypy
через TYPE_CHECKING.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import requests

if TYPE_CHECKING:
    from .MPSIEMAuth import MPSIEMAuth


class Settings:
    connection_timeout: int = 60
    # Коэффициент увеличения timeout при генерации отчета.
    connection_timeout_x: int = 6
    # В ES все события приведены к UTC
    storage_events_timezone: str = "UTC"
    # В какой временной зоне работает MP
    local_timezone: str = "Europe/Moscow"
    # Размер бакета агрегации в Elastic (по умолчанию в конфиге 50000)
    storage_bucket_size: int = 33000
    # Размер выгружаемой пачки событий без агрегации
    storage_batch_size: int = 10000
    # Размер выгружаемой пачки записей из табличек
    tables_batch_size: int = 1000
    # Размер выгружаемой пачки правил из KB
    kb_objects_batch_size: int = 1000
    # Размер выгружаемой пачки инцидентов
    incidents_batch_size: int = 100
    # Размер выгружаемой пачки источников
    source_monitor_batch_size: int = 1000
    # Размер выгружаемой пачки активов
    assets_batch_size: int = 1000
    # Размер выгружаемой пачки событий через EventsAPI
    events_batch_size: int = 1000


class AuthType:
    LOCAL = 0
    LDAP = 1
    SECRET = 2
    PAT_TOKEN = 3

    VALUES = (LOCAL, LDAP, SECRET, PAT_TOKEN)


class ModuleNames:
    MACROS = "macros"
    CONVEYOR = "conveyor"
    AUTH = "auth"
    EVENTS = "events"
    EVENTSAPI = "eventsapi"
    ASSETS = "assets"
    TABLES = "tables"
    FILTERS = "filters"
    TASKS = "tasks"
    HEALTH = "health"
    URM = "users_and_roles"
    KB = "knowledge_base"
    INCIDENTS = "incidents"
    SOURCE_MONITOR = "source_monitor"
    EDR = "edr"

    @staticmethod
    def get_modules_list() -> list[str]:
        return [
            ModuleNames.AUTH,
            ModuleNames.ASSETS,
            ModuleNames.EVENTS,
            ModuleNames.EVENTSAPI,
            ModuleNames.TABLES,
            ModuleNames.FILTERS,
            ModuleNames.TASKS,
            ModuleNames.HEALTH,
            ModuleNames.URM,
            ModuleNames.KB,
            ModuleNames.INCIDENTS,
            ModuleNames.SOURCE_MONITOR,
            ModuleNames.MACROS,
            ModuleNames.CONVEYOR,
            ModuleNames.EDR,
        ]


class MPComponents:
    """
    Именование компонент. Должны совпадать с названиями в IAM
    """

    CORE = "mpx"
    SIEM = "siem"
    STORAGE = "storage"
    MS = "idmgr"
    KB = "ptkb"


class MPContentTypes:
    NORMALIZATION = "Normalization"
    AGGREGATION = "Aggregation"
    ENRICHMENT = "Enrichment"
    CORRELATION = "Correlation"
    TABLE = "TabularList"


class StorageVersion:
    ES7_17 = "7.17"
    ES7 = "7"
    ES17 = "1.7"
    ALL = "ALL"
    LS = "1"


class Creds:
    def __init__(self, params: dict[str, Any] | None = None) -> None:
        core: dict[str, Any] = (params or {}).get("core", {})
        self.__core_hostname: str | None = core.get("hostname")
        self.__core_login: str | None = core.get("login")
        self.__core_pass: str | None = core.get("pass")
        self.__core_auth_type: int | None = core.get("auth_type")
        self.__siem_hostname: str | None = (
            (params or {}).get("siem", {}).get("hostname")
        )
        self.__storage_hostname: str | None = (
            (params or {}).get("storage", {}).get("hostname")
        )
        self.__client_secret: str | None = (params or {}).get("client_secret")
        self.__pat_token: str | None = (params or {}).get("pat_token")

    @property
    def core_hostname(self) -> str | None:
        return self.__core_hostname

    @core_hostname.setter
    def core_hostname(self, value: str | None) -> None:
        self.__core_hostname = value

    @property
    def core_login(self) -> str | None:
        return self.__core_login

    @core_login.setter
    def core_login(self, value: str | None) -> None:
        self.__core_login = value

    @property
    def core_pass(self) -> str | None:
        return self.__core_pass

    @core_pass.setter
    def core_pass(self, value: str | None) -> None:
        self.__core_pass = value

    @property
    def core_auth_type(self) -> int | None:
        return self.__core_auth_type

    @core_auth_type.setter
    def core_auth_type(self, value: int | None) -> None:
        if value not in AuthType.VALUES:
            raise ValueError(
                "Auth Type must be 0 - Local, 1 - LDAP, 2 - SECRET, 3 - PAT_TOKEN"
            )
        self.__core_auth_type = value

    @property
    def siem_hostname(self) -> str | None:
        return self.__siem_hostname

    @siem_hostname.setter
    def siem_hostname(self, value: str | None) -> None:
        self.__siem_hostname = value

    @property
    def storage_hostname(self) -> str | None:
        return self.__storage_hostname

    @storage_hostname.setter
    def storage_hostname(self, value: str | None) -> None:
        self.__storage_hostname = value

    @property
    def client_secret(self) -> str | None:
        return self.__client_secret

    @client_secret.setter
    def client_secret(self, value: str | None) -> None:
        self.__client_secret = value

    @property
    def pat_token(self) -> str | None:
        return self.__pat_token

    @pat_token.setter
    def pat_token(self, value: str | None) -> None:
        self.__pat_token = value


class WorkerInterface:
    """
    Базовый интерфейс любого модуля для работы с MP SIEM
    """

    def __init__(self, creds: Creds, settings: Settings) -> None:
        self.creds = creds
        self.settings = settings

    def get_module(self, module_name: str, creds: Creds | None = None) -> Any:
        """
        Получить экземпляр модуля
        :param module_name: имя модуля
        :param creds: креды для подключения (опционально)
        :return: экземпляр класса
        """
        raise NotImplementedError


class ModuleInterface:
    def __init__(self, auth: MPSIEMAuth, settings: Settings) -> None:
        self.auth = auth
        self.settings = settings

    def close(self) -> None:
        pass


class AuthInterface:
    def __init__(self, creds: Creds, settings: Settings) -> None:
        self.creds = creds
        self.settings = settings

    def connect(self, component: str, creds: Creds | None = None) -> requests.Session:
        raise NotImplementedError

    def disconnect(self) -> None:
        raise NotImplementedError

    def get_session(self) -> requests.Session:
        raise NotImplementedError

    def get_component(self) -> str:
        raise NotImplementedError

    def get_creds(self) -> Creds:
        raise NotImplementedError


class LoggingHandler:
    def __init__(self) -> None:
        self.log = logging.getLogger(self.__class__.__name__)
