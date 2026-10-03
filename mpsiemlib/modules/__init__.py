from collections.abc import Callable
from typing import Any

from mpsiemlib.common import (
    Creds,
    LoggingHandler,
    ModuleNames,
    MPComponents,
    MPSIEMAuth,
    Settings,
    WorkerInterface,
)

from .Assets import Assets
from .Conveyor import Conveyor
from .EventsAPI import EventsAPI
from .Filters import Filters
from .HealthMonitor import HealthMonitor
from .Incidents import Incidents
from .KnowledgeBase import KnowledgeBase
from .Macros import Macros
from .SourceMonitor import SourceMonitor
from .Tables import Tables
from .Tasks import Tasks
from .UsersAndRoles import UsersAndRoles


class MPSIEMWorker(WorkerInterface, LoggingHandler):
    def __init__(self, creds: Creds, settings: Settings) -> None:
        WorkerInterface.__init__(self, creds, settings)
        LoggingHandler.__init__(self)
        self.__auth = MPSIEMAuth(self.creds, self.settings)
        sessions = {}
        if self.creds.core_hostname:
            sessions["core"] = self.__auth.connect(MPComponents.CORE)
            sessions["ms"] = self.__auth.connect(MPComponents.MS)
            # sessions["kb"] = self.__auth.connect(MPComponents.KB)
        self.__auth.sessions = sessions

    def _events_factory(self, auth: MPSIEMAuth) -> Callable[[], Any]:
        from .Events import Events

        return lambda: Events(auth, self.settings)

    def _module_factories(self, auth: MPSIEMAuth) -> dict[str, Callable[[], Any]]:
        """Реестр классов модулей: имя модуля -> factory, привязанный к `auth`."""
        return {
            ModuleNames.AUTH: lambda: auth,
            ModuleNames.EVENTS: self._events_factory(auth),
            ModuleNames.EVENTSAPI: lambda: EventsAPI(auth, self.settings),
            ModuleNames.ASSETS: lambda: Assets(auth, self.settings),
            ModuleNames.TABLES: lambda: Tables(auth, self.settings),
            ModuleNames.URM: lambda: UsersAndRoles(auth, self.settings),
            ModuleNames.KB: lambda: KnowledgeBase(auth, self.settings),
            ModuleNames.INCIDENTS: lambda: Incidents(auth, self.settings),
            ModuleNames.HEALTH: lambda: HealthMonitor(auth, self.settings),
            ModuleNames.FILTERS: lambda: Filters(auth, self.settings),
            ModuleNames.TASKS: lambda: Tasks(auth, self.settings),
            ModuleNames.SOURCE_MONITOR: lambda: SourceMonitor(auth, self.settings),
            ModuleNames.MACROS: lambda: Macros(auth, self.settings),
            ModuleNames.CONVEYOR: lambda: Conveyor(auth, self.settings),
        }

    def get_module(self, module_name: str, creds: Creds | None = None) -> Any:
        """Получить экземпляр модуля.

        :param module_name: имя модуля из `ModuleNames`
        :param creds: Креды для отдельного подключения (опционально)
        :return: Экземпляр класса модуля
        :raise ValueError: для неизвестного или нереализованного имени модуля
        """
        auth = self.__auth

        if creds is not None:
            self.creds = creds
            auth = MPSIEMAuth(self.creds, self.settings)

        try:
            factory = self._module_factories(auth)[module_name]
        except KeyError:
            raise ValueError(
                f"Unknown module: {module_name!r}. "
                f"Supported: {sorted(self._module_factories(auth))}"
            )
        return factory()
