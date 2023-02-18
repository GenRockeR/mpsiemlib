from mpsiemlib.common import LoggingHandler, WorkerInterface, Creds, ModuleNames, MPSIEMAuth, Settings
from .Assets import Assets
from .Events import Events
from .EventsAPI import EventsAPI
from .Tables import Tables
from .UsersAndRoles import UsersAndRoles
from .KnowledgeBase import KnowledgeBase
from .Incidents import Incidents
from .HealthMonitor import HealthMonitor
from .Filters import Filters
from .Tasks import Tasks
from .SourceMonitor import SourceMonitor


class MPSIEMWorker(WorkerInterface, LoggingHandler):

    def __init__(self, creds, settings: Settings):
        WorkerInterface.__init__(self, creds, settings)
        LoggingHandler.__init__(self)
        self.__module_name = None

    def get_module(self, module_name: ModuleNames, creds: Creds = None):
        self.__module_name = module_name

        if creds is not None:
            self.creds = creds

        auth = MPSIEMAuth(self.creds, self.settings)

        if self.__module_name == ModuleNames.AUTH:
            return auth
        if self.__module_name == ModuleNames.EVENTS:
            return Events(auth, self.settings)
        if self.__module_name == ModuleNames.EVENTSAPI:
            return EventsAPI(auth, self.settings)

        if self.__module_name == ModuleNames.ASSETS:
            return Assets(auth, self.settings)
        if self.__module_name == ModuleNames.TABLES:
            return Tables(auth, self.settings)
        if self.__module_name == ModuleNames.URM:
            return UsersAndRoles(auth, self.settings)
        if self.__module_name == ModuleNames.KB:
            return KnowledgeBase(auth, self.settings)
        if self.__module_name == ModuleNames.INCIDENTS:
            return Incidents(auth, self.settings)
        if self.__module_name == ModuleNames.HEALTH:
            return HealthMonitor(auth, self.settings)
        if self.__module_name == ModuleNames.FILTERS:
            return Filters(auth, self.settings)
        if self.__module_name == ModuleNames.TASKS:
            return Tasks(auth, self.settings)
        if self.__module_name == ModuleNames.SOURCE_MONITOR:
            return SourceMonitor(auth, self.settings)
