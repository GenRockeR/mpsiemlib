from mpsiemlib.common import LoggingHandler, WorkerInterface, Creds, ModuleNames, MPSIEMAuth, Settings, MPComponents
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
from .Macros import Macros
from .Conveyor import Conveyor


class MPSIEMWorker(WorkerInterface, LoggingHandler):

    def __init__(self, creds, settings: Settings):
        WorkerInterface.__init__(self, creds, settings)
        LoggingHandler.__init__(self)
        self.__module_name = None
        self.__auth = MPSIEMAuth(self.creds, self.settings)
        sessions = {}
        if self.creds.core_hostname:
            target_components = [('core', MPComponents.CORE), ('ms', MPComponents.MS), ('kb', MPComponents.KB)]
            for name, component in target_components:
                try:
                    session = self.__auth.connect(component)
                    if session:
                        sessions[name] = session
                    else:
                        self.log.warning(f"Connection to {name} returned empty session. Skipping...")
                except Exception as e:
                    self.log.warning(f"Failed to connect to component {name}: {e}. Skipping this component.")
        # if self.creds.siem_hostname:
        #     sessions['siem'] = self.__auth.connect(MPComponents.SIEM)
        # if self.creds.storage_hostname:
        #     sessions['storage'] = self.__auth.connect(MPComponents.STORAGE)
        self.__auth.sessions = sessions

    def get_module(self, module_name: ModuleNames, creds: Creds = None):
        self.__module_name = module_name
        auth = self.__auth

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
        if self.__module_name == ModuleNames.MACROS:
            return Macros(auth, self.settings)
        if self.__module_name == ModuleNames.CONVEYOR:
            return Conveyor(auth, self.settings)
