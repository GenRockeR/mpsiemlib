from mpsiemlib.common import ModuleInterface, MPSIEMAuth, LoggingHandler, MPComponents, Settings
from mpsiemlib.common import exec_request


class Conveyor(ModuleInterface, LoggingHandler):
    """Conveyor module."""

    __api_conveyor_list = '/api/siem_manager/v1/siems'

    def __init__(self, auth: MPSIEMAuth, settings: Settings):
        ModuleInterface.__init__(self, auth, settings)
        LoggingHandler.__init__(self)
        self.__core_session = auth.connect(MPComponents.CORE)
        self.__core_hostname = auth.creds.core_hostname
        self.__conveyor = []
        self.log.debug('status=success, action=prepare, msg="Conveyor Module init"')
        self.__default_conveyor = None

    def get_conveyor_list(self) -> list:
        """Получить список всех конвейеров."""
        if len(self.__conveyor) != 0:
            return self.__conveyor

        url = f'https://{self.__core_hostname}:{self.__api_conveyor_list}'

        conveyors = exec_request(self.__core_session,
                                 url,
                                 method='GET',
                                 timeout=self.settings.connection_timeout).json()

        for conveyor in conveyors:
            self.__conveyor.append(dict(id=conveyor.get('id'),
                                        alias=conveyor.get('alias'),
                                        is_primary=conveyor.get('isPrimary'),
                                        version=conveyor.get('version')))

        return self.__conveyor

    def get_primary_conveyor(self) -> dict:
        conveyors = self.get_conveyor_list()
        for conveyor in conveyors:
            if conveyor.get('is_primary'):
                return conveyor

    def get_conveyor_id_by_alias(self, alias: str) -> str:
        conveyors = self.get_conveyor_list()
        for conveyor in conveyors:
            if conveyor.get('alias') == alias:
                return conveyor.get('id')
            else:
                return ''

    def set_default_conveyor(self):
        def_conveyor = self.get_primary_conveyor()
        self.__default_conveyor = def_conveyor
