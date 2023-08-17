from mpsiemlib.common import ModuleInterface, MPSIEMAuth, LoggingHandler, MPComponents, Settings
from mpsiemlib.common import exec_request


class Macros(ModuleInterface, LoggingHandler):
    """
    Filters module
    """

    __kb_port = 8091
    __api_macros_list = '/api-studio/siem/macros/list'
    __api_macros_info = '/api-studio/siem/macros/'

    def __init__(self, auth: MPSIEMAuth, settings: Settings):
        ModuleInterface.__init__(self, auth, settings)
        LoggingHandler.__init__(self)
        self.__core_session = auth.connect(MPComponents.CORE)
        self.__core_hostname = auth.creds.core_hostname
        self.__kb_session = auth.connect(MPComponents.KB)
        self.__kb_hostname = auth.creds.core_hostname
        self.__macros = []
        self.__filters = {}
        self.__db_name = None
        self.log.debug('status=success, action=prepare, msg="Macros Module init"')

    def set_db_name(self, db_name: str):
        """
        Установить БД для работы с макросами.
        Используются ID правил из KB.

        :param db_name: Имя БД в KB
        :return:
        """
        self.__db_name = db_name

    def get_macros_list(self) -> list:
        """
        Получить список всех макросов

        :return: {"id": {"parent_id": "value", "name": "value", "source": "value"}}
        """
        if len(self.__macros) != 0:
            return self.__macros

        url = f'https://{self.__core_hostname}:{self.__kb_port}{self.__api_macros_list}'

        params = dict(tagId=None, sort=[
            dict(name='objectId', order=0, type=0)
        ], filters=None, search='', skip=0, take=1000)

        headers = {'Content-Database': self.__db_name,
                   'Content-Locale': 'RUS'}

        macros = exec_request(self.__kb_session,
                              url,
                              method='POST',
                              timeout=self.settings.connection_timeout,
                              json=params,
                              headers=headers).json()

        for macro in macros.get('Rows'):
            self.__macros.append(dict(id=macro.get('Id'), name=macro.get('Name'), object_id=macro.get('ObjectId')))

        return self.__macros

    def get_macros_info(self, macro_id: str):
        """
        Получение информации о фильтре по id макроса
        """
        url = f'https://{self.__core_hostname}:{self.__kb_port}{self.__api_macros_info}{macro_id}'

        headers = {'Content-Database': self.__db_name,
                   'Content-Locale': 'RUS'}

        response = exec_request(self.__kb_session,
                                url,
                                method='GET', headers=headers).json()
        return response
