from mpsiemlib.common import ModuleInterface, MPSIEMAuth, MPComponents, LoggingHandler, Settings
from mpsiemlib.common import exec_request

class EventsAPI(ModuleInterface, LoggingHandler):
    """
    Модуль получения информации о событиях через API UI
    """
    __api_events_metadata = "/api/events/v2/events_metadata"
    __api_event_details = "/api/events/v2/events/{}/normalized?time={}"
    

    def __init__(self, auth: MPSIEMAuth, settings: Settings):
        ModuleInterface.__init__(self, auth, settings)
        LoggingHandler.__init__(self)
        self.__core_session = auth.connect(MPComponents.CORE)
        self.__core_hostname = auth.creds.core_hostname
        self.log.debug('status=succes, action=prepare, msg="EventsUI Module init"')
        
    def get_event_details(self, event_id, event_date) -> dict:
        """
        Получить событие (все заполненные поля) по его идентификатору и дате

        Args:
            event_id : идентификатор события
            event_date : дата
        Returns:
            [type]: событие
        """
        api_url = self.__api_event_details.format(event_id, event_date)
        url = "https://{}{}".format(self.__core_hostname, api_url)
        rq = exec_request(self.__core_session, url)
        response = rq.json()

        if response is None or "event" not in response:
            self.log.error('status=failed, action=get_event_details, msg="Core data request return None or '
                           'has wrong response structure", '
                           'hostname="{}"'.format(self.__core_hostname))
            raise Exception("Core data request return None or has wrong response structure")
        return response.get("event")
    
    def get_events_metadata(self):
        """
        Получить список поддерживаемых полей таксономии событий
        """
        url = "https://{}{}".format(self.__core_hostname, self.__api_events_metadata)
        rq = exec_request(self.__core_session, url)
        response = rq.json()

        if response is None or "fields" not in response:
            self.log.error('status=failed, action=get_events_metadata, msg="Core data request return None or '
                           'has wrong response structure", '
                           'hostname="{}"'.format(self.__core_hostname))
            raise Exception("Core data request return None or has wrong response structure")
        return response.get("fields")    
        