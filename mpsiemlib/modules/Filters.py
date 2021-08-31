from mpsiemlib.common import ModuleInterface, MPSIEMAuth, LoggingHandler, MPComponents, Settings
from mpsiemlib.common import exec_request


class Filters(ModuleInterface, LoggingHandler):
    """
    Filters module
    """

    __api_filters_list = "/api/v2/events/filters_hierarchy"
    __api_filter_info = "/api/v2/events/filters/{}"

    def __init__(self, auth: MPSIEMAuth, settings: Settings):
        ModuleInterface.__init__(self, auth, settings)
        LoggingHandler.__init__(self)
        self.__core_session = auth.connect(MPComponents.CORE)
        self.__core_hostname = auth.creds.core_hostname
        self.__folders = {}
        self.__filters = {}
        self.log.debug('status=success, action=prepare, msg="Filters Module init"')

    def get_folders_list(self) -> dict:
        """
        Получить список всех папок с фильтрами

        :return: {"id": {"parent_id": "value", "name": "value", "source": "value"}}
        """
        if len(self.__folders) != 0:
            return self.__folders

        url = "https://{}{}".format(self.__core_hostname, self.__api_filters_list)

        r = exec_request(self.__core_session,
                         url,
                         method='GET',
                         timeout=self.settings.connection_timeout)
        filters = r.json()

        self.__iterate_folders_tree(filters.get("roots"))

        self.log.info('status=success, action=get_folders_list, msg="Got {} folders", '
                      'hostname="{}"'.format(len(self.__folders), self.__core_hostname))

        return self.__folders

    def get_filters_list(self) -> dict:
        """
        Получить список всех фильтров

        :return: {"id": {"folder_id": "value", "name": "value", "source": "value"}}
        """
        if len(self.__filters) != 0:
            return self.__filters

        # папки и фильтры лежат в одной структуре и парсятся совместно
        self.get_folders_list()

        self.log.info('status=success, action=get_filters_list, msg="Got {} filters", '
                      'hostname="{}"'.format(len(self.__filters), self.__core_hostname))

        return self.__filters

    def __iterate_folders_tree(self, root_node, parent_id=None):
        for i in root_node:
            node_id = i.get("id")
            node_name = i.get("name")
            node_source = i.get("meta", {}).get("source")
            if i.get("type") == "filter_node":
                self.__filters[node_id] = {"folder_id": parent_id,
                                           "name": node_name,
                                           "source": node_source}
                continue
            if i.get("type") == "folder_node":
                self.__folders[node_id] = {"parent_id": parent_id,
                                           "name": node_name,
                                           "source": node_source}
                node_children = i.get("children")
                if node_children is not None and len(node_children) != 0:
                    self.__iterate_folders_tree(node_children, node_id)

    def get_filter_info(self, filter_id: str) -> dict:
        """
        Получить информацию по фильтру

        :param filter_id: ID фильтра
        :return: {"param1": "value", "param2": "value"}
        """
        api_url = self.__api_filter_info.format(filter_id)
        url = "https://{}{}".format(self.__core_hostname, api_url)

        r = exec_request(self.__core_session,
                         url,
                         method='GET',
                         timeout=self.settings.connection_timeout)
        filters = r.json()

        self.log.info('status=success, action=get_filter_info, msg="Got info for filter {}", '
                      'hostname="{}"'.format(filter_id, self.__core_hostname))

        return {"name": filters.get("name"),
                "folder_id": filters.get("folderId"),
                "removed": filters.get("isRemoved"),
                "source": filters.get("source"),
                "query": {"select": filters.get("select"),
                          "where": filters.get("where"),
                          "group": filters.get("groupBy"),
                          "order": filters.get("groupBy"),
                          "aggregate": filters.get("aggregateBy"),
                          "distribute": filters.get("distributeBy"),
                          "top": filters.get("top"),
                          "aliases": filters.get("aliases")}
                }

    def close(self):
        if self.__core_session is not None:
            self.__core_session.close()
