from mpsiemlib.common import ModuleInterface, MPSIEMAuth, LoggingHandler, MPComponents, Settings
from mpsiemlib.common import exec_request


class UsersAndRoles(ModuleInterface, LoggingHandler):
    """
    Users and Roles management
    """

    __ms_port = 3334

    __api_applications_list = "/ptms/api/sso/v1/applications"
    __api_roles_list = "/ptms/api/sso/v2/applications/{}/roles"
    __api_privileges_list = "/ptms/api/sso/v2/applications/{}/privileges"
    __api_users_list = "/ptms/api/sso/v1/users/query"

    def __init__(self, auth: MPSIEMAuth, settings: Settings):
        ModuleInterface.__init__(self, auth, settings)
        LoggingHandler.__init__(self)
        self.__ms_session = auth.connect(MPComponents.MS)
        self.__ms_hostname = auth.creds.core_hostname
        self.__applications = {}
        self.__roles = {}
        self.__privileges = {}
        self.__users = {}

    def get_applications_list(self) -> dict:
        """
        Получить информацию по приложениям, включая тенанты

        :return: {'app_id': {'name': 'value', 'tenants': ['', '']}}
        """
        self.__applications.clear()
        self.log.debug('status=prepare, action=get_applications, msg="Try to get applications as {}", '
                       'hostname="{}"'.format(self.auth.creds.core_login, self.__ms_hostname))

        url = "https://{}:{}{}".format(self.__ms_hostname, self.__ms_port, self.__api_applications_list)
        rq = exec_request(self.__ms_session, url, method="GET", timeout=self.settings.connection_timeout)
        response = rq.json()

        for i in response:
            app_id = i.get("id")
            app_name = i.get("name")
            app_tenants = i.get("tenantIds")
            if self.__applications.get(app_id) is None:
                self.__applications[app_id] = {}
            self.__applications[app_id] = {'name': app_name, 'tenants_ids': app_tenants}

        self.log.info('status=success, action=get_applications, msg="Got {} apps", '
                      'hostname="{}"'.format(len(self.__applications), self.__ms_hostname))

        return self.__applications

    def get_users_list(self, filters=None) -> dict:
        """
        Получить список всех пользователей

        :param filters: {"rolesIds": ["id", "id"],
                  "authTypes": [1, 0],
                  "ldapPoolNames": ["ldap_pool_name"],
                  "statuses": ["active", "blocked"],
                  "withoutRoles": True}
        :return: {"user_name": {"param1": "value"}}
        """

        # TODO Пользователи могут дублироваться в рамках разных сайтов. Сейчас не учитывается.

        self.__users.clear()
        self.log.debug('status=prepare, action=get_users_list, msg="Try to get users list as {}", '
                       'hostname="{}"'.format(self.auth.creds.core_login, self.__ms_hostname))
        self.log.warning('status=prepare, action=get_users_list, msg="Call high privileged operation with {}", '
                         'hostname="{}"'.format(self.auth.creds.core_login, self.__ms_hostname))

        params = {"authTypes": [1, 0],
                  "statuses": ["active", "blocked"],
                  "withoutRoles": True}
        if filters is not None:
            params = filters

        url = "https://{}:{}{}".format(self.__ms_hostname, self.__ms_port, self.__api_users_list)
        rq = exec_request(self.__ms_session,
                          url,
                          method="POST",
                          timeout=self.settings.connection_timeout,
                          json=params)
        response = rq.json()

        # маршалинг, т.к. контракт может меняться
        for i in response.get("items"):
            user_id = i.get("id")
            user_name = i.get("userName")
            user_status = i.get("status")
            user_readonly = i.get("isReadOnly")
            user_site_id = i.get("siteId")
            user_auth_type = i.get("authType")
            user_ldap_aliases = i.get("ldapAliases")
            user_roles = i.get("roles")
            user_is_system = i.get("system")
            if len(user_ldap_aliases) == 1 and user_ldap_aliases[0] == '':
                user_ldap_aliases = []
            if user_roles is not None:
                reformatted_roles = {}
                for r in user_roles:
                    reformatted_roles = {"id": r.get("roleId"),
                                         "application_id": r.get("applicationId"),
                                         "tenant_id": r.get("tenantId")}
                user_roles = reformatted_roles

            if self.__users.get(user_name) is None:
                self.__users[user_name] = {}

            self.__users[user_name] = {"id": user_id,
                                       "status": user_status,
                                       "readonly": user_readonly,
                                       "site_id": user_site_id,
                                       "auth_type": user_auth_type,
                                       "ldap_aliases": user_ldap_aliases,
                                       "roles": user_roles,
                                       "system": user_is_system}
        self.log.info('status=success, action=get_applications, msg="Got {} ussers", '
                      'hostname="{}"'.format(len(self.__users), self.__ms_hostname))

        return self.__users

    def get_user_info(self, user_name: str) -> dict:
        """
        Получить информацию по пользователю

        :param user_name:
        :return: {"param1": "value", "param2": "value"}
        """
        if len(self.__users) == 0:
            self.get_users_list()

        return self.__users.get(user_name)

    def create_user(self, user_name: str, user_password: str, role_name: str):
        raise NotImplementedError()

    def delete_user(self, user_id: str):
        raise NotImplementedError()

    def get_roles_list(self) -> dict:
        """
        Получить полный список ролей

        :return: {'component': {'role_name': {'param1': 'value1'}}}
        """
        self.__roles.clear()
        self.log.debug('status=prepare, action=get_groups, msg="Try to get roles as {}", '
                       'hostname="{}"'.format(self.auth.creds.core_login, self.__ms_hostname))
        count = 0
        self.__get_roles(MPComponents.MS)
        count += len(self.__roles.get(MPComponents.MS))
        self.__get_roles(MPComponents.KB)
        count += len(self.__roles.get(MPComponents.KB))
        self.__get_roles(MPComponents.CORE)
        count += len(self.__roles.get(MPComponents.CORE))

        self.log.info('status=success, action=get_roles, msg="Got {} roles", '
                      'hostname="{}"'.format(count, self.__ms_hostname))

        return self.__roles

    def __get_roles(self, app_type: str):
        api_url = self.__api_roles_list.format(app_type)
        url = "https://{}:{}{}".format(self.__ms_hostname, self.__ms_port, api_url)
        rq = exec_request(self.__ms_session, url, method="GET", timeout=self.settings.connection_timeout)
        response = rq.json()

        self.__roles[app_type] = {}
        for i in response:
            role_id = i.get("id")
            role_name = i.get("name")
            role_priv = i.get("privileges")
            if self.__roles[app_type].get(role_name) is None:
                self.__roles[app_type][role_name] = {}
            self.__roles[app_type][role_name] = {"id": role_id, "privileges": role_priv}

        self.log.debug('status=success, action=get_roles, msg="Got roles from {}", '
                       'hostname="{}" roles="{}"'.format(app_type, self.__ms_hostname, self.__roles))

    def get_role_info(self, role_name: str, component: str) -> dict:
        """
        Получить информацию по конкретной роле
        :param role_name: Имя роли
        :param component: MPComponents
        :return: dict
        """
        if len(self.__roles) == 0:
            self.get_roles_list()

        return self.__roles[component][role_name]

    def get_privileges_list(self) -> dict:
        """
        Получить полный список всех доступных в системе привелегий

        :return: {'component': {'priv': 'name'}}
        """
        self.__privileges.clear()
        self.log.debug('status=prepare, action=get_groups, msg="Try to get privileges as {}", '
                       'hostname="{}"'.format(self.auth.creds.core_login, self.__ms_hostname))
        count = 0
        self.__get_privileges(MPComponents.MS)
        count += len(self.__privileges.get(MPComponents.MS))
        self.__get_privileges(MPComponents.KB)
        count += len(self.__privileges.get(MPComponents.KB))
        self.__get_privileges(MPComponents.CORE)
        count += len(self.__privileges.get(MPComponents.CORE))

        self.log.info('status=success, action=get_roles, msg="Got {} privileges", '
                      'hostname="{}"'.format(count, self.__ms_hostname))

        return self.__privileges

    def __get_privileges(self, app_type: str) -> None:
        """
        Парсинг ответа от сервера и заполнение привилегий

        :param app_type: MPComponent
        :return: None
        """
        api_url = self.__api_privileges_list.format(app_type)
        url = "https://{}:{}{}".format(self.__ms_hostname, self.__ms_port, api_url)
        rq = exec_request(self.__ms_session, url, method="GET", timeout=self.settings.connection_timeout)
        response = rq.json()

        self.__privileges[app_type] = {}
        for i in response:
            for p in i.get("privileges", {}):
                priv_id = p.get("code")
                priv_name = p.get("name")
                self.__privileges[app_type][priv_id] = priv_name

        self.log.debug('status=success, action=get_roles, msg="Got privileges from {}", '
                       'hostname="{}" privileges="{}"'.format(app_type, self.__ms_hostname, self.__privileges))

    def create_role(self, role_name: str):
        raise NotImplementedError()

    def delete_role(self, role_id: str):
        raise NotImplementedError()

    def close(self):
        if self.__ms_session is not None:
            self.__ms_session.close()
