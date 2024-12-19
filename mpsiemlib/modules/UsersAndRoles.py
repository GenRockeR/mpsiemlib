from mpsiemlib.common import ModuleInterface, MPSIEMAuth, LoggingHandler, MPComponents, Settings
from mpsiemlib.common import exec_request


class UsersAndRoles(ModuleInterface, LoggingHandler):
    """Users and Roles management."""

    __ms_port = 3334
    __headers = {'Content-Type': 'application/json'}

    __api_applications_list = '/ptms/api/sso/v1/applications'
    __api_applications_v2_list = '/ptms/api/sso/v2/applications'
    __api_roles_list = '/ptms/api/sso/v2/applications/{}/roles'
    __api_roles_delete = '/ptms/api/sso/v2/applications/{}/roles/delete'
    __api_privileges_list = '/ptms/api/sso/v2/applications/{}/privileges'
    __api_users_list = '/ptms/api/sso/v1/users/query'
    __api_users = '/ptms/api/sso/v1/users'
    __api_users_password = '/ptms/api/sso/v1/users/password'
    __api_users_block = '/ptms/api/sso/v1/users/block'
    __api_users_unblock = '/ptms/api/sso/v1/users/unblock'
    __api_users_roles = '/ptms/api/sso/v1/users/roles'

    def __init__(self, auth: MPSIEMAuth, settings: Settings):
        ModuleInterface.__init__(self, auth, settings)
        LoggingHandler.__init__(self)
        self.__ms_session = auth.sessions['core']
        self.__ms_hostname = auth.creds.core_hostname
        self.__core_version = auth.get_core_version()
        self.__applications = {}
        self.__roles = {}
        self.__privileges = {}
        self.__users = {}
        self.__role_id = []
        self.__code_privileges = {}

    def get_applications_list(self) -> dict:
        """Получить информацию по приложениям, включая тенанты.

        :return: {'app_id': {'name': 'value', 'tenants': ['', '']}}
        """

        self.__applications.clear()
        self.log.debug(f'status=prepare, action=get_applications, msg="Try to get applications '
                       f'as {self.auth.creds.core_login}", hostname="{self.__ms_hostname}"')

        if int(self.__core_version.split('.')[0]) < 27:
            url = f'https://{self.__ms_hostname}:{self.__ms_port}{self.__api_applications_list}'
        else:
            self.log.debug(f'version={self.__core_version}')
            self.log.debug(f'session={self.__ms_session}')
            url = f'https://{self.__ms_hostname}:{self.__ms_port}{self.__api_applications_v2_list}'
        response = exec_request(self.__ms_session, url, method="GET", timeout=self.settings.connection_timeout).json()

        for i in response:
            app_id = i.get('id')
            app_name = i.get('name')
            app_tenants = i.get('tenantIds')
            if self.__applications.get(app_id) is None:
                self.__applications[app_id] = {}
            self.__applications[app_id] = {'name': app_name, 'tenants_ids': app_tenants}

        self.log.info(f'status=success, action=get_applications, msg="Got {len(self.__applications)} apps", '
                      f'hostname="{self.__ms_hostname}"')

        return self.__applications

    def get_users_list(self, filters=None) -> dict:
        """Получить список всех пользователей.

        :param filters: {"rolesIds": ["id", "id"], "authTypes": [1, 0],
            "ldapPoolNames": ["ldap_pool_name"], "statuses": ["active",
            "blocked"], "withoutRoles": True}
        :return: {"user_name": {"param1": "value"}}
        """

        # TODO Пользователи могут дублироваться в рамках разных сайтов. Сейчас не учитывается.

        self.__users.clear()
        self.log.debug(f'status=prepare, action=get_users_list, msg="Try to get users list as '
                       f'{self.auth.creds.core_login}", hostname="{self.__ms_hostname}"')
        self.log.warning(f'status=prepare, action=get_users_list, msg="Call high privileged operation with '
                         f'{self.auth.creds.core_login}", hostname="{self.__ms_hostname}"')

        params = {'authTypes': [1, 0],
                  'statuses': ['active', 'blocked'],
                  'withoutRoles': True}
        if filters is not None:
            params = filters

        url = f'https://{self.__ms_hostname}:{self.__ms_port}{self.__api_users_list}'
        response = exec_request(self.__ms_session,
                                url,
                                method='POST',
                                timeout=self.settings.connection_timeout,
                                json=params).json()

        # маршалинг, т.к. контракт может меняться
        for i in response.get('items'):
            user_id = i.get('id')
            user_name = i.get('userName')
            user_status = i.get('status')
            user_readonly = i.get('isReadOnly')
            user_site_id = i.get('siteId')
            user_auth_type = i.get('authType')
            user_ldap_aliases = i.get('ldapAliases')
            user_roles = i.get('roles')
            user_is_system = i.get('system')
            if len(user_ldap_aliases) == 1 and user_ldap_aliases[0] == '':
                user_ldap_aliases = []
            if user_roles is not None:
                reformatted_roles = {}
                for r in user_roles:
                    reformatted_roles = {'id': r.get('roleId'),
                                         'application_id': r.get('applicationId'),
                                         'tenant_id': r.get('tenantId')}
                user_roles = reformatted_roles

            if self.__users.get(user_name) is None:
                self.__users[user_name] = {}

            self.__users[user_name] = {'id': user_id,
                                       'status': user_status,
                                       'readonly': user_readonly,
                                       'site_id': user_site_id,
                                       'auth_type': user_auth_type,
                                       'ldap_aliases': user_ldap_aliases,
                                       'roles': user_roles,
                                       'system': user_is_system}
        self.log.info(f'status=success, action=get_applications, msg="Got {len(self.__users)} users", '
                      f'hostname="{self.__ms_hostname}"')

        return self.__users

    def get_user_info(self, user_name: str) -> dict:
        """Получить информацию по пользователю.

        :param user_name:
        :return: {"param1": "value", "param2": "value"}
        """

        if len(self.__users) == 0:
            self.get_users_list()

        return self.__users.get(user_name)

    def create_user(self, data: dict, password_generation: True) -> None:
        """Создать пользователя
        :param data: {"userName": str,
        "email": str or None,
        "authType": 0, # 0 - локальный, 1 - LDAP
        "ldapSyncEnabled": False,
        "status": "active" or "blocked",
        "passwordChange": False,
        "firstName": str or None,
        "lastName": str or None,
        "middleName": str or None,
        "phone": str or None,
        "position": str or None,
        "manager": str or None,
        "department": str or None,
        "password": str}
        :param password_generation # Генерация пароля для пользователя

        :return: None"""

        params = data

        if len(self.__users) == 0:
            self.get_users_list()

        if self.__users.get(params.get('userName')):
            self.log.error(f'status=failed, action=create_user, msg="Such a user already exists", '
                           f'hostname="{self.__ms_hostname}"')
            return

        if password_generation:
            url = f'https://{self.__ms_hostname}:{self.__ms_port}{self.__api_users_password}'
            password = exec_request(self.__ms_session,
                                    url,
                                    method="POST",
                                    timeout=self.settings.connection_timeout,
                                    headers=self.__headers).json()
            params['password'] = password['password']

        url = f'https://{self.__ms_hostname}:{self.__ms_port}{self.__api_users}'

        response = exec_request(self.__ms_session,
                                url,
                                method="POST",
                                timeout=self.settings.connection_timeout,
                                headers=self.__headers,
                                json=params).json()

        self.log.info(f'status=success, action=create_user, msg="User {params.get("userName")} (ID: {response["id"]}) '
                      f'created", hostname="{self.__ms_hostname}"')
        return response

    def update_user(self, data: dict) -> None:
        """Изменить пользователя :param data: {"userName": str,

            "email": str or None,
            "authType": 0, # 0 - локальный, 1 - LDAP
            "ldapSyncEnabled": False,
            "status": "active" or "blocked",
            "passwordChange": False,
            "firstName": str or None,
            "lastName": str or None,
            "middleName": str or None,
            "phone": str or None,
            "position": str or None,
            "manager": str or None,
            "department": str or None,
            "newPassword": str or None}

        :return: None
        """

        params = data

        if len(self.__users) == 0:
            self.get_users_list()

        if not self.__users.get(params.get('userName')):
            self.log.error(f'status=failed, action=create_user, msg="Such a user already exists", '
                           f'hostname="{self.__ms_hostname}"')
            return

        url = f'https://{self.__ms_hostname}:{self.__ms_port}{self.__api_users}/' \
              f'{self.__users.get(params.get("userName"))["id"]}'

        response = exec_request(self.__ms_session,
                                url,
                                method="PUT",
                                timeout=self.settings.connection_timeout,
                                headers=self.__headers,
                                json=params)

        self.log.info(f'status=success, action=create_user, msg="User {params.get("userName")} '
                      f'(ID: {self.__users.get(params.get("userName"))["id"]}) '
                      f'update", hostname="{self.__ms_hostname}"')

    def lock_user(self, user_name: str) -> None:
        """Заблокировать пользователя.
        :param user_name:

        :return: None
        """
        if len(self.__users) == 0:
            self.get_users_list()

        if not self.__users.get(user_name):
            self.log.error(f'status=failed, action=delete_user, msg="Such a user does not exist", '
                           f'hostname="{self.__ms_hostname}"')
            return
        else:
            if self.__users.get(user_name).get('status') == "blocked":
                self.log.error(f'status=failed, action=delete_user, msg="The user is already blocked", '
                               f'hostname="{self.__ms_hostname}"')
                return

        url = f'https://{self.__ms_hostname}:{self.__ms_port}{self.__api_users_block}'

        params = [self.__users.get(user_name).get("id")]

        response = exec_request(self.__ms_session,
                                url,
                                method="POST",
                                timeout=self.settings.connection_timeout,
                                headers=self.__headers,
                                json=params).json()

        self.log.debug(f'status=success, action=delete_user, msg="User {user_name}'
                       f'blocked", hostname="{self.__ms_hostname}"')
        return response

    def unlock_user(self, user_name: str) -> None:
        """Разблокировать пользователя.
        :param user_name:

        :return: None
        """
        if len(self.__users) == 0:
            self.get_users_list()

        if not self.__users.get(user_name):
            self.log.error(f'status=failed, action=recover_user, msg="Such a user does not exist", '
                           f'hostname="{self.__ms_hostname}"')
            return
        else:
            if self.__users.get(user_name).get('status') == "active":
                self.log.error(f'status=failed, action=recover_user, msg="The user is not blocked", '
                               f'hostname="{self.__ms_hostname}"')
                return

        url = f'https://{self.__ms_hostname}:{self.__ms_port}{self.__api_users_unblock}'

        params = [self.__users.get(user_name).get("id")]

        response = exec_request(self.__ms_session,
                                url,
                                method="POST",
                                timeout=self.settings.connection_timeout,
                                headers=self.__headers,
                                json=params).json()

        self.log.debug(f'status=success, action=recover_user, msg="User {user_name}'
                       f'unblocked", hostname="{self.__ms_hostname}"')

        return response

    def user_roles_update(self, user_name: str, roles: dict) -> None:
        """Назначение/изменение ролей пользователя.

        :param user_name:
        :param roles: {'idmgr': [role_name], 'mpx': [role_name], 'ptkb':
            [role_name]}

        :return: None
        """

        self.__role_id.clear()

        if len(self.__users) == 0:
            self.get_users_list()

        if not self.__users.get(user_name):
            self.log.error(f'status=failed, action=user_roles_update, msg="Such a user does not exist", '
                           f'hostname="{self.__ms_hostname}"')
            return

        if len(self.__roles) == 0:
            self.get_roles_list()

        for key, value in roles.items():
            for elem in value:
                if self.__roles.get(key).get(elem):
                    self.__role_id.append(self.__roles.get(key).get(elem).get('id'))
                else:
                    self.log.error(f'status=failed, action=user_roles_update, msg="The role {elem} does not exist", '
                                   f'hostname="{self.__ms_hostname}"')
                    return

        params = [{"userId": self.__users.get(user_name).get("id"), "rolesIds": self.__role_id}]

        url = f'https://{self.__ms_hostname}:{self.__ms_port}{self.__api_users_roles}'

        response = exec_request(self.__ms_session,
                                url,
                                method="PUT",
                                timeout=self.settings.connection_timeout,
                                headers=self.__headers,
                                json=params).json()

        self.log.debug(f'status=success, action=user_roles_update, msg="User {user_name}'
                       f'update roles", hostname="{self.__ms_hostname}"')
        return response

    def get_roles_list(self) -> dict:
        """Получить полный список ролей.

        :return: {'component': {'role_name': {'param1': 'value1'}}}
        """

        self.__roles.clear()
        self.log.debug(f'status=prepare, action=get_groups, msg="Try to get roles as {self.auth.creds.core_login}", '
                       f'hostname="{self.__ms_hostname}"')
        count = 0
        self.__get_roles(MPComponents.MS)
        count += len(self.__roles.get(MPComponents.MS))
        self.__get_roles(MPComponents.KB)
        count += len(self.__roles.get(MPComponents.KB))
        self.__get_roles(MPComponents.CORE)
        count += len(self.__roles.get(MPComponents.CORE))

        self.log.info(f'status=success, action=get_roles, msg="Got {count} roles", '
                      f'hostname="{self.__ms_hostname}"')

        return self.__roles

    def __get_roles(self, app_type: str):
        api_url = self.__api_roles_list.format(app_type)
        url = f'https://{self.__ms_hostname}:{self.__ms_port}{api_url}'
        response = exec_request(self.__ms_session, url, method='GET', timeout=self.settings.connection_timeout).json()

        self.__roles[app_type] = {}
        for i in response:
            role_id = i.get('id')
            role_name = i.get('name')
            role_privileges = i.get('privileges')
            if self.__roles[app_type].get(role_name) is None:
                self.__roles[app_type][role_name] = {}
            self.__roles[app_type][role_name] = {'id': role_id, 'privileges': role_privileges}

        self.log.debug(f'status=success, action=get_roles, msg="Got roles from {app_type}", '
                       f'hostname="{self.__ms_hostname}" roles="{self.__roles}"')

    def get_role_info(self, role_name: str, component: str) -> dict:
        """Получить информацию по конкретной роле
        :param role_name: Имя роли
        :param component: MPComponents

        :return: dict."""

        if len(self.__roles) == 0:
            self.get_roles_list()

        return self.__roles[component][role_name]

    def get_privileges_list(self) -> dict:
        """Получить полный список всех доступных в системе привилегий.

        :return: {'component': {'priv': 'name'}}
        """

        self.__privileges.clear()
        self.log.debug(f'status=prepare, action=get_groups, msg="Try to get privileges as '
                       f'{self.auth.creds.core_login}", hostname="{self.__ms_hostname}"')
        count = 0
        self.__get_privileges(MPComponents.MS)
        count += len(self.__privileges.get(MPComponents.MS))
        self.__get_privileges(MPComponents.KB)
        count += len(self.__privileges.get(MPComponents.KB))
        self.__get_privileges(MPComponents.CORE)
        count += len(self.__privileges.get(MPComponents.CORE))

        self.log.info(f'status=success, action=get_roles, msg="Got {count} privileges", '
                      f'hostname="{self.__ms_hostname}"')

        return self.__privileges

    def __get_privileges(self, app_type: str) -> None:
        """Парсинг ответа от сервера и заполнение привилегий.
        :param app_type: MPComponent

        :return: None
        """

        api_url = self.__api_privileges_list.format(app_type)
        url = f'https://{self.__ms_hostname}:{self.__ms_port}{api_url}'
        response = exec_request(self.__ms_session, url, method='GET', timeout=self.settings.connection_timeout).json()

        self.__privileges[app_type] = {}
        for i in response:
            for p in i.get('privileges', {}):
                priv_id = p.get('code')
                priv_name = p.get('name')
                self.__privileges[app_type][priv_id] = priv_name

        self.log.debug(f'status=success, action=get_roles, msg="Got privileges from {app_type}", '
                       f'hostname="{self.__ms_hostname}" privileges="{self.__privileges}"')

    def create_role(self, role_name: str, role_description: str, role_component: str, role_privileges: list) -> None:
        """Создание роли
        :param role_name: Имя роли
        :param role_component: MPComponents
        :param role_description: Описание роли
        :param role_privileges: привилегии роли

        :return: None."""

        if len(self.__roles) == 0:
            self.get_roles_list()

        if role_component not in self.__roles:
            self.log.error(f'status=failed, action=create_role, msg="The component {role_component} does not '
                           f'exist", hostname="{self.__ms_hostname}"')
            return

        if role_name in self.__roles.get(role_component):
            self.log.error(f'status=failed, action=create_role, msg="The role {role_name} exists"'
                           f', hostname="{self.__ms_hostname}"')
            return

        self.__code_privileges.clear()
        self.__get_code_privileges(role_component)

        privileges = []

        for elem in role_privileges:
            if self.__code_privileges.get(elem):
                privileges.append(self.__code_privileges.get(elem))
            else:
                self.log.error(f'status=failed, action=create_role, msg="The privilege {elem} does not exist", '
                               f'hostname="{self.__ms_hostname}"')
                return

        api_url = self.__api_roles_list.format(role_component)
        url = f'https://{self.__ms_hostname}:{self.__ms_port}{api_url}'

        params = {"description": role_description, "name": role_name, "privileges": privileges}

        response = exec_request(self.__ms_session,
                                url,
                                method="POST",
                                timeout=self.settings.connection_timeout,
                                headers=self.__headers,
                                json=params).json()

        self.log.info(f'status=success, action=create_role, msg="Role {role_name} (ID: {response}) '
                      f'created", hostname="{self.__ms_hostname}"')

    def __get_code_privileges(self, app_type: str) -> dict:
        api_url = self.__api_privileges_list.format(app_type)
        url = f'https://{self.__ms_hostname}:{self.__ms_port}{api_url}'
        response = exec_request(self.__ms_session, url, method='GET', timeout=self.settings.connection_timeout).json()

        if app_type == MPComponents.MS:
            for elem in response:
                for i in elem.get('privileges'):
                    self.__code_privileges[i.get('name')] = i.get('code')

        elif app_type == MPComponents.KB:
            for elem in response:
                for i in elem.get('groups'):
                    for e in i.get('privileges'):
                        self.__code_privileges[e.get('name')] = e.get('code')
                for j in elem.get('privileges'):
                    if j.get('code') not in ['kb.access.allow']:
                        self.__code_privileges[j.get('name')] = j.get('code')

        elif app_type == MPComponents.CORE:
            for elem in response:
                for i in elem.get('privileges'):
                    if i.get('code') not in ['access.allow', 'dashboards']:
                        self.__code_privileges[i.get('name')] = i.get('code')

        return self.__code_privileges

    def update_role(self, role_name: str, role_new_name: None, role_description: str,
                    role_component: str, role_privileges: list) -> None:
        """Редактирование роли
        :param role_name: Имя роли
        :param role_new_name: Новое имя роли
        :param role_component: MPComponents
        :param role_description: Описание роли
        :param role_privileges: Присваемые привилегии (имена)

         :return: None."""

        if len(self.__roles) == 0:
            self.get_roles_list()

        if role_component not in self.__roles:
            self.log.error(f'status=failed, action=update_role, msg="The component {role_component} does not '
                           f'exist", hostname="{self.__ms_hostname}"')
            return

        if role_name not in self.__roles.get(role_component):
            self.log.error(f'status=failed, action=update_role, msg="The role {role_name} does not exist"'
                           f', hostname="{self.__ms_hostname}"')
            return

        self.__code_privileges.clear()
        self.__get_code_privileges(role_component)

        privileges = []

        for elem in role_privileges:
            if self.__code_privileges.get(elem):
                privileges.append(self.__code_privileges.get(elem))
            else:
                self.log.error(f'status=failed, action=update_role, msg="The privilege {elem} does not exist", '
                               f'hostname="{self.__ms_hostname}"')
                return

        api_url = self.__api_roles_list.format(role_component)
        url = f'https://{self.__ms_hostname}:{self.__ms_port}{api_url}'

        params = [{"description": role_description, "name": role_new_name if role_new_name else role_name,
                   "privileges": privileges, "type": "Custom", "id": self.__roles[role_component][role_name]['id']}]

        response = exec_request(self.__ms_session,
                                url,
                                method="PUT",
                                timeout=self.settings.connection_timeout,
                                headers=self.__headers,
                                json=params)

        self.log.info(f'status=success, action=update_role, msg="Role {role_name} '
                      f'(ID: {self.__roles[role_component][role_name]["id"]}) update"'
                      f', hostname="{self.__ms_hostname}"')

    def delete_role(self, role_name: str, role_component: str) -> None:
        """Удаление роли
        :param role_name: Имя роли
        :param role_component: MPComponents

        :return: None."""
        if len(self.__roles) == 0:
            self.get_roles_list()

        if role_component not in self.__roles:
            self.log.error(f'status=failed, action=delete_role, msg="The component {role_component} does not '
                           f'exist", hostname="{self.__ms_hostname}"')
            return

        if role_name not in self.__roles.get(role_component):
            self.log.error(f'status=failed, action=delete_role, msg="The role {role_name} does not exist"'
                           f', hostname="{self.__ms_hostname}"')
            return

        api_url = self.__api_roles_delete.format(role_component)
        url = f'https://{self.__ms_hostname}:{self.__ms_port}{api_url}'

        params = [self.__roles[role_component][role_name]['id']]

        response = exec_request(self.__ms_session,
                                url,
                                method="DELETE",
                                timeout=self.settings.connection_timeout,
                                headers=self.__headers,
                                json=params)

        self.log.info(f'status=success, action=delete_role, msg="Role {role_name} '
                      f'(ID: {self.__roles[role_component][role_name]["id"]}) deleted"'
                      f', hostname="{self.__ms_hostname}"')

    def close(self):
        if self.__ms_session is not None:
            self.__ms_session.close()
