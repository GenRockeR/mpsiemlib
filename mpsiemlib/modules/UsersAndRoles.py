from typing import Any

from mpsiemlib.common import (
    AuthError,
    LoggingHandler,
    ModuleInterface,
    MPComponents,
    MPSIEMAuth,
    Settings,
    exec_request,
)


class UsersAndRoles(ModuleInterface, LoggingHandler):
    """Модуль управления пользователями и ролями (SSO/IAM).

    Контракты (``docs/api/api_contracts_documentation.md``):

    - ``ClientRoles.yaml``   - ``/ptms/api/sso/v2`` (applications, roles,
      privileges, sites, tenants)
    - ``ClientInfo.yaml``    - ``/ptms/api/sso/v1`` (applications - для 26.x)
    - ``UserManagement``     - ``/ptms/api/sso/v1/users`` (query/create/update,
      block/unblock, roles, password)

    MP SIEM 26.x отдаёт список приложений через ``GET /ptms/api/sso/v1/
    applications``; с 27.x - через ``/ptms/api/sso/v2/applications`` (в v2
    добавлены ``tenantIds``/``roleManagementEnabled``).
    """

    __ms_port = 3334

    __api_applications_v1 = "/ptms/api/sso/v1/applications"
    __api_applications_v2 = "/ptms/api/sso/v2/applications"
    __api_users_query = "/ptms/api/sso/v1/users/query"
    __api_users = "/ptms/api/sso/v1/users"
    __api_users_password = "/ptms/api/sso/v1/users/password"
    __api_users_block = "/ptms/api/sso/v1/users/block"
    __api_users_unblock = "/ptms/api/sso/v1/users/unblock"
    __api_users_roles = "/ptms/api/sso/v1/users/roles"

    def __init__(self, auth: MPSIEMAuth, settings: Settings) -> None:
        ModuleInterface.__init__(self, auth, settings)
        LoggingHandler.__init__(self)
        if auth.sessions is None or "core" not in auth.sessions:
            raise AuthError("Core session is not initialized")
        creds = auth.get_creds()
        if creds is None or creds.core_hostname is None:
            raise AuthError("Core hostname is not set")
        self.__ms_session = auth.sessions["core"]
        self.__ms_hostname = creds.core_hostname
        self.__core_version = auth.get_core_version()
        # Релиз ядра (MAJOR, MINOR): "27.6.40521" -> (27, 6).
        version_parts = self.__core_version.split(".")
        self.__core_release: tuple[int, int] = (
            int(version_parts[0]),
            int(version_parts[1]),
        )
        self.__applications: dict[str, dict[str, Any]] = {}
        self.__roles: dict[str, dict[str, dict[str, Any]]] = {}
        self.__privileges: dict[str, dict[str, str]] = {}
        self.__users: dict[str, dict[str, Any]] = {}
        self.__users_page_size = 1000
        self.log.debug(
            'status=success, action=prepare, msg="UsersAndRoles Module init"'
        )

    # ------------------------------------------------------------------ #
    # Приложения
    # ------------------------------------------------------------------ #

    def get_applications_list(self) -> dict[str, dict[str, Any]]:
        """Список зарегистрированных приложений (GetClients).

        27.x: ``GET /ptms/api/sso/v2/applications``; 26.x:
        ``GET /ptms/api/sso/v1/applications``.

        :return: {app_id: {"name", "type", "tenants_ids", "role_management_enabled"}}
        """
        self.log.debug(
            "status=prepare, action=get_applications, "
            'msg="Try to get applications", '
            f"hostname={self.__ms_hostname!r}"
        )

        if self.__core_release >= (27, 0):
            api_path = self.__api_applications_v2
        else:
            api_path = self.__api_applications_v1

        url = f"https://{self.__ms_hostname}:{self.__ms_port}{api_path}"
        response: list[dict[str, Any]] = exec_request(
            self.__ms_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
        ).json()

        self.__applications = {
            item["id"]: {
                "name": item.get("name"),
                "type": item.get("type"),
                "tenants_ids": item.get("tenantIds"),
                "role_management_enabled": item.get("roleManagementEnabled"),
            }
            for item in response
        }

        self.log.info(
            "status=success, action=get_applications, "
            f'msg="Got {len(self.__applications)} apps", '
            f"hostname={self.__ms_hostname!r}"
        )
        return {k: dict(v) for k, v in self.__applications.items()}

    # ------------------------------------------------------------------ #
    # Пользователи
    # ------------------------------------------------------------------ #

    def get_users_list(
        self, filters: dict[str, Any] | None = None
    ) -> dict[str, dict[str, Any]]:
        """Список пользователей (QueryUsers, ``POST /ptms/api/sso/v1/users/query``).

        Ответ кэшируется полностью (контракт может меняться); ключ результата -
        логин пользователя. Пользователи с одинаковым логином на разных сайтах
        схлопываются в одну запись (site не учитывается).

        :param filters: ``GetUsersFilter`` - {"rolesIds", "authTypes",
            "statuses", "withoutRoles", "siteId", "applicationId"}; None -
            взять активных и заблокированных локальных/LDAP без ролей
        :return: {user_name: {"id", "status", "email", "first_name",
            "last_name", "readonly", "site_id", "auth_type", "ldap_aliases",
            "roles", "system"}}
        """
        self.log.debug(
            "status=prepare, action=get_users_list, "
            'msg="Try to get users list (high privileged)", '
            f"hostname={self.__ms_hostname!r}"
        )

        params: dict[str, Any] = {
            "authTypes": [1, 0],
            "statuses": ["active", "blocked"],
            "withoutRoles": True,
        }
        if filters is not None:
            params = filters

        url = f"https://{self.__ms_hostname}:{self.__ms_port}{self.__api_users_query}"

        self.__users = {}
        offset = 0
        while True:
            # offset/limit - query-параметры (QueryUsers), фильтр - в теле
            response: dict[str, Any] = exec_request(
                self.__ms_session,
                url,
                method="POST",
                timeout=self.settings.connection_timeout,
                params={"offset": offset, "limit": self.__users_page_size},
                json=params,
            ).json()

            items = response.get("items") or []
            for item in items:
                ldap_aliases = item.get("ldapAliases") or []
                if ldap_aliases == [""]:
                    ldap_aliases = []
                roles = [
                    {
                        "id": r.get("roleId"),
                        "application_id": r.get("applicationId"),
                        "tenant_id": r.get("tenantId"),
                    }
                    for r in (item.get("roles") or [])
                ]

                self.__users[item.get("userName")] = {
                    "id": item.get("id"),
                    "status": item.get("status"),
                    "email": item.get("email"),
                    "first_name": item.get("firstName"),
                    "last_name": item.get("lastName"),
                    "readonly": item.get("isReadOnly"),
                    "site_id": item.get("siteId"),
                    "auth_type": item.get("authType"),
                    "ldap_aliases": ldap_aliases,
                    "roles": roles,
                    "system": item.get("system"),
                }

            if len(items) < self.__users_page_size:
                break
            offset += len(items)

        self.log.info(
            "status=success, action=get_users_list, "
            f'msg="Got {len(self.__users)} users", '
            f"hostname={self.__ms_hostname!r}"
        )
        return {k: dict(v) for k, v in self.__users.items()}

    def get_user_info(self, user_name: str) -> dict[str, Any] | None:
        """Информация о пользователе по логину (из кэша ``get_users_list``).

        :param user_name: Логин пользователя
        :return: описание пользователя либо None, если он не найден
        """
        if not self.__users:
            self.get_users_list()
        info = self.__users.get(user_name)
        return dict(info) if info is not None else None

    def create_user(
        self, data: dict[str, Any], password_generation: bool = True
    ) -> str | None:
        """Создать пользователя (CreateUser, ``POST /ptms/api/sso/v1/users``).

        :param data: ``CreateUserModel`` - {"userName", "email", "authType",
            "ldapSyncEnabled", "status", "passwordChange", "firstName",
            "lastName", "middleName", "phone", "position", "manager",
            "department", "password"}
        :param password_generation: сгенерировать пароль (GeneratePassword) и
            подставить в ``data["password"]`` вместо переданного
        :return: id созданного пользователя либо None, если пользователь с
            таким логином уже существует
        """
        if not self.__users:
            self.get_users_list()

        user_name = data.get("userName")
        if not user_name:
            raise ValueError("data must contain 'userName'")
        user_name = str(user_name)
        if self.__users.get(user_name):
            self.log.error(
                "status=failed, action=create_user, "
                f'msg="User {user_name!r} already exists", '
                f"hostname={self.__ms_hostname!r}"
            )
            return None

        params = dict(data)
        if password_generation:
            password = self.__generate_password()
            params["password"] = password

        url = f"https://{self.__ms_hostname}:{self.__ms_port}{self.__api_users}"
        response: dict[str, Any] = exec_request(
            self.__ms_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
            json=params,
        ).json()

        user_id = response.get("id")
        # прогреваем кэш, чтобы последующий create_user того же логина
        # отбился локальной проверкой (сервер на дубль отвечает 400)
        self.__users[user_name] = {
            "id": user_id,
            "status": data.get("status", "active"),
            "email": data.get("email"),
            "first_name": data.get("firstName"),
            "last_name": data.get("lastName"),
        }
        self.log.info(
            "status=success, action=create_user, "
            f'msg="User {user_name!r} (ID: {user_id}) created", '
            f"hostname={self.__ms_hostname!r}"
        )
        return user_id

    def update_user(self, data: dict[str, Any]) -> None:
        """Изменить пользователя (UpdateUser, ``PUT /ptms/api/sso/v1/users/{id}``).

        :param data: ``UpdateUserModel`` (обязателен ``userName`` для поиска
            id); поддерживаются те же поля, что и в ``create_user``, плюс
            ``newPassword``
        """
        if not self.__users:
            self.get_users_list()

        user_name = data.get("userName")
        if not user_name:
            raise ValueError("data must contain 'userName'")
        user_name = str(user_name)
        info = self.__users.get(user_name)
        if info is None:
            self.log.error(
                "status=failed, action=update_user, "
                f'msg="User {user_name!r} does not exist", '
                f"hostname={self.__ms_hostname!r}"
            )
            return

        user_id = info["id"]
        url = (
            f"https://{self.__ms_hostname}:{self.__ms_port}{self.__api_users}/{user_id}"
        )
        exec_request(
            self.__ms_session,
            url,
            method="PUT",
            timeout=self.settings.connection_timeout,
            json=data,
        )

        self.log.info(
            "status=success, action=update_user, "
            f'msg="User {user_name!r} (ID: {user_id}) updated", '
            f"hostname={self.__ms_hostname!r}"
        )

    def lock_user(self, user_name: str) -> bool:
        """Заблокировать пользователя (BlockUsers).

        :param user_name: Логин пользователя
        :return: True при успехе, False если пользователь не найден или уже
            заблокирован
        """
        return self.__change_block_state(
            user_name, blocked=True, api_path=self.__api_users_block
        )

    def unlock_user(self, user_name: str) -> bool:
        """Разблокировать пользователя (UnblockUsers).

        :param user_name: Логин пользователя
        :return: True при успехе, False если пользователь не найден или не
            заблокирован
        """
        return self.__change_block_state(
            user_name, blocked=False, api_path=self.__api_users_unblock
        )

    def user_roles_update(self, user_name: str, roles: dict[str, list[str]]) -> bool:
        """Назначить роли пользователя (UpdateUsersRoles).

        Полностью заменяет набор ролей пользователя ролями из ``roles``.

        :param user_name: Логин пользователя
        :param roles: {application_id: [role_name, ...]}, ключи - значения
            ``MPComponents`` (``idmgr``/``ptkb``/``mpx``)
        :return: True при успехе; False если пользователь или роль не найдены
        """
        if not self.__users:
            self.get_users_list()

        info = self.__users.get(user_name)
        if info is None:
            self.log.error(
                "status=failed, action=user_roles_update, "
                f'msg="User {user_name!r} does not exist", '
                f"hostname={self.__ms_hostname!r}"
            )
            return False

        if not self.__roles:
            self.get_roles_list()

        role_ids: list[str] = []
        for application_id, role_names in roles.items():
            app_roles = self.__roles.get(application_id)
            if app_roles is None:
                self.log.error(
                    "status=failed, action=user_roles_update, "
                    f'msg="Application {application_id!r} does not exist", '
                    f"hostname={self.__ms_hostname!r}"
                )
                return False
            for role_name in role_names:
                role = app_roles.get(role_name)
                if role is None:
                    self.log.error(
                        "status=failed, action=user_roles_update, "
                        f'msg="Role {role_name!r} does not exist", '
                        f"hostname={self.__ms_hostname!r}"
                    )
                    return False
                role_ids.append(role["id"])

        params = [{"userId": info["id"], "rolesIds": role_ids}]
        url = f"https://{self.__ms_hostname}:{self.__ms_port}{self.__api_users_roles}"
        exec_request(
            self.__ms_session,
            url,
            method="PUT",
            timeout=self.settings.connection_timeout,
            json=params,
        )

        self.log.info(
            "status=success, action=user_roles_update, "
            f'msg="User {user_name!r} roles updated", '
            f"hostname={self.__ms_hostname!r}"
        )
        return True

    # ------------------------------------------------------------------ #
    # Роли
    # ------------------------------------------------------------------ #

    def get_roles_list(self) -> dict[str, dict[str, dict[str, Any]]]:
        """Полный список ролей по компонентам (GetClientRoles, v2).

        :return: {application_id: {role_name: {"id", "description", "type",
            "privileges"}}} для MS/KB/CORE
        """
        self.log.debug(
            "status=prepare, action=get_roles, "
            'msg="Try to get roles", '
            f"hostname={self.__ms_hostname!r}"
        )
        self.__roles = {}
        count = 0
        for application in (MPComponents.MS, MPComponents.KB, MPComponents.CORE):
            roles = self.__get_roles(application)
            count += len(roles)

        self.log.info(
            "status=success, action=get_roles, "
            f'msg="Got {count} roles", '
            f"hostname={self.__ms_hostname!r}"
        )
        return self.__roles

    def get_role_info(self, role_name: str, component: str) -> dict[str, Any] | None:
        """Информация о роли по имени и компоненту (из кэша ``get_roles_list``).

        :param role_name: имя роли
        :param component: приложение (``MPComponents``)
        :return: описание роли либо None, если компонент/роль не найдены
        """
        if not self.__roles:
            self.get_roles_list()
        role = self.__roles.get(component, {}).get(role_name)
        return dict(role) if role is not None else None

    def create_role(
        self,
        role_name: str,
        role_description: str,
        role_component: str,
        role_privileges: list[str],
    ) -> str | None:
        """Создать роль приложения (CreateRole).

        :param role_name: имя роли
        :param role_description: описание роли
        :param role_component: приложение (``MPComponents``)
        :param role_privileges: имена привилегий (резолвятся в коды через
            ``get_privileges_list``)
        :return: id созданной роли либо None (роль/привилегии не валидны)
        """
        if not self.__roles:
            self.get_roles_list()

        if role_component not in self.__roles:
            self.log.error(
                "status=failed, action=create_role, "
                f'msg="Component {role_component!r} does not exist", '
                f"hostname={self.__ms_hostname!r}"
            )
            return None

        if role_name in self.__roles.get(role_component, {}):
            self.log.error(
                "status=failed, action=create_role, "
                f'msg="Role {role_name!r} already exists", '
                f"hostname={self.__ms_hostname!r}"
            )
            return None

        privileges = self.__resolve_privilege_codes(role_component, role_privileges)
        if privileges is None:
            self.log.error(
                "status=failed, action=create_role, "
                'msg="Some privileges are invalid", '
                f"hostname={self.__ms_hostname!r}"
            )
            return None

        url = (
            f"https://{self.__ms_hostname}:{self.__ms_port}"
            f"{self.__api_applications_v2}/{role_component}/roles"
        )
        params = {
            "name": role_name,
            "description": role_description,
            "privileges": privileges,
        }
        role_id = exec_request(
            self.__ms_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
            json=params,
        ).json()

        self.log.info(
            "status=success, action=create_role, "
            f'msg="Role {role_name!r} created", '
            f"hostname={self.__ms_hostname!r}"
        )
        return str(role_id)

    def update_role(
        self,
        role_name: str,
        role_new_name: str | None,
        role_description: str,
        role_component: str,
        role_privileges: list[str],
    ) -> bool:
        """Редактировать роль приложения (EditRoles).

        :param role_name: Текущее имя роли
        :param role_new_name: новое имя роли (None - оставить текущее)
        :param role_description: описание роли
        :param role_component: приложение (``MPComponents``)
        :param role_privileges: имена привилегий (резолвятся в коды через
            ``get_privileges_list``)
        :return: True при успехе; False если компонент/роль/привилегии не валидны
        """
        if not self.__roles:
            self.get_roles_list()

        app_roles = self.__roles.get(role_component)
        if app_roles is None:
            self.log.error(
                "status=failed, action=update_role, "
                f'msg="Component {role_component!r} does not exist", '
                f"hostname={self.__ms_hostname!r}"
            )
            return False

        role = app_roles.get(role_name)
        if role is None:
            self.log.error(
                "status=failed, action=update_role, "
                f'msg="Role {role_name!r} does not exist", '
                f"hostname={self.__ms_hostname!r}"
            )
            return False

        privileges = self.__resolve_privilege_codes(role_component, role_privileges)
        if privileges is None:
            self.log.error(
                "status=failed, action=update_role, "
                'msg="Some privileges are invalid", '
                f"hostname={self.__ms_hostname!r}"
            )
            return False

        url = (
            f"https://{self.__ms_hostname}:{self.__ms_port}"
            f"{self.__api_applications_v2}/{role_component}/roles"
        )
        params = [
            {
                "id": role["id"],
                "name": role_new_name if role_new_name else role_name,
                "description": role_description,
                "privileges": privileges,
            }
        ]
        exec_request(
            self.__ms_session,
            url,
            method="PUT",
            timeout=self.settings.connection_timeout,
            json=params,
        )

        self.log.info(
            "status=success, action=update_role, "
            f'msg="Role {role_name!r} updated", '
            f"hostname={self.__ms_hostname!r}"
        )
        return True

    def delete_role(self, role_name: str, role_component: str) -> bool:
        """Удалить роль приложения (DeleteClientRoles).

        :param role_name: имя роли
        :param role_component: приложение (``MPComponents``)
        :return: True при успехе; False если компонент/роль не найдены
        """
        if not self.__roles:
            self.get_roles_list()

        app_roles = self.__roles.get(role_component)
        if app_roles is None:
            self.log.error(
                "status=failed, action=delete_role, "
                f'msg="Component {role_component!r} does not exist", '
                f"hostname={self.__ms_hostname!r}"
            )
            return False

        role = app_roles.get(role_name)
        if role is None:
            self.log.error(
                "status=failed, action=delete_role, "
                f'msg="Role {role_name!r} does not exist", '
                f"hostname={self.__ms_hostname!r}"
            )
            return False

        url = (
            f"https://{self.__ms_hostname}:{self.__ms_port}"
            f"{self.__api_applications_v2}/{role_component}/roles/delete"
        )
        exec_request(
            self.__ms_session,
            url,
            method="DELETE",
            timeout=self.settings.connection_timeout,
            json=[role["id"]],
        )

        self.log.info(
            "status=success, action=delete_role, "
            f'msg="Role {role_name!r} deleted", '
            f"hostname={self.__ms_hostname!r}"
        )
        return True

    # ------------------------------------------------------------------ #
    # Привилегии
    # ------------------------------------------------------------------ #

    def get_privileges_list(self) -> dict[str, dict[str, str]]:
        """Полный список доступных привилегий (GetClientPrivileges, v2).

        Дерево ``PrivilegeGroupInfo`` (группы + привилегии, рекурсивно)
        разворачивается в плоский словарь {код: имя}.

        :return: {application_id: {privilege_code: privilege_name}} для
            MS/KB/CORE
        """
        self.log.debug(
            "status=prepare, action=get_privileges, "
            'msg="Try to get privileges", '
            f"hostname={self.__ms_hostname!r}"
        )
        self.__privileges = {}
        count = 0
        for application in (MPComponents.MS, MPComponents.KB, MPComponents.CORE):
            privileges = self.__get_privileges(application)
            count += len(privileges)

        self.log.info(
            "status=success, action=get_privileges, "
            f'msg="Got {count} privileges", '
            f"hostname={self.__ms_hostname!r}"
        )
        return self.__privileges

    # ------------------------------------------------------------------ #
    # Вспомогательное
    # ------------------------------------------------------------------ #

    def __get_roles(self, application_id: str) -> dict[str, dict[str, Any]]:
        """Запросить и закэшировать роли одного приложения (GetClientRoles)."""
        url = (
            f"https://{self.__ms_hostname}:{self.__ms_port}"
            f"{self.__api_applications_v2}/{application_id}/roles"
        )
        response: list[dict[str, Any]] = exec_request(
            self.__ms_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
        ).json()

        app_roles: dict[str, dict[str, Any]] = {}
        for item in response:
            app_roles[item["name"]] = {
                "id": item.get("id"),
                "description": item.get("description"),
                "type": item.get("type"),
                "privileges": item.get("privileges"),
            }
        self.__roles[application_id] = app_roles

        self.log.debug(
            "status=success, action=get_roles, "
            f'msg="Got {len(app_roles)} roles from {application_id!r}", '
            f"hostname={self.__ms_hostname!r}"
        )
        return app_roles

    def __get_privileges(self, application_id: str) -> dict[str, str]:
        """Запросить и закэшировать привилегии одного приложения (GetClientPrivileges).

        Обходит дерево групп рекурсивно; скрытые (``hidden``) привилегии не
        отфильтровываются - фильтрует только вызывающий код при подборе кодов.
        """
        url = (
            f"https://{self.__ms_hostname}:{self.__ms_port}"
            f"{self.__api_applications_v2}/{application_id}/privileges"
        )
        response: list[dict[str, Any]] = exec_request(
            self.__ms_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
        ).json()

        privileges_map: dict[str, str] = {}
        for item in response:
            self.__collect_privileges(item, privileges_map)

        self.__privileges[application_id] = privileges_map

        self.log.debug(
            "status=success, action=get_privileges, "
            f'msg="Got {len(privileges_map)} privileges from {application_id!r}", '
            f"hostname={self.__ms_hostname!r}"
        )
        return privileges_map

    @staticmethod
    def __collect_privileges(group: dict[str, Any], into: dict[str, str]) -> None:
        """Собрать {code: name} из группы привилегий рекурсивно."""
        for privilege in group.get("privileges") or []:
            code = privilege.get("code")
            if code is not None:
                into[code] = privilege.get("name")
        for sub_group in group.get("groups") or []:
            UsersAndRoles.__collect_privileges(sub_group, into)

    def __resolve_privilege_codes(
        self, application_id: str, privilege_names: list[str]
    ) -> list[str] | None:
        """Имена привилегий -> их коды по кэшу ``get_privileges_list``.

        :return: Список кодов либо None, если хотя бы одна привилегия не
            найдена в приложении
        """
        name_to_code = {
            name: code for code, name in self.__get_privileges(application_id).items()
        }
        codes: list[str] = []
        for name in privilege_names:
            code = name_to_code.get(name)
            if code is None:
                self.log.error(
                    "status=failed, action=resolve_privileges, "
                    f'msg="Privilege {name!r} not found in {application_id!r}", '
                    f"hostname={self.__ms_hostname!r}"
                )
                return None
            codes.append(code)
        return codes

    def __change_block_state(
        self, user_name: str, blocked: bool, api_path: str
    ) -> bool:
        """Блокировка/разблокировка пользователя (BlockUsers/UnblockUsers)."""
        action = "lock_user" if blocked else "unlock_user"
        if not self.__users:
            self.get_users_list()

        info = self.__users.get(user_name)
        if info is None:
            self.log.error(
                f"status=failed, action={action}, "
                f'msg="User {user_name!r} does not exist", '
                f"hostname={self.__ms_hostname!r}"
            )
            return False

        target_status = "blocked" if blocked else "active"
        if info.get("status") == target_status:
            self.log.error(
                f"status=failed, action={action}, "
                f'msg="User {user_name!r} already {target_status}", '
                f"hostname={self.__ms_hostname!r}"
            )
            return False

        url = f"https://{self.__ms_hostname}:{self.__ms_port}{api_path}"
        exec_request(
            self.__ms_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
            json=[info["id"]],
        )

        self.log.info(
            f"status=success, action={action}, "
            f'msg="User {user_name!r} {target_status}", '
            f"hostname={self.__ms_hostname!r}"
        )
        return True

    def __generate_password(self) -> str:
        """Сгенерировать пароль (GeneratePassword, ``POST /users/password``)."""
        url = (
            f"https://{self.__ms_hostname}:{self.__ms_port}{self.__api_users_password}"
        )
        response: dict[str, Any] = exec_request(
            self.__ms_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
        ).json()
        return str(response.get("password"))

    def close(self) -> None:
        if self.__ms_session is not None:
            self.__ms_session.close()
