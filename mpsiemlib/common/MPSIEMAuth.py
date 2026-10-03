from __future__ import annotations

import html
import re
from typing import Any

import requests
from lxml import html as lxml_html  # type: ignore[import-untyped]
from requests import RequestException

from .BaseFunctions import exec_request
from .Interfaces import (
    AuthInterface,
    AuthType,
    Creds,
    LoggingHandler,
    MPComponents,
    Settings,
    StorageVersion,
)

MAX_AUTH_FORM_ROUNDS = 10


class AuthError(RuntimeError):
    """Базовое исключение для всех ошибок аутентификации/подключения."""


class MPSIEMAuth(AuthInterface, LoggingHandler):
    """Аутентификация на компонентах MP, если требуется. Получение текущий
    версии компонент.

    :no-index:
    """

    __component: str

    # AuthType. (0 - Local, 1 - LDAP, 2 - SECRET, 3 - PAT_TOKEN)
    __auth_type: int | None = None

    __ms_port = 3334
    __api_ms_authorize = "/connect/authorize"
    __token_uri = "/connect/token"

    __api_core_auth_login_page = "/ui/login"
    __api_core_auth_form_page = "/account/login?returnUrl=/#/authorization/landing"
    __api_core_check_page = "/api/deployment_configuration/v1/system_info"

    __kb_port = 8091
    __api_kb_auth_login_page = "/account/login"
    __api_kb_signin = "/signin-oidc"
    __api_kb_check_page = "/api-studio/aboutSystem"
    __api_kb_db_list = "/api-studio/content-database-selector/content-databases"

    __siem_port = ""
    __api_siem_check_page = ""

    __storage_port = 9200
    __api_storage_check_page = "/_nodes"

    def __init__(self, creds: Creds, settings: Settings) -> None:
        AuthInterface.__init__(self, creds, settings)
        LoggingHandler.__init__(self)
        self.__session: requests.Session | None = None
        self.__is_connected = False
        self.__component = MPComponents.CORE
        self.__storage_version: str | None = None
        self.__core_version: str | None = None
        self.__kb_version: str | None = None
        self.sessions: dict[str, requests.Session] | None = None
        self.__auth_type = creds.core_auth_type

    def get_token(self) -> str | None:
        """Получить Bearer-токен для REST-API.

        - `PAT_TOKEN` - токен из `creds.pat_token`;
        - `SECRET` - `grant_type=password` с `client_secret` (обязателен);
        - `LOCAL`/`LDAP` - тот же grant, но только если `client_secret` задан.
          Cookie-сессии UI достаточно для Core-API, но не для API только под
          Bearer (`/ptms/api/sso/*` и `licensing/v4` на :3334) - без токена они
          отвечают 401.
        """
        if self.__auth_type == AuthType.PAT_TOKEN:
            if self.creds.pat_token is None:
                raise AuthError(
                    f'hostname="{self.creds.core_hostname}", status=failed, action=auth, '
                    f'msg="PAT_TOKEN is empty"'
                )
            return self.creds.pat_token

        if self.__auth_type in (
            AuthType.SECRET,
            AuthType.LOCAL,
            AuthType.LDAP,
        ):
            # SECRET обязан иметь client_secret (проверяется в check_missing_envs);
            # LOCAL/LDAP получают токен только если секрет задан явно, иначе
            # остаёмся только на cookie-сессии (Core-API), вернув None.
            if self.__auth_type != AuthType.SECRET and self.creds.client_secret is None:
                return None
            url = (
                f"https://{self.creds.core_hostname}:{self.__ms_port}{self.__token_uri}"
            )
            payload = {
                "grant_type": "password",
                "client_id": "mpx",
                "client_secret": self.creds.client_secret,
                "scope": "authorization offline_access mpx.api ptkb.api idmgr.api",
                "response_type": "code id_token token",
                "username": self.creds.core_login,
                "password": self.creds.core_pass,
            }
            try:
                response = requests.post(
                    url,
                    data=payload,
                    verify=False,
                    timeout=self.settings.connection_timeout,
                )
                response.raise_for_status()
            except RequestException as err:
                self.log.error(
                    f"hostname={self.creds.core_hostname!r}, status=failed, "
                    f"action=get_token, msg={err!r}"
                )
                raise AuthError(f"Can not get token: {err}") from err
            token = response.json().get("access_token")
            if token is None:
                raise AuthError("Token endpoint response has no access_token")
            return str(token)

        return None

    def set_auth_header(self, token: str | None) -> None:
        if token is None:
            return
        if self.__session is None:
            raise AuthError("Session is not initialized")
        self.__session.headers.update({"Authorization": f"Bearer {token}"})

    def connect(self, component: str, creds: Creds | None = None) -> requests.Session:
        """Подключение к выбранным компонентам :param component: Компонент для
        подключения Interfaces.MPComponents :param creds: креды для подключения
        Interfaces.Creds.

        :return: session
        """
        if creds is not None:
            self.creds = creds
            self.__auth_type = creds.core_auth_type
        self.__session = requests.Session()
        self.__session.verify = False
        self.__component = component

        if component in (MPComponents.CORE, MPComponents.MS):
            self.__core_try_connect()
        elif component == MPComponents.SIEM:
            self.__siem_try_connect()
        elif component == MPComponents.STORAGE:
            self.__storage_try_connect()
        elif component == MPComponents.KB:
            self.__kb_try_connect()
        else:
            raise NotImplementedError(f"Unsupported component for Auth {component!r}")

        self.set_auth_header(token=self.get_token())

        if self.__session is None:
            raise AuthError("Session was not created")
        return self.__session

    def disconnect(self) -> None:
        """Очистка сессии."""
        # TODO logout in MP CORE

        if self.__session is not None:
            self.__session.close()
        self.__is_connected = False
        self.__session = None

    def get_session(self) -> requests.Session:
        if not self.__is_connected or self.__session is None:
            self.connect(self.__component, self.creds)
        if self.__session is None:
            raise AuthError("Session was not created")
        return self.__session

    def get_component(self) -> str:
        return self.__component

    def get_creds(self) -> Creds:
        return self.creds

    def get_core_version(self) -> str:
        """Текущая версия Core.

        :return: StorageVersion
        """
        if self.__core_version is None:
            self.connect(MPComponents.CORE)
        if self.__core_version is None:
            raise AuthError("Core version is unknown")
        return self.__core_version

    def get_storage_version(self) -> str:
        """Текущая версия Storage.

        :return: StorageVersion
        """
        if self.__storage_version is None:
            self.connect(MPComponents.STORAGE)
        if self.__storage_version is None:
            raise AuthError("Storage version is unknown")
        return self.__storage_version

    def get_kb_version(self) -> str:
        """Текущая версия PT KB.

        :return: KBVersion
        """
        if self.__kb_version is None:
            self.connect(MPComponents.KB)
        if self.__kb_version is None:
            raise AuthError("KB version is unknown")
        return self.__kb_version

    def request_core_version(self) -> None:
        if self.__session is None:
            raise AuthError("Session is not initialized")

        core_version_url = (
            f"https://{self.creds.core_hostname}{self.__api_core_check_page}"
        )

        r = exec_request(
            self.__session,
            core_version_url,
            method="GET",
            timeout=self.settings.connection_timeout,
        )
        core_info: dict[str, Any] = r.json()

        if core_info.get("productVersion") is None:
            self.log.error(
                f"hostname={self.creds.core_hostname!r}, status=failed, "
                f"action=check_core_version, "
                f'msg="Unsupported core info json"'
            )
            raise AuthError("Unsupported core info json")

        self.__core_version = str(core_info["productVersion"])

    def check_missing_envs(self) -> None:
        required_by_type: dict[int, tuple[str, ...]] = {
            AuthType.LOCAL: ("core_hostname", "core_login", "core_pass"),
            AuthType.LDAP: ("core_hostname", "core_login", "core_pass"),
            AuthType.SECRET: (
                "core_hostname",
                "core_login",
                "core_pass",
                "client_secret",
            ),
            AuthType.PAT_TOKEN: ("core_hostname", "pat_token"),
        }
        auth_type = self.__auth_type
        if auth_type is None or auth_type not in required_by_type:
            raise AuthError(
                f"Unsupported or missing auth type: {auth_type!r}. "
                f"Expected one of {sorted(required_by_type)}"
            )
        missing = [
            param
            for param in required_by_type[auth_type]
            if getattr(self.creds, param) is None
        ]
        if missing:
            raise AuthError(f"Missing credentials: {', '.join(missing)}")

    def __core_try_connect(self) -> None:
        """Пробуем подключиться к Core."""

        self.check_missing_envs()

        if self.__auth_type in (AuthType.PAT_TOKEN, AuthType.SECRET):
            # PAT и SECRET - чистый Bearer: форма авторизации не нужна (MC
            # отклоняет POST /ui/login с authType=2 ошибкой `userId`).
            self.set_auth_header(token=self.get_token())
            self.__is_connected = True

            # Пробуем узнать версию Core
            self.log.debug(
                f"hostname={self.creds.core_hostname!r}, status=prepare, "
                f"action=check_core_version, "
                f'msg="Try to check Core version"'
            )

            self.request_core_version()

        else:
            auth_params = {
                "authType": self.creds.core_auth_type,
                "username": self.creds.core_login,
                "password": self.creds.core_pass,
                "newPassword": None,
            }
            try:
                pre_auth_url = f"https://{self.creds.core_hostname}{self.__api_core_auth_form_page}"

                # Нужно для иерархий
                self.log.debug(
                    f"hostname={self.creds.core_hostname!r}, url={pre_auth_url!r}, status=prepare, "
                    f'action=auth, msg="Auth. Phase 0. Get MC redirect"'
                )
                exec_request(
                    self.__current_session(),
                    pre_auth_url,
                    method="GET",
                    timeout=self.settings.connection_timeout,
                )

                self.log.debug(
                    f"hostname={self.creds.core_hostname!r}, url={pre_auth_url!r}, status=prepare, "
                    f'action=auth, msg="Auth. Phase 1. Response"'
                )

                login_url = f"https://{self.creds.core_hostname}:{self.__ms_port}{self.__api_core_auth_login_page}"

                self.log.debug(
                    f"hostname={self.creds.core_hostname!r}, url={login_url!r}, status=prepare, action=auth, "
                    f'msg="Auth. Phase 2. Send creds."'
                )

                r = exec_request(
                    self.__current_session(),
                    login_url,
                    timeout=self.settings.connection_timeout,
                    method="POST",
                    json=auth_params,
                )
                if '"requiredPasswordChange":true' in r.text:
                    self.log.error(
                        f"hostname={self.creds.core_hostname!r}, url={login_url!r}, status=failed, "
                        f'action=auth, msg="Required Password Change"'
                    )
                    raise AuthError("SIEM respond: requiredPasswordChange = true")
                if "access_denied" in r.url:
                    self.log.error(
                        f"hostname={self.creds.core_hostname!r}, url={login_url!r}, status=failed, "
                        f'action=auth, msg="Access Denied"'
                    )
                    raise AuthError("SIEM respond: access_denied")

                auth_url = f"https://{self.creds.core_hostname}{self.__api_core_auth_form_page}"

                self.log.debug(
                    f"hostname={self.creds.core_hostname!r}, url={auth_url!r}, status=prepare, "
                    f'action=auth, msg="Auth. Phase 3. Get auth form"'
                )
                r = exec_request(
                    self.__current_session(),
                    auth_url,
                    method="GET",
                    timeout=self.settings.connection_timeout,
                )

                rounds = 0
                while "<form" in r.text:
                    rounds += 1
                    if rounds > MAX_AUTH_FORM_ROUNDS:
                        raise AuthError(
                            f"Too many auth form rounds (> {MAX_AUTH_FORM_ROUNDS})"
                        )
                    form_action, form_data = self.__core_parse_form(r.text)

                    self.log.debug(
                        f"hostname={self.creds.core_hostname!r}, url={form_action!r}, "
                        f'status=prepare, action=auth, msg="Auth. Phase 4. Send data form"'
                    )
                    r = exec_request(
                        self.__current_session(),
                        form_action,
                        method="POST",
                        timeout=self.settings.connection_timeout,
                        data=form_data,
                    )

                # Пробуем узнать версию Core
                self.log.debug(
                    f"hostname={self.creds.core_hostname!r}, status=prepare, "
                    f'action=check_core_version, msg="Try to check Core version"'
                )

                self.request_core_version()

            except RequestException as rex:
                self.log.error(
                    f"hostname={self.creds.core_hostname!r}, status=failed, "
                    f"action=auth, msg={rex!r}"
                )
                raise

            self.__is_connected = True
            self.log.info(
                f"hostname={self.creds.core_hostname!r}, status=success, "
                f"action=check_core_version, version={self.__core_version!r}"
            )
            self.log.info(
                f"hostname={self.creds.core_hostname!r}, status=success, action=auth"
            )

    def __current_session(self) -> requests.Session:
        """Текущая сессия, созданная в connect()."""
        if self.__session is None:
            raise AuthError("Session is not initialized")
        return self.__session

    @staticmethod
    def __core_parse_form(data: str) -> tuple[str, dict[str, str]]:
        tree = lxml_html.fromstring(data)
        forms = tree.xpath("//form")
        if not forms:
            raise AuthError("No form found in auth response")
        form = forms[0]  # берём первую форму
        action_url = form.attrib.get("action")
        if action_url is None:
            raise AuthError("action attribute not found in form")

        # Скрытые (hidden) поля – обычно именно они нужны при auth‑форме
        fields = {
            el.attrib["name"]: html.unescape(el.attrib.get("value", ""))
            for el in form.xpath(".//input[@type='hidden']")
            if "name" in el.attrib
        }
        return str(action_url), fields

    def __siem_try_connect(self) -> None:
        raise NotImplementedError()

    def __storage_try_connect(self) -> None:
        if self.creds.storage_hostname is None:
            raise AuthError(
                f"hostname={self.creds.storage_hostname!r}, status=failed, "
                f'action=auth, msg="STORAGE hostname is empty"'
            )
        start_url = f"http://{self.creds.storage_hostname}:{self.__storage_port}{self.__api_storage_check_page}"
        try:
            r = exec_request(
                self.__current_session(),
                start_url,
                timeout=self.settings.connection_timeout,
                method="GET",
            )

            self.log.debug(
                f"hostname={self.creds.storage_hostname!r}, status=prepare, "
                f'action=check_storage_version, msg="Try to check Storage version"'
            )

            es_info: dict[str, Any] = r.json()
            if es_info.get("nodes") is None:
                self.log.error(
                    f"hostname={self.creds.storage_hostname!r}, status=failed, "
                    f"action=check_storage_version, "
                    f'msg="Unsupported node info json"'
                )
                raise AuthError("Unsupported node info json")

            node = next(iter(es_info["nodes"].values()))
            version = node["version"]

            if version.startswith("7.17"):
                self.__storage_version = StorageVersion.ES7_17
            elif version.startswith("7."):
                self.__storage_version = StorageVersion.ES7
            else:
                self.log.error(
                    f"hostname={self.creds.storage_hostname!r}, status=failed, "
                    f"action=check_storage_version, "
                    f'msg="Storage version found, but not supported"'
                )
                raise AuthError("Storage version found, but not supported")
        except RequestException as rex:
            self.log.error(
                f"hostname={self.creds.storage_hostname!r}, status=failed, "
                f"action=auth, msg={rex!r}"
            )
            raise

        self.__is_connected = True
        self.log.info(
            f"hostname={self.creds.storage_hostname!r}, status=success, "
            f"action=check_storage_version, version={self.__storage_version!r}"
        )
        self.log.info(
            f"hostname={self.creds.storage_hostname!r}, status=success, action=auth"
        )

    def __kb_check_version(self) -> None:
        """Проверить доступность KB и получить её версию (без формы авторизации)."""

        kb_dbs_url = f"https://{self.creds.core_hostname}:{self.__kb_port}{self.__api_kb_db_list}"
        r = exec_request(
            self.__current_session(),
            kb_dbs_url,
            method="GET",
            timeout=self.settings.connection_timeout,
        )
        db_names = r.json()
        if not db_names:
            raise AuthError("Not found DBs on KB. API does not work")

        headers = {
            "Content-Database": db_names[0].get("Name"),
            "Content-Locale": "RUS",
        }
        kb_version_url = f"https://{self.creds.core_hostname}:{self.__kb_port}{self.__api_kb_check_page}"

        r = exec_request(
            self.__current_session(),
            kb_version_url,
            method="GET",
            timeout=self.settings.connection_timeout,
            headers=headers,
        )
        kb_info: dict[str, Any] = r.json()

        if kb_info.get("CoreVersion") is None:
            self.log.error(
                f"hostname={self.creds.core_hostname!r}, status=failed, "
                f"action=check_kb_version, "
                f'msg="Unsupported KB info json"'
            )
            raise AuthError("Unsupported KB info json")

        self.__kb_version = str(kb_info["CoreVersion"])

    def __kb_try_connect(self) -> None:
        """Пробуем подключиться к PT KB."""

        self.check_missing_envs()

        if self.__auth_type in (AuthType.PAT_TOKEN, AuthType.SECRET):
            # PAT/SECRET принимаются KB напрямую (Bearer), форма авторизации не нужна
            self.__session = requests.Session()
            self.__session.verify = False
            self.set_auth_header(token=self.get_token())
            self.__kb_check_version()
            self.__is_connected = True
            return

        self.__core_try_connect()

        try:
            login_url = f"https://{self.creds.core_hostname}:{self.__kb_port}{self.__api_kb_auth_login_page}"

            # Получаем форму с токенами, так как мы уже аутентифицированы
            self.log.debug(
                f"hostname={self.creds.core_hostname!r}, status=prepare, "
                f'action=auth, msg="Auth. Phase 1. Get auth form from KB"'
            )

            r = exec_request(
                self.__current_session(),
                login_url,
                method="GET",
                timeout=self.settings.connection_timeout,
            )

            m = re.findall("name='([^']+)' value='([^']+)'", r.text)
            if not m:
                raise AuthError("Can not get form with tokens")
            params = {name: value for name, value in m}

            auth_url = f"https://{self.creds.core_hostname}:{self.__ms_port}{self.__api_ms_authorize}"

            # Отправляем токены в MS, чтобы получить правильные cookies
            self.log.debug(
                f"hostname={self.creds.core_hostname!r}, status=prepare, "
                f'action=auth, msg="Auth. Phase 2. Send tokens to MS"'
            )

            exec_request(
                self.__current_session(),
                auth_url,
                method="GET",
                timeout=self.settings.connection_timeout,
                params=params,
            )

            sign_url = f"https://{self.creds.core_hostname}:{self.__kb_port}{self.__api_kb_signin}"

            # Логинимся в KB с правильными cookies
            self.log.debug(
                f"hostname={self.creds.core_hostname!r}, status=prepare, "
                f'action=auth, msg="Auth. Phase 3. Sign in to KB"'
            )

            exec_request(
                self.__current_session(),
                sign_url,
                method="POST",
                timeout=self.settings.connection_timeout,
                data=params,
            )

            self.__kb_check_version()

        except RequestException as rex:
            self.log.error(
                f"hostname={self.creds.core_hostname!r}, status=failed, "
                f"action=auth, msg={rex!r}"
            )
            raise

        self.__is_connected = True
        self.log.info(
            f"hostname={self.creds.core_hostname!r}, status=success, "
            f"action=check_kb_version, version={self.__kb_version!r}"
        )
        self.log.info(
            f"hostname={self.creds.core_hostname!r}, status=success, action=auth"
        )
