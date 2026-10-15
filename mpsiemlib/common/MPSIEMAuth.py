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
)

MAX_AUTH_FORM_ROUNDS = 10

# Минимальный поддерживаемый релиз ядра (MP SIEM 26.0).
MIN_SUPPORTED_CORE_RELEASE: tuple[int, int] = (26, 0)

# Имя продукта MaxPatrol 10 в ответе `/api/licensing/v1/product`.
_MP10_PRODUCT_NAME = "MaxPatrol 10"


class AuthError(RuntimeError):
    """Базовое исключение для всех ошибок аутентификации/подключения."""


class StorageNotSupportedError(AuthError):
    """Хранилище событий недоступно библиотеке в данном окружении.

    Производитель отказывается от Elasticsearch в пользу Logspace, поэтому
    ядро библиотеки не знает, как определять версию и тип хранилища: чтение
    событий из Elasticsearch - ответственность extra-модуля
    `mpsiemlib[events]` (`mpsiemlib.modules.Events`).
    """


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
    # Выход cookie-сессии (LOCAL/LDAP): AccountV2.Logout. Реальное завершение
    # серверной сессии делает второй_stage - GET /ui/logout?logoutId=...,
    # поэтому logout выполняется с обходом редиректов (измерено на R28.0:
    # без обхода 302 серверную сессию не гасит).
    __api_ms_logout = "/ptms/api/sso/v2/account/logout"

    __api_core_auth_login_page = "/ui/login"
    __api_core_auth_form_page = "/account/login?returnUrl=/#/authorization/landing"
    __api_core_check_page = "/api/deployment_configuration/v1/system_info"
    # Резервный источник версии ядра: Licensing v1 (Core, без MC Portal).
    # `system_info` обслуживает MC Portal и на R28.0 может отвечать 500, тогда
    # версию отдаёт `/api/licensing/v1/product` (поле `version` продукта MP10).
    __api_core_version_fallback_page = "/api/licensing/v1/product"

    __kb_port = 8091
    __api_kb_auth_login_page = "/account/login"
    __api_kb_signin = "/signin-oidc"
    __api_kb_check_page = "/api-studio/aboutSystem"
    __api_kb_db_list = "/api-studio/content-database-selector/content-databases"

    __siem_port = ""
    __api_siem_check_page = ""

    def __init__(self, creds: Creds, settings: Settings) -> None:
        AuthInterface.__init__(self, creds, settings)
        LoggingHandler.__init__(self)
        self.__session: requests.Session | None = None
        self.__is_connected = False
        self.__component = MPComponents.CORE
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
        :raise StorageNotSupportedError: для `MPComponents.STORAGE` - хранилище
            событий не является компонентом REST API ядра (см. модуль Events)
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
            raise self.__storage_unsupported()
        elif component == MPComponents.KB:
            self.__kb_try_connect()
        else:
            raise NotImplementedError(f"Unsupported component for Auth {component!r}")

        self.set_auth_header(token=self.get_token())

        if self.__session is None:
            raise AuthError("Session was not created")
        return self.__session

    def disconnect(self) -> None:
        """Закрыть сессию.

        Для cookie-сессий (LOCAL/LDAP) Core/MS сначала выполняется выход
        (`POST /ptms/api/sso/v2/account/logout` с обходом редиректов до
        `/ui/logout?logoutId=...`): он завершает серверную сессию IAM
        (измерено на R28.0: после выхода API отвечает 401, повторный
        `connect()` аутентифицируется заново). Для PAT/SECRET выход не
        вызывается: Bearer-токен им не отзывается, а серверная cookie-сессия
        при таких типах не создаётся.
        """
        if self.__session is not None:
            if (
                self.__is_connected
                and self.__auth_type in (AuthType.LOCAL, AuthType.LDAP)
                and self.__component
                in (MPComponents.CORE, MPComponents.MS, MPComponents.KB)
            ):
                self.__logout_ms_session()
            self.__session.close()
        self.__is_connected = False
        self.__session = None

    def __logout_ms_session(self) -> None:
        """Выполнить logout IAM для текущей cookie-сессии.

        Ошибки выхода не должны срывать очистку сессии вызывающим кодом
        (disconnect() вызывается и в finally), поэтому они только логируются.
        """
        if self.__session is None:
            return
        logout_url = (
            f"https://{self.creds.core_hostname}:{self.__ms_port}{self.__api_ms_logout}"
        )
        try:
            # allow_redirects=True обязателен: POST отдаёт 302 на
            # /ui/logout?logoutId=..., и только этот GET завершает сессию
            # (измерено на R28.0).
            self.__session.request(
                "POST",
                logout_url,
                timeout=self.settings.connection_timeout,
                verify=False,
                allow_redirects=True,
            )
            self.log.info(
                f"hostname={self.creds.core_hostname!r}, status=success, "
                f'action=logout, msg="MS session logged out"'
            )
        except RequestException as err:
            self.log.warning(
                f"hostname={self.creds.core_hostname!r}, status=failed, "
                f"action=logout, msg={err!r}, "
                f'msg="Session closed without server-side logout"'
            )

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

        :return: версия ядра (например `28.0.45355`)
        """
        if self.__core_version is None:
            self.connect(MPComponents.CORE)
        if self.__core_version is None:
            raise AuthError("Core version is unknown")
        return self.__core_version

    def get_kb_version(self) -> str:
        """Текущая версия PT KB.

        :return: версия KB (поле `CoreVersion` ответа `/api-studio/aboutSystem`)
        """
        if self.__kb_version is None:
            self.connect(MPComponents.KB)
        if self.__kb_version is None:
            raise AuthError("KB version is unknown")
        return self.__kb_version

    def request_core_version(self) -> None:
        """Определить версию Core.

        Основной источник - `system_info` (MC Portal). Он может быть недоступен
        (на R28.0 отвечает 500), тогда версия берётся из Licensing v1
        (`/api/licensing/v1/product`, продукт `MaxPatrol 10`).

        :raises AuthError: Если ни один источник не вернул версию
        """
        version = self.__request_core_version_system_info()
        if version is None:
            version = self.__request_core_version_licensing()
        self.__core_version = version
        self._check_supported_core_release()

    def __request_core_version_system_info(self) -> str | None:
        """Версия Core из `system_info`, либо None если источник недоступен."""
        if self.__session is None:
            raise AuthError("Session is not initialized")

        url = f"https://{self.creds.core_hostname}{self.__api_core_check_page}"
        try:
            r = exec_request(
                self.__session,
                url,
                method="GET",
                timeout=self.settings.connection_timeout,
            )
            core_info: dict[str, Any] = r.json()
        except RequestException as err:
            # `system_info` обслуживает MC Portal: он может отвечать 500
            # (наблюдается на R28.0). Это не ошибка аутентификации -
            # переходим на резервный источник.
            self.log.warning(
                f"hostname={self.creds.core_hostname!r}, status=failed, "
                f"action=check_core_version, source=system_info, msg={err!r}, "
                f'msg="Fallback to Licensing v1 product info"'
            )
            return None

        version = core_info.get("productVersion")
        if version is None:
            self.log.warning(
                f"hostname={self.creds.core_hostname!r}, status=failed, "
                f"action=check_core_version, source=system_info, "
                f'msg="Unsupported core info json, fallback to Licensing v1"'
            )
            return None

        self.log.info(
            f"hostname={self.creds.core_hostname!r}, status=success, "
            f"action=check_core_version, source=system_info, "
            f"version={str(version)!r}"
        )
        return str(version)

    def __request_core_version_licensing(self) -> str:
        """Версия Core из Licensing v1 (`/api/licensing/v1/product`).

        Берётся `version` продукта `MaxPatrol 10` (ядро), а не `MaxPatrol SIEM`
        (версия сервиса SIEM, например `100.1.3900`).

        :raises AuthError: если версия не получена
        """
        if self.__session is None:
            raise AuthError("Session is not initialized")

        url = (
            f"https://{self.creds.core_hostname}{self.__api_core_version_fallback_page}"
        )
        try:
            r = exec_request(
                self.__session,
                url,
                method="GET",
                timeout=self.settings.connection_timeout,
            )
            products: Any = r.json()
        except RequestException as err:
            self.log.error(
                f"hostname={self.creds.core_hostname!r}, status=failed, "
                f"action=check_core_version, source=licensing_v1, msg={err!r}"
            )
            raise

        if not isinstance(products, list):
            raise AuthError("Unsupported licensing product info json")

        for product in products:
            if not isinstance(product, dict):
                continue
            if product.get("name") == _MP10_PRODUCT_NAME and product.get("version"):
                version = str(product["version"])
                self.log.info(
                    f"hostname={self.creds.core_hostname!r}, status=success, "
                    f"action=check_core_version, source=licensing_v1, "
                    f"version={version!r}"
                )
                return version

        raise AuthError(
            f"Core version is unknown ({_MP10_PRODUCT_NAME} not found "
            f"in {self.__api_core_version_fallback_page})"
        )

    def get_core_release(self) -> tuple[int, int]:
        """Релиз ядра `(MAJOR, MINOR)` из строки версии ("28.0.45355" -> (28, 0)).

        Сравнение кортежем, а не float: "26.10" превращается в 26.1 и
        неотличим от 26.1.

        :return: Кортеж (мажор, минор)
        :raises AuthError: если версия неизвестна или не разобрана
        """
        version = self.get_core_version()
        parts = version.split(".")
        try:
            return int(parts[0]), int(parts[1])
        except (IndexError, ValueError) as err:
            raise AuthError(f"Can not parse Core version {version!r}") from err

    def is_core_release_at_least(self, release: tuple[int, int]) -> bool:
        """True, если релиз ядра >= переданного, например `(28, 0)`."""
        return self.get_core_release() >= release

    def _check_supported_core_release(self) -> None:
        """Предупредить, если релиз ядра ниже поддерживаемого минимума."""
        release = self.get_core_release()
        if release < MIN_SUPPORTED_CORE_RELEASE:
            self.log.warning(
                f"hostname={self.creds.core_hostname!r}, "
                f"action=check_core_version, core_release={release!r}, "
                f"min_supported={MIN_SUPPORTED_CORE_RELEASE!r}, "
                f'msg="Core release is below the supported minimum"'
            )

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

                try:
                    r = exec_request(
                        self.__current_session(),
                        login_url,
                        timeout=self.settings.connection_timeout,
                        method="POST",
                        json=auth_params,
                    )
                except requests.HTTPError as err:
                    # IAM отвечает 400 Platform.Iam.User.Login.Error как на
                    # неверные креды, так и на отсутствующее LDAP-подключение
                    # (измерено на R28.0). Поднимаем AuthError с сообщением
                    # сервера: тип ошибки аутентификации, а не HTTP.
                    message = self.__extract_login_error_message(err)
                    self.log.error(
                        f"hostname={self.creds.core_hostname!r}, url={login_url!r}, "
                        f"status=failed, action=auth, msg={err!r}, "
                        f"iam_message={message!r}"
                    )
                    raise AuthError(f"SIEM login failed: {message}") from err

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
    def __extract_login_error_message(err: requests.HTTPError) -> str:
        """Сообщение IAM из тела ответа входа.

        Форма ответа: `{"message": "...", "errors": [{"error": {"message":
        "...", "type": "Platform.Iam.User.Login.Error"}}]}`. Если тело не
        распарсилось (не JSON или неизвестная форма), возвращается текст
        ответа - он информативнее кода статуса.
        """
        response = err.response
        if response is None:
            return str(err)

        try:
            body: Any = response.json()
        except ValueError:
            return response.text or str(err)

        if isinstance(body, dict):
            errors = body.get("errors")
            if isinstance(errors, list):
                for item in errors:
                    if isinstance(item, dict) and isinstance(item.get("error"), dict):
                        message = item["error"].get("message")
                        if message:
                            return str(message)
            message = body.get("message")
            if message:
                return str(message)

        return response.text or str(err)

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

    def __storage_unsupported(self) -> StorageNotSupportedError:
        """Ошибка «storage недоступен ядру библиотеки».

        Хранилище событий не является REST API ядра: ранее определялось
        прямым `GET {storage}:9200/_nodes`, что работало только пока
        хранилище - Elasticsearch. Производитель переводит storage на
        Logspace, поэтому ES-зонд из ядра библиотеки удалён, а чтение
        событий осталось в extra-модуле `mpsiemlib[events]`.
        """
        message = (
            "storage is not a Core REST API component: read events via the "
            "mpsiemlib[events] extra (mpsiemlib.modules.Events)"
        )
        self.log.error(
            f"hostname={self.creds.storage_hostname!r}, status=failed, "
            f"action=auth, component={MPComponents.STORAGE!r}, msg={message!r}"
        )
        return StorageNotSupportedError(
            f"hostname={self.creds.storage_hostname!r}, {message}"
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
