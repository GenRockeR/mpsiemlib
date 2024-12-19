import html
import json
import re
# TODO: requests.utils.urlparse ?
from urllib.parse import urlparse

import requests
from requests import RequestException

from .BaseFunctions import exec_request
from .Interfaces import LoggingHandler, AuthType, MPComponents, AuthInterface, Settings, StorageVersion


class MPSIEMAuth(AuthInterface, LoggingHandler):
    """Аутентификация на компонентах MP, если требуется. Получение текущий
    версии компонент.

    :no-index:
    """
    __auth_type = AuthType.LOCAL  # 0 - Local, 1 - LDAP

    __ms_port = 3334
    __api_ms_authorize = '/connect/authorize'
    __token_uri = '/connect/token'

    __api_core_auth_login_page = '/ui/login'
    __api_core_auth_form_page = '/account/login?returnUrl=/#/authorization/landing'
    __api_core_check_page = '/api/deployment_configuration/v1/system_info'

    __kb_port = 8091
    __api_kb_auth_login_page = '/account/login'
    __api_kb_signin = '/signin-oidc'
    __api_kb_check_page = '/api-studio/aboutSystem'
    __api_kb_db_list = '/api-studio/content-database-selector/content-databases'

    __siem_port = ''
    __api_siem_check_page = ''

    __storage_port = 9200
    __api_storage_check_page = '/_nodes'

    def __init__(self, creds, settings: Settings):
        AuthInterface.__init__(self, creds, settings)
        LoggingHandler.__init__(self)
        self.__session = None
        self.__is_connected = False
        self.__component = MPComponents.CORE
        self.__storage_version = None
        self.__core_version = None
        self.__kb_version = None
        self.__kb_token = None
        self.sessions = None
        self.client_secret = None

    def get_token(self):
        """Аутентификация в MC через токены."""

        url = f'https://{self.creds.core_hostname}:{self.__ms_port}{self.__token_uri}'
        payload = dict(grant_type='password', client_id='mpx', client_secret=self.creds.client_secret,
                       scope='authorization offline_access mpx.api ptkb.api idmgr.api',
                       response_type='code id_token token', username=self.creds.core_login,
                       password=self.creds.core_pass)
        token = requests.post(url, data=payload, verify=False).json().get('access_token')
        return token

    def set_auth_header(self, token):
        """Установка токена bearer."""

        self.__session.headers.update({'Authorization': 'Bearer ' + token})

    def connect(self, component, creds=None):
        """Подключение к выбранным компонентам :param component: Компонент для
        подключения Interfaces.MPComponents :param creds: креды для подключения
        Interfaces.Creds.

        :return: session или None
        """
        if creds is not None:
            self.creds = creds
        self.__session = requests.Session()
        self.__session.verify = False
        self.__component = component

        if component == MPComponents.CORE or component == MPComponents.MS:
            self.__core_try_connect()
        elif component == MPComponents.SIEM:
            self.__siem_try_connect()
        elif component == MPComponents.STORAGE:
            self.__storage_try_connect()
        elif component == MPComponents.KB:
            self.__kb_try_connect()
        else:
            raise NotImplementedError(f"Unsupported component for Auth {component}")

        self.set_auth_header(token=self.get_token())

        return self.__session

    def disconnect(self):
        # TODO logout in MP CORE
        """Очистка сессии."""

        if self.__session is not None:
            self.__session.close()
        self.__is_connected = False
        self.__session = None

    def get_session(self):
        if not self.__is_connected or self.__session is None:
            self.connect(self.__component, self.creds)
        return self.__session

    def get_component(self):
        return self.__component

    def get_creds(self):
        return self.creds

    def get_core_version(self) -> str:
        """Текущая версия Core.

        :return: StorageVersion
        """
        if self.__core_version is None:
            self.connect(MPComponents.CORE)
        return self.__core_version

    def get_storage_version(self) -> str:
        """Текущая версия Storage.

        :return: StorageVersion
        """
        if self.__storage_version is None:
            self.connect(MPComponents.STORAGE)
        return self.__storage_version

    def get_kb_version(self) -> str:
        """Текущая версия PT KB.

        :return: StorageVersion
        """
        if self.__kb_version is None:
            self.connect(MPComponents.KB)
        return self.__kb_version

    def __core_try_connect(self):
        """Пробуем подключиться к Core."""

        if self.creds.core_hostname is None or self.creds.core_login is None or self.creds.core_pass is None:
            raise Exception('hostname="{}", status=failed, action=auth, '
                            'msg="Core hostname or login or pass is empty"'.format(self.creds.core_hostname))

        login_url = f'https://{self.creds.core_hostname}:{self.__ms_port}{self.__api_core_auth_login_page}'

        auth_params = {
            'authType': self.creds.core_auth_type,
            'username': self.creds.core_login,
            'password': self.creds.core_pass,
            'newPassword': None
        }
        try:
            pre_auth_url = f'https://{self.creds.core_hostname}{self.__api_core_auth_form_page}'

            # Нужно для иерархий
            self.log.debug('hostname="{}", url={}, status=prepare, action=auth, '
                           'msg="Auth. Phase 0. Get MC redirect"'.
                           format(self.creds.core_hostname, pre_auth_url))
            r = exec_request(self.__session, pre_auth_url, method='GET', timeout=self.settings.connection_timeout)
            # TODO: Save main mc address for future use?
            main_ms = urlparse(r.url)
            self.log.debug('hostname="{}", url="{}", status=prepare, action=auth, msg="Auth. Phase 1. Response.", '
                           'url_main_ms="{}"'.format(main_ms.hostname, pre_auth_url, main_ms))

            login_url = f'https://{main_ms.hostname}:{self.__ms_port}{self.__api_core_auth_login_page}'

            self.log.debug('hostname="{}", url="{}", status=prepare, action=auth, msg="Auth. Phase 2. Send creds."'.
                           format(main_ms.hostname, login_url))
            r = exec_request(self.__session,
                             login_url,
                             timeout=self.settings.connection_timeout,
                             method='POST',
                             json=auth_params)
            if '"requiredPasswordChange":true' in r.text:
                self.log.error('hostname="{}", url={}, status=failed, action=auth, msg="Required Password Change"'.
                               format(self.creds.core_hostname, login_url))
                raise Exception('SIEM respond: requiredPasswordChange = true')
            if 'access_denied' in r.url:
                self.log.error('hostname="{}", url={}, status=failed, action=auth, msg="Access Denied"'.
                               format(self.creds.core_hostname, login_url))
                raise Exception('SIEM respond: access_denied')

            auth_url = f'https://{self.creds.core_hostname}{self.__api_core_auth_form_page}'

            self.log.debug('hostname="{}", url={}, status=prepare, action=auth, '
                           'msg="Auth. Phase 3. Get auth form"'.
                           format(self.creds.core_hostname, auth_url))
            r = exec_request(self.__session, auth_url, method='GET', timeout=self.settings.connection_timeout)

            while '<form' in r.text:
                form_action, form_data = self.__core_parse_form(r.text)

                self.log.debug('hostname="{}", url={}, status=prepare, action=auth, '
                               'msg="Auth. Phase 4. Send data form"'.
                               format(self.creds.core_hostname, form_action))
                r = exec_request(self.__session, form_action,
                                 method='POST',
                                 timeout=self.settings.connection_timeout,
                                 data=form_data)

            # Пробуем узнать версию Core
            self.log.debug('hostname="{}", status=prepare, action=check_core_version, '
                           'msg="Try to check Core version"'.format(self.creds.core_hostname))
            core_version_url = f"https://{self.creds.core_hostname}{self.__api_core_check_page}"

            r = exec_request(self.__session, core_version_url, method='GET', timeout=self.settings.connection_timeout)
            r.raise_for_status()
            core_info = json.loads(r.text)

            if core_info.get('productVersion') is None:
                self.log.error('hostname="{}", status=failed, action=check_core_version, '
                               'msg="Unsupported core info json"'.format(self.creds.core_hostname))
                raise Exception('Unsupported core info json')

            self.__core_version = core_info.get('productVersion')

        except RequestException as rex:
            self.log.error('hostname="{}", status=failed, action=auth, msg="{}"'.
                           format(self.creds.core_hostname, rex))
            raise rex

        self.__is_connected = True
        self.log.info('hostname="{}", status=success, action=check_core_version, '
                      'version="{}"'.format(self.creds.core_hostname, self.__core_version))
        self.log.info('hostname="{}", status=success, action=auth'.format(self.creds.core_hostname))

    def __core_parse_form(self, data):  # noqa
        return re.search('action=[\'"]([^\'"]*)[\'"]', data).groups()[0], {
            item.groups()[0]: html.unescape(item.groups()[1])
            for item in re.finditer(
                'name=[\'"]([^\'"]*)[\'"] value=[\'"]([^\'"]*)[\'"]',
                data
            )
        }

    def __siem_try_connect(self):
        raise NotImplementedError()

    def __storage_try_connect(self):
        if self.creds.storage_hostname is None:
            raise Exception('hostname="{}", status=failed, action=auth, '
                            'msg="SIEM hostname is empty"'.format(self.creds.storage_hostname))
        start_url = f'http://{self.creds.storage_hostname}:{self.__storage_port}{self.__api_storage_check_page}'  # noqa
        try:
            r = exec_request(self.__session, start_url, timeout=self.settings.connection_timeout, method='GET')
            r.raise_for_status()

            self.log.debug('hostname="{}", status=prepare, action=check_storage_version, '
                           'msg="Try to check Storage version"'.format(self.creds.storage_hostname))

            es_info = json.loads(r.text)
            if es_info.get('nodes') is None:
                self.log.error('hostname="{}", status=failed, action=check_storage_version, '
                               'msg="Unsupported node info json"'.format(self.creds.storage_hostname))
                raise Exception('Unsupported node info json')

            # Пытаемся узнать версию ES
            for k, v in es_info.get('nodes').items():
                if v.get('version') is None:
                    self.log.error('hostname="{}", status=failed, action=check_storage_version, '
                                   'msg="Version not found"'.format(self.creds.storage_hostname))
                    raise Exception('Version not found')

                if v.get('version').startswith('7.17'):
                    self.__storage_version = StorageVersion.ES7_17
                elif v.get('version').startswith('7.'):
                    self.__storage_version = StorageVersion.ES7
                else:
                    self.log.error('hostname="{}", status=failed, action=check_storage_version, '
                                   'msg="Storage version found, but not supported"'.format(self.creds.storage_hostname))
                    raise Exception('Storage version found, but not supported')
        except RequestException as rex:
            self.log.error('hostname="{}", status=failed, action=auth, msg="{}"'.
                           format(self.creds.storage_hostname, rex))
            raise rex

        self.__is_connected = True
        self.log.info('hostname="{}", status=success, action=check_storage_version, '
                      'version="{}"'.format(self.creds.storage_hostname, self.__storage_version))
        self.log.info('hostname="{}", status=success, action=auth'.format(self.creds.storage_hostname))

    def __kb_try_connect(self):
        """Пробуем подключиться к PT KB."""

        if self.creds.core_hostname is None or self.creds.core_login is None or self.creds.core_pass is None:
            raise Exception('hostname="{}", status=failed, action=auth, '
                            'msg="Core hostname or login or pass is empty"'.format(self.creds.core_hostname))

        self.__core_try_connect()

        try:
            login_url = f"https://{self.creds.core_hostname}:{self.__kb_port}{self.__api_kb_auth_login_page}"

            # Получаем форму с токенами, так как мы уже аутентифицированы
            self.log.debug('hostname="{}", status=prepare, action=auth, '
                           'msg="Auth. Phase 1. Get auth form from KB"'.format(self.creds.core_hostname))

            r = exec_request(self.__session, login_url, method="GET", timeout=self.settings.connection_timeout)

            m = re.findall("name='([^']+)' value='([^']+)'", r.text)
            if m is None:
                raise Exception('Can not get form with tokens')
            params = {}
            for i in m:
                params[i[0]] = i[1]

            auth_url = f"https://{self.creds.core_hostname}:{self.__ms_port}{self.__api_ms_authorize}"

            # Отправляем токены в MS, чтобы получить правильные куки
            self.log.debug('hostname="{}", status=prepare, action=auth, '
                           'msg="Auth. Phase 2. Send tokens to MS"'.format(self.creds.core_hostname))

            r = exec_request(self.__session,
                             auth_url,
                             method='GET',
                             timeout=self.settings.connection_timeout,
                             params=params)

            sign_url = f"https://{self.creds.core_hostname}:{self.__kb_port}{self.__api_kb_signin}"

            # Логинимся в KB с правильными куками
            self.log.debug('hostname="{}", status=prepare, action=auth, '
                           'msg="Auth. Phase 3. Sign in to KB"'.format(self.creds.core_hostname))

            r = exec_request(self.__session,
                             sign_url,
                             method='POST',
                             timeout=self.settings.connection_timeout,
                             data=params)

            # невозможно узнать версию KB, не указав существующую базу
            kb_dbs_url = f"https://{self.creds.core_hostname}:{self.__kb_port}{self.__api_kb_db_list}"
            r = exec_request(self.__session,
                             kb_dbs_url,
                             method='GET',
                             timeout=self.settings.connection_timeout)
            db_names = r.json()
            if db_names is None or len(db_names) == 0:
                raise Exception('Not found DBs on KB. API does not work')

            headers = {'Content-Database': db_names[0].get('Name'),  # берем первую попавшуюся БД
                       'Content-Locale': 'RUS'}
            kb_version_url = f"https://{self.creds.core_hostname}:{self.__kb_port}{self.__api_kb_check_page}"

            r = exec_request(self.__session,
                             kb_version_url,
                             method='GET',
                             timeout=self.settings.connection_timeout,
                             headers=headers)
            r.raise_for_status()
            kb_info = r.json()

            if kb_info.get('CoreVersion') is None:
                self.log.error('hostname="{}", status=failed, action=check_kb_version, '
                               'msg="Unsupported KB info json"'.format(self.creds.core_hostname))
                raise Exception('Unsupported KB info json')

            self.__kb_version = kb_info.get('CoreVersion')

        except RequestException as rex:
            self.log.error('hostname="{}", status=failed, action=auth, msg="{}"'.
                           format(self.creds.core_hostname, rex))
            raise rex

        self.__is_connected = True
        self.log.info('hostname="{}", status=success, action=check_kb_version, '
                      'version="{}"'.format(self.creds.core_hostname, self.__kb_version))
        self.log.info(f'hostname="{self.creds.core_hostname}", status=success, action=auth')
