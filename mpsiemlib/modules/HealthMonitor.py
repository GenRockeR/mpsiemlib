import json
import time
from datetime import datetime
from typing import List

from mpsiemlib.common import ModuleInterface, MPSIEMAuth, LoggingHandler, MPComponents, Settings
from mpsiemlib.common import exec_request


class HealthMonitor(ModuleInterface, LoggingHandler):
    """
    Health monitor module
    """

    __api_global_status = '/api/health_monitoring/v2/total_status'
    __api_checks = '/api/health_monitoring/v2/checks?limit={}&offset={}'
    __api_license_status = '/api/licensing/v2/license_validity'
    __api_agents_status = '/api/components/agent'
    __api_agents_status_new = '/api/v1/scanner_agents'
    __api_kb_status = '/api/v1/knowledgeBase'

    # шаблоны сообщений
    __api_error_pattern_messages_new = "/assets/i18n/ru-RU/navigation.json?{}" # V.25
    __api_error_pattern_messages_old = "/Content/locales/l10n/ru-RU/navigation.json?{}" # V.23 - V.24

    __kb_port = 8091

    def __init__(self, auth: MPSIEMAuth, settings: Settings):
        ModuleInterface.__init__(self, auth, settings)
        LoggingHandler.__init__(self)
        self.__core_session = auth.connect(MPComponents.CORE)
        self.__core_hostname = auth.creds.core_hostname
        self.__core_version = auth.get_core_version()
        self.__kb_session = auth.connect(MPComponents.KB)
        self.__error_patterns = None

    def get_health_status(self) -> str:
        """
        Получить общее состояние системы

        :return: "ok" - если нет ошибок
        """
        url = f'https://{self.__core_hostname}{self.__api_global_status}'
        r = exec_request(self.__core_session,
                         url,
                         method='GET',
                         timeout=self.settings.connection_timeout)
        response = r.json()
        status = response.get('status')

        self.log.info('status=success, action=get_health_status, msg="Got global status", '
                      'hostname="{}" status="{}"'.format(self.__core_hostname, status))

        return status

    def get_health_errors(self) -> List[dict]:
        """
        Получить список ошибок из семафора.

        :return: Список ошибок или пустой массив, если ошибок нет
        """
        limit = 1000
        offset = 0
        api_url = self.__api_checks.format(limit, offset)
        url = "https://{}{}".format(self.__core_hostname, api_url)
        r = exec_request(self.__core_session,
                         url,
                         method='GET',
                         timeout=self.settings.connection_timeout)
        response = r.json()
        errors = response.get("items")

        ret = []
        for i in errors:
            source = i.get("source") if i.get("source") is not None else {}
            params = i.get("parameters") if i.get("parameters") is not None else {}
            ret.append({"id": i.get("id"),
                        "timestamp": i.get("timestamp"),
                        "status": i.get("status", "").lower(),
                        "type": i.get("type").lower(),
                        "name": source.get("displayName").lower(),
                        "hostname": source.get("hostName"),
                        "ip": source.get("ipAddresses"),
                        "component_name": params.get("componentName"),
                        "component_hostname": params.get("hostName"),
                        "component_ip": params.get("ipAddresses"),
                        "parameters": params,
                        "source": source,
                        "sensitive": i.get("sensitive"),
                        "displayName": source.get('displayName'),
                        "componentName": params.get("componentName"),
                        "hostName": params.get("hostName"),
                        })

        self.log.info('status=success, action=get_health_errors, msg="Got errors", '
                      'hostname="{}" count="{}"'.format(self.__core_hostname, len(errors)))

        return ret

    def get_health_license_status(self) -> dict:
        """
        Получить статус лицензии.

        :return: Dict
        """
        url = f'https://{self.__core_hostname}{self.__api_license_status}'
        r = exec_request(self.__core_session,
                         url,
                         method='GET',
                         timeout=self.settings.connection_timeout)
        response = r.json()
        lic = response.get('license')
        status = {'valid': response.get('validity') == 'valid',
                  'key': lic.get('keyNumber'),
                  'type': lic.get('licenseType'),
                  'granted': lic.get('keyDate'),
                  'expiration': lic.get('expirationDate'),
                  'assets': lic.get('assetsCount')}

        self.log.info('status=success, action=get_health_license_status, msg="Got license status", '
                      'hostname="{}"'.format(self.__core_hostname))

        return status

    def get_health_agents_status(self) -> List[dict]:
        """
        Получить статус агентов.

        :return: Список агентов и их параметры.
        """
        if int(self.__core_version.split('.')[0]) < 25:
            url = f'https://{self.__core_hostname}{self.__api_agents_status}'
        else:
            url = f'https://{self.__core_hostname}{self.__api_agents_status_new}'
        r = exec_request(self.__core_session,
                         url,
                         method='GET',
                         timeout=self.settings.connection_timeout)
        response = r.json()

        agents = []
        for i in response:
            agents.append({
                'id': i.get('id'),
                'name': i.get('name'),
                'hostname': i.get('address'),
                'version': i.get('version'),
                'updates': i.get('availableUpdates'),
                'status': i.get('status'),
                'roles': i.get('roleNames'),
                'ip': i.get('ipAddresses'),
                'platform': i.get('platform'),
                'modules': i.get('modules')
            })

        self.log.info('status=success, action=get_health_agents_status, msg="Got agents status", '
                      'hostname="{}" count="{}"'.format(self.__core_hostname, len(agents)))

        return agents

    def get_health_kb_status(self) -> dict:
        """
        Получить статус обновления VM контента в Core.

        :return: dict.
        """
        url = f'https://{self.__core_hostname}:{self.__kb_port}{self.__api_kb_status}'
        r = exec_request(self.__kb_session,
                         url,
                         method='GET',
                         timeout=self.settings.connection_timeout)
        response = r.json()
        local = response.get('localKnowledgeBase')
        remote = response.get('remoteKnowledgeBase')
        status = {'status': response.get('status'),
                  'local_updated': local.get('lastUpdate'),
                  'local_current_revision': local.get('localRevision'),
                  'local_global_revision': local.get('globalRevision'),
                  'kb_db_name': remote.get('name')}

        self.log.info('status=success, action=get_health_kb_status, msg="Got KB status", '
                      'hostname="{}"'.format(self.__core_hostname))

        return status

    def get_error_messages(self) -> list:
        """
        Позволяет получить список сообщений в HealthMonitor'e

        :return: list.
        example returned data:
        [{'time': '2023-09-15T10:18:21.0000000Z', 'status': 'warning', 'displayName': 'Core Deployment Configuration',
        'componentName': 'Update and Configuration Service',
        'message': 'Компонент Update and Configuration Service на узле https://10.0.0.1:9035 недоступен.
        \nОт Core Deployment Configuration\nна узле example.ru (10.0.0.2)'},
        {'time': '2023-11-26T15:44:11.0000000Z', 'status': 'warning', 'displayName': 'SIEM Server correlator',
        'componentName': None, 'message': 'Некоторые правила корреляции были приостановлены, поскольку срабатывали слишком часто.
        \nОт SIEM Server correlator\nна узле primer.example.ru (10.0.0.3)'}]
        """

        if self.get_health_status() == "ok":
            return [{"time": datetime.utcfromtimestamp(time.time()).strftime('%Y-%m-%dT%H:%M:%S.0000000Z'),
                     "status": "ok", "message": "Сообщений нет. Система работает нормально"}]

        items = self.get_health_errors()
        if self.__error_patterns is None:  # в кеше нет списка шаблонов
            # time.time() потому что данные могут быть кешированы и дабы избежать добавляется текущие время для макс актуальности
            api_url = self.__api_error_pattern_messages_new.format(time.time()) \
                if int(self.__core_version.split('.')[0]) >= 25 \
                else self.__api_error_pattern_messages_old.format(time.time())
            url = f"https://{self.__core_hostname}{api_url}"
            r = exec_request(self.__core_session, url, method='GET', timeout=self.settings.connection_timeout)
            self.__error_patterns = json.loads(str(r.text).encode('utf-8'))  # тут могут быть траблы с кодировкой, поэтому utf-8
            self.__error_patterns = {pattern.lower(): self.__error_patterns[pattern]
                                     for pattern in self.__error_patterns}

        prefix, rows = "navigation.notifications.message", []
        for item in items:
            type_err, type_err_sensitive = item['type'].replace('.', '').lower(), ""
            if item['sensitive']:
                type_err_sensitive = f"{type_err}sensitive"

            # если существует ключ с добавлением sensitive, то берём этот шаблон, иначе без sensitive
            pattern = self.__error_patterns[f"{prefix}.{type_err_sensitive}"
            if f"{prefix}.{type_err_sensitive}" in self.__error_patterns.keys()
            else f"{prefix}.{type_err}"]

            # замением все значения типа {value} в шаблоне. Нужные значения хранятся в виде json по ключу parameters
            params = item.get("parameters")
            for param in params:
                if param == "ipAddresses":
                    params[param] = f" ({', '.join([adr for adr in params[param]])})"
                pattern = pattern.replace('{' + param + '}', str(params[param]))
                pattern = pattern.replace('{{' + param + '}}', str(params[param]))

            # Далее танцы с бубном, потому что эти значения могут быть, а могут и не быть, поэтому если они есть, то будут добавлены
            if "source" in item.keys() and "displayName" in item['source']:
                pattern += f"\nОт {item['source']['displayName']}"
            if "source" in item.keys() and "hostName" in item['source']:
                addresses = ""
                if "source" in item.keys() and "ipAddresses" in item['source']:
                    addresses = ', '.join([adr for adr in item['source']['ipAddresses']])
                pattern += f"\nна узле {item['source']['hostName']} ({addresses})"
            rows.append({"time": item['timestamp'], "status": item['status'], "displayName": item['displayName'],
                         "componentName": item['componentName'], "message": pattern})

        return rows

    def close(self):
        if self.__core_session is not None:
            self.__core_session.close()
