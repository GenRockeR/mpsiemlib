from typing import List

from mpsiemlib.common import (
    ModuleInterface,
    MPSIEMAuth,
    LoggingHandler,
    MPComponents,
    Settings,
)
from mpsiemlib.common import exec_request


class HealthMonitor(ModuleInterface, LoggingHandler):
    """Health monitor module."""

    __api_global_status = "/api/health_monitoring/v2/total_status"
    __api_checks = "/api/health_monitoring/v2/checks?limit={}&offset={}"
    __api_license_status = "/api/licensing/v2/license_validity"
    __api_licence_status_r27 = "/api/licensing/v4/licenses"
    __api_agents_status = "/api/components/agent"
    __api_agents_status_new = "/api/v1/scanner_agents"
    __api_kb_status = "/api/v1/knowledgeBase"
    __kb_port = 8091
    __mc_port = 3334

    def __init__(self, auth: MPSIEMAuth, settings: Settings):
        ModuleInterface.__init__(self, auth, settings)
        LoggingHandler.__init__(self)
        self.__core_session = auth.sessions["core"]
        self.__core_hostname = auth.creds.core_hostname
        self.__core_version = auth.get_core_version()
        # self.__kb_session = auth.connect(MPComponents.KB)  # noqa

    def get_health_status(self) -> str:
        """Получить общее состояние системы.

        :return: "ok" - если нет ошибок
        """
        url = f"https://{self.__core_hostname}{self.__api_global_status}"
        response = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout).json()
        status = response.get("status")

        self.log.info(f'status=success, action=get_health_status, msg="Got global status", '
                      f'hostname={self.__core_hostname!r} status={status!r}')

        return status

    def get_health_errors(self) -> List[dict]:
        """Получить список ошибок из семафора.

        :return: Список ошибок или пустой массив, если ошибок нет
        """
        limit = 1000
        offset = 0
        api_url = self.__api_checks.format(limit, offset)
        url = f"https://{self.__core_hostname}{api_url}"
        response = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout).json()
        errors = response.get("items")

        ret = []
        for i in errors:
            source = i.get("source") if i.get("source") is not None else {}
            params = i.get("parameters") if i.get("parameters") is not None else {}
            ret.append(
                {
                    "id": i.get("id"),
                    "timestamp": i.get("timestamp"),
                    "status": i.get("status", "").lower(),
                    "type": i.get("type", "").lower(),
                    "name": source.get("displayName").lower(),
                    "hostname": source.get("hostName"),
                    "ip": source.get("ipAddresses"),
                    "component_name": params.get("componentName"),
                    "component_hostname": params.get("hostName"),
                    "component_ip": params.get("ipAddresses"),
                }
            )

        self.log.info(f'status=success, action=get_health_errors, msg="Got errors", '
                      f'hostname={self.__core_hostname!r} count={len(errors)!r}')

        return ret

    def get_health_license_status(self) -> dict:
        """Получить статус лицензии.

        :return: Dict
        """
        if int(self.__core_version.split(".")[0]) < 27:
            url = f"https://{self.__core_hostname}{self.__api_license_status}"
        else:
            url = f"https://{self.__core_hostname}:{self.__mc_port}{self.__api_licence_status_r27}"

        response = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout).json()

        if int(self.__core_version.split(".")[0]) < 27:
            lic = response.get("license")

            status = {
                'valid': response.get('validity') == 'valid',
                'key': lic.get('keyNumber'),
                'type': lic.get('licenseType'),
                'granted': lic.get('keyDate'),
                'expiration': lic.get('expirationDate'),
                'assets': lic.get('assetsCount'),
            }
        else:
            lic = response[0].get('license')
            status = {
                'id': lic.get('id'),
                'key': lic.get('licenseFile').get('licenseNumber'),
                'type': lic.get('licenseFile').get('general').get('licenseType'),
                'mode': lic.get("licenseFile").get("general").get('licenseMode'),
                'current_eps': lic.get('licenseFile').get('workloads')[0].get('current'),
                'eps': lic.get('licenseFile').get('workloads')[0].get('quantity'),
                'expiration': lic.get('gracePeriodExpiredTime')
            }

        self.log.info(f'status=success, action=get_health_license_status, msg="Got license status", '
                      f'hostname={self.__core_hostname!r}')

        return status

    def get_health_agents_status(self) -> List[dict]:
        """Получить статус агентов.

        :return: Список агентов и их параметры.
        """
        if int(self.__core_version.split(".")[0]) < 25:
            url = f"https://{self.__core_hostname}{self.__api_agents_status}"
        else:
            url = f"https://{self.__core_hostname}{self.__api_agents_status_new}"

        response = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout).json()

        agents = []
        for i in response:
            agents.append(
                {
                    "id": i.get("id"),
                    "name": i.get("name"),
                    "hostname": i.get("address"),
                    "version": i.get("version"),
                    "updates": i.get("availableUpdates"),
                    "status": i.get("status"),
                    "roles": i.get("roleNames"),
                    "ip": i.get("ipAddresses"),
                    "platform": i.get("platform"),
                    "modules": i.get("modules"),
                }
            )

        self.log.info(f'status=success, action=get_health_agents_status, msg="Got agents status", '
                      f'hostname={self.__core_hostname!r} count={len(agents)!r}')

        return agents

    def get_health_kb_status(self) -> dict:
        """Получить статус обновления VM контента в Core.

        :return: dict.
        """
        url = f"https://{self.__core_hostname}:{self.__kb_port}{self.__api_kb_status}"
        response = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout).json()

        local = response.get("localKnowledgeBase")
        remote = response.get("remoteKnowledgeBase")
        status = {
            "status": response.get("status"),
            "local_updated": local.get("lastUpdate"),
            "local_current_revision": local.get("localRevision"),
            "local_global_revision": local.get("globalRevision"),
            "kb_db_name": remote.get("name"),
        }

        self.log.info(f'status=success, action=get_health_kb_status, msg="Got KB status", '
                      f'hostname={self.__core_hostname!r}')

        return status

    def close(self):
        if self.__core_session is not None:
            self.__core_session.close()
