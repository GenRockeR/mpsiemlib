from collections.abc import Iterator
from typing import Any, cast

from mpsiemlib.common import (
    AuthError,
    LoggingHandler,
    ModuleInterface,
    MPSIEMAuth,
    Settings,
    exec_request,
)


class HealthMonitor(ModuleInterface, LoggingHandler):
    """Health monitor module."""

    __api_global_status = "/api/health_monitoring/v2/total_status"
    __api_checks = "/api/health_monitoring/v2/checks"
    __api_license_status = "/api/licensing/v2/license_validity"
    # Licensing v4 - на Marketing Core (порт __mc_port), доступен с R27.0
    __api_licence_status_r27 = "/api/licensing/v4/licenses"
    __api_licenses_archive = "/api/licensing/v4/licenses/archive"
    __api_license_files = "/api/licensing/v4/licenses/download_license_files"
    __api_licensing_archive = (
        "/api/licensing/v4/licenses/offline_activation/download_archive_for_licensing"
    )
    __api_installation_key = "/api/licensing/v4/installation_key"
    __api_platform_activation_status = "/api/licensing/v4/platform_activation_status"
    __api_agents_status = "/api/v1/scanner_agents"
    __api_agents_available = "/api/agents/available"
    __api_agents_by_ids = "/api/agents/scanner_agents"
    __api_agents_validate_delete = "/api/agents/validateDelete"
    __api_kb_status = "/api/v1/knowledgeBase"
    __checks_limit = 1000
    __kb_port = 8091
    __mc_port = 3334

    def __init__(self, auth: MPSIEMAuth, settings: Settings) -> None:
        ModuleInterface.__init__(self, auth, settings)
        LoggingHandler.__init__(self)
        if auth.sessions is None or "core" not in auth.sessions:
            raise AuthError("Core session is not initialized")
        creds = auth.get_creds()
        if creds is None or creds.core_hostname is None:
            raise AuthError("Core hostname is not set")
        self.__core_session = auth.sessions["core"]
        self.__core_hostname = creds.core_hostname
        self.__core_version = auth.get_core_version()
        # Релиз ядра (MAJOR, MINOR) из версии "27.6.40521". Сравнение кортежем,
        # а не float: "26.10" превращается в 26.1 и неотличим от 26.1.
        version_parts = self.__core_version.split(".")
        self.__core_release: tuple[int, int] = (
            int(version_parts[0]),
            int(version_parts[1]),
        )
        self.log.debug(
            f'status=success, action=prepare, msg="HealthMonitor Module init", '
            f"hostname={self.__core_hostname!r}, version={self.__core_version!r}"
        )

    def get_health_status(self) -> str:
        """Получить общее состояние системы.

        :return: "ok" - если нет ошибок
        """
        url = f"https://{self.__core_hostname}{self.__api_global_status}"
        response: dict[str, Any] = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
        ).json()
        # status - обязательное поле HealthTotal по контракту
        status = cast("str", response.get("status"))

        self.log.info(
            f'status=success, action=get_health_status, msg="Got global status", '
            f"hostname={self.__core_hostname!r}, status={status!r}"
        )

        return status

    def get_health_errors(self) -> list[dict[str, Any]]:
        """Получить список ошибок из семафора.

        Обходит все страницы `/checks` до `totalItems` (контракт
        health_monitoring_v2): API отдаёт не более `limit` записей за запрос.

        :return: Список ошибок или пустой массив, если ошибок нет
        """
        ret: list[dict[str, Any]] = []
        for issue in self.__iterate_checks():
            source: dict[str, Any] = issue.get("source") or {}
            params: dict[str, Any] = issue.get("parameters") or {}
            application: dict[str, Any] = source.get("application") or {}
            ret.append(
                {
                    "id": issue.get("id"),
                    "timestamp": issue.get("timestamp"),
                    "status": (issue.get("status") or "").lower(),
                    "type": (issue.get("type") or "").lower(),
                    "sensitive": issue.get("sensitive"),
                    "message": issue.get("message"),
                    "link": issue.get("link"),
                    "name": (source.get("displayName") or "").lower(),
                    "hostname": source.get("hostName"),
                    "ip": source.get("ipAddresses"),
                    "application": application.get("displayName"),
                    "component_name": params.get("componentName"),
                    "component_hostname": params.get("hostName"),
                    "component_ip": params.get("ipAddresses"),
                }
            )

        self.log.info(
            f'status=success, action=get_health_errors, msg="Got errors", '
            f"hostname={self.__core_hostname!r}, count={len(ret)!r}"
        )

        return ret

    def __iterate_checks(self) -> Iterator[dict[str, Any]]:
        """Обойти постранично проверки семафора (`/checks`).

        :return: Итератор сырых элементов `HealthIssue`
        """
        offset = 0
        total_read = 0
        total_items = 0

        while True:
            api_url = f"{self.__api_checks}?limit={self.__checks_limit}&offset={offset}"
            response: dict[str, Any] = exec_request(
                self.__core_session,
                f"https://{self.__core_hostname}{api_url}",
                method="GET",
                timeout=self.settings.connection_timeout,
            ).json()

            items: list[dict[str, Any]] = response.get("items") or []
            if offset == 0:
                total_items = int(response.get("totalItems") or len(items))

            yield from items

            total_read += len(items)
            if len(items) < self.__checks_limit or total_read >= total_items:
                break
            offset += self.__checks_limit

        if total_items > total_read:
            self.log.warning(
                f"hostname={self.__core_hostname!r}, status=failed, "
                f'action=get_health_errors, msg="Semaphore return {total_items!r} '
                f'issues, but only {total_read!r} read"'
            )

    def get_health_license_status(self) -> dict[str, Any]:
        """Получить статус лицензии.

        С R27.0 лицензия отдаёт Marketing Core (`/api/licensing/v4/licenses`),
        до R27 - Core (`/api/licensing/v2/license_validity`).

        :return: Dict
        """
        if self.__core_release < (27, 0):
            url = f"https://{self.__core_hostname}{self.__api_license_status}"
        else:
            url = (
                f"https://{self.__core_hostname}:{self.__mc_port}"
                f"{self.__api_licence_status_r27}"
            )

        raw_response: Any = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
        ).json()

        if self.__core_release < (27, 0):
            response: dict[str, Any] = raw_response
            lic: dict[str, Any] = response.get("license") or {}

            status = {
                "valid": response.get("validity") == "valid",
                "key": lic.get("keyNumber"),
                "type": lic.get("licenseType"),
                "granted": lic.get("keyDate"),
                "expiration": lic.get("expirationDate"),
                "assets": lic.get("assetsCount"),
            }
        else:
            licenses: list[dict[str, Any]] = raw_response
            if len(licenses) == 0:
                raise ValueError(
                    f"Core {self.__core_version}: Licensing return empty license list"
                )

            lic = licenses[0].get("license") or {}
            workloads: list[dict[str, Any]] = (lic.get("licenseFile") or {}).get(
                "workloads"
            ) or []
            # Рабочие нагрузки различаются по `code` (Assets/Events), контракт
            # не гарантирует порядок: для EPS-статуса берём первую, как раньше
            workload: dict[str, Any] = workloads[0] if len(workloads) > 0 else {}

            status = {
                **self.__license_common_fields(lic),
                "current_eps": workload.get("current"),
                "eps": workload.get("quantity"),
            }

        self.log.info(
            f"status=success, action=get_health_license_status, "
            f'msg="Got license status", hostname={self.__core_hostname!r}'
        )

        return status

    def get_health_licenses(self) -> list[dict[str, Any]]:
        """Получить все валидные лицензии с привязанными продуктами (R27.0+).

        Контракт: `licensing_v4` `GET /licenses` (GetLicensesWithProducts).

        :return: Список лицензий и привязанных продуктов
        """
        url = self.__marketing_core_url(self.__api_licence_status_r27)
        response: list[dict[str, Any]] = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
        ).json()

        ret: list[dict[str, Any]] = []
        for item in response:
            lic: dict[str, Any] = item.get("license") or {}
            product: dict[str, Any] = item.get("product") or {}
            ret.append(
                {
                    **self.__license_common_fields(lic),
                    **self.__license_workloads(lic),
                    "product_id": product.get("id"),
                    "product_name": product.get("name"),
                    "product_version": product.get("version"),
                }
            )

        self.log.info(
            f"status=success, action=get_health_licenses, "
            f'msg="Got licenses", hostname={self.__core_hostname!r}, '
            f"count={len(ret)!r}"
        )

        return ret

    def get_health_archive_licenses(self) -> list[dict[str, Any]]:
        """Получить лицензии из архива (R27.0+).

        Контракт: `licensing_v4` `GET /licenses/archive` (GetArchiveLicenses).

        :return: Список архивных лицензий
        """
        url = self.__marketing_core_url(self.__api_licenses_archive)
        response: list[dict[str, Any]] = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
        ).json()

        ret = [self.__license_common_fields(lic) for lic in response]

        self.log.info(
            f"status=success, action=get_health_archive_licenses, "
            f'msg="Got archive licenses", hostname={self.__core_hostname!r}, '
            f"count={len(ret)!r}"
        )

        return ret

    def get_health_license_by_product(self, product_id: str) -> dict[str, Any]:
        """Получить используемую лицензию по идентификатору продукта (R27.0+).

        Контракт: `licensing_v4` `GET /licenses/{productId}`
        (GetUsedLicenseByProductId).

        :param product_id: Уникальный идентификатор продукта
        :return: Статус использования и поля лицензии
        """
        url = self.__marketing_core_url(f"{self.__api_licence_status_r27}/{product_id}")
        response: dict[str, Any] = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
        ).json()

        lic: dict[str, Any] = response.get("license") or {}
        status = {
            "use_license": response.get("useLicense"),
            **self.__license_common_fields(lic),
            **self.__license_workloads(lic),
        }

        self.log.info(
            f"status=success, action=get_health_license_by_product, "
            f'msg="Got license by product", hostname={self.__core_hostname!r}, '
            f"product_id={product_id!r}"
        )

        return status

    def get_health_platform_activation_status(self) -> dict[str, Any]:
        """Получить статус активации платформы (R27.0+).

        Контракт: `licensing_v4` `GET /platform_activation_status`
        (GetPlatformActivationStatus).

        :return: {"status": "activated", "revoked_time": "..."}
        """
        url = self.__marketing_core_url(self.__api_platform_activation_status)
        response: dict[str, Any] = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
        ).json()

        status = {
            "status": response.get("status"),
            "revoked_time": response.get("revokedTime"),
        }

        self.log.info(
            f"status=success, action=get_health_platform_activation_status, "
            f'msg="Got platform activation status", '
            f"hostname={self.__core_hostname!r}, status={status['status']!r}"
        )

        return status

    def get_health_license_files(
        self,
        license_ids: list[str],
        local_filepath: str | None = None,
    ) -> bytes:
        """Скачать файлы выбранных лицензий (R27.0+).

        Контракт: `licensing_v4` `GET /licenses/download_license_files`
        (DownloadLicenseFiles). Сервер отвечает zip-архивом
        (`application/octet-stream`), а на неизвестные/пустые идентификаторы —
        пустым zip (HTTP 200), поэтому пустой результат — не ошибка.

        :param license_ids: Идентификаторы лицензий
        :param local_filepath: Необязательный путь, куда сохранить архив
        :return: Содержимое архива
        """
        if not license_ids:
            raise ValueError("license_ids must not be empty")

        url = self.__marketing_core_url(self.__api_license_files)
        response = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
            params={"licenseIds": license_ids},
        )
        archive = response.content

        if local_filepath is not None:
            with open(local_filepath, "wb") as license_file:
                license_file.write(archive)

        self.log.info(
            f"status=success, action=get_health_license_files, "
            f'msg="Got license files", hostname={self.__core_hostname!r}, '
            f"count={len(license_ids)!r}, size={len(archive)!r}, "
            f"filepath={local_filepath!r}"
        )

        return archive

    def get_health_licensing_archive(
        self,
        local_filepath: str | None = None,
    ) -> bytes:
        """Скачать архив для передачи в отдел лицензирования (R27.0+).

        Контракт: `licensing_v4`
        `GET /licenses/offline_activation/download_archive_for_licensing`
        (DownloadArchiveForLicensing). Сервер отвечает zip-архивом с отпечатком
        установки (`fingerprint.zip`), который используется для офлайн
        активации.

        :param local_filepath: Необязательный путь, куда сохранить архив
        :return: Содержимое архива
        """
        url = self.__marketing_core_url(self.__api_licensing_archive)
        response = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
        )
        archive = response.content

        if local_filepath is not None:
            with open(local_filepath, "wb") as archive_file:
                archive_file.write(archive)

        self.log.info(
            f"status=success, action=get_health_licensing_archive, "
            f'msg="Got licensing archive", hostname={self.__core_hostname!r}, '
            f"size={len(archive)!r}, filepath={local_filepath!r}"
        )

        return archive

    def get_health_installation_key(self) -> str:
        """Получить installation_key (R27.0+).

        Контракт: `licensing_v4` `GET /installation_key`
        (GetInstallationKey). Ключ используется клиентом для авторизации на
        внешнем сервере обновлений.

        :return: Installation key
        :raises requests.HTTPError: 403 forbidden (недостаточно прав у учётной
            записи) или 400, если ключ на ядре не заведен
        """
        url = self.__marketing_core_url(self.__api_installation_key)
        response = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
        )
        # Контракт объявляет `type: string`, а не JSON-объект, поэтому читаем
        # текст ответа и снимаем обрамляющие кавычки, если сервер вернул JSON
        installation_key = response.text.strip().strip('"')

        self.log.info(
            f"status=success, action=get_health_installation_key, "
            f'msg="Got installation key", hostname={self.__core_hostname!r}, '
            f"length={len(installation_key)!r}"
        )

        return installation_key

    @staticmethod
    def __license_common_fields(lic: dict[str, Any]) -> dict[str, Any]:
        """Привести объект `License` (licensing_v4) к плоскому словарю."""
        license_file: dict[str, Any] = lic.get("licenseFile") or {}
        general: dict[str, Any] = license_file.get("general") or {}

        return {
            "id": lic.get("id"),
            "key": license_file.get("licenseNumber"),
            "type": general.get("licenseType"),
            "mode": general.get("licenseMode"),
            "product": general.get("productName"),
            "granted": lic.get("createdTime"),
            "expiration": lic.get("gracePeriodExpiredTime"),
            "token_expiration": license_file.get("tokenExpirationTime"),
            "is_archived": lic.get("isArchived"),
            "is_grace_period_expired": lic.get("isGracePeriodExpired"),
            "alerts": [alert.get("alertType") for alert in lic.get("alerts") or []],
        }

    @staticmethod
    def __license_workloads(lic: dict[str, Any]) -> dict[str, Any]:
        """Развернуть `workloads` лицензии в словарь по коду нагрузки."""
        license_file: dict[str, Any] = lic.get("licenseFile") or {}

        return {
            "workloads": {
                workload.get("code"): {
                    "name": workload.get("localizedName"),
                    "current": workload.get("current"),
                    "quantity": workload.get("quantity"),
                }
                for workload in license_file.get("workloads") or []
            }
        }

    def __marketing_core_url(self, api: str) -> str:
        """Собрать URL Marketing Core (licensing_v4 доступен с R27.0).

        :param api: Путь эндпоинта
        :return: Полный URL
        """
        if self.__core_release < (27, 0):
            raise ValueError(
                f"Core {self.__core_version}: licensing_v4 требует R27.0+, "
                f"для более старых ядер используйте get_health_license_status"
            )

        return f"https://{self.__core_hostname}:{self.__mc_port}{api}"

    def get_health_agents_status(self) -> list[dict[str, Any]]:
        """Получить статус агентов.

        Контракт: `Agents.yaml` `GET /api/v1/scanner_agents` (GetAgentList).

        :return: Список агентов и их параметры.
        """
        response: list[dict[str, Any]] = exec_request(
            self.__core_session,
            f"https://{self.__core_hostname}{self.__api_agents_status}",
            method="GET",
            timeout=self.settings.connection_timeout,
        ).json()

        agents = [self.__make_agent_view(agent) for agent in response]

        self.log.info(
            f"status=success, action=get_health_agents_status, "
            f'msg="Got agents status", hostname={self.__core_hostname!r}, '
            f"count={len(agents)!r}"
        )

        return agents

    def get_health_available_agents(self) -> list[dict[str, Any]]:
        """Получить список доступных агентов (короткий вид).

        Контракт: `Agents.yaml` `GET /api/agents/available`
        (GetAvailableAgents).

        :return: Список доступных агентов
        """
        response: list[dict[str, Any]] = exec_request(
            self.__core_session,
            f"https://{self.__core_hostname}{self.__api_agents_available}",
            method="GET",
            timeout=self.settings.connection_timeout,
        ).json()

        agents = [self.__make_agent_view(agent) for agent in response]

        self.log.info(
            f"status=success, action=get_health_available_agents, "
            f'msg="Got available agents", hostname={self.__core_hostname!r}, '
            f"count={len(agents)!r}"
        )

        return agents

    def get_health_agents_by_ids(self, agent_ids: list[str]) -> list[dict[str, Any]]:
        """Получить перечень агентов по их идентификаторам.

        Контракт: `Agents.yaml` `POST /api/agents/scanner_agents`
        (GetAgentsByIds).

        :param agent_ids: Идентификаторы агентов
        :return: Список агентов и их параметров
        """
        if not agent_ids:
            raise ValueError("agent_ids must not be empty")

        response: list[dict[str, Any]] = exec_request(
            self.__core_session,
            f"https://{self.__core_hostname}{self.__api_agents_by_ids}",
            method="POST",
            timeout=self.settings.connection_timeout,
            json=agent_ids,
        ).json()

        agents = [self.__make_agent_view(agent) for agent in response]

        self.log.info(
            f"status=success, action=get_health_agents_by_ids, "
            f'msg="Got agents by ids", hostname={self.__core_hostname!r}, '
            f"count={len(agents)!r}"
        )

        return agents

    def validate_health_agents_delete(self, agent_ids: list[str]) -> int:
        """Проверить агентов на наличие задач перед удалением.

        Контракт: `Agents.yaml` `POST /api/agents/validateDelete`
        (AgentsDeleteValidate). Запрос read-only: удаляющие ветки
        (`startDelete`/`checkDeleteStatus`) модуль не вызывает.

        :param agent_ids: Идентификаторы агентов
        :return: Количество задач, выполняющихся на агентах
        """
        if not agent_ids:
            raise ValueError("agent_ids must not be empty")

        response: dict[str, Any] = exec_request(
            self.__core_session,
            f"https://{self.__core_hostname}{self.__api_agents_validate_delete}",
            method="POST",
            timeout=self.settings.connection_timeout,
            json=agent_ids,
        ).json()
        # totalJobsCount - обязательное поле AgentJobResult по контракту
        total_jobs = cast("int", response.get("totalJobsCount"))

        self.log.info(
            f"status=success, action=validate_health_agents_delete, "
            f'msg="Validated agents before delete", '
            f"hostname={self.__core_hostname!r}, "
            f"count={len(agent_ids)!r}, total_jobs={total_jobs!r}"
        )

        return total_jobs

    @staticmethod
    def __make_agent_view(agent: dict[str, Any]) -> dict[str, Any]:
        """Привести `AgentView`/`AgentShortView` к плоскому словарю."""
        return {
            "id": agent.get("id"),
            "name": agent.get("name"),
            "hostname": agent.get("address"),
            "version": agent.get("version"),
            "product_version": agent.get("productVersion"),
            "updates": agent.get("availableUpdates"),
            "status": agent.get("status"),
            "roles": agent.get("roleNames"),
            "ip": agent.get("ipAddresses"),
            "platform": agent.get("platform"),
            "modules": agent.get("modules"),
            "siem_id": agent.get("siemId"),
        }

    def get_health_kb_status(self) -> dict[str, Any]:
        """Получить статус обновления VM контента в Core.

        :return: dict.
        """
        url = f"https://{self.__core_hostname}:{self.__kb_port}{self.__api_kb_status}"
        response: dict[str, Any] = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
        ).json()

        local: dict[str, Any] = response.get("localKnowledgeBase") or {}
        remote: dict[str, Any] = response.get("remoteKnowledgeBase") or {}
        status = {
            "status": response.get("status"),
            "local_updated": local.get("lastUpdate"),
            "local_current_revision": local.get("localRevision"),
            "local_global_revision": local.get("globalRevision"),
            "kb_db_name": remote.get("name"),
        }

        self.log.info(
            f'status=success, action=get_health_kb_status, msg="Got KB status", '
            f"hostname={self.__core_hostname!r}"
        )

        return status

    def close(self) -> None:
        if self.__core_session is not None:
            self.__core_session.close()
