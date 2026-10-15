import time
import uuid
from typing import Any, ClassVar
from urllib.parse import unquote

import requests

from mpsiemlib.common import (
    AuthError,
    LoggingHandler,
    ModuleInterface,
    MPSIEMAuth,
    Settings,
    exec_request,
)


class Tasks(ModuleInterface, LoggingHandler):
    """Tasks module.

    Задачи сканирования, агенты и справочники сканера. Контракты:

    - ``Agents.yaml``          - ``/api/v1/scanner_agents``,
      ``/api/agents/available``, ``/api/agents/scanner_agents``, удаление
      агентов (validateDelete/startDelete/checkDeleteStatus)
    - ``AgentsV2.yaml``        - ``/api/agents/v2`` (ёмкость модулей агента:
      get/upsert/delete)
    - ``ScannerModules.yaml``  - ``GET /api/v1/scanner_modules``
    - ``Profiles.yaml``        - ``/api/scanning/v3/scanner_profiles`` (CRUD,
      pentest), ``/api/scanning/v3/scanner_profile_schemas``
    - ``Credentials.yaml``     - ``/api/v3/credentials`` (CRUD четырёх типов,
      LAPS check_connection), ``/api/v3/credential_tags``,
      ``PUT /api/v4/credentials/certificates/{id}``
    - ``ScannerTasks.yaml``    - ``/api/scanning/v3/scanner_tasks`` (CRUD,
      start/stop/suspend, cleanBookmarks, migration_errors, count, bookmarks,
      validation)
    - ``ScannerTasksV4.yaml``  - ``/api/scanning/v4`` (список/карточка/создание
      v4, следующий запуск, start_connection_check)
    - ``ScannerTaskRuns.yaml`` - ``/api/scanning/v2`` (runs, jobs, job_errors)
    - ``TaskSelections.yaml``  - ``/api/scanning/v3/task_selections``
    - ``TaskBulkOperations.yaml`` - ``/api/scanning/v3/task_bulk_operations``
    - ``TaskReportDownload.yaml`` - ``POST /api/scanning/v4/task_run_report_download``
    - ``ScanDictionaries.yaml`` - ``/api/dictionaries`` (CRUD)
    - ``Scans.Contracts.yaml`` - ``PUT /api/v1/scans/{scanId}``

    MP SIEM 26.0+: ветки под ядра R23 (``/api/v2/scanner_profiles``,
    ``/api/v1/scanner_metatransports``) удалены как мёртвый код.

    Bulk-операции работают через выделение (:meth:`create_task_selection`),
    а не через список id; проверять их безопасно на копиях существующих задач:
    :meth:`copy_task` собирает такую копию (:meth:`get_task_params_for_copy` -
    её параметры).

    Не реализовано осознанно: ``POST /api/v1/scanner_agents`` (404 на R28.0),
    rest-часть ``Scans.Contracts.yaml`` (чтение снимков - вне домена модуля),
    справочник групп задач (GET-пути групп в контрактах сканера отсутствуют).
    """

    __api_agents_list = "/api/v1/scanner_agents"
    __api_agents_available = "/api/agents/available"
    __api_agents_by_ids = "/api/agents/scanner_agents"
    __api_agents_validate_delete = "/api/agents/validateDelete"
    __api_agents_start_delete = "/api/agents/startDelete"
    __api_agents_delete_status = "/api/agents/checkDeleteStatus"
    __api_agents_v2 = "/api/agents/v2"
    __api_modules_list = "/api/v1/scanner_modules"
    __api_profiles_list = "/api/scanning/v3/scanner_profiles"
    __api_profiles_pentest_generate = (
        "/api/scanning/v4/scanner_profiles/generate_profile_for_pentest"
    )
    __api_profiles_pentest_validate = (
        "/api/scanning/v3/scanner_profiles/pentest/validate_vulners_for_pentest"
    )
    __api_profile_schemas = "/api/scanning/v3/scanner_profile_schemas"
    __api_credentials_list = "/api/v3/credentials"
    __api_credentials_v4 = "/api/v4/credentials"
    __api_credentials_laps_check = "/api/v3/credentials/laps_provider/check_connection"
    __api_credential_tags = "/api/v3/credential_tags"
    __api_dictionaries = "/api/dictionaries"
    __api_tasks_list = "/api/scanning/v3/scanner_tasks"
    __api_tasks_list_v2 = "/api/scanning/v2/scanner_tasks"
    __api_tasks_list_v4 = "/api/scanning/v4/scanner_tasks"
    __api_task_selections = "/api/scanning/v3/task_selections"
    __api_bulk_operations = "/api/scanning/v3/task_bulk_operations"
    __api_report_download = "/api/scanning/v4/task_run_report_download"
    __api_runs_list_v2 = "/api/scanning/v2/runs"
    __api_jobs_list_v2 = "/api/scanning/v2/jobs"
    __api_scans = "/api/v1/scans"

    # ScannerTasks.yaml: mainFilter / additionalFilter
    MAIN_FILTERS = ("all", "running", "withWrongParameters", "withWarnings")
    ADDITIONAL_FILTERS = ("all", "scan", "import", "batch", "retro")
    # ScannerModules.yaml: workType
    WORK_TYPES = ("active", "passive", "undefined")
    # ScannerTaskRuns.yaml: RunMode
    RUN_MODES = ("default", "connectionCheck")
    # Credentials.yaml: вид учётной записи для GET /credentials/{type}/{id}
    CREDENTIAL_TYPES = (
        "login_passwords",
        "passwords_only",
        "certificates",
        "laps_provider",
    )
    # Credentials.yaml: ShortCredentialDto.type -> вид для GET/PUT
    CREDENTIAL_TYPE_ALIASES: ClassVar[dict[str, str]] = {
        "LoginPassword": "login_passwords",
        "PasswordOnly": "passwords_only",
        "Certificate": "certificates",
        "LapsProvider": "laps_provider",
    }
    # Profiles.yaml / ScannerTasksV4.yaml: ConnectionCheckMode
    CONNECTION_CHECK_MODES = ("onlyConnection", "connectionAndAccounts")
    # Profiles.yaml / ScannerTasksV4.yaml: ConnectionCheckAgentSelectionMode
    CONNECTION_CHECK_AGENT_SELECTION_MODES = ("single", "all")
    # TaskBulkOperations.yaml: SchedulersState
    SCHEDULERS_STATES = ("enabled", "disabled", "partial")
    # TaskBulkOperations.yaml: BulkOperationStatus
    BULK_OPERATION_STATUSES = ("inProgress", "finished")
    # Scans.Contracts.yaml: source - проверяется сервером; на R28.0 приняты
    # только эти два значения ("Scanner" -> scanSourceIsInvalidError)
    SCAN_SOURCES = ("HostDiscovery", "PenTest")

    # ScannerTasks.yaml ScannerTaskStatus: статусы «задача крутится»
    RUNNING_STATUSES = (
        "preparing",
        "waiting",
        "running",
        "finishing",
        "suspendingManually",
        "suspendingByDeniedPeriod",
    )
    # статусы, из которых допустим запуск
    STARTABLE_STATUSES = (
        "new",
        "finished",
        "imported",
        "suspendedManually",
        "suspendedByDeniedPeriod",
    )
    # статусы, из которых сервер принимает suspend: уже приостанавливаемая или
    # приостановленная задача отвечает 400 "Cannot suspend scanner task."
    SUSPENDABLE_STATUSES = ("preparing", "waiting", "running", "finishing")

    V4_SORT_ORDERS = (
        "byName",
        "byTarget",
        "byProfile",
        "byCreated",
        "byAgent",
        "byCredential",
        "byLastStartTime",
        "byNextStartTime",
    )

    # limit по контрактам runs/jobs/bookmarks
    MAX_BATCH = 1000

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
        self.__agents: dict[str, dict[str, Any]] = {}
        self.__available_agents: dict[str, dict[str, Any]] = {}
        self.__modules: dict[str | None, dict[str, dict[str, Any]]] = {}
        self.__profiles: dict[str, dict[str, Any]] = {}
        self.__profile_schemas: dict[str | None, list[dict[str, Any]]] = {}
        self.__profile_schemas_localization: dict[str, Any] = {}
        self.__credentials: dict[tuple[str, ...], dict[str, dict[str, Any]]] = {}
        self.__credential_tags: list[str] = []
        self.__dictionaries: dict[str, dict[str, Any]] = {}
        self.__tasks: dict[tuple[str, str], dict[str, dict[str, Any]]] = {}
        self.log.debug('status=success, action=prepare, msg="Tasks Module init"')

    def __url(self, api_path: str) -> str:
        return f"https://{self.__core_hostname}{api_path}"

    # ------------------------------------------------------------------ #
    # Справочники сканера
    # ------------------------------------------------------------------ #

    def get_agents_list(self, refresh: bool = False) -> dict[str, dict[str, Any]]:
        """Список агентов сканирования (контракт GetAgentList).

        :param refresh: принудительно обновить кэш
        :return: {agent_id: {name, hostname, version, status, modules, siem_id}}
        """
        if self.__agents and not refresh:
            return {k: dict(v) for k, v in self.__agents.items()}

        url = self.__url(self.__api_agents_list)
        response: list[dict[str, Any]] = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
        ).json()

        self.__agents = {}
        for item in response:
            self.__agents[item["id"]] = {
                "name": item.get("name"),
                "hostname": item.get("address"),
                "version": item.get("version"),
                "status": item.get("status"),
                "modules": item.get("modules"),
                "siem_id": item.get("siemId"),
            }

        self.log.info(
            f'status=success, action=get_agents_list, msg="Got agents list", '
            f"hostname={self.__core_hostname!r}, count={len(self.__agents)}"
        )
        return {k: dict(v) for k, v in self.__agents.items()}

    def get_available_agents_list(
        self, refresh: bool = False
    ) -> dict[str, dict[str, Any]]:
        """Доступные агенты (контракт GetAvailableAgents, v1).

        Подмножество ``get_agents_list()``: те, кого можно назначить задачей.
        Полей меньше (нет ``platform``/``ipAddresses``/``roleNames``).

        :param refresh: принудительно обновить кэш
        :return: {agent_id: {name, address, version, status, modules, siem_id}}
        """
        if self.__available_agents and not refresh:
            return {k: dict(v) for k, v in self.__available_agents.items()}

        url = self.__url(self.__api_agents_available)
        response: list[dict[str, Any]] = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
        ).json()

        self.__available_agents = {}
        for item in response:
            self.__available_agents[item["id"]] = {
                "name": item.get("name"),
                "hostname": item.get("address"),
                "version": item.get("version"),
                "status": item.get("status"),
                "modules": item.get("modules"),
                "siem_id": item.get("siemId"),
            }

        self.log.info(
            f"status=success, action=get_available_agents_list, "
            f'msg="Got available agents list", '
            f"hostname={self.__core_hostname!r}, "
            f"count={len(self.__available_agents)}"
        )
        return {k: dict(v) for k, v in self.__available_agents.items()}

    def get_agents_by_ids(self, agent_ids: list[str]) -> dict[str, dict[str, Any]]:
        """Агенты по идентификаторам (контракт GetAgentsByIds, v1).

        :param agent_ids: идентификаторы агентов
        :return: {agent_id: {...}} - см. :meth:`get_agents_list`; неизвестные
            id молча отсутствуют в ответе (замерено на R28.0)
        """
        url = self.__url(self.__api_agents_by_ids)
        response: list[dict[str, Any]] = exec_request(
            self.__core_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
            json=agent_ids,
        ).json()

        agents: dict[str, dict[str, Any]] = {}
        for item in response:
            agents[item["id"]] = {
                "name": item.get("name"),
                "hostname": item.get("address"),
                "version": item.get("version"),
                "status": item.get("status"),
                "modules": item.get("modules"),
                "siem_id": item.get("siemId"),
            }

        self.log.info(
            f"status=success, action=get_agents_by_ids, "
            f'msg="Got agents by ids", hostname={self.__core_hostname!r}, '
            f"requested={len(agent_ids)}, found={len(agents)}"
        )
        return agents

    def get_agent_module_capacity(
        self, agent_id: str, module_id: str
    ) -> dict[str, Any]:
        """Ёмкость модуля агента (AgentsV2.yaml GetModuleCapacity).

        :param agent_id: UUID агента
        :param module_id: идентификатор модуля агента (``audit``, ``syslog``, ...)
        :return: ``{system_defined_capacity, user_defined_capacity}``
        """
        api_url = f"{self.__api_agents_v2}/{agent_id}/modules/{module_id}/capacity"
        response: dict[str, Any] = self.__get_json(api_url)

        self.log.info(
            f"status=success, action=get_agent_module_capacity, "
            f'msg="Got capacity for agent {agent_id!r} module {module_id!r}", '
            f"hostname={self.__core_hostname!r}"
        )
        return {
            "system_defined_capacity": response.get("systemDefinedCapacity"),
            "user_defined_capacity": response.get("userDefinedCapacity"),
        }

    def set_agent_module_capacity(
        self, agent_id: str, module_id: str, user_defined_capacity: int
    ) -> bool:
        """Задать ёмкость модуля агента вручную (AgentsV2.yaml
        UpsertUserDefinedCapacity, ``PUT /api/agents/v2/.../capacity``).

        :param agent_id: UUID агента
        :param module_id: идентификатор модуля агента
        :param user_defined_capacity: новая ёмкость (0 допустим)
        :return: True при успехе (204)
        :raises requests.RequestException: некорректное значение (400);
            неизвестный агент (500 - как и у GET). Внимание: в отличие от GET,
            PUT на R28.0 `module_id` не валидирует - неизвестный модуль
            принимается (204), хотя GET/DELETE того же пути отдают 404.
        """
        api_url = f"{self.__api_agents_v2}/{agent_id}/modules/{module_id}/capacity"
        self.__put_json(api_url, {"userDefinedCapacity": user_defined_capacity})
        self.__agents.clear()
        self.__available_agents.clear()

        self.log.info(
            f"status=success, action=set_agent_module_capacity, "
            f'msg="Capacity of agent {agent_id!r} module {module_id!r} '
            f'set to {user_defined_capacity}", '
            f"hostname={self.__core_hostname!r}"
        )
        return True

    def delete_agent_module_capacity(self, agent_id: str, module_id: str) -> bool:
        """Сбросить пользовательскую ёмкость модуля агента (AgentsV2.yaml
        DeleteUserDefinedCapacity).

        После сброса ``user_defined_capacity`` становится ``None``, работает
        системное значение.

        :param agent_id: UUID агента
        :param module_id: идентификатор модуля агента
        :return: True при успехе (204)
        """
        api_url = f"{self.__api_agents_v2}/{agent_id}/modules/{module_id}/capacity"
        self.__delete_request(api_url)
        self.__agents.clear()
        self.__available_agents.clear()

        self.log.info(
            f"status=success, action=delete_agent_module_capacity, "
            f'msg="User capacity of agent {agent_id!r} module {module_id!r} '
            f'deleted", hostname={self.__core_hostname!r}'
        )
        return True

    def validate_agents_delete(self, agent_ids: list[str]) -> int:
        """Проверить агентов перед удалением (Agents.yaml AgentsDeleteValidate,
        ``POST /api/agents/validateDelete``).

        Ничего не меняет: сервер считает, сколько подзадач ещё висит на
        агентах.

        :param agent_ids: UUID агентов
        :return: ``totalJobsCount`` - количество подзадач на этих агентах
        """
        body: dict[str, Any] = (
            self.__post_json(self.__api_agents_validate_delete, agent_ids) or {}
        )
        total_jobs = int(body.get("totalJobsCount") or 0)
        self.log.info(
            f"status=success, action=validate_agents_delete, "
            f'msg="Agents {agent_ids!r}: totalJobsCount={total_jobs}", '
            f"hostname={self.__core_hostname!r}"
        )
        return total_jobs

    def delete_agents(self, agent_ids: list[str]) -> str | None:
        """Удалить агентов (Agents.yaml AgentsDelete,
        ``POST /api/agents/startDelete``).

        Операция необратима и применяется к зарегистрированным агентам SIEM -
        на стенде проверяйте только на несуществующих id.

        :param agent_ids: UUID агентов
        :return: токен процесса удаления (``operationId``)
        """
        body: dict[str, Any] = (
            self.__post_json(self.__api_agents_start_delete, agent_ids) or {}
        )
        token = body.get("operationId")
        self.__agents.clear()
        self.__available_agents.clear()

        self.log.info(
            f"status=success, action=delete_agents, "
            f'msg="Agents {agent_ids!r} deletion started, token={token!r}", '
            f"hostname={self.__core_hostname!r}"
        )
        return str(token) if token is not None else None

    def get_agents_delete_status(self, token: str) -> dict[str, Any]:
        """Статус процесса удаления агентов (Agents.yaml AgentsDeleteCheck,
        ``GET /api/agents/checkDeleteStatus/{token}``).

        :param token: токен из :meth:`delete_agents`
        :return: ``AgentDeleteProcessResult`` -
            ``{totalCount, succeedCount, failedCount, errorModel}``
        :raises requests.RequestException: 202 - процесс ещё идёт; 400 -
            токен не принят (на R28.0 токен из startDelete не резолвится:
            ``core.agents.token.invalid`` / ``validation.error: view``)
        """
        api_url = f"{self.__api_agents_delete_status}/{token}"
        response: dict[str, Any] = self.__get_json(api_url)
        self.log.info(
            f"status=success, action=get_agents_delete_status, "
            f'msg="Deletion token {token!r}: total={response.get("totalCount")}, '
            f"succeed={response.get('succeedCount')}, "
            f'failed={response.get("failedCount")}", '
            f"hostname={self.__core_hostname!r}"
        )
        return response

    def get_modules_list(
        self, refresh: bool = False, work_type: str | None = None
    ) -> dict[str, dict[str, Any]]:
        """Список модулей сканирования (контракт GetModules).

        :param refresh: принудительно обновить кэш
        :param work_type: фильтр active|passive|undefined (опционально)
        :return: {module_id: {name, type}}
        """
        if work_type is not None:
            self.__check_enum(work_type, self.WORK_TYPES, "workType")

        if work_type in self.__modules and not refresh:
            return {k: dict(v) for k, v in self.__modules[work_type].items()}

        params: dict[str, str] = {}
        if work_type is not None:
            params["workType"] = work_type

        url = self.__url(self.__api_modules_list)
        response: list[dict[str, Any]] = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
            params=params,
        ).json()

        modules: dict[str, dict[str, Any]] = {}
        for item in response:
            modules[item["id"]] = {
                "name": item.get("name"),
                "type": str(item.get("outputType") or "").lower(),
            }
        self.__modules[work_type] = modules

        self.log.info(
            f'status=success, action=get_modules_list, msg="Got modules list", '
            f"hostname={self.__core_hostname!r}, workType={work_type!r}, "
            f"count={len(modules)}"
        )
        return {k: dict(v) for k, v in modules.items()}

    def get_profiles_list(
        self, refresh: bool = False, module_id: str | None = None
    ) -> dict[str, dict[str, Any]]:
        """Список профилей сканирования (контракт GetAllProfiles).

        :param refresh: принудительно обновить кэш
        :param module_id: фильтр по идентификатору модуля (опционально)
        :return: {profile_id: {name, system, base_profile, module_id, output}}
        """
        if self.__profiles and not refresh and module_id is None:
            return {k: dict(v) for k, v in self.__profiles.items()}

        params: dict[str, str] = {}
        if module_id is not None:
            params["moduleId"] = module_id

        url = self.__url(self.__api_profiles_list)
        response: list[dict[str, Any]] = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
            params=params,
        ).json()

        profiles = self.__map_profiles(response)
        # Кэш держим только по выборке без фильтра, иначе отфильтрованный
        # набор подменил бы полный
        if module_id is None:
            self.__profiles = profiles

        self.log.info(
            f'status=success, action=get_profiles_list, msg="Got profiles list", '
            f"hostname={self.__core_hostname!r}, moduleId={module_id!r}, "
            f"count={len(profiles)}"
        )
        return {k: dict(v) for k, v in profiles.items()}

    def get_profile_info(self, profile_id: str) -> dict[str, Any]:
        """Профиль сканирования с полным набором настроек (контракт
        GetProfile, ``GET /api/scanning/v3/scanner_profiles/{profileId}``).

        :param profile_id: UUID профиля
        :return: карточка профиля (ProfileModel) как есть
        :raises requests.RequestException: профиль не найден (400 с
            ``core.scanningProfile.profile.not.found.error``, замерено на R28.0)
        """
        api_url = f"{self.__api_profiles_list}/{profile_id}"
        response: dict[str, Any] = exec_request(
            self.__core_session,
            self.__url(api_url),
            method="GET",
            timeout=self.settings.connection_timeout,
        ).json()

        self.log.info(
            f"status=success, action=get_profile_info, "
            f'msg="Got profile {profile_id!r}", '
            f"hostname={self.__core_hostname!r}"
        )
        return response

    def get_profile_schemas(
        self, module_id: str | None = None, refresh: bool = False
    ) -> list[dict[str, Any]]:
        """Схемы профилей сканирования (контракт GetAllProfileSchemas).

        Ответ очень большой (963 КБ на R28.0 без фильтра), поэтому результат
        кэшируется отдельно для каждой выборки.

        :param module_id: фильтр по идентификатору модуля
        :param refresh: принудительно обновить кэш
        :return: список ``ProfileSchema`` (``moduleId`` + ``dataSchema``)
        """
        cached = self.__profile_schemas.get(module_id)
        if cached is not None and not refresh:
            return list(cached)

        params: dict[str, str] = {}
        if module_id is not None:
            params["moduleId"] = module_id

        response: list[dict[str, Any]] = exec_request(
            self.__core_session,
            self.__url(self.__api_profile_schemas),
            method="GET",
            timeout=self.settings.connection_timeout,
            params=params,
        ).json()

        self.__profile_schemas[module_id] = response
        self.log.info(
            f"status=success, action=get_profile_schemas, "
            f'msg="Got profile schemas", hostname={self.__core_hostname!r}, '
            f"moduleId={module_id!r}, count={len(response)}"
        )
        return list(response)

    def get_profile_schemas_localization(self, refresh: bool = False) -> dict[str, Any]:
        """Локализации параметров схем профилей (контракт
        GetProfileSchemasLocalization).

        Плоский словарь ``{ключ схемы: перевод}``; применяется к
        :meth:`get_profile_schemas`.

        :param refresh: принудительно обновить кэш
        :return: словарь локализаций
        """
        if self.__profile_schemas_localization and not refresh:
            return dict(self.__profile_schemas_localization)

        response: dict[str, Any] = exec_request(
            self.__core_session,
            self.__url(f"{self.__api_profile_schemas}/localization"),
            method="GET",
            timeout=self.settings.connection_timeout,
        ).json()

        self.__profile_schemas_localization = response
        self.log.info(
            f"status=success, action=get_profile_schemas_localization, "
            f'msg="Got profile schemas localization", '
            f"hostname={self.__core_hostname!r}, keys={len(response)}"
        )
        return dict(response)

    def create_profile(
        self,
        name: str,
        base_profile_id: str,
        description: str = "",
        overrides: dict[str, Any] | None = None,
    ) -> str | None:
        """Создать профиль сканирования (Profiles.yaml CreateProfile,
        ``POST /api/scanning/v3/scanner_profiles``).

        Профиль создаётся как надстройка над базовым (``base_profile_id`` -
        id из :meth:`get_profiles_list`); ``overrides`` перекрывают параметры
        базового профиля (пустой dict - копия без изменений).

        :param name: имя профиля
        :param base_profile_id: UUID базового профиля
        :param description: описание
        :param overrides: перекрытия настроек
        :return: UUID созданного профиля
        """
        body = {
            "name": name,
            "baseProfileId": base_profile_id,
            "description": description,
            "overrides": overrides or {},
        }
        response: dict[str, Any] = (
            self.__post_json(self.__api_profiles_list, body) or {}
        )

        profile_id = response.get("id")
        self.__profiles.clear()
        self.log.info(
            f"status=success, action=create_profile, "
            f'msg="Profile {profile_id!r} ({name!r}) created", '
            f"hostname={self.__core_hostname!r}"
        )
        return str(profile_id) if profile_id is not None else None

    def update_profile(
        self,
        profile_id: str,
        name: str,
        description: str = "",
        overrides: dict[str, Any] | None = None,
    ) -> bool:
        """Обновить профиль сканирования (Profiles.yaml UpdateProfile).

        ``baseProfileId`` у обновления нет - меняется только name/description и
        перекрытия.

        :param profile_id: UUID профиля
        :param name: имя профиля
        :param description: описание
        :param overrides: перекрытия настроек
        :return: True при успехе (204)
        :raises requests.RequestException: профиля нет (400,
            ``core.scanningProfile.profile.not.found.error``)
        """
        body = {
            "name": name,
            "description": description,
            "overrides": overrides or {},
        }
        api_url = f"{self.__api_profiles_list}/{profile_id}"
        self.__put_json(api_url, body)
        self.__profiles.clear()

        self.log.info(
            f"status=success, action=update_profile, "
            f'msg="Profile {profile_id!r} updated", '
            f"hostname={self.__core_hostname!r}"
        )
        return True

    def delete_profile(self, profile_id: str) -> bool:
        """Удалить профиль сканирования (Profiles.yaml DeleteProfile).

        Системный профиль сервер не удаляет.

        :param profile_id: UUID профиля
        :return: True при успехе (204)
        """
        api_url = f"{self.__api_profiles_list}/{profile_id}"
        self.__delete_request(api_url)
        self.__profiles.clear()

        self.log.info(
            f"status=success, action=delete_profile, "
            f'msg="Profile {profile_id!r} deleted", '
            f"hostname={self.__core_hostname!r}"
        )
        return True

    def clear_profile_migration_errors(self, profile_id: str) -> bool:
        """Удалить ошибки миграции профиля (Profiles.yaml
        DeleteMigrationErrors, ``DELETE /scanner_profiles/{id}/migration_errors``).

        :param profile_id: UUID профиля
        :return: True при успехе (204)
        """
        api_url = f"{self.__api_profiles_list}/{profile_id}/migration_errors"
        self.__delete_request(api_url)
        self.__profiles.clear()

        self.log.info(
            f"status=success, action=clear_profile_migration_errors, "
            f'msg="Migration errors of profile {profile_id!r} deleted", '
            f"hostname={self.__core_hostname!r}"
        )
        return True

    def generate_pentest_profile(self, identifiers: list[str]) -> dict[str, Any]:
        """Сгенерировать профиль PenTest по перечню уязвимостей (Profiles.yaml
        GeneratePentestProfile,
        ``POST /api/scanning/v4/scanner_profiles/generate_profile_for_pentest``).

        Профиль при этом не сохраняется - в ответе приходит
        ``{baseProfileId, profile}``.

        :param identifiers: идентификаторы уязвимостей (``CVE-...``)
        :return: ``PenTestProfileResult``
        :raises requests.RequestException: на R28.0 стенда эндпоинт отвечает
            ``500 internal.error`` (база уязвимостей не готова) - замерено
        """
        response: dict[str, Any] = (
            self.__post_json(
                self.__api_profiles_pentest_generate, {"identifiers": identifiers}
            )
            or {}
        )
        self.log.info(
            f"status=success, action=generate_pentest_profile, "
            f'msg="PenTest profile generated for '
            f'{len(identifiers)} identifiers", '
            f"hostname={self.__core_hostname!r}"
        )
        return response

    def validate_vulnerability_for_pentest(self, identifier: str) -> dict[str, Any]:
        """Проверить наличие уязвимости для пентест-сканирования (Profiles.yaml
        ValidateVulnerability).

        Контракт описывает ``GET .../pentest/validate_vulners_for_pentest``, но
        сервер R28.0 отвечает на GET ``405`` - запрос идёт методом POST с тем же
        query-параметром.

        :param identifier: идентификатор уязвимости (``CVE-2017-0144``)
        :return: ``CveLookup``
        :raises requests.RequestException: на R28.0 стенда эндпоинт отвечает
            ``500 internal.error`` независимо от идентификатора - замерено
        """
        response: dict[str, Any] = (
            self.__post_json(
                self.__api_profiles_pentest_validate,
                None,
                params={"identifier": identifier},
            )
            or {}
        )
        self.log.info(
            f"status=success, action=validate_vulnerability_for_pentest, "
            f'msg="Vulnerability {identifier!r} lookup done", '
            f"hostname={self.__core_hostname!r}"
        )
        return response

    @staticmethod
    def __map_profiles(response: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        return {
            str(item["id"]): {
                "name": item.get("name"),
                "system": item.get("isSystem"),
                "base_profile": (
                    str(item.get("baseProfileName")).strip('"')
                    if item.get("baseProfileName") is not None
                    else None
                ),
                "module_id": item.get("moduleId"),
                "output": item.get("output"),
            }
            for item in response
        }

    def get_credentials_list(
        self, refresh: bool = False, credential_tags: list[str] | None = None
    ) -> dict[str, dict[str, Any]]:
        """Список учетных записей сканера (контракт GetCredentials).

        :param refresh: принудительно обновить кэш
        :param credential_tags: фильтр по тэгам. Сервер трактует его не как
            «только с этими тэгами»: в ответ попадают учётки вообще без тэгов
            плюс учётки, у которых есть хотя бы один запрошенный тэг
            (замерено на R28.0). Точную выборку даёт поле ``credential_tags``
            в элементах ответа
        :return: {credential_id: {name, type, description, credential_tags}}
        """
        cache_key = (
            tuple(sorted(credential_tags)) if credential_tags is not None else ()
        )
        cached = self.__credentials.get(cache_key)
        if cached is not None and not refresh:
            return {k: dict(v) for k, v in cached.items()}

        params: dict[str, Any] = {}
        if credential_tags is not None:
            params["credentialTags"] = credential_tags

        url = self.__url(self.__api_credentials_list)
        response: list[dict[str, Any]] = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
            params=params,
        ).json()

        credentials: dict[str, dict[str, Any]] = {}
        for item in response:
            credentials[item["id"]] = {
                "name": item.get("name"),
                "type": item.get("type"),
                "description": item.get("description"),
                "credential_tags": item.get("credentialTags"),
            }
        self.__credentials[cache_key] = credentials

        self.log.info(
            f"status=success, action=get_credentials_list, "
            f'msg="Got credentials list", '
            f"hostname={self.__core_hostname!r}, "
            f"credentialTags={credential_tags!r}, count={len(credentials)}"
        )
        return {k: dict(v) for k, v in credentials.items()}

    def get_credential(
        self, credential_id: str, credential_type: str = "login_passwords"
    ) -> dict[str, Any]:
        """Учетная запись целиком (Credentials.yaml,
        ``GET /api/v3/credentials/{type}/{id}``).

        Список (:meth:`get_credentials_list`) отдаёт только краткую форму, а
        детальный GET привязан к типу учётной записи: GET не своего типа
        отвечает ``500`` (замерено на R28.0), поэтому ``credential_type``
        берётся из поля ``type`` списка (``LoginPassword`` ->
        ``login_passwords``).

        :param credential_id: UUID учётной записи
        :param credential_type: ``login_passwords`` | ``passwords_only`` |
            ``certificates`` | ``laps_provider``
        :return: карточка учётной записи
        """
        self.__check_enum(credential_type, self.CREDENTIAL_TYPES, "credentialType")
        api_url = f"{self.__api_credentials_list}/{credential_type}/{credential_id}"
        response: dict[str, Any] = self.__get_json(api_url)

        self.log.info(
            f"status=success, action=get_credential, "
            f'msg="Got credential {credential_id!r} ({credential_type})", '
            f"hostname={self.__core_hostname!r}"
        )
        return response

    def get_credentials_tags(self, refresh: bool = False) -> list[str]:
        """Перечень тэгов учетных записей (контракт GetCredentialTags).

        :param refresh: принудительно обновить кэш
        :return: список тэгов (``Terminal``, ``SNMP``, ``LDAP``, ...)
        """
        if self.__credential_tags and not refresh:
            return list(self.__credential_tags)

        response: list[str] = self.__get_json(self.__api_credential_tags)

        self.__credential_tags = list(response)
        self.log.info(
            f"status=success, action=get_credentials_tags, "
            f'msg="Got credentials tags", hostname={self.__core_hostname!r}, '
            f"count={len(self.__credential_tags)}"
        )
        return list(self.__credential_tags)

    def get_credential_type(self, credential_id: str) -> str:
        """Вид учётной записи по её UUID.

        GET/PUT учётной записи привязаны к типу (не свой тип -> 500), а тип
        известен только из краткой формы списка, поэтому тип читается из
        :meth:`get_credentials_list`.

        :param credential_id: UUID учётной записи
        :return: значение из ``CREDENTIAL_TYPES``
        :raises ValueError: учётная запись не найдена в списке
        """
        entry = self.get_credentials_list().get(credential_id)
        if entry is None or not entry.get("type"):
            raise ValueError(f"Credential {credential_id!r} not found")
        return self.__credential_type_path(str(entry["type"]))

    def __credential_type_path(self, credential_type: str) -> str:
        for alias, path in self.CREDENTIAL_TYPE_ALIASES.items():
            if credential_type.lower() == alias.lower():
                return path
        self.__check_enum(credential_type, self.CREDENTIAL_TYPES, "credentialType")
        return credential_type

    def create_credential(
        self, credential_type: str, params: dict[str, Any]
    ) -> str | None:
        """Создать учётную запись (Credentials.yaml Create*).

        Тип определяет тело запроса (``CreateLoginPasswordCredentialViewModel``
        и т.п.) и путь; обязательные поля - ``name``, ``credentialTags`` плюс
        специфичные (``login``/``password``/``certificate``/... ).

        :param credential_type: ``login_passwords`` | ``passwords_only`` |
            ``certificates`` | ``laps_provider``
        :param params: viewModel соответствующего типа
        :return: UUID созданной учётной записи
        :raises requests.RequestException: дубль имени (400,
            ``DuplicateCredentialError``) либо неизвестный тэг (400,
            ``CredentialTagNotFoundError``)
        """
        self.__check_enum(credential_type, self.CREDENTIAL_TYPES, "credentialType")
        api_url = f"{self.__api_credentials_list}/{credential_type}"
        response: dict[str, Any] = self.__post_json(api_url, params) or {}

        credential_id = response.get("id")
        self.__credentials.clear()
        self.__credential_tags = []
        self.log.info(
            f"status=success, action=create_credential, "
            f'msg="Credential {credential_id!r} created ({credential_type})", '
            f"hostname={self.__core_hostname!r}"
        )
        return str(credential_id) if credential_id is not None else None

    def update_credential(
        self,
        credential_id: str,
        credential_type: str,
        params: dict[str, Any],
    ) -> bool:
        """Обновить учётную запись (Credentials.yaml Update*).

        Для ``login_passwords``/``passwords_only``/``certificates`` в теле
        обязателен ``shouldResetPassword``
        (``shouldResetCertificatePassword`` - у сертификатов).

        :param credential_id: UUID учётной записи
        :param credential_type: вид учётной записи
            (см. :meth:`get_credential_type`)
        :param params: viewModel соответствующего типа
        :return: True при успехе (204)
        """
        self.__check_enum(credential_type, self.CREDENTIAL_TYPES, "credentialType")
        api_url = f"{self.__api_credentials_list}/{credential_type}/{credential_id}"
        self.__put_json(api_url, params)
        self.__credentials.clear()

        self.log.info(
            f"status=success, action=update_credential, "
            f'msg="Credential {credential_id!r} updated ({credential_type})", '
            f"hostname={self.__core_hostname!r}"
        )
        return True

    def update_credential_v4(self, credential_id: str, params: dict[str, Any]) -> bool:
        """Обновить сертификат-учётку по контракту v4 (Credentials.yaml
        UpdateCertificateV4, ``PUT /api/v4/credentials/certificates/{id}``).

        Отличие от v3: добавлен обязательный ``shouldResetCertificate``.

        :param credential_id: UUID учётной записи типа Certificate
        :param params: ``UpdateCertificateCredentialViewModelV4``
        :return: True при успехе (204)
        """
        api_url = f"{self.__api_credentials_v4}/certificates/{credential_id}"
        self.__put_json(api_url, params)
        self.__credentials.clear()

        self.log.info(
            f"status=success, action=update_credential_v4, "
            f'msg="Credential {credential_id!r} updated (v4)", '
            f"hostname={self.__core_hostname!r}"
        )
        return True

    def delete_credential(self, credential_id: str) -> bool:
        """Удалить учётную запись (Credentials.yaml Delete).

        :param credential_id: UUID учётной записи
        :return: True при успехе (204)
        :raises requests.RequestException: учётку используют другие объекты -
            400 ``core.scanning.credential.dependency.credential.error``
            (замерено на R28.0: LAPS-провайдер ссылается на AD-учётку, удалять
            надо в обратном порядке)
        """
        api_url = f"{self.__api_credentials_list}/{credential_id}"
        self.__delete_request(api_url)
        self.__credentials.clear()
        self.__credential_tags = []

        self.log.info(
            f"status=success, action=delete_credential, "
            f'msg="Credential {credential_id!r} deleted", '
            f"hostname={self.__core_hostname!r}"
        )
        return True

    def check_laps_provider_connection(
        self,
        domain: str,
        port: int,
        ssl: bool,
        active_directory_credentials_id: str,
        search_bases: list[str] | None = None,
    ) -> dict[str, Any]:
        """Проверить подключение LAPS-провайдера к Active Directory
        (Credentials.yaml CheckConnectionLapsProvider).

        :param domain: домен LDAP-сервера
        :param port: порт LDAP-сервера
        :param ssl: использовать SSL
        :param active_directory_credentials_id: UUID учётки для подключения к AD
        :param search_bases: базы поиска
        :return: ``LapsProviderStatusDto`` -
            ``{connectionSuccess, error}``; при недоступном AD
            ``connectionSuccess=False`` и ``error`` вроде
            ``"LDAP server: 'Timeout'. Connect Error  91"`` (замерено на R28.0)
        """
        body = {
            "domain": domain,
            "port": port,
            "ssl": ssl,
            "searchBases": search_bases or [],
            "activeDirectoryCredentialsId": active_directory_credentials_id,
        }
        response: dict[str, Any] = (
            self.__post_json(self.__api_credentials_laps_check, body) or {}
        )
        self.log.info(
            f"status=success, action=check_laps_provider_connection, "
            f'msg="LAPS connection success='
            f'{response.get("connectionSuccess")!r}", '
            f"hostname={self.__core_hostname!r}"
        )
        return response

    def get_transports_list(self, refresh: bool = False) -> dict[str, dict[str, Any]]:
        """Метатранспорты: эндпоинт ``/api/v1/scanner_metatransports``
        отсутствовал ещё в ядрах до 26.0 и не описан ни в одном актуальном
        контракте. Метатранспорты доступны только как поле задачи
        (``metatransports`` в ``TaskListItem``).
        """
        raise NotImplementedError(
            "Metatransports list API is not present in MP SIEM >= 26.0 contracts; "
            "use get_task_info()['metatransports'] instead"
        )

    # ------------------------------------------------------------------ #
    # Справочники (ScanDictionaries.yaml)
    # ------------------------------------------------------------------ #

    def get_dictionaries_list(self, refresh: bool = False) -> dict[str, dict[str, Any]]:
        """Список справочников сканера (контракт GetAll, ``GET /api/dictionaries``).

        Сервер игнорирует фильтры ``isSystem``/``name`` (замерено на R28.0),
        поэтому фильтрация остаётся на стороне вызывающего.

        :param refresh: принудительно обновить кэш
        :return: {dictionary_id: {name, is_system}}
        """
        if self.__dictionaries and not refresh:
            return {k: dict(v) for k, v in self.__dictionaries.items()}

        response: list[dict[str, Any]] = self.__get_json(self.__api_dictionaries)

        self.__dictionaries = {
            item["id"]: {
                "name": item.get("name"),
                "is_system": item.get("isSystem"),
            }
            for item in response
        }

        self.log.info(
            f"status=success, action=get_dictionaries_list, "
            f'msg="Got dictionaries list", hostname={self.__core_hostname!r}, '
            f"count={len(self.__dictionaries)}"
        )
        return {k: dict(v) for k, v in self.__dictionaries.items()}

    def get_dictionary(self, dictionary_id: str) -> dict[str, Any]:
        """Справочник целиком, с содержимым (контракт Get).

        :param dictionary_id: UUID справочника
        :return: ``{id, name, isSystem, content}``; несуществующий id отдаёт
            пустое тело (замерено на R28.0) - тогда вернется пустой dict
        """
        api_url = f"{self.__api_dictionaries}/{dictionary_id}"
        response = self.__request("get_dictionary", api_url)

        if not response.text:
            self.log.warning(
                f"status=failed, action=get_dictionary, "
                f'msg="Dictionary {dictionary_id!r} not found", '
                f"hostname={self.__core_hostname!r}"
            )
            return {}

        payload: dict[str, Any] = response.json()
        self.log.info(
            f"status=success, action=get_dictionary, "
            f'msg="Got dictionary {dictionary_id!r}", '
            f"hostname={self.__core_hostname!r}"
        )
        return payload

    def create_dictionary(self, name: str, content: str) -> dict[str, Any]:
        """Создать справочник сканера (ScanDictionaries.yaml Create,
        ``POST /api/dictionaries``).

        :param name: имя справочника (уникально в системе)
        :param content: содержимое - текст (CSV/список строк)
        :return: созданный ``Dictionary`` - ``{id, name, isSystem, content}``
        :raises requests.RequestException: дубль имени (400,
            ``modelState.name`` - «Справочник с таким названием уже существует»)
        """
        response: dict[str, Any] = (
            self.__post_json(
                self.__api_dictionaries, {"name": name, "content": content}
            )
            or {}
        )
        self.__dictionaries.clear()

        self.log.info(
            f"status=success, action=create_dictionary, "
            f'msg="Dictionary {response.get("id")!r} ({name!r}) created", '
            f"hostname={self.__core_hostname!r}"
        )
        return response

    def update_dictionary(self, dictionary_id: str, name: str, content: str) -> bool:
        """Обновить справочник сканера (ScanDictionaries.yaml Update).

        :param dictionary_id: UUID справочника
        :param name: новое имя
        :param content: новое содержимое
        :return: True при успехе (200 с пустым телом)
        """
        api_url = f"{self.__api_dictionaries}/{dictionary_id}"
        body = {"id": dictionary_id, "name": name, "content": content}
        self.__put_json(api_url, body)
        self.__dictionaries.clear()

        self.log.info(
            f"status=success, action=update_dictionary, "
            f'msg="Dictionary {dictionary_id!r} updated", '
            f"hostname={self.__core_hostname!r}"
        )
        return True

    def delete_dictionary(self, dictionary_id: str) -> bool:
        """Удалить справочник сканера (ScanDictionaries.yaml Delete).

        :param dictionary_id: UUID справочника
        :return: True при успехе (200; сервер отвечает 200 с пустым телом,
            а не 204 - замерено на R28.0)
        """
        api_url = f"{self.__api_dictionaries}/{dictionary_id}"
        self.__delete_request(api_url)
        self.__dictionaries.clear()

        self.log.info(
            f"status=success, action=delete_dictionary, "
            f'msg="Dictionary {dictionary_id!r} deleted", '
            f"hostname={self.__core_hostname!r}"
        )
        return True

    # ------------------------------------------------------------------ #
    # Задачи
    # ------------------------------------------------------------------ #

    def get_tasks_list(
        self,
        refresh: bool = False,
        main_filter: str = "all",
        additional_filter: str = "all",
    ) -> dict[str, dict[str, Any]]:
        """Список задач сканирования (контракт GetScannerTasks, v3).

        :param refresh: принудительно обновить кэш
        :param main_filter: all|running|withWrongParameters|withWarnings
        :param additional_filter: all|scan|import|batch|retro
        :return: {task_id: {name, status, ...}}
        """
        tasks = self.__get_tasks_cache(main_filter, additional_filter, refresh)
        return {k: dict(v) for k, v in tasks.items()}

    def __get_tasks_cache(
        self,
        main_filter: str = "all",
        additional_filter: str = "all",
        refresh: bool = False,
    ) -> dict[str, dict[str, Any]]:
        """Кэш списка задач ключуется фильтром: иначе выборка
        ``main_filter='running'`` подменила бы полный список."""
        self.__check_enum(main_filter, self.MAIN_FILTERS, "mainFilter")
        self.__check_enum(
            additional_filter, self.ADDITIONAL_FILTERS, "additionalFilter"
        )

        cache_key = (main_filter, additional_filter)
        cached = self.__tasks.get(cache_key)
        if cached is not None and not refresh:
            return cached

        params = {
            "mainFilter": main_filter,
            "additionalFilter": additional_filter,
        }
        url = self.__url(self.__api_tasks_list)
        response: list[dict[str, Any]] = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
            params=params,
        ).json()

        tasks: dict[str, dict[str, Any]] = {}
        for item in response:
            profile = item.get("profile") or {}
            tasks[item["id"]] = {
                "name": item.get("name"),
                "agent": item.get("agent"),
                "scope": item.get("scope"),
                "profile": {
                    "id": profile.get("id"),
                    "name": profile.get("name"),
                },
                "module": item.get("module"),
                "transports": item.get("metatransports"),
                "status": item.get("status"),
                "created": item.get("created"),
                "run_last": item.get("lastRun"),
                "run_next": item.get("nextRun"),
                "run_last_error_level": item.get("lastRunErrorLevel"),
                "run_last_error": item.get("lastRunError"),
                "target_include": item.get("include"),
                "target_exclude": item.get("exclude"),
                "status_validation": item.get("validationState"),
                "host_discovery": item.get("hostDiscovery"),
                "bookmarks": item.get("hasBookmarks"),
                "credentials": item.get("credentials"),
                "trigger_parameters": item.get("triggerParameters"),
            }
        self.__tasks[cache_key] = tasks

        self.log.info(
            f'status=success, action=get_tasks_list, msg="Got task list", '
            f"hostname={self.__core_hostname!r}, mainFilter={main_filter!r}, "
            f"additionalFilter={additional_filter!r}, count={len(tasks)}"
        )
        return tasks

    def get_tasks_count(
        self, main_filter: str = "all", additional_filter: str = "all"
    ) -> int:
        """Количество задач (контракт CountScannerTasks).

        :param main_filter: all|running|withWrongParameters|withWarnings
        :param additional_filter: all|scan|import|batch|retro
        :return: ``total``
        """
        self.__check_enum(main_filter, self.MAIN_FILTERS, "mainFilter")
        self.__check_enum(
            additional_filter, self.ADDITIONAL_FILTERS, "additionalFilter"
        )
        params = {
            "mainFilter": main_filter,
            "additionalFilter": additional_filter,
        }
        api_url = f"{self.__api_tasks_list}/count"
        response: dict[str, Any] = self.__get_json(api_url, params=params)
        return int(response.get("total") or 0)

    def get_scope_tasks_count(self, scope_id: str) -> int:
        """Количество задач в инфраструктуре (контракт GetScopeTasksCount,
        ``GET /scanner_tasks/scope/{scopeId}/count``).

        :param scope_id: UUID scope
        :return: ``total``
        """
        api_url = f"{self.__api_tasks_list}/scope/{scope_id}/count"
        response: dict[str, Any] = self.__get_json(api_url)
        return int(response.get("total") or 0)

    def get_tasks_bookmarks(
        self, offset: int = 0, limit: int = MAX_BATCH
    ) -> dict[str, Any]:
        """Savepoint'ы всех задач (контракт GetBookmarks).

        :param offset: смещение страницы
        :param limit: размер страницы (1..1000)
        :return: ``{items, totalItems}``; элемент -
            ``{taskId, scopeId, target, savepoint}``
        """
        self.__check_batch(limit)
        api_url = f"{self.__api_tasks_list}/bookmarks"
        response: dict[str, Any] = self.__get_json(
            api_url, params={"offset": offset, "limit": limit}
        )
        return response

    def validate_task(self, task_id: str) -> bool:
        """Валидация задачи сервером (контракт ValidateScannerTask).

        Успешная валидация отвечает пустым телом (замерено на R28.0); задача с
        некорректными параметрами - ``400 validation.error``.

        :param task_id: ID задачи
        :return: True если сервер не нашёл ошибок
        :raises requests.RequestException: сервер ответил 400
        """
        api_url = f"{self.__api_tasks_list}/{task_id}/validation"
        exec_request(
            self.__core_session,
            self.__url(api_url),
            method="GET",
            timeout=self.settings.connection_timeout,
        )

        self.log.info(
            f"status=success, action=validate_task, "
            f'msg="Task {task_id!r} is valid", '
            f"hostname={self.__core_hostname!r}"
        )
        return True

    def get_task_info(self, task_id: str, refresh: bool = False) -> dict[str, Any]:
        """Информация о задаче с параметрами (контракт GetScannerTask, v3).

        Ответ карточки дополняется полями из кэша списка (``run_next``,
        ``transports`` и др., которых в карточке нет).

        :param task_id: ID задачи
        :param refresh: обновить кэш списка задач
        :return: карточка задачи (ScannerTask)
        :raises requests.RequestException: задача не найдена (404)
        """
        api_url = f"{self.__api_tasks_list}/{task_id}"
        response: dict[str, Any] = self.__get_json(api_url)

        if not self.__tasks or refresh:
            self.__get_tasks_cache(refresh=refresh)

        cached = self.__tasks.get(("all", "all"), {}).get(task_id) or {}
        task_info: dict[str, Any] = dict(cached, **response)

        self.log.info(
            f"status=success, action=get_task_info, "
            f'msg="Got info for task {task_id!r}", '
            f"hostname={self.__core_hostname!r}"
        )
        return task_info

    def get_task_info_v4(self, task_id: str) -> dict[str, Any]:
        """Карточка задачи в контракте v4 (ScannerTasksV4.yaml GetScannerTask).

        Отличается от v3 (замерено на R28.0): вместо ``agent``/``component``
        поля ``agents``/``gen2Agents``/``agentGeneration``, добавлены
        ``output``, ``lastRunMode``, ``lastRunFinished``,
        ``connectionCheckSupported``.

        :param task_id: ID задачи
        :return: карточка задачи (v4 ScannerTask) как есть
        :raises requests.RequestException: задача не найдена (404)
        """
        api_url = f"{self.__api_tasks_list_v4}/{task_id}"
        response: dict[str, Any] = self.__get_json(api_url)

        self.log.info(
            f"status=success, action=get_task_info_v4, "
            f'msg="Got v4 info for task {task_id!r}", '
            f"hostname={self.__core_hostname!r}"
        )
        return response

    def get_tasks_list_v4(
        self,
        filter_body: dict[str, Any] | None = None,
        token: str | None = None,
        offset: int = 0,
        limit: int = MAX_BATCH,
        main_filter: str = "all",
        additional_filter: str = "all",
        order_by: str | None = None,
        order_direction: str | None = None,
        validate: bool = True,
    ) -> dict[str, Any]:
        """Список задач по контракту v4 (ScannerTasksV4.yaml GetScannerTasks,
        ``POST /api/scanning/v4/scanner_tasks``).

        Список читается POST'ом: ``filter`` передаётся телом, остальное -
        query. Сервер требует тело даже пустым (без него - 415). Ответ
        кэшируется сервером под ``token``: переданный ``token`` читает страницы
        из уже построенной выборки.

        Внимание: на R28.0 dev-стенде список стабильно отвечает
        ``totalCount=0`` при 10 задачах в v3-списке и работающем
        :meth:`get_task_info_v4` (замерено) - v4-выборка отдаёт задачи другого
        поколения агентов. Для обычных выборок используйте
        :meth:`get_tasks_list`.

        :param filter_body: ``Filter`` v4 (``text``/``statuses``/``modules``/
            ``profiles``/``credentials``/``scopes``/``groups``/``target``/
            ``agents``)
        :param token: токен ранее построенной выборки
        :param offset: смещение страницы
        :param limit: размер страницы (1..1000)
        :param main_filter: all|running|withWrongParameters|withWarnings
        :param additional_filter: all|scan|import|batch|retro
        :param order_by: ``byName``/``byTarget``/``byProfile``/``byCreated``/
            ``byAgent``/``byCredential``/``byLastStartTime``/
            ``byNextStartTime``
        :param order_direction: ``asc`` | ``desc``
        :param validate: валидировать ли задачи на сервере
        :return: ``TaskListResult`` - ``{items, token, totalCount}``
        """
        self.__check_batch(limit)
        self.__check_enum(main_filter, self.MAIN_FILTERS, "mainFilter")
        self.__check_enum(
            additional_filter, self.ADDITIONAL_FILTERS, "additionalFilter"
        )
        if order_by is not None:
            self.__check_enum(order_by, self.V4_SORT_ORDERS, "orderBy")
        if order_direction is not None:
            self.__check_enum(order_direction, ("asc", "desc"), "orderDirection")

        params: dict[str, Any] = {
            "mainFilter": main_filter,
            "additionalFilter": additional_filter,
            "offset": offset,
            "limit": limit,
            "validate": self.__query_bool(validate),
        }
        if token is not None:
            params["token"] = token
        if order_by is not None:
            params["orderBy"] = order_by
        if order_direction is not None:
            params["orderDirection"] = order_direction

        response: dict[str, Any] = (
            self.__post_json(self.__api_tasks_list_v4, filter_body or {}, params=params)
            or {}
        )

        total_count = response.get("totalCount")
        items_count = len(response.get("items") or [])
        self.log.info(
            f"status=success, action=get_tasks_list_v4, "
            f'msg="Got v4 task list", hostname={self.__core_hostname!r}, '
            f"totalCount={total_count!r}, items={items_count}, "
            f"token={response.get('token')!r}"
        )
        return response

    def calc_next_run_time(self, trigger_parameters: dict[str, Any]) -> str | None:
        """Расчёт времени следующего запуска (ScannerTasksV4.yaml
        CalcNextRunTime, ``POST /scanner_tasks/next_run``).

        :param trigger_parameters: ``TriggerBySchedulerParametersDto``
            (``type`` + ``isEnabled`` обязательны). Удобно взять
            ``get_task_info(task_id)["triggerParameters"]``
        :return: дата следующего запуска (ISO 8601) либо None, если сервер
            не вернул дату
        :raises requests.RequestException: некорректные параметры (400)
        """
        api_url = f"{self.__api_tasks_list_v4}/next_run"
        response: dict[str, Any] = self.__post_json(api_url, trigger_parameters)

        next_run = response.get("date")
        self.log.info(
            f"status=success, action=calc_next_run_time, "
            f'msg="Next run: {next_run!r}", hostname={self.__core_hostname!r}'
        )
        return str(next_run) if next_run is not None else None

    def get_task_status(self, task_id: str, refresh: bool = True) -> str:
        """Статус задачи по её ID.

        :param task_id: ID задачи
        :param refresh: обновить кэш списка перед чтением статуса
        :return: статус из ScannerTaskStatus
        :raises ValueError: задача не найдена
        """
        tasks = self.__get_tasks_cache(refresh=refresh)
        task = tasks.get(task_id)
        if task is None:
            raise ValueError(f"Task {task_id!r} not found")
        return str(task["status"])

    def start_task(self, task_id: str) -> str | None:
        """Запустить задачу, если она не выполняется.

        :param task_id: ID задачи
        :return: ID запуска (TaskRunId) либо None, если запуск не выполнен
        """
        status = self.get_task_status(task_id)
        if status in self.STARTABLE_STATUSES:
            return self.__manipulate_task(task_id, "start")

        self.log.warning(
            f"status=failed, action=manipulate_task, "
            f'msg="Task {task_id!r} not startable from status {status!r}", '
            f"hostname={self.__core_hostname!r}"
        )
        return None

    def stop_task(self, task_id: str) -> bool:
        """Остановить задачу, если она выполняется.

        :param task_id: ID задачи
        :return: True если остановка отправлена, иначе False
        """
        status = self.get_task_status(task_id)
        if status in self.RUNNING_STATUSES:
            self.__manipulate_task(task_id, "stop")
            return True

        self.log.warning(
            f"status=failed, action=manipulate_task, "
            f'msg="Task {task_id!r} not running (status {status!r})", '
            f"hostname={self.__core_hostname!r}"
        )
        return False

    def suspend_task(self, task_id: str) -> bool:
        """Приостановить задачу (ScannerTasks.yaml SuspendScannerTask).

        Отличие от :meth:`stop_task`: сервер принимает suspend только у
        выполняемой задачи; у ``new``/``finished``/уже приостановленной он
        отвечает ``400 "Cannot suspend scanner task."`` (замерено на R28.0).

        :param task_id: ID задачи
        :return: True если приостановка отправлена, иначе False
        """
        status = self.get_task_status(task_id)
        if status in self.SUSPENDABLE_STATUSES:
            self.__manipulate_task(task_id, "suspend")
            return True

        self.log.warning(
            f"status=failed, action=suspend_task, "
            f'msg="Task {task_id!r} not suspendable from status {status!r}", '
            f"hostname={self.__core_hostname!r}"
        )
        return False

    def clean_task_bookmarks(self, task_id: str) -> bool:
        """Сбросить savepoint'ы задачи (ScannerTasks.yaml
        ClearTaskSavePoints, ``POST /scanner_tasks/{id}/cleanBookmarks``).

        :param task_id: ID задачи
        :return: True при успехе (204)
        """
        api_url = f"{self.__api_tasks_list}/{task_id}/cleanBookmarks"
        self.__request("clean_task_bookmarks", api_url, method="POST")
        self.__tasks.clear()

        self.log.info(
            f"status=success, action=clean_task_bookmarks, "
            f'msg="Bookmarks of task {task_id!r} cleared", '
            f"hostname={self.__core_hostname!r}"
        )
        return True

    def clear_task_migration_errors(self, task_id: str) -> bool:
        """Удалить ошибки миграции задачи (ScannerTasks.yaml
        DeleteMigrationErrors, ``DELETE /scanner_tasks/{id}/migration_errors``).

        :param task_id: ID задачи
        :return: True при успехе (204)
        """
        api_url = f"{self.__api_tasks_list}/{task_id}/migration_errors"
        self.__delete_request(api_url)
        self.__tasks.clear()

        self.log.info(
            f"status=success, action=clear_task_migration_errors, "
            f'msg="Migration errors of task {task_id!r} deleted", '
            f"hostname={self.__core_hostname!r}"
        )
        return True

    def start_task_connection_check(
        self,
        task_id: str,
        mode: str = "onlyConnection",
        agent_selection_mode: str = "single",
    ) -> str | None:
        """Запустить задачу в режиме проверки подключения (ScannerTasksV4.yaml
        StartConnectionCheckScannerTask).

        Поля ``agents``/``gen2Agents``/``connectionCheckSupported`` есть только
        в v4-карточке (:meth:`get_task_info_v4`).

        :param task_id: ID задачи
        :param mode: ``onlyConnection`` | ``connectionAndAccounts``
        :param agent_selection_mode: ``single`` | ``all``
        :return: ID запуска (TaskRunId) либо None
        :raises requests.RequestException: задача не поддерживает проверку
            подключения либо лицензия сканера её запрещает
            (``core.scanning.task.license.access.denied``, 400 - замерено на
            R28.0 стенда)
        """
        self.__check_enum(mode, self.CONNECTION_CHECK_MODES, "mode")
        self.__check_enum(
            agent_selection_mode,
            self.CONNECTION_CHECK_AGENT_SELECTION_MODES,
            "agentSelectionMode",
        )

        api_url = f"{self.__api_tasks_list_v4}/{task_id}/start_connection_check"
        body: dict[str, Any] = (
            self.__post_json(
                api_url, {"mode": mode, "agentSelectionMode": agent_selection_mode}
            )
            or {}
        )
        run_id = body.get("id")
        self.log.info(
            f"status=success, action=start_task_connection_check, "
            f'msg="Connection check of task {task_id!r} started, '
            f'run={run_id!r}", hostname={self.__core_hostname!r}'
        )
        return str(run_id) if run_id is not None else None

    def get_task_report(
        self,
        run_id: str,
        status: str | None = None,
        agent_id: str | None = None,
        target_pattern: str | None = None,
    ) -> tuple[bytes, str | None]:
        """Скачать отчёт подзадачи (TaskReportDownload.yaml DownloadTaskReport,
        ``POST /api/scanning/v4/task_run_report_download``).

        Сервер отдаёт xlsx с именем вида ``kafka_23.03.2026 12:46:30.xlsx``
        (имя задачи + время запуска) в ``Content-Disposition``.

        :param run_id: UUID запуска (см. :meth:`get_runs_list`)
        :param status: фильтр по статусу подзадачи (``success``/``error``, ...)
        :param agent_id: фильтр по агенту (UUID)
        :param target_pattern: подстрока цели
        :return: ``(содержимое файла, имя файла)``
        :raises requests.RequestException: запуск не найден (400,
            ``Task run ... not found``)
        """
        body: dict[str, Any] = {
            "runId": run_id,
            "status": status,
            "agentId": agent_id,
            "targetPattern": target_pattern,
        }
        body = {k: v for k, v in body.items() if v is not None}
        return self.__download("get_task_report", self.__api_report_download, body)

    def __manipulate_task(self, task_id: str, control: str) -> str | None:
        api_url = f"{self.__api_tasks_list}/{task_id}/{control}"
        response = self.__request(
            f"{control}_task", api_url, method="POST", timeout_up=2
        )

        run_id: str | None = None
        if control == "start":
            # start отвечает TaskRunId {id}; stop/suspend - 204 без тела
            run_id = self.__payload_or_empty(response).get("id")
            if run_id is None:
                raise RuntimeError("Task start returned no run id")

        self.log.info(
            f"status=success, action=manipulate_task, "
            f'msg="{control} task {task_id!r}", '
            f"hostname={self.__core_hostname!r}"
        )
        return run_id

    # ------------------------------------------------------------------ #
    # CRUD задач
    # ------------------------------------------------------------------ #

    @staticmethod
    def get_default_audit_task_params() -> dict[str, Any]:
        """Шаблон ScannerTaskParam для задачи аудита (create_task).

        Значения-плейсхолдеры заменить на реальные UUID из
        ``get_profiles_list()`` / ``get_agents_list()`` /
        ``get_credentials_list()``.
        """
        return {
            "name": "task_name",
            "scope": "00000000-0000-0000-0000-000000000005",
            "profile": "use get_profiles_list() to get profile UUID",
            "agent": "use get_agents_list() to get agent UUID",
            "overrides": {
                "transports": {
                    "terminal": {
                        "ssh": {
                            "connection": {
                                "auth": {
                                    "ref_value": "use get_credentials_list() "
                                    "to get credentials UUID",
                                    "ref_type": "credential",
                                },
                                "privilege_elevation": {
                                    "sudo": {
                                        "auth": {
                                            "ref_value": "use get_credentials_list() "
                                            "to get credentials UUID",
                                            "ref_type": "credential",
                                        }
                                    }
                                },
                            }
                        }
                    }
                }
            },
            "hostDiscovery": {"enabled": "false", "profile": "null"},
            "include": {
                "targets": ["list", "of", "ip", "addresses", "to", "scan"],
                "assets": [],
                "assetsGroups": [],
            },
            "exclude": {"targets": [], "assets": [], "assetsGroups": []},
            "triggerParameters": {
                "isEnabled": "false",
                "fromDate": "2023-01-18T14:46:02.717Z",
                "timeZone": "+03:00",
                "type": "Daily",
                "atTime": "09:00:00",
                "daysOfWeek": [
                    "monday",
                    "tuesday",
                    "wednesday",
                    "thursday",
                    "friday",
                    "saturday",
                    "sunday",
                ],
            },
        }

    @staticmethod
    def get_default_syslog_task_params() -> dict[str, Any]:
        """Шаблон ScannerTaskParam для задачи сбора syslog (create_task).

        Значения-плейсхолдеры заменить на реальные UUID из
        ``get_profiles_list()`` / ``get_agents_list()``.
        """
        return {
            "name": "task_name",
            "scope": "00000000-0000-0000-0000-000000000005",
            "profile": "use get_profiles_list() to get profile UUID",
            "agent": "use get_agents_list() to get agent UUID",
            "overrides": {},
            "hostDiscovery": {"enabled": "false", "profile": "null"},
            "include": {"targets": [], "assets": [], "assetsGroups": []},
            "exclude": {"targets": [], "assets": [], "assetsGroups": []},
            "triggerParameters": {
                "isEnabled": "false",
                "fromDate": "2023-02-04T12:36:01.663Z",
                "timeZone": "+03:00",
                "type": "Daily",
                "atTime": "09:00:00",
                "daysOfWeek": [
                    "monday",
                    "tuesday",
                    "wednesday",
                    "thursday",
                    "friday",
                    "saturday",
                    "sunday",
                ],
            },
        }

    def create_task(self, params: dict[str, Any]) -> str | None:
        """Создать задачу (контракт CreateScannerTask, v3).

        :param params: ScannerTaskParam
        :return: ID созданной задачи
        """
        body: dict[str, Any] = self.__post_json(self.__api_tasks_list, params) or {}

        task_id: str | None = body.get("id")
        self.__tasks.clear()
        self.log.info(
            f"status=success, action=create_task, "
            f'msg="Task {task_id!r} created", '
            f"hostname={self.__core_hostname!r}"
        )
        return task_id

    def edit_task(self, task_id: str, params: dict[str, Any]) -> str | None:
        """Обновить задачу (контракт UpdateScannerTask, v3).

        :param task_id: ID задачи
        :param params: ScannerTaskParam
        :return: ID обновлённой задачи
        """
        api_url = f"{self.__api_tasks_list}/{task_id}"
        body: dict[str, Any] = self.__payload_or_empty(self.__put_json(api_url, params))

        updated_id: str = body.get("id") or task_id
        self.__tasks.clear()
        self.log.info(
            f"status=success, action=edit_task, "
            f'msg="Task {updated_id!r} updated", '
            f"hostname={self.__core_hostname!r}"
        )
        return updated_id

    def delete_task(self, task_id: str) -> bool:
        """Удалить задачу (контракт DeleteScannerTask, v3).

        :param task_id: ID задачи
        :return: True при успехе (204)
        """
        api_url = f"{self.__api_tasks_list}/{task_id}"
        self.__delete_request(api_url)

        self.__tasks.clear()
        self.log.info(
            f"status=success, action=delete_task, "
            f'msg="Task {task_id!r} deleted", '
            f"hostname={self.__core_hostname!r}"
        )
        return True

    # ------------------------------------------------------------------ #
    # Задачи v4
    # ------------------------------------------------------------------ #

    def create_task_v4(self, params: dict[str, Any]) -> str | None:
        """Создать задачу по контракту v4 (ScannerTasksV4.yaml,
        ``POST /api/scanning/v4/scanner_tasks/create``).

        Отличие от :meth:`create_task`: вместо ``agent`` передаётся
        ``agents: {"agentIds": [...], "components": {...}}`` (v4
        ScannerTaskParam).

        :param params: v4 ScannerTaskParam (``name``, ``scope``, ``profile``,
            ``include``, ``exclude``, ``hostDiscovery``, ``triggerParameters``
            обязательны)
        :return: ID созданной задачи
        """
        body: dict[str, Any] = (
            self.__post_json(f"{self.__api_tasks_list_v4}/create", params) or {}
        )

        task_id: str | None = body.get("id")
        self.__tasks.clear()
        self.log.info(
            f"status=success, action=create_task_v4, "
            f'msg="Task {task_id!r} created (v4)", '
            f"hostname={self.__core_hostname!r}"
        )
        return task_id

    def edit_task_v4(self, task_id: str, params: dict[str, Any]) -> str | None:
        """Обновить задачу по контракту v4 (ScannerTasksV4.yaml,
        ``PUT /api/scanning/v4/scanner_tasks/{id}``).

        Тело - v4 ScannerTaskParam, см. :meth:`create_task_v4`.

        :param task_id: ID задачи
        :param params: v4 ScannerTaskParam
        :return: ID обновлённой задачи
        """
        api_url = f"{self.__api_tasks_list_v4}/{task_id}"
        body: dict[str, Any] = self.__payload_or_empty(self.__put_json(api_url, params))

        updated_id: str = body.get("id") or task_id
        self.__tasks.clear()
        self.log.info(
            f"status=success, action=edit_task_v4, "
            f'msg="Task {updated_id!r} updated (v4)", '
            f"hostname={self.__core_hostname!r}"
        )
        return updated_id

    def get_task_params_for_copy(
        self, task_id: str, use_v4: bool = False
    ) -> dict[str, Any]:
        """Тело ScannerTaskParam для копирования/обновления задачи.

        Карточка задачи (:meth:`get_task_info`) содержит ``scope``/``profile``/
        ``agent`` объектами, а контракт создания требует строки UUID, поэтому
        перед созданием копии карточку надо развернуть - это делает данный
        метод.

        :param task_id: ID задачи-источника
        :param use_v4: ``True`` - собрать v4-параметры (``agents.agentIds``)
            из v4-карточки, ``False`` - v3 (поле ``agent``)
        :return: dict c ключами ``name``/``description``/``agent`` (или
            ``agents``)/``scope``/``profile``/``include``/``exclude``/
            ``hostDiscovery``/``triggerParameters``/``overrides``/
            ``isFqdnPriority``
        """
        card = self.get_task_info_v4(task_id) if use_v4 else self.get_task_info(task_id)

        def identifier(value: Any) -> Any:
            if isinstance(value, dict):
                return value.get("id")
            return value

        params: dict[str, Any] = {
            "name": card.get("name"),
            "description": card.get("description"),
            "scope": identifier(card.get("scope")),
            "profile": identifier(card.get("profile")),
            "include": card.get("include"),
            "exclude": card.get("exclude"),
            "hostDiscovery": card.get("hostDiscovery"),
            "triggerParameters": card.get("triggerParameters"),
            "overrides": card.get("overrides") or {},
            "isFqdnPriority": card.get("isFqdnPriority", False),
        }
        if use_v4:
            agents = card.get("agents") or {}
            params["agents"] = {
                "agentIds": [a["id"] for a in agents.get("agents") or []]
            }
        else:
            params["agent"] = identifier(card.get("agent"))

        return params

    def copy_task(
        self,
        task_id: str,
        new_name: str,
        use_v4: bool = False,
        wait_new_status: bool = True,
        timeout: int = 300,
    ) -> str | None:
        """Создать копию существующей задачи.

        Назначение - безопасная проверка write-операций (start/stop/suspend,
        bulk-операций, удаления) на копии, не затрагивая рабочую задачу.
        Копия наследует профиль, агента, цели и overrides источника;
        ``description`` не копируется.

        :param task_id: ID задачи-источника
        :param new_name: имя копии (уникальное в системе)
        :param use_v4: создавать копию через v4-контракт (``agents`` вместо
            ``agent``)
        :param wait_new_status: ждать перехода копии в статус ``new``
            (задача создаётся асинхронно)
        :param timeout: сколько секунд ждать статуса ``new``
        :return: ID копии либо None, если сервер не вернул id
        """
        params = self.get_task_params_for_copy(task_id, use_v4=use_v4)
        params["name"] = new_name
        params.pop("description", None)

        copy_id = self.create_task_v4(params) if use_v4 else self.create_task(params)
        if copy_id is not None and wait_new_status:
            self.wait_task_status(copy_id, "new", timeout=timeout)

        self.log.info(
            f"status=success, action=copy_task, "
            f'msg="Task {task_id!r} copied as {copy_id!r} ({new_name!r})", '
            f"hostname={self.__core_hostname!r}"
        )
        return copy_id

    def wait_task_status(
        self, task_id: str, expected_status: str, timeout: int = 300, delay: int = 5
    ) -> str:
        """Дождаться статуса задачи (создание/остановка - асинхронные).

        :param task_id: ID задачи
        :param expected_status: ожидаемый статус
        :param timeout: сколько секунд ждать
        :param delay: период опроса, секунд
        :return: фактический статус по истечении ожидания
        :raises ValueError: задача исчезла из списка
        """
        deadline = time.monotonic() + timeout
        status = self.get_task_status(task_id, refresh=True)
        while status != expected_status and time.monotonic() < deadline:
            time.sleep(delay)
            status = self.get_task_status(task_id, refresh=True)

        if status != expected_status:
            self.log.warning(
                f"status=failed, action=wait_task_status, "
                f'msg="Task {task_id!r} stayed in status {status!r} '
                f'instead of {expected_status!r} within {timeout}s", '
                f"hostname={self.__core_hostname!r}"
            )
        return status

    # ------------------------------------------------------------------ #
    # Выделения задач (TaskSelections.yaml)
    # ------------------------------------------------------------------ #

    def create_task_selection(
        self,
        task_ids: list[str] | None = None,
        unselected_task_ids: list[str] | None = None,
        filter_body: dict[str, Any] | None = None,
        token: str | None = None,
        main_filter: str = "all",
        additional_filter: str = "all",
    ) -> dict[str, Any]:
        """Создать выделение задач (TaskSelections.yaml CreateSelection,
        ``POST /api/scanning/v3/task_selections``).

        Выделение - обязательный аргумент всех bulk-операций
        (``*_tasks``). ``task_ids`` и ``unselected_task_ids`` взаимоисключающие
        (контракт TaskIdsLists): второй вариант означает «выбрано всё, кроме
        перечисленного».

        :param task_ids: явный список выделенных задач
        :param unselected_task_ids: список исключений при «выбрано всё»
        :param filter_body: ``Filter`` v4 (``text``/``statuses``/``modules``/
            ``scopes``/...), применяется к выборке
        :param token: токен выборки из v4-списка (работать с кэшем, а не с БД)
        :param main_filter: all|running|withWrongParameters|withWarnings
        :param additional_filter: all|scan|import|batch|retro
        :return: ``TasksSelectionResult`` -
            ``{selectionId, selectedTasksCount, availableOperations, token}``
        :raises ValueError: переданы оба списка id или не передан ни один
        """
        if (task_ids is None) == (unselected_task_ids is None):
            raise ValueError(
                "Exactly one of task_ids / unselected_task_ids must be given"
            )

        self.__check_enum(main_filter, self.MAIN_FILTERS, "mainFilter")
        self.__check_enum(
            additional_filter, self.ADDITIONAL_FILTERS, "additionalFilter"
        )

        tasks_ids: dict[str, Any] = {"selected": task_ids}
        if unselected_task_ids is not None:
            tasks_ids = {
                "allSelected": True,
                "unselected": unselected_task_ids,
            }

        body: dict[str, Any] = {"tasksIds": tasks_ids, "filter": filter_body}
        params: dict[str, Any] = {
            "mainFilter": main_filter,
            "additionalFilter": additional_filter,
        }
        if token is not None:
            params["token"] = token

        response: dict[str, Any] = (
            self.__post_json(self.__api_task_selections, body, params=params) or {}
        )
        self.log.info(
            f"status=success, action=create_task_selection, "
            f'msg="Selection {response.get("selectionId")!r} created, '
            f'tasks={response.get("selectedTasksCount")}", '
            f"hostname={self.__core_hostname!r}"
        )
        return response

    def edit_task_selection(
        self,
        selection_id: str,
        task_ids: list[str] | None = None,
        unselected_task_ids: list[str] | None = None,
        filter_body: dict[str, Any] | None = None,
        token: str | None = None,
        main_filter: str = "all",
        additional_filter: str = "all",
    ) -> dict[str, Any]:
        """Изменить выделение (TaskSelections.yaml EditSelection).

        :param selection_id: ID выделения
        :param task_ids: явный список выделенных задач
        :param unselected_task_ids: список исключений при «выбрано всё»
        :param filter_body: ``Filter`` v4, применяется к выборке
        :param token: токен выборки из v4-списка
        :param main_filter: all|running|withWrongParameters|withWarnings
        :param additional_filter: all|scan|import|batch|retro
        :return: обновлённый ``TasksSelectionResult``
        :raises requests.RequestException: пустая выборка
            (``core.scanning.task.selections.tasks.empty``, 400) либо выделения
            нет (400)
        """
        if (task_ids is None) == (unselected_task_ids is None):
            raise ValueError(
                "Exactly one of task_ids / unselected_task_ids must be given"
            )

        self.__check_enum(main_filter, self.MAIN_FILTERS, "mainFilter")
        self.__check_enum(
            additional_filter, self.ADDITIONAL_FILTERS, "additionalFilter"
        )

        tasks_ids: dict[str, Any] = {"selected": task_ids}
        if unselected_task_ids is not None:
            tasks_ids = {
                "allSelected": True,
                "unselected": unselected_task_ids,
            }

        params: dict[str, Any] = {
            "mainFilter": main_filter,
            "additionalFilter": additional_filter,
        }
        if token is not None:
            params["token"] = token

        api_url = f"{self.__api_task_selections}/{selection_id}"
        response: dict[str, Any] = self.__payload_or_empty(
            self.__put_json(
                api_url, {"tasksIds": tasks_ids, "filter": filter_body}, params=params
            )
        )
        self.log.info(
            f"status=success, action=edit_task_selection, "
            f'msg="Selection {selection_id!r} updated, '
            f'tasks={response.get("selectedTasksCount")}", '
            f"hostname={self.__core_hostname!r}"
        )
        return response

    def get_task_selection(self, selection_id: str) -> dict[str, Any]:
        """Получить выделение по ID (TaskSelections.yaml GetSelection).

        :param selection_id: ID выделения
        :return: ``TasksSelectionResult`` (``token`` у прочитанного выделения
            обычно ``null``)
        :raises requests.RequestException: выделения нет или оно удалено
            (400, ``SelectionNotFoundError``)
        """
        api_url = f"{self.__api_task_selections}/{selection_id}"
        response: dict[str, Any] = self.__get_json(api_url)
        self.log.info(
            f"status=success, action=get_task_selection, "
            f'msg="Got selection {selection_id!r}, '
            f'tasks={response.get("selectedTasksCount")}", '
            f"hostname={self.__core_hostname!r}"
        )
        return response

    def delete_task_selection(self, selection_id: str) -> bool:
        """Удалить выделение (TaskSelections.yaml DeleteSelection).

        :param selection_id: ID выделения
        :return: True при успехе (204)
        """
        api_url = f"{self.__api_task_selections}/{selection_id}"
        self.__delete_request(api_url)
        self.log.info(
            f"status=success, action=delete_task_selection, "
            f'msg="Selection {selection_id!r} deleted", '
            f"hostname={self.__core_hostname!r}"
        )
        return True

    # ------------------------------------------------------------------ #
    # Массовые операции (TaskBulkOperations.yaml)
    # ------------------------------------------------------------------ #

    def __bulk_operation(
        self,
        action: str,
        operation: str,
        selection_id: str,
        json_body: Any = None,
    ) -> str | None:
        api_url = f"{self.__api_bulk_operations}/{operation}"
        body: dict[str, Any] = (
            self.__post_json(
                api_url,
                json_body if json_body is not None else {},
                params={"selectionId": selection_id},
            )
            or {}
        )
        operation_id = body.get("operationId")
        self.log.info(
            f"status=success, action={action}, "
            f'msg="Bulk operation {operation!r} over selection '
            f'{selection_id!r} accepted, operationId={operation_id!r}", '
            f"hostname={self.__core_hostname!r}"
        )
        return str(operation_id) if operation_id is not None else None

    def start_tasks(self, selection_id: str) -> str | None:
        """Запустить выделенные задачи (TaskBulkOperations.yaml
        StartScannerTasks, ``POST /task_bulk_operations/start_tasks``).

        :param selection_id: ID выделения (см. :meth:`create_task_selection`)
        :return: ID массовой операции (``operationId``)
        """
        return self.__bulk_operation("start_tasks", "start_tasks", selection_id)

    def stop_tasks(self, selection_id: str) -> str | None:
        """Остановить выделенные задачи (StopScannerTasks).

        :param selection_id: ID выделения
        :return: ID массовой операции
        """
        return self.__bulk_operation("stop_tasks", "stop_tasks", selection_id)

    def suspend_tasks(self, selection_id: str) -> str | None:
        """Приостановить выделенные задачи (SuspendScannerTasks).

        :param selection_id: ID выделения
        :return: ID массовой операции
        """
        return self.__bulk_operation("suspend_tasks", "suspend_tasks", selection_id)

    def delete_tasks(self, selection_id: str) -> str | None:
        """Удалить выделенные задачи (DeleteScannerTasks).

        :param selection_id: ID выделения
        :return: ID массовой операции
        """
        self.__tasks.clear()
        return self.__bulk_operation("delete_tasks", "delete_tasks", selection_id)

    def clean_tasks_bookmarks(self, selection_id: str) -> str | None:
        """Сбросить savepoint'ы выделенных задач (ClearScannerTasksSavePoints,
        ``POST /task_bulk_operations/clean_bookmarks``).

        :param selection_id: ID выделения
        :return: ID массовой операции
        """
        self.__tasks.clear()
        return self.__bulk_operation(
            "clean_tasks_bookmarks", "clean_bookmarks", selection_id
        )

    def start_tasks_connection_check(
        self,
        selection_id: str,
        mode: str = "onlyConnection",
        agent_selection_mode: str = "single",
    ) -> str | None:
        """Запустить выделенные задачи в режиме проверки подключения
        (StartConnectionCheckScannerTasks).

        :param selection_id: ID выделения
        :param mode: ``onlyConnection`` | ``connectionAndAccounts``
        :param agent_selection_mode: ``single`` | ``all``
        :return: ID массовой операции
        """
        self.__check_enum(mode, self.CONNECTION_CHECK_MODES, "mode")
        self.__check_enum(
            agent_selection_mode,
            self.CONNECTION_CHECK_AGENT_SELECTION_MODES,
            "agentSelectionMode",
        )
        return self.__bulk_operation(
            "start_tasks_connection_check",
            "start_connection_check_tasks",
            selection_id,
            {"mode": mode, "agentSelectionMode": agent_selection_mode},
        )

    def update_tasks_schedule(
        self, selection_id: str, trigger_parameters: dict[str, Any]
    ) -> str | None:
        """Установить расписание выделенных задач (UpdateTasksSchedule).

        :param selection_id: ID выделения
        :param trigger_parameters: ``TriggerBySchedulerParametersDto``
            (``type`` + ``isEnabled`` обязательны), см.
            :meth:`get_task_params_for_copy`
        :return: ID массовой операции
        """
        return self.__bulk_operation(
            "update_tasks_schedule",
            "update_tasks_schedule",
            selection_id,
            {"trigger": trigger_parameters},
        )

    def compare_tasks_schedule(self, selection_id: str) -> dict[str, Any]:
        """Сравнить расписания выделенных задач (CompareSchedulerScannerTasks).

        Операция ничего не меняет.

        :param selection_id: ID выделения
        :return: ``CompareTriggersBySchedulerParametersDto`` -
            ``{triggersAreSame, schedulersState, schedulerParameters}``;
            ``schedulersState`` - см. ``SCHEDULERS_STATES``
        """
        api_url = f"{self.__api_bulk_operations}/compare_scheduler_tasks"
        response: dict[str, Any] = (
            self.__post_json(api_url, {}, params={"selectionId": selection_id}) or {}
        )
        self.log.info(
            f"status=success, action=compare_tasks_schedule, "
            f'msg="Selection {selection_id!r}: triggersAreSame='
            f"{response.get('triggersAreSame')!r}, state="
            f'{response.get("schedulersState")!r}", '
            f"hostname={self.__core_hostname!r}"
        )
        return response

    def get_bulk_operation_status(self, operation_id: str) -> str:
        """Статус массовой операции (TaskBulkOperations.yaml
        GetBulkOperation, ``GET /task_bulk_operations/{id}``).

        :param operation_id: ``operationId`` из ``*_tasks``
        :return: ``inProgress`` | ``finished`` (см. ``BULK_OPERATION_STATUSES``)
        :raises requests.RequestException: операции нет (400,
            ``core.scanning.bulk.operation.not.found.error``)
        """
        api_url = f"{self.__api_bulk_operations}/{operation_id}"
        response: dict[str, Any] = self.__get_json(api_url)
        return str(response.get("status"))

    def wait_bulk_operation(
        self, operation_id: str, timeout: int = 120, delay: int = 3
    ) -> str:
        """Дождаться завершения массовой операции.

        Операции запуска задач (в т.ч. ``start_connection_check_tasks``) могут
        оставаться ``inProgress`` дольше прочих - сервер не отменяет их по
        таймауту.

        :param operation_id: ``operationId``
        :param timeout: сколько секунд ждать
        :param delay: период опроса, секунд
        :return: ``finished`` либо последний наблюдаемый статус по истечении
            ожидания
        """
        deadline = time.monotonic() + timeout
        status = self.get_bulk_operation_status(operation_id)
        while status != "finished" and time.monotonic() < deadline:
            time.sleep(delay)
            status = self.get_bulk_operation_status(operation_id)

        if status != "finished":
            self.log.warning(
                f"status=failed, action=wait_bulk_operation, "
                f'msg="Bulk operation {operation_id!r} stayed in status '
                f'{status!r} within {timeout}s", '
                f"hostname={self.__core_hostname!r}"
            )
        return status

    def delete_bulk_operation(self, operation_id: str) -> bool:
        """Удалить массовую операцию (TaskBulkOperations.yaml
        DeleteBulkOperation).

        :param operation_id: ``operationId``
        :return: True при успехе (204)
        """
        api_url = f"{self.__api_bulk_operations}/{operation_id}"
        self.__delete_request(api_url)
        self.log.info(
            f"status=success, action=delete_bulk_operation, "
            f'msg="Bulk operation {operation_id!r} deleted", '
            f"hostname={self.__core_hostname!r}"
        )
        return True

    def get_tasks_groups(self, selection_id: str) -> list[dict[str, Any]]:
        """Группы, в которые входят выделенные задачи (TaskBulkOperations.yaml
        GetSelectedTasksGroupOperation, ``GET /task_bulk_operations/tasks_groups``).

        :param selection_id: ID выделения
        :return: список ``TasksGroupEntryDto`` -
            ``{groupId, entryState}``, ``entryState`` ∈ ``all`` | ``partly``;
            пустой список, если задачи ни в одной группе
        """
        api_url = f"{self.__api_bulk_operations}/tasks_groups"
        response: list[dict[str, Any]] = self.__get_json(
            api_url, params={"selectionId": selection_id}
        )
        self.log.info(
            f"status=success, action=get_tasks_groups, "
            f'msg="Got {len(response)} groups for selection '
            f'{selection_id!r}", hostname={self.__core_hostname!r}'
        )
        return response

    def update_tasks_groups(
        self,
        selection_id: str,
        include_in_groups: list[str] | None = None,
        exclude_from_groups: list[str] | None = None,
    ) -> bool:
        """Обновить состав групп для выделенных задач
        (TaskBulkOperations.yaml UpdateSelectedTasksGroupOperation).

        Справочника групп в контрактах сканера нет - UUID групп берутся из
        :meth:`get_tasks_groups` либо из поля ``groups`` карточки задачи.

        :param selection_id: ID выделения
        :param include_in_groups: добавить задачи в эти группы
        :param exclude_from_groups: убрать задачи из этих групп
        :return: True при успехе (204)
        """
        body = {
            "includeInGroups": include_in_groups or [],
            "excludeFromGroups": exclude_from_groups or [],
        }
        api_url = f"{self.__api_bulk_operations}/tasks_groups"
        self.__put_json(api_url, body, params={"selectionId": selection_id})
        self.__tasks.clear()
        self.log.info(
            f"status=success, action=update_tasks_groups, "
            f'msg="Groups of selection {selection_id!r} updated", '
            f"hostname={self.__core_hostname!r}"
        )
        return True

    # ------------------------------------------------------------------ #
    # Запуски и подзадачи
    # ------------------------------------------------------------------ #

    def get_runs_list(
        self,
        task_id: str,
        limit: int = MAX_BATCH,
        offset: int = 0,
        with_errors: bool | None = None,
        time_from: str | None = None,
        time_to: str | None = None,
        run_mode: str | None = None,
    ) -> dict[str, Any]:
        """История запусков задачи (контракт GetTaskRuns, v2).

        :param task_id: ID задачи
        :param limit: размер выборки (1..1000 по контракту)
        :param offset: смещение страницы
        :param with_errors: фильтр «с ошибками»
        :param time_from: начало временного фильтра (ISO 8601)
        :param time_to: конец временного фильтра (ISO 8601)
        :param run_mode: режим запуска - см. ``RUN_MODES``
        :return: ``{items, totalItems}`` (RunsPack)
        """
        self.__check_batch(limit)
        if run_mode is not None:
            self.__check_enum(run_mode, self.RUN_MODES, "runMode")

        params: dict[str, Any] = {"limit": limit, "offset": offset}
        if with_errors is not None:
            params["withErrors"] = self.__query_bool(with_errors)
        if time_from is not None:
            params["timeFrom"] = time_from
        if time_to is not None:
            params["timeTo"] = time_to
        if run_mode is not None:
            params["runMode"] = run_mode

        api_url = f"{self.__api_tasks_list_v2}/{task_id}/runs"
        response: dict[str, Any] = self.__get_json(api_url, params=params)
        return response

    def get_jobs_list(
        self, task_id: str, limit: int = MAX_BATCH, run_id: str | None = None
    ) -> dict[str, dict[str, Any]]:
        """Подзадачи активного (или указанного) запуска задачи.

        Контракты GetTaskRuns + GetTaskRunJobs (v2). Ищется незавершённый
        запуск (``finishedAt is None``); переданный ``run_id`` использует его
        напрямую.

        :param task_id: ID задачи; нужен только для поиска активного запуска
            (при переданном ``run_id`` не используется)
        :param limit: размер выборки (1..1000 по контракту)
        :param run_id: ID запуска; None - искать активный
        :return: {job_id: {status, status_error, started, finished, agent,
            targets}}
        """
        self.__check_batch(limit)
        if run_id is None:
            run_id = self.__find_active_run(task_id, limit)
            if run_id is None:
                self.log.info(
                    f"status=success, action=get_jobs_list, "
                    f'msg="Running tasks not found", '
                    f"hostname={self.__core_hostname!r}"
                )
                return {}

        api_url = f"{self.__api_runs_list_v2}/{run_id}/jobs"
        response: dict[str, Any] = self.__get_json(api_url, params={"limit": limit})

        items: list[dict[str, Any]] = self.__items_or_fail("get_jobs_list", response)

        jobs: dict[str, dict[str, Any]] = {}
        for item in items:
            jobs[item["id"]] = {
                "status": item.get("status"),
                "status_error": item.get("errorStatus"),
                "started": item.get("startedAt"),
                "finished": item.get("finishedAt"),
                "agent": item.get("agent"),
                "targets": item.get("targets"),
            }

        self.log.info(
            f"status=success, action=get_jobs_list, "
            f'msg="Got {len(jobs)} jobs for task {task_id!r}", '
            f"hostname={self.__core_hostname!r}"
        )
        return jobs

    def get_job_info(self, job_id: str) -> dict[str, Any]:
        """Информация по подзадаче (контракт GetJob, ``GET /v2/jobs/{jobId}``).

        :param job_id: UUID подзадачи
        :return: ``JobView`` как есть
        """
        api_url = f"{self.__api_jobs_list_v2}/{job_id}"
        response: dict[str, Any] = self.__get_json(api_url)
        return response

    def get_job_errors(
        self,
        job_id: str,
        limit: int = MAX_BATCH,
        offset: int = 0,
        orderby: str = "occurredAt desc",
    ) -> dict[str, Any]:
        """Ошибки подзадачи (контракт GetJobErrors).

        :param job_id: UUID подзадачи
        :param limit: размер выборки (1..1000 по контракту)
        :param offset: смещение страницы
        :param orderby: сортировка; сервер допускает только ``occurredAt``
            c ``asc``/``desc``
        :return: ``{items, totalItems, logsDownloadUnavailable}``
        """
        self.__check_batch(limit)
        api_url = f"{self.__api_jobs_list_v2}/{job_id}/job_errors"
        response: dict[str, Any] = self.__get_json(
            api_url,
            params={"limit": limit, "offset": offset, "orderby": orderby},
        )
        return response

    def get_run_agents(self, run_id: str) -> list[dict[str, Any]]:
        """Агенты в рамках запуска задачи (контракт GetTaskRunAgents).

        :param run_id: UUID запуска
        :return: список ``AgentView``; для несуществующего запуска - пустой
            список (замерено на R28.0)
        """
        api_url = f"{self.__api_runs_list_v2}/{run_id}/agents"
        response: list[dict[str, Any]] = self.__get_json(api_url)
        return response

    def __find_active_run(self, task_id: str, limit: int) -> str | None:
        api_url = f"{self.__api_tasks_list_v2}/{task_id}/runs"
        response: dict[str, Any] = self.__get_json(api_url, params={"limit": limit})

        items: list[dict[str, Any]] = self.__items_or_fail("find_active_run", response)

        for item in items:
            if item.get("finishedAt") is None:
                return str(item.get("id"))
        return None

    # ------------------------------------------------------------------ #
    # Снимок результатов скана (Scans.Contracts.yaml)
    # ------------------------------------------------------------------ #

    @staticmethod
    def build_scan_snapshot(addresses: list[str], version: str = "28.0.0.0") -> str:
        """Собрать XML снимка для :meth:`add_scan` (Scans.Contracts.yaml).

        Атрибут ``Version`` сервер проверяет только на непустоту (замерено на
        R28.0: ``9.9.9.9`` принят, пустая строка - 400).

        :param addresses: IP-адреса найденных хостов
        :param version: версия формата снимка
        :return: XML-строка ``Snapshot``
        """
        hosts = "".join(
            f'<ns0:Host Id="{uuid.uuid4()}"><Endpoints>'
            f'<ns1:IpEndpoint Id="{uuid.uuid4()}"><Address>{address}</Address>'
            f"</ns1:IpEndpoint></Endpoints></ns0:Host>"
            for address in addresses
        )
        # xmlns-значения - идентификаторы пространства имён, а не ссылки
        # noinspection HttpUrlsUsage
        return (
            f'<Snapshot Version="{version}" '
            'xmlns:ns0="http://schemas.MaxPatrol.com/Core" '
            'xmlns:ns1="http://schemas.MaxPatrol.com/Network" '
            'xmlns:meta="http://schemas.MaxPatrol.com/_metadata">'
            f"<Base>{hosts}</Base></Snapshot>"
        )

    def add_scan(
        self,
        scan_id: str,
        source: str,
        scope_id: str,
        scan_time: str,
        content: str,
        job_id: str | None = None,
        no_ttl: bool = False,
        replace_entities: bool = False,
        create_only: bool = False,
    ) -> bool:
        """Загрузить снимок результатов скана (Scans.Contracts.yaml AddScan,
        ``PUT /api/v1/scans/{scanId}``).

        Тело - XML снимка (``text/xml``), см. :meth:`build_scan_snapshot`.
        Сервер проверяет ``source`` по перечню источников сканера: на R28.0
        приняты ``HostDiscovery`` и ``PenTest``
        (``Scanner`` -> ``scanSourceIsInvalidError``), а ``PenTest`` сервер
        нормализует до ``PenTest`` независимо от регистра.

        :param scan_id: UUID снимка (при повторной загрузке того же id снимок
            перезаписывается)
        :param source: источник сканирования, см. ``SCAN_SOURCES``
        :param scope_id: UUID инфраструктуры
        :param scan_time: время сканирования (ISO 8601)
        :param content: XML снимка
        :param job_id: UUID подзадачи сканирования (опционально)
        :param no_ttl: отключить устаревание актива
        :param replace_entities: замещать все сущности при мерже
        :param create_only: пометить снимок как «только для создания актива»
        :return: True при успехе (204)
        :raises requests.RequestException: неизвестный источник, битый XML
            (``core.scans.ScanCorruptedError``), чужой scope (400)
        """
        self.__check_enum(source, self.SCAN_SOURCES, "source")

        params: dict[str, Any] = {
            "source": source,
            "scopeId": scope_id,
            "time": scan_time,
            "noTtl": self.__query_bool(no_ttl),
            "replaceEntities": self.__query_bool(replace_entities),
            "createOnly": self.__query_bool(create_only),
        }
        if job_id is not None:
            params["jobId"] = job_id

        api_url = f"{self.__api_scans}/{scan_id}"
        self.__put_body(api_url, content.encode("utf-8"), params=params)

        self.log.info(
            f"status=success, action=add_scan, "
            f'msg="Scan {scan_id!r} accepted (source={source!r}, '
            f'scope={scope_id!r})", hostname={self.__core_hostname!r}'
        )
        return True

    # ------------------------------------------------------------------ #
    # Общее
    # ------------------------------------------------------------------ #

    def __request(
        self,
        action: str,
        api_url: str,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        json_body: Any = None,
        data: Any = None,
        headers: dict[str, str] | None = None,
        timeout_up: float = 1,
    ) -> requests.Response:
        """Обёртка exec_request c统一的 логом неуспеха по действию модуля."""
        try:
            return exec_request(
                self.__core_session,
                self.__url(api_url),
                method=method,
                timeout=self.settings.connection_timeout,
                timeout_up=timeout_up,
                params=params,
                json=json_body,
                data=data,
                headers=headers,
            )
        except Exception as err:
            self.log.warning(
                f"status=failed, action={action}, "
                f'msg="{method} {api_url} failed", '
                f"hostname={self.__core_hostname!r}, error={err!r}"
            )
            raise

    def __get_json(self, api_url: str, params: dict[str, Any] | None = None) -> Any:
        return self.__request("get_json", api_url, params=params).json()

    def __post_json(
        self, api_url: str, json_body: Any, params: dict[str, Any] | None = None
    ) -> Any:
        response = self.__request(
            "post_json", api_url, method="POST", params=params, json_body=json_body
        )
        return response.json() if response.text else None

    def __put_json(
        self, api_url: str, json_body: Any, params: dict[str, Any] | None = None
    ) -> requests.Response:
        return self.__request(
            "put_json", api_url, method="PUT", params=params, json_body=json_body
        )

    def __put_body(
        self,
        api_url: str,
        data: Any,
        params: dict[str, Any] | None = None,
        content_type: str = "text/xml",
    ) -> requests.Response:
        return self.__request(
            "put_body",
            api_url,
            method="PUT",
            params=params,
            data=data,
            headers={"Content-Type": content_type},
        )

    def __delete_request(
        self, api_url: str, params: dict[str, Any] | None = None
    ) -> requests.Response:
        return self.__request("delete", api_url, method="DELETE", params=params)

    @staticmethod
    def __payload_or_empty(response: requests.Response) -> dict[str, Any]:
        """DELETE/PUT у ряда эндпоинтов сканера отвечают 200 с телом, а у
        большинства - 204 без тела (замерено на R28.0)."""
        return response.json() if response.text else {}

    @staticmethod
    def __query_bool(value: bool) -> str:
        """requests сериализует bool как ``True``/``False`` - API ждёт
        ``true``/``false``."""
        return "true" if value else "false"

    def __download(
        self, action: str, api_url: str, json_body: Any, timeout_up: float = 4
    ) -> tuple[bytes, str | None]:
        """POST, отвечающий файлом: возвращает (содержимое, имя файла)."""
        response = self.__request(
            action,
            api_url,
            method="POST",
            json_body=json_body,
            timeout_up=timeout_up,
        )
        filename = self.__content_disposition_filename(response)
        self.log.info(
            f"status=success, action={action}, "
            f'msg="Download prepared", hostname={self.__core_hostname!r}, '
            f"filename={filename!r}, bytes={len(response.content)}"
        )
        return response.content, filename

    @staticmethod
    def __content_disposition_filename(response: requests.Response) -> str | None:
        """Имя файла из Content-Disposition: ``filename*`` (RFC 5987) важнее."""
        disposition = response.headers.get("content-disposition")
        if not disposition:
            return None

        filename: str | None = None
        for part in disposition.split(";")[1:]:
            name, _, value = part.strip().partition("=")
            if not value:
                continue
            value = value.strip('"')
            if name == "filename*":
                return unquote(value.rsplit("''", 1)[-1])
            if name == "filename":
                filename = value
        return filename

    def __items_or_fail(
        self, action: str, response: dict[str, Any]
    ) -> list[dict[str, Any]]:
        raw_items: Any = response.get("items")
        if raw_items is None:
            self.log.error(
                f"status=failed, action={action}, "
                f'msg="No items in response", '
                f"hostname={self.__core_hostname!r}"
            )
            raise RuntimeError(f"No items in {action} response")

        return list(raw_items)

    @staticmethod
    def __check_batch(limit: int) -> None:
        if not 1 <= limit <= 1000:
            raise ValueError(f"limit must be in 1..1000 range, got {limit!r}")

    @staticmethod
    def __check_enum(value: str, allowed: tuple[str, ...], name: str) -> None:
        if value not in allowed:
            raise ValueError(
                f"Unknown {name} {value!r}; expected one of {list(allowed)!r}"
            )

    def close(self) -> None:
        if self.__core_session is not None:
            self.__core_session.close()
