from typing import Any

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

    Задачи сканирования и справочники сканера. Контракты:

    - ``Agents.yaml``          - ``GET /api/v1/scanner_agents``
    - ``ScannerModules.yaml``  - ``GET /api/v1/scanner_modules``
    - ``Profiles.yaml``        - ``GET /api/scanning/v3/scanner_profiles``
    - ``Credentials.yaml``     - ``GET /api/v3/credentials``
    - ``ScannerTasks.yaml``    - ``/api/scanning/v3/scanner_tasks`` (CRUD,
      start/stop)
    - ``ScannerTaskRuns.yaml`` - ``/api/scanning/v2`` (runs, jobs)

    MP SIEM 26.0+: ветки под ядра R23 (``/api/v2/scanner_profiles``,
    ``/api/v1/scanner_metatransports``) удалены как мёртвый код.
    """

    __api_agents_list = "/api/v1/scanner_agents"
    __api_modules_list = "/api/v1/scanner_modules"
    __api_profiles_list = "/api/scanning/v3/scanner_profiles"
    __api_credentials_list = "/api/v3/credentials"
    __api_tasks_list = "/api/scanning/v3/scanner_tasks"
    __api_tasks_list_v2 = "/api/scanning/v2/scanner_tasks"
    __api_runs_list_v2 = "/api/scanning/v2/runs"

    # ScannerTasks.yaml: mainFilter / additionalFilter
    MAIN_FILTERS = ("all", "running", "withWrongParameters", "withWarnings")
    ADDITIONAL_FILTERS = ("all", "scan", "import", "batch", "retro")
    # ScannerModules.yaml: workType
    WORK_TYPES = ("active", "passive", "undefined")

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
        self.__modules: dict[str, dict[str, Any]] = {}
        self.__profiles: dict[str, dict[str, Any]] = {}
        self.__credentials: dict[str, dict[str, Any]] = {}
        self.__tasks: dict[str, dict[str, Any]] = {}
        self.log.debug('status=success, action=prepare, msg="Tasks Module init"')

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

        url = f"https://{self.__core_hostname}{self.__api_agents_list}"
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

    def get_modules_list(
        self, refresh: bool = False, work_type: str | None = None
    ) -> dict[str, dict[str, Any]]:
        """Список модулей сканирования (контракт GetModules).

        :param refresh: принудительно обновить кэш
        :param work_type: фильтр active|passive|undefined (опционально)
        :return: {module_id: {name, type}}
        """
        if work_type is not None and work_type not in self.WORK_TYPES:
            raise ValueError(
                f"Unknown workType {work_type!r}; "
                f"expected one of {list(self.WORK_TYPES)!r}"
            )
        if self.__modules and not refresh:
            return {k: dict(v) for k, v in self.__modules.items()}

        params: dict[str, str] = {}
        if work_type is not None:
            params["workType"] = work_type

        url = f"https://{self.__core_hostname}{self.__api_modules_list}"
        response: list[dict[str, Any]] = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
            params=params,
        ).json()

        self.__modules = {}
        for item in response:
            self.__modules[item["id"]] = {
                "name": item.get("name"),
                "type": str(item.get("outputType") or "").lower(),
            }

        self.log.info(
            f'status=success, action=get_modules_list, msg="Got modules list", '
            f"hostname={self.__core_hostname!r}, count={len(self.__modules)}"
        )
        return {k: dict(v) for k, v in self.__modules.items()}

    def get_profiles_list(
        self, refresh: bool = False, module_id: str | None = None
    ) -> dict[str, dict[str, Any]]:
        """Список профилей сканирования (контракт GetAllProfiles).

        :param refresh: принудительно обновить кэш
        :param module_id: фильтр по идентификатору модуля (опционально)
        :return: {profile_id: {name, system, base_profile, module_id, output}}
        """
        if self.__profiles and not refresh:
            return {k: dict(v) for k, v in self.__profiles.items()}

        params: dict[str, str] = {}
        if module_id is not None:
            params["moduleId"] = module_id

        url = f"https://{self.__core_hostname}{self.__api_profiles_list}"
        response: list[dict[str, Any]] = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
            params=params,
        ).json()

        self.__profiles = {
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

        self.log.info(
            f'status=success, action=get_profiles_list, msg="Got profiles list", '
            f"hostname={self.__core_hostname!r}, count={len(self.__profiles)}"
        )
        return {k: dict(v) for k, v in self.__profiles.items()}

    def get_credentials_list(
        self, refresh: bool = False, credential_tags: list[str] | None = None
    ) -> dict[str, dict[str, Any]]:
        """Список учетных записей сканера (контракт GetCredentials).

        :param refresh: принудительно обновить кэш
        :param credential_tags: фильтр по тэгам (опционально)
        :return: {credential_id: {name, type, description, credential_tags}}
        """
        if self.__credentials and not refresh:
            return {k: dict(v) for k, v in self.__credentials.items()}

        params: dict[str, Any] = {}
        if credential_tags is not None:
            params["credentialTags"] = credential_tags

        url = f"https://{self.__core_hostname}{self.__api_credentials_list}"
        response: list[dict[str, Any]] = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
            params=params,
        ).json()

        self.__credentials = {}
        for item in response:
            self.__credentials[item["id"]] = {
                "name": item.get("name"),
                "type": item.get("type"),
                "description": item.get("description"),
                "credential_tags": item.get("credentialTags"),
            }

        self.log.info(
            f"status=success, action=get_credentials_list, "
            f'msg="Got credentials list", '
            f"hostname={self.__core_hostname!r}, count={len(self.__credentials)}"
        )
        return {k: dict(v) for k, v in self.__credentials.items()}

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
        self.__check_enum(main_filter, self.MAIN_FILTERS, "mainFilter")
        self.__check_enum(
            additional_filter, self.ADDITIONAL_FILTERS, "additionalFilter"
        )

        if self.__tasks and not refresh:
            return {k: dict(v) for k, v in self.__tasks.items()}

        params = {
            "mainFilter": main_filter,
            "additionalFilter": additional_filter,
        }
        url = f"https://{self.__core_hostname}{self.__api_tasks_list}"
        response: list[dict[str, Any]] = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
            params=params,
        ).json()

        self.__tasks = {}
        for item in response:
            profile = item.get("profile") or {}
            self.__tasks[item["id"]] = {
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

        self.log.info(
            f'status=success, action=get_tasks_list, msg="Got task list", '
            f"hostname={self.__core_hostname!r}, count={len(self.__tasks)}"
        )
        return {k: dict(v) for k, v in self.__tasks.items()}

    def get_task_info(self, task_id: str, refresh: bool = False) -> dict[str, Any]:
        """Информация о задаче с параметрами (контракт GetScannerTask, v3).

        Кэш списка пополняется параметрами из карточки задачи.

        :param task_id: ID задачи
        :param refresh: обойти кэш списка задач
        :return: карточка задачи (ScannerTask) + ``parameters``
        :raises ValueError: задача не найдена
        """
        if not self.__tasks or refresh:
            self.get_tasks_list(refresh=refresh)

        api_url = f"{self.__api_tasks_list}/{task_id}"
        url = f"https://{self.__core_hostname}{api_url}"
        response: dict[str, Any] = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
        ).json()

        # 404 (задача не найдена) обработает exec_request (HTTPError)
        cached = self.__tasks.get(task_id) or {}
        task_info: dict[str, Any] = dict(cached, **response)
        task_info["parameters"] = response.get("parameters")

        self.log.info(
            f"status=success, action=get_task_info, "
            f'msg="Got info for task {task_id!r}", '
            f"hostname={self.__core_hostname!r}"
        )
        return task_info

    def get_task_status(self, task_id: str, refresh: bool = True) -> str:
        """Статус задачи по её ID.

        :param task_id: ID задачи
        :param refresh: обновить кэш списка перед чтением статуса
        :return: статус из ScannerTaskStatus
        :raises ValueError: задача не найдена
        """
        self.get_tasks_list(refresh=refresh)
        task = self.__tasks.get(task_id)
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

    def __manipulate_task(self, task_id: str, control: str) -> str | None:
        api_url = f"{self.__api_tasks_list}/{task_id}/{control}"
        url = f"https://{self.__core_hostname}{api_url}"
        response = exec_request(
            self.__core_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
        )

        run_id: str | None = None
        if control == "start":
            # start отвечает TaskRunId {id}; stop - 204 без тела
            body = response.json() if response.text else {}
            run_id = body.get("id")
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
        url = f"https://{self.__core_hostname}{self.__api_tasks_list}"
        response: dict[str, Any] = exec_request(
            self.__core_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
            json=params,
        ).json()

        task_id: str | None = response.get("id")
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
        url = f"https://{self.__core_hostname}{api_url}"
        response: dict[str, Any] = exec_request(
            self.__core_session,
            url,
            method="PUT",
            timeout=self.settings.connection_timeout,
            json=params,
        ).json()

        updated_id: str = response.get("id") or task_id
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
        url = f"https://{self.__core_hostname}{api_url}"
        response = exec_request(
            self.__core_session,
            url,
            method="DELETE",
            timeout=self.settings.connection_timeout,
        )

        deleted = response.status_code == 204
        self.log.info(
            f"status=success, action=delete_task, "
            f'msg="Task {task_id!r} deletion status {response.status_code}", '
            f"hostname={self.__core_hostname!r}"
        )
        return deleted

    # ------------------------------------------------------------------ #
    # Запуски и подзадачи
    # ------------------------------------------------------------------ #

    def get_jobs_list(
        self, task_id: str, limit: int = 1000, run_id: str | None = None
    ) -> dict[str, dict[str, Any]]:
        """Подзадачи активного (или указанного) запуска задачи.

        Контракты GetTaskRuns + GetTaskRunJobs (v2). Ищется незавершённый
        запуск (``finishedAt is None``); переданный ``run_id`` использует его
        напрямую.

        :param task_id: ID задачи
        :param limit: размер выборки (1..1000 по контракту)
        :param run_id: ID запуска; None - искать активный
        :return: {job_id: {status, status_error, started, finished, agent,
            targets}}
        """
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
        url = f"https://{self.__core_hostname}{api_url}"
        response: dict[str, Any] = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
            params={"limit": limit},
        ).json()

        raw_items: Any = response.get("items")
        if raw_items is None:
            raise RuntimeError("No items in jobs response")
        items: list[dict[str, Any]] = list(raw_items)

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

    def __find_active_run(self, task_id: str, limit: int) -> str | None:
        api_url = f"{self.__api_tasks_list_v2}/{task_id}/runs"
        url = f"https://{self.__core_hostname}{api_url}"
        response: dict[str, Any] = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
            params={"limit": limit},
        ).json()

        raw_items: Any = response.get("items")
        if raw_items is None:
            raise RuntimeError("No items in run history response")
        items: list[dict[str, Any]] = list(raw_items)

        for item in items:
            if item.get("finishedAt") is None:
                return str(item.get("id"))
        return None

    @staticmethod
    def __check_enum(value: str, allowed: tuple[str, ...], name: str) -> None:
        if value not in allowed:
            raise ValueError(
                f"Unknown {name} {value!r}; expected one of {list(allowed)!r}"
            )

    def close(self) -> None:
        if self.__core_session is not None:
            self.__core_session.close()
