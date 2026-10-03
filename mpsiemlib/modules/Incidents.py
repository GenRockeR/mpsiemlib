from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any, cast

from mpsiemlib.common import (
    AuthError,
    LoggingHandler,
    ModuleInterface,
    MPSIEMAuth,
    Settings,
    exec_request,
    get_metrics_start_time,
    get_metrics_took_time,
)


class Incidents(ModuleInterface, LoggingHandler):
    """Incidents worker."""

    __time_format = "%Y-%m-%dT%H:%M:%S.000Z"

    # Контракт: `IncidentsRead.yaml`
    __api_incidents_list = "/api/v2/incidents/"
    __api_incidents_read_model = "/api/incidentsReadModel/incidents"
    __api_incidents_read_model_v2 = "/api/v2/incidentsReadModel"
    __api_incident_tasks = "/api/incidentsReadModel/tasks"
    __api_incidents = "/api/incidents"
    __api_incidents_settings = "/api/incidents/settings"
    __api_incident_queries = "/api/incident_queries"
    # Справочники контрактов IncidentStatus / IncidentSeverity /
    # IncidentTaskStatus / IncidentTaskType
    __api_statuses = "/api/statuses"
    __api_statuses_localizations = "/api/statuses/localizations"
    __api_severities = "/api/severities"
    __api_severities_localizations = "/api/severities/localizations"
    __api_task_statuses = "/api/taskStatuses"
    __api_task_types = "/api/taskTypes"
    __api_task_statuses_localizations = "/api/issues/dictionaries/statuses"
    __api_task_types_localizations = "/api/issues/dictionaries/types"
    __netfor_batch_size = 2000

    class TimeFilterType:
        CREATED = "creation"
        DETECTED = "detection"
        MODIFIED = "lastModification"
        APPROVED = "approval"
        IN_PROGRESS = "inProgress"
        RESOLVED = "resolving"
        CLOSED = "closing"

    class AssetType:
        """Перечень `IncidentAssetType` контракта `IncidentsRead.yaml`."""

        TARGETS = "Targets"
        ATTACKERS = "Attackers"

    class QueryCategory:
        """Перечень `IncidentQueryCategory` контракта `IncidentsRead.yaml`."""

        LIST = "List"
        NOTIFICATIONS = "Notifications"

    # Core отдаёт 500 Internal Server Error на `Undefined` (измерено на R27.6),
    # поэтому наружу выпускаем только два осмысленных типа
    __allowed_asset_types = (AssetType.TARGETS, AssetType.ATTACKERS)

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
        self.__incidents_mapping: dict[str, str] = {}

        self.log.debug(
            f'status=success, action=prepare, msg="Incidents Module init", '
            f"hostname={self.__core_hostname!r}, version={self.__core_version!r}"
        )

    def get_incidents_list(
        self,
        begin: int,
        end: int,
        time_type: str = TimeFilterType.CREATED,
        filters: dict[str, Any] | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Получить список инцидентов за временной период.

        Контракт: `IncidentsRead.yaml` `POST /api/v2/incidents` (GetIncidents).

        :param begin: Timestamp. Начало диапазона выгрузки
        :param end: Timestamp. Окончание диапазона выгрузки
        :param time_type: Статус инцидента, к которому применяется
            временное окно. По умолчанию "созданные"
        :param filters: filters = {"select": [...], "where": "",
            "orderby": [{"field": "created", "sortOrder": "descending"}]}
        :return: Итератор
        """
        self.log.debug(
            f"status=prepare, action=get_incidents_list, "
            f'msg="Try to get incidents list", '
            f"hostname={self.__core_hostname!r}, filters={filters!r}, "
            f"begin={begin!r}, end={end!r}"
        )

        url = f"https://{self.__core_hostname}{self.__api_incidents_list}"
        time_from = datetime.fromtimestamp(begin, tz=UTC).strftime(self.__time_format)
        time_to = datetime.fromtimestamp(end, tz=UTC).strftime(self.__time_format)

        # Набор select фиксирован: Core отвечает 400/500 на произвольные наборы
        # полей (измерено на R27.6), поэтому дополняется только `where`/`orderby`
        query_filter: dict[str, Any] = {
            "select": [
                "key",
                "name",
                "category",
                "type",
                "status",
                "created",
                "assigned",
            ],
            "where": "",
            "orderby": [{"field": "created", "sortOrder": "descending"}],
        }
        if filters is not None:
            query_filter.update(filters)

        params: dict[str, Any] = {
            "timeFrom": time_from,
            "timeTo": time_to,
            "groups": {"filterType": "no_filter"},
            "filter": query_filter,
            "filterTimeType": time_type,
            "queryIds": ["all_incidents"],
        }

        # Пачками выгружаем содержимое
        is_end = False
        offset = 0
        limit = self.settings.incidents_batch_size
        line_counter = 0
        total_items = 0
        start_time = get_metrics_start_time()
        while not is_end:
            incidents, page_total = self.__iterate_incidents(url, params, offset, limit)
            if offset == 0:
                total_items = page_total
            if len(incidents) < limit:
                is_end = True
            offset += len(incidents)
            for incident in incidents:
                line_counter += 1
                inc_id = cast("str", incident.get("id"))
                inc_key = cast("str", incident.get("key"))
                self.__incidents_mapping[inc_key] = inc_id  # прогреваем кэш
                yield {
                    "id": inc_id,
                    "key": inc_key,
                    "created": incident.get("created"),
                    "name": incident.get("name"),
                    "confirmed": incident.get("isConfirmed"),
                    "status": self.__lower(incident.get("status")),
                    "category": self.__lower(incident.get("category")),
                    "type": self.__lower(incident.get("type")),
                    "assigned": incident.get("assigned"),
                    "severity": self.__lower(incident.get("severity")),
                }

        took_time = get_metrics_took_time(start_time)

        self.log.info(
            f"status=success, action=get_incidents_list, "
            f'msg="Query executed, response have been read", '
            f"hostname={self.__core_hostname!r}, filter={filters!r}, "
            f"lines={line_counter!r}, total_items={total_items!r}"
        )
        self.log.info(
            f"hostname={self.__core_hostname!r}, metric=get_incidents_list, "
            f"took={took_time:.4f}ms, objects={line_counter!r}"
        )

    def __iterate_incidents(
        self, url: str, params: dict[str, Any], offset: int, limit: int
    ) -> tuple[list[dict[str, Any]], int]:
        """Запросить одну пачку инцидентов.

        :param url: URL `POST /api/v2/incidents`
        :param params: Тело запроса (дополняется offset/limit)
        :param offset: Смещение пачки
        :param limit: Размер пачки
        :return: Список инцидентов (`IncidentDto`) и `totalItems`
        """
        page_params = dict(params)
        page_params["offset"] = offset
        page_params["limit"] = limit

        response: dict[str, Any] = exec_request(
            self.__core_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
            json=page_params,
        ).json()

        if response is None or "incidents" not in response:
            self.log.error(
                f"status=failed, action=iterate_incidents, "
                f'msg="Core data request return None or '
                f'has wrong response structure", '
                f"hostname={self.__core_hostname!r}, offset={offset!r}"
            )
            raise ValueError(
                f"Core data request return None or has wrong response structure "
                f"for offset={offset!r}"
            )

        incidents: list[dict[str, Any]] = response["incidents"]
        return incidents, int(response.get("totalItems") or 0)

    def get_incident_id_by_key(self, incident_key: str) -> str | None:
        """Получить ID инцидента по его Key (INC-<Number>). Поиск возможен
        только на глубину в 365 дней.

        :param incident_key: Key инцидента (INC-<Number>)
        :return: ID инцидента либо None, если инцидент не найден
        """
        incident_id = self.__incidents_mapping.get(incident_key)
        if incident_id is not None:
            return incident_id

        # Ищем по инцидентам на глубину в 365 дней
        end = round(datetime.now(tz=UTC).timestamp())
        begin = end - (365 * 86400)
        filters = {"where": f'key="{incident_key}"'}
        incident = next(
            self.get_incidents_list(begin, end, self.TimeFilterType.CREATED, filters),
            None,
        )

        return incident.get("id") if incident is not None else None

    def get_incident_info(self, incident_id: str) -> dict[str, Any]:
        """Получить информацию по инциденту.

        Контракт: `IncidentsRead.yaml` `GET /api/incidentsReadModel/incidents/
        {incidentId}` (GetIncident) + связанные коллекции.

        :param incident_id: ID инцидента. Key (INC-<Number>) != ID
        :return: Перечень свойств инцидента
        """
        self.log.debug(
            f"status=prepare, action=get_incident_info, "
            f'msg="Try to get incident info for {incident_id!r}", '
            f"hostname={self.__core_hostname!r}"
        )

        # получаем сам инцидент
        api_url = f"{self.__api_incidents_read_model}/{incident_id}"
        url = f"https://{self.__core_hostname}{api_url}"
        inc: dict[str, Any] = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
        ).json()

        comments = self.__load_comments(incident_id)
        events = self.__load_events(incident_id)
        issues = self.__load_issues(incident_id)

        ret = {
            "id": inc.get("id"),
            "key": inc.get("key"),
            "correlation_rule": self.__correlation_rule_names(inc),
            "created": inc.get("created"),
            "detected": inc.get("detected"),
            "modification_history": inc.get("modified"),
            "name": inc.get("name"),
            "description": inc.get("description"),
            "confirmed": inc.get("isConfirmed"),
            "status": self.__lower(inc.get("status")),
            "category": self.__lower(inc.get("category")),
            "type": self.__lower(inc.get("type")),
            "assigned": inc.get("assigned"),
            "reporter": inc.get("reporter"),
            "severity": self.__lower(inc.get("severity")),
            "targets": inc.get("targets"),
            "attackers": inc.get("attackers"),
            "parameters": inc.get("parameters"),
            "groups": inc.get("groups"),
            "comments": comments,
            "events": events,
            "issues": issues,
        }

        if inc.get("source") == "netFor":
            ret["netfor"] = self.__load_netfor(incident_id)

        self.log.info(
            f"status=success, action=get_incident_info, "
            f'msg="Get {len(ret)!r} properties for incident {incident_id!r}", '
            f"hostname={self.__core_hostname!r}"
        )

        return ret

    @staticmethod
    def __lower(value: Any) -> str:
        """Нормализовать enum-поле ответа в нижний регистр (Core отдаёт enum с
        заглавной буквы, а null - если поле не заполнено).

        :param value: Значение поля
        :return: Строка в нижнем регистре либо пустая строка для None
        """
        return value.lower() if isinstance(value, str) else ""

    @staticmethod
    def __correlation_rule_names(incident: dict[str, Any]) -> list[str]:
        """Извлечь имена коррелирующих правил из `correlationRules`.

        Поле `correlationRuleNames` контрактом не объявлено и Core его не
        отдаёт (измерено на R27.6), имена лежат в `correlationRules`.

        :param incident: Ответ `GetIncident`
        :return: Имена правил
        """
        return [
            str(rule.get("correlationName"))
            for rule in incident.get("correlationRules") or []
            if rule.get("correlationName")
        ]

    def __load_comments(self, incident_id: str) -> list[dict[str, Any]]:
        """Загрузка комментариев.

        Контракт: `IncidentsRead.yaml` `GET /incidentsReadModel/incidents/
        {incidentId}/transitions` (GetIncidentStatusTransitions).

        :param incident_id: ID инцидента
        :return: История переходов между статусами
        """
        api_url = f"{self.__api_incidents_read_model}/{incident_id}/transitions"
        url = f"https://{self.__core_hostname}{api_url}"
        response: list[dict[str, Any]] = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
        ).json()

        return [
            {
                "status_old": self.__lower(i.get("prevStatus")),
                "status_new": self.__lower(i.get("nextStatus")),
                "comment": i.get("comment"),
                "timestamp": i.get("timestamp"),
                "user_id": (i.get("changedBy") or {}).get("id"),
            }
            for i in response
        ]

    def __load_events(self, incident_id: str) -> list[dict[str, Any]]:
        """Загрузка событий.

        Контракт: `IncidentsRead.yaml` `GET /incidents/{incidentId}/events`
        (GetEvents) c offset/limit постранично.

        :param incident_id: ID инцидента
        :return: События инцидента
        """
        return [
            {
                "id": i.get("id"),
                "description": i.get("description"),
                "date": i.get("date"),
            }
            for i in self.__iterate_paged_collection(
                f"{self.__api_incidents}/{incident_id}/events",
                f"{self.__api_incidents}/{incident_id}/events/count",
            )
        ]

    def __load_issues(self, incident_id: str) -> list[dict[str, Any]]:
        """Загрузить задачи.

        Контракт: `IncidentsRead.yaml` `GET /incidents/{incidentId}/issues`
        (GetIncidentTaskList).

        :param incident_id: ID инцидента
        :return: Задачи инцидента
        """
        api_url = f"{self.__api_incidents}/{incident_id}/issues"
        url = f"https://{self.__core_hostname}{api_url}"
        response: list[dict[str, Any]] = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
        ).json()

        return [
            {
                "type": self.__lower(i.get("type")),
                "description": i.get("description"),
                "id": i.get("id"),
                "status": self.__lower(i.get("status")),
                "name": i.get("name"),
                "assigned": (i.get("assigned") or {}).get("id"),
                "estimated": i.get("estimatedTime"),
            }
            for i in response
        ]

    def __load_netfor(self, incident_id: str) -> dict[str, Any]:
        """Загрузить информацию по сетевому трафику (для инцидентов, созданных
        нажатием кнопки в PT NAD)

        Контракт: `IncidentsRead.yaml` `GET /incidents/{incidentId}/linkedObjects/
        netfor` (GetNetForensicData).

        :param incident_id: ID инцидента
        :return: Краткая информация о трафике, дополненная sessions/alerts
        """
        api_url = f"{self.__api_incidents}/{incident_id}/linkedObjects/netfor"
        url = f"https://{self.__core_hostname}{api_url}"
        response: dict[str, Any] = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
        ).json()

        netfor = dict(response)
        netfor["sessions"] = self.__load_netfor_sessions(incident_id)
        netfor["alerts"] = self.__load_netfor_alerts(incident_id)

        return netfor

    def __load_netfor_sessions(self, incident_id: str) -> list[dict[str, Any]]:
        """Загрузить информацию по сессиям в сетевом трафике (для инцидентов,
        созданных нажатием кнопки в PT NAD)

        Контракт: `IncidentsRead.yaml` `GET /incidents/{incidentId}/linkedObjects/
        netfor/sessions` (GetNetForensicSessions) - обход постранично до
        `.../sessions/count` (GetNetForensicSessionsCount).

        :param incident_id: ID инцидента
        :return: Сессии сетевого трафика
        """
        sessions_base = f"{self.__api_incidents}/{incident_id}/linkedObjects"
        return list(
            self.__iterate_paged_collection(
                f"{sessions_base}/netfor/sessions",
                f"{sessions_base}/netfor/sessions/count",
                self.__netfor_batch_size,
            )
        )

    def __load_netfor_alerts(self, incident_id: str) -> list[dict[str, Any]]:
        """Загрузить информацию по алертам в сетевом трафике (для инцидентов,
        созданных нажатием кнопки в PT NAD)

        Контракт: `IncidentsRead.yaml` `GET /incidents/{incidentId}/linkedObjects/
        netfor/alerts` (GetNetForensicAlerts) - обход постранично до
        `.../alerts/count` (GetNetForensicAlertsCount).

        :param incident_id: ID инцидента
        :return: Алерты сетевого трафика
        """
        alerts_base = f"{self.__api_incidents}/{incident_id}/linkedObjects"
        return list(
            self.__iterate_paged_collection(
                f"{alerts_base}/netfor/alerts",
                f"{alerts_base}/netfor/alerts/count",
                self.__netfor_batch_size,
            )
        )

    def __iterate_paged_collection(
        self,
        api_url: str,
        count_api_url: str | None = None,
        batch_size: int | None = None,
        extra_params: dict[str, Any] | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Обойти коллекцию Core постранично, сверяясь с count-эндпоинтом.

        :param api_url: Путь эндпоинта коллекции
        :param count_api_url: Необязательный путь count-эндпоинта коллекции
        :param batch_size: Размер пачки (по умолчанию `incidents_batch_size`)
        :param extra_params: Необязательные query-параметры коллекции
        :return: Итератор элементов коллекции
        """
        limit = batch_size or self.settings.incidents_batch_size
        offset = 0
        total_read = 0
        total_items = 0

        if count_api_url is not None:
            total_items = self.__load_count(count_api_url)

        while True:
            params: dict[str, Any] = {"offset": offset, "limit": limit}
            if extra_params is not None:
                params.update(extra_params)

            response: list[dict[str, Any]] = exec_request(
                self.__core_session,
                f"https://{self.__core_hostname}{api_url}",
                method="GET",
                timeout=self.settings.connection_timeout,
                params=params,
            ).json()

            items = response or []
            yield from items

            total_read += len(items)
            if len(items) < limit:
                break
            if count_api_url is not None and total_read >= total_items:
                break
            offset += limit

        if count_api_url is not None and total_items > total_read:
            self.log.warning(
                f"hostname={self.__core_hostname!r}, status=failed, "
                f'action=iterate_collection, msg="Core return {total_items!r} '
                f'items in {api_url!r}, but only {total_read!r} read"'
            )

    def __load_count(self, count_api_url: str) -> int:
        """Запросить количество элементов коллекции (`*CountDto`).

        :param count_api_url: Путь count-эндпоинта
        :return: Количество элементов
        """
        response: dict[str, Any] = exec_request(
            self.__core_session,
            f"https://{self.__core_hostname}{count_api_url}",
            method="GET",
            timeout=self.settings.connection_timeout,
        ).json()

        return int(response.get("count") or 0)

    def get_incident_short_info(self, incident_id: str) -> dict[str, Any]:
        """Получить краткую информацию по инциденту и его версию.

        Контракт: `IncidentsRead.yaml`
        `GET /api/v2/incidentsReadModel/{incidentId}/short`
        (GetIncidentShortInfo).

        :param incident_id: ID инцидента
        :return: {"id": "...", "key": "...", "name": "...", "source": "...",
            "version": 1}
        """
        api_url = f"{self.__api_incidents_read_model_v2}/{incident_id}/short"
        url = f"https://{self.__core_hostname}{api_url}"
        response: dict[str, Any] = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
        ).json()

        short_info = {
            "id": response.get("incidentId"),
            "key": response.get("key"),
            "name": response.get("name"),
            "source": response.get("source"),
            "version": response.get("version"),
        }

        self.log.info(
            f"status=success, action=get_incident_short_info, "
            f'msg="Got short info for incident", '
            f"hostname={self.__core_hostname!r}, incident_id={incident_id!r}, "
            f"version={short_info['version']!r}"
        )

        return short_info

    def get_incident_events_count(self, incident_id: str) -> int:
        """Получить количество событий инцидента.

        Контракт: `IncidentsRead.yaml`
        `GET /api/incidents/{incidentId}/events/count` (GetEventsCount).

        :param incident_id: ID инцидента
        :return: Количество событий
        """
        events_count = self.__load_count(
            f"{self.__api_incidents}/{incident_id}/events/count"
        )

        self.log.info(
            f"status=success, action=get_incident_events_count, "
            f'msg="Got events count", hostname={self.__core_hostname!r}, '
            f"incident_id={incident_id!r}, count={events_count!r}"
        )

        return events_count

    def get_incident_events_severity_counts(self, incident_id: str) -> list[dict]:
        """Получить разбивку событий инцидента по критичности.

        Контракт: `IncidentsRead.yaml`
        `GET /api/incidents/{incidentId}/events/severity_counts`
        (GetEventsSeverityCounts).

        :param incident_id: ID инцидента
        :return: [{"severity": "low", "count": 1}, ...]
        """
        api_url = f"{self.__api_incidents}/{incident_id}/events/severity_counts"
        response: list[dict[str, Any]] = exec_request(
            self.__core_session,
            f"https://{self.__core_hostname}{api_url}",
            method="GET",
            timeout=self.settings.connection_timeout,
        ).json()

        severities = [
            {"severity": self.__lower(i.get("severity")), "count": i.get("count")}
            for i in response
        ]

        self.log.info(
            f"status=success, action=get_incident_events_severity_counts, "
            f'msg="Got events severity counts", '
            f"hostname={self.__core_hostname!r}, incident_id={incident_id!r}, "
            f"count={len(severities)!r}"
        )

        return severities

    def get_incident_assets(self, incident_id: str, asset_type: str) -> list[dict]:
        """Получить активы инцидента по типу (`Targets` / `Attackers`).

        Контракт: `IncidentsRead.yaml`
        `GET /api/incidents/{incidentId}/assets/{assetType}`
        (GetIncidentAssets).

        :param incident_id: ID инцидента
        :param asset_type: Тип активов: `Targets` или `Attackers`
        :return: Список активов инцидента
        """
        if asset_type.capitalize() not in self.__allowed_asset_types:
            raise ValueError(
                f"asset_type must be one of {self.__allowed_asset_types!r}, "
                f"got {asset_type!r}"
            )

        api_url = (
            f"{self.__api_incidents}/{incident_id}/assets/{asset_type.capitalize()}"
        )
        response: list[dict[str, Any]] = exec_request(
            self.__core_session,
            f"https://{self.__core_hostname}{api_url}",
            method="GET",
            timeout=self.settings.connection_timeout,
        ).json()

        self.log.info(
            f"status=success, action=get_incident_assets, "
            f'msg="Got incident assets", hostname={self.__core_hostname!r}, '
            f"incident_id={incident_id!r}, asset_type={asset_type!r}, "
            f"count={len(response)!r}"
        )

        return response

    def get_incident_tasks(self, incident_ids: list[str]) -> list[dict]:
        """Получить задачи по списку инцидентов.

        Контракт: `IncidentsRead.yaml` `GET /api/incidentsReadModel/tasks`
        (GetTasks) - обход постранично.

        :param incident_ids: ID инцидентов
        :return: Список задач (`TaskDto`)
        """
        if not incident_ids:
            raise ValueError("incident_ids must not be empty")

        tasks = [
            {
                "id": task.get("id"),
                "key": task.get("key"),
                "name": task.get("name"),
                "status": self.__lower(task.get("status")),
                "type": self.__lower(task.get("type")),
                "assigned": (task.get("assigned") or {}).get("id"),
                "reporter": (task.get("reporter") or {}).get("id"),
                "incident_id": (task.get("incident") or {}).get("id"),
                "created": task.get("created"),
                "updated": task.get("updated"),
                "estimated": task.get("estimatedTime"),
            }
            for task in self.__iterate_paged_collection(
                self.__api_incident_tasks,
                extra_params={"incidentIds": incident_ids},
            )
        ]

        self.log.info(
            f"status=success, action=get_incident_tasks, "
            f'msg="Got incident tasks", hostname={self.__core_hostname!r}, '
            f"incidents={len(incident_ids)!r}, count={len(tasks)!r}"
        )

        return tasks

    def get_incident_issues_count(self, incident_id: str) -> int:
        """Получить количество задач инцидента.

        Контракт: `IncidentsRead.yaml`
        `GET /api/incidents/{incidentId}/issues/count` (GetIncidentTasksCount).

        :param incident_id: ID инцидента
        :return: Количество задач
        """
        issues_count = self.__load_count(
            f"{self.__api_incidents}/{incident_id}/issues/count"
        )

        self.log.info(
            f"status=success, action=get_incident_issues_count, "
            f'msg="Got issues count", hostname={self.__core_hostname!r}, '
            f"incident_id={incident_id!r}, count={issues_count!r}"
        )

        return issues_count

    def get_incident_netfor_alerts_count(self, incident_id: str) -> int:
        """Получить количество алертов сетевого трафика инцидента.

        Контракт: `IncidentsRead.yaml`
        `GET /api/incidents/{incidentId}/linkedObjects/netfor/alerts/count`
        (GetNetForensicAlertsCount).

        :param incident_id: ID инцидента
        :return: Количество алертов
        """
        alerts_count = self.__load_count(
            f"{self.__api_incidents}/{incident_id}/linkedObjects/netfor/alerts/count"
        )

        self.log.info(
            f"status=success, action=get_incident_netfor_alerts_count, "
            f'msg="Got netforensic alerts count", '
            f"hostname={self.__core_hostname!r}, incident_id={incident_id!r}, "
            f"count={alerts_count!r}"
        )

        return alerts_count

    def get_incident_netfor_sessions_count(self, incident_id: str) -> int:
        """Получить количество сессий сетевого трафика инцидента.

        Контракт: `IncidentsRead.yaml`
        `GET /api/incidents/{incidentId}/linkedObjects/netfor/sessions/count`
        (GetNetForensicSessionsCount).

        :param incident_id: ID инцидента
        :return: Количество сессий
        """
        sessions_count = self.__load_count(
            f"{self.__api_incidents}/{incident_id}/linkedObjects/netfor/sessions/count"
        )

        self.log.info(
            f"status=success, action=get_incident_netfor_sessions_count, "
            f'msg="Got netforensic sessions count", '
            f"hostname={self.__core_hostname!r}, incident_id={incident_id!r}, "
            f"count={sessions_count!r}"
        )

        return sessions_count

    def get_incident_queries(self, category: str = "List") -> list[dict]:
        """Получить сохранённые запросы инцидентов по категории.

        Контракт: `IncidentsRead.yaml` `GET /api/incident_queries`
        (GetIncidentQuieriesByCategory).

        :param category: Категория запроса: `List` или `Notifications`
        :return: [{"id": "all_incidents", "name": "Все инциденты"}, ...]
        """
        if category.capitalize() not in (
            self.QueryCategory.LIST,
            self.QueryCategory.NOTIFICATIONS,
        ):
            raise ValueError(
                f"category must be {self.QueryCategory.LIST!r} or "
                f"{self.QueryCategory.NOTIFICATIONS!r}, got {category!r}"
            )

        response: list[dict[str, Any]] = exec_request(
            self.__core_session,
            f"https://{self.__core_hostname}{self.__api_incident_queries}",
            method="GET",
            timeout=self.settings.connection_timeout,
            params={"category": category.capitalize()},
        ).json()

        self.log.info(
            f"status=success, action=get_incident_queries, "
            f'msg="Got incident queries", hostname={self.__core_hostname!r}, '
            f"category={category!r}, count={len(response)!r}"
        )

        return response

    def get_incidents_settings(self) -> dict[str, Any]:
        """Получить настройки подсистемы инцидентов.

        Контракт: `IncidentsRead.yaml` `GET /api/incidents/settings`
        (GetSettings).

        :return: {"showContentFromDescendants": false}
        """
        response: dict[str, Any] = exec_request(
            self.__core_session,
            f"https://{self.__core_hostname}{self.__api_incidents_settings}",
            method="GET",
            timeout=self.settings.connection_timeout,
        ).json()

        settings = {
            "show_content_from_descendants": response.get("showContentFromDescendants")
        }

        self.log.info(
            f"status=success, action=get_incidents_settings, "
            f'msg="Got incidents settings", hostname={self.__core_hostname!r}'
        )

        return settings

    def get_incident_statuses(self) -> list[str]:
        """Получить все статусы инцидентов.

        Контракт: `IncidentStatus.yaml` `GET /api/statuses`
        (GetIncidentStatuses).

        :return: ["New", "Approved", "InProgress", "Resolved", "Closed"]
        """
        return self.__load_dictionary(self.__api_statuses, "get_incident_statuses")

    def get_incident_statuses_localizations(self) -> list[dict]:
        """Получить локализации статусов инцидентов.

        Контракт: `IncidentStatus.yaml` `GET /api/statuses/localizations`
        (GetIncidentStatusesLocalizations).

        :return: [{"key": "New", "value": "Новый"}, ...]
        """
        return self.__load_dictionary(
            self.__api_statuses_localizations,
            "get_incident_statuses_localizations",
        )

    def get_incident_severities(self) -> list[str]:
        """Получить все критичности инцидентов.

        Контракт: `IncidentSeverity.yaml` `GET /api/severities`
        (GetIncidentSeverities).

        :return: ["High", "Medium", "Low"]
        """
        return self.__load_dictionary(self.__api_severities, "get_incident_severities")

    def get_incident_severities_localizations(self) -> list[dict]:
        """Получить локализации критичностей инцидентов.

        Контракт: `IncidentSeverity.yaml` `GET /api/severities/localizations`
        (GetIncidentSeveritiesLocalizations).

        :return: [{"key": "High", "value": "Высокая"}, ...]
        """
        return self.__load_dictionary(
            self.__api_severities_localizations,
            "get_incident_severities_localizations",
        )

    def get_incident_task_statuses(self) -> list[str]:
        """Получить все статусы задач инцидентов.

        Контракт: `IncidentTaskStatus.yaml` `GET /api/taskStatuses`
        (GetIncidentTaskStatuses).

        :return: ["New", "Assigned", "InProgress", "Closed"]
        """
        return self.__load_dictionary(
            self.__api_task_statuses, "get_incident_task_statuses"
        )

    def get_incident_task_types(self) -> list[str]:
        """Получить все типы задач инцидентов.

        Контракт: `IncidentTaskType.yaml` `GET /api/taskTypes`
        (GetIncidentTaskTypes).

        :return: ["Investigation", "EvidenceGathering", "Recovery"]
        """
        return self.__load_dictionary(self.__api_task_types, "get_incident_task_types")

    def get_incident_task_statuses_localizations(self) -> list[dict]:
        """Получить локализованные статусы задач инцидентов.

        Контракт: `IncidentTaskStatus.yaml`
        `GET /api/issues/dictionaries/statuses`
        (GetIncidentTaskStatusesLocalizations).

        :return: [{"id": "New", "name": "Новая"}, ...]
        """
        return self.__load_dictionary(
            self.__api_task_statuses_localizations,
            "get_incident_task_statuses_localizations",
        )

    def get_incident_task_types_localizations(self) -> list[dict]:
        """Получить локализованные типы задач инцидентов.

        Контракт: `IncidentTaskType.yaml` `GET /api/issues/dictionaries/types`
        (GetIncidentTaskTypesLocalizations).

        :return: [{"id": "Investigation", "name": "Расследование"}, ...]
        """
        return self.__load_dictionary(
            self.__api_task_types_localizations,
            "get_incident_task_types_localizations",
        )

    def __load_dictionary(self, api_url: str, action: str) -> list[Any]:
        """Запросить справочник инцидентов (значения или локализации).

        Core отдаёт как список строк (значения enum), так и список словарей
        (локализации) - контракт различает эти формы, наружу выпускаем ответ
        как есть.

        :param api_url: Путь эндпоинта справочника
        :param action: Имя действия для логирования
        :return: Ответ Core как есть (словари или список строк)
        """
        response: list[Any] = exec_request(
            self.__core_session,
            f"https://{self.__core_hostname}{api_url}",
            method="GET",
            timeout=self.settings.connection_timeout,
        ).json()

        self.log.info(
            f"status=success, action={action}, "
            f'msg="Got dictionary", hostname={self.__core_hostname!r}, '
            f"count={len(response)!r}"
        )

        return response

    def close(self) -> None:
        if self.__core_session is not None:
            self.__core_session.close()
