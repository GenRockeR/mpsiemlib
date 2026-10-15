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
    # Справочники контракта `Dictionaries.yaml`: `GET /api/incidents/dictionaries/*`.
    # Статусы и критичности дублируют семейства `IncidentStatus`/
    # `IncidentSeverity`, поэтому берются через специализированные эндпоинты
    # выше; типы инцидентов есть только здесь.
    __api_dictionaries = "/api/incidents/dictionaries"
    __api_dictionary_categories = f"{__api_dictionaries}/categories"
    __api_dictionary_influences = f"{__api_dictionaries}/influences"
    __api_dictionary_types = f"{__api_dictionaries}/types"
    __api_dictionary_parameters = f"{__api_dictionaries}/parameters"
    __api_dictionary_types_hierarchy = f"{__api_dictionaries}/types/hierarchy"
    __netfor_batch_size = 2000

    # Read-контракты: статистика (IncidentStatistics.yaml), задачи/коллекции
    # read-model (IncidentsRead.yaml), синхронизация (IncidentsSync.yaml),
    # внешний публичный id (IncidentsReadPublic.yaml), фильтры и папки
    # (IncidentFilter.yaml / IncidentFilterFolder.yaml)
    __api_statistics = "/api/incidentsReadModel/statistics/incidents"
    __api_deleted_incident_ids = "/api/incidentsReadModel/deletedIncidentIds"
    __api_read_model_issues = "/api/incidentsReadModel/issues"
    __api_incident_task_read = "/api/incidentsReadModel/tasks"
    __api_incident_events_v2 = "/api/incidentsReadModel/v2/incidents"
    __api_sync_incidents = "/api/sync/incidents"
    __api_external_incident_ids = "/api/v1/external_incident_ids"
    __api_filters = "/api/v1/incidents/filters"
    __api_filters_default = "/api/v1/incidents/filters/default"
    __api_filters_public = "/api/incident_filters"
    __api_filters_hierarchy = "/api/v1/incidents/filters_hierarchy"
    __api_filters_hierarchy_notifications = (
        "/api/v1/notifications/incidents/filters_hierarchy"
    )
    __api_filter_folders = "/api/v1/incidents/folders"

    # Write-контракты (Incidents.yaml): создание/правка инцидента, смена
    # статуса/мер/групп, связывание событий, задачи, удаление, экспорт
    __api_incidents_v2 = "/api/v2/incidents"
    __api_incident_events_link = "/api/incidents/events"
    __api_issues_write = "/api/issues"
    __api_export = "/api/v1/incidents/export"

    GRANULARITIES = ("Day", "Week", "Month", "Quarter")
    # IncidentAssetType / IncidentElementType контракта Incidents.yaml
    # (значения Undefined в URL не передаются)
    ASSET_TYPES = ("Targets", "Attackers")
    ELEMENT_TYPES = ("Group", "Asset", "Network", "Address", "Other")

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
    def __check_enum(value: str, allowed: tuple[str, ...], name: str) -> None:
        """Проверить значение enum-параметра до обращения к Core.

        :param value: Проверяемое значение
        :param allowed: Допустимые значения
        :param name: Имя параметра для сообщения об ошибке
        """
        if value not in allowed:
            raise ValueError(
                f"Unknown {name} {value!r}; expected one of {list(allowed)!r}"
            )

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

        Замерено на R28.0: эндпоинт отвечает `TaskFullDto`, где ID задачи -
        `taskId`, а инцидент - `innerIncident.incidentId` (в отличие от
        `TaskDto` того же контракта, где это `id`/`incident.id`).

        :param incident_ids: ID инцидентов
        :return: Список задач (`TaskDto`)
        """
        if not incident_ids:
            raise ValueError("incident_ids must not be empty")

        tasks = [
            {
                "id": task.get("taskId"),
                "key": task.get("key"),
                "name": task.get("name"),
                "status": self.__lower(task.get("status")),
                "type": self.__lower(task.get("type")),
                "assigned": (task.get("assigned") or {}).get("id"),
                "reporter": (task.get("reporter") or {}).get("id"),
                "incident_id": (task.get("innerIncident") or {}).get("incidentId"),
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

    def get_incident_categories(self) -> list[dict]:
        """Получить справочник категорий инцидентов.

        Контракт: `Dictionaries.yaml` `GET /api/incidents/dictionaries/categories`
        (GetIncidentCategoriesDictionary).

        :return: [{"id": "...", "name": "..."}, ...]
        """
        return self.__load_dictionary(
            self.__api_dictionary_categories, "get_incident_categories"
        )

    def get_incident_influences(self) -> list[dict]:
        """Получить справочник влияний инцидентов.

        Контракт: `Dictionaries.yaml` `GET /api/incidents/dictionaries/influences`
        (GetIncidentInfluencesDictionary).

        :return: [{"id": "...", "name": "..."}, ...]
        """
        return self.__load_dictionary(
            self.__api_dictionary_influences, "get_incident_influences"
        )

    def get_incident_types(self) -> list[dict]:
        """Получить справочник типов инцидентов.

        Контракт: `Dictionaries.yaml` `GET /api/incidents/dictionaries/types`
        (GetIncidentTypesDictionary).

        :return: [{"id": "...", "name": "..."}, ...]
        """
        return self.__load_dictionary(self.__api_dictionary_types, "get_incident_types")

    def get_incident_parameters(self) -> list[dict]:
        """Получить справочник параметров инцидентов.

        Контракт: `Dictionaries.yaml` `GET /api/incidents/dictionaries/parameters`
        (GetIncidentParametersDictionary).

        :return: [{"id": "...", "name": "...", "incidentTypeId": "...",
            "isHidden": ...}, ...]
        """
        return self.__load_dictionary(
            self.__api_dictionary_parameters, "get_incident_parameters"
        )

    def get_incident_types_hierarchy(self) -> list[dict]:
        """Получить иерархию типов инцидентов по категориям.

        Контракт: `Dictionaries.yaml`
        `GET /api/incidents/dictionaries/types/hierarchy`
        (GetIncidentTypesHierarchy).

        :return: [{"categoryId": "...", "name": "...",
            "types": [{"typeId": "...", "name": "..."}]}, ...]
        """
        return self.__load_dictionary(
            self.__api_dictionary_types_hierarchy, "get_incident_types_hierarchy"
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

    # ------------------------------------------------------------------ #
    # HTTP-хелперы (с общим логом неуспеха по действию модуля)
    # ------------------------------------------------------------------ #

    def __request(
        self,
        action: str,
        api_url: str,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        json_body: Any = None,
        timeout_up: float = 1,
    ) -> Any:
        """Выполнить запрос и вернуть распарсенный JSON (или None для пустого).

        :param action: Имя действия для лога
        :param api_url: Путь эндпоинта (без хоста)
        :param method: GET|POST|PUT|DELETE
        :param params: Query-параметры
        :param json_body: Тело запроса
        :param timeout_up: увеличение тайм-аута (для экспорта)
        :return: Распарсенный JSON либо None, если тело пустое
        :raises requests.RequestException: при ошибке выполнения запроса
        """
        try:
            response = exec_request(
                self.__core_session,
                f"https://{self.__core_hostname}{api_url}",
                method=method,
                timeout=self.settings.connection_timeout,
                timeout_up=timeout_up,
                params=params,
                json=json_body,
            )
        except Exception as err:
            self.log.warning(
                f"status=failed, action={action}, "
                f'msg="{method} {api_url} failed", '
                f"hostname={self.__core_hostname!r}, error={err!r}"
            )
            raise

        if not response.content:
            return None
        return response.json()

    def __no_content(
        self,
        action: str,
        api_url: str,
        method: str = "PUT",
        params: dict[str, Any] | None = None,
        json_body: Any = None,
    ) -> bool:
        """Выполнить запрос, ожидающий пустой ответ (204).

        :return: True при успехе
        """
        self.__request(
            action, api_url, method=method, params=params, json_body=json_body
        )

        self.log.info(
            f"status=success, action={action}, "
            f'msg="{method} {api_url} accepted", '
            f"hostname={self.__core_hostname!r}"
        )
        return True

    @staticmethod
    def __iso_time(timestamp: int | str) -> str:
        """Нормализовать время в формат ISO 8601, который принимает Core.

        :param timestamp: Unix-время (int) либо готовая ISO-строка
        :return: Строка `YYYY-MM-DDTHH:MM:SS.000Z`
        """
        if isinstance(timestamp, str):
            return timestamp
        return datetime.fromtimestamp(timestamp, tz=UTC).strftime(
            Incidents.__time_format
        )

    def __statistics_body(
        self,
        start: int | str,
        end: int | str,
        granularity: str,
        query_ids: list[str] | None = None,
        groups_filter: dict[str, Any] | None = None,
        time_zone_offset: str | None = None,
    ) -> dict[str, Any]:
        """Собрать тело запроса статистики (`GetIncidents*Params`)."""
        body: dict[str, Any] = {
            "groups": groups_filter or {"filterType": "no_filter"},
            "startDate": self.__iso_time(start),
            "endDate": self.__iso_time(end),
            "granularity": granularity,
            "queryIds": query_ids or ["all_incidents"],
        }
        if time_zone_offset is not None:
            body["timeZoneOffset"] = time_zone_offset
        return body

    def __statistics(
        self, action: str, endpoint: str, body: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Выполнить запрос статистики и вернуть список периодов."""
        response: list[dict[str, Any]] = self.__request(
            action,
            f"{self.__api_statistics}/{endpoint}",
            method="POST",
            json_body=body,
        )

        self.log.info(
            f"status=success, action={action}, "
            f'msg="Got statistics", hostname={self.__core_hostname!r}, '
            f"periods={len(response)!r}"
        )
        return response

    def get_incidents_status_statistics(
        self,
        start: int | str,
        end: int | str,
        granularity: str = "Day",
        query_ids: list[str] | None = None,
        groups_filter: dict[str, Any] | None = None,
        time_zone_offset: str | None = None,
    ) -> list[dict[str, Any]]:
        """Статистика статусов инцидентов по периодам.

        Контракт: `IncidentStatistics.yaml` `POST
        /api/incidentsReadModel/statistics/incidents/statuses`
        (GetIncidentsStatuses).

        :param start: Начало периода (Unix-время или ISO 8601)
        :param end: Конец периода (Unix-время или ISO 8601)
        :param granularity: Granularity: `Day`/`Week`/`Month`/`Quarter`
        :param query_ids: Сохранённые запросы (по умолчанию `all_incidents`)
        :param groups_filter: `BaseGroupsFilter` (по умолчанию `no_filter`)
        :param time_zone_offset: Смещение тайм-зоны вида `+03:00`
        :return: `[{periodStart, periodEnd, new, approved, inProgress,
            resolved, closed}, ...]`
        """
        self.__check_enum(granularity, self.GRANULARITIES, "granularity")
        return self.__statistics(
            "get_incidents_status_statistics",
            "statuses",
            self.__statistics_body(
                start, end, granularity, query_ids, groups_filter, time_zone_offset
            ),
        )

    def get_created_incidents_distribution(
        self,
        start: int | str,
        end: int | str,
        granularity: str = "Day",
        query_ids: list[str] | None = None,
        groups_filter: dict[str, Any] | None = None,
        time_zone_offset: str | None = None,
    ) -> list[dict[str, Any]]:
        """Распределение созданных инцидентов по критичности по дням.

        Контракт: `IncidentStatistics.yaml` `POST
        /api/incidentsReadModel/statistics/incidents/created`
        (GetCreatedIncidentsDistribution).

        :return: `[{periodStart, periodEnd, criticalSeverity, highSeverity,
            mediumSeverity, lowSeverity, totalDuration}, ...]`
        """
        self.__check_enum(granularity, self.GRANULARITIES, "granularity")
        return self.__statistics(
            "get_created_incidents_distribution",
            "created",
            self.__statistics_body(
                start, end, granularity, query_ids, groups_filter, time_zone_offset
            ),
        )

    def get_closed_incidents_distribution(
        self,
        start: int | str,
        end: int | str,
        granularity: str = "Day",
        query_ids: list[str] | None = None,
        groups_filter: dict[str, Any] | None = None,
        time_zone_offset: str | None = None,
    ) -> list[dict[str, Any]]:
        """Статистика закрытых инцидентов по периодам.

        Контракт: `IncidentStatistics.yaml` `POST
        /api/incidentsReadModel/statistics/incidents/closed`
        (GetClosedIncidentsDistribution).

        :return: Распределение по критичности (см.
            :meth:`get_created_incidents_distribution`)
        """
        self.__check_enum(granularity, self.GRANULARITIES, "granularity")
        return self.__statistics(
            "get_closed_incidents_distribution",
            "closed",
            self.__statistics_body(
                start, end, granularity, query_ids, groups_filter, time_zone_offset
            ),
        )

    def get_open_incidents_distribution(
        self,
        start: int | str,
        end: int | str,
        granularity: str = "Day",
        query_ids: list[str] | None = None,
        groups_filter: dict[str, Any] | None = None,
        time_zone_offset: str | None = None,
    ) -> list[dict[str, Any]]:
        """Статистика незакрытых инцидентов по периодам.

        Контракт: `IncidentStatistics.yaml` `POST
        /api/incidentsReadModel/statistics/incidents/open`
        (GetOpenIncidentsDistribution).

        :return: Распределение по критичности (см.
            :meth:`get_created_incidents_distribution`)
        """
        self.__check_enum(granularity, self.GRANULARITIES, "granularity")
        return self.__statistics(
            "get_open_incidents_distribution",
            "open",
            self.__statistics_body(
                start, end, granularity, query_ids, groups_filter, time_zone_offset
            ),
        )

    # ------------------------------------------------------------------ #
    # Read: задачи / коллекции read-model (IncidentsRead.yaml)
    # ------------------------------------------------------------------ #

    def get_incident_task_by_id(self, task_id: str) -> dict[str, Any]:
        """Получить задачу инцидента по её ID.

        Контракт: `IncidentsRead.yaml` `GET
        /api/incidentsReadModel/tasks/{incidentTaskId}` (GetTask).

        Замерено на R28.0: неизвестный ID сервер отвечает 200 с пустым телом
        (не 404), поэтому отсутствующая задача поднимает ValueError.

        :param task_id: ID задачи
        :return: `TaskDto`
        :raises ValueError: задача не найдена (пустой ответ сервера)
        :raises requests.RequestException: пустой ID (400)
        """
        response: dict[str, Any] | None = self.__request(
            "get_incident_task_by_id", f"{self.__api_incident_task_read}/{task_id}"
        )

        if not response:
            self.log.error(
                f"status=failed, action=get_incident_task_by_id, "
                f'msg="Core returned empty task", '
                f"hostname={self.__core_hostname!r}, task_id={task_id!r}"
            )
            raise ValueError(f"Incident task {task_id!r} not found")

        self.log.info(
            f"status=success, action=get_incident_task_by_id, "
            f'msg="Got incident task", hostname={self.__core_hostname!r}, '
            f"task_id={task_id!r}"
        )
        return response

    def get_incident_issue_by_id(self, issue_id: str) -> dict[str, Any]:
        """Получить задачу (issue) по её ID.

        Контракт: `IncidentsRead.yaml` `GET
        /api/incidentsReadModel/issues/{issueId}` (GetIssue).

        :param issue_id: ID задачи
        :return: `TaskDto`
        :raises requests.RequestException: неизвестный ID (400)
        """
        response: dict[str, Any] = self.__request(
            "get_incident_issue_by_id", f"{self.__api_read_model_issues}/{issue_id}"
        )

        self.log.info(
            f"status=success, action=get_incident_issue_by_id, "
            f'msg="Got incident issue", hostname={self.__core_hostname!r}, '
            f"issue_id={issue_id!r}"
        )
        return response

    def get_incident_issues(
        self, offset: int = 0, limit: int | None = None
    ) -> list[dict[str, Any]]:
        """Получить все задачи инцидентов (read-model).

        Контракт: `IncidentsRead.yaml` `GET /api/incidentsReadModel/issues`
        (GetIssues).

        :param offset: Смещение страницы
        :param limit: Размер страницы (по умолчанию `incidents_batch_size`)
        :return: Список `TaskDto`
        """
        response: list[dict[str, Any]] = self.__request(
            "get_incident_issues",
            self.__api_read_model_issues,
            params={
                "offset": offset,
                "limit": limit or self.settings.incidents_batch_size,
            },
        )

        self.log.info(
            f"status=success, action=get_incident_issues, "
            f'msg="Got incidents issues", hostname={self.__core_hostname!r}, '
            f"count={len(response)!r}"
        )
        return response

    def get_incident_events_by_id(self, incident_id: str) -> list[dict[str, Any]]:
        """Получить события инцидента (v2, только id/timestamp).

        Контракт: `Incidents.yaml` `GET
        /api/incidentsReadModel/v2/incidents/{incidentId}/events`
        (GetIncidentEvents).

        :param incident_id: ID инцидента
        :return: Список `EventAddDto`
        """
        response: list[dict[str, Any]] = self.__request(
            "get_incident_events_by_id",
            f"{self.__api_incident_events_v2}/{incident_id}/events",
        )

        self.log.info(
            f"status=success, action=get_incident_events_by_id, "
            f'msg="Got incident events (v2)", hostname={self.__core_hostname!r}, '
            f"incident_id={incident_id!r}, count={len(response)!r}"
        )
        return response

    def get_deleted_incident_ids(self, incident_ids: list[str]) -> list[str]:
        """Отфильтровать ID инцидентов, оставив только удалённые.

        Контракт: `IncidentsRead.yaml` `POST
        /api/incidentsReadModel/deletedIncidentIds` (GetDeletedIncidentIds).

        Замерено на R28.0: сервер возвращает те из переданных ID, которых нет в
        read-model, - на стенде с непрогнанным read-model в ответ попадают и
        живые инциденты, поэтому полагаться на ответ как на признак удаления
        нельзя.

        :param incident_ids: ID инцидентов на проверку
        :return: Подмножество ID, которых нет в read-model
        :raises ValueError: пустой список (сервер требует тело)
        """
        if not incident_ids:
            raise ValueError("incident_ids must not be empty")

        response: list[str] = self.__request(
            "get_deleted_incident_ids",
            self.__api_deleted_incident_ids,
            method="POST",
            json_body=incident_ids,
        )

        self.log.info(
            f"status=success, action=get_deleted_incident_ids, "
            f'msg="Got deleted incident ids", hostname={self.__core_hostname!r}, '
            f"requested={len(incident_ids)!r}, deleted={len(response)!r}"
        )
        return response

    # ------------------------------------------------------------------ #
    # Read: синхронизация и публичный id
    # (IncidentsSync.yaml / IncidentsReadPublic.yaml)
    # ------------------------------------------------------------------ #

    def get_sync_incidents_short_info(self) -> list[dict[str, Any]]:
        """Получить краткую сводку всех инцидентов для синхронизации.

        Контракт: `IncidentsSync.yaml` `GET /api/sync/incidents`
        (GetAllIncidentShortInfo).

        :return: Список кратких сведений (на пустом стенде - пустой список)
        """
        response: list[dict[str, Any]] = self.__request(
            "get_sync_incidents_short_info", self.__api_sync_incidents
        )

        self.log.info(
            f"status=success, action=get_sync_incidents_short_info, "
            f'msg="Got sync incidents", hostname={self.__core_hostname!r}, '
            f"count={len(response)!r}"
        )
        return response

    def get_incident_version(self, incident_id: str) -> int:
        """Получить номер последней версии инцидента.

        Контракт: `IncidentsSync.yaml` `GET
        /api/sync/incidents/{incidentId}/latestversionnumber` (GetVersion).

        :param incident_id: ID инцидента
        :return: Номер версии (0, если изменений не было)
        """
        version: int = self.__request(
            "get_incident_version",
            f"{self.__api_sync_incidents}/{incident_id}/latestversionnumber",
        )

        self.log.info(
            f"status=success, action=get_incident_version, "
            f'msg="Got incident version", hostname={self.__core_hostname!r}, '
            f"incident_id={incident_id!r}, version={version!r}"
        )
        return version

    def get_public_incident_id(self, incident_id: str) -> dict[str, Any]:
        """Получить публичный (Key) идентификатор инцидента по-внутреннему ID.

        Контракт: `IncidentsReadPublic.yaml` `GET
        /api/v1/external_incident_ids/{internalId}` (GetPublicByInternalId).

        :param incident_id: Внутренний ID инцидента
        :return: `{id, publicId}`
        :raises requests.RequestException: инцидент не найден (400)
        """
        response: dict[str, Any] = self.__request(
            "get_public_incident_id",
            f"{self.__api_external_incident_ids}/{incident_id}",
        )

        self.log.info(
            f"status=success, action=get_public_incident_id, "
            f'msg="Got public incident id", hostname={self.__core_hostname!r}, '
            f"incident_id={incident_id!r}, public_id={response.get('publicId')!r}"
        )
        return response

    # ------------------------------------------------------------------ #
    # Read: фильтры и папки (IncidentFilter.yaml / IncidentFilterFolder.yaml)
    # ------------------------------------------------------------------ #

    def get_incident_filter(self, filter_id: str) -> dict[str, Any]:
        """Получить фильтр инцидентов по ID.

        Контракт: `IncidentFilter.yaml` `GET
        /api/v1/incidents/filters/{filterId}` (GetFilter).

        :param filter_id: ID фильтра
        :return: `FilterViewModel`
        :raises requests.RequestException: неизвестный ID (400)
        """
        response: dict[str, Any] = self.__request(
            "get_incident_filter", f"{self.__api_filters}/{filter_id}"
        )

        self.log.info(
            f"status=success, action=get_incident_filter, "
            f'msg="Got incident filter", hostname={self.__core_hostname!r}, '
            f"filter_id={filter_id!r}"
        )
        return response

    def get_default_incident_filter(self) -> dict[str, Any]:
        """Получить фильтр инцидентов, выбранный по умолчанию.

        Контракт: `IncidentFilter.yaml` `GET
        /api/v1/incidents/filters/default` (GetDefaultFilter).

        Замерено на R28.0: приватный маршрут нестабилен и отвечает
        500 internal.error (`source: Unkown`); рабочий аналог -
        :meth:`get_default_incident_filter_public`.

        :return: `FilterViewModel`
        """
        response: dict[str, Any] = self.__request(
            "get_default_incident_filter", self.__api_filters_default
        )

        self.log.info(
            f"status=success, action=get_default_incident_filter, "
            f'msg="Got default incident filter", '
            f"hostname={self.__core_hostname!r}, "
            f"filter_id={response.get('id')!r}"
        )
        return response

    def get_default_incident_filter_public(self) -> dict[str, Any]:
        """Получить дефолтный фильтр через публичный маршрут.

        Контракт: `IncidentFilter.yaml` `GET
        /api/incident_filters/default` (GetDefaultFilterPublic).

        :return: `FilterViewModel`
        """
        response: dict[str, Any] = self.__request(
            "get_default_incident_filter_public",
            f"{self.__api_filters_public}/default",
        )

        self.log.info(
            f"status=success, action=get_default_incident_filter_public, "
            f'msg="Got default incident filter (public)", '
            f"hostname={self.__core_hostname!r}, "
            f"filter_id={response.get('id')!r}"
        )
        return response

    def get_incident_filter_public(self, filter_id: str) -> dict[str, Any]:
        """Получить фильтр инцидентов через публичный маршрут.

        Контракт: `IncidentFilter.yaml` `GET
        /api/incident_filters/{filterId}` (GetIncidentFilterPublic).

        :param filter_id: ID фильтра
        :return: `FilterViewModel`
        """
        response: dict[str, Any] = self.__request(
            "get_incident_filter_public", f"{self.__api_filters_public}/{filter_id}"
        )

        self.log.info(
            f"status=success, action=get_incident_filter_public, "
            f'msg="Got incident filter (public)", '
            f"hostname={self.__core_hostname!r}, filter_id={filter_id!r}"
        )
        return response

    def get_incident_filters_by_ids(
        self, filter_ids: list[str]
    ) -> list[dict[str, Any]]:
        """Получить краткие сведения о фильтрах по списку ID.

        Контракт: `IncidentFilter.yaml` `GET /api/v1/incidents/filters`
        (GetFiltersByIds). Неизвестные ID сервер пропускает.

        :param filter_ids: ID фильтров
        :return: Список `FilterInfoDto` - `{id, name, isRemoved}`
        :raises ValueError: пустой список (параметр обязателен)
        """
        if not filter_ids:
            raise ValueError("filter_ids must not be empty")

        response: list[dict[str, Any]] = self.__request(
            "get_incident_filters_by_ids",
            self.__api_filters,
            params={"filterId": filter_ids},
        )

        self.log.info(
            f"status=success, action=get_incident_filters_by_ids, "
            f'msg="Got incident filters by ids", '
            f"hostname={self.__core_hostname!r}, count={len(response)!r}"
        )
        return response

    def validate_incident_filter(self, filter_body: dict[str, Any]) -> bool:
        """Проверить корректность фильтра инцидентов без сохранения.

        Контракт: `IncidentFilter.yaml` `POST
        /api/v1/incidents/filters/validate` (ValidateFilter).

        :param filter_body: `{select, where, orderBy}`
        :return: True если сервер ответил `Valid`
        """
        # Замерено на R28.0: сервер проверяет только синтаксис `where` - пустое
        # тело и тело без `where` возвращают `Valid`
        response: str = self.__request(
            "validate_incident_filter",
            f"{self.__api_filters}/validate",
            method="POST",
            json_body=filter_body,
        )

        self.log.info(
            f"status=success, action=validate_incident_filter, "
            f'msg="Filter validation result", '
            f"hostname={self.__core_hostname!r}, result={response!r}"
        )
        return response == "Valid"

    def get_incident_filters_hierarchy(
        self, notifications: bool = False
    ) -> dict[str, Any]:
        """Получить дерево фильтров и папок инцидентов.

        Контракт: `IncidentFilterFolder.yaml`
        `GET /api/v1/incidents/filters_hierarchy` (GetFilterFolderTree) либо
        `GET /api/v1/notifications/incidents/filters_hierarchy`
        (GetFilterFolderTreeForNotifications).

        :param notifications: True - дерево для настроек уведомлений
        :return: `{roots: [{type, id, name, ...}, ...]}`
        """
        api_url = (
            self.__api_filters_hierarchy_notifications
            if notifications
            else self.__api_filters_hierarchy
        )
        action = (
            "get_notifications_incident_filters_hierarchy"
            if notifications
            else "get_incident_filters_hierarchy"
        )
        response: dict[str, Any] = self.__request(action, api_url)

        self.log.info(
            f"status=success, action={action}, "
            f'msg="Got incident filters hierarchy", '
            f"hostname={self.__core_hostname!r}, "
            f"roots={len(response.get('roots') or [])!r}"
        )
        return response

    # ------------------------------------------------------------------ #
    # Write: фильтры и папки фильтров (IncidentFilter.yaml,
    # IncidentFilterFolder.yaml)
    #
    # Замерено на R28.0: запись разрешена только для объектов пользователя
    # (папки в ветке «Пользовательские фильтры»). Системные папки/фильтры
    # (guuid вида 00000000-0000-0000-0000-...) отвечают 403 forbidden.error.
    # Удаление папки каскадно снимает вложенные папки и фильтры.
    # ------------------------------------------------------------------ #

    def create_incident_filter(
        self,
        folder_id: str,
        name: str,
        select: list[str],
        where: str = "",
        orderby: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Создать фильтр инцидентов.

        Контракт: `IncidentFilter.yaml` `POST /api/v1/incidents/filters`
        (CreateFilter).

        Замерено на R28.0: контракт объявляет обязательным только `folderId`,
        но сервер требует ещё `name`, `select` и `where` (400
        `Value cannot be null. (Parameter 'where')`), поэтому они вынесены в
        обязательные параметры. Невалидный `where` отбивается на 400
        `Некорректный формат фильтра`.

        :param folder_id: UUID папки (только пользовательская ветка)
        :param name: Название фильтра
        :param select: Выбираемые поля
        :param where: Условие отбора (пустая строка - без условия)
        :param orderby: Сортировка `[{field, sortOrder}]`,
            `sortOrder` - `ascending`|`descending`
        :return: `{id}` - UUID созданного фильтра
        :raises requests.RequestException: 400 невалидное тело/условие,
            403 нет прав на папку
        """
        body: dict[str, Any] = {
            "folderId": folder_id,
            "name": name,
            "select": select,
            "where": where,
        }
        if orderby is not None:
            body["orderby"] = orderby

        response: dict[str, Any] = self.__request(
            "create_incident_filter", self.__api_filters, method="POST", json_body=body
        )

        self.log.info(
            f"status=success, action=create_incident_filter, "
            f'msg="Incident filter created", '
            f"hostname={self.__core_hostname!r}, filter_id={response.get('id')!r}"
        )
        return response

    def update_incident_filter(
        self,
        filter_id: str,
        folder_id: str,
        name: str,
        select: list[str],
        where: str = "",
        orderby: list[dict[str, Any]] | None = None,
    ) -> bool:
        """Обновить фильтр инцидентов.

        Контракт: `IncidentFilter.yaml` `PUT /api/v1/incidents/filters/{filterId}`
        (UpdateFilter).

        Замерено на R28.0: тело такое же, как у создания - `name`, `select` и
        `where` обязательны (400 `Value cannot be null. (Parameter 'where')`);
        несуществующий фильтр - 400 `Фильтр не существует`. Частичное
        обновление не поддерживается.

        :return: True при успехе (204)
        """
        body: dict[str, Any] = {
            "folderId": folder_id,
            "name": name,
            "select": select,
            "where": where,
        }
        if orderby is not None:
            body["orderby"] = orderby

        return self.__no_content(
            "update_incident_filter",
            f"{self.__api_filters}/{filter_id}",
            method="PUT",
            json_body=body,
        )

    def delete_incident_filter(self, filter_id: str) -> bool:
        """Удалить фильтр инцидентов.

        Контракт: `IncidentFilter.yaml`
        `DELETE /api/v1/incidents/filters/{filterId}` (RemoveFilter).

        Замерено на R28.0: операция не идемпотентна - повторное удаление того
        же фильтра даёт 500 internal.error (не 404/204).

        :return: True при успехе (204)
        """
        return self.__no_content(
            "delete_incident_filter",
            f"{self.__api_filters}/{filter_id}",
            method="DELETE",
        )

    def create_incident_filter_folder(
        self, name: str, parent_id: str
    ) -> dict[str, Any]:
        """Создать папку фильтров инцидентов.

        Контракт: `IncidentFilterFolder.yaml` `POST /api/v1/incidents/folders`
        (CreateFilterFolder).

        Замерено на R28.0: создавать папки можно только внутри пользовательской
        ветки (guuid «Пользовательские фильтры» -
        `00000000-0000-0000-0000-000000000007`); в системные папки - 403.

        :param name: Название папки
        :param parent_id: UUID родительской папки
        :return: `{id}` - UUID созданной папки
        :raises requests.RequestException: 403 при отсутствии прав
        """
        response: dict[str, Any] = self.__request(
            "create_incident_filter_folder",
            self.__api_filter_folders,
            method="POST",
            json_body={"name": name, "parentId": parent_id},
        )

        self.log.info(
            f"status=success, action=create_incident_filter_folder, "
            f'msg="Incident filter folder created", '
            f"hostname={self.__core_hostname!r}, folder_id={response.get('id')!r}"
        )
        return response

    def get_incident_filter_folder(self, folder_id: str) -> dict[str, Any]:
        """Получить информацию о папке фильтров.

        Контракт: `IncidentFilterFolder.yaml`
        `GET /api/v1/incidents/folders/{folderId}` (GetFolderInfo).

        Замерено на R28.0: несуществующая (или уже удалённая) папка -
        500 internal.error, а не 404.

        :return: `FilterFolderShortInfo`
        """
        response: dict[str, Any] = self.__request(
            "get_incident_filter_folder", f"{self.__api_filter_folders}/{folder_id}"
        )

        self.log.info(
            f"status=success, action=get_incident_filter_folder, "
            f'msg="Got incident filter folder", '
            f"hostname={self.__core_hostname!r}, folder_id={folder_id!r}"
        )
        return response

    def update_incident_filter_folder(
        self, folder_id: str, name: str, parent_id: str
    ) -> bool:
        """Обновить папку фильтров инцидентов.

        Контракт: `IncidentFilterFolder.yaml`
        `PUT /api/v1/incidents/folders/{folderId}` (UpdateFolder).

        Замерено на R28.0: 403 forbidden.error для системных папок,
        400 `Папка не найдена` для несуществующих.

        :return: True при успехе (204)
        """
        return self.__no_content(
            "update_incident_filter_folder",
            f"{self.__api_filter_folders}/{folder_id}",
            method="PUT",
            json_body={"name": name, "parentId": parent_id},
        )

    def delete_incident_filter_folder(self, folder_id: str) -> bool:
        """Удалить папку фильтров инцидентов.

        Контракт: `IncidentFilterFolder.yaml`
        `DELETE /api/v1/incidents/folders/{folderId}` (RemoveFolder).

        Замерено на R28.0: удаление каскадно снимает вложенные папки и
        фильтры - удалять содержимое по отдельности не требуется.
        Несуществующая папка - 400 `Папка не найдена`.

        Внимание: read-model удалённых вместе с папкой фильтров устаревает -
        :meth:`get_incident_filter` ещё отвечает 200. Проверять факт удаления
        нужно по:meth:`get_incident_filters_hierarchy`.

        :return: True при успехе (204)
        """
        return self.__no_content(
            "delete_incident_filter_folder",
            f"{self.__api_filter_folders}/{folder_id}",
            method="DELETE",
        )

    def get_incident_filter_folders_in_folder(
        self, folder_id: str
    ) -> list[dict[str, Any]]:
        """Получить вложенные папки фильтров.

        Контракт: `IncidentFilterFolder.yaml`
        `GET /api/v1/incidents/folders/{folderId}/folders`
        (GetFoldersInFolder).

        :return: Список `FilterFolderShortInfo`
        """
        response: list[dict[str, Any]] = self.__request(
            "get_incident_filter_folders_in_folder",
            f"{self.__api_filter_folders}/{folder_id}/folders",
        )

        self.log.info(
            f"status=success, action=get_incident_filter_folders_in_folder, "
            f'msg="Got incident filter folders", '
            f"hostname={self.__core_hostname!r}, "
            f"folder_id={folder_id!r}, folders={len(response)!r}"
        )
        return response

    def get_incident_filters_in_folder(self, folder_id: str) -> list[dict[str, Any]]:
        """Получить фильтры, лежащие в папке.

        Контракт: `IncidentFilterFolder.yaml`
        `GET /api/v1/incidents/folders/{folderId}/filters`
        (GetFiltersInFolder).

        :return: Список `FilterShortInfo`
        """
        response: list[dict[str, Any]] = self.__request(
            "get_incident_filters_in_folder",
            f"{self.__api_filter_folders}/{folder_id}/filters",
        )

        self.log.info(
            f"status=success, action=get_incident_filters_in_folder, "
            f'msg="Got incident filters in folder", '
            f"hostname={self.__core_hostname!r}, "
            f"folder_id={folder_id!r}, filters={len(response)!r}"
        )
        return response

    # ------------------------------------------------------------------ #
    # Write: инциденты (Incidents.yaml)
    # ------------------------------------------------------------------ #

    @staticmethod
    def __entities(
        groups: list[str] | None = None,
        assets: list[str] | None = None,
        networks: list[str] | None = None,
        addresses: list[str] | None = None,
        others: list[str] | None = None,
    ) -> dict[str, Any]:
        """Собрать `EntitiesViewModel` (мишени/атакующие инцидента)."""
        return {
            "groups": groups or [],
            "assets": assets or [],
            "networks": networks or [],
            "addresses": addresses or [],
            "others": others or [],
        }

    def create_incident(
        self,
        name: str,
        incident_type: str,
        severity: str,
        detected: int | str,
        targets: dict[str, Any] | None = None,
        attackers: dict[str, Any] | None = None,
        description: str | None = None,
        assigned: str | None = None,
        influence: str | None = None,
        groups: list[str] | None = None,
        parameters: list[dict[str, Any]] | None = None,
        events: list[dict[str, Any]] | None = None,
        source: str = "User",
    ) -> dict[str, Any]:
        """Создать инцидент.

        Контракт: `Incidents.yaml` `POST /api/incidents` (CreateIncident).

        Проверено на R28.0: `type` сверяется со справочником типов инцидентов
        сервера (неизвестный - 400 `Incorrect incident type`), `source`
        обязателен и валиден (`Unknown`/`User`/`SiemScript`/`NetFor`/`Hobod`/
        `Import`), `detected` обязан быть в прошлом (400 `Incident detection
        time should be in the past`).

        :param name: Название инцидента
        :param incident_type: Тип инцидента (из карточек существующих
            инцидентов, например `CriticalVulnerabilityDetection`)
        :param severity: Критичность (`High`/`Medium`/`Low`)
        :param detected: Время обнаружения (Unix-время или ISO 8601)
        :param targets: `EntitiesViewModel` мишеней
        :param attackers: `EntitiesViewModel` атакующих
        :param description: Описание
        :param assigned: ID ответственного пользователя
        :param influence: Ущерб
        :param groups: UUID групп активов
        :param parameters: Дополнительные параметры `[{id, value}]`
        :param events: Связываемые события `[{id, timestamp}]`
        :param source: Источник инцидента
        :return: `{id, key}`
        :raises requests.RequestException: невалидный тип/источник/время (400)
        """
        body: dict[str, Any] = {
            "name": name,
            "type": incident_type,
            "severity": severity,
            "detected": self.__iso_time(detected),
            "targets": targets or self.__entities(),
            "attackers": attackers or self.__entities(),
            "source": source,
        }
        if description is not None:
            body["description"] = description
        if assigned is not None:
            body["assigned"] = assigned
        if influence is not None:
            body["influence"] = influence
        if groups is not None:
            body["groups"] = groups
        if parameters is not None:
            body["parameters"] = parameters
        if events is not None:
            body["events"] = events

        response: dict[str, Any] = self.__request(
            "create_incident", self.__api_incidents, method="POST", json_body=body
        )

        self.log.info(
            f"status=success, action=create_incident, "
            f'msg="Incident created", hostname={self.__core_hostname!r}, '
            f"incident_id={response.get('id')!r}, key={response.get('key')!r}"
        )
        return response

    def update_incident(
        self,
        incident_id: str,
        name: str,
        incident_type: str,
        severity: str,
        detected: int | str,
        targets: dict[str, Any] | None = None,
        attackers: dict[str, Any] | None = None,
        description: str | None = None,
        assigned: str | None = None,
        influence: str | None = None,
        groups: list[str] | None = None,
        parameters: list[dict[str, Any]] | None = None,
        source: str = "User",
    ) -> bool:
        """Обновить инцидент.

        Контракт: `Incidents.yaml` `PUT /api/incidents/{incidentId}`
        (UpdateIncident). Тело - `IncidentUpdateViewModel`, где обязательны все
        поля создания плюс `source`; частичное обновление не поддерживается.

        :return: True при успехе (204)
        """
        body: dict[str, Any] = {
            "name": name,
            "type": incident_type,
            "severity": severity,
            "detected": self.__iso_time(detected),
            "targets": targets or self.__entities(),
            "attackers": attackers or self.__entities(),
            "source": source,
        }
        if description is not None:
            body["description"] = description
        if assigned is not None:
            body["assigned"] = assigned
        if influence is not None:
            body["influence"] = influence
        if groups is not None:
            body["groups"] = groups
        if parameters is not None:
            body["parameters"] = parameters

        return self.__no_content(
            "update_incident",
            f"{self.__api_incidents}/{incident_id}",
            method="PUT",
            json_body=body,
        )

    def change_incident_status(
        self, incident_id: str, status: str, message: str = "", measures: str = ""
    ) -> bool:
        """Сменить статус инцидента переходом.

        Контракт: `Incidents.yaml` `PUT /api/incidents/{incidentId}/transitions`
        (ChangeIncidentStatus).

        Замерено на R28.0: допустимая цепочка `New -> Approved -> InProgress ->
        Resolved -> Closed` проходит; обратные переходы из `Closed` и
        неизвестный статус сервер не валидирует заранее, а отвечает
        `500 internal.error` (не 400).

        :param incident_id: ID инцидента
        :param status: Целевой статус (`Approved`/`InProgress`/`Resolved`/
            `Closed`)
        :param message: Комментарий перехода
        :param measures: Меры по инциденту
        :return: True при успехе (204)
        """
        return self.__no_content(
            "change_incident_status",
            f"{self.__api_incidents}/{incident_id}/transitions",
            method="PUT",
            json_body={"id": status, "message": message, "measures": measures},
        )

    def change_incident_measures(self, incident_id: str, measures: str) -> bool:
        """Редактировать меры по инциденту.

        Контракт: `Incidents.yaml` `PUT /api/incidents/{incidentId}/measures`
        (ChangeMeasures).

        :return: True при успехе (204)
        """
        return self.__no_content(
            "change_incident_measures",
            f"{self.__api_incidents}/{incident_id}/measures",
            method="PUT",
            json_body={"measures": measures},
        )

    def update_incident_groups(self, incident_id: str, group_ids: list[str]) -> bool:
        """Обновить группы активов инцидента.

        Контракт: `Incidents.yaml` `PUT /api/incidents/{incidentId}/groups`
        (UpdateIncidentGroups). Тело - плоский список UUID групп.

        :return: True при успехе (204)
        """
        return self.__no_content(
            "update_incident_groups",
            f"{self.__api_incidents}/{incident_id}/groups",
            method="PUT",
            json_body=group_ids,
        )

    def link_incident_events(
        self, incident_key: str, events: list[dict[str, Any]]
    ) -> bool:
        """Связать события с инцидентом.

        Контракт: `Incidents.yaml` `POST /api/incidents/events` (LinkEvents).
        События задаются `EventAddDto` - `{id, timestamp}`.

        :param incident_key: Key инцидента (INC-<Number>), не ID
        :param events: События для привязки
        :return: True при успехе (204)
        """
        return self.__no_content(
            "link_incident_events",
            self.__api_incident_events_link,
            method="POST",
            json_body={"incidentKey": incident_key, "events": events},
        )

    def unlink_incident_event(self, incident_id: str, event_id: str) -> bool:
        """Отвязать событие от инцидента.

        Контракт: `Incidents.yaml` `DELETE
        /api/incidents/{incidentId}/events/{eventId}` (UnlinkEvent).

        Замерено на R28.0: отвязка несвязанного события - 400 (операция не
        идемпотентна).

        :return: True при успехе (204)
        """
        return self.__no_content(
            "unlink_incident_event",
            f"{self.__api_incidents}/{incident_id}/events/{event_id}",
            method="DELETE",
        )

    def unlink_incident_asset(
        self, incident_id: str, asset_type: str, element_type: str, element_id: str
    ) -> bool:
        """Отвязать актив (группу/сеть/адрес/сущность) от инцидента.

        Контракт: `Incidents.yaml` `DELETE
        /api/incidents/{incidentId}/assets/{assetType}/{elementType}/{id}`
        (UnlinkGeneralAsset).

        Замерено на R28.0: неизвестный элемент - 500 internal.error (не 404).

        :param incident_id: ID инцидента
        :param asset_type: `Targets` | `Attackers`
        :param element_type: `Group`/`Asset`/`Network`/`Address`/`Other`
        :param element_id: ID (или адрес) элемента
        :return: True при успехе (204)
        """
        self.__check_enum(asset_type, self.ASSET_TYPES, "assetType")
        self.__check_enum(element_type, self.ELEMENT_TYPES, "elementType")

        return self.__no_content(
            "unlink_incident_asset",
            f"{self.__api_incidents}/{incident_id}/assets/"
            f"{asset_type}/{element_type}/{element_id}",
            method="DELETE",
        )

    def add_incident_task(
        self,
        incident_id: str,
        name: str,
        task_type: str,
        description: str | None = None,
        assigned: str | None = None,
        estimated_time: int | str | None = None,
    ) -> dict[str, Any]:
        """Создать задачу инцидента.

        Контракт: `Incidents.yaml` `POST /api/issues` (AddTask).

        Замерено на R28.0: контракт объявляет обязательным только `incidentId`,
        но без `name` и `type` сервер отвечает 500 internal.error, поэтому оба
        вынесены в обязательные параметры. Неизвестный инцидент - тоже 500.

        :param incident_id: ID инцидента
        :param name: Название задачи
        :param task_type: Тип задачи (`Investigation`/`EvidenceGathering`/
            `Recovery`)
        :param description: Описание
        :param assigned: ID исполнителя
        :param estimated_time: Срок (Unix-время или ISO 8601)
        :return: `TaskDto` - `{id, status, name, assigned, estimatedTime}`
        """
        body: dict[str, Any] = {
            "incidentId": incident_id,
            "name": name,
            "type": task_type,
        }
        if description is not None:
            body["description"] = description
        if assigned is not None:
            body["assigned"] = assigned
        if estimated_time is not None:
            body["estimatedTime"] = self.__iso_time(estimated_time)

        response: dict[str, Any] = self.__request(
            "add_incident_task", self.__api_issues_write, method="POST", json_body=body
        )

        self.log.info(
            f"status=success, action=add_incident_task, "
            f'msg="Incident task created", hostname={self.__core_hostname!r}, '
            f"incident_id={incident_id!r}, task_id={response.get('id')!r}"
        )
        return response

    def update_incident_task(
        self,
        issue_id: str,
        name: str | None = None,
        task_type: str | None = None,
        description: str | None = None,
        assigned: str | None = None,
        estimated_time: int | str | None = None,
        status: str | None = None,
    ) -> bool:
        """Обновить задачу инцидента.

        Контракт: `Incidents.yaml` `PUT /api/issues/{issueId}` (UpdateTask).

        Замерено на R28.0: сервер не принимает частичное тело - `name`, `type`,
        `status` и `incidentId` обязательны, иначе 500 internal.error. Поэтому
        реализовано read-modify-write: текущее состояние задачи читается через
        read-model (:meth:`get_incident_task_by_id`), перезаписываются только
        переданные поля. `estimatedTime`/`description` при отсутствии в
        аргументах сохраняются как есть.

        :param issue_id: ID задачи
        :param name: Название задачи
        :param task_type: Тип задачи (`Investigation`/`EvidenceGathering`/
            `Recovery`)
        :param description: Описание
        :param assigned: ID исполнителя
        :param estimated_time: Срок (Unix-время или ISO 8601)
        :param status: Новый статус задачи (`New`/`Assigned`/`InProgress`/
            `Closed`)
        :return: True при успехе (204)
        :raises requests.RequestException: 500 при неизвестном ID задачи,
            недопустимом статусе или чужом инциденте
        """
        current = self.get_incident_task_by_id(issue_id)
        incident_id = str(
            (current.get("innerIncident") or {}).get("incidentId")
            or current.get("incidentId")
            or ""
        )
        body: dict[str, Any] = {
            "incidentId": incident_id,
            "name": name if name is not None else current.get("name"),
            "type": task_type if task_type is not None else current.get("type"),
            "status": status if status is not None else current.get("status"),
            "description": description
            if description is not None
            else current.get("description"),
        }
        if estimated_time is not None:
            body["estimatedTime"] = self.__iso_time(estimated_time)
        elif current.get("estimatedTime") is not None:
            body["estimatedTime"] = current["estimatedTime"]
        if assigned is not None:
            body["assigned"] = assigned

        return self.__no_content(
            "update_incident_task",
            f"{self.__api_issues_write}/{issue_id}",
            method="PUT",
            json_body=body,
        )

    def delete_incidents(self, incident_ids: list[str]) -> bool:
        """Удалить инциденты по списку ID.

        Контракт: `Incidents.yaml` `POST /api/v2/incidents/delete_by_ids`
        (DeleteIncidents). Замерено на R28.0: неизвестный ID сервер принимает
        молча (204).

        :param incident_ids: ID инцидентов
        :return: True при успехе (204)
        :raises ValueError: пустой список
        """
        if not incident_ids:
            raise ValueError("incident_ids must not be empty")

        return self.__no_content(
            "delete_incidents",
            f"{self.__api_incidents_v2}/delete_by_ids",
            method="POST",
            json_body={"incidents": incident_ids},
        )

    def delete_incidents_by_filter(
        self,
        time_from: int | str,
        filter_body: dict[str, Any],
        time_type: str = "Creation",
        time_to: int | str | None = None,
        exclude_incidents: list[str] | None = None,
        query_ids: list[str] | None = None,
        groups_filter: dict[str, Any] | None = None,
    ) -> bool:
        """Удалить инциденты по фильтру.

        Контракт: `Incidents.yaml` `POST /api/v2/incidents/delete_by_filter`
        (DeleteIncidentsByFilter). Операция необратима и затрагивает рабочие
        инциденты - применять только с точно ограниченным `filter_body`.

        Замерено на R28.0: выборка по `queryIds` обязательна (400
        `Property FilterTimeType/QueryIds is required`); если фильтр не
        совпадает ни с одним инцидентом, сервер отвечает 400
        `Во время удаления инцидентов произошла ошибка` - пустая выборка
        успехом не считается.

        :param time_from: Начало выборки (Unix-время или ISO 8601)
        :param filter_body: `IncidentFilterViewModel` - `{select, where, orderby}`
        :param time_type: `FilterTimeType` (по умолчанию `Creation`)
        :param time_to: Конец выборки
        :param exclude_incidents: ID, которые удалять не надо
        :param query_ids: Сохранённые запросы
        :param groups_filter: `BaseGroupsFilter` (по умолчанию `no_filter`)
        :return: True при успехе (204)
        :raises requests.RequestException: 400 при пустой выборке
        """
        body: dict[str, Any] = {
            "timeFrom": self.__iso_time(time_from),
            "filterTimeType": time_type,
            "filter": filter_body,
            "groups": groups_filter or {"filterType": "no_filter"},
            "queryIds": query_ids or ["all_incidents"],
        }
        if time_to is not None:
            body["timeTo"] = self.__iso_time(time_to)
        if exclude_incidents is not None:
            body["excludeIncidents"] = exclude_incidents

        return self.__no_content(
            "delete_incidents_by_filter",
            f"{self.__api_incidents_v2}/delete_by_filter",
            method="POST",
            json_body=body,
        )

    # ------------------------------------------------------------------ #
    # Экспорт (IncidentsExport.yaml)
    # ------------------------------------------------------------------ #

    def export_incidents_by_ids(self, incident_ids: list[str]) -> dict[str, Any]:
        """Экспортировать инциденты по списку ID.

        Контракт: `IncidentsExport.yaml` `POST
        /api/v1/incidents/export/export_by_ids` (ExportByIds). Сервер отдаёт
        файл выгрузки (JSON `named-file`).

        :param incident_ids: ID инцидентов
        :return: Разобранный файл выгрузки
        """
        response: dict[str, Any] = self.__request(
            "export_incidents_by_ids",
            f"{self.__api_export}/export_by_ids",
            method="POST",
            json_body={"incidents": incident_ids},
            timeout_up=self.settings.connection_timeout_x,
        )

        self.log.info(
            f"status=success, action=export_incidents_by_ids, "
            f'msg="Incidents exported", hostname={self.__core_hostname!r}, '
            f"requested={len(incident_ids)!r}"
        )
        return response

    def export_incidents_by_filter(
        self,
        time_from: int | str,
        filter_body: dict[str, Any],
        time_type: str = "Creation",
        time_to: int | str | None = None,
        query_ids: list[str] | None = None,
        groups_filter: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Экспортировать инциденты по фильтру.

        Контракт: `IncidentsExport.yaml` `POST
        /api/v1/incidents/export/export_by_filter` (ExportByFilter).

        :return: Разобранный файл выгрузки
        """
        body: dict[str, Any] = {
            "timeFrom": self.__iso_time(time_from),
            "filterTimeType": time_type,
            "filter": filter_body,
            "groups": groups_filter or {"filterType": "no_filter"},
            "queryIds": query_ids or ["all_incidents"],
        }
        if time_to is not None:
            body["timeTo"] = self.__iso_time(time_to)

        response: dict[str, Any] = self.__request(
            "export_incidents_by_filter",
            f"{self.__api_export}/export_by_filter",
            method="POST",
            json_body=body,
            timeout_up=self.settings.connection_timeout_x,
        )

        self.log.info(
            f"status=success, action=export_incidents_by_filter, "
            f'msg="Incidents exported by filter", '
            f"hostname={self.__core_hostname!r}"
        )
        return response

    def close(self) -> None:
        if self.__core_session is not None:
            self.__core_session.close()
