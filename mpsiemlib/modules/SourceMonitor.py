from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Any

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


class SourceMonitor(ModuleInterface, LoggingHandler):
    """Source monitor module.

    MP SIEM 27.x: контракт ``Monitoring.yaml`` (``/api/events_monitoring/v3``)
    - ``POST /assets`` и ``POST /forwarders``: фильтр (время, группы, активы,
    форвардеры) передаётся в теле запроса, ``stateFilter``/``limit``/``offset``
    - в query; ответ - ``{totalItems, items}``.

    MP SIEM 26.x: ``GET /api/events_monitoring/v2/{sources,forwarders}`` с
    ``timeFrom``/``timeTo``/``controlState`` в query и плоским списком в ответе
    (в контрактах 27.x v2 отсутствует).
    """

    __api_sources_v2 = "/api/events_monitoring/v2/sources"
    __api_forwarders_v2 = "/api/events_monitoring/v2/forwarders"
    __api_assets_v3 = "/api/events_monitoring/v3/assets"
    __api_forwarders_v3 = "/api/events_monitoring/v3/forwarders"

    # stateFilter для /assets (Monitoring.yaml AssetsStateFilter)
    ASSET_STATE_FILTERS = (
        "all",
        "withoutRules",
        "withRules",
        "activityAlerted",
        "flowAlerted",
        "delayAlerted",
    )
    # stateFilter для /forwarders (Monitoring.yaml ForwardersStateFilter)
    FORWARDER_STATE_FILTERS = (
        "all",
        "withoutRules",
        "withRules",
        "noEventsAlerted",
        "delayAlerted",
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
        self.__core_version = auth.get_core_version()
        # Релиз ядра (MAJOR, MINOR): "27.6.40521" -> (27, 6). Сравнение
        # кортежем, а не float: "26.10" превращается в 26.1.
        version_parts = self.__core_version.split(".")
        self.__core_release: tuple[int, int] = (
            int(version_parts[0]),
            int(version_parts[1]),
        )
        self.log.debug(
            'status=success, action=prepare, msg="SourceMonitor Module init"'
        )

    # ------------------------------------------------------------------ #
    # Публичное API
    # ------------------------------------------------------------------ #

    def get_sources_list(
        self,
        begin: int,
        end: int | None = None,
        forwarder_id: str | None = None,
        group_ids: list[str] | None = None,
        state_filter: str = "all",
    ) -> Iterator[dict[str, Any]]:
        """Получить источники из мониторинга событий.

        27.x: ``POST /api/events_monitoring/v3/assets`` - выгружает активы с
        источниками на них; на выходе по строке на источник актива (поля
        актива и контроля идут вместе с ним).

        26.x: ``GET /api/events_monitoring/v2/sources``.

        :param begin: Timestamp начала диапазона (UTC)
        :param end: timestamp конца диапазона; если задан, то (end-begin)>=24h,
            иначе API вернет пустой результат
        :param forwarder_id: ID форвардера, источники которого надо вывести
        :param group_ids: фильтр по группам активов (UUID); None - без фильтра
        :param state_filter: фильтр по состоянию, см. ``ASSET_STATE_FILTERS``
        :return: Итератор по источникам
        """
        self.__check_state_filter(state_filter, self.ASSET_STATE_FILTERS)

        if self.__core_release >= (27, 0):
            yield from self.__iterate_assets_v3(
                begin, end, forwarder_id, group_ids, state_filter
            )
        else:
            yield from self.__iterate_sources_v2(begin, end, forwarder_id)

    def get_forwarders_list(
        self,
        begin: int,
        end: int | None = None,
        group_ids: list[str] | None = None,
        state_filter: str = "all",
    ) -> Iterator[dict[str, Any]]:
        """Получить форвардеры из мониторинга событий.

        27.x: ``POST /api/events_monitoring/v3/forwarders``.
        26.x: ``GET /api/events_monitoring/v2/forwarders``.

        :param begin: timestamp начала диапазона (UTC)
        :param end: timestamp конца диапазона; если задан, то (end-begin)>=24h,
            иначе API вернет пустой результат
        :param group_ids: фильтр по группам активов (UUID); None - без фильтра
        :param state_filter: фильтр по состоянию, см.
            ``FORWARDER_STATE_FILTERS``
        :return: Итератор по форвардерам
        """
        self.__check_state_filter(state_filter, self.FORWARDER_STATE_FILTERS)

        if self.__core_release >= (27, 0):
            yield from self.__iterate_forwarders_v3(begin, end, group_ids, state_filter)
        else:
            yield from self.__iterate_forwarders_v2(begin, end)

    def get_sources_by_forwarder(
        self,
        forwarder_id: str,
        begin: int,
        end: int | None = None,
        group_ids: list[str] | None = None,
        state_filter: str = "all",
    ) -> Iterator[dict[str, Any]]:
        """Получить все источники для форвардера.

        Обертка над :meth:`get_sources_list`.

        :param forwarder_id: ID форвардера
        :param begin: timestamp начала диапазона (UTC)
        :param end: timestamp конца диапазона
        :param group_ids: фильтр по группам активов (UUID)
        :param state_filter: фильтр по состоянию
        :return: Итератор по источникам
        """
        return self.get_sources_list(begin, end, forwarder_id, group_ids, state_filter)

    # ------------------------------------------------------------------ #
    # v3 (MP SIEM 27.x, Monitoring.yaml)
    # ------------------------------------------------------------------ #

    def __iterate_assets_v3(
        self,
        begin: int,
        end: int | None,
        forwarder_id: str | None,
        group_ids: list[str] | None,
        state_filter: str,
    ) -> Iterator[dict[str, Any]]:
        url = f"https://{self.__core_hostname}{self.__api_assets_v3}"
        body = self.__prepare_filter(begin, end, forwarder_id, group_ids)
        start_time = get_metrics_start_time()
        line_counter = 0

        for page in self.__iterate_pages_v3(url, body, state_filter, "assets_iterate"):
            for item in page:
                line_counter += 1
                yield from self.__map_asset_source_v3(item)

        self.__log_result("get_sources_list", start_time, line_counter)

    def __iterate_forwarders_v3(
        self,
        begin: int,
        end: int | None,
        group_ids: list[str] | None,
        state_filter: str,
    ) -> Iterator[dict[str, Any]]:
        url = f"https://{self.__core_hostname}{self.__api_forwarders_v3}"
        body = self.__prepare_filter(begin, end, None, group_ids)
        start_time = get_metrics_start_time()
        line_counter = 0

        for page in self.__iterate_pages_v3(
            url, body, state_filter, "forwarders_iterate"
        ):
            for item in page:
                line_counter += 1
                yield self.__map_forwarder_v3(item)

        self.__log_result("get_forwarders_list", start_time, line_counter)

    def __iterate_pages_v3(
        self, url: str, body: dict[str, Any], state_filter: str, action: str
    ) -> Iterator[list[dict[str, Any]]]:
        """Постраничная выгрузка v3: ``limit``/``offset`` в query, фильтр в
        теле; конец - по ``totalItems``."""
        limit = self.settings.source_monitor_batch_size
        offset = 0
        total: int | None = None

        while total is None or offset < total:
            params: dict[str, Any] = {
                "stateFilter": state_filter,
                "limit": limit,
                "offset": offset,
            }
            response: dict[str, Any] = exec_request(
                self.__core_session,
                url,
                method="POST",
                timeout=self.settings.connection_timeout,
                params=params,
                json=body,
            ).json()

            if isinstance(response, dict) and "items" in response:
                items = list(response.get("items") or [])
                total = int(response.get("totalItems") or 0)
                if not items:
                    break
                yield items
                offset += limit
                continue

            self.log.error(
                f"status=failed, action={action}, "
                f'msg="Core data request has wrong response structure", '
                f"hostname={self.__core_hostname!r}"
            )
            raise RuntimeError("Core data request has wrong response structure")

    @staticmethod
    def __prepare_filter(
        begin: int,
        end: int | None,
        forwarder_id: str | None,
        group_ids: list[str] | None,
    ) -> dict[str, Any]:
        """Тело запроса v3 (Monitoring.yaml Filter)."""
        body: dict[str, Any] = {"fromDateTime": SourceMonitor.__iso_time(begin)}
        if end is not None:
            body["toDateTime"] = SourceMonitor.__iso_time(end)
        if group_ids is not None:
            body["groupIds"] = group_ids
            body["recursive"] = True
        if forwarder_id is not None:
            body["forwarderIds"] = [forwarder_id]
        return body

    @staticmethod
    def __map_asset_source_v3(item: dict[str, Any]) -> Iterator[dict[str, Any]]:
        """AssetSource (asset + sources[]) -> строка на источник.

        Актив без источников отдается одной строкой с пустыми полями
        источника, иначе он исчезнет из выгрузки.
        """
        asset: dict[str, Any] = item.get("asset") or {}
        base: dict[str, Any] = {
            "asset_id": asset.get("assetId"),
            "asset_name": asset.get("name"),
            "asset_type": asset.get("assetType"),
            "asset_importance": asset.get("importance"),
            "activity_control": item.get("activityControl"),
            "delay_control": item.get("delayControl"),
        }
        sources = item.get("sources") or []
        if not sources:
            yield dict(
                base,
                source_id=None,
                source_vendor=None,
                source_title=None,
                source_subsystem=None,
                control_status=None,
                period_check_time=None,
                policy_rule=None,
                eps=None,
                events_count=None,
                deviation=None,
            )
            return

        for source_item in sources:
            source: dict[str, Any] = source_item.get("source") or {}
            control_data = SourceMonitor.__control_data(source_item)
            yield dict(
                base,
                source_id=source.get("id"),
                source_vendor=source.get("vendor"),
                source_title=source.get("title"),
                source_subsystem=source.get("subsystem"),
                control_status=source_item.get("state"),
                period_check_time=source_item.get("periodCheckDateTime"),
                policy_rule=(source_item.get("policyRule") or {}).get("name"),
                eps=control_data.get("eps"),
                events_count=control_data.get("eventsCount"),
                deviation=control_data.get("deviation"),
            )

    @staticmethod
    def __map_forwarder_v3(item: dict[str, Any]) -> dict[str, Any]:
        asset: dict[str, Any] = item.get("asset") or {}
        return {
            "asset_id": asset.get("assetId"),
            "asset_name": asset.get("name"),
            "asset_type": asset.get("assetType"),
            "asset_importance": asset.get("importance"),
            "activity_control": item.get("activityControl"),
            "delay_control": item.get("delayControl"),
            "eps": item.get("eps"),
            "last_event_time": item.get("lastEventDateTime"),
        }

    @staticmethod
    def __control_data(source_item: dict[str, Any]) -> dict[str, Any]:
        """sourceControlData разнотипен (eps / eventsCount / deviation)."""
        data = source_item.get("sourceControlData")
        return data if isinstance(data, dict) else {}

    # ------------------------------------------------------------------ #
    # v2 (MP SIEM 26.x)
    # ------------------------------------------------------------------ #

    def __iterate_sources_v2(
        self, begin: int, end: int | None, forwarder_id: str | None
    ) -> Iterator[dict[str, Any]]:
        url = f"https://{self.__core_hostname}{self.__api_sources_v2}"
        params = self.__prepare_params_v2(begin, end)
        if forwarder_id is not None:
            params["forwarderId"] = forwarder_id

        start_time = get_metrics_start_time()
        line_counter = 0
        for page in self.__iterate_pages_v2(url, params, "sources_iterate"):
            for item in page:
                line_counter += 1
                yield self.__map_source_v2(item)

        self.__log_result("get_sources_list", start_time, line_counter)

    def __iterate_forwarders_v2(
        self, begin: int, end: int | None
    ) -> Iterator[dict[str, Any]]:
        url = f"https://{self.__core_hostname}{self.__api_forwarders_v2}"
        params = self.__prepare_params_v2(begin, end)

        start_time = get_metrics_start_time()
        line_counter = 0
        for page in self.__iterate_pages_v2(url, params, "forwarders_iterate"):
            for item in page:
                line_counter += 1
                yield self.__map_source_v2(item)

        self.__log_result("get_forwarders_list", start_time, line_counter)

    def __iterate_pages_v2(
        self, url: str, params: dict[str, Any], action: str
    ) -> Iterator[list[dict[str, Any]]]:
        limit = self.settings.source_monitor_batch_size
        offset = 0

        while True:
            page_params = dict(params, offset=offset, limit=limit)
            response: list[dict[str, Any]] = exec_request(
                self.__core_session,
                url,
                method="GET",
                timeout=self.settings.connection_timeout,
                params=page_params,
            ).json()

            if isinstance(response, list):
                if not response:
                    break
                yield response
                if len(response) < limit:
                    break
                offset += limit
                continue

            self.log.error(
                f"status=failed, action={action}, "
                f'msg="Core data request has wrong response structure", '
                f"hostname={self.__core_hostname!r}"
            )
            raise RuntimeError("Core data request has wrong response structure")

    def __prepare_params_v2(self, begin: int, end: int | None) -> dict[str, Any]:
        params: dict[str, Any] = {
            "timeFrom": self.__iso_time(begin),
            "controlState": "all",
        }
        if end is not None:
            params["timeTo"] = self.__iso_time(end)
        return params

    @staticmethod
    def __map_source_v2(item: dict[str, Any]) -> dict[str, Any]:
        source: dict[str, Any] = item.get("source") or {}
        service: dict[str, Any] = item.get("service") or {}
        return {
            "id": source.get("id"),
            "control_status": source.get("controlStatus"),
            "control_time_status": source.get("timeControlStatus"),
            "control_delay_status": source.get("delayControlStatus"),
            "control_eps_status": source.get("epsControlStatus"),
            "asset_id": source.get("assetId"),
            "name": source.get("name"),
            "hostname": source.get("host"),
            "ip": source.get("ip"),
            "service_vendor": service.get("vendor"),
            "service_title": service.get("title"),
            "service_subsystem": service.get("subsystem"),
            "discovered": item.get("discoveredTime"),
            "seen": item.get("lastSeenTime"),
            "eps": item.get("eps"),
            "eps_diff": item.get("epsDiff"),
            "time_shift": item.get("timeShift"),  # minutes (+/-)
            "events_count": item.get("eventsCount"),
        }

    # ------------------------------------------------------------------ #
    # Общее
    # ------------------------------------------------------------------ #

    @staticmethod
    def __iso_time(timestamp: int) -> str:
        """Unix timestamp -> ISO 8601 в UTC (миллисекунды, суффикс Z)."""
        dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")

    @staticmethod
    def __check_state_filter(state_filter: str, allowed: tuple[str, ...]) -> None:
        if state_filter not in allowed:
            raise ValueError(
                f"Unknown stateFilter {state_filter!r}; "
                f"expected one of {list(allowed)!r}"
            )

    def __log_result(self, action: str, start_time: float, line_counter: int) -> None:
        took_time = get_metrics_took_time(start_time)
        self.log.info(
            f"status=success, action={action}, "
            f'msg="Query executed, response have been read", '
            f"hostname={self.__core_hostname!r}, lines={line_counter}"
        )
        self.log.info(
            f"hostname={self.__core_hostname!r}, metric={action}, "
            f"took={took_time:.4f}ms, objects={line_counter}"
        )

    def close(self) -> None:
        if self.__core_session is not None:
            self.__core_session.close()
