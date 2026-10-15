from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any, NoReturn
from urllib.parse import unquote

import requests

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

    MP SIEM 27.x+: контракт ``Monitoring.yaml`` (``/api/events_monitoring/v3``)
    - ``POST /assets`` и ``POST /forwarders``: фильтр (время, группы, активы,
    форвардеры) передаётся в теле запроса, ``stateFilter``/``limit``/``offset``
    - в query; ответ - ``{totalItems, items}``.
    - ``GET /{assets,forwarders}/stat``, ``GET /unidentified/{sources,forwarders}``,
    графики EPS (``.../eps/graph``), средний EPS (``/sources/eps/average``) и
    экспорт в CSV (``.../export``, ``.../export_specified``).

    MP SIEM 26.x: ``GET /api/events_monitoring/v2/{sources,forwarders}`` с
    ``timeFrom``/``timeTo``/``controlState`` в query и плоским списком в ответе
    (в контрактах 27.x v2 отсутствует). Остальные методы (stat, unidentified,
    графики, средний EPS, экспорт) описаны только для v3 и на 26.x недоступны.
    """

    __api_sources_v2 = "/api/events_monitoring/v2/sources"
    __api_forwarders_v2 = "/api/events_monitoring/v2/forwarders"
    __api_v3 = "/api/events_monitoring/v3"
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
    # assetSourceType для /unidentified/{id}/eps/graph (Monitoring.yaml)
    ASSET_SOURCE_TYPES = ("Source", "Forwarder")

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
        self.__check_enum_value(state_filter, self.ASSET_STATE_FILTERS)

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
        self.__check_enum_value(state_filter, self.FORWARDER_STATE_FILTERS)

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
    # Статистика (v3, Monitoring.yaml GetAssetsStat / GetForwardersStat)
    # ------------------------------------------------------------------ #

    def get_assets_statistics(
        self,
        begin: int,
        end: int | None = None,
        group_ids: list[str] | None = None,
        recursive: bool = True,
        asset_ids: list[str] | None = None,
        forwarder_ids: list[str] | None = None,
    ) -> dict[str, Any]:
        """Статистика по активам (``GET /api/events_monitoring/v3/assets/stat``).

        Счётчики в ответе: ``withoutRules``, ``withRules``, ``activityAlerted``,
        ``flowAlerted``, ``delayAlerted``, ``unidentifiedSources``.

        :param begin: timestamp начала диапазона (UTC); параметр обязателен для
            сервера (замерено на R28.0)
        :param end: timestamp конца диапазона
        :param group_ids: фильтр по группам активов (UUID)
        :param recursive: рекурсивно ли учитывать вложенные группы
        :param asset_ids: фильтр по активам (UUID)
        :param forwarder_ids: фильтр по форвардерам (UUID)
        :return: ``AssetsStatisticsData``
        """
        params: dict[str, Any] = {"fromDateTime": self.__iso_time(begin)}
        if end is not None:
            params["toDateTime"] = self.__iso_time(end)
        self.__add_array_params(
            params,
            groupIds=group_ids,
            assetIds=asset_ids,
            forwarderIds=forwarder_ids,
        )
        if group_ids is not None:
            params["recursive"] = recursive

        return self.__get_json_dict(
            "get_assets_statistics", f"{self.__api_v3}/assets/stat", params
        )

    def get_forwarders_statistics(
        self,
        begin: int,
        end: int | None = None,
        group_ids: list[str] | None = None,
        recursive: bool = True,
        asset_ids: list[str] | None = None,
    ) -> dict[str, Any]:
        """Статистика по форвардерам (``GET .../v3/forwarders/stat``).

        Счётчики в ответе: ``withoutRules``, ``withRules``, ``noEventsAlerted``,
        ``delayAlerted``, ``unidentifiedForwarders``.

        :param begin: timestamp начала диапазона (UTC)
        :param end: timestamp конца диапазона
        :param group_ids: фильтр по группам активов (UUID)
        :param recursive: рекурсивно ли учитывать вложенные группы
        :param asset_ids: фильтр по активам (UUID)
        :return: ``ForwardersStatisticsData``
        """
        params: dict[str, Any] = {"fromDateTime": self.__iso_time(begin)}
        if end is not None:
            params["toDateTime"] = self.__iso_time(end)
        self.__add_array_params(params, groupIds=group_ids, assetIds=asset_ids)
        if group_ids is not None:
            params["recursive"] = recursive

        return self.__get_json_dict(
            "get_forwarders_statistics", f"{self.__api_v3}/forwarders/stat", params
        )

    # ------------------------------------------------------------------ #
    # Неидентифицированные источники и форвардеры
    # ------------------------------------------------------------------ #

    def get_unidentified_sources(
        self, begin: int, end: int | None = None
    ) -> dict[str, Any]:
        """Неидентифицированные источники (``GET .../v3/unidentified/sources``).

        :param begin: timestamp начала диапазона (UTC)
        :param end: timestamp конца диапазона
        :return: ``UnidentifiedItems`` - ``{items, totalItems}``, где элемент
            ``items`` содержит ``source`` (id/vendor/title/subsystem),
            ``lastEventDateTime``, ``eps``, ``delay`` (ISO 8601 duration)
        """
        params: dict[str, Any] = {"fromDateTime": self.__iso_time(begin)}
        if end is not None:
            params["toDateTime"] = self.__iso_time(end)

        return self.__get_json_dict(
            "get_unidentified_sources", f"{self.__api_v3}/unidentified/sources", params
        )

    def get_unidentified_forwarders(
        self, begin: int, end: int | None = None
    ) -> dict[str, Any]:
        """Неидентифицированные форвардеры (``GET .../v3/unidentified/forwarders``).

        :param begin: timestamp начала диапазона (UTC)
        :param end: timestamp конца диапазона
        :return: ``UnidentifiedItems`` - см. :meth:`get_unidentified_sources`
        """
        params: dict[str, Any] = {"fromDateTime": self.__iso_time(begin)}
        if end is not None:
            params["toDateTime"] = self.__iso_time(end)

        return self.__get_json_dict(
            "get_unidentified_forwarders",
            f"{self.__api_v3}/unidentified/forwarders",
            params,
        )

    # ------------------------------------------------------------------ #
    # Графики EPS и средний EPS
    # ------------------------------------------------------------------ #

    def get_asset_source_eps_graph(
        self,
        asset_id: str,
        source_description_id: str,
        begin: int,
        end: int,
        granularity: str = "PT5M",
    ) -> list[dict[str, Any]]:
        """График EPS источника на активе (``GET .../v3/assets/{assetId}/{sourceDescriptionId}/eps/graph``).

        :param asset_id: UUID актива
        :param source_description_id: идентификатор SourceDescription
        :param begin: timestamp начала диапазона (UTC)
        :param end: timestamp конца диапазона
        :param granularity: шаг графика в ISO 8601 duration (``PT1M``, ``PT5M``,
            ``PT1H``); сервер не принимает ``HH:mm``
        :return: ``GraphData`` - список ``{dateTime, value}``
        """
        params = self.__graph_params(begin, end, granularity)
        return self.__get_json_list(
            "get_asset_source_eps_graph",
            f"{self.__api_v3}/assets/{asset_id}/{source_description_id}/eps/graph",
            params,
        )

    def get_forwarder_eps_graph(
        self,
        forwarder_id: str,
        begin: int,
        end: int,
        granularity: str = "PT5M",
    ) -> list[dict[str, Any]]:
        """График EPS форвардера (``GET .../v3/forwarders/{id}/eps/graph``).

        :param forwarder_id: UUID форвардера (в v3 форвардер - это актив,
            см. :meth:`get_forwarders_list`)
        :param begin: timestamp начала диапазона (UTC)
        :param end: timestamp конца диапазона
        :param granularity: шаг графика в ISO 8601 duration
        :return: ``GraphData`` - список ``{dateTime, value}``
        """
        params = self.__graph_params(begin, end, granularity)
        return self.__get_json_list(
            "get_forwarder_eps_graph",
            f"{self.__api_v3}/forwarders/{forwarder_id}/eps/graph",
            params,
        )

    def get_unidentified_source_eps_graph(
        self,
        source_description_id: str,
        source_type: str,
        begin_minutes: int,
        end_minutes: int,
        granularity: str = "PT5M",
    ) -> list[dict[str, Any]]:
        """График EPS неидентифицированного источника (``GET .../v3/unidentified/{sourceDescriptionId}/eps/graph``).

        В отличие от остальных графиков, сервер ждёт время суток относительно
        00:00 в формате ``HH:mm`` (замерено на R28.0: ISO duration отбивается с
        ``validation.error``). Точки в ответе сервер привязывает к текущим
        суткам в UTC.

        :param source_description_id: идентификатор SourceDescription
        :param source_type: ``Source`` или ``Forwarder`` (``AssetSourceType``)
        :param begin_minutes: левая граница, минут от 00:00 UTC (0..1439)
        :param end_minutes: правая граница, минут от 00:00 UTC (0..1439)
        :param granularity: шаг графика в ISO 8601 duration
        :return: ``GraphData`` - список ``{dateTime, value}``
        """
        self.__check_enum_value(source_type, self.ASSET_SOURCE_TYPES, "assetSourceType")
        params: dict[str, Any] = {
            "assetSourceType": source_type,
            "granularity": granularity,
            "fromDateTime": self.__minutes_to_hhmm(begin_minutes),
            "toDateTime": self.__minutes_to_hhmm(end_minutes),
        }

        return self.__get_json_list(
            "get_unidentified_source_eps_graph",
            f"{self.__api_v3}/unidentified/{source_description_id}/eps/graph",
            params,
        )

    def get_average_eps(
        self,
        begin_minutes: int,
        end_minutes: int,
        source_description_id: str | None = None,
    ) -> float:
        """Средний EPS (``GET .../v3/sources/eps/average``).

        Границы передаются как ISO 8601 duration относительно 00:00 UTC; сервер
        принимает значения строго меньше 24 часов (``PT24H`` отбивается с
        ``BadRequest``, замерено на R28.0). ``HH:mm``, который обещан в
        контракте, сервером не принимается.

        :param begin_minutes: левая граница, минут от 00:00 UTC (0..1439)
        :param end_minutes: правая граница, минут от 00:00 UTC (0..1439)
        :param source_description_id: идентификатор SourceDescription; None -
            средний EPS по всем источникам
        :return: ``averageEps``
        """
        params: dict[str, Any] = {
            "fromTime": self.__minutes_to_iso_duration(begin_minutes),
            "toTime": self.__minutes_to_iso_duration(end_minutes),
        }
        if source_description_id is not None:
            params["sourceDescriptionId"] = source_description_id

        response = self.__get_json_dict(
            "get_average_eps", f"{self.__api_v3}/sources/eps/average", params
        )

        return float(response.get("averageEps", 0.0))

    # ------------------------------------------------------------------ #
    # Экспорт в CSV
    # ------------------------------------------------------------------ #

    def export_assets(
        self,
        begin: int,
        end: int | None = None,
        forwarder_id: str | None = None,
        group_ids: list[str] | None = None,
        state_filter: str = "all",
    ) -> str:
        """Экспорт активов с источниками в CSV (``POST .../v3/assets/export``).

        :param begin: timestamp начала диапазона (UTC)
        :param end: timestamp конца диапазона
        :param forwarder_id: ID форвардера, активы которого надо выгрузить
        :param group_ids: фильтр по группам активов (UUID)
        :param state_filter: фильтр по состоянию, см. ``ASSET_STATE_FILTERS``
        :return: Текст CSV (разделитель ``;``, BOM снят)
        """
        self.__check_enum_value(state_filter, self.ASSET_STATE_FILTERS)
        body = self.__prepare_filter(begin, end, forwarder_id, group_ids)

        return self.__post_csv(
            "export_assets",
            f"{self.__api_v3}/assets/export",
            json=body,
            params={"stateFilter": state_filter},
        )

    def export_assets_specified(self, assets_with_sources: list[dict[str, Any]]) -> str:
        """Экспорт выбранных активов (``POST .../v3/assets/export_specified``).

        :param assets_with_sources: ``AssetsWithSourcesFilter`` - список
            ``{"assetId": <UUID>, "sourceDescriptionId": <id>}``; источники
            можно не указывать, тогда выгружается актив целиком
        :return: Текст CSV (разделитель ``;``, BOM снят)
        """
        return self.__post_csv(
            "export_assets_specified",
            f"{self.__api_v3}/assets/export_specified",
            json={"assetsWithSources": assets_with_sources},
        )

    def export_forwarders(
        self,
        begin: int,
        end: int | None = None,
        group_ids: list[str] | None = None,
        state_filter: str = "all",
    ) -> str:
        """Экспорт форвардеров в CSV (``POST .../v3/forwarders/export``).

        :param begin: timestamp начала диапазона (UTC)
        :param end: timestamp конца диапазона
        :param group_ids: фильтр по группам активов (UUID)
        :param state_filter: фильтр по состоянию, см.
            ``FORWARDER_STATE_FILTERS``
        :return: Текст CSV (разделитель ``;``, BOM снят)
        """
        self.__check_enum_value(state_filter, self.FORWARDER_STATE_FILTERS)
        body = self.__prepare_filter(begin, end, None, group_ids)

        return self.__post_csv(
            "export_forwarders",
            f"{self.__api_v3}/forwarders/export",
            json=body,
            params={"stateFilter": state_filter},
        )

    def export_forwarders_specified(
        self, assets_with_sources: list[dict[str, Any]]
    ) -> str:
        """Экспорт выбранных форвардеров (``POST .../v3/forwarders/export_specified``).

        :param assets_with_sources: список ``{"assetId": <UUID>}``; по контракту
            источники в этом фильтре заполнять не нужно
        :return: Текст CSV (разделитель ``;``, BOM снят); пустой список
            отдаётся как CSV с одной строкой заголовков
        """
        return self.__post_csv(
            "export_forwarders_specified",
            f"{self.__api_v3}/forwarders/export_specified",
            json={"assetsWithSources": assets_with_sources},
        )

    def export_unidentified_sources(
        self,
        begin: int,
        end: int | None = None,
        source_description_ids: list[str] | None = None,
    ) -> str:
        """Экспорт неидентифицированных источников (``GET .../v3/unidentified/sources/export``).

        :param begin: timestamp начала диапазона (UTC)
        :param end: timestamp конца диапазона
        :param source_description_ids: конкретные источники; None - все
        :return: Текст CSV (разделитель ``;``, BOM снят)
        """
        params: dict[str, Any] = {"fromDateTime": self.__iso_time(begin)}
        if end is not None:
            params["toDateTime"] = self.__iso_time(end)
        self.__add_array_params(params, sourceDescriptionIds=source_description_ids)

        return self.__get_csv(
            "export_unidentified_sources",
            f"{self.__api_v3}/unidentified/sources/export",
            params,
        )

    def export_unidentified_forwarders(
        self,
        begin: int,
        end: int | None = None,
        source_description_ids: list[str] | None = None,
    ) -> str:
        """Экспорт неидентифицированных форвардеров (``GET .../v3/unidentified/forwarders/export``).

        :param begin: timestamp начала диапазона (UTC)
        :param end: timestamp конца диапазона
        :param source_description_ids: конкретные источники; None - все
        :return: Текст CSV (разделитель ``;``, BOM снят)
        """
        params: dict[str, Any] = {"fromDateTime": self.__iso_time(begin)}
        if end is not None:
            params["toDateTime"] = self.__iso_time(end)
        self.__add_array_params(params, sourceDescriptionIds=source_description_ids)

        return self.__get_csv(
            "export_unidentified_forwarders",
            f"{self.__api_v3}/unidentified/forwarders/export",
            params,
        )

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

    def __url(self, api_path: str) -> str:
        return f"https://{self.__core_hostname}{api_path}"

    def __check_v3(self, action: str) -> None:
        """Методы Monitoring.yaml (stat, unidentified, графики, экспорт) живут
        только в ``/api/events_monitoring/v3``; на 26.x их нет."""
        if self.__core_release < (27, 0):
            raise NotImplementedError(
                f"{action} requires MP SIEM >= 27.0 "
                f"(core {self.__core_version}); on 26.x only "
                "get_sources_list / get_forwarders_list are available"
            )

    def __request(
        self,
        action: str,
        api_path: str,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> requests.Response:
        self.__check_v3(action)
        return exec_request(
            self.__core_session,
            self.__url(api_path),
            method=method,
            timeout=self.settings.connection_timeout,
            params=params,
            json=json_body,
        )

    def __get_json_dict(
        self, action: str, api_path: str, params: dict[str, Any]
    ) -> dict[str, Any]:
        payload = self.__get_json(action, api_path, params)
        if not isinstance(payload, dict):
            self.__wrong_structure_error(action)

        return payload

    def __get_json_list(
        self, action: str, api_path: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Ответ - список точек графика (``GraphData``)."""
        payload = self.__get_json(action, api_path, params)
        if not isinstance(payload, list):
            self.__wrong_structure_error(action)

        return payload

    def __get_json(self, action: str, api_path: str, params: dict[str, Any]) -> Any:
        start_time = get_metrics_start_time()
        response = self.__request(action, api_path, params=params)
        payload: Any = response.json()

        self.__log_result(action, start_time, 1)
        return payload

    def __post_csv(
        self,
        action: str,
        api_path: str,
        json: dict[str, Any],
        params: dict[str, Any] | None = None,
    ) -> str:
        return self.__csv_result(
            action,
            self.__request(
                action, api_path, method="POST", params=params, json_body=json
            ),
        )

    def __get_csv(self, action: str, api_path: str, params: dict[str, Any]) -> str:
        return self.__csv_result(
            action, self.__request(action, api_path, params=params)
        )

    def __csv_result(self, action: str, response: requests.Response) -> str:
        """Сервер отдаёт CSV с UTF-8 BOM и именем файла в Content-Disposition
        (замерено на R28.0). BOM снимается, имя файла - только в лог."""
        content = response.content.decode("utf-8-sig")
        self.log.info(
            f"status=success, action={action}, "
            f'msg="Export prepared", hostname={self.__core_hostname!r}, '
            f"filename={self.__content_disposition_filename(response)!r}, "
            f"chars={len(content)}"
        )
        return content

    @staticmethod
    def __content_disposition_filename(response: requests.Response) -> str | None:
        """Имя файла из Content-Disposition: ``filename*`` (RFC 5987) важнее."""
        disposition = response.headers.get("content-disposition")
        if not disposition:
            return None

        plain: str | None = None
        for part in disposition.split(";"):
            key, _, value = part.strip().partition("=")
            if not value:
                continue
            value = value.strip('"')
            name = key.lower()
            if name == "filename*":
                return unquote(value.rsplit("''", 1)[-1])
            if name == "filename":
                plain = value

        return plain

    def __wrong_structure_error(self, action: str) -> NoReturn:
        self.log.error(
            f"status=failed, action={action}, "
            f'msg="Core data request has wrong response structure", '
            f"hostname={self.__core_hostname!r}"
        )
        raise RuntimeError("Core data request has wrong response structure")

    @staticmethod
    def __add_array_params(params: dict[str, Any], **arrays: list[str] | None) -> None:
        """Массивы в query (requests раскрывает список в повтор параметра,
        сервер это принимает)."""
        for name, values in arrays.items():
            if values is not None:
                params[name] = values

    @staticmethod
    def __graph_params(begin: int, end: int, granularity: str) -> dict[str, Any]:
        return {
            "granularity": granularity,
            "fromDateTime": SourceMonitor.__iso_time(begin),
            "toDateTime": SourceMonitor.__iso_time(end),
        }

    @staticmethod
    def __minutes_to_hhmm(minutes: int) -> str:
        """Минуты от 00:00 -> ``HH:mm`` (формат TimeRange/unidentified graph)."""
        if not 0 <= minutes <= 1439:
            raise ValueError(
                f"Minutes of day must be in 0..1439 range, got {minutes!r}"
            )

        return f"{minutes // 60:02d}:{minutes % 60:02d}"

    @staticmethod
    def __minutes_to_iso_duration(minutes: int) -> str:
        """Минуты от 00:00 -> ISO 8601 duration (sources/eps/average).

        Сервер принимает значения строго меньше суток: ``PT24H`` отбивается с
        BadRequest, поэтому максимум - 1439 минут (23:59).
        """
        if not 0 <= minutes <= 1439:
            raise ValueError(
                f"Minutes of day must be in 0..1439 range, got {minutes!r}"
            )

        duration = timedelta(minutes=minutes)
        hours, remainder = divmod(duration.total_seconds(), 3600)
        minutes_part, seconds = divmod(int(remainder), 60)
        result = "PT"
        if hours:
            result += f"{int(hours)}H"
        if minutes_part:
            result += f"{minutes_part}M"
        if seconds:
            result += f"{seconds}S"

        return result if result != "PT" else "PT0M"

    @staticmethod
    def __iso_time(timestamp: int) -> str:
        """Unix timestamp -> ISO 8601 в UTC (миллисекунды, суффикс Z)."""
        dt = datetime.fromtimestamp(timestamp, tz=UTC)
        return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")

    @staticmethod
    def __check_enum_value(
        value: str, allowed: tuple[str, ...], param_name: str = "stateFilter"
    ) -> None:
        if value not in allowed:
            raise ValueError(
                f"Unknown {param_name} {value!r}; expected one of {list(allowed)!r}"
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
