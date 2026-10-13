from collections.abc import Iterator
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


class EventsAPI(ModuleInterface, LoggingHandler):
    """Модуль получения информации о событиях через API UI.

    Контракты: ``EventsV2.Contracts.yaml`` (``/api/events/v2/*``),
    ``EventsV3.Contracts.yaml`` (``/api/events/v3/*``),
    ``Events.MetadataV2.Contracts.yaml``, ``IncidentsRead.yaml``.
    Все эндпоинты живы на ядрах 26.x–28.x (стенд R28.0), кроме
    ``GET /api/events/v2/events/{id}/raw``: на R28.0 он выведен из
    маршрутизации (404 на всех портах, студийной замены нет), поэтому
    ``get_event_raw`` ограничен ядрами <28.0 (см. ``__check_raw_supported``).
    """

    __api_events_metadata = "/api/events/v2/events_metadata"
    __api_events_taxonomy = "/api/events/v2/taxonomy"
    __api_events_settings = "/api/events/v2/events_settings"
    __api_events_v2 = "/api/events/v2/events"
    __api_events_v3 = "/api/events/v3/events"
    __api_events_aggregation_v3 = f"{__api_events_v3}/aggregation"
    __api_events_countminmax_v2 = "/api/events/v2/events_countminmax"
    __api_events_countminmax_v3 = "/api/events/v3/events_countminmax"
    __api_events_distinct_values = "/api/events/v2/events/count_distinct_field_values"
    __api_events_group_values = "/api/events/v2/events/group_values"
    __api_events_localization = "/api/events/v2/localization"
    __api_events_subevents_v2 = "/api/events/v2/subevents"
    __api_events_subevents_countminmax_v2 = "/api/events/v2/subevents_countminmax"
    __api_incidents = "/api/incidents"
    __api_events_aggregation = f"{__api_events_v2}/aggregation?offset=0"

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
        # Релиз ядра (MAJOR, MINOR): "28.0.45355" -> (28, 0). Сравнение
        # кортежем, а не float: "26.10" превращается в 26.1.
        version_parts = self.__core_version.split(".")
        self.__core_release: tuple[int, int] = (
            int(version_parts[0]),
            int(version_parts[1]),
        )
        self.log.debug('status=success, action=prepare, msg="EventsAPI Module init"')

    def __check_raw_supported(self, action: str) -> None:
        """Защитить raw-эндпоинт версией ядра.

        ``GET /api/events/v2/events/{id}/raw`` выведен из маршрутизации на
        R28.0 (404 на 443/8091/3334, в KB Studio замены нет). На ядрах 26.x
        и 27.x эндпоинт жив, поэтому метод сохраняется с версионной
        проверкой, а не удаляется.

        :param action: имя вызывающего метода для сообщения об ошибке
        :raises RuntimeError: на ядрах 28.0 и новее
        """
        if self.__core_release >= (28, 0):
            raise RuntimeError(
                f"SIEM version {self.__core_version} does not support raw event API "
                f"({action}); endpoint removed since R28.0"
            )

    def get_events_group_by_api_json(
        self, params: dict[str, Any], group_ids: str | None = None
    ) -> list[dict[str, Any]]:
        """Получить группировки событий в формате JSON.

        :param params: Параметры запроса
        :param group_ids: UUID групп активов
        :return: Строки распределения (rows)
        """
        self.log.debug(
            f"status=prepare, action=get_events_group_by_api_json, "
            f'msg="Try to aggregate events by query '
            f'{params.get("filter", {}).get("groupBy")!r}", '
            f"hostname={self.__core_hostname!r}"
        )

        if group_ids:
            api_url = f"{self.__api_events_aggregation}&groupIds={group_ids}"
        else:
            api_url = self.__api_events_aggregation
        url = f"https://{self.__core_hostname}{api_url}"

        start_time = get_metrics_start_time()
        response: dict[str, Any] | None = exec_request(
            self.__core_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
            json=params,
        ).json()
        took_time = get_metrics_took_time(start_time)

        if response is None or "rows" not in response:
            self.log.error(
                f"status=failed, action=get_events_group_by_api_json, "
                f'msg="Core data request return None or '
                f'has wrong response structure", hostname={self.__core_hostname!r}'
            )
            raise RuntimeError(
                "Core data request return None or has wrong response structure"
            )

        rows = cast("list[dict[str, Any]]", response["rows"])
        self.log.info(
            f"status=success, action=get_events_group_by_api_json, "
            f'msg="Query executed, response have been read", '
            f"hostname={self.__core_hostname!r}, lines={len(rows)!r}"
        )
        self.log.info(
            f"hostname={self.__core_hostname!r}, metric=get_events_group_by_api_json, "
            f"took={took_time:.4f} ms, objects={len(rows)!r}"
        )
        return rows

    def get_event_details(self, event_id: str, event_date: str) -> dict[str, Any]:
        """Получить событие (все заполненные поля) по его идентификатору и
        дате.

        Контракт ``EventDetailsResponseV2``: ``{errors, event, actions}``,
        где ``event`` - произвольный объект. На ядрах R28.0 ``event``
        заворачивает событие в paginated-обёртку ``{Token, TotalCount,
        Data: [...]}`` (изменение формы ответа ядра); метод распаковывает
        её и на обеих формах возвращает словарь события.

        Args:
            event_id : идентификатор события
            event_date : дата (ISO date-time)
        Returns:
            [type]: событие
        """
        api_url = f"{self.__api_events_v2}/{event_id}/normalized?time={event_date}"

        url = f"https://{self.__core_hostname}{api_url}"
        response: dict[str, Any] | None = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
        ).json()

        if response is None or "event" not in response:
            self.log.error(
                f'status=failed, action=get_event_details, msg="Core data request return None or '
                f'has wrong response structure", hostname={self.__core_hostname!r}'
            )
            raise RuntimeError(
                "Core data request return None or has wrong response structure"
            )
        event = cast("dict[str, Any]", response.get("event"))
        # R28.0: {Token, TotalCount, Data: [событие]} -> само событие
        data = event.get("Data")
        if isinstance(data, list) and data and isinstance(data[0], dict):
            return cast("dict[str, Any]", data[0])
        return event

    def get_events_metadata(self) -> list[dict[str, Any]]:
        """Получить список поддерживаемых полей таксономии событий."""

        url = f"https://{self.__core_hostname}{self.__api_events_metadata}"

        response: dict[str, Any] | None = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
        ).json()

        if response is None or "fields" not in response:
            self.log.error(
                f'status=failed, action=get_events_metadata, msg="Core data request return None or '
                f'has wrong response structure", hostname={self.__core_hostname!r}'
            )
            raise RuntimeError(
                "Core data request return None or has wrong response structure"
            )
        return cast("list[dict[str, Any]]", response.get("fields"))

    def get_events_grouped_by_fields(
        self,
        query_filter: str,
        group_by_fields: list[str],
        time_from: int,
        time_to: int,
    ) -> dict[str, int]:
        """Получить события по фильтру, сгруппированные по заданным полям.

        Args:
            query_filter : фильтр на языке PDQL
            group_by_fields: список полей для группировки
            time_from : начало диапазона поиска (Unix timestamp в секундах)
            time_to : конец диапазона поиска (Unix timestamp в секундах)
        Returns:
            [type]: словарь «значение группировки -> количество событий»
        """
        params = {
            "filter": {
                "select": ["time", "event_src.host", "text"],
                "where": query_filter,
                "orderBy": [{"field": "time", "sortOrder": "descending"}],
                "groupBy": group_by_fields,
                "aggregateBy": [{"function": "COUNT", "field": "*", "unique": False}],
                "distributeBy": [],
                "top": 10000,
                "aliases": {"groupBy": {}, "aggregateBy": {"COUNT": "Cnt"}},
                "searchType": None,
                "searchSources": None,
                "localSources": None,
                "groupByOrder": [{"field": "count", "sortOrder": "Descending"}],
                "showNullGroups": True,
            },
            "timeFrom": time_from,
            "timeTo": time_to,
        }
        api_url = self.__api_events_aggregation
        url = f"https://{self.__core_hostname}{api_url}"

        response: dict[str, Any] | None = exec_request(
            self.__core_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
            json=params,
        ).json()

        if response is None or "rows" not in response:
            self.log.error(
                f'status=failed, action=get_events_grouped_by_fields, msg="Core data request return None or '
                f'has wrong response structure", hostname={self.__core_hostname!r}'
            )
            raise RuntimeError(
                "Core data request return None or has wrong response structure"
            )

        rows = cast("list[dict[str, Any]]", response["rows"])
        return {
            " | ".join(str(s) for s in e["groups"]): int(e["values"][0]) for e in rows
        }

    def get_events_by_filter(
        self,
        pdql_filter: str,
        fields: list[str],
        time_from: int,
        time_to: int,
        limit: int,
        offset: int,
    ) -> list[dict[str, Any]]:
        """Получить события по фильтру.

        Args:
            pdql_filter : фильтр на языке PDQL
            fields : список запрашиваемых полей событий
            time_from : начало диапазона поиска (Unix timestamp в секундах)
            time_to : конец диапазона поиска (Unix timestamp в секундах)
            limit: число запрашиваемых событий, соответствующих фильтру
            offset: позиция, начиная с которой возвращать требуемое число событий, соответствующих фильтру
        Returns:
            [type]: массив событий
        """
        params = {
            "filter": {
                "select": fields,
                "where": f"{pdql_filter}",
                "orderBy": [{"field": "time", "sortOrder": "ascending"}],
                "groupBy": [],
                "aggregateBy": [],
                "distributeBy": [],
                "top": None,
                "aliases": {"groupBy": {}},
            },
            "groupValues": [],
            "timeFrom": time_from,
            "timeTo": time_to,
        }
        api_url = f"{self.__api_events_v2}?limit={limit}&offset={offset}"
        url = f"https://{self.__core_hostname}{api_url}"

        response: dict[str, Any] | None = exec_request(
            self.__core_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
            json=params,
        ).json()

        if response is None or "events" not in response:
            self.log.error(
                f'status=failed, action=get_events_by_filter, msg="Core data request return None or '
                f'has wrong response structure", hostname={self.__core_hostname!r}'
            )
            raise RuntimeError(
                "Core data request return None or has wrong response structure"
            )
        return cast("list[dict[str, Any]]", response.get("events"))

    # noinspection PyUnusedParameter
    def get_events_for_incident(
        self,
        fields: Any,
        incident_id: str,
        time_from: int,
        time_to: int,
        limit: int,
        offset: int,
    ) -> list[dict[str, Any]]:
        """Получить события, связанные с инцидентом.

        GET /api/incidents/{incidentId}/events?limit=...&offset=...
        возвращает привязанные к инциденту события списком
        [{id, date, description}, ...]. Эндпоинт не поддерживает
        fields/time window (параметры fields, time_from, time_to
        оставлены для совместимости сигнатуры и игнорируются;
        за выгрузку всех событий следят limit/offset).

        Args:
            fields : список полей (не используется)
            incident_id: идентификатор инцидента
            time_from : начало диапазона поиска (не используется)
            time_to : конец диапазона поиска (не используется)
            limit: число запрашиваемых событий, связанных с инцидентом
            offset: позиция, начиная с которой возвращать события
        Returns:
            [type]: массив событий
        """
        api_url = (
            f"{self.__api_incidents}/{incident_id}/events?limit={limit}&offset={offset}"
        )
        url = f"https://{self.__core_hostname}{api_url}"

        response: list[dict[str, Any]] | None = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
        ).json()

        if response is None or not isinstance(response, list):
            self.log.error(
                f'status=failed, action=get_events_for_incident, msg="Core data request return None or '
                f'has wrong response structure", '
                f"hostname={self.__core_hostname!r}"
            )
            raise RuntimeError(
                "Core data request return None or has wrong response structure"
            )
        return response

    def __get_events_by_filter_v3(
        self,
        query_filter: str,
        time_from: int,
        time_to: int,
        offset: int,
        limit: int,
        token: str | None = None,
    ) -> dict[str, Any]:
        """
        Получить события по фильтру
        Args:
            query_filter (str): фильтр на языке PDQL в виде одной строки ""
            time_from (int): начало диапазона поиска (Unix timestamp в секундах)
            time_to (int): конец диапазона поиска (Unix timestamp в секундах)
            offset: (int) позиция, начиная с которой возвращать требуемое число событий, соответсвующих фильтру
            limit: (int) число запрашиваемых событий, соответсвующих фильтру
            token: (str) токен запроса (ускоряет ответ при запросе новой пачки событий по предыдущему фильтру)
        Returns:
            [dict]: ответ API с массивом событий
        """
        params = {
            "filter": query_filter,
            "timeFrom": time_from,
            "timeTo": time_to,
        }
        api_url = f"{self.__api_events_v3}?offset={offset}&limit={limit}&noCount=true"
        url = f"https://{self.__core_hostname}{api_url}"
        if token is not None:
            url += f"&token={token}"

        response: dict[str, Any] | None = exec_request(
            self.__core_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
            json=params,
        ).json()

        if response is None or "events" not in response:
            self.log.error(
                f'status=failed, action=get_events_by_filter, msg="Core data request return None or '
                f'has wrong response structure", '
                f"hostname={self.__core_hostname!r}"
            )
            raise RuntimeError(
                "Core data request return None or has wrong response structure"
            )
        return response

    def get_events_by_filter_v3_page(
        self,
        query_filter: str,
        time_from: int,
        time_to: int,
        offset: int,
        limit: int,
        token: str | None = None,
    ) -> dict[str, Any]:
        """Получить одну страницу событий по фильтру (без цикловой выгрузки).

        В отличие от ``get_events_by_filter_v3``, который выгружает выборку
        целиком, возвращает сырой ответ API одной пачки: ``events`` и
        ``token``. Токен нужен для последующих страниц (ускоряет ответ) и для
        ``get_events_count_by_token`` (``noCount=true`` не считает события на
        сервере).

        :param query_filter: фильтр на языке PDQL в виде одной строки
        :param time_from: начало диапазона поиска (Unix timestamp в секундах)
        :param time_to: конец диапазона поиска (Unix timestamp в секундах)
        :param offset: позиция, начиная с которой возвращать события
        :param limit: число запрашиваемых событий
        :param token: токен предыдущей страницы (ускоряет ответ)
        :return: ответ API (``events``, ``token``, ...)
        """
        return self.__get_events_by_filter_v3(
            query_filter, time_from, time_to, offset, limit, token
        )

    def get_events_by_filter_v3(
        self, query_filter: str, time_from: int, time_to: int
    ) -> Iterator[dict[str, Any]]:
        """Получить события по фильтру
        Args:
            query_filter (str): фильтр на языке PDQL в виде одной строки
            time_from (int): начало диапазона поиска (Unix timestamp в секундах)
            time_to (int): конец диапазона поиска (Unix timestamp в секундах)
        Yields:
            Iterator[dict]: Итератор
        """
        self.log.debug(
            f'status=prepare, action=get_events_by_filter_v3, msg="Try to get events", '
            f"hostname={self.__core_hostname!r}, filter={query_filter!r}, "
            f"begin={time_from!r}, end={time_to!r}"
        )
        # Пачками выгружаем содержимое
        is_end = False
        offset = 0
        limit = self.settings.events_batch_size
        token: str | None = None
        line_counter = 0
        start_time = get_metrics_start_time()
        while not is_end:
            ret = self.__get_events_by_filter_v3(
                query_filter, time_from, time_to, offset, limit, token
            )
            events = ret.get("events") or []
            token = ret.get(
                "token"
            )  # последующие запросы с токеном должны быстрее, чем без него
            if len(events) < limit:
                is_end = True
            offset += limit
            for event in events:
                line_counter += 1
                yield event
        took_time = get_metrics_took_time(start_time)
        self.log.info(
            f"hostname={self.__core_hostname!r}, metric=get_events_by_filter_v3, "
            f"took={took_time:.4f} ms, objects={line_counter!r}"
        )

    def get_events_aggregation_v3(
        self,
        query_filter: str,
        time_from: int,
        time_to: int,
        limit: int = 10000,
    ) -> dict[str, Any]:
        """Агрегировать события по PDQL v3 (контракт ``GetAggregation`` v3).

        POST /api/events/v3/events/aggregation. Фильтр - строка PDQL с
        конвейерным ``group(...)``: агрегация задаётся внутри запроса, а не
        структурированным ``aggregateBy`` как в v2
        (``get_events_group_by_api_json``). Обязательны ``select(...)`` с
        временем, секция ``group(key: [...], agg: COUNT(*) as Alias)`` и
        ``timeFrom``. Пример PDQL::

            select(time, event_src.host) |
            group(key: [event_src.host], agg: COUNT(*) as Cnt) |
            sort(Cnt desc) | limit(10000)

        Ответ - ``{errors, hasMoreResults, columns, rows}``, где каждая строка
        ``{groups: [...], values: [...]}`` (см. ``AggregationResponse``).

        :param query_filter: PDQL-запрос с секцией ``group(...)`` (строка)
        :param time_from: начало диапазона агрегации (Unix timestamp в секундах)
        :param time_to: конец диапазона агрегации (Unix timestamp в секундах)
        :param limit: ограничение количества строк (query-параметр)
        :return: ответ API (``errors``, ``hasMoreResults``, ``columns``,
            ``rows``)
        """
        params = {
            "filter": query_filter,
            "timeFrom": time_from,
            "timeTo": time_to,
        }
        url = (
            f"https://{self.__core_hostname}"
            f"{self.__api_events_aggregation_v3}?limit={limit}"
        )

        response: dict[str, Any] | None = exec_request(
            self.__core_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
            json=params,
        ).json()

        if response is None or "rows" not in response:
            self.log.error(
                'status=failed, action=get_events_aggregation_v3, msg="Core data '
                f'request return None or has wrong response structure", '
                f"hostname={self.__core_hostname!r}"
            )
            raise RuntimeError(
                "Core data request return None or has wrong response structure"
            )
        return response

    # ------------------------------------------------------------------ #
    # Метаданные таксономии и настройки (Events.MetadataV2.Contracts.yaml)
    # ------------------------------------------------------------------ #

    def get_events_taxonomy(self) -> list[dict[str, Any]]:
        """Получить таксономию полей событий (все поля, не только выбранные).

        GET /api/events/v2/taxonomy (``GetTaxonomy``) - полный каталог полей
        с признаками filterable/groupable/aggregatable. В отличие от
        ``get_events_metadata``, который возвращает поля доступной метаданных
        выборки.

        :return: список описаний полей taxonomy
        """
        url = f"https://{self.__core_hostname}{self.__api_events_taxonomy}"

        response: dict[str, Any] | None = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
        ).json()

        if response is None or "fields" not in response:
            self.log.error(
                'status=failed, action=get_events_taxonomy, msg="Core data request '
                f'return None or has wrong response structure", '
                f"hostname={self.__core_hostname!r}"
            )
            raise RuntimeError(
                "Core data request return None or has wrong response structure"
            )
        return cast("list[dict[str, Any]]", response.get("fields"))

    def get_events_settings(self) -> dict[str, Any]:
        """Получить настройки отображения событий (``GetSettings``).

        GET /api/events/v2/events_settings - например
        ``{"showContentFromDescendants": false}``.

        :return: настройки событий
        """
        url = f"https://{self.__core_hostname}{self.__api_events_settings}"

        response: dict[str, Any] | None = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
        ).json()

        if response is None:
            self.log.error(
                'status=failed, action=get_events_settings, msg="Core data request '
                f'return None", hostname={self.__core_hostname!r}'
            )
            raise RuntimeError("Core data request return None")
        return response

    # ------------------------------------------------------------------ #
    # Подсчёт событий (EventsV2/EventsV3 Contracts, GetCountMinMax /
    # GetEventsCountByToken)
    # ------------------------------------------------------------------ #

    def get_events_count_by_token(self, token: str) -> int:
        """Получить общее количество событий по токену v3-запроса.

        GET /api/events/v3/events/{token}/count (``GetEventsCountByToken``).
        Токен возвращает ответ ``get_events_by_filter_v3``/
        ``__get_events_by_filter_v3`` при ``noCount=true`` - запрос
        событий не считается на сервере, а досчёт идёт этим методом.

        :param token: токен предыдущего v3-запроса (GUID)
        :return: общее количество событий
        """
        url = f"https://{self.__core_hostname}{self.__api_events_v3}/{token}/count"

        response: dict[str, Any] | None = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
        ).json()

        if response is None or "totalCount" not in response:
            self.log.error(
                'status=failed, action=get_events_count_by_token, msg="Core data '
                f'request return None or has wrong response structure", '
                f"token={token!r}, hostname={self.__core_hostname!r}"
            )
            raise RuntimeError(
                "Core data request return None or has wrong response structure"
            )
        return int(response["totalCount"])

    def get_events_count(
        self,
        query_filter: str,
        time_from: int,
        time_to: int,
        group_ids: str | None = None,
    ) -> dict[str, Any]:
        """Посчитать события по фильтру с границами периода (``GetCountMinMax``).

        POST /api/events/v2/events_countminmax - быстрый счётчик без выгрузки
        событий; возвращает ``totalCount`` и фактические границы периода
        (``periodStart``/``periodEnd``) найденных событий.

        :param query_filter: фильтр на языке PDQL
        :param time_from: начало диапазона поиска (Unix timestamp в секундах)
        :param time_to: конец диапазона поиска (Unix timestamp в секундах)
        :param group_ids: UUID групп активов (опционально)
        :return: ответ API (``totalCount``, ``periodStart``, ``periodEnd``)
        """
        params = {
            "filter": {
                "select": ["time"],
                "where": query_filter,
                "orderBy": [],
                "groupBy": [],
                "aggregateBy": [],
                "distributeBy": [],
                "top": None,
                "aliases": {"groupBy": {}},
            },
            "timeFrom": time_from,
            "timeTo": time_to,
        }
        api_url = self.__api_events_countminmax_v2
        if group_ids:
            api_url = f"{api_url}?groupIds={group_ids}"
        url = f"https://{self.__core_hostname}{api_url}"

        response: dict[str, Any] | None = exec_request(
            self.__core_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
            json=params,
        ).json()

        if response is None or "totalCount" not in response:
            self.log.error(
                'status=failed, action=get_events_count, msg="Core data request '
                f'return None or has wrong response structure", '
                f"hostname={self.__core_hostname!r}"
            )
            raise RuntimeError(
                "Core data request return None or has wrong response structure"
            )
        return response

    def get_events_count_v3(
        self,
        query_filter: str,
        group_ids: str | None = None,
    ) -> dict[str, Any]:
        """Посчитать события по PDQL v3 (``GetCountMinMax`` v3).

        POST /api/events/v3/events_countminmax. Тело - ``PdqlQuery``
        (``{"pdqlText": "...", "groupValues": [...]}``). В отличие от v2
        (``get_events_count``) временное окно в запросе не задаётся: на R28.0
        ``period(...)`` в PDQL не разбирается (400), а без него ``totalCount``
        считается по всему периоду хранения (проверено на стенде - сотни
        миллионов событий при пустом фильтре). Ответ идентичен v2:
        ``totalCount``, ``periodStart``, ``periodEnd`` (фактические границы
        данных).

        :param query_filter: PDQL v3-запрос (например ``select(time,
            event_src.host)``; ``where(...)`` парсер эндпоинта не принимает)
        :param group_ids: UUID групп активов (query, опционально)
        :return: ответ API (``totalCount``, ``periodStart``, ``periodEnd``)
        """
        params: dict[str, Any] = {"pdqlText": query_filter}
        api_url = self.__api_events_countminmax_v3
        if group_ids:
            api_url = f"{api_url}?groupIds={group_ids}"
        url = f"https://{self.__core_hostname}{api_url}"

        response: dict[str, Any] | None = exec_request(
            self.__core_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
            json=params,
        ).json()

        if response is None or "totalCount" not in response:
            self.log.error(
                'status=failed, action=get_events_count_v3, msg="Core data request '
                f'return None or has wrong response structure", '
                f"hostname={self.__core_hostname!r}"
            )
            raise RuntimeError(
                "Core data request return None or has wrong response structure"
            )
        return response

    # ------------------------------------------------------------------ #
    # Значения полей (EventsV2.Contracts.yaml, CountDistinctFieldValues /
    # GetGroupValues)
    # ------------------------------------------------------------------ #

    def count_distinct_field_values(
        self,
        query_filter: str,
        fields: list[str],
        time_from: int,
        time_to: int,
        function: str = "COUNT",
        top: int = 100,
    ) -> list[dict[str, Any]]:
        """Получить уникальные значения полей с количеством (контракт
        ``CountDistinctFieldValues``).

        POST /api/events/v2/events/count_distinct_field_values. Фильтр -
        строка PDQL (в отличие от структурированного ``filter`` у выборки
        событий). Ответ - список ``{values: [...], count: N}``.

        :param query_filter: фильтр на языке PDQL (строка)
        :param fields: поля, значения которых считаются
        :param time_from: начало диапазона поиска (Unix timestamp в секундах)
        :param time_to: конец диапазона поиска (Unix timestamp в секундах)
        :param function: функция агрегации (``COUNT``/``SUM``/...)
        :param top: максимальное число значений
        :return: список уникальных значений с количеством
        """
        params = {
            "timeFrom": time_from,
            "timeTo": time_to,
            "filter": query_filter,
            "fields": fields,
            "function": function,
            "top": top,
        }
        url = f"https://{self.__core_hostname}{self.__api_events_distinct_values}"

        response: list[dict[str, Any]] | None = exec_request(
            self.__core_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
            json=params,
        ).json()

        if response is None or not isinstance(response, list):
            self.log.error(
                'status=failed, action=count_distinct_field_values, msg="Core data '
                f'request return None or has wrong response structure", '
                f"hostname={self.__core_hostname!r}"
            )
            raise RuntimeError(
                "Core data request return None or has wrong response structure"
            )
        return response

    def get_group_values(
        self,
        query_filter: str,
        fields: list[str],
        time_from: int,
        time_to: int,
        function: str = "COUNT",
        top: int = 100,
    ) -> dict[str, Any]:
        """Получить значения групп с агрегатом (контракт ``GetGroupValues``).

        POST /api/events/v2/events/group_values. Отличается от
        ``count_distinct_field_values`` формой ответа:
        ``{"groups": [{"fieldValues": [...], "aggregateValue": N}], "errors":
        [...]}``.

        :param query_filter: фильтр на языке PDQL (строка)
        :param fields: поля группировки
        :param time_from: начало диапазона поиска (Unix timestamp в секундах)
        :param time_to: конец диапазона поиска (Unix timestamp в секундах)
        :param function: функция агрегации (``COUNT``/``SUM``/...)
        :param top: максимальное число групп
        :return: ответ API (``groups``/``errors``)
        """
        params = {
            "timeFrom": time_from,
            "timeTo": time_to,
            "filter": query_filter,
            "fields": fields,
            "function": function,
            "top": top,
        }
        url = f"https://{self.__core_hostname}{self.__api_events_group_values}"

        response: dict[str, Any] | None = exec_request(
            self.__core_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
            json=params,
        ).json()

        if response is None or "groups" not in response:
            self.log.error(
                'status=failed, action=get_group_values, msg="Core data request '
                f'return None or has wrong response structure", '
                f"hostname={self.__core_hostname!r}"
            )
            raise RuntimeError(
                "Core data request return None or has wrong response structure"
            )
        return response

    # ------------------------------------------------------------------ #
    # Локализация и вложенные события (EventsV2.Contracts.yaml)
    # ------------------------------------------------------------------ #

    def get_event_localization(
        self,
        event_data: dict[str, Any],
        locale: str = "RUS",
        siem_id: str | None = None,
    ) -> dict[str, Any]:
        """Получить локализованный текст события (контракт ``GetLocalization``).

        POST /api/events/v2/localization. Тело - ``{data, locale, siemId}``,
        где ``data`` - событие целиком (например из ``get_event_details``).
        Требует прав, больших чем read-only PAT (на стенде R28.0 под PAT
        ответ ``403 forbidden.error``).

        :param event_data: событие (data-часть, см. ``get_event_details``)
        :param locale: локаль (``RUS``/``ENG``)
        :param siem_id: id конвейера (GUID), опционально
        :return: ответ API (``text`` - локализованный текст)
        """
        params: dict[str, Any] = {"data": event_data, "locale": locale}
        if siem_id is not None:
            params["siemId"] = siem_id
        url = f"https://{self.__core_hostname}{self.__api_events_localization}"

        response: dict[str, Any] | None = exec_request(
            self.__core_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
            json=params,
        ).json()

        if response is None or "text" not in response:
            self.log.error(
                'status=failed, action=get_event_localization, msg="Core data '
                f'request return None or has wrong response structure", '
                f"hostname={self.__core_hostname!r}"
            )
            raise RuntimeError(
                "Core data request return None or has wrong response structure"
            )
        return response

    def get_event_subevents(
        self,
        event_id: str,
        event_date: str,
        query_filter: str,
        time_from: int,
        time_to: int,
        limit: int = 1000,
        offset: int = 0,
        field: str = "subevents",
    ) -> list[dict[str, Any]]:
        """Получить вложенные (sub)события события (``GetLinkedEvents``).

        POST /api/events/v2/subevents/{id}?field=...&time=...&limit=...&
        offset=... Эндпоинт жив только для событий, у которых поле ``field``
        заполнено (``count.subevents > 0``); иначе Core отвечает ``400``
        (``The 'subevents' field for searching subevents is missing in the
        event``) - в этом случае бросается RuntimeError.

        :param event_id: идентификатор родительского события
        :param event_date: время события (ISO date-time из ``_meta.time``)
        :param query_filter: PDQL-фильтр по вложенным событиям
        :param time_from: начало диапазона поиска (Unix timestamp в секундах)
        :param time_to: конец диапазона поиска (Unix timestamp в секундах)
        :param limit: число запрашиваемых вложенных событий
        :param offset: позиция выдачи
        :param field: поле-связка (по умолчанию ``subevents``)
        :return: массив вложенных событий
        """
        params = {
            "filter": {
                "select": ["time", "event_src.host", "text"],
                "where": query_filter,
                "orderBy": [{"field": "time", "sortOrder": "ascending"}],
                "groupBy": [],
                "aggregateBy": [],
                "distributeBy": [],
                "top": None,
                "aliases": {"groupBy": {}},
            },
            "timeFrom": time_from,
            "timeTo": time_to,
        }
        url = (
            f"https://{self.__core_hostname}{self.__api_events_subevents_v2}/"
            f"{event_id}?field={field}&time={event_date}"
            f"&offset={offset}&limit={limit}"
        )

        response: dict[str, Any] | None = exec_request(
            self.__core_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
            json=params,
        ).json()

        if response is None or "events" not in response:
            self.log.error(
                'status=failed, action=get_event_subevents, msg="Core data request '
                f"return None or has wrong response structure (event has no "
                f"subevents?), event_id={event_id!r}, "
                f"hostname={self.__core_hostname!r}"
            )
            raise RuntimeError(
                "Core data request return None or has wrong response structure"
            )
        return cast("list[dict[str, Any]]", response.get("events"))

    def get_event_subevents_count(
        self,
        event_id: str,
        event_date: str,
        query_filter: str,
        time_from: int,
        time_to: int,
        field: str = "subevents",
    ) -> dict[str, Any]:
        """Посчитать вложенные события (``GetLinkedEventsCountMinMax``).

        POST /api/events/v2/subevents_countminmax/{id}?field=...&time=...
        Возвращает ``totalCount`` и границы периода вложенных событий. Как и
        ``get_event_subevents``, работает только для событий с заполненным
        полем-связкой.

        :param event_id: идентификатор родительского события
        :param event_date: время события (ISO date-time из ``_meta.time``)
        :param query_filter: PDQL-фильтр по вложенным событиям
        :param time_from: начало диапазона поиска (Unix timestamp в секундах)
        :param time_to: конец диапазона поиска (Unix timestamp в секундах)
        :param field: поле-связка (по умолчанию ``subevents``)
        :return: ответ API (``totalCount``, ``periodStart``, ``periodEnd``)
        """
        params = {
            "filter": {
                "select": ["time"],
                "where": query_filter,
                "orderBy": [],
                "groupBy": [],
                "aggregateBy": [],
                "distributeBy": [],
                "top": None,
                "aliases": {"groupBy": {}},
            },
            "timeFrom": time_from,
            "timeTo": time_to,
        }
        url = (
            f"https://{self.__core_hostname}"
            f"{self.__api_events_subevents_countminmax_v2}/"
            f"{event_id}?field={field}&time={event_date}"
        )

        response: dict[str, Any] | None = exec_request(
            self.__core_session,
            url,
            method="POST",
            timeout=self.settings.connection_timeout,
            json=params,
        ).json()

        if response is None or "totalCount" not in response:
            self.log.error(
                'status=failed, action=get_event_subevents_count, msg="Core data '
                f"request return None or has wrong response structure, event_id="
                f'{event_id!r}, hostname={self.__core_hostname!r}"'
            )
            raise RuntimeError(
                "Core data request return None or has wrong response structure"
            )
        return response

    # ------------------------------------------------------------------ #
    # Raw-событие (EventsV2.Contracts.yaml, GetRawEventData).
    # Жив на 26.x/27.x; на R28.0 404 - см. __check_raw_supported
    # ------------------------------------------------------------------ #

    def get_event_raw(
        self, event_id: str, recv_time: str | None = None
    ) -> dict[str, Any]:
        """Получить сырое (до нормализации) событие (``GetRawEventData``).

        GET /api/events/v2/events/{id}/raw?recvTime=...

        Версионная поддержка: на ядрах до R28.0 эндпоинт жив; на R28.0 он
        выведен из маршрутизации (404 на 443/8091/3334, студийной замены в
        KB Studio нет), и метод бросает RuntimeError до запроса.

        :param event_id: идентификатор события
        :param recv_time: время приёма (ISO date-time, опционально)
        :return: сырое событие (произвольный объект)
        :raises RuntimeError: на ядрах 28.0+; если ответ не объект
        """
        self.__check_raw_supported("get_event_raw")

        api_url = f"{self.__api_events_v2}/{event_id}/raw"
        if recv_time is not None:
            api_url = f"{api_url}?recvTime={recv_time}"
        url = f"https://{self.__core_hostname}{api_url}"

        response: dict[str, Any] | None = exec_request(
            self.__core_session,
            url,
            method="GET",
            timeout=self.settings.connection_timeout,
        ).json()

        if response is None or not isinstance(response, dict):
            self.log.error(
                'status=failed, action=get_event_raw, msg="Core data request return '
                f'None or has wrong response structure", event_id={event_id!r}, '
                f"hostname={self.__core_hostname!r}"
            )
            raise RuntimeError(
                "Core data request return None or has wrong response structure"
            )
        return response
