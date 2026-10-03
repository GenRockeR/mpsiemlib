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
    """Модуль получения информации о событиях через API UI."""

    __api_events_metadata = "/api/events/v2/events_metadata"
    __api_events_v2 = "/api/events/v2/events"
    __api_events_v3 = "/api/events/v3/events"
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
        self.log.debug('status=success, action=prepare, msg="EventsAPI Module init"')

    def get_events_group_by_api_json(
        self, params: dict, group_ids: str | None = None
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
            raise Exception(
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
            raise Exception(
                "Core data request return None or has wrong response structure"
            )
        return cast("dict[str, Any]", response.get("event"))

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
            raise Exception(
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
            raise Exception(
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
            raise Exception(
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
            raise Exception(
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
            raise Exception(
                "Core data request return None or has wrong response structure"
            )
        return response

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
