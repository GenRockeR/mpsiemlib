"""Сбор данных метрик через модули mpsiemlib (MP SIEM 26.x-28.x).

Раньше exporter ходил напрямую по порту 8013 (``/events/counter/simple``,
``/v2/control/tables``) - прямой интерфейс хранилища выведен из MP SIEM
и на R28.0 недоступен (даже порт закрыт; хранилище - Logspace). Все метрики
теперь берутся через REST API ядра силами SDK:

- EPS - ``SourceMonitor.get_average_eps``
  (``GET /api/events_monitoring/v3/sources/eps/average``, Monitoring v3);
  прежняя разбивка raw/norm/corr_in/corr_out контрактом не отдаётся -
  доступен средний EPS по всем (или одному) источникам;
- размеры табличных списков - ``Tables.get_tables_list`` +
  ``Tables.get_table_info`` (``currentSize``).
"""

from datetime import UTC, datetime
from typing import Any

from mpsiemlib.common import ModuleNames
from mpsiemlib.modules import MPSIEMWorker


def get_eps(worker: MPSIEMWorker, window_minutes: int = 5) -> dict[str, float]:
    """Средний EPS по всем источникам за окно последних N минут.

    Границы контракта задаются в минутах от 00:00 UTC, окно строго короче
    24 часов (замерено на R28.0); при переходе через полночь окно сжимается
    до [0, текущая минута].

    :param worker: Инициализированный MPSIEMWorker
    :param window_minutes: размер окна наблюдения в минутах (1..1439)
    :return: {"average_eps": <float>}
    """
    window_minutes = min(max(window_minutes, 1), 1439)
    now = datetime.now(UTC)
    end = max(now.hour * 60 + now.minute, 1)
    begin = max(end - window_minutes, 0)

    module = worker.get_module(ModuleNames.SOURCE_MONITOR)
    try:
        return {"average_eps": module.get_average_eps(begin, end)}
    finally:
        module.close()


def get_siem_tables(
    worker: MPSIEMWorker, siem_id: str | None = None
) -> list[dict[str, Any]]:
    """Список табличных списков с текущими размерами.

    ``get_tables_list`` отдаёт имена без размеров, поэтому по каждой таблице
    идёт отдельный ``get_table_info`` (``currentSize``).

    :param worker: Инициализированный MPSIEMWorker
    :param siem_id: id конвейера (GUID); обязателен при нескольких конвейерах
    :return: [{"name", "id", "fillType", "currentSize"}, ...]
    """
    module = worker.get_module(ModuleNames.TABLES)
    try:
        tables = []
        for name in module.get_tables_list(siem_id):
            info = module.get_table_info(name, siem_id)
            tables.append(
                {
                    "name": info.get("name"),
                    "id": info.get("id"),
                    "fillType": info.get("type"),
                    "currentSize": info.get("size_current"),
                }
            )
        return tables
    finally:
        module.close()
