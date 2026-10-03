# Быстрый старт

## Установка

```bash
pip install mpsiemlib
```

Модуль `Events` работает напрямую с Elasticsearch и требует дополнительного
экстра (на стендах со storage Logspace не требуется):

```bash
pip install "mpsiemlib[events]"
```

## Аутентификация

SDK поддерживает Local/LDAP/SECRET и PAT-токен (рекомендуемый, MP SIEM 26.0+).
Параметры подключения - в `Creds`, настройки запросов - в `Settings`:

```python
from mpsiemlib.common import AuthType, Creds, Settings
from mpsiemlib.modules import MPSIEMWorker

creds = Creds()
creds.core_hostname = "mow03-mpsiem-dev.soc.bi.zone"
creds.core_auth_type = AuthType.PAT_TOKEN
creds.pat_token = "<PAT token>"

worker = MPSIEMWorker(creds, Settings())
```

## Получение модуля и работа

Модули возвращаются по имени из `ModuleNames` (см.
[Справочник модулей](reference/worker.md)):

```python
import time

from mpsiemlib.common import ModuleNames

events_api = worker.get_module(ModuleNames.EVENTSAPI)

time_to = int(time.time())
time_from = time_to - 5 * 60  # последние 5 минут

for event in events_api.get_events_by_filter_v3(
    query_filter="select(time, event_src.host, event_src.ip)",
    time_from=time_from,
    time_to=time_to,
):
    print(event)

events_api.close()
```

Доступные модули: `ASSETS`, `CONVEYOR`, `EVENTS` (Elasticsearch), `EVENTSAPI`,
`FILTERS`, `HEALTH`, `INCIDENTS`, `KB`, `MACROS`, `SOURCE_MONITOR`, `TABLES`,
`TASKS`, `URM`, `AUTH`.

## Переменные окружения (для примеров и тестов)

- `MP_CORE_HOSTNAME` - хост MP Core (без схемы)
- `MP_PAT_TOKEN` - PAT-токен (или `MP_LOGIN`/`MP_PASS` + `CLIENT_SECRET`)
- `MP_SIEM_HOSTNAME`, `MP_STORAGE_HOSTNAME` - SIEM и Storage (для модуля `Events`)

## Примеры

В репозитории: `examples/data_query_v3/query.py` (выгрузка событий PDQL-фильтром),
`examples/Assets/` (экспорт/импорт активов), `examples/mpsiem_exporter/`
(метрики в Prometheus). Готовые сценарии - в `tests/unit_*.py`.
