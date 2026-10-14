# Unit тесты
## Запуск
Добавить переменные окружения:
- MP_CORE_HOSTNAME: IP/Hostname для доступа к MP CORE (без схемы http(s)) 
- MP_STORAGE_HOSTNAME: IP/Hostname для доступа к MP Storage (Elasticsearch) (без схемы http(s))
- MP_SIEM_HOSTNAME: IP/Hostname для доступа к MP SIEM Server (без схемы http(s))
- MP_LOGIN: учетная запись с ролью администратора в PT KB, IAM, SIEM
- MP_PASSWORD: пароль
- USE_LOCAL_AUTH: true
- CLIENT_SECRET: токен из MP SIEM
```bash
  sudo grep ClientSecret /var/lib/deployer/role_instances/core*/params.yaml
```
- При запуске выставить директорию запуска: mpsiemlib

# Особенности
- Все тесты используют локальную учетную запись. Сменить поведение можно в tests/settings.py
- Далеко не все тесты имеют строгие проверки возвращаемых данных.

## MPSIEMAuth (unit_worker)
- `AuthMethodsTestCase` проверяет все типы аутентификации на реальном стенде:
  PAT (токен из кредов, CORE/KB), SECRET (`:3334/connect/token`), LOCAL
  (cookie-вход через UI-form), версия ядра (`system_info` на R28.0 отвечает
  500 - версия добирается fallback'ом licensing v1).
- `disconnect()` под LOCAL/LDAP гасит серверную cookie-сессию IAM
  (`POST /ptms/api/sso/v2/account/logout` + обход 302 на `/ui/logout`); после
  него probe-запрос отвечает 401. Кейс логинится изолированным `MPSIEMAuth`,
  поэтому общие PAT-сессии других классов не затрагиваются. Под PAT/SECRET
  выход не вызывается (Bearer не отзывается).
- Неверные креды и отсутствующее LDAP-подключение - оба `400
  Platform.Iam.User.Login.Error` - оборачиваются в `AuthError` с сообщением
  IAM. LDAP-кейсы скипаются (LDAP на стенде нет), STORAGE-кейсы скипаются
  (storage - Logspace, ES `:9200` закрыт).

## Conveyor
- SiemManager API (`/api/siem_manager/v1/siems*`) на R28.0 требует токен с
  правами на чтение реестра SIEM: без прав GET отвечает 401 (падает весь
  класс).
- `UpdateSiem` (PUT `/siems/{id}`) и `UpdateSiemAgents`
  (PUT `/siems/{id}/agents`) на R28.0 отвечают 403 (пустое тело) и под PAT,
  и под Local Administrator — эндпоинт закрыт на стороне ядра, кейсы
  скипаются. `UpdateSiemInfo` (PUT `/siems/{id}/info`) и `MakeSiemPrimary`
  (POST `/siems/{id}/make_primary`) живы — 204 под PAT.
- Write-кейсы идемпотентны: отправляют текущие значения основного конвейера
  (register/info/agents/make_primary), стенд не меняется; удаление не
  проверяется на реальном конвейере (только негативный кейс на несуществующем
  id — 400 SM.SiemNotFound.Error, как в контракте).

## Events
- Storage на стендах — Logspace, порт Elasticsearch `:9200` закрыт, поэтому
  `unit_events.py` — мок-тесты (`unittest.mock` вместо `elasticsearch`-клиента):
  тело ES-запроса, расчёт индексов (`siem_events_*`) и data stream
  (`siem_events`), разбор агрегаций, пустые ответы и `index_not_found_exception`.
  Сетевых запросов нет, прогон не требует стенда:
  `PYTHONPATH=.. python -m unittest -v unit_events` (нужен extras `events`:
  `poetry install --extras events`).
- Кейсы проверяют контракт поведения модуля: пустой результат группировки —
  одна строка-схема с `count=0`; `get_events` выгружает выборку целиком
  пачками по `storage_batch_size` через point in time + `search_after`
  (tie-breaker `_shard_doc`, PIT закрывается в `finally` и при досрочном
  выходе из генератора); ошибки шардов (`root_cause`) дают пустой результат
  с log.error.

## EventsAPI
- `get_event_raw` закрыт version-gate: на R28.0 `GET /api/events/v2/events/{id}/raw`
  отвечает 404 на всех портах (443/8091/3334) и студийной замены нет, поэтому на
  ядрах ≥28.0 метод бросает `RuntimeError` до запроса; тест принимает обе ветки
  (RuntimeError на 28+, dict на 26/27).
- `get_event_localization` (`POST /api/events/v2/localization`) требует прав выше
  read-only: под PAT стенд отвечает 403 `forbidden.error` — тест скипается по 403.
- `get_event_subevents`/`get_event_subevents_count` на стенде проверяются только
  негативным кейсом: v3-выборка за окно не содержит событий с заполненным
  `count.subevents`, а на событие без поля-связки Core отвечает 400 (эндпоинт жив,
  доходит до валидации).
- `get_events_aggregation_v3` требует синтаксис v3-агрегации:
  `select(...) | group(key: [...], agg: COUNT(*) as Alias) | sort(...) | limit(n)`
  (синтаксис снят с боевого запроса KB UI; `where(...)` после `select` этот парсер
  не принимает — фильтр задаётся внутри `select`/`group`).
- `get_events_count_v3` не принимает временное окно: `period(...)` в PDQL не
  парсится, `where(...)` этот парсер тоже отклоняет — тест считает по
  `select(time, event_src.host)` (всё хранилище).

## SourceMonitor
- Модуль читает мониторинг источников из `Monitoring.yaml` (`/api/events_monitoring/v3`):
  списки активов/форвардеров, статистика, неидентифицированные источники,
  графики и средний EPS, экспорт в CSV.
- Кейсы `test_get_forwarders_*`, `test_get_sources_by_forwarder` и
  `test_get_forwarder_eps_graph` скипаются: на R28.0-стенде форвардеров нет
  (`POST /v3/forwarders` отдаёт `totalItems = 0`), объект создать нечем.
  Проверено read-only зондом — эндпоинт жив, пустой ответ.
- `test_get_unidentified_source_eps_graph` скипается, если нет неидентифицированных
  источников. График по неидентифицированному источнику принимает границы только в
  `HH:mm` (не ISO duration) — поведение сервера, модуль конвертирует минуты суток.
- Экспорт-кейсы только читающие: CSV пишется в память теста, на диск ничего не
  сохраняется. `export_forwarders_specified([])` контракт разрешает, но сервер
  R28.0 отвечает `400` — кейс фиксирует именно это (`HTTPError`).

## Tables
- Пишущие тесты (import/set_table_row/truncate) работают с табличным списком из
  MP_TEST_TABLE (по умолчанию `test_tl_r272`). Такой список нужно создать в KB
  и установить на конвейер (создание списков — домен KB, не модуля Tables);
  без него эти тесты скипаются. На R28.0-стенде такого списка нет — тесты
  скипаются, есть `test_AD_hosts_lab_local` (`enrichmentRule`).
- Читающие тесты сами выбирают первый непустой список из установленных.
- Whitelist-тесты скипаются, если среди установленных списков не найден
  отвечающий `canEdit` (см. `__pick_whitelist_table`). На R28.0 whitelist API
  ядра `/api/whitelists/{token}` удалён (404 для любых token), работает
  MP10-домен `/app/kb/api/whitelists/{token}` с обязательным `Content-Database`
  (модуль резолвит первую deployable БД KB).
- `test_whitelist_insert_exists_remove` делает round-trip на найденном `_WL`
  (пишет в контентный справочник PTKB!) и убирает probe-строку cleanup'ом. На
  R28.0 `remove` существующей строки отвечает 500, уже применив удаление, —
  кейс это и фиксирует. Арity строки = поля списка без `_id`/`_last_changed`.

## Macros
- Модуль работает через KB Studio API (`/api-studio/siem/macros*`, порт 8091):
  контрактный `/api/SiemMacros*` на R28.0 отвечает 404 и тестами не проверяется.
- Нужна deployable контентная БД — берётся первая deployable из KB; без неё
  тесты скипаются целиком.
- CRUD-кейсы создают макросы/метки с префиксом `unittest_` и удаляют их в
  `finally`; `tearDownClass` подчищает остаток по полному списку (не по
  `search`: серверный поиск в `/macros/list` нестрогий и по нему легко задеть
  посторонние объекты).
- Тесты долгие (~6 мин): каждое изменение читает модель через `.../edit` и
  прогоняет текст через `validate` (обязательный хэш `SiemMacros.HashMismatch`).

## KB
- Тесты KB долгие, т.к. есть проверки установки и удаления контента в/из SIEM. Тест дожидается завершения выполнения операции, делая проверки статуса каждые 10 сек и так 6 раз. Если KB нагружен, то тест может не успеть и завершится с ошибкой
- Тесты KB могут провалиться, если нет deploy базы или она пуста.
