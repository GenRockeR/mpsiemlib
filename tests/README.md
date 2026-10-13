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
- Для выборки событий используют интервал в 60 сек, для группировки в 3600 сек. Если у вас нет событий в этом интервале, тесты провалятся.

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

## Tables
- Пишущие тесты (import/set_table_row/truncate) работают с табличным списком из
  MP_TEST_TABLE (по умолчанию `test_tl_r272`). Такой список нужно создать в KB
  и установить на конвейер (создание списков — домен KB, не модуля Tables);
  без него эти тесты скипаются.
- Читающие тесты сами выбирают первый непустой список из установленных.
- Whitelist-тест скипаётся, если на стенде недоступен `/api/whitelists`
  (на R28.0 отвечает 404 для любых token).

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
