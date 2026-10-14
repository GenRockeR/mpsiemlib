=======
# Unreleased
- Elasticsearch окончательно вынесен из ядра библиотеки в extra
  `mpsiemlib[events]` (производитель переводит storage на Logspace):
  **breaking changes** в `MPSIEMAuth` - `connect(MPComponents.STORAGE)` и
  `get_storage_version()` удалены, вместо ES-зонда (`GET :9200/_nodes`)
  бросается `StorageNotSupportedError` (наследник `AuthError`); из
  `StorageVersion` удалены `ES7`, `ES17`, `LS` (остались `ES7_17` и маркер
  фильтров `ALL`)
- Версию хранилища теперь определяет сам модуль Events (`info()` клиента) и
  проверяет линейку 7.17.x; неподдерживаемое хранилище - `StorageNotSupportedError`
  при создании модуля
- `Events.get_events()` выгружает выборку целиком через point in time +
  `search_after` (tie-breaker `_shard_doc`, закрытие PIT в finally) - вместо
  ограничения в одну пачку `storage_batch_size`
- Whitelist-методы `Tables` стали версионно-адаптивными: на R28.0 маршруты
  ядра `/api/whitelists/{tokenId}` (контракт R27.0) удалены (404 для любых
  token), поэтому с R28.0+ запросы идут в MP10-домен
  `/app/kb/api/whitelists/{tokenId}` с заголовком   `Content-Database`; имя БД резолвится лениво (первая deployable БД KB) и
  переопределяется параметром
  `db_name` во всех четырёх методах
- `Tables.remove_whitelist_rows()` на R28.0 сверяет удаление через `exists`:
  сервер отвечает `500 InternalServerError` уже после применения удаления,
  `RuntimeError` бросается только если строка осталась в списке
- `MP_TEST_TABLE` по умолчанию - `test_tl_r272` (на стендах R28 такого списка
  нет, пишущие тесты скипаются); whitelist-кейсы `unit_tables` теперь покрывают
  round-trip insert/exists/remove и негативные сценарии
- Модуль `SourceMonitor` дополнен остальными 14 эндпоинтами `Monitoring.yaml`
  (реализованы все 16 путей контракта): `get_assets_statistics`,
  `get_forwarders_statistics`, `get_unidentified_sources`,
  `get_unidentified_forwarders`, `get_asset_source_eps_graph`,
  `get_forwarder_eps_graph`, `get_unidentified_source_eps_graph`,
  `get_average_eps` и шесть экспортов в CSV (`export_assets`,
  `export_assets_specified`, `export_forwarders`, `export_forwarders_specified`,
  `export_unidentified_sources`, `export_unidentified_forwarders`). Экспорты
  возвращают текст CSV со снятым UTF-8 BOM, имя файла из `Content-Disposition`
  пишется в лог
- Границы периода у `get_average_eps` и `get_unidentified_source_eps_graph`
  принимаются в минутах от 00:00 UTC: сервер (R28.0) требует для среднего EPS
  ISO 8601 duration строго меньше суток (`PT24H` → 400), а для графика
  неидентифицированного источника - `HH:mm`; выходящие за сутки значения
  отбиваются `ValueError` до запроса. Все v3-методы на ядрах 26.x бросают
  `NotImplementedError` (v3 там отсутствует)
- `unit_sourcemonitor` расширен до 28 кейсов: статистика, неидентифицированные
  источники, графики EPS, средний EPS, все экспорт-ветки и негативы по
  `stateFilter`/`assetSourceType`/границам периода
- Модуль `Tasks` дополнен read-only эндпоинтами сканера (20 новых методов):
  агенты `get_available_agents_list`, `get_agents_by_ids`,
  `get_agent_module_capacity`; профили `get_profile_info`,
  `get_profile_schemas`, `get_profile_schemas_localization`; учётные записи
  `get_credential`, `get_credentials_tags`; задачи `get_tasks_count`,
  `get_scope_tasks_count`, `get_tasks_bookmarks`, `validate_task`,
  `get_task_info_v4`, `calc_next_run_time`; запуски `get_runs_list`,
  `get_job_info`, `get_job_errors`, `get_run_agents`; справочники сканера
  `get_dictionaries_list`, `get_dictionary`
- Модуль `Tasks` дополнен write-доменом сканера (50 методов) - реализованы все
  остальные маршруты контрактов `TaskSelections`, `TaskBulkOperations`,
  `TaskReportDownload`, `Scans.Contracts`, а также CRUD профилей, учётных
  записей (четыре типа + `PUT /api/v4/credentials/certificates/{id}`),
  справочников, ёмкости модулей агента и удаления агентов:
  - управления задачей: `suspend_task`, `clean_task_bookmarks`,
    `clear_task_migration_errors`, `start_task_connection_check`,
    `get_task_report`, `create_task_v4`, `edit_task_v4`,
    `get_task_params_for_copy`, `copy_task`, `wait_task_status`,
    `get_tasks_list_v4`
  - выделения (обязательный аргумент bulk-операций): `create_task_selection`,
    `edit_task_selection`, `get_task_selection`, `delete_task_selection`
  - массовые операции: `start_tasks`, `stop_tasks`, `suspend_tasks`,
    `delete_tasks`, `clean_tasks_bookmarks`, `start_tasks_connection_check`,
    `update_tasks_schedule`, `compare_tasks_schedule`,
    `get_bulk_operation_status`, `wait_bulk_operation`, `delete_bulk_operation`,
    `get_tasks_groups`, `update_tasks_groups`
  - агенты: `set_agent_module_capacity`, `delete_agent_module_capacity`,
    `validate_agents_delete`, `delete_agents`, `get_agents_delete_status`
  - профили: `create_profile`, `update_profile`, `delete_profile`,
    `clear_profile_migration_errors`, `generate_pentest_profile`,
    `validate_vulnerability_for_pentest`
  - учётные записи: `create_credential`, `update_credential`,
    `update_credential_v4`, `delete_credential`, `get_credential_type`,
    `check_laps_provider_connection`
  - справочники: `create_dictionary`, `update_dictionary`, `delete_dictionary`
  - снимки результатов скана: `build_scan_snapshot`, `add_scan`
  Write-методы сбрасывают кэши списка задач/профилей/справочников/учётных
  записей/агентов. Не реализовано осознанно: `POST /api/v1/scanner_agents`
  (404 на R28.0 - агенты ставятся вне API сканера), rest-чтение снимков
  `Scans.Contracts` и справочник групп задач (GET-путей групп в контрактах
  сканера нет)
- Замеренные особенности write-контрактов на R28.0 (отличия от доков учтены в
  докстрингах): `suspend` принимается только из `preparing`/`waiting`/
  `running`/`finishing` (из `new` - 400 «Cannot suspend scanner task.»);
  `POST /api/scanning/v4/scanner_tasks` требует тело (без него 415) и на
  dev-стенде отвечает `totalCount=0`; `PUT task_selections` пустым списком -
  400, count при этом не уменьшается; bulk-операции работают по `selectionId`,
  `start_tasks`/`start_connection_check_tasks` остаются `inProgress` из-за
  лицензии/очереди; `checkDeleteStatus` агентов сломан (всегда 400);
  certificates принимают только base64 (PEM «как есть» -
  `invalid.certificate.error`); `DELETE` справочника и учётной записи - 200/204
  с пустым телом, дубль имени - 400; учётка, на которую ссылается LAPS, не
  удаляется (`credential.dependency.error`); PUT ёмкости модуля агента
  `module_id` не валидирует (неизвестный модуль - 204, GET/DELETE - 404);
  `PUT /api/v1/scans/{scanId}`
  валидирует `source` (`Scanner` - 400) и не имеет DELETE-маршрута;
  pentest-эндпоинты на стенде отвечают 500/405
- Исправлены дефекты кэширования `Tasks`: кэш `get_tasks_list()` и
  `get_modules_list()` теперь ключуется фильтром (раньше второй вызов с
  `main_filter`/`work_type` подменял полный набор отфильтрованным), кэш
  `get_credentials_list()` - списком тэгов; кэш профилей не пополняется
  отфильтрованной по модулю выборкой
- `get_task_info()` больше не добавляет в ответ поля `parameters`: в контракте
  v3 `ScannerTask` его нет (вместо него `overrides`/`triggerParameters`).
  Для полей v4 (`agents`, `gen2Agents`, `output`, `connectionCheckSupported`)
  добавлен `get_task_info_v4()`
- `unit_tasks` расширен до 109 кейсов: справочники, пагинация, кэши и негативы
  по кодам ответа сервера (неизвестный агент → 500, профиль/job → 400, задача
  → 404, справочник → пустое тело); write-кейсы идут на копиях задач
  (`copy_task`) и на вновь созданных учётках/справочниках/профилях, все
  created-объекты убираются в `tearDownClass`; `test_create_delete` переведён
  на skip вместо падения при отсутствии агента/SysLog-профиля
- Модуль `Incidents` дополнен read-доменом (22 новых метода): статистика
  `IncidentStatistics.yaml` - `get_incidents_status_statistics`,
  `get_created_incidents_distribution`, `get_closed_incidents_distribution`,
  `get_open_incidents_distribution`; read-model задач и коллекций -
  `get_incident_task_by_id`, `get_incident_issue_by_id`, `get_incident_issues`,
  `get_incident_events_by_id`, `get_deleted_incident_ids`; синхронизация и
  публичный id - `get_sync_incidents_short_info`, `get_incident_version`,
  `get_public_incident_id`; фильтры и папки `IncidentFilter.yaml` /
  `IncidentFilterFolder.yaml` - `get_incident_filter`,
  `get_default_incident_filter`, `get_incident_filter_public`,
  `get_default_incident_filter_public`, `get_incident_filters_by_ids`,
  `validate_incident_filter`, `get_incident_filters_hierarchy`,
  `get_incident_filter_folder`, `get_incident_filter_folders_in_folder`,
  `get_incident_filters_in_folder`
- Модуль `Incidents` дополнен write-доменом (20 методов) - реализованы все
  маршруты контрактов `Incidents.yaml` и `IncidentsExport.yaml`, а также
  запись фильтров/папок:
  - инциденты: `create_incident`, `update_incident`, `change_incident_status`,
    `change_incident_measures`, `update_incident_groups`, `link_incident_events`,
    `unlink_incident_event`, `unlink_incident_asset`, `delete_incidents`,
    `delete_incidents_by_filter`
  - задачи: `add_incident_task`, `update_incident_task`
  - экспорт: `export_incidents_by_ids`, `export_incidents_by_filter`
  - фильтры: `create_incident_filter`, `update_incident_filter`,
    `delete_incident_filter`
  - папки фильтров: `create_incident_filter_folder`,
    `update_incident_filter_folder`, `delete_incident_filter_folder`
  Не реализовано осознанно (замерено на R28.0): MPX-ветка
  `IncidentManagement.yaml` / `IncidentReadModel.yaml`
  (`/api/incident_management/v1`, `/api/incident_read_model/v1` - архив,
  комментарии, файлы, вердикт, назначенный, связи parent/child),
  `IncidentAssignees.yaml`, `IncidentSource.yaml`, импорт сессиями
  (`IncidentImportSession.yaml`), триггеры `IncidentsTriggers*Contracts.yaml` и
  `GET /api/sync/incidents/{incidentId}/domainevent` - все отвечают `404`
  (маршруты ядром не подняты); публичные `IncidentsPublic.yaml`
  (`GET /api/v1/external_incident_ids?publicId=`) и `PublicIncidents.yaml`
  (`/api/public/v1/incidents`, `incidents_bundle*`) живы, но в модуль не
  берутся, MPX-словари `/api/incidents/dictionaries/*` дублируют контрактные
  справочники
- `update_incident_task()` работает как read-modify-write: `PUT /api/issues/{id}`
  на R28.0 отклоняет частичное тело (500 internal.error), поэтому текущее
  состояние задачи читается через read-model и перезаписываются только
  переданные поля
- Исправлен маппинг `get_incident_tasks()`: read-model `TaskDto` отдаёт
  `taskId`/`innerIncident.incidentId`, а не `id`/`incident.id` — раньше поля
  `id` и `incident_id` всегда были пустыми
- Замеренные особенности контрактов инцидентов на R28.0 (учтены в докстрингах):
  приватный `GET /api/v1/incidents/filters/default` отвечает
  `500 internal.error` (`source: Unkown`) - рабочим является публичный
  `/api/incident_filters/default`; неизвестная задача read-model - `200` с
  пустым телом (не `404`), поэтому метод бросает `ValueError`; несуществующая
  папка фильтров - `500`, повторное удаление фильтра - `500`, недопустимый
  статус/переход инцидента и отвязка несвязанного события - `500`/`400`;
  `create_incident_filter`/`add_incident_task` требуют поля, которые контракт
  объявляет необязательными (`name`/`select`/`where`, `name`/`type`);
  пустая выборка `delete_by_filter` - `400`; `get_deleted_incident_ids`
  на стенде с непрогнанным read-model включает живые инциденты; удаление папки
  фильтров каскадно (но read-model удалённых фильтров устаревает); запись
  фильтров разрешена только в пользовательскую ветку (в системную - 403)
- `unit_incidents` расширен до 76 кейсов: read-model, справочники, статистика
  по всем гранулярностям, фильтры/папки (в т.ч. каскадное удаление и негативы
  по системным папкам), полный write-цикл инцидента и задачи, экспорт;
  write-кейсы создаются прогоном и убираются в `tearDownClass` (инциденты - по
  одному, т.к. пакетное удаление на R28.0 теряет часть ID), факт удаления
  сверяется по экспорту


# v1.7.1
- Переработан модуль Assets
- `get_queries()` теперь возвращает список узлов дерева запросов
- `update_query()` возвращает актуальный объект запроса
- В Assets добавлены методы: `get_pdql_available_fields`, `get_pdql_available_types`,
  `get_group_assets_ids`, `get_group_input_permissions`, `validate_group_predicate`,
  `get_import_csv_metadata`, `get_import_csv_example`
- Unit-тесты Assets расширены до 36 кейсов: пагинация, кэш групп, изменение
  конфигурации актива, негативные сценарии
- Версия Core определяется через `system_info`, при его недоступности (на R28.0
  отвечает 500) - через `/api/licensing/v1/product`; добавлены `get_core_release()`,
  `is_core_release_at_least()` и предупреждение о релизе ниже поддерживаемого
  минимума 26.0
- `tests/token` (локальные креды для тестов) добавлен в `.gitignore`

# v1.7.0
- Добавлен новый метод аутентификации через PAT-токен
- Обновлены модули
- Поддержка версий от 26.0 до 27.6.x
- Документация переведена со Sphinx на MkDocs (mkdocstrings): сгенерированный
  HTML и каталог `sphinx/` удалены из репозитория, сборка - `./build_docs.sh`
  в каталог `site/`
- Добавлен метод `KnowledgeBase.get_revisions_diff` - разница ревизий базы контента

# v1.6.1
- Добавлена возможность работы с белыми списками


# v1.6.0
- Поддержка R27.2.x
- Возможность записывать в табличные списки в конвейерном варианте
- Обновлены unit-тесты
- Добавлена возможность работы с токеном Bearer
- В модуль EventsAPI добавлен метод позволяющих получать события при помощи api v3 с использованием PDQL-синтаксиса для текстового представления.
- Переход на poetry
---

# v1.5.3.0
- Поддержка R26.1
- Добавлен модуль для работы с макросами
---

# Examples
## Assets
- Добавлены примеры использования библиотеки. Огромное спасибо @m.kosmach
---

# v1.5.1.0
## Events
- Добавлен новый модуль для получения детальной информации о событиях через UI API.
Позволяет отказаться от прямого доступа к хранилищу для получения информации о событиях.
Например, при интеграции с внешними системами для обработки инцидентов позволяет получать не только краткую информацию о связанных с инцидентами событиях (идентификатор, текст и время), но и полную информацию обо всех полях связанных событий при необходимости. Это может существенно улучшить представление карточки инцидента во внешних системах IRP/SOAR.
---

# v1.5.0.2
## Incidents
- Для инцидентов, созданных из PT NAD, детальная информация по инциденту дополнена информацией о сетевом трафике 
(время, фильтр для PT NAD, информация о сетевых сессиях, информация об алертах)
---

# v1.5.0.1
- Поддержка R25.1
- Создание и удаление задач
- Создание и удаление пользователей, ролей
---

# v1.4.0
- Поддержка R23.0, R24.0, R24.1
---

# v1.3.5
## KnowledgeBase
- Функции процессинга наборов установки исключены из библиотеки
---

# v1.3.4
## KnowledgeBase
- Улучшение логирования операций
- Синхронные операции install/uninstall
---

# v1.3.3
## KnowledgeBase
- Улучшение логирования операций
- Удаление контента
---

# v1.3.2
## KnowledgeBase
- Перемещение контента между папками
---

# v1.3.1
## KnowledgeBase
- Экспорт наборов установки в файловую структуру
- Импорт контента из файловой структуры
---

# v1.3.1
## MP SIEM test: 
- R23.1.3482
## KnowledgeBase
- Парсинг структуры набора установки в объект
- Дамп объекта в файловую структуру набора установки
- Выгрузка перечня наборов установки из SIEM (с возможностью выгрузки дерева)
- Загрузка перечня наборов установки в SIEM

---

# v1.3.0
## MP SIEM test: 
- R23.1.3482
## Assets
- Выгрузка списка инфраструктур
- Выгрузка списка групп
- Создание/удаление статических и динамических групп
- Выгрузка активов по PDQL в JSON и CSV
- Импорт активов из CSV в заданную группу 
## KnowledgeBase
- Установка всех объектов из набора установки в SIEM
---

# v0.3.5
## MP SIEM test: 
- R23.1.3482
## KnowledgeBase
- Создание/удаление папок.
- Создание/удаление наборов установки.
- Создание/удаление правил корреляции.
- Экспортировать/импортировать всего содержимого из набора установки
в форматах SIEM Lite и KB.

---

# v0.3.4
## MP SIEM test: 
- R23.1.3482
## MPSIEMAuth
- Добавлена поддержка аутентификации в иерархических инсталляциях.

---

# v0.3.3
## MP SIEM test: 
- R23.1.3482
## Tables
- Добавлен учет специфики API при построчной работе с табличками (set_table_row).  
Теперь делается автоматическое приведение к int для колонок типа number.  
Колонки типа datetime ожидают строку в формате `%d.%m.%Y %H:%M:%S` или int - timestamp
## Wheel
- Добавлены зависимости для сборки whl

---

# v0.3.2
## MP SIEM test: 
- R22.0.3192
- R23.1.3482
## Tables
- Добавление и удаление записей в таблицах построчно
## Helpers
- Загрузка данных в табличные списки через массив JSON

---
# v0.3.1
## MP SIEM test: 
- R22.0.3192
- R23.1.3482
## Incidents
- Учтены изменения в API
---

# v0.2.0
## MP SIEM test: 
- R22.0.3192
## Common
- Зафиксированы имена функций

## Filters
- выгрузка списка папок с фильтрами
- выгрузка списка фильтров
- выгрузка деталей по фильтру

## Health Monitor
- получить общий статус
- получить список ошибок (если есть)
- получить статус лицензии
- получить статус агентов
- получить статус VM контента в MP Core

## Source Monitor
- выгрузка источников и их статуса в мониторинге
- выгрузка форвардеров и их статуса в мониторинге
- получить все источники выбранного форвардера

## Tasks
- остановить, запустить, получить статус задачи
- получить список агентов, модулей, профилей, транспортов, учеток
- получить список задач
- получить список подзадач для активной задачи (история не поддерживается)

---
# v0.1.0
## MP SIEM test: 
- R22.0.3192
## Common
- Аутентификация в IAM, Core
- Аутентификация в PT KB
- Централизованное логирование в каждом модуле (conf/logging.yml)
- Доступ к централизованным настройкам из каждого модуля
- Unit-тесты

## PT KB
- выгрузка списка групп, папок и паков
- выгрузка контента, кроме макросов
- инсталляция и удаление контента из SIEM Server

## Events
- выгрузка событий
- выгрузка с группировкой

## Incidents
- выгрузка инцидентов
- детальная информация по инциденту

## Users and Roles
- выгрузка пользователей, ролей, привилегий, приложений
- детальная информация по пользователю, роле

## Tables
- выгрузка списков из SIEM Server
- выгрузка содержимого
- загрузка содержимого
