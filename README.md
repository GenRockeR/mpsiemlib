# Maxpatrol SIEM API SDK
Неофициальный SDK для работы с MP SIEM через API.  
В SDK используется как UI API, так и прямые вызовы к микросервисам вследствие того, что UI API имеет ряд ограничений.  
Пример использования можно посмотреть в tests и examples.

# Поддерживаемые версии
R24.1.x - R27.2.x

# Основные функции
1. Unit-тесты для проверки совместимости с новыми версиями MP SIEM.
2. Аутентификация с Core, KB, Storage.
3. Журналирование и его настройка.
4. Хранение параметров и передача модулям.
5. Работа с активами
6. Работа с событиями в Elasticsearch или API
7. Работа с фильтрами событий.
8. Работа с инцидентами.
9. Работа с табличными списками.
10. Работа с контентом в KB.
11. Работа с задачами сбора.
12. Работа со встроенным мониторингом SIEM.
13. Работа со встроенным мониторингом источников.
14. Работа с пользователями в IAM.

## Сетевой доступ и права
Для работы SDK необходимы следующие сетевые разрешения:
- MP Core: tcp 443, tcp 3334
- PT KB: tcp 8091
- Storage (Elastic): tcp 9200

SDK аутентифицируется в Core, PT KB, PT MC
Ряд функций требует административной учетной записи в IAM, PT KB, SIEM.

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

# Спасибо за помощь
[mkosmach](https://github.com/mkosmach), [srublev](https://github.com/srublev)


# Release Notes
[CHANGELOG.md](CHANGELOG.md)

