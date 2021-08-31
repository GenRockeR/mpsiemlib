import os
import logging

from mpsiemlib.common import AuthType, Creds, Settings, setup_logging

LOG_CONF = './conf/logging.yml'
setup_logging(LOG_CONF, logging.INFO)

# Используется только в тестах MPAuth
creds_local = Creds()
creds_local.core_hostname = os.getenv("MP_CORE_HOSTNAME")
creds_local.storage_hostname = os.getenv("MP_STORAGE_HOSTNAME")
creds_local.siem_hostname = os.getenv("MP_SIEM_HOSTNAME")
creds_local.core_auth_type = AuthType.LOCAL
creds_local.core_login = os.getenv("MP_LOGIN")
creds_local.core_pass = os.getenv("MP_PASS")

# Используется во всех тестах
creds_ldap = Creds()
creds_ldap.core_hostname = os.getenv("MP_CORE_HOSTNAME")
creds_ldap.storage_hostname = os.getenv("MP_STORAGE_HOSTNAME")
creds_ldap.siem_hostname = os.getenv("MP_SIEM_HOSTNAME")
creds_ldap.core_auth_type = AuthType.LDAP
creds_ldap.core_login = os.getenv("MP_LOGIN")
creds_ldap.core_pass = os.getenv("MP_PASS")

# Использовать локальную аутентификацию в тестах если это требуется
creds = creds_local if os.getenv("USE_LOCAL_AUTH") else creds_ldap

settings = Settings()
