import logging
import os

from mpsiemlib.common import AuthType, Creds, Settings, setup_logging

LOG_CONF = "./conf/logging.yml"
setup_logging(LOG_CONF, logging.INFO)

# Используется только в тестах MPAuth
creds_local = Creds()
creds_local.core_hostname = os.getenv("MP_CORE_HOSTNAME")
creds_local.storage_hostname = os.getenv("MP_STORAGE_HOSTNAME")
creds_local.siem_hostname = os.getenv("MP_SIEM_HOSTNAME")
creds_local.core_auth_type = AuthType.LOCAL
creds_local.core_login = os.getenv("MP_LOGIN")
creds_local.core_pass = os.getenv("MP_PASS")
creds_local.client_secret = os.getenv("CLIENT_SECRET")

# Используется во всех тестах
creds_ldap = Creds()
creds_ldap.core_hostname = os.getenv("MP_CORE_HOSTNAME")
creds_ldap.storage_hostname = os.getenv("MP_STORAGE_HOSTNAME")
creds_ldap.siem_hostname = os.getenv("MP_SIEM_HOSTNAME")
creds_ldap.core_auth_type = AuthType.LDAP
creds_ldap.core_login = os.getenv("MP_LOGIN")
creds_ldap.core_pass = os.getenv("MP_PASS")
creds_ldap.client_secret = os.getenv("CLIENT_SECRET")

# PAT Token
creds_pat = Creds()
creds_pat.core_hostname = os.getenv("MP_CORE_HOSTNAME")
creds_pat.storage_hostname = os.getenv("MP_STORAGE_HOSTNAME")
creds_pat.siem_hostname = os.getenv("MP_SIEM_HOSTNAME")
creds_pat.core_auth_type = AuthType.PAT_TOKEN
creds_pat.pat_token = os.getenv("MP_PAT_TOKEN")

# Использовать локальную аутентификацию в тестах если это требуется
# creds = creds_local if os.getenv("USE_LOCAL_AUTH") else creds_ldap
creds = creds_pat

settings = Settings()
