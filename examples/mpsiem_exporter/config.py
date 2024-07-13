from dynaconf import Dynaconf
from mpsiemlib.common import *

conf = Dynaconf(
    envvar_prefix="CONF",
    environments=True,
    load_dotenv=True
)

creds = Creds()
settings = Settings()
