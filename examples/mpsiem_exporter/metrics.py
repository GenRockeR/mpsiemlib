import logging
import sys
from datetime import datetime, timezone

from common import get_eps, get_siem_tables
from config import conf, creds, settings
from prometheus_client import Gauge
from requests import HTTPError

from mpsiemlib.common import AuthType, ModuleNames
from mpsiemlib.modules import MPSIEMWorker

log = logging.getLogger("logger")


def _parse_expiration(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


class Metrics:
    def __init__(self):
        token = conf.get("MP_PAT_TOKEN")
        if token:
            creds.core_auth_type = AuthType.PAT_TOKEN
            creds.pat_token = token
        else:
            creds.core_auth_type = (
                AuthType.LDAP if conf.get("MP_USE_LDAP") else AuthType.LOCAL
            )
            creds.core_login = conf.get("MP_LOGIN")
            creds.core_pass = conf.get("MP_PASSWORD")
        creds.core_hostname = conf.get("MP_CORE_HOSTNAME")

        self.worker = MPSIEMWorker(creds=creds, settings=settings)
        self.labels_map = dict(
            eps=["MP SIEM eps statistics", "type", "server_name", "siem"],
            siem_table_size=[
                "MP SIEM table size",
                "fill_type",
                "name",
                "object_id",
                "server_name",
                "siem",
            ],
            siem_lic_expiration_days=[
                "MP SIEM license expiration days",
                "key",
                "type",
                "valid",
                "server_name",
                "assets",
            ],
        )
        self.gauges = {}
        self.module = None

        self.update_errors_count = 0

    def get_license_status(self):
        try:
            self.module = self.worker.get_module(ModuleNames.HEALTH)
            lic = self.module.get_health_license_status()
            expiration = _parse_expiration(lic.get("expiration"))
            if expiration is not None:
                now = datetime.now(timezone.utc if expiration.tzinfo else None)
                lic["expiration_days"] = (expiration - now).days
            else:
                lic["expiration_days"] = 0
            # R27.0+: licensing_v4 не отдаёт `valid`/`assets`
            if "valid" not in lic:
                lic["valid"] = not lic.get("is_grace_period_expired", False)
            lic.setdefault("assets", None)
            return lic
        except Exception as e:
            logging.critical(e, exc_info=True)
            sys.exit(255)

    def clear_metrics(self):
        for gauge in self.gauges.values():
            gauge.clear()

    def update_metrics(self):
        for metric, data in self.labels_map.items():
            if metric not in self.gauges:
                if metric == "siem_lic_expiration_days" and conf.get("CORE") is True:
                    logging.info("Prepare EPS metrics")
                    raw_data = self.get_license_status()
                    label_values = [
                        raw_data.get("key"),
                        raw_data.get("type"),
                        raw_data.get("valid"),
                        conf.get("MP_CORE_HOSTNAME"),
                        raw_data.get("assets"),
                    ]
                    self.gauges[metric] = Gauge(metric, data.pop(0), data)
                    self.gauges[metric].labels(*label_values).set(
                        raw_data.get("expiration_days")
                    )
                elif metric == "eps":
                    log.info("Prepare EPS metrics")
                    raw_data = get_eps(siem_address=conf.get("MP_SIEM_HOSTNAME"))

                    self.gauges[metric] = Gauge(metric, data.pop(0), data)

                    for metric_keys in raw_data.keys():
                        self.gauges[metric].labels(
                            type=metric_keys,
                            server_name=conf.get("MP_SIEM_HOSTNAME"),
                            siem="on",
                        ).set(raw_data.get(metric_keys))
                elif metric == "siem_table_size" and conf.get("CORE") is True:
                    log.info("Prepare SIEM tables size metrics")
                    self.gauges[metric] = Gauge(metric, data.pop(0), data)
                    for siem_data in get_siem_tables(
                        siem_address=conf.get("MP_SIEM_HOSTNAME")
                    ):
                        self.gauges[metric].labels(
                            fill_type=siem_data.get("fillType"),
                            name=siem_data.get("name"),
                            object_id=siem_data.get("objectId"),
                            server_name=conf.get("MP_SIEM_HOSTNAME"),
                            siem="on",
                        ).set(siem_data.get("currentSize"))

    def refresh_metrics(self):
        self.clear_metrics()
        try:
            self.update_metrics()
            self.update_errors_count = 0
        except HTTPError as e:
            if self.update_errors_count > 2:
                log.error(f'I can not update metrics. Error "{e}". Bye!')
                sys.exit(1)
            self.update_errors_count += 1
            log.info(
                f'#{self.update_errors_count} Update metrics error "{e}". I will try to update token and continue'
            )
            self.refresh_metrics()
