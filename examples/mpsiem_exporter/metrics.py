import logging
import sys
from datetime import datetime

from mpsiemlib.modules import MPSIEMWorker
from prometheus_client import Gauge
from requests import HTTPError

from common import *
from config import conf, creds, settings, AuthType, ModuleNames

log = logging.getLogger('logger')


class Metrics:
    def __init__(self):
        creds.core_auth_type = AuthType.LOCAL
        creds.core_login = conf.get('MP_LOGIN')
        creds.core_pass = conf.get('MP_PASSWORD')
        creds.core_hostname = conf.get('MP_CORE_HOSTNAME')

        self.worker = MPSIEMWorker(
            creds=creds,
            settings=settings)
        self.labels_map = dict(eps=['MP SIEM eps statistics', 'type', 'server_name', 'siem'],
                               siem_table_size=['MP SIEM table size', 'fill_type', 'name', 'object_id', 'server_name',
                                                'siem'],
                               siem_lic_expiration_days=['MP SIEM license expiration days', 'key', 'type', 'valid',
                                                         'server_name', 'assets'])
        self.gauges = {}
        self.module = None

        self.update_errors_count = 0

    def get_license_status(self):
        try:
            self.module = self.worker.get_module(ModuleNames.HEALTH)
            lic = self.module.get_health_license_status()
            days = (datetime.strptime(lic.get('expiration'), "%Y-%m-%dT%H:%M:%SZ") - datetime.today()).days
            lic['expiration_days'] = days
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
                if metric == 'siem_lic_expiration_days' and conf.get('CORE') is True:
                    logging.info('Prepare EPS metrics')
                    raw_data = self.get_license_status()
                    label_values = [raw_data.get('key'),
                                    raw_data.get('type'),
                                    raw_data.get('valid'),
                                    conf.get('MP_CORE_HOSTNAME'),
                                    raw_data.get('assets')]
                    self.gauges[metric] = Gauge(metric, data.pop(0), data)
                    self.gauges[metric].labels(*label_values).set(raw_data.get('expiration_days'))
                elif metric == 'eps':
                    log.info('Prepare EPS metrics')
                    raw_data = get_eps(siem_address=conf.get('MP_SIEM_HOSTNAME'))

                    self.gauges[metric] = Gauge(metric, data.pop(0), data)

                    for metric_keys in raw_data.keys():
                        self.gauges[metric].labels(type=metric_keys, server_name=conf.get('MP_SIEM_HOSTNAME'),
                                                   siem='on').set(raw_data.get(metric_keys))
                elif metric == 'siem_table_size' and conf.get('CORE') is True:
                    log.info('Prepare SIEM tables size metrics')
                    self.gauges[metric] = Gauge(metric, data.pop(0), data)
                    for siem_data in get_siem_tables(siem_address=conf.get('MP_SIEM_HOSTNAME')):
                        self.gauges[metric].labels(fill_type=siem_data.get('fillType'),
                                                   name=siem_data.get('name'),
                                                   object_id=siem_data.get('objectId'),
                                                   server_name=conf.get('MP_SIEM_HOSTNAME'), siem='on').set(
                            siem_data.get('currentSize'))

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
            log.info(f'#{self.update_errors_count} Update metrics error "{e}". I will try to update token and continue')
            self.refresh_metrics()
