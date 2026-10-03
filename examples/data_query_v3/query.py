"""Сбор событий через EventsAPI (контракт Events.DataQuery v3).

Метод `get_events_by_filter_v3` требует обязательного временного диапазона
и PDQL-фильтра в виде одной строки.

Пример запуска:

    python query.py --core mow03-mpsiem-dev.soc.bi.zone \
        --token "$MP_PAT_TOKEN" --minutes 5
"""

import argparse
import logging
import sys
import time

from mpsiemlib.common import AuthType, Creds, ModuleNames, Settings
from mpsiemlib.modules import MPSIEMWorker

FORMAT = "%(asctime)s - [%(filename)s][%(funcName)s] - %(levelname)s - %(message)s"

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--core", help="core address or fqdn", required=True)
    parser.add_argument("--token", help="PAT token (MP SIEM 26.0+)", required=True)
    parser.add_argument(
        "--minutes", type=int, default=5, help="time window in minutes (default: 5)"
    )
    parser.add_argument(
        "--filter",
        dest="pdql",
        default="select(time, event_src.host, event_src.ip)",
        help="PDQL filter as a single string",
    )
    parser.add_argument(
        "-d", "--debug", help="increase output verbosity", action="store_true"
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO, format=FORMAT
    )
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    log = logging.getLogger("data_query_v3")

    creds = Creds()
    creds.core_hostname = args.core
    creds.core_auth_type = AuthType.PAT_TOKEN
    creds.pat_token = args.token

    settings = Settings()

    try:
        mpsiemworker = MPSIEMWorker(creds, settings)
        events_api = mpsiemworker.get_module(ModuleNames.EVENTSAPI)
    except Exception as e:
        log.error(f"Ошибка при подключении: {e}")
        sys.exit(255)

    # Временной диапазон: текущее время минус N минут
    time_to = int(time.time())
    time_from = time_to - args.minutes * 60

    events_count = 0
    try:
        for event in events_api.get_events_by_filter_v3(
            query_filter=args.pdql,
            time_from=time_from,
            time_to=time_to,
        ):
            events_count += 1
            print(event)
    finally:
        events_api.close()

    log.info(f"Получено событий: {events_count}")
