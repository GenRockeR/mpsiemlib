import logging
import logging.config
import os
import time
import warnings
from typing import Any

import requests
import urllib3
import yaml

warnings.simplefilter("ignore", urllib3.exceptions.InsecureRequestWarning)

log = logging.getLogger("Common")

_MAX_LOG_BODY_SIZE = 2048


def _truncate(text: str, limit: int = _MAX_LOG_BODY_SIZE) -> str:
    if len(text) <= limit:
        return text
    return text[:limit] + "...<truncated>"


def _mask_headers(headers: Any) -> Any:
    if not isinstance(headers, dict):
        return headers
    return {k: "***" if k.lower() == "authorization" else v for k, v in headers.items()}


def setup_logging(
    default_path: str = "logging.yml",
    default_level: int = logging.INFO,
    env_key: str = "LOG_CFG",
) -> None:
    """Настройка логирования из YAML-конфига.

    :param default_path: Путь к конфигу по умолчанию
    :param default_level: Уровень логирования, если конфиг не найден
    :param env_key: Имя переменной окружения с путём к конфигу
    """
    path = os.getenv(env_key) or default_path
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            config = yaml.safe_load(f)
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)


def exec_request(
    session: requests.Session,
    url: str,
    method: str = "GET",
    timeout: float = 30,
    timeout_up: float = 1,
    **kwargs: Any,
) -> requests.Response:
    """Выполнение HTTP запросов. Если в окружении MP_DEBUG_LOG_BODY, выводит в
    DEBUG лог сырой ответ от сервера.

    :param session:
    :param url:
    :param method: Метод GET|POST|PUT|DELETE
    :param timeout: timeout соединения
    :param timeout_up: увеличение timeout от базового на коэффициент
        (нужно при генерации отчетов)
    :param kwargs: параметры запроса, передаваемые в requests
    :return: Response
    :raises requests.RequestException: при ошибке выполнения запроса
    """

    log_body = "MP_DEBUG_LOG_BODY" in os.environ
    if log_body:  # включаем verbose для requests
        import http.client as http_client

        http_client.HTTPConnection.debuglevel = 1

    response: requests.Response
    request_timeout: float | tuple[float, float] = (
        (timeout * timeout_up, timeout * timeout_up * 2)
        if method not in ("PUT", "DELETE")
        else timeout * timeout_up
    )

    body = ""
    if log_body:
        body = _truncate(str(kwargs.get("data")) + str(kwargs.get("json")))
    log.debug(
        f'status=prepare, action=request, msg="Try to exec request", url={url!r}, '
        f"method={method!r}, "
        f'body="{body if log_body else "masked"}", '
        f"headers={_mask_headers(kwargs.get('headers'))!r}, "
        f'parameters="{kwargs.get("params")!r}"'
    )

    try:
        response = session.request(
            method,
            url,
            verify=False,
            timeout=request_timeout,
            **kwargs,
        )
        response.raise_for_status()
    except Exception as err:
        err_response: requests.Response | None = getattr(err, "response", None)
        error_text = _truncate(err_response.text) if err_response is not None else ""
        code = err_response.status_code if err_response is not None else "0"
        log.error(
            f"url={url!r}, status=failed, action=request, msg={err!r}, "
            f'error="{error_text}", '
            f"code={code}"
        )
        raise

    if log_body:
        log.debug(f"status=success, action=request, msg={_truncate(response.text)!r}")

    return response


def get_metrics_start_time() -> float:
    return time.perf_counter() * 1000


def get_metrics_took_time(start_time: float) -> float:
    return (time.perf_counter() * 1000) - start_time
