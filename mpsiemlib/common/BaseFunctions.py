import os
import time
import urllib3
import requests
import logging
import logging.config

from urllib3.exceptions import InsecureRequestWarning

urllib3.disable_warnings(InsecureRequestWarning)

log = logging.getLogger('Common')


def setup_logging(default_path='logging.yml', default_level=logging.INFO, env_key='LOG_CFG'):
    import yaml.parser
    path = default_path
    value = os.getenv(env_key, None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path, 'rt') as f:
            config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)


def exec_request(session: requests.Session, url: str, method='GET', timeout=30, timeout_up=1,
                 **kwargs) -> requests.Response:
    """Выполнение HTTP запросов Если в окружении MP_DEBUG_LOG_BODY, выводит в
    DEBUG лог сырой ответ от сервера.

    :param session:
    :param url:
    :param method: метод GET|POST
    :param timeout: timeout соединения
    :param timeout_up: увеличение timeout от базового на коэффициент
        (нужно при генерации отчетов)
    :param kwargs: параметры запроса, передаваемые в requests
    :return: requests.Response
    """

    log_body = 'MP_DEBUG_LOG_BODY' in os.environ
    if log_body:  # включаем verbose для requests
        import http.client as http_client
        http_client.HTTPConnection.debuglevel = 1

    response = None
    log.debug('status=prepare, action=request, '
              'msg="Try to exec request", '
              'url="{}", method="{}", body="{}", headers="{}", '
              'parameters="{}"'.format(url, method,
                                       str(kwargs.get('data')) + str(kwargs.get('json')) if log_body else 'masked',
                                       kwargs.get('headers'),
                                       kwargs.get('params'))
              )

    try:
        if method == 'POST':
            response = session.post(url,
                                    verify=False,
                                    timeout=(timeout * timeout_up, timeout * timeout_up * 2),
                                    **kwargs)
        elif method == 'DELETE':
            response = session.delete(url,
                                      verify=False,
                                      timeout=(timeout * timeout_up),
                                      **kwargs)
        elif method == 'PUT':
            response = session.put(url,
                                   verify=False,
                                   timeout=(timeout * timeout_up),
                                   **kwargs)
        else:
            response = session.get(url,
                                   verify=False,
                                   timeout=(timeout * timeout_up, timeout * timeout_up * 2),
                                   **kwargs)
        response.raise_for_status()
        if response.status_code >= 400:
            raise Exception()
    except Exception as err:
        log.error('url="{}", status=failed, action=request, msg="{}", '
                  'error="{}", code={}'.format(url,
                                               err,
                                               response.text if response is not None else '',
                                               response.status_code if response is not None else '0'))
        raise err

    if log_body:
        log.debug(f'status=success, action=request, msg="{response.text}"')

    return response


def get_metrics_start_time():
    return int(time.time() * 1000)


def get_metrics_took_time(start_time):
    return int(time.time() * 1000) - start_time
